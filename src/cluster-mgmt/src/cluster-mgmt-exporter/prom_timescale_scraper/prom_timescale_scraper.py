# Copyright (C) 2024 Cerebras Systems - All Rights Reserved

from prom_scraper import PromScraper
from pathlib import Path
import json
import diag_db_lib as db_lib
import datetime

class Prom2Timescale:
    """
    Scrapes a prometheus database based on a config, and sends that 
    data to a timescaledb instance. 
    The config has two main parts, db_info, which describes the database 
    that data will be pushed to, and scrapers, which contains one element 
    for each table that needs to be populated

    Within each scraper, there are several componenets: 
        match_conditions: This provides the promQL conditions that will 
            be used to make the request to the prometheus server being scraped
        metrics: This is a dict, where the key represents the prometheus 
            metric name, and the value represents the sql column name
        labels: This is a dict, which represents a mapping of a prom label name
            to an sql column name
        prom_keys: This is a list of labels that are used to find distinct nodes
        schema: The sql schema to use
        table: The sql table to insert into (and create)
        columns: The columns that should be inserted into the table. Note that 
            these columns must appear in either "metrics" or "labels", with the 
            exception of "time" and "multibox" which are added automatically. 
        primary key: This defines the columns that make up the primary key of 
            the table
    """
    def __init__(self, config_file, hosts=None, multiboxes=None):
        """
        config_file: a json file specifying the queries and stats to use
        hosts: list of prometheus hosts to scrape
        multiboxes: list of multiboxes associated with the given hosts
        """
        with Path(config_file).open('r') as f:
            self.config = json.load(f)
        self.hosts = hosts if hosts is not None else []
        self.multiboxes = multiboxes if multiboxes is not None else []
        assert len(self.hosts) == len(self.multiboxes), \
            "must provide a multibox list the same length as host list"
        self.scraper = PromScraper()
        self.create_tables()

    def add_host(self, host, multibox, ip=None):
        """
        Add a host to the scraper
        """
        self.hosts.append(host)
        self.multiboxes.append(multibox)
        if ip is not None:
            self.scraper.override_dns(host, ip)

    def scrape_prom(self):
        """
        Scrape the prometheus server, returns a dict containing a list dicts, 
        each dict in the list represents a DB row to be inserted
        """
        results = {}
        for node_type in self.config["scrapers"]:
            results[node_type] = []
            match_conditions = self.config["scrapers"][node_type]["match_conditions"]
            metrics = self.config["scrapers"][node_type]["metrics"]
            keys = self.config["scrapers"][node_type]["prom_keys"]
            labels = self.config["scrapers"][node_type]["labels"]
            for host, multibox in zip(self.hosts, self.multiboxes):
                data = self.scraper.scrape_prom_server(host, match_conditions)
                if data is None:
                    print(f"Failed to run {node_type} on {host}")
                    continue
                if len(data) == 0:
                    print(f"Running {node_type} on {host} returned no results")
                    continue
                else:
                    count = 0
                    query_results = {}
                    for res in data:
                        if res.name in metrics:
                            metric_name = metrics[res.name]
                            try:
                                res_keys = tuple([res.labels[k] for k in keys])
                                if res_keys not in query_results:
                                    query_results[res_keys] = {}
                                    # keep just one timestamp
                                    query_results[res_keys]["time"] = datetime.datetime.utcfromtimestamp(int(res.timestamp)/1000).strftime("%Y-%m-%d %H:%M:%S%z")
                                    query_results[res_keys]["multibox"] = multibox
                                for l in labels:
                                    if l in res.labels:
                                        query_results[res_keys][labels[l]] = res.labels[l]
                                query_results[res_keys][metric_name] = res.value
                                count += 1
                            except KeyError:
                                print(f"Hit key error for {res}")
                                pass
                results[node_type] += [query_results[k] for k in query_results]
        return results

    def create_tables(self):
        for node_type in self.config["scrapers"]:
            schema_name = self.config["scrapers"][node_type]["schema"]
            table_name = self.config["scrapers"][node_type]["table"]
            columns = self.config["scrapers"][node_type]["columns"]
            pk = self.config["scrapers"][node_type]["primary_key"]
            pk = ", ".join(pk)
            pk = f"PRIMARY KEY({pk})"
            db_conn = self.set_up_db_conn(schema_name, table_name)
            # create a table if it doesn't already exist
            db_conn._create_tbl(columns, pk)


    def set_up_db_conn(self, schema_name, table_name):
        db_info = self.config["db_info"]
        db = db_lib.DiagDB(db_info["db_database"], schema_name, table_name, db_info["db_host"], db_info["db_user"], db_info["db_password"], db_info["db_port"])
        return db

    def insert_into_db(self, data):
        for node_type in data:
            if len(data[node_type]) == 0:
                continue
            metrics = self.config["scrapers"][node_type]["metrics"]
            metric_keys = [metrics[m] for m in metrics]
            table_name = self.config["scrapers"][node_type]["table"]
            schema_name = self.config["scrapers"][node_type]["schema"]
            columns = self.config["scrapers"][node_type]["columns"]
            column_names = [k for k in columns.keys()]
            db_conn = self.set_up_db_conn(schema_name, table_name)
            column_str = ', '.join(column_names)
            command = f"INSERT INTO {table_name} ({column_str}) VALUES " 
            all_values = []
            all_value_strs = []
            for entry in data[node_type]:
                local_values = []
                for k in column_names:
                    if columns[k] in ["INT", "BIGINT"]:
                        # We need to convert float->int because prometheus serves
                        # ints in scientific notation which python doesn't handle
                        local_values.append(int(float(entry[k])) if k in entry else None)
                    elif columns[k] == ["FLOAT"]:
                        local_values.append(float(entry[k]) if k in entry else None)
                    else:
                        local_values.append(entry[k] if k in entry else None)
                all_values += local_values
                value_str = ', '.join(['%s'] * len(column_names))
                all_value_strs.append(f"({value_str})")
            all_values_str = ', '.join(all_value_strs)
            command += all_values_str
            command += " ON CONFLICT DO NOTHING"
            command += ";"
            rc = db_conn._execute_query(command, all_values)
            if rc == -1:
                print(f'Failed to push {node_type} stats into Postgres DB')
            else:
                print(f'Inserted {len(data[node_type])} entries for {node_type}')

    def scrape_and_insert(self):
        self.insert_into_db(self.scrape_prom())

if __name__ == "__main__":
    """
    Just a basic test
    """
    import os
    import time
    gittop = os.getenv("GITTOP")
    scraper_conf = f"{gittop}/src/cluster_mgmt/src/cluster-mgmt-exporter/prom_timescale_scraper/config_scraper.json"
    converter = Prom2Timescale(scraper_conf)
    converter.add_host("prometheus.multibox-hostio.cerebrassc.local", "multibox-hostio", ip="172.28.103.20")
    converter.add_host("query.multibox-301.cerebrassc.local", "multibox-301", ip="172.28.219.20")
    converter.add_host("query.multibox-30.cerebrassc.local", "multibox-30", ip="172.28.53.20")

    start = time.time()
    results = converter.scrape_prom()
    mid = time.time()
    converter.insert_into_db(results)
    end = time.time()

    for x in results:
        print(f"{x}: {len(results[x])}")
    print(f"Collect took {mid - start:.2f}s, Insert took {end - mid:.2f}s, total {end - start:.2f}s")

