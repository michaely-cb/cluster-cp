"""
CSX (System) Survey module for linkmon
Nothing in this file is executed when running with Prometheus - instead CSX
stats are provided by system_exporter, which uses the common redfish_fetcher
code.
"""
import os
import re
import sqlite3
import sys
import threading
import time
from http import HTTPStatus

import requests
from requests.auth import HTTPBasicAuth

from redfish_fetcher import SystemRedfishNetStats


class CSXSurvey:
    """
    Reuse SystemRedfishFetcher to report stats to linkmon - used for standalone version only
    """

    eth_query = "/redfish/v1/Systems/system/EthernetInterfaces/N"
    fmt_str = "%-12s %-4s %-18s %-20s %5s %12s %11s %12s %15s %-8s"
    headings = (
        "CS2",
        "Port",
        "MAC",
        "LinkStatus",
        "Flaps",
        "RxKBytes",
        "Bad FCS Pkt",
        "FEC Corr Blk",
        "FEC Uncorr Blk",
        "Health",
    )

    @classmethod
    def sqlite_create_table_cmd(cls):
        csx_create_query = """
CREATE TABLE IF NOT EXISTS csx (
id INTEGER PRIMARY KEY
"""
        for hkey in CSXSurvey.headings:
            hkey = re.sub(r" ", "_", hkey)
            csx_create_query += f",{hkey} TEXT NOT NULL"

        csx_create_query += ");"
        return csx_create_query

    def __init__(self, csx_list):
        self.stats = dict()
        self.key = 0

        if csx_list:
            self.csxs = list(csx_list)
            t1 = time.time()
            thr = []
            for c in self.csxs:
                t = threading.Thread(target=self.collect, args=(c,))
                thr.append(t)
                t.start()
            for cc in thr:
                cc.join()
            t2 = time.time()
            self.populate()
            t3 = time.time()

            self.collect_time = t2 - t1
            self.process_time = t3 - t2
            return

    def collect(self, c):
        # handle collection in a generic way, let the CS2Info cons pick out what it needs
        iface_stats = []

        user = os.getenv('CSX_USER', 'admin')
        password = os.getenv('CSX_PASSWORD', 'admin')

        for i in range(0, 12):
            iface_stats.append(dict())

        for i in range(0, 12):
            eth_request_uri = f"https://{c}{__class__.eth_query}{i:0>2}"
            try:
                response = requests.get(
                    eth_request_uri, auth=HTTPBasicAuth(user, password), verify=False, timeout=3
                )
            except requests.exceptions.ConnectionError:
                # The cs2 is down. Make an empty entry and go to the next CS2.
                self.stats[c] = CS2Info(c, iface_stats)
                return
            if response.status_code == HTTPStatus.OK:
                try:
                    content = response.json()
                except requests.exceptions.JSONDecodeError:
                    continue
                iface_stats[i]['link_status'] = content["LinkStatus"]
                iface_stats[i]['mac'] = content["MACAddress"]

        sys_stats = SystemRedfishNetStats(user, password)
        sys_stats.fetch_for_system(c, c)
        for rm in sys_stats.metrics:
            for s in rm.metric_family.samples:
                nic_id = int(s.labels['port'][1:])  # drop the 'n', already 0-idx
                val = int(float(s.value))
                iface_stats[nic_id][rm.name] = val

        self.stats[c] = CS2Info(c, iface_stats)

    def populate(self):
        pass

    @staticmethod
    def print_header(dest=sys.stdout):
        print(CSXSurvey.fmt_str % CSXSurvey.headings, file=dest)

    @classmethod
    def print_two_runs(cls, survey1, survey2, dest=sys.stdout):
        # find the CSXs that weren't present in both runs
        # or didn't have data because they were in standby, etc
        init_set = set({k: survey1.stats[k] for k in survey1.stats if survey1.stats[k].has_data})
        final_set = set({k: survey2.stats[k] for k in survey2.stats if survey2.stats[k].has_data})

        up_down_set = init_set - final_set
        down_up_set = final_set - init_set
        for k in sorted((init_set | final_set)):  # everything
            if k in init_set:
                i = common = survey1.stats[k]
            if k in final_set:
                f = common = survey2.stats[k]

            for port in range(0, 12):
                if k in up_down_set:
                    linkstatus = i.port_list[port].link_status + " -> ?"
                    flaps = rxbytes = bad_fcs = corr = uncorr = "?"
                    health = "BAD"
                elif k in down_up_set:
                    linkstatus = "? -> " + f.port_list[port].link_status
                    flaps = rxbytes = bad_fcs = corr = uncorr = "?"
                    health = "BAD"
                else:
                    ip = i.port_list[port]
                    fp = f.port_list[port]
                    linkstatus = ip.link_status + " -> " + fp.link_status
                    flaps = fp.link_flap_count - ip.link_flap_count
                    rxbytes = int((fp.rx_bytes - ip.rx_bytes) / 1024)
                    bad_fcs = fp.rx_pkt_bad_fcs - ip.rx_pkt_bad_fcs
                    corr = fp.rx_fec_corr - ip.rx_fec_corr
                    uncorr = fp.rx_fec_uncorr - ip.rx_fec_uncorr

                    if flaps != 0:
                        health = "FLAP"
                    elif ip.link_status == "LinkUp" and fp.link_status == "LinkUp":
                        if (
                                fp.rx_pkt_bad_fcs - ip.rx_pkt_bad_fcs != 0
                                or fp.rx_fec_uncorr - ip.rx_fec_uncorr != 0
                        ):
                            health = "ERRORS"
                        elif corr / ((1 + fp.rx_bytes - ip.rx_bytes) / 1024) > 1:
                            health = "MANY_CORRECTED_ERRORS"
                        elif fp.rx_fec_corr - ip.rx_fec_corr > 0:
                            health = "SOME_CORRECTED"
                        else:
                            health = "GOOD"
                    else:
                        health = "BAD"

                out_tuple = (
                    common.name,
                    common.port_list[port].port_id,
                    common.port_list[port].mac,
                    linkstatus,
                    flaps,
                    rxbytes,
                    bad_fcs,
                    corr,
                    uncorr,
                    health,
                )

                print(CSXSurvey.fmt_str % out_tuple, file=dest)

    def insert_run(self, dbfile):
        if not self.stats:
            return

        con = sqlite3.connect(dbfile)
        cur = con.cursor()
        for s in self.stats:
            if not self.stats[s].has_data:
                continue
            for p in range(0, 12):
                dbcmd = "INSERT INTO csx VALUES(?,   ?, ?, ?, ?,   ?, ?, ?, ?,   ?,   ?)"

                data = (
                    None,
                    s,
                    p,
                    self.stats[s].port_list[p].mac,
                    self.stats[s].port_list[p].link_status,
                    self.stats[s].port_list[p].link_flap_count,
                    int(self.stats[s].port_list[p].rx_bytes),
                    self.stats[s].port_list[p].rx_pkt_bad_fcs,
                    self.stats[s].port_list[p].rx_fec_corr,
                    self.stats[s].port_list[p].rx_fec_uncorr,
                    "",
                )  # health

                cur.execute(dbcmd, data)
                self.stats[s].port_list[p].key = cur.lastrowid

        surveycmd = "INSERT INTO survey(survey_type) VALUES('csx')"
        cur.execute(surveycmd)
        survey_id = cur.lastrowid

        for s in self.stats:
            if not self.stats[s].has_data:
                continue
            for p in range(0, 12):
                dscmd = f"INSERT INTO device_survey(survey, devicequery) VALUES(?, ?)"
                data = (survey_id, self.stats[s].port_list[p].key)
                cur.execute(dscmd, data)

        con.commit()
        con.close()
        self.key = survey_id

    def from_db(self, con, csxid):
        cur = con.cursor()
        # get last runs
        qstring = """
SELECT *
FROM device_survey
    INNER JOIN csx ON device_survey.devicequery = csx.id
WHERE device_survey.survey = ?
"""
        res = cur.execute(qstring, (csxid,))
        rows = res.fetchall()
        self.csxs = []
        for db_row in rows:
            csname = db_row["CS2"]
            portnum = int(db_row["Port"])
            if not csname in self.stats:
                self.stats[csname] = CS2Info(csname, None)
                self.stats[csname].has_data = True
                self.stats[csname].port_list = [None] * 12
                self.csxs.append(csname)

            pi = CS2FpgaPortInfo(portnum)
            pi.from_db(db_row)
            self.stats[csname].port_list[portnum] = pi


class CS2Info:
    def __init__(self, name, iface_stats):
        self.name = name
        self.port_list = []
        if not iface_stats:
            self.has_data = False
            # print("setting has_data false for name=%s" % name)
            return
        self.has_data = True
        if iface_stats:
            try:
                for i in range(0, 12):
                    self.port_list.append(CS2FpgaPortInfo(i))
                    self.port_list[i].mac = iface_stats[i]['mac']
                    self.port_list[i].link_status = iface_stats[i]['link_status']
                    self.port_list[i].link_flap_count = iface_stats[i]['link_flap_count']
                    self.port_list[i].rx_bytes = iface_stats[i]['rx_bytes']
                    self.port_list[i].rx_pkt_bad_fcs = iface_stats[i]['rx_pkt_bad_fcs']
                    self.port_list[i].rx_fec_corr = iface_stats[i]['rx_fec_corr']
                    self.port_list[i].rx_fec_uncorr = iface_stats[i]['rx_fec_uncorr']
            except KeyError:
                # iface_stats was not fully populated. Probably the system is down or in standby.
                self.has_data = False


class CS2FpgaPortInfo:
    def __init__(self, i):
        self.rowid = 0

        self.port_id = f"n{i:0>2}"
        self.mac = ""
        self.link_status = "unk"
        self.link_flap_count = 0
        self.rx_bytes = 0
        self.rx_pkt_bad_fcs = 0
        self.rx_fec_corr = 0
        self.rx_fec_uncorr = 0

    def from_db(self, db_row):
        self.port_id = f"n{int(db_row['Port']):0>2}"
        self.mac = db_row["MAC"]
        self.link_status = db_row["LinkStatus"]
        self.link_flap_count = db_row["Flaps"]
        self.rx_bytes = int(db_row["RxKBytes"])
        self.rx_pkt_bad_fcs = int(db_row["Bad_FCS_Pkt"])
        self.rx_fec_corr = int(db_row["FEC_Corr_Blk"])
        self.rx_fec_uncorr = int(db_row["FEC_Uncorr_Blk"])


def main():
    if len(sys.argv) < 2:
        sys.exit()
    if sys.argv[1] != "test":
        sys.exit()

    import urllib3

    urllib3.disable_warnings()  # don't show certificate/https spam

    s1list = []
    s2list = []
    if len(sys.argv) > 2 and sys.argv[2] and sys.argv[2] != "-":
        s1list = [sys.argv[2]]
    if len(sys.argv) > 3 and sys.argv[3] and sys.argv[3] != "-":
        s2list = [sys.argv[3]]
    survey1 = CSXSurvey(s1list)
    survey2 = CSXSurvey(s2list)

    CSXSurvey.print_header()
    CSXSurvey.print_two_runs(survey1, survey2)


if __name__ == "__main__":
    main()
