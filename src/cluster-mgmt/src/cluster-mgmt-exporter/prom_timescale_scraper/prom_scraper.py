# Copyright (C) 2024 Cerebras Systems - All Rights Reserved

import urllib.request
import urllib.parse
import ssl
import socket
import re
import json

class PromDatum:
    def __init__(self, name, labels, value, timestamp):
        self.name = name
        self.labels = labels
        self.value = value
        self.timestamp = timestamp

class PromScraper:
    dns_cache = {}
    def __init__(self):
        # We need to allow for self-signed certs
        self.ssl_ctx = ssl.create_default_context()
        self.ssl_ctx.check_hostname=False
        self.ssl_ctx.verify_mode=ssl.CERT_NONE

        # Override DNS to allow specific hosts
        self.override_dns_setup()

    def override_dns(self, host, ip):
        self.dns_cache[host] = ip

    def override_dns_setup(self):
        """
        Not all prom servers have dns set up, enable backups here
        Backup dict is global. Repeatedly calling this function is 
        inefficient, but will still work. 
        """
        prv_getaddrinfo = socket.getaddrinfo

        def new_getaddrinfo(*args):
            """
            Trys to resolve the address, if it fails, it checks the cache to see
            if there is an address set. If yes, use that IP, if not, propagate the 
            error
            """
            try: 
                return prv_getaddrinfo(*args)
            except socket.gaierror as err:
                if args[0] in PromScraper.dns_cache:
                    return prv_getaddrinfo(PromScraper.dns_cache[args[0]], *args[1:])
                else:
                    raise err
        # Override the underlying getaddrinfo function
        socket.getaddrinfo = new_getaddrinfo

    def scrape_prom_server(self, host, match_conditions):
        """
        scrapes data from host, using federate endpoint, for query matching 
        the match conditions
        """
        req = urllib.parse.quote(f"{{{match_conditions}}}")
        url = f"https://{host}/api/v1/query?query={req}"
        results = []
        try:
            result = urllib.request.urlopen(url, context=self.ssl_ctx).read().decode("utf-8")
            data = json.loads(result)
            if data["status"] != "success":
                return None
            if data['data']['resultType'] != "vector":
                return None
            for metric in data['data']['result']:
                results.append(PromDatum(metric['metric']['__name__'], metric['metric'], metric['value'][1], metric['value'][0]))
        except urllib.error.URLError:
            return None
        except KeyError:
            return None
        return results




if __name__ == "__main__":
    # Just some basic tests for now
    scraper = PromScraper()
    scraper.override_dns("prometheus.multibox-hostio.cerebrassc.local", "172.28.103.20")
    match_conditions = f'job=~".*cluster-mgmt-system-exporter", __name__="system_network_flap_count"'
    data = scraper.scrape_prom_server("prometheus.multibox-hostio.cerebrassc.local", match_conditions)
    if data is not None:
        print(len(data))
        for datum in data:
            print(f"{datum.name}, {datum.labels['system']}, {datum.value}, {datum.timestamp}")

    scraper.override_dns("query.multibox-301.cerebrassc.local", "172.28.219.20")
    data = scraper.scrape_prom_server("query.multibox-301.cerebrassc.local", match_conditions)
    if data is not None:
        print(len(data))
        for datum in data:
            print(f"{datum.name}, {datum.labels['system']}, {datum.value}, {datum.timestamp}")

    scraper.override_dns("query.multibox-30.cerebrassc.local", "172.28.53.20")
    data = scraper.scrape_prom_server("query.multibox-30.cerebrassc.local", match_conditions)
    if data is not None:
        print(len(data))
        for datum in data:
            print(f"{datum.name}, {datum.labels['system']}, {datum.value}, {datum.timestamp}")

