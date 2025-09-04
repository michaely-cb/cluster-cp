# Copyright (C) 2024 Cerebras Systems - All Rights Reserved

import argparse
from pathlib import Path
import socket
import time

from cerebras.envapi import get_multibox_management_hosts
from prom_timescale_scraper import Prom2Timescale

# Some MBs require you to query Thanos rather than prometheus
thanos_mbs = ["multibox-cg2", "multibox-30", "multibox-301"]

def parse_args():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description="Scrape prometheus DBs and upload to timescale")
    
    parser.add_argument("-i", "--interval", type=int, help="Scraping interval, in seconds")
    parser.add_argument("-m", "--multibox", action="append", default=[], help="List of multiboxes to scrape")
    parser.add_argument("-r", "--runtime", type=int, help="Total runtime in minutes")
    parser.add_argument("-c", "--conf", type=Path, help="Scraper config file")
    
    args = parser.parse_args()
    assert args.conf.exists(), f"Provided conf file not found {args.conf}"
    return args


def main(args):
    """
    example: 
        python run_scraper.py --interval 350 --multibox multibox-hostio --runtime 30 --conf ./config_scraper.json
    """
    converter = Prom2Timescale(args.conf)
    prom_urls = []
    prom_backup_ips = []
    for mb in args.multibox:
        if mb in thanos_mbs:
            url = f"query.{mb}.cerebrassc.local"
        else:
            url = f"prometheus.{mb}.cerebrassc.local"
        ip = None
        try: 
            mgmt_host = get_multibox_management_hosts(mb)[0]
            ip = socket.gethostbyname_ex(mgmt_host)[2][0]
        except socket.gaierror:
            print(f"Failed to get IP for {mgmt_host[0]}, will still attempt collection")
        except IndexError:
            print(f"Failed to get mgmt host for {mb}, will still attempt collection")
        except ValueError:
            print(f"Failed to get mgmt host for {mb}, will still attempt collection")
        converter.add_host(url, mb, ip=ip)


    total_runtime_seconds = args.runtime * 60 if args.runtime > 0 else 0.1
    start_time = time.time()
    while time.time() - start_time < total_runtime_seconds:
        iter_start = time.time()
        converter.scrape_and_insert()
        iter_time = time.time() - iter_start
        print(f"Scrape and insert took {iter_time:.2f}")
        if args.interval - iter_time > 0:
            time.sleep(args.interval - iter_time)


if __name__ == "__main__":
    args = parse_args()
    main(args)
