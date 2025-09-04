# !/usr/bin/env python3
import argparse
import bisect
import csv
import ipaddress
import json
import logging
import shlex
import subprocess
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def find_sys_ip_mac(name: str) -> Tuple[str, Optional[str]]:
    try:
        out = subprocess.check_output(
            shlex.split(f"timeout 10 ssh -oStrictHostKeyChecking=no root@{name} cs network show --output-format json"))
        doc = json.loads(out.decode())
        return doc["mgmt"]["ipAddress"].split("/")[0], doc["mgmt"]["macAddress"]
    except subprocess.CalledProcessError as e:
        return f"failed to ssh to system: {e}", None
    except KeyError as e:
        return "bad json doc returned", None


def _csv_prop(d: dict, name: str, default=None) -> str:
    v = d.get(name, default)
    if v is None:
        raise ValueError(f"missing required prop '{name}'")
    return v.lower().strip()


def parse_row(row: dict,
              line_num: int,
              name_mac_ip: dict,
              br_addrs: list,
              subnet_prefix_rack: list,
              alias_mode: bool,
              prefix: str,
              global_props: dict) -> Tuple[list, Optional[str]]:
    """Process a single CSV row and return a tuple:
       ([error messages], command string or None).
    """
    errs_local = []
    sys_name, rack, rack_id, position = row["name"], row["rack"], row["rack_id"], row["position"]

    alias_name = f"{prefix}{rack}-cs-sy0{'1' if position == 'bottom' else '2'}"
    if sys_name not in name_mac_ip:
        ip_or_err, mac = find_sys_ip_mac(sys_name)
        if not mac:
            errs_local.append(f"line {line_num}: {sys_name} not found in leases and {ip_or_err}")
            return errs_local, None
        ip = ip_or_err
    else:
        mac, ip = name_mac_ip[sys_name]

    if not rack:
        errs_local.append(f"line {line_num}: {sys_name}/{ip} not associated with rack")
        return errs_local, None

    addr = ipaddress.ip_address(ip)
    i = bisect.bisect_left(br_addrs, addr)
    # Ensure that we have a valid index and that addr is in the network
    if i >= len(subnet_prefix_rack) or addr not in subnet_prefix_rack[i][0]:
        errs_local.append(f"line {line_num}: {sys_name}/{ip} not associated with a rack prefix")
        return errs_local, None

    args = [
        "cscfg device add",
        alias_name if alias_mode else sys_name,
        f"SY CS -p location.rack={rack} management_info.ip={ip} management_info.mac={mac}",
        "vendor.name=CS management_credentials.user=admin management_credentials.password=admin",
        f"management_info.name={sys_name}" if alias_mode else "",
        f"location.position={rack_id}",
        *[f"{k}={v}" for k, v in global_props.items()]
    ]
    ok_cmd = " ".join(arg for arg in args if arg)
    return errs_local, ok_cmd


def parse_file(csv_filepath: str, alias_mode: bool = False, prefix="", global_props: Optional[dict] = None):
    if global_props is None:
        global_props = {}

    # Retrieve existing leases and systems (unchanged)
    out = subprocess.check_output(shlex.split("cscfg dhcp show_leases -ojson"))
    doc = json.loads(out.decode())
    name_mac_ip = {entry["host"]: (entry["mac"], entry["ip"]) for entry in doc}

    systems = json.loads(subprocess.check_output(shlex.split("cscfg device show -f type=SY -ojson")))
    existing_systems = [s["name"] for s in systems] + [
        s.get("properties", {}).get("management_info", {}).get("name", "")
        for s in systems
    ]
    existing_systems = set(existing_systems)

    out = subprocess.check_output(shlex.split("cscfg device show -f role=MG type=SW -ojson"))
    subnet_prefix_rack = []
    for obj in json.loads(out):
        rack = obj.get("properties", {}).get("location", {}).get("rack", "")
        subnet = obj.get("properties", {}).get("subnet_info", {}).get("subnet", "")
        subnet = ipaddress.ip_network(subnet)
        subnet_prefix_rack.append((subnet, rack))
    subnet_prefix_rack = sorted(subnet_prefix_rack, key=lambda p: p[0])
    br_addrs = [s[0].broadcast_address for s in subnet_prefix_rack]

    errs = []
    ok = []

    # clean the rows. Carry the rack/rack_id from the previous csv row
    rows = []
    rack, rack_id = "unknown", "unknown"
    with open(csv_filepath, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                rack = _csv_prop(row, "rack", default=rack)
                rack_id = _csv_prop(row, "rack_id", default=rack_id)
                position = _csv_prop(row, "position", "bottom")
                sys_name = _csv_prop(row, "system_name")
                if position not in ('top', 'bottom'):
                    raise ValueError("position must be either 'top' or 'bottom'")
            except ValueError as e:
                errs.append(f"line {reader.line_num}: {e}")
                continue

            if sys_name in existing_systems:
                errs.append(f"line {reader.line_num}: {sys_name} already exists in cscfg, skipping")
            else:
                rows.append(({"name": sys_name, "rack": rack, "rack_id": rack_id, "position": position,}, reader.line_num))

    with ThreadPoolExecutor(16) as executor:
        futures = [
            executor.submit(
                parse_row,
                row,
                line_num,
                name_mac_ip,
                br_addrs,
                subnet_prefix_rack,
                alias_mode,
                prefix,
                global_props
            )
            for row, line_num in rows
        ]
        for future in as_completed(futures):
            errs_local, ok_cmd = future.result()
            if errs_local:
                errs.extend(errs_local)
            if ok_cmd:
                ok.append(ok_cmd)

    for err in errs:
        print(err)
    if errs:
        print("")
    for o in ok:
        print(o)


def main():
    parser = argparse.ArgumentParser(
        description="Process a system CSV file into 'cscfg add' commands for adding new systems",
        usage=textwrap.dedent("""
        ./generate_system_add_cmds.py [system.csv]
        
        The system.csv should have the form:
        
            rack,rack_id,system_name
            wse001,A0,xs20001
            
            OR
            
            rack,rack_id,position,system_name
            wse001,A0,top|bottom,xs20001
            
        Where order of the columns does not matter.
        
        Note: the position field should be used when systems are double-stacked (2 per rack). Otherwise the system
        is assumed to be in the bottom position.
        """))
    parser.add_argument("csv", help="Path to system CSV. CSV should include a header.")
    parser.add_argument("--tag", default="", help="Optional property deploy.tag")
    parser.add_argument("--stamp", default="", help="Optional property location.stamp")
    parser.add_argument("--alias_mode", action="store_true", default=False,
                        help="If the system should be called `wse001-cs-sy01` rather than xs20001")
    parser.add_argument("--prefix", default="",
                        help="If the scrape is in alias_mode, add this prefix to the name, e.g. dh1- names become dh1-wse001-cs-sy01")
    args = parser.parse_args()
    global_props = {}
    if args.tag:
        global_props["deploy.tag"] = args.tag
    if args.stamp:
        global_props["location.stamp"] = args.stamp
    parse_file(args.csv, args.alias_mode, args.prefix, global_props)


if __name__ == "__main__":
    main()
