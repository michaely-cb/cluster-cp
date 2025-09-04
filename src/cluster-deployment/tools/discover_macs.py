#!/usr/bin/env python3
import argparse
import csv
import json
import logging
import pathlib
import re
import subprocess
import sys
from collections import defaultdict
from typing import List, Tuple, Dict, Optional

# configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)


def format_mac(mac: str) -> Optional[str]:
    """Format MAC address to standard format."""
    mac = mac.lower().replace("-", ":").replace(".", ":")
    if len(mac) == 12:
        mac = ":".join(mac[i:i + 2] for i in range(0, 12, 2))
    return mac if re.match(r"^([0-9a-f]{2}:){5}[0-9a-f]{2}$", mac) else None


def run_cmd(cmd, check: bool = True) -> str:
    logger.debug(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if check and result.returncode != 0:
        logger.error(f"Command failed: {' '.join(cmd)} ; stderr: {result.stderr.strip()}")
        sys.exit(1)
    return result.stdout


def get_switch_names(pattern: str) -> List[str]:
    logger.debug(f"Querying switches matching: {pattern}")
    out = run_cmd([
        "cscfg", "device", "show",
        "-f", f"name=~{pattern}", "type=SW", "role=MG", "-ojson"
    ])
    switches = [d["name"] for d in json.loads(out)]
    logger.debug(f"Found switches: {switches}")
    return switches


def scrape_mac_tables(switches: List[str]) -> Dict[str, Dict[str, str]]:
    """ Returns a dictionary of switch names to their MAC address tables."""
    switches_re = "(^" + "$)|(^".join(switches) + "$)"
    logger.debug(f"Scraping MAC table for switches: {switches_re}")
    for name in switches:
        tmp_file = pathlib.Path(f"/tmp/{name}")
        if tmp_file.is_file():
            logger.debug(f"Temporary file already exists for {name}, removing it")
            tmp_file.unlink()
    run_cmd([
        "cscfg", "dev", "exec", "-y", "--timeout", "12",
        "-f", f"name=~{switches_re}", "vendor.name=AR",  # Arista format only
        "--output_dir", "/tmp/",
        "--", "show mac address-table | json",
    ], check=False)
    sw_macs = {name: {} for name in switches}
    for name in switches:
        tmp_file = pathlib.Path(f"/tmp/{name}")
        if not tmp_file.is_file():
            logger.warning(f"MAC table file not found for {name}: {tmp_file}")
            continue
        data = json.loads(tmp_file.read_text())
        port_mac = {}
        for e in data.get("unicastTable", {}).get("tableEntries", []):
            port = e.get("interface")
            if port in port_mac:
                logger.warning(f"Multiple MACs for {name} port {port}, ignoring port")
                port_mac[port] = None
                continue
            elif e.get("macAddress"):
                port_mac[port] = format_mac(e.get("macAddress"))
        logger.debug(f"Retrieved {len(port_mac)} MAC entries for {name}")
        sw_macs[name] = port_mac
    logger.debug(json.dumps(sw_macs, indent=2))
    return sw_macs


def scrape_interface_statuses(switches: List[str]) -> Dict[str, Dict[str, str]]:
    """ Returns a dictionary of switch names -> port -> up=true."""
    switches_re = "(^" + "$)|(^".join(switches) + "$)"
    logger.debug(f"Scraping interface status for switches: {switches_re}")
    for name in switches:
        tmp_file = pathlib.Path(f"/tmp/{name}")
        if tmp_file.is_file():
            logger.debug(f"Temporary file already exists for {name}, removing it")
            tmp_file.unlink()
    run_cmd([
        "cscfg", "dev", "exec", "-y", "--timeout", "12",
        "-f", f"name=~{switches_re}", "vendor.name=AR",  # Arista format only
        "--output_dir", "/tmp/",
        "--", "show interface status | json | no-more",
    ], check=False)
    sw_status = {name: {} for name in switches}
    for name in switches:
        tmp_file = pathlib.Path(f"/tmp/{name}")
        if not tmp_file.is_file():
            logger.warning(f"Interface status table file not found for {name}: {tmp_file}")
            continue
        data = json.loads(tmp_file.read_text())
        port_up = {}
        for port, details in data.get("interfaceStatuses", {}).items():
            port_up[port] = details.get("linkStatus", "NOTSET")
        sw_status[name] = port_up
    logger.debug(json.dumps(sw_status, indent=2))
    return sw_status


def get_cscfg_device_macs() -> Dict[str, str]:
    """ Returns a dictionary of device names to their MAC addresses from cscfg."""
    devices = json.loads(run_cmd(["cscfg", "device", "show", "-ojson"]))
    dev_mac = {}
    for dev in devices:
        name = dev["name"]
        props = dev.get("properties", {}).get("management_info", {})
        dev_mac[name] = props.get("mac")
    return dev_mac


def apply_updates(updates: List[Tuple[str, str, str]]):
    for name, old, new in updates:
        print(f"Updating {name}: {old} → {new}")
        run_cmd([
            "cscfg", "device", "edit", "-y",
            "-f", f"name={name}",
            "-p", f"management_info.mac={new}"
        ])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("links", help="Links file for management network")
    p.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    args = p.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    expected_sw_port_dev = defaultdict(dict)

    # read csv file with format: src_name,src_if,dst_name,dst_if,speed using csv reader
    with open(args.links, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            src_name = row["src_name"].strip()
            src_if = row["src_if"].strip()
            dst_name = row["dst_name"].strip()
            if not src_name or not src_if or not dst_name:
                continue
            expected_sw_port_dev[src_name][src_if] = dst_name

    mac_table = scrape_mac_tables(list(expected_sw_port_dev.keys()))
    port_status = scrape_interface_statuses(list(expected_sw_port_dev.keys()))
    cscfg_device_macs = get_cscfg_device_macs()
    matches = []
    notfound = []
    changes = []
    additions = []
    for sw_name, ports in expected_sw_port_dev.items():
        if sw_name not in mac_table:
            continue
        for port, expected_dev in ports.items():
            if expected_dev not in cscfg_device_macs:
                logger.warning(f"Expected device {expected_dev} not in cscfg")
                continue

            if port not in mac_table[sw_name]:
                notfound.append((expected_dev, sw_name, port))
                continue

            actual_mac = mac_table[sw_name][port]
            if actual_mac is None:
                logger.error(f"Port {port} on switch {sw_name} has multiple MACs, expected={expected_dev}")
                continue

            cscfg_mac = cscfg_device_macs.get(expected_dev)
            if not cscfg_mac:
                additions.append((expected_dev, None, actual_mac))
            elif actual_mac != cscfg_mac:
                changes.append((expected_dev, cscfg_mac, actual_mac))
            else:
                matches.append((expected_dev, cscfg_mac, actual_mac))
    logger.info(f"Found {len(matches)} matches, {len(changes)} changes, {len(additions)} additions, {len(notfound)} not found")

    # display the results and prompt the user to confirm the updates
    if notfound:
        print("MISSING")
        print("src_name,src_if,dst_name,link_state")
        for dev, sw, port in notfound:
            print(f"{sw},{port},{dev},{port_status.get(sw, {}).get(port, 'NOT_SET')}")
    if changes:
        print("CHANGES:")
        for dev, old_mac, new_mac in changes:
            print(f"{dev}: {old_mac} → {new_mac}")
    if additions:
        print("NEW:")
        for dev, _, new_mac in additions:
            print(f"  {dev}: {new_mac}")

    updates = changes + additions
    if not updates:
        print("No updates.")
        return
    while True:
        resp = input(f"Accept {len(changes)} changes and {len(additions)} new? Choices: [none|changes|new|both] ")
        if resp.lower() == "none":
            print("No updates applied. Here's the commands you could run to do the updates manually:")
            for dev, old_mac, new_mac in changes + additions:
                print(f"  cscfg device edit -y -f name={dev} -p management_info.mac={new_mac}")
            return
        elif resp.lower() == "changes":
            updates = changes
            break
        elif resp.lower() == "new":
            updates = additions
            break
        elif resp.lower() == "both":
            updates = changes + additions
            break
        else:
            print("Invalid choice, please try again.")
    apply_updates(updates)


if __name__ == "__main__":
    main()
