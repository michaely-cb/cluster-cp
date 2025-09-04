#!/usr/bin/env python3

import argparse
import logging
import os
import re
import subprocess
import xml.etree.ElementTree as ET
from enum import Enum
from subprocess import CalledProcessError
from typing import List

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DEFAULT_NETWORK_DIR = "/etc/systemd/network"


class NicInfo:
    def __init__(self, name: str, mac: str, pci_bus: str, speed: int):
        self.name = name
        self.mac = mac
        self.pci_bus = pci_bus
        self.speed = speed  # in Gbps

    def __repr__(self):
        return (f"NicInfo(name={self.name!r}, mac={self.mac!r}, "
                f"pci_bus={self.pci_bus!r}, speed={self.speed!r})")

    def __eq__(self, other):
        if isinstance(other, NicInfo):
            return (self.name == other.name and self.mac == other.mac and
                    self.pci_bus == other.pci_bus and self.speed == other.speed)
        return False


def run_for_output(cmd: str) -> str:
    logger.info(f"running subproccess command '{cmd}'")
    output = subprocess.getoutput(cmd).strip()
    logger.debug(f"command output: {output}")
    return output


def interface_name(con_name):
    return run_for_output(f"nmcli con show \"{con_name}\" | grep interface-name | cut -d\":\" -f2")

def ethtool_max_speed_lookup(if_name: str) -> int:
    """ return GBPS max supported speed mode for this interface """
    try:
        ethtoolout = run_for_output(f"ethtool {if_name}")
    except subprocess.CalledProcessError:
        return 0

    speeds = re.findall(r'(\d+)base', ethtoolout)

    speeds = [int(speed) for speed in speeds]
    if not speeds:
        return 0
    return max(speeds) // 1000  # mbps -> gbps

def detect_connection_name():
    """
    A function to figure out device to connection mapping for each network interface
    """
    output = run_for_output("nmcli con show")
    con_names = []
    for line in output.splitlines():
        line = line.rstrip("\n")
        tokens = line.split()
        if tokens[0] == "NAME":
            assert (tokens[1] == "UUID")
            assert (tokens[2] == "TYPE")
            assert (tokens[3] == "DEVICE")
            uuid_pos = line.find("UUID") - 2
            device_pos = line.find("DEVICE")
            continue
        con_names.append(line[:uuid_pos].rstrip(" "))

    result = {}
    for con in con_names:
        array = result.get(interface_name(con), [])
        array.append(con)
        result[interface_name(con)] = array
    return result


class Mode(Enum):
    FIX_100G = 0  # fix existing 100G nic names
    RENAME_100G = 1  # rename all X00G nic names to ethX00g{index}


def parse_lshw_xml(xml_data: str) -> List[NicInfo]:
    """ Parse / find mellanox NICS from lshw xml output """
    root = ET.fromstring(xml_data)
    nic_info_list = []

    for node in root.findall(".//node[@class='network']"):
        name = node.find("logicalname")
        if name is None:
            logger.debug(f"skipping NIC since the logicalname was not set, node: {node}")
            continue
        name = name.text

        vendor = node.find('vendor')
        if vendor is None or 'Mellanox' not in vendor.text:
            logger.warning(f"skipping NIC {name} since vendor was not Mellanox: {'None' if vendor is None else vendor.text}")
            continue

        serial = node.find('serial')
        mac = serial.text if serial is not None else ""

        businfo = node.find('businfo')
        pci_bus = businfo.text.split('@')[1] if businfo is not None else ""

        # since the card may not be configured when we run this command, we infer the speed from the model
        speed_gbps = -1
        product = node.find('product')
        if product is not None:
            product = product.text
            if 'ConnectX-6 Lx' in product:
                speed_gbps = 25
            elif 'ConnectX-6' in product:
                speed_gbps = 100
            elif "ConnectX-7" in product:
                speed_gbps = 400
        else:
            product = "unknown"

        if speed_gbps == -1:
            speed_gbps = ethtool_max_speed_lookup(name) or 100
            logger.warning(f"inferring speed {speed_gbps} gbps for card {name} since product was not recognized {product}")

        nic_info_list.append(NicInfo(name=name, mac=mac, pci_bus=pci_bus, speed=speed_gbps))
    return nic_info_list


def main(mode: Mode, output_dir: str):
    """
    Fix or rename the 100G network interface names
    """
    output = run_for_output("lshw -quiet -c network -xml")
    nics = parse_lshw_xml(output)
    nics = sorted(nics, key=lambda n: (n.speed, n.pci_bus,))

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    index = 0  # global index, e.g. nics would be named eth100g0, eth400g1 not eth100g0, eth400g0
    for link in nics:
        logger.debug(f"{link}")
        if mode == Mode.FIX_100G:
            # Check if the network interface name is already fixed
            cmd = f"grep -r -i '{link.mac}' /etc/systemd/network/"
            try:
                run_for_output(cmd)
            except CalledProcessError as e:
                # name is already fixed, no need to write out another .link file
                continue

        if mode == Mode.FIX_100G:
            filename = f"30-{link.name}.link"
        else:
            filename = f"30-eth{link.speed}g{index}.link"

        outfile_name = os.path.join(output_dir, filename)
        with open(outfile_name, "w") as outfile:
            outfile.write("[Match]\n")
            outfile.write(f"MACAddress={link.mac}\n")
            outfile.write("[Link]\n")
            if mode == Mode.FIX_100G:
                outfile.write(f"Name={link.name}\n")
            else:
                outfile.write(f"Name=eth{link.speed}g{index}\n")
            index = index + 1

    if mode == Mode.RENAME_100G:
        if output_dir != DEFAULT_NETWORK_DIR:
            logger.info(f"skipping cleanup of nmcli connections because output dir is not {DEFAULT_NETWORK_DIR}")
            return
        dev_to_con = detect_connection_name()
        logger.debug(f"devices to nmcli conn: {dev_to_con}")
        for link in nics:
            for con_name in dev_to_con.get(link.name, []):
                run_for_output(f"nmcli con del \"{con_name}\"")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NIC name configuration tool")
    subparsers = parser.add_argument(dest='mode', choices=("fix_100g", "rename_100g",),
                                     help='Mode of operation')
    parser.add_argument('--output_dir', type=str, default=DEFAULT_NETWORK_DIR,
                        help='Output directory for configuration files')
    parser.add_argument('--verbose', "-v", action="store_true", default=False,
                        help='Verbose logging')

    args = parser.parse_args()
    if args.mode == 'fix_100g':
        mode = Mode.FIX_100G
    elif args.mode == 'rename_100g':
        mode = Mode.RENAME_100G
    else:
        exit(1)

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    main(mode=mode, output_dir=args.output_dir)
