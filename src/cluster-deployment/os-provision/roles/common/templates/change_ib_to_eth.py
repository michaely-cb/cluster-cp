#!/usr/bin/python3

import subprocess
import sys

import xml.etree.ElementTree as ET

output = subprocess.getoutput(
    "/opt/mlnx/mlxup --no --query --query-format=XML")
root = ET.fromstring(output)


def run_cmd(command):
    ret = subprocess.call(command,
                          shell=True,
                          stdout=sys.stdout,
                          stderr=subprocess.STDOUT)
    return ret


def change_ibmode(dev_name):
    print(f"change from ib to eth mode for {dev_name}")

    for i in range(1, 3):
        cmd = f"/usr/bin/mlxconfig -y -d {dev_name} q | grep LINK_TYPE_P{str(i)}"
        if run_cmd(cmd) == 0:
            cmd = f"/usr/bin/mlxconfig -y -d {dev_name} set LINK_TYPE_P{str(i)}=2"
            run_cmd(cmd)
        else:
            print(f"{dev_name} P{str(i)} don't support ib mode")


for child in root:
    change_ibmode(child.attrib["pciName"])
