import json
import pathlib
import subprocess
import sys
import argparse

import xml.etree.ElementTree as ET


def run_cmd_output(command):
    print(f"running: {command}")
    return subprocess.getoutput(command)


def run_cmd(command):
    print(f"running: {command}")
    subprocess.check_call(command, shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)


def get_pcie_address(dev_name):
    if "dev" in dev_name:
        cmd = f"cat {dev_name} | head -2 | tail -1 | cut -d= -f2 | cut -d' ' -f1"
        address = subprocess.getoutput(cmd)
        address = address.split(":")
        return ":".join([address[1],address[2]])
    else:
        address = dev_name.split(":")
        return ":".join([address[1],address[2]])



def update_mlnx_fw(bin_dir: pathlib.Path, manifest: dict, dev_name, part_name):
    if part_name not in manifest:
        raise AssertionError(f"Part number {part_name} not found in manifest")

    file_name = manifest[part_name]
    if not file_name:
        print(f"no firmware image available for part {part_name}, skipping FW install")
        return

    fw_path = bin_dir / file_name
    if fw_path.name.endswith(".tar.gz"):
        print(f"extracting {fw_path}")
        run_cmd(f"tar -xzf {fw_path} -C {bin_dir}")
        fw_path = bin_dir / fw_path.name.replace(".tar.gz", ".signed.bin")
    elif fw_path.name.endswith(".zip"):
        print(f"extracting {fw_path}")
        run_cmd(f"unzip -o {fw_path} -d {bin_dir}")
        fw_path = bin_dir / fw_path.name.replace(".zip", "")

    print(f"updating fw for {dev_name}, part number is {part_name}")
    run_cmd(f"flint --yes -d {get_pcie_address(dev_name)} -i {fw_path} burn")
    run_cmd(f"mstfwreset -d {get_pcie_address(dev_name)} -y -l 3 r")


def update_all(bin_dir: pathlib.Path, manifest: dict):
    output = run_cmd_output("/opt/mlnx/mlxup --no --query --query-format=XML")
    root = ET.fromstring(output)
    for child in root:
        update_mlnx_fw(bin_dir, manifest, child.attrib["pciName"], child.attrib["partNumber"])


def main():
    parser = argparse.ArgumentParser(description="Update NIC firmware")
    parser.add_argument(
        "--bin_dir",
        type=str,
        required=True,
        help="Directory containing the firmware binary files and json manifest mapping part numbers to file names relative to the bin_dir",
    )
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    bin_dir = pathlib.Path(args.bin_dir)
    manifest = json.loads((bin_dir / "part_to_fw.json").read_text())
    update_all(bin_dir, manifest)


if __name__ == "__main__":
    main()