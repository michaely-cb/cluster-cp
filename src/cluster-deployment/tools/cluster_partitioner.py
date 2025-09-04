#!/usr/bin/env python3

import argparse
import csv
import dataclasses
import datetime
import ipaddress
import json
import logging
import pathlib
import shlex
import subprocess
import sys
import tempfile
import textwrap
from collections import defaultdict
from io import StringIO
from typing import Tuple, List, Optional, Set

import tabulate
import yaml

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@dataclasses.dataclass
class NodeInterface:
    role: str
    sw: str
    sw_if: str

@dataclasses.dataclass
class DeviceMapping:
    name: str
    mgmt_ip: str
    mgmt_subnet: Optional[str] = None
    mgmt_vlan: Optional[int] = None


@dataclasses.dataclass
class EnvSettings:
    name: str
    data_vrf_name: str

    data_network_vlan_start: int = 0
    # each subnet in the src cluster is shifted by some number of addresses to become the next partition
    data_network_shift_addrs: int = 0

    fe_network_file: str = ""
    mg_links_file: str = "" # src_name,src_if,dst_name,dst_if,speed

    device_mappings: list = dataclasses.field(default_factory=list)

    def add_mapping(self, m: DeviceMapping):
        for e in self.device_mappings:
            if e.name == m.name:
                raise ValueError(f"device {m.name} is already present")
        self.device_mappings.append(m)

    def get_mapping(self, name: str) -> Optional[DeviceMapping]:
        for e in self.device_mappings:
            if e.name == name:
                return e
        return None

    @classmethod
    def load_yaml(cls, doc: dict) -> 'EnvSettings':
        return cls(
            name=doc["name"],
            data_vrf_name=doc["data_vrf_name"],
            data_network_vlan_start=doc.get("data_network_vlan_start", 0),
            data_network_shift_addrs=doc.get("data_network_shift_addrs", 0),
            fe_network_file=doc.get("fe_network_file", ""),
            mg_links_file=doc.get("mg_links_file", ""),
            device_mappings=[
                DeviceMapping(name=m["name"],
                              mgmt_ip=m["mgmt_ip"],
                              mgmt_subnet=m.get("mgmt_subnet", None),
                              mgmt_vlan=m.get("mgmt_vlan", None))
                for m in doc.get("device_mappings", [])
            ]
        )



def _exec_json(cmd: str) -> List[dict]:
    try:
        out = subprocess.check_output(shlex.split(cmd), text=True)
        return json.loads(out)
    except Exception as e:
        print(f"failed to exec '{cmd}': {e}")
        sys.exit(1)

def _exec(cmd: str) -> str:
    try:
        return subprocess.check_output(shlex.split(cmd), text=True)
    except Exception as e:
        print(f"failed to exec '{cmd}': {e}")
        sys.exit(1)


@dataclasses.dataclass
class SwitchPortVlan:
    switch: str
    switch_interface: str
    node: str
    vlan_src: int
    vlan_dst: int


def _discover_switch_port_vlan(src: EnvSettings, targets: Set[str]) -> List[SwitchPortVlan]:
    """ Device to [(switch,sw_if,node_if,vlan), ...] that need to be moved into the target vlan """
    rv = []
    try:
        with open(src.fe_network_file, 'r') as f:
            doc = json.loads(f.read())
        sw_eth_po = {}
        for sw in doc.get("switches", []):
            for inf in sw.get("interfaces", []):
                if 'portchannel' in inf:
                    sw_eth_po[f"{sw['name']}:{inf['name']}"] = inf['portchannel']
        node_po = {}
        for link in doc.get("links", []):
            if link["dst_name"] not in targets:
                continue
            k = f"{link['src_name']}:{link['src_if']}"
            if k in sw_eth_po:
                rv.append(
                    SwitchPortVlan(
                        switch=link['src_name'],
                        switch_interface=f"Portchannel{sw_eth_po[k]}",
                        node=link['dst_name'],
                        vlan_src=506, # TODO: this should come from the fe network doc
                        vlan_dst=508,
                    )
                )
            else:
                logger.warning(f"link {link} not associated with a port channel")
        for n, sw_pc in node_po.items():
            rv[n] = sw_pc
    except Exception as e:
        logger.error(f"unable to open frontend network file '{src.fe_network_file}', skipping: {e.__class__.__name__}: {e}")

    # all the mgmt switch connections will toggle between vlan 4000 (src) <> 4001 (dst)
    try:
        with open(src.mg_links_file, 'r') as f:
            doc = f.read()
        reader = csv.DictReader(StringIO(doc))
        for d in reader:
            node = d["dst_name"]
            if node not in targets:
                continue
            vlan_src, vlan_dst = 4000, 4001
            if node[0:6] in ("inf001", "inf002"):  # FE IPMI network
                vlan_src, vlan_dst = 507, 509  # TODO: this is hardcoded - we don't represent the IT vlans in cscfg so...
            rv.append(
                SwitchPortVlan(
                    switch=d["src_name"],
                    switch_interface=d["src_if"],
                    node=node,
                    vlan_src=vlan_src,
                    vlan_dst=vlan_dst,
                )
            )
    except Exception as e:
        logger.error(f"unable to open links file '{src.mg_links_file}', skipping: {e.__class__.__name__}: {e}")
    return rv


def shift_addr_net(network_str: str, shift_addrs: int) -> Tuple[str, str]:
    addr = ipaddress.IPv4Address(network_str.split("/")[0])
    net = ipaddress.IPv4Network(network_str, strict=False)
    new_addr = addr + shift_addrs
    new_net_addr = net.network_address + shift_addrs
    return f"{new_net_addr}/{net.prefixlen}", f"{new_addr}/{net.prefixlen}"

def show_ax_port_affinity(s: EnvSettings):
    """ print port affinities for AX nodes """
    with open(f"/opt/cerebras/cluster-deployment/meta/{s.name}/network_config.json", 'r') as f:
        doc = json.load(f)
    sw_cs_port = defaultdict(set)
    for conn in doc["system_connections"]:
        sw, csport = conn["switch_name"], conn["system_port"].split(" ")[1]
        sw_cs_port[sw].add(csport)
    sw2node = defaultdict(list)
    for ax in doc["activation_nodes"]:
        sw = set(i["switch_name"] for i in ax["interfaces"])
        if len(sw) != 1:
            logger.error(f"node {ax['name']} does not have 1 switch, interfaces: {ax['interfaces']}")
            continue
        sw2node[sw.pop()].append(ax["name"])

    sw = sorted(list(sw_cs_port.keys()))
    header = ["switch", "cs_ports", "ax_count", "ax_nodes"]
    rows = []

    for name in sw:
        if len(sw_cs_port[name]) == 0:
            continue
        row = [
            name,
            ",".join([str(p) for p in sorted(list(sw_cs_port[name]))]),
            str(len(sw2node.get(name))),
            ",".join(sorted(sw2node.get(name)))
        ]
        rows.append(row)
    print(tabulate.tabulate(rows, headers=header))


def migrate_network_config(doc: dict, s: EnvSettings) -> dict:
    doc = doc.copy()

    def _clear_settings():
        for k, v in list(doc.items()):
            if k.endswith("_nodes") or k in ("systems", "system_connections"):
                doc[k] = []
            elif k in ("cluster_data_vip",):
                del doc[k]
            elif k == "switches":
                for sw in v:
                    sw["ports"] = []
                    if "virtual_addrs" in sw:
                        del sw["virtual_addrs"]

    def _shift_vlans():
        for sw in doc["switches"]:
            for vlan_id in ("swarmx_vlan", "vlan", "system_vlan"):
                if vlan_id in sw:
                    sw[vlan_id] = sw[vlan_id] + s.data_network_vlan_start
            for addr in ("system_prefix", "system_vlan_address",
                         "prefix", "address",
                         "swarmx_prefix", "swarmx_vlan_address"):
                if addr in sw:
                    _, new_addr = shift_addr_net(sw[addr], s.data_network_shift_addrs)
                    sw[addr] = new_addr

        def _shift_subnets(obj):
            new_subnets = []
            for subnet in obj["subnets"]:
                new_nw, _ = shift_addr_net(subnet, s.data_network_shift_addrs)
                new_subnets.append(new_nw)
            obj["subnets"] = new_subnets

        for subnet_pool in doc["reserved_subnet_pools"]:
            _shift_subnets(subnet_pool)
        for subnet_pool in doc["environment"]["subnet_pools"]:
            _shift_subnets(subnet_pool)

    def _shift_xconnect():
        update = []
        for xc in doc["xconnect"]["connections"]:
            name, port = xc["links"][0]["name"], xc["links"][0]["port"]
            if "lf" in name:
                name, port = xc["links"][1]["name"], xc["links"][1]["port"]
            port = int(port.split("/")[1])
            if port % 2 == 1:
                _, addr = shift_addr_net(xc["prefix"], s.data_network_shift_addrs)
                xc["prefix"] = addr
                update.append(xc)
        doc["xconnect"]["connections"] = update
        doc["xconnect"]["starting_prefix"] = shift_addr_net(
            doc["xconnect"]["starting_prefix"], s.data_network_shift_addrs
        )[0]

    def _set_env():
        doc["name"] = s.name
        doc["environment"]["vrf_name"] = s.data_vrf_name
        new_prefix, _ = shift_addr_net(doc["environment"]["cluster_prefix"], s.data_network_shift_addrs)
        doc["environment"]["cluster_prefix"] = new_prefix

        for tier in doc["tiers"]:
            tier["starting_prefix"] = shift_addr_net(tier["starting_prefix"], s.data_network_shift_addrs)[0]
            tier["system_starting_prefix"] = shift_addr_net(tier["system_starting_prefix"], s.data_network_shift_addrs)[0]

        doc["vips"]["prefix"] = shift_addr_net(doc["vips"]["prefix"], s.data_network_shift_addrs)[0]

    _clear_settings()
    _shift_vlans()
    _shift_xconnect()
    _set_env()

    return doc


def create_mgmt_switch_dump(settings: EnvSettings, filter: Optional[List[str]]) -> List[dict]:
    args = ""
    if filter:
        args = " ".join(filter)
    devices = _exec_json(f"cscfg device show -f role=~'MG|MA|FE' type=SW {args} -ojson")
    result = []
    for dev in devices:
        name = dev["name"]
        mapping = settings.get_mapping(name)
        if not mapping or not mapping.mgmt_ip or not mapping.mgmt_subnet:
            logger.warning(f"switch {name} not found in mappings, skip adding to output")
            continue
        if "subnet_info" in dev["properties"]:
            subnet = ipaddress.ip_network(mapping.mgmt_subnet, strict=False)
            dev["properties"]["subnet_info"]["subnet"] = str(subnet)
            dev["properties"]["subnet_info"]["gateway"] = str(subnet.broadcast_address - 1)
        dev["properties"]["management_info"]["ip"] = mapping.mgmt_ip
        result.append(dev)
    return result


def create_data_switch_dump(settings: EnvSettings, filter: Optional[List[str]]) -> List[dict]:
    args = ""
    if filter:
        args = " ".join(filter)
    devices = _exec_json(f"cscfg device show -f role=~'LF|SP' type=SW {args} -ojson")
    for dev in devices:
        name = dev["name"]
        mapping = settings.get_mapping(name)
        if not mapping:
            logger.warning(f"switch {name} not found in mappings, skip adding to output")
            continue

        dev["properties"]["management_info"]["ip"] = mapping.mgmt_ip
    return devices


def find_mgmt_node() -> str:
    cluster_devices = sorted(
        [
            d["name"] for d in _exec_json("cscfg cluster device show -ojson")
            if d.get("controlplane", False)
        ]
    )
    if not cluster_devices:
        raise ValueError("no management node found in the cluster, try running 'cscfg cluster device show' to see the devices")
    return cluster_devices[0]


class MigrationManager:
    def __init__(self, src: EnvSettings, dst: EnvSettings, db_path: str):
        self.src = src
        self.dst = dst
        self.db_path = pathlib.Path(db_path)
        # a simple json db to store the migration state
        if not self.db_path.is_file():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.db_path, 'w') as f:
                json.dump({
                    "exports": {},
                    "imports": {},
                    "devices": {},
                    "vlan_map": [],
                }, f, indent=2)
        self.db = json.loads(self.db_path.read_text())

    def save(self):
        doc = json.dumps(self.db, indent=2)
        with open(self.db_path, 'w') as f:
            f.write(doc)

    def _refresh_vlan_map(self, devices: List[str]):
        """ Update the vlan mapping for the devices in the given list of names. Overwrites based on the
        switch:switch interface
        """
        updated_vlan_map = _discover_switch_port_vlan(self.src, set(devices))
        current_vlan_map = {f"{v['switch']}:{v['switch_interface']}": v for v in self.db["vlan_map"]}
        for v in updated_vlan_map:
            key = f"{v.switch}:{v.switch_interface}"
            current_vlan_map[key] = v.__dict__
        self.db["vlan_map"] = list(current_vlan_map.values())


    def export_create(self, name: str, filter: List[str]) -> dict:
        if self.db["exports"].get(name):
            raise ValueError(f"export with name '{name}' already exists")

        devices = _exec_json(f"cscfg device show -f type=~'SR|SY' {' '.join(filter)} -ojson")
        names = []
        for device in devices:
            names.append(device["name"])
            self.db["devices"][device["name"]] = device
        self._refresh_vlan_map(names)

        now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        self.db["exports"][name] = {
            "name": name,
            "devices": names,
            "create_time": now,
            "update_time": now,
            "stage": "not_started",
        }
        return self.db["exports"][name]

    def export_update(self, name: str, mode: str, filter: List[str]) -> dict:
        if not self.db["exports"].get(name):
            raise ValueError(f"export with name '{name}' does not exist")

        devices = _exec_json(f"cscfg device show -f type=~'SR|SY' {' '.join(filter)} -ojson")
        current_names = set(self.db["exports"][name]["devices"])
        if mode == "append":
            names = set()
            for device in devices:
                names.add(device["name"])
                self.db["devices"][device["name"]] = device
            current_names.update(names)
            self.db["exports"][name]["devices"] = sorted(list(current_names))
            self._refresh_vlan_map(list(names))
        elif mode == "remove":
            names = set()
            for device in devices:
                names.add(device["name"])
            current_names.difference_update(names)
            self.db["exports"][name]["devices"] = sorted(list(current_names))

        self.db["exports"][name]["update_time"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        return self.db["exports"][name]

    @staticmethod
    def _print_import_export_list(doc: List[dict]):
        # tabulate the exports, sorting by create_time
        exports = sorted(doc, key=lambda x: x["create_time"], reverse=True)
        header = ["name", "create_time", "update_time", "stage", "devices", ]
        rows = []
        for e in exports:
            devices_str = ", ".join(e.get("devices", []))
            wrapped_devices = "\n".join(textwrap.wrap(devices_str, width=80, break_long_words=False))
            rows.append([
                e["name"],
                e["create_time"],
                e["update_time"],
                e["stage"],
                wrapped_devices,
            ])
        print(tabulate.tabulate(rows, headers=header))

    def export_list_print(self):
        self._print_import_export_list(self.db["exports"].values())

    def export_run_remove(self, name: str, dry_run: bool = False) -> int:
        export = self.db["exports"].get(name)
        if not export:
            raise ValueError(f"export with name '{name}' does not exist")

        mgmt_node = find_mgmt_node()

        commands = []
        for name in export.get("devices", []):
            commands.append((f"cscfg device remove {name}", False,))

        commands.append((f"cscfg k8s update_network_config -y", False))
        commands.append((f"ssh {mgmt_node} csadm.sh update-cluster-config", False))

        for name in export.get("devices", []):
            device = self.db["devices"][name]
            if device["type"] == "SR":
                commands.append((f"ssh {name} kubeadm reset -f", True))
                commands.append((f"ssh {name} reboot", True))
                commands.append((f"ssh {mgmt_node} kubectl delete node {name}", False))

        commands.append(("cscfg dhcp update -y", False))

        logger.info("going to run:")
        for c, _ in commands:
            print(c)
        if dry_run:
            return 0

        for c, failure_ok in commands:
            try:
                logger.info(f"exec: {c}")
                out = subprocess.check_output(shlex.split(c), text=True, env={"DM_VERBOSITY": "4"})
                logger.info(f"returned:\n{out}")
            except subprocess.SubprocessError as e:
                if failure_ok:
                    logger.warning(f"returned err:\n{e.__class__.__name__}: {e}")
                else:
                    logger.error(f"returned err:\n{e.__class__.__name__}: {e}")
                    return 1
        # update the export stage
        export["stage"] = "01_remove"
        export["update_time"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        self.save()
        return 0


    def export_run_update_vrf(self, name: str, dry_run: bool = False) -> int:
        export = self.db["exports"].get(name)
        if not export:
            raise ValueError(f"export with name '{name}' does not exist")
        include_devices = set(export["devices"])

        sw_cmds = defaultdict(list)
        for vlan_info in self.db["vlan_map"]:
            vlan_info = SwitchPortVlan(**vlan_info)
            if vlan_info.node not in include_devices:
                continue
            interface_name = vlan_info.switch_interface.replace("Portchannel", "Port-Chan ")
            sw_cmds[vlan_info.switch].extend([
                f"! {vlan_info.node}",
                f"interface {interface_name}",
                f"  switchport access vlan {vlan_info.vlan_dst}",
                f"  shutdown",
                f"  run bash sleep 5",
                f"  no shutdown",
            ])

        # create a temporary directory for the commands with tempdir
        output_dir = pathlib.Path(tempfile.mkdtemp(prefix="switch_cmds_"))
        for sw, cmds in sw_cmds.items():
            cmds = ["en", "config"] + cmds + ["exit", "exit", "write memory", ""]
            with open(f"{output_dir}/{sw}.cmds", 'w') as f:
                f.write("\n".join(cmds))
                if dry_run:
                    print(f"{sw}")
                    print("\n".join(cmds))
                    print("")

        if dry_run:
            logger.info(f"saved commands to {output_dir.absolute()}")
            return 0

        for sw in sw_cmds:
            cmd = f"cscfg dev exec -f name={sw} --file {output_dir.absolute()}/{sw}.cmds -y"
            try:
                logger.info(f"exec: {cmd}")
                out = subprocess.check_output(shlex.split(cmd), text=True, env={"DM_VERBOSITY": "4"})
                logger.info(f"output:\n{out}")
            except subprocess.SubprocessError as e:
                logger.error(e)
                return 1
        # update the export stage
        export["stage"] = "02_update_vrf"
        export["update_time"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        self.save()
        return 0

    def export_show(self, name: str) -> List[dict]:
        export = self.db["exports"].get(name)
        if not export:
            raise ValueError(f"export with name '{name}' does not exist")

        names = export.get("devices", [])
        devices = []
        for name in names:
            if name not in self.db["devices"]:
                logger.warning(f"device {name} not found in db, skipping")
                continue
            d = self.db["devices"][name]
            if d.get("properties", {}).get("management_info", {}).get("ip"):
                del d["properties"]["management_info"]["ip"]
            if d.get("properties", {}).get("ipmi_info", {}).get("ip"):
                del d["properties"]["ipmi_info"]["ip"]
            devices.append(d)
        return devices

    def import_create(self, name: str, devices: List[dict]):
        if self.db["imports"].get(name):
            raise ValueError(f"import with name '{name}' already exists")

        if not devices:
            raise ValueError("no devices to import")

        now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        self.db["imports"][name] = {
            "name": name,
            "devices": [d["name"] for d in devices],
            "create_time": now,
            "update_time": now,
            "stage": "not_started",
        }

        for d in devices:
            # keep the old entry since we'd like to preserve the IP if possible
            if not d["name"] in self.db:
                self.db["devices"][d["name"]] = d
        self.save()

    def import_list_print(self):
        self._print_import_export_list(self.db["imports"].values())

    def import_run_add(self, name: str, dry_run: bool = False) -> int:
        import_data = self.db["imports"].get(name)
        if not import_data:
            raise ValueError(f"import with name '{name}' does not exist")

        # add a deploy.tag property to the device as import-<name>, then write the devices to a file and load
        # into cscfg. Then set their deploy state to NETWORK_GENERATE NOT_STARTED
        deploy_tag = f"import-{name}"
        importable = []
        for device_name in import_data["devices"]:
            device = self.db["devices"].get(device_name)
            if not device:
                logger.warning(f"device {device_name} not found in db, skipping")
                continue
            if "properties" not in device:
                device["properties"] = {}
            if "deploy" not in device["properties"]:
                device["properties"]["deploy"] = {}
            device["properties"]["deploy"]["tag"] = deploy_tag
            importable.append(device)

        # write the devices to a file
        devices_file = pathlib.Path(tempfile.mkdtemp(prefix="import_devices_")) / "devices.json"
        with open(devices_file, 'w') as f:
            json.dump(importable, f, indent=2)

        commands = [f"cscfg device update {devices_file}",
                    f"cscfg deploy update -f deploy.tag={deploy_tag} --stage NETWORK_GENERATE --status NOT_STARTED -y"]
        if dry_run:
            logger.info("going to run:")
            for c in commands:
                print(c)
            return 0
        for c in commands:
            try:
                logger.info(f"exec: {c}")
                out = subprocess.check_output(shlex.split(c), text=True, env={"DM_VERBOSITY": "4"})
                logger.info(f"output:\n{out}")
            except subprocess.SubprocessError as e:
                logger.error(f"failed to exec '{c}': {e.__class__.__name__}: {e}")
                return 1
        # update the import stage
        import_data["stage"] = "01_add"
        import_data["update_time"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        self.save()
        return 0

    def import_run_update_vrf(self, name: str, dry_run: bool = False) -> int:
        batch = self.db["imports"].get(name)
        if not batch:
            raise ValueError(f"import with name '{name}' does not exist")
        include_names = set(batch["devices"])

        sw_cmds = defaultdict(list)
        for vlan_info in self.db["vlan_map"]:
            vlan_info = SwitchPortVlan(**vlan_info)
            if vlan_info.node not in include_names:
                continue
            interface_name = vlan_info.switch_interface.replace("Portchannel", "Port-Chan ")
            sw_cmds[vlan_info.switch].extend([
                f"! {vlan_info.node}",
                f"interface {interface_name}",
                f"  switchport access vlan {vlan_info.vlan_src}",
                f"  shutdown",
                f"  run bash sleep 5",
                f"  no shutdown",
            ])

        # create a temporary directory for the commands with tempdir
        output_dir = pathlib.Path(tempfile.mkdtemp(prefix="switch_cmds_"))
        for sw, cmds in sw_cmds.items():
            cmds = ["en", "config"] + cmds + ["exit", "exit", "write memory", ""]
            with open(f"{output_dir}/{sw}.cmds", 'w') as f:
                f.write("\n".join(cmds))
                if dry_run:
                    print(f"{sw}")
                    print("\n".join(cmds))
                    print("")

        if dry_run:
            logger.info(f"saved commands to {output_dir.absolute()}")
            return 0

        for sw in sw_cmds:
            cmd = f"cscfg dev exec -f name={sw} --file {output_dir.absolute()}/{sw}.cmds -y"
            try:
                logger.info(f"exec: {cmd}")
                out = subprocess.check_output(shlex.split(cmd), text=True, env={"DM_VERBOSITY": "4"})
                logger.info(f"output:\n{out}")
            except subprocess.SubprocessError as e:
                logger.error(e)
                return 1
        # update the export stage
        batch["stage"] = "02_update_vrf"
        batch["update_time"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        self.save()
        return 0


def main():
    parser = argparse.ArgumentParser(description="Cluster Partitioner Tool")
    parser.add_argument("--config", "-c", help="src/dst config settings file", default="config.yaml")
    parser.add_argument("--db", help="migration state db json file", default="migration_state.json")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "create_initial_network_config",
              help=str(
                  "Create a network config with the new cluster settings. This attempts to migrate most but not all "
                  "of the fields affected by VRF partitioning. A few fields (clusters, mgmt_vip) will need to be hand "
                  "modified."
              )
    )

    p = subparsers.add_parser(
        "create_data_switch_dump",
        help=str(
            "Create inventory transfer file (device JSON) for data switches. This will override some fields based on "
            "the given mapping file"
        )
    )
    p.add_argument("--filter", "-f", help="cscfg device filter", nargs="*")

    p = subparsers.add_parser(
        "create_mgmt_switch_dump",
        help=str(
            "Create inventory transfer file (device JSON) for management switches. This will override some fields based on "
            "the given mapping file"
        )
    )
    p.add_argument("--filter", "-f", help="cscfg device filter", nargs="*")

    p = subparsers.add_parser(
        "show_ax_port_affinity",
        help=str(
            "Show a list of switch/csport(s)/ax-servers. Useful for deciding the set of AX nodes which should be transferred while "
            "retaining some balance in the ports"
        )
    )

    # Export command
    export_parser = subparsers.add_parser("export", help="Manage export batches")
    export_subparsers = export_parser.add_subparsers(dest="action", required=True, help="action")

    p = export_subparsers.add_parser(
        "create",
        help=str("Create a new export batch with the given name and devices selected by the filter")
    )
    p.add_argument("NAME", help="batch name")
    p.add_argument("--filter", "-f", help="cscfg device filter", nargs="+")

    p = export_subparsers.add_parser(
        "update",
        help=str("Update an existing export batch with the given name. The filter is used to select devices to append or remove from the batch")
    )
    p.add_argument("NAME", help="batch name")
    p.add_argument("--filter", "-f", help="cscfg device filter", nargs="+")
    p.add_argument("--mode", "-m", help="append or remove", choices=("append", "remove"), default="append")

    p = export_subparsers.add_parser("list", help="list the export names and devices")

    p = export_subparsers.add_parser("run", help="run an export deployment stage")
    p.add_argument("NAME", help="batch name")
    p.add_argument("--stage", "-s", help="stage to run", choices=("01_remove", "02_update_vrf"), required=True)
    p.add_argument("--dry-run", action="store_true", help="Print the commands but do not execute them")

    p = export_subparsers.add_parser("show", help="show export devices for the purpose of copying them to the dst machine")
    p.add_argument("NAME", help="batch name")

    # Import command
    import_parser = subparsers.add_parser("import", help="Manage import batches")
    import_subparsers = import_parser.add_subparsers(dest="action", required=True, help="action")

    p = import_subparsers.add_parser("create", help="Create a new import from the given export json file")
    p.add_argument("NAME", help="batch name")
    p.add_argument("FILE", help="export json file to import from")

    p = import_subparsers.add_parser("list", help="Print the imports")

    p = import_subparsers.add_parser("run", help="Run an import deployment stage")
    p.add_argument("NAME", help="batch name")
    p.add_argument("--stage", "-s", help="stage to run", choices=("01_add","02_update_vrf"), required=True)
    p.add_argument("--dry-run", action="store_true", help="Print the commands but do not execute them")

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    config_path = pathlib.Path(args.config)
    if not config_path.is_file():
        print(f"config file '{args.config}' does not exist")
        sys.exit(1)
    doc = yaml.safe_load(config_path.read_text())
    src_settings = EnvSettings.load_yaml(doc["src"])
    dst_settings = EnvSettings.load_yaml(doc["dst"])

    rv = 0

    if args.command == "create_initial_network_config":
        with open(f"/opt/cerebras/cluster-deployment/meta/{src_settings.name}/network_config.json", 'r') as f:
            doc = json.loads(f.read())
        new_doc = migrate_network_config(doc, dst_settings)
        print(json.dumps(new_doc, indent=2))
        logger.warning("there are still some fields that MUST be updated manually like `clusters` and `root_server`!")

    elif args.command == "create_mgmt_switch_dump":
        doc = create_mgmt_switch_dump(dst_settings, args.filter)
        print(json.dumps(doc, indent=2))

    elif args.command == "create_data_switch_dump":
        doc = create_data_switch_dump(dst_settings, args.filter)
        print(json.dumps(doc, indent=2))

    elif args.command == "show_ax_port_affinity":
        show_ax_port_affinity(src_settings)

    elif args.command == "export":
        mm = MigrationManager(src_settings, dst_settings, args.db)
        try:
            if args.action == "create":
                doc = mm.export_create(args.NAME, args.filter)
                mm.save()
                print(json.dumps(doc, indent=2))
            elif args.action == "update":
                doc = mm.export_update(args.NAME, args.mode, args.filter)
                mm.save()
                print(json.dumps(doc, indent=2))
            elif args.action == "list":
                mm.export_list_print()
            elif args.action == "run":
                if args.stage == "01_remove":
                    rv = mm.export_run_remove(args.NAME, args.dry_run)
                elif args.stage == "02_update_vrf":
                    rv = mm.export_run_update_vrf(args.NAME, args.dry_run)
                else:
                    raise ValueError(f"unknown stage '{args.stage}'")
            elif args.action == "show":
                doc = mm.export_show(args.NAME)
                print(json.dumps(doc, indent=2))
            else:
                # This case should not be reachable due to argparse 'required=True'
                # but as a safeguard:
                export_parser.print_help()
                sys.exit(1)
        except ValueError as e:
            logger.error(e)
            sys.exit(1)

    elif args.command == "import":
        mm = MigrationManager(src_settings, dst_settings, args.db)
        try:
            if args.action == "create":
                devices = json.loads(pathlib.Path(args.FILE).read_text())
                mm.import_create(args.NAME, devices)
                mm.import_list_print()
                mm.save()
            elif args.action == "list":
                mm.import_list_print()
            elif args.action == "run":
                if args.stage == "01_add":
                    rv = mm.import_run_add(args.NAME, args.dry_run)
                elif args.stage == "02_update_vrf":
                    rv = mm.import_run_update_vrf(args.NAME, args.dry_run)
                else:
                    raise ValueError(f"unknown stage '{args.stage}'")
            else:
                # This case should not be reachable due to argparse 'required=True'
                # but as a safeguard:
                import_parser.print_help()
                sys.exit(1)
        except ValueError as e:
            logger.error(e)
            sys.exit(1)

    else:
        parser.print_help()
        sys.exit(1)

    sys.exit(rv)


if __name__ == "__main__":
    main()
