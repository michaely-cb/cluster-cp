#!/usr/bin/env python3

import argparse
import difflib
import ipaddress
import json
import logging
import pathlib
import shlex
import subprocess
import sys
import textwrap
import time
from collections import defaultdict
from ipaddress import ip_address
from typing import List, Union, Tuple

import paramiko
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class LacpGroupConfig(BaseModel):
    vlan: int
    switches: List[str]
    # Arista doesn't let you do mlag with portchannel ids > 2048
    portchannel_start: int = Field(1001, lt=2002)
    mlag_start: int = Field(1001, lt=2002)


class VlanConfig(BaseModel):
    vlan: int
    subnet: str
    gateway: str


class Link(BaseModel):
    src_name: str
    src_if: str
    dst_name: str
    dst_if: str

    def __str__(self):
        return f"{self.src_name}:{self.src_if} -> {self.dst_name}:{self.dst_if}"


class NodeInterface(BaseModel):
    name: str
    type: str  # e.g., "bond", "ethernet"
    address: str = ""


class Node(BaseModel):
    name: str
    interfaces: List[NodeInterface] = []


class SwitchInterface(BaseModel):
    name: str
    vlan: int
    portchannel: int = Field(1, ge=1, lt=2048)
    mlag: int = Field(1, ge=1, lt=2048)


class Switch(BaseModel):
    name: str
    vendor: str
    interfaces: List[SwitchInterface] = []


class NetworkEnvironment(BaseModel):
    lacp_groups: List[LacpGroupConfig]
    vlans: List[VlanConfig] = []
    config_root: str = "./config"


class NetworkConfig(BaseModel):
    environment: NetworkEnvironment
    switches: List[Switch]
    nodes: List[Node]
    links: List[Link]


def save_config(config: NetworkConfig, filename: str):
    with open(filename, "w") as f:
        f.write(config.model_dump_json(indent=2))


def load_config(filename: str) -> NetworkConfig:
    with open(filename, "r") as f:
        return NetworkConfig.model_validate_json(f.read())


def _exec(cmd: Union[List[str], str]) -> str:
    if isinstance(cmd, list):
        cmd_str = ' '.join(cmd)
    else:
        cmd = shlex.split(cmd)
        cmd_str = cmd
    try:
        logger.debug(f"running subprocess: {cmd_str}")
        return subprocess.check_output(cmd, text=True).strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"subprocess failed '{cmd_str}': {e}")
        raise


class LacpPortChannelAllocator:
    def __init__(self, lacp_config: LacpGroupConfig):
        self.lacp_config = lacp_config
        self._used_portchannels = []
        self._used_mlags = []

    def load_interface(self, interface: SwitchInterface):
        if interface.portchannel > 0:
            self._used_portchannels.append(interface.portchannel)
        if interface.mlag > 0:
            self._used_mlags.append(interface.mlag)

    def allocate(self) -> Tuple[int, int]:
        next_portchannel = self.lacp_config.portchannel_start
        while next_portchannel in self._used_portchannels:
            next_portchannel += 1

        next_mlag = self.lacp_config.mlag_start
        while next_mlag in self._used_mlags:
            next_mlag += 1

        self._used_portchannels.append(next_portchannel)
        self._used_mlags.append(next_mlag)
        return next_portchannel, next_mlag


class VlanIPAllocator:
    def __init__(self, vlan: VlanConfig):
        self._vlan = vlan
        self._subnet = ipaddress.ip_network(vlan.subnet)

        self._used_ips = {ipaddress.ip_address(vlan.gateway)}

    def load_interface(self, interface: NodeInterface):
        if interface.address:
            ip = ip_address(interface.address)
            if ip in self._subnet and ip not in self._used_ips:
                self._used_ips.add(ip)

    def allocate(self) -> ipaddress.IPv4Address:
        subnet = ipaddress.ip_network(self._vlan.subnet)
        for ip in subnet.hosts():
            if ip not in self._used_ips:
                allocated_ip = ip
                break
        else:
            raise ValueError(f"No available IP addresses in subnet {self._vlan.subnet}")
        self._used_ips.add(allocated_ip)
        return allocated_ip


def import_command(
        doc: NetworkConfig,
        filters: List[str],
) -> None:
    """Import nodes and switches based on pattern(s) and add to config file"""
    logger.debug(f"Importing nodes and switches based on pattern(s): {filters}")

    switches = {switch.name for switch in doc.switches}
    nodes = {node.name for node in doc.nodes}

    devices_doc = json.loads(_exec(["cscfg", "device", "show", "-ojson", "-f", *filters]))
    new_switches = {}
    new_nodes = []
    for device in devices_doc:
        name = device.get("name")
        device_type = device.get("type", "")
        role = device.get("role", "")

        if device_type == "SW" and role == "FE" and name not in switches:
            new_switches[name] = device["properties"].get("vendor", {}).get("name", "")
        elif device_type == "SR" and name not in nodes:
            new_nodes.append(name)

    logger.info(f"found {len(new_switches)} new switches and {len(new_nodes)} new nodes")
    for switch_name, vendor in new_switches.items():
        doc.switches.append(Switch(name=switch_name, vendor=vendor, interfaces=[]))
    for node_name in new_nodes:
        doc.nodes.append(Node(name=node_name))
    doc.nodes.sort(key=lambda n: n.name)
    doc.switches.sort(key=lambda s: s.name)


def discover_command(doc: NetworkConfig):
    """
    Discover LLDP connections between switches and nodes.
    Run LLDP discovery on each switch and add links to the configuration.
    """
    # Create indices
    nodes = {node.name: node for node in doc.nodes}
    switches = {switch.name: switch for switch in doc.switches}
    links = {}
    for link in doc.links:
        key = (link.src_name, link.src_if)
        links[key] = link

    # Run LLDP discovery on each switch
    lldp_results = []
    for switch in doc.switches:
        logger.info(f"query LLDP state on {switch.name}")
        result = _exec(shlex.split(f"cscfg switch interface lldp -fname={switch.name} -ojson"))
        lldp_results.extend([
            (d["src"], d["src_if"], d["dst_name"].split(".")[0], d["dst_if"],)
            for d in json.loads(result)
        ])

    # Process LLDP: find added/removed/changed links. For changed links, raise an error. For removed, log a warning
    changed_switch_if = []
    added_links = []
    seen_links = set()
    for src, src_if, dst, dst_if in lldp_results:
        if src not in switches or dst not in nodes:
            logger.debug(f"ignoring link: ({src}:{src_if} -> {dst}:{dst_if})")
            continue

        link_key = (src, src_if)
        if link_key in links:
            existing_link = links[link_key]
            if existing_link.dst_name != dst or existing_link.dst_if != dst_if:
                changed_switch_if.append((src, src_if,))
        else:
            added_links.append(Link(src_name=src, src_if=src_if, dst_name=dst, dst_if=dst_if))
        seen_links.add((src, src_if,))

    if changed_switch_if:
        logger.error(f"found changed links: {changed_switch_if}. These MUST be resolved manually!")
        raise ValueError("previously configured links have changed and require manual intervention to resolve")

    # Check for removed links
    prev_links = set(links.keys())
    prev_links.difference_update(seen_links)
    if prev_links:
        logger.warning(f"found {len(prev_links)} removed links. This indicates DOWN interfaces or removed devices")
        for src, src_if in prev_links:
            link = links[(src, src_if)]
            logger.warning(f"removed link: {link.src_name}:{link.src_if} -> {link.dst_name}:{link.dst_if}")

    # Add discovered links and their interfaces to the configuration
    if added_links:
        logger.info(f"Adding {len(added_links)} discovered links to configuration")
        doc.links.extend(added_links)
        for link in added_links:
            logger.debug(f"Adding link: {link}")
            sw = switches[link.src_name]
            if not any(iface.name == link.src_if for iface in sw.interfaces):
                sw.interfaces.append(SwitchInterface(name=link.src_if, vlan=0, portchannel=0, mlag=0))
            node = nodes[link.dst_name]
            if not any(iface.name == link.dst_if for iface in node.interfaces):
                node.interfaces.append(NodeInterface(name=link.dst_if, type="ethernet", address=""))


def _allocate(doc: NetworkConfig):
    vlan_ip_allocators = {}
    lacp_group_allocators = {}
    for lacp_group in doc.environment.lacp_groups:
        allocator = LacpPortChannelAllocator(lacp_group)
        for sw in lacp_group.switches:
            assert sw not in lacp_group_allocators, f"switch {sw} is already in an LACP group"
            lacp_group_allocators[sw] = allocator
        if lacp_group.vlan not in vlan_ip_allocators:
            vlan_ip_allocators[lacp_group.vlan] = VlanIPAllocator(
                vlan=[v for v in doc.environment.vlans if v.vlan == lacp_group.vlan].pop(),
            )

    # create indices for quick access
    nodes = {node.name: node for node in doc.nodes}
    node_to_link = {}
    for link in doc.links:
        node_to_link[(link.dst_name, link.dst_if)] = link

    sw_if_map = {}
    for sw in doc.switches:
        for interface in sw.interfaces:
            sw_if_map[(sw.name, interface.name)] = interface

    # pre-allocate ips
    needs_allocate = []
    node_switch_ifs = {}
    for node in doc.nodes:
        switch_ifs = []
        has_bond = False
        for interface in node.interfaces:
            if interface.type == "ethernet":
                link = node_to_link[(node.name, interface.name,)]
                switch_ifs.append((link.src_name, link.src_if,))
            elif interface.type == "bond":
                for alloc in vlan_ip_allocators.values():
                    alloc.load_interface(interface)
                has_bond = True
        if not has_bond and len(switch_ifs) == 2:
            needs_allocate.append(node)
            node_switch_ifs[node.name] = switch_ifs
        elif not has_bond and len(switch_ifs) < 2:
            logger.warning(f"Node {node.name} doesn't have 2 associated ethernet interfaces. "
                           "Check port status! skipping IP allocation")
        elif not has_bond:
            raise AssertionError(f"Node {node.name} has more than 2 associated ethernet interfaces!")

    # pre-allocate the portchannels/mlag ids
    for sw in doc.switches:
        if sw.name in lacp_group_allocators:
            alloc = lacp_group_allocators[sw.name]
        else:
            continue
        for interface in sw.interfaces:
            alloc.load_interface(interface)

    for node in sorted(needs_allocate, key=lambda n: n.name):
        switch_ifs = node_switch_ifs[node.name]
        sw_if0, sw_if1 = switch_ifs[0], switch_ifs[1]
        pc_alloc0 = lacp_group_allocators[sw_if0[0]]
        pc_alloc1 = lacp_group_allocators[sw_if1[0]]
        assert pc_alloc0 is pc_alloc1, f"Switches {sw_if0[0]} and {sw_if1[0]} are not in the same LACP group"
        portchannel, mlag = pc_alloc0.allocate()
        sw_if_map[sw_if0].portchannel = portchannel
        sw_if_map[sw_if1].portchannel = portchannel
        sw_if_map[sw_if0].mlag = mlag
        sw_if_map[sw_if1].mlag = mlag
        sw_if_map[sw_if0].vlan = pc_alloc0.lacp_config.vlan
        sw_if_map[sw_if1].vlan = pc_alloc0.lacp_config.vlan
        vlan_alloc = vlan_ip_allocators[pc_alloc0.lacp_config.vlan]
        bond_if = NodeInterface(name="bond25g0", type="bond", address=str(vlan_alloc.allocate()))
        nodes[node.name].interfaces.append(bond_if)
        logger.debug(f"assigned {node.name}::bond25g0 interface with address {bond_if.address}")


def _refresh_config_dir(cfg_root: pathlib.Path):
    """Remove the configuration directory if it exists"""
    if cfg_root.exists():
        for item in cfg_root.iterdir():
            item.unlink()
    cfg_root.mkdir(parents=True, exist_ok=True)


def _generate_switch_configs(doc: NetworkConfig):
    if_mapping = {}
    for link in doc.links:
        if_mapping[(link.src_name, link.src_if)] = (link.dst_name, link.dst_if)
        if_mapping[(link.dst_name, link.dst_if)] = (link.src_name, link.src_if)

    enter_cmds = [
        "terminal length 0",
        "terminal dont-ask",
        "en",
        "config",
    ]
    exit_cmds = [
        "exit",
        "write memory",
    ]

    for sw in doc.switches:
        if sw.vendor != "AR":
            raise AssertionError(f"switch {sw.name} is not an Arista switch, cannot generate config")
        logger.info(f"Generating configuration for switch {sw.name}")
        interface_cfg = []
        switch_config = [*enter_cmds]
        for interface in sw.interfaces:
            if not interface.portchannel:
                continue
            # ethernet + portchannel + mlag
            node, node_if = if_mapping[(sw.name, interface.name)]
            if_config = [
                f"default interface Port-Channel {interface.portchannel}",
                f"interface Port-Channel {interface.portchannel}",
                f"  description {node} bond25g0",
                f"  switchport access vlan {interface.vlan}",
                f"  mlag {interface.mlag}",
                "exit",
                f"default interface {interface.name}",
                f"interface {interface.name}",
                f"  description {node} {node_if}",
                f"  channel-group {interface.portchannel} mode active",
                "  lacp timer fast",
                "exit",
                "",
            ]
            interface_cfg.append({
                "dst": node,
                "dst_if": node_if,
                "cmds": if_config,
            })
            switch_config.extend(if_config)
        switch_config.extend(exit_cmds)

        cfg_root = pathlib.Path(f"{doc.environment.config_root}/{sw.name}")
        _refresh_config_dir(cfg_root)
        with open(cfg_root / "cmds.txt", "w") as f:
            f.write("\n".join(switch_config))
        with open(cfg_root / "cmds.json", "w") as f:
            f.write(json.dumps({
                "enter": enter_cmds,
                "interfaces": interface_cfg,
                "exit": exit_cmds,
            }, indent=2))


def _generate_node_configs(doc: NetworkConfig):
    subnet_gateway = {}
    for vlan in doc.environment.vlans:
        subnet = ipaddress.ip_network(vlan.subnet)
        subnet_gateway[subnet] = ipaddress.ip_address(vlan.gateway)

    for node in doc.nodes:
        configs = {}
        logger.info(f"Generating configuration for node {node.name}")
        for interface in node.interfaces:
            if interface.type != "bond":
                continue
            if not interface.address:
                logger.warning(f"Node {node.name} interface {interface.name} has no address, skipping")
                continue

            subnet, gateway = None, None
            addr = ipaddress.ip_address(interface.address)
            for sn, gw in subnet_gateway.items():
                if addr in sn:
                    subnet = sn
                    gateway = gw
                    break
            if not subnet or not gateway:
                logger.error(
                    f"Node {node.name} interface {interface.name} address {interface.address} not in any subnet")
                continue

            configs[f"ifcfg-{interface.name}"] = textwrap.dedent(f"""
                BONDING_OPTS="mode=802.3ad miimon=100 lacp_rate=fast"
                TYPE=Bond
                BONDING_MASTER=yes
                PROXY_METHOD=none
                BROWSER_ONLY=no
                BOOTPROTO=none
                IPADDR={interface.address}
                PREFIX={subnet.prefixlen}
                DEFROUTE=yes
                IPV4_FAILURE_FATAL=no
                IPV6INIT=yes
                IPV6_AUTOCONF=yes
                IPV6_DEFROUTE=yes
                IPV6_FAILURE_FATAL=no
                IPV6_ADDR_GEN_MODE=stable-privacy
                NAME={interface.name}
                DEVICE={interface.name}
                ONBOOT=yes
                AUTOCONNECT_SLAVES=yes""").strip()

            for eth in node.interfaces:
                if eth.type == "ethernet":
                    configs[f"ifcfg-{eth.name}-bond"] = textwrap.dedent(f"""
                        TYPE=Ethernet
                        NAME={eth.name}-bond
                        DEVICE={eth.name}
                        ONBOOT=yes
                        MASTER={interface.name}
                        SLAVE=yes""").strip()
            configs[f"route-{interface.name}"] = textwrap.dedent(f"""
                        ADDRESS0=0.0.0.0
                        NETMASK0=0.0.0.0
                        GATEWAY0={gateway}""").strip()

        cfg_root = pathlib.Path(f"{doc.environment.config_root}/{node.name}")
        _refresh_config_dir(cfg_root)
        for filename, content in configs.items():
            with open(cfg_root / filename, "w") as f:
                f.write(content)
            logger.debug(f"Generated config file {filename} for node {node.name}")


def generate_command(doc: NetworkConfig) -> None:
    """Generate configuration files based on network config"""
    _allocate(doc)
    _generate_switch_configs(doc)
    _generate_node_configs(doc)


def _find_devices_by_filter(doc: NetworkConfig, filter: List[str]) -> List[str]:
    node_names = {node.name for node in doc.nodes}
    result = _exec("cscfg device show -ojson -f " + " ".join(filter))
    devices = json.loads(result)
    return [d["name"] for d in devices if d["name"] in node_names]


def push_command(doc, filter: List[str], dry_run: bool = False) -> bool:
    """Push configurations to devices matching the name pattern"""
    names = _find_devices_by_filter(doc, filter)

    sw_cmds = defaultdict(list)
    for sw in doc.switches:
        with open(f"{doc.environment.config_root}/{sw.name}/cmds.json", "r") as f:
            cmds_doc = json.load(f)
        for interface in cmds_doc.get("interfaces", []):
            if interface["dst"] not in names:
                continue
            sw_cmds[sw.name].extend(interface["cmds"])
        if sw.name in sw_cmds:
            # add enter and exit commands
            sw_cmds[sw.name] = cmds_doc["enter"] + sw_cmds[sw.name] + cmds_doc["exit"]

    # for each switch make a temporary file, write the commands to it, and push it to the switch using cscfg
    for sw_name, cmds in sw_cmds.items():
        # create a temporary file with the commands
        temp_cmds_file = f"/tmp/{sw_name}_cmds.txt"
        with open(temp_cmds_file, "w") as f:
            f.write("\n".join(cmds))
        if dry_run:
            continue
        _exec(f"cscfg dev exec -f name={sw_name} -y --file {temp_cmds_file}")

    if dry_run:
        logger.info("Dry run mode enabled, skipping actual push to devices")
        return True

    # for each node, copy over the configs to /etc/sysconfig/network-scripts/
    ok, failed = [], []
    for name in names:
        config_path = pathlib.Path(f"{doc.environment.config_root}/{name}/")
        if not config_path.exists():
            logger.warning(f"No configuration found for node {name}, skipping")
            continue
        # use scp to copy the files to the node
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(name, username="root")
            sftp = ssh.open_sftp()
            for filename in config_path.iterdir():
                if filename.is_file():
                    remote_path = f"/etc/sysconfig/network-scripts/{filename.name}"
                    sftp.put(str(filename), remote_path)
                    logger.info(f"Copied {filename.name} to {name}:{remote_path}")
            sftp.close()

            # now edit Wired connection 1 to never default
            stdin, stdout, stderr = ssh.exec_command(
                'bash -c \'nmcli -t -f NAME,TYPE,DEVICE con show --active | grep Wired | grep -v idrac | cut -d: -f1 | xargs -I% -n1 nmcli conn mod % ipv4.never-default yes\''
            )
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                logger.error(f"Failed to remove default route on {name}: {stderr.read().decode()}")

            # restart NetworkManager to apply changes
            logger.info(f"Successfully pushed configs to {name}")

            ssh.close()

            # finally, update the device management info with the new IP address
            for node in doc.nodes:
                if node.name == name:
                    for interface in node.interfaces:
                        if interface.type == "bond" and interface.address:
                            addr = ipaddress.ip_address(interface.address)
                            _exec(f"cscfg device edit -f name={name} -y -p management_info.ip={addr}")
                            logger.info(f"Updated management info for {name} with IP {addr}")
                            break
                    break
            ok.append(name)

        except Exception as e:
            logger.error(f"Failed to push configs to {name}: {e}")
            failed.append(name)

    # reboot the node to apply the changes
    for node in ok:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            logger.info(f"Rebooting node {node} after pushing configs")
            ssh.connect(node, username="root")
            ssh.exec_command("reboot")
            ssh.close()
        except Exception as e:
            logger.error(f"Failed to reboot {node} after pushing configs: {e}")

    return len(failed) == 0


def test_command(doc: NetworkConfig, filter: List[str]) -> bool:
    """Test command to verify configurations"""
    names = _find_devices_by_filter(doc, filter)
    if not names:
        logger.warning("No devices found matching the filter")
        return True

    # get the node/ip pairings and ping them
    node_ips = {}
    for node in doc.nodes:
        if node.name in names:
            for interface in node.interfaces:
                if interface.type == "bond" and interface.address:
                    node_ips[node.name] = interface.address
                    break
    # ping them
    failed = []
    for node, ip in node_ips.items():
        try:
            logger.debug(f"Pinging {node} at {ip}")
            result = _exec(f"ping -W3 -c 1 {ip}")
            logger.debug(f"Ping to {node} ({ip}) successful: {result}")
        except subprocess.CalledProcessError as e:
            logger.info(f"Ping to {node} ({ip}) failed: {e}")
            failed.append((node, ip,))
    if failed:
        for node, ip in failed:
            print(f"{node}:{ip} failed")
    return len(failed) == 0


def _confirm_config_diff(doc: NetworkConfig, working_doc: NetworkConfig, prompt=False) -> bool:
    if doc == working_doc:
        if prompt:
            logger.info("No changes detected in the configuration")
        return False

    diff = difflib.unified_diff(
        doc.model_dump_json(indent=2).splitlines(),
        working_doc.model_dump_json(indent=2).splitlines(),
        fromfile="Original Config",
        tofile="Updated Config",
        lineterm='',
    )
    if not prompt:
        logger.debug("diff: " + "\n".join(diff))
        return True

    print("Configuration has changed. Please review the changes")
    print("\n".join(diff))
    confirm = input("Do you want to save the changes? (y/N): ")
    if confirm.lower() not in ("y", "yes"):
        print("Aborting save")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Frontend network configuration tool")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    parser.add_argument("--config", "-c", type=str, default="fe_network_config.json",
                        help="Frontend Network configuration file")
    parser.add_argument("--diff", action="store_true", help="Prompt to save changes if configuration has changed")

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create a new network configuration file")

    # Import command
    import_parser = subparsers.add_parser("import", help="Discover network topology")
    import_parser.add_argument("--filter", "-f", nargs="+", help="cscfg filter patterns")

    # Discover command
    discover_parser = subparsers.add_parser('discover', help='Discover LLDP connections')
    discover_parser.add_argument("--dry-run", action="store_true",
                                 help="Dry run mode, display discovered links without saving")

    # Generate command
    generate_parser = subparsers.add_parser("generate", help="Generate configuration files based on network config")
    generate_parser.add_argument("--dry-run", action="store_true",
                                 help="Dry run mode, display updated configurations without saving")

    push_parser = subparsers.add_parser("push", help="Push configs to devices")
    push_parser.add_argument("--filter", "-f", nargs="+", help="cscfg filter patterns")
    push_parser.add_argument("--dry-run", action="store_true", help="Dry run mode, do not push configs")

    test_parser = subparsers.add_parser("test", help="Test")
    test_parser.add_argument("--filter", "-f", nargs="+", help="cscfg filter patterns")

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    if args.command == "init":
        try:
            load_config(args.config)
        except FileNotFoundError:
            doc = NetworkConfig(
                environment=NetworkEnvironment(lacp_groups=[]),
                switches=[],
                nodes=[],
                links=[]
            )
            save_config(doc, args.config)
        return

    doc = load_config(args.config)
    working_doc = doc.model_copy(deep=True)
    if args.command == "import":
        import_command(working_doc, filters=args.filter)
    elif args.command == 'discover':
        discover_command(working_doc)
    elif args.command == "generate":
        generate_command(working_doc)
    elif args.command == "push":
        rv = 0 if push_command(doc, filter=args.filter, dry_run=args.dry_run) else 1
        sys.exit(rv)
    elif args.command == "test":
        rv = 0 if test_command(doc, filter=args.filter) else 1
        sys.exit(rv)
    else:
        raise AssertionError("unknown command")

    if _confirm_config_diff(doc, working_doc, prompt=args.diff):
        save_config(working_doc, args.config)


if __name__ == "__main__":
    main()
