#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved

"""
Sanity checker for network configuration
"""
import collections
import dataclasses
import ipaddress
import json
from collections import defaultdict
from typing import Dict, List

from .base import \
    SubCommandBase, \
    UsesNetworkConfig
from ..common.context import NetworkCfgDoc
from ..schema import JSIPNetwork, NetworkConfigSchema
from ..schema.utils import NODE_ROLE_SCHEMA_MAPPING, DEFAULT_NODE_ROLE_IF_COUNT
from ..common import SY_MAX_INTERFACE_COUNT

@dataclasses.dataclass
class CheckResult:
    type: str
    message: str
    fatal: bool = False


@dataclasses.dataclass
class NamedCheckResult:
    name: str
    results: List[CheckResult] = dataclasses.field(default_factory=list)

    def asdict(self, include_nonfatal: bool=True) -> Dict[str, List[Dict[str, str]]]:
        accept = lambda e: e.fatal or include_nonfatal
        return {
            "name": self.name,
            "results": sorted([dataclasses.asdict(r) for r in self.results if accept(r)], key=lambda e: (e['fatal'], e['type'],))
        }


@dataclasses.dataclass
class NetworkDocCheckResult:
    device_errors: List[NamedCheckResult] = dataclasses.field(default_factory=list)
    allocation_errors: List[CheckResult] = dataclasses.field(default_factory=list)

    def add_device_error(self, name: str, err: CheckResult):
        """ Add or append a device error """
        for dev_err in self.device_errors:
            if dev_err.name == name:
                dev_err.results.append(err)
                return
        self.device_errors.append(NamedCheckResult(name=name, results=[err]))

    def add_allocation_error(self, err: CheckResult):
        """ Add an allocation error """
        self.allocation_errors.append(err)

    def asdict(self, include_nonfatal=True) -> Dict[str, List[Dict[str, str]]]:
        """ Convert the check result to a dictionary for JSON serialization """
        accept = lambda e: e.fatal or include_nonfatal
        return {
            "device_errors": sorted([n.asdict(include_nonfatal) for n in self.device_errors], key=lambda e: (e['name'],)),
            "allocation_errors": sorted([dataclasses.asdict(r) for r in self.allocation_errors if accept(r)], key=lambda e: (e['fatal'], e['type'],))
        }

    def is_ok(self) -> bool:
        """ Check if there are no errors in the result """
        return not (self.device_errors or self.allocation_errors)

    def is_fatal(self) -> bool:
        """ Check if there are any fatal errors in the result """
        return any(r.fatal for r in self.allocation_errors) or any(any(r.fatal for r in n.results) for n in self.device_errors)


class SystemGroup:
    def __init__(self):
        self._switches = [[] for _ in range(SY_MAX_INTERFACE_COUNT)]
        self._systems = []

    def add(self, sys, port_switch) -> 'SystemGroup':
        self._systems.append(sys)
        for i, switch in enumerate(port_switch):
            self._switches[i].append(switch)
        return self

    def check_port_switch_consistency(self) -> List[str]:
        """ check each group has 1 unique switch per port index """
        errors = []
        for port_index, switches in enumerate(self._switches):
            if not switches:
                continue
            count = collections.Counter(switches)
            most_common_switch = count.most_common(1)[0][0]
            for sys_index, switch in enumerate(switches):
                if most_common_switch != switch:
                    # System ports are 0 indexed in the config but generally 1 indexed when reporting issues to IT
                    errors.append({
                        "system": self._systems[sys_index],
                        "error": (f"system {self._systems[sys_index]} port {port_index + 1} connected "
                                 f"to switch {switch} but should have connected to {most_common_switch}")
                    })

        return errors

    def __str__(self):
        switches = set()
        for s in self._switches:
            switches = switches.union(set(s))
        return f"system group sharing {len(switches)} switches ({', '.join(switches)})"


def system_switch_check(network_config: dict) -> List[Dict[str, str]]:
    """ Group systems into systems sharing the same switches. Ensure that each port index connects to only 1 switch.
    Returns a list of errors if found.
    """
    errors = []
    switch_group = {}

    # group systems by their set of connected switches
    nwdoc = NetworkCfgDoc(network_config)
    for sys in network_config.get("systems", []):
        port_switch = [""] * nwdoc.get_system_expected_ports(sys['name'])
        for inf in sys.get("interfaces", []):
            if inf['name'].startswith("data"):
                port_switch[int(inf['name'][4:])] = inf['switch_name']

        group, err = None, ""
        for switch in set(port_switch):
            if group and switch in switch_group and switch_group[switch] is not group:
                err = f"system {sys['name']} should have connected only to {group} but " \
                      f"also connected to {switch_group[switch]}. Check system connections"
                break
            elif not group and switch in switch_group:
                group = switch_group[switch]
        if err:
            errors.append({"system": sys['name'], "error": err})
        elif group:
            group.add(sys['name'], port_switch)
        else:
            group = SystemGroup().add(sys['name'], port_switch)
            for switch in set(port_switch):
                switch_group[switch] = group

    for group in set(switch_group.values()):
        errors.extend(group.check_port_switch_consistency())
    return errors


def check_xconn_count(network_config: dict, result: NetworkDocCheckResult):
    """ Validate number of cross-connects """
    # Check if all leaf/memx switches have the same number of cross-connects
    switch_tier = 'LF' if network_config.get("environment", {}).get("topology") == "leaf_spine" else 'AW'
    switches = [s['name'] for s in network_config.get("switches", []) if s['tier'] == switch_tier]

    xconn_counts = collections.defaultdict(int)
    for conn in network_config.get("xconnect", {}).get("connections", []):
        for l in conn.get("links", []):
            if l['name'] not in switches:
                continue
            xconn_counts[l['name']] += 1

    if not xconn_counts:
        if len(switches) > 1:
            for switch in switches:
                result.add_device_error(switch, CheckResult(
                    type="missing_xconn",
                    message=f"No cross-connects found for {switch_tier} switch {switch}",
                    fatal=True,
                ))
        return
    # xxx: we don't know the design of the network, so we assume that all switches have the same number of xconnects
    # and that's the most commonly counted value
    assumed_expected_count, _ = collections.Counter(xconn_counts.values()).most_common(1)[0]
    for switch, count in xconn_counts.items():
        if count != assumed_expected_count:
            result.add_device_error(switch, CheckResult(
                type="incorrect_xconn_count",
                message=f"Expected {assumed_expected_count} spine/leaf or br/aw links but found {count}",
                fatal=False,
            ))


def check_interface_count(network_config: dict, node_type: str, min_if_count: int, result: NetworkDocCheckResult):
    """ Compare the number of interfaces on a node >=  expected value, and has an address + switch assigned """
    for node_obj in network_config.get(node_type, []):
        iface_list = node_obj.get("interfaces", list())
        if len(iface_list) < min_if_count:
            result.add_device_error(node_obj['name'], CheckResult(
                type="insufficient_interface_count",
                message=f"expected {min_if_count} interfaces, got {len(iface_list)}",
                fatal=True
            ))

        for iface_obj in iface_list:
            switch_name = iface_obj.get("switch_name", "")
            if not switch_name:
                result.add_device_error(node_obj['name'], CheckResult(
                    type="interface_missing_switch",
                    message=f"{iface_obj['name']} did not have a neighboring switch",
                ))
            address = iface_obj.get("address", "")
            if not address:
                result.add_device_error(node_obj['name'], CheckResult(
                    type="interface_missing_address",
                    message=f"{iface_obj['name']} did not have an address assigned",
                ))


def check_node_switch_connections(network_config: dict, node_type: str, result: NetworkDocCheckResult):
    """ Verify that all interfaces on a node are on the same switch """
    for node_obj in network_config.get(node_type, []):
        saved_switch = ""
        iface_list = node_obj.get("interfaces", list())
        for iface_obj in iface_list:
            switch_name = iface_obj.get("switch_name", "")
            if not saved_switch:
                saved_switch = switch_name
            if switch_name != saved_switch:
                result.add_device_error(node_obj['name'], CheckResult(
                    type="interface_incorrect_switch",
                    message=f"{iface_obj['name']} is connected to switch {switch_name} "
                            f"but should have been connected to {saved_switch}",
                    fatal=True
                ))


def check_ax_switch_connections(network_config: dict, result: NetworkDocCheckResult):
    """
    For a switch, the number of connections from individual AX nodes must be the same
    for all AX nodes connected to it
    """
    if not network_config.get('activation_nodes'):
        return

    # switch->node->num_connections_to_switch
    switch_conn = dict()
    for n in network_config['activation_nodes']:
        for i in n.get('interfaces', []):
            switch_name = i.get('switch_name')
            if not switch_name:
                continue
            if switch_name not in switch_conn:
                switch_conn[switch_name] = dict()
            if n['name'] not in switch_conn[switch_name]:
                switch_conn[switch_name][n['name']] = 0
            switch_conn[switch_name][n['name']] += 1
    # Check if the number of connections is the same from each node
    # for a given switch
    for switch_name, nodes in switch_conn.items():
        if len(set(nodes.values())) > 1:
            most_common_count, _ = collections.Counter(nodes.values()).most_common(1)[0]
            unequal_nodes = [n for n, count in nodes.items() if count != most_common_count]
            for node in unequal_nodes:
                result.add_device_error(node, CheckResult(
                    type="unequal_ax_connections",
                    message=f"expected {most_common_count} connections to switch {switch_name}",
                    fatal=True
                ))


def check_system_vip_assigned(network_config: dict, result: NetworkDocCheckResult):
    """ Ensure that the VIP is set """
    for system_obj in network_config.get("systems", []):
        vip_addr = system_obj.get("vip", "")
        if not vip_addr:
            result.add_device_error(system_obj["name"], CheckResult(
                type="system_missing_vip",
                message=f"System {system_obj['name']} does not have a VIP assigned",
                fatal=True
            ))



_MIN_VIRTUAL_INTERFACES = {
    "activation_interfaces": 30,  # ax nodes have 2 interfaces connected to 2 switches, count 30 pods per interface
    "worker_nodes": 2,  # max 2 workers pods per worker node
    "memoryx_nodes": 3,  # max 3 wgt+cmd+chf pods per memx
    "management_nodes": 12,  # estimate ~2 crd pods plus some pods for ceph, nginx, ...
    "inferencedriver_nodes": 10,  # similar to mgmt nodes but without core services pods
}

_MIN_VIRTUAL_INTERFACES_INFERENCE = {
    "activation_interfaces": 20,  # Fewer pods per interface mode clusters
}


def check_virtual_interface_count(network_config: dict, result: NetworkDocCheckResult):
    """
    Check vlans have sufficient number of virtual interfaces. Populate the NetworkDocCheckResult with allocation errors
    if any.
    """
    cluster_count = len(network_config.get("clusters", ["default to 1 cluster"]))

    # count SWITCH/VLAN{memx,swarmx} -> COUNTS{ax_interfaces,sx_interfaces,...}
    min_virtual_interfaces = _MIN_VIRTUAL_INTERFACES.copy()
    if NetworkCfgDoc(network_config).is_inference_cluster():
        min_virtual_interfaces.update(_MIN_VIRTUAL_INTERFACES_INFERENCE)

    # scale the minimum virtual interfaces by the number of clusters. If there are 2 clusters, we need double the
    # number of virtual interfaces since each cluster will have its own set of virtual interfaces.
    for key in min_virtual_interfaces.keys():
        min_virtual_interfaces[key] = min_virtual_interfaces[key] * cluster_count

    def count_interfaces(node: dict, vlan: str, key: str, counts: dict):
        for inf in node.get("interfaces", []):
            sw = inf.get("switch_name")
            k = f"{sw}/{vlan}"
            if k not in counts:
                counts[k] = {}
            if key not in counts[k]:
                counts[k][key] = 0
            counts[k][key] = counts[k][key] + 1

    def count_nodes(node: dict, vlan: str, key: str, counts: dict):
        for inf in node.get("interfaces", []):
            sw = inf.get("switch_name")
            k = f"{sw}/{vlan}"
            if k not in counts:
                counts[k] = {}
            if key not in counts[k]:
                counts[k][key] = set()
            counts[k][key].add(node["name"])

    # estimate the number of virtual addresses needed for each vlan
    counts = {}
    for host_type in min_virtual_interfaces.keys():
        host, count_type = host_type.split("_")
        counter_fn = count_interfaces if count_type == "interfaces" else count_nodes
        for node in network_config.get(f"{host}_nodes", []):
            vlan = "swarmx" if host == "swarmx" else "memoryx"
            counter_fn(node, vlan, host_type, counts)

    min_switch_vlan_size = {}
    interface_msgs = defaultdict(list)
    for counter_key, type_counts in counts.items():
        min_switch_vlan_size[counter_key] = 0
        for count_type, count_val in type_counts.items():
            if count_type.endswith("_interfaces"):
                min_switch_vlan_size[counter_key] += count_val * min_virtual_interfaces[count_type]
                interface_msgs[counter_key].append(f"{count_val} {count_type.split('_')[0]} with {min_virtual_interfaces[count_type]} addrs each interface")
            else:
                min_switch_vlan_size[counter_key] += len(count_val) * min_virtual_interfaces[count_type]
                interface_msgs[counter_key].append(f"{len(count_val)} {count_type.split('_')[0]} with {min_virtual_interfaces[count_type]} addrs each node")

    # compare the actual vrange sizes with the given
    def range_size(start_end: dict) -> int:
        if not start_end:
            return 0
        ip_start = ipaddress.ip_address(start_end["starting_address"].split("/")[0])
        ip_end = ipaddress.ip_address(start_end["ending_address"].split("/")[0])
        return int(ip_end) - int(ip_start)

    for sw in network_config.get("switches", []):
        for vlan_type, k in {"memoryx": "virtual_addrs"}.items():
            key = f"{sw['name']}/{vlan_type}"
            actual_size = range_size(sw.get(k, {}))
            if min_switch_vlan_size.get(key, 0) > actual_size:
                result.add_allocation_error(CheckResult(
                    type="insufficient_virtual_addresses",
                    message=f"Switch {sw['name']} VLAN {vlan_type} has {actual_size} virtual addresses "
                            f"but requires at least {min_switch_vlan_size.get(key, 0)} to support "
                            f"{cluster_count} cluster(s) over {', '.join(interface_msgs[key])}",
                    fatal=True,
                ))


def check_duplicate_ips(network_config: dict) -> List[str]:
    duplicates = list()
    ip_instances = collections.defaultdict(list)
    for sect in NetworkConfigSchema.node_sections + NetworkConfigSchema.system_sections:
        for device in network_config.get(sect, []):
            for iface in device.get('interfaces', []):
                if iface.get('address'):
                    addr = iface['address'].split('/')[0]
                    ip_instances[addr].append(
                        f"{device['name']}: {iface['name']}"
                    )
            if sect in NetworkConfigSchema.system_sections:
                if device.get('vip'):
                    addr = device['vip'].split('/')[0]
                    ip_instances[addr].append(
                        f"{device['name']}: VIP"
                    )

    for connection in network_config.get('xconnect', {}).get('connections', []):
        if not connection.get('prefix'):
            continue
        nw = JSIPNetwork(connection['prefix'])
        for addr in nw:
            ip_instances[addr].append(connection['name'])

    for connection in network_config.get('exterior_connections', []):
        for conn in ('exterior', 'interior'):
            if connection[conn].get('address'):
                addr = connection[conn]['address'].split('/')[0]
                ip_instances[addr].append(connection['name'])

    for cluster in network_config.get("clusters", []):
        for iface in cluster.get('data_network', {}).get('interfaces', []):
            if iface.get('address'):
                addr = iface['address'].split('/')[0]
                ip_instances[addr].append(f"{cluster['name']}: {iface['name']}")

    for ip, targets in ip_instances.items():
        if len(targets) > 1:
            duplicates.append(f"{ip} assigned to {', '.join(targets)}")

    return duplicates


_NODE_ROLE_ONE_SWITCH = ("memoryx_nodes", "swarmx_nodes")


def sanity_check(
        network_config: dict,
        node_expected_nic_count: Dict[str, int] = None
) -> NetworkDocCheckResult:
    """
    Args:
        network_config: parsed network config document
        node_expected_nic_count: dict of shorthand role to expected interface count, e.g. AX: 2
    """
    result = NetworkDocCheckResult()

    for role, expected_if_count in DEFAULT_NODE_ROLE_IF_COUNT.items():
        if node_expected_nic_count and role in node_expected_nic_count:
            expected_if_count = node_expected_nic_count[role]
        check_interface_count(network_config, NODE_ROLE_SCHEMA_MAPPING[role], expected_if_count, result)

    for role in _NODE_ROLE_ONE_SWITCH:
        check_node_switch_connections(network_config, role, result)

    check_ax_switch_connections(network_config, result)

    check_system_vip_assigned(network_config, result)

    for sys_err in system_switch_check(network_config):
        result.add_device_error(sys_err["system"], CheckResult(
            type="inconsistent_system_connection",
            message=sys_err["error"],
            fatal=True
        ))

    check_xconn_count(network_config, result)

    check_virtual_interface_count(network_config, result)
    for duplicate_ip in check_duplicate_ips(network_config):
        result.add_allocation_error(CheckResult(
            type="duplicate_ip",
            message=duplicate_ip,
            fatal=True
        ))

    return result


@UsesNetworkConfig(save=False)
class SanityCheckConfig(SubCommandBase):
    """Run some basic sanity checks on the network configuration and
    report results.
    """
    sub_command = 'sanity'

    @staticmethod
    def build_parser(parser):
        pass

    def __call__(self, args):
        result = sanity_check(args.network_config)
        print(json.dumps(result.asdict(), indent=2))
