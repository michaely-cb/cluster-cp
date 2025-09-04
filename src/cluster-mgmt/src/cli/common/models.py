import collections
import ipaddress
import json
import logging
import re
import socket
import subprocess
import typing
from typing import Optional

logger = logging.getLogger(__name__)

_json_serializers = {}

MEMX_PER_POP_RACK_V2 = 12
MIN_MEMX_POPULATED_RACK = 10
DEFAULT_DEPOP_RACK_SIZE = 4
MIN_WORKERS_PER_CV_RACK = 6  # CV racks are IO bound on workers, have more workers than LLM racks

NAME_RE = r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$'


def _json_serializer(obj):
    if type(obj) in _json_serializers:
        return _json_serializers[type(obj)](obj)
    return obj


def json_repr(*fields):
    """
    Registers the decorated class with yaml library's representers.
    Arguments:
         *fields: A list of names of fields of the targetted class to be serialized.
            Names may end in '?' in which case they will only be included if
            they are non-empty. Names may also be formatted as
            "<field>:<equivalent_field>" in which case <field> will only be
            included if the value of field != value of equivalent_field.
    """

    def dataclass_repr(obj):
        map = {}
        for k in fields:
            if k.endswith("?"):
                v = getattr(obj, k[:-1])
                if v is not None and v != [] and v != {}:
                    map[k[:-1]] = v
            elif ":" in k:
                k, equiv_k = k.split(":")
                if getattr(obj, k) != getattr(obj, equiv_k):
                    map[k] = getattr(obj, k)
            else:
                map[k] = getattr(obj, k)

        for k, v in map.items():
            if type(v) in _json_serializers:
                map[k] = _json_serializers[type(v)](v)

        return map

    def to_yaml(self) -> str:
        # lazy load since this file should be able to be imported without pyyaml
        import yaml
        jsonstr = json.dumps(self, default=_json_serializer)
        return yaml.dump(json.loads(jsonstr), sort_keys=False)

    def to_json(self) -> str:
        return json.dumps(self, default=_json_serializer)

    def _jsonrepr(cls):
        _json_serializers[cls] = dataclass_repr
        cls.to_yaml = to_yaml
        cls.to_json = to_json
        return cls

    return _jsonrepr


def flatten(l):
    return [item for sublist in l for item in sublist]


def bucket(items: typing.List[any], n: int) -> typing.List[typing.List[any]]:
    """puts items into n buckets where the first len(items)/n items are in bucket 1 and so on."""
    n = max(1, n)
    buckets = []
    count = int(len(items) / n)
    extra = len(items) % n
    lo, hi = 0, 0
    while hi < len(items):
        lo = hi
        hi = lo + count + (1 if extra > 0 else 0)
        extra -= 1
        buckets.append(items[lo:hi])
    while len(buckets) < n:
        buckets.append([])
    return buckets


class NIC:
    def __init__(self, node: str, address: str, name: str):
        self.node = node
        self.address = address
        self.name = name

    def __repr__(self):
        return f"{self.node}/{self.name}:{self.address}"

    def __hash__(self):
        return hash((self.node, self.address, self.name))

    def __eq__(self, other):
        return (self.node, self.address, self.name) == (other.node, other.address, other.name)


@json_repr("name", "address", "csPort?", "v2Group?", "gbps?")
class NetworkInterface:
    def __init__(self,
                 name: str,
                 address: str,
                 csPort: typing.Optional[int] = None,
                 v2Group: typing.Optional[str] = None,
                 gbps: typing.Optional[str] = None,
                 ):
        self.name = name
        self.address = address
        self.csPort = csPort
        self.v2Group = v2Group
        self.gbps = gbps

    def __repr__(self):
        return f"{self.name}(csPort={self.csPort},v2Group={self.v2Group},gbps={self.gbps})"


@json_repr("name", "role?", "hostname:name", "networkInterfaces?", "properties?")
class Node:
    """Node corresponds with a kubernetes node.

    Attributes:
        name (str): Corresponds to the kubernetes name of the node.
        role (str): One of management, memory, broadcastreduce, worker, or any
        hostname (:obj:`str`, optional): The DNS resolvable hostname of the node
            used for SSH'ing to the node. This will default to the name if not
            specified.
        networkInterfaces: List of networkInterfaces. Required for broadcastreduce
            nodes and otherwise empty.
        properties: optional key/value pairs describing arbitrary admin-defined
            attributes of the node.
    """

    def __init__(self,
                 name: str,
                 role: typing.Optional[str] = None,
                 hostname: typing.Optional[str] = None,
                 networkInterfaces: typing.List[NetworkInterface] = None,
                 properties: typing.Dict[str, str] = None,
                 **kwargs):
        self.name = name
        self.role = role
        self.hostname = hostname
        self.networkInterfaces = networkInterfaces or []
        self.properties = properties or {}

        if self.hostname is None:
            self.hostname = self.name
        self.networkInterfaces = [
            NetworkInterface(**d) if isinstance(d, dict) else d
            for d in self.networkInterfaces
        ]


@json_repr("name", "type", "controlAddress", "hostname?",
           "managementAddress?", "managementUser?", "managementPassword?")
class System:
    def __init__(self,
                 name: str,
                 type: str,
                 controlAddress: str,
                 hostname: typing.Optional[str] = None,
                 managementAddress: typing.Optional[str] = None,
                 managementUser: typing.Optional[str] = "admin",
                 managementPassword: typing.Optional[str] = "admin",
                 nodegroup_id: int = -1,
                 **kwargs):
        self.name = name
        self.type = type
        self.controlAddress = controlAddress
        self.hostname = hostname
        self.managementAddress = managementAddress
        self.managementUser = managementUser
        self.managementPassword = managementPassword
        self.nodegroup = None
        if nodegroup_id != -1:
            self.nodegroup = f"nodegroup{nodegroup_id}"


@json_repr("name", "parentnet", "subnet", "gateway", "asn", "virtualStart", "virtualEnd")
class Vlan:
    """
    Attributes:
        name: The vlan name (memx, swarmx)
        subnet: The subnet covering the switch in CIDR notation. Addresses from the
            first IP in the subnet to `maxUsedIP` are assigned to NICs statically
            while the remaining IPs can be dynamically allocated by k8s.
        virtualStart: The start ip for macvlan.
        virtualEnd: The end ip for macvlan.
        asn: BGP ASN
        kwargs: ignored but allows unknown fields in json to be ignored
    """

    def __init__(self,
                 name: str,
                 parentnet: str,
                 subnet: str,
                 gateway: str,
                 virtualStart: str,
                 virtualEnd: str,
                 asn: typing.Optional[str] = None,
                 **kwargs):
        self.name = name
        self.parentnet = parentnet
        self.subnet = subnet
        self.gateway = gateway
        self.asn = asn
        self.virtualStart = virtualStart
        self.virtualEnd = virtualEnd

    @classmethod
    def from_switch(cls, switch: typing.Dict, cluster_prefix: str) -> typing.List['Vlan']:
        """Create a Vlans from a network.json switch dict. """
        rv = []
        vaddrs = switch.get('virtual_addrs', {})
        if "starting_address" in vaddrs and "ending_address" in vaddrs:
            gateway = switch['address']
            rv.append(
                cls(
                    "memx",
                    cluster_prefix,
                    str(ipaddress.ip_network(gateway, False)),
                    gateway.split("/")[0],
                    vaddrs['starting_address'].split("/")[0],
                    vaddrs['ending_address'].split("/")[0],
                    asn=switch.get("asn"),
                )
            )
        if "swarmx_virtual_addrs" in switch:
            swarmx_vrange = switch["swarmx_virtual_addrs"]
            swarmx_gateway = switch['swarmx_vlan_address']
            rv.append(
                cls(
                    "swarmx",
                    cluster_prefix,
                    str(ipaddress.ip_network(swarmx_gateway, False)),
                    swarmx_gateway.split("/")[0],
                    swarmx_vrange['starting_address'].split("/")[0],
                    swarmx_vrange['ending_address'].split("/")[0],
                    asn=switch.get("asn"),
                )
            )
        return rv


@json_repr("subnet", "gateway", "parentnet", "asn?",
           "virtualStart", "virtualEnd",
           "memoryNIC?", "workerNIC?", "managementNIC?")
class SwitchConfig:
    """
    Attributes:
        subnet: The subnet covering the switch in CIDR notation. Addresses from the
            first IP in the subnet to `maxUsedIP` are assigned to NICs statically
            while the remaining IPs can be dynamically allocated by k8s.
        virtualStart: The start ip for macvlan.
        virtualEnd: The end ip for macvlan.
        *NIC: Name of NIC connected to this switch's subnet for different node roles.
        asn: BGP ASN
        kwargs: ignored but allows unknown fields in json to be ignored
    """

    def __init__(self,
                 subnet: str,
                 gateway: str,
                 parentnet: str,
                 virtualStart: str,
                 virtualEnd: str,
                 memoryNIC: typing.Optional[str] = None,
                 workerNIC: typing.Optional[str] = None,
                 managementNIC: typing.Optional[str] = None,
                 asn: typing.Optional[str] = None,
                 **kwargs):
        self.subnet = subnet
        self.gateway = gateway
        self.parentnet = parentnet
        self.asn = asn
        self.virtualStart = virtualStart
        self.virtualEnd = virtualEnd
        self.memoryNIC = memoryNIC
        self.workerNIC = workerNIC
        self.managementNIC = managementNIC
        self._role_nic = {"management": None, "memory": None, "worker": None}

    @classmethod
    def of(cls, cluster_prefix: str, switch: typing.Dict):
        """Create a SwitchConfig from a switch dict. """
        addr = switch['address']
        vaddrs = switch.get('virtual_addrs', {})
        start, end = None, None
        if "starting_address" in vaddrs and "ending_address" in vaddrs:
            start = vaddrs['starting_address'].split("/")[0]
            end = vaddrs['ending_address'].split("/")[0]

        return cls(
            str(ipaddress.ip_network(addr, False)),
            addr.split("/")[0],
            cluster_prefix,
            start,
            end,
            asn=switch.get("asn"),
        )

    def update(self, node: Node) -> 'SwitchConfig':
        """Update switch config based on linked Node NIC info.

        Finds a common NIC amonst all nodes in a switch node group and sets
        the corresponding *NIC attribute in the switch config.

        Parameters:
            node: node attached to switch
        """
        if node.role not in self._role_nic:
            raise ValueError(f"unhandled node role '{node.role}'")
        my_nics = set([n.name for n in node.networkInterfaces])
        role_nics = self._role_nic[node.role]
        role_nics = my_nics if role_nics is None else role_nics.intersection(my_nics)
        if len(role_nics) == 0:
            raise Exception(f"bad config file: inconsistent NICs found for role {node.role} "
                            f"in switch node group. No common set of NICs names found. Must ensure "
                            f"that all nodes in a switch node group have at least one NIC in common.")
        self._role_nic[node.role] = role_nics
        self.__setattr__(f"{node.role}NIC", sorted(list(role_nics))[0])
        return self


@json_repr("name", "properties?", "switchConfig?", "vlans?")
class NodeGroup:
    """
    Attributes:
        name: name of the node group
        properties: optional key/value pairs describing arbitrary admin-defined
            attributes of the node.
        switchConfig: optional key/value pairs describing the macvlan setup info including addr/nic/...
            Used in v1 networks.
        vlans: optional list of switchConfigs describing the vlans per leaf switch in v2 network
    """

    def __init__(self,
                 name: str,
                 properties: typing.Optional[typing.Dict[str, str]] = None,
                 switchConfig: typing.Optional[SwitchConfig] = None,
                 vlans: typing.Optional[typing.List[Vlan]] = None,
                 **kwargs):
        self.name = name
        self.properties = properties or {}
        self.switchConfig = switchConfig
        if self.switchConfig and type(self.switchConfig) != SwitchConfig:
            self.switchConfig = SwitchConfig(**self.switchConfig)
        self.vlans = vlans
        if self.vlans:
            self.vlans = [
                Vlan(**sc) if type(sc) != Vlan else sc for sc in self.vlans
            ]


@json_repr("kind?", "name", "properties", "systems", "nodes", "groups?", "v2Groups?", "userNode?")
class Cluster:
    """
    Attributes:
        userNode: node where the end user
            executes their run.py scripts. It is not part of the k8s cluster. Keep here for backwards compatible
    """

    def __init__(self, name, systems, nodes, groups=None, v2Groups=None, userNode=None,
                 kind="cluster-configuration", properties=None, **kwargs):
        self.name = name
        self.systems = systems
        self.nodes = nodes
        self.groups = groups or []
        self.v2Groups = v2Groups or []
        self.userNode = userNode
        self.kind = kind
        self.has_multi_mgmt = False
        self.properties = properties or {}

        self._sys_node_map: typing.Dict[str, typing.List[Node]] = {}
        self._group_node_map: typing.Dict[str, typing.List[Node]] = {}
        self._node_map: typing.Dict[str, Node] = {}
        self._sys_group_map: typing.Dict[str, str] = {}
        self._group_map: typing.Dict[str, NodeGroup] = {}

        self.systems = _sort_systems([
            System(**d) if type(d) != System else d
            for d in self.systems
        ])
        self.nodes = _sort_nodes([
            Node(**d) if type(d) != Node else d
            for d in self.nodes
        ])
        self.groups = _sort_groups([
            NodeGroup(**d) if type(d) != NodeGroup else d
            for d in self.groups
        ])
        self.v2Groups = _sort_groups([
            NodeGroup(**d) if type(d) != NodeGroup else d
            for d in self.v2Groups
        ])
        self.build_index()

    def build_index(self):
        """
        Build indexes:
        1. system name to node list
        2. group name to node list
        3. node name to node
        4. system name to group
        Ungrouped nodes (BR) are assigned to the "*" group.
        """
        if len(self.systems) == 0:
            return
        # group name to system name
        group_sys = {
            g.name: g.properties.get("systemAffinity")
            for g in self.groups
            if g.properties and "systemAffinity" in g.properties
        }
        if len(group_sys) == 0:
            group_sys = {
                g.name: self.systems[i].name
                for i, g in enumerate(self.groups)
                if i < len(self.systems)
            }
        # system name to group name
        sys_group = {
            s: g
            for g, s in group_sys.items()
        }
        # system name to nodes list
        sys_node = {}
        # group name to nodes list
        group_node = {}
        # node name to node mapping
        node_map = {}
        mgmt_count = 0
        for node in self.nodes:
            if node.role == MANAGEMENT:
                mgmt_count += 1
            node_map[node.name] = node
            group = node.properties.get("group", "*")
            system = group_sys.get(group, "*")
            if system not in sys_node:
                sys_node[system] = []
            sys_node[system].append(node)
            if group != "*":
                if group not in group_node:
                    group_node[group] = []
                group_node[group].append(node)
        self._sys_node_map = sys_node
        self._group_node_map = group_node
        self._node_map = node_map
        self._sys_group_map = sys_group
        self._group_map = {g.name: g for g in self.groups}
        self.has_multi_mgmt = mgmt_count > 1

    def is_v2_network(self) -> bool:
        return self.properties.get("topology") == "v2"

    def get_system_nodes(self, system: str = "") -> typing.List[Node]:
        """ Nodes in group with affinity to system. If system arg is empty, then return unassigned (e.g. br) """
        if system and not system in self._sys_node_map:
            raise ValueError(f"system {system} not found")
        return self._sys_node_map.get(system if system else "*", [])

    def summarize_nodegroup(self, name: str) -> dict:
        if name not in self._group_map:
            return {}
        ng = self._group_map[name]
        role_count = collections.Counter([n.role for n in self._group_node_map[name]])
        return {
            "properties": ng.properties,
            "role_counts": {k: v for k, v in role_count.items()},
            "populated": role_count["memory"] >= MIN_MEMX_POPULATED_RACK,
            "crd_node_exists": role_count["management"] > 0 or role_count["worker"] > 1,
        }

    def get_unaffined_systems(self) -> typing.List[str]:
        """ Return: systems where no group has systemAffinity to that group """
        unaffined_systems = list(set([s.name for s in self.systems]) - set(self._sys_group_map.keys()))
        unaffined_systems.sort(key=lambda s: (split_str_int(s)[1], s,), reverse=True)
        return unaffined_systems

    def split_groups(self, additional_group_count: typing.Union[str, int] = "all") -> typing.Tuple[
        typing.List[str], typing.List[str]]:
        """
        Split groups into depopulated (<10 memx, at least 1 worker) groups modifying self.

        If additional_group_count is not set, then the Cluster will be split until no populated goups are left to split
        or all unaffined systems have a group.

        Returns: names of original groups that were split, new groups resulting from the split
        """
        if isinstance(additional_group_count, str):
            if additional_group_count == "all":
                additional_group_count = max(len(self.systems) - len(self.groups), 0)
            else:
                additional_group_count = int(additional_group_count)

        group_splits = []
        group_role_nodes = {}
        for group, nodes in self._group_node_map.items():
            role_nodes = {n.role: [] for n in nodes}
            for n in nodes:
                role_nodes[n.role].append(n)
            nworkers, nmemx = len(role_nodes.get("worker", [])), len(role_nodes.get("memory", []))
            if nworkers > MIN_WORKERS_PER_CV_RACK:  # ignore CV racks
                continue
            # limited by either 4 memx per group or 1 worker per group
            max_group_splits = min(int(nmemx / DEFAULT_DEPOP_RACK_SIZE), nworkers)
            if max_group_splits > 1:
                _, group_index = split_str_int(group)  # nodegroup123 -> 123
                sort_key = (nmemx, -1 * group_index, group,)
                group_splits.append((sort_key, max_group_splits, group))
            group_role_nodes[group] = role_nodes

        # sort split preference by number of memx (split depop racks first),
        # then by group index (split nodegroup10 before nodegroup9), then name as a tie-breaker
        group_splits.sort(key=lambda x: x[0], reverse=True)
        unaffined_systems = self.get_unaffined_systems()

        split_group_names, new_group_names = [], []
        next_group_index = len(self.groups)
        while additional_group_count > 0 and group_splits:
            _, max_group_splits, group = group_splits.pop()
            nbuckets = min(additional_group_count + 1, max_group_splits)
            workers, memx = group_role_nodes[group]["worker"], group_role_nodes[group]["memory"]
            # cosmetic hack: nodes are usually named similar to prefix-racknumber-nodenumber and are in the same rack so sort by nodenumber
            workers.sort(key=lambda n: (split_str_int(n.name)[1], n.name,))
            memx.sort(key=lambda n: (split_str_int(n.name)[1], n.name,))
            worker_splits = bucket(workers, nbuckets)
            memx_splits = bucket(memx, nbuckets)
            for i in range(1, nbuckets):
                new_group = NodeGroup(
                    name=f"nodegroup{next_group_index}",
                    properties={**self._group_map[group].properties, "originalGroup": group},
                    switchConfig=self._group_map[group].switchConfig,
                )
                if unaffined_systems:
                    new_group.properties["systemAffinity"] = unaffined_systems.pop()
                self.groups.append(new_group)

                for node in [*worker_splits[i], *memx_splits[i]]:
                    node.properties["group"] = new_group.name

                new_group_names.append(new_group.name)
                next_group_index += 1
            split_group_names.append(group)
            additional_group_count -= (nbuckets - 1)  # 1 group is not net-new

        self.build_index()
        return split_group_names, new_group_names


ACTIVATION = 'activation'
BROADCAST_REDUCE = 'broadcastreduce'
MEMORY = 'memory'
WORKER = 'worker'
MANAGEMENT = 'management'
INFERENCEDRIVER = 'inferencedriver'
NODE_NAME_MAPPING = {"swarmx": BROADCAST_REDUCE, "memoryx": MEMORY, "generic": WORKER}
NET_DOC_NODES = {
    "activation_nodes": ACTIVATION,
    "swarmx_nodes": BROADCAST_REDUCE,
    "management_nodes": MANAGEMENT,
    "memoryx_nodes": MEMORY,
    "worker_nodes": WORKER,
    "inferencedriver_nodes": INFERENCEDRIVER,
}


def is_v2_network(doc: dict) -> bool:
    if doc.get("environment", {}).get("topology", None) == "leaf_spine":
        return True
    return False


def parse_nic_csport(doc: dict) -> typing.Dict[NIC, int]:
    """
    Uses switch connectivity information to determine the optimal assignment
    of BR NIC to CS Port.
    Arguments:
        doc: The parsed network configuration JSON doc.
    Returns:
        Dict of BR NIC to the CSPort assigned to that NIC.
    """
    if "swarmx_nodes" not in doc:
        return {}
    port_switch = ["" for _ in range(12)]
    for cs in doc.get("systems", []):
        for iface in cs["interfaces"]:
            if not iface["name"].startswith("data"):
                continue
            port = int(iface["name"][4:])
            if port_switch[port] == "":
                port_switch[port] = iface["switch_name"]
            else:
                assert port_switch[port] == iface["switch_name"], \
                    str(
                        f"invalid network: expected exactly 1 switch connected with same CS port {port}: "
                        f"expected switch {port_switch[port]} but got {iface['switch_name']} for "
                        f"system {cs.get('name')}/{iface.get('name')}"
                    )

    switch_ports = {s: [] for s in port_switch}
    for port, s in enumerate(port_switch):
        switch_ports[s].append(port)

    # Scheduling needs at least 3 nics with the same BR Port to minimally schedule BR
    # In the normal case, all 6 NICs of a BR will be assigned to the same CSPort.
    # In the odd case (<6 NIC available at time of deploy), assign in groups of 3 to avoid staggering
    # This could be optimized further to never assign like
    #     nodeA:[port0,port0,port0,port1] and instead assign like
    #     nodeA:[port0,port0,port0,port0]
    switch_nic_3sets = {s: [] for s in port_switch}
    for node in doc.get("swarmx_nodes", []):
        name = node["name"]
        nic_3sets = {}  # switch -> sets of 3 or fewer NICs per node
        for iface in node["interfaces"]:
            addr = iface["address"].split("/")[0]
            switch_name = iface["switch_name"]
            nic = NIC(name, addr, iface["name"])
            if switch_name not in nic_3sets:
                nic_3sets[switch_name] = [[nic]]
            elif len(nic_3sets[switch_name][-1]) < 3:
                nic_3sets[switch_name][-1].append(nic)
            else:
                nic_3sets[switch_name].append([nic])
        for switch_name, nic_3sets in nic_3sets.items():
            switch_nic_3sets[switch_name].extend(nic_3sets)

    nic_port = {}
    for switch, nic_3sets in switch_nic_3sets.items():
        ports = switch_ports[switch]
        nic_3sets_per_port = int(len(nic_3sets) / len(ports))
        i, port_index = 0, 0
        # assign in groups of 3
        while i < len(nic_3sets):
            port_3set = ports[port_index]
            nic_3set = nic_3sets[i]
            for nic in nic_3set:
                nic_port[nic] = port_3set
            i += 1
            if i % nic_3sets_per_port == 0:
                port_index = min(len(ports) - 1, port_index + 1)  # assign extra NICs to last port
    return nic_port


def parse_switches(doc: dict) -> typing.Dict[str, typing.Dict]:
    """switch name => addr mapping"""
    return {
        s["name"]: s
        for s in doc.get("switches", [])
    }


def parse_nodes_switch_mapping(doc: dict) -> typing.Dict[str, str]:
    """
    Returns:
        Map of nodeName -> to its respective switch
    """
    rv = {}
    for node in [*doc.get("memoryx_nodes", []), *doc.get("worker_nodes", []), *doc.get("management_nodes", [])]:
        name = node.get("name", "unknown")
        switch = None
        for iface in node.get("interfaces", []):
            s = iface.get("switch_name", None)
            if switch is None:
                switch = s
            elif switch != s:
                logger.warning(f"node {name} has more than 1 switch: {switch}, {s}")
        if switch is None:
            logger.warning(f"node {name} has no switch, using 'default'")
            switch = 'default'
        rv[name] = switch
    return rv


def _parse_systems(network: dict) -> typing.List[System]:
    rv = []
    for sys in network.get("systems", []):
        if isinstance(sys.get("model"), str):
            system_type = sys["model"].lower()
        else:
            # Set default system type to cs2.
            system_type = "cs2"
            if sys["name"].startswith("systems") or sys["name"].startswith("xs"):
                system_type = "cs3"

        rv.append(System(
            sys["name"],
            system_type,
            sys["vip"].split("/")[0] + ":9000",
            sys.get("default_hostname", None),
            sys.get("management_address", None),
            sys.get("management_user", None),
            sys.get("management_password", None),
            sys.get("nodegroup", -1),
        ))
    return rv


def split_str_int(si: str) -> typing.Tuple[str, int]:
    str_int_match = re.match(r'^.*(\D+)(\d+)$', si)
    if str_int_match:
        return str_int_match[1], int(str_int_match[2])
    return si, -1


def _extract_system_number(sys_name: str) -> str:
    _, index = split_str_int(sys_name)
    if index >= 0:
        return f"{index:-05}"
    return sys_name


def _sort_systems(sys: typing.List[System]) -> typing.List[System]:
    # sort by type, then by name ensuring that the number XX of the systemfXX is the sort key
    sys.sort(key=lambda s: (s.type, _extract_system_number(s.name)))
    return sys


def _sort_groups(gs: typing.List[NodeGroup]) -> typing.List[NodeGroup]:
    def sort_key(g: NodeGroup) -> str:
        return g.name

    gs.sort(key=sort_key)
    return gs


def _sort_nodes(nodes):
    # sort management nodes first, then by role, then by any digits in the name, then by name
    # ('digits' ensures f11 sorts after f7 for instance)
    my_hostname = socket.gethostname()

    def sort_key(node):
        return (
            node.role != "management",
            node.role,
            node.hostname != my_hostname,
            # bubble the host node to the top so that it's later elected as a controlplane
            [int(digits) for digits in re.findall(r'\d+', node.name)],
            node.name,
        )

    nodes.sort(key=sort_key)
    return nodes


def _parse_br_node(doc: dict, nic_csport: typing.Dict[NIC, int], is_v2: bool = False) -> Node:
    """ BR nodes have csPort affinity in v1 network. In v2, the BR gets a reference to its switch/'v2Group' """
    name = doc["name"]
    node = Node(name=name, hostname=name, role=BROADCAST_REDUCE)

    br_switch = ""
    for i in doc.get("interfaces", []):
        ninf = NetworkInterface(name=i["name"],
                                address=i["address"].split("/")[0],
                                gbps=i["gbps"] if i.get("gbps", 100) != 100 else None)

        if not is_v2:
            nic = NIC(name, ninf.address, ninf.name)
            if nic not in nic_csport:
                raise ValueError(f"NIC {nic.name} on node {name} has no CS Port")
            ninf.csPort = nic_csport[nic]
            ninf.v2Group = i["switch_name"]
        else:
            switch_name = i["switch_name"]
            if br_switch and switch_name != br_switch:
                raise ValueError(f"V2 BR node {name} connected with more than 1 switch: {br_switch} {switch_name}")
            br_switch = switch_name

        node.networkInterfaces.append(ninf)

    node.networkInterfaces.sort(key=lambda n: (n.csPort, n.name,))

    if is_v2:
        # explicit separate with existing WGT group to avoid interfering scheduling
        # and also WGT group concept may change
        node.properties["v2Group"] = br_switch

    return node


def _parse_node(doc: dict, role: str, nic_csport: typing.Dict[NIC, int], is_v2: bool = False) -> Node:
    if role == BROADCAST_REDUCE:
        return _parse_br_node(doc, nic_csport, is_v2)

    props = {} if doc.get("nodegroup", "nodegroup-1") == "nodegroup-1" else {"group": str(doc["nodegroup"])}

    # set v2Group in top level props if node shares the same switch for all interfaces
    # else set the v2Group in the NetworkInterface
    always_include_nic_group = role in (ACTIVATION, INFERENCEDRIVER)
    interface_switch = set(i.get("switch_name", "unknown") for i in doc.get("interfaces", []))
    if "unknown" in interface_switch:
        interface_switch.remove("unknown")

    # write leaf switch as v2Group even for v1 networks, for the purpose of `csctl cluster topology` query.
    if len(interface_switch) == 1 and not always_include_nic_group:
        props["v2Group"] = list(interface_switch)[0]

    node = Node(
        name=doc["name"],
        hostname=doc["name"],
        role=role,
        properties=props,
        networkInterfaces=sorted([
            NetworkInterface(
                name=i["name"],
                address=i["address"].split("/")[0],
                gbps=i.get("gbps"),
                v2Group=i["switch_name"] if len(interface_switch) > 1 or always_include_nic_group else None,
            ) for i in doc.get("interfaces", [])
        ], key=lambda n: n.name)
    )

    return node


def _parse_nodes(doc, is_v2) -> typing.List[Node]:
    nic_csport = {}
    if not is_v2:
        nic_csport = parse_nic_csport(doc)
    return flatten([
        [_parse_node(n, role, nic_csport, is_v2) for n in doc.get(k, [])]
        for k, role in NET_DOC_NODES.items()
    ])


def _filter_nodes(nodes: typing.List[Node]) -> typing.List[Node]:
    groups_specified = any([n.properties.get("group", "") != "" for n in nodes])

    rv = []
    for node in nodes:
        if node.role not in [ACTIVATION, BROADCAST_REDUCE] and groups_specified and "group" not in node.properties:
            logger.info(f"filtering node {node.name}: 'properties.group' not set")
            continue

        if not node.networkInterfaces:
            logger.warning(f"filtering node {node.name}: 'networkInterfaces' empty")
            continue

        rv.append(node)
    return rv


def _normalize_nodegroup_names(network: dict):
    """ Renames group: INTEGER to preferred group: nodegroupINTEGER format """
    if not all([isinstance(n.get("nodegroup", 0), int) for k in NET_DOC_NODES.keys() for n in network.get(k, [])]):
        return
    for node_type in NET_DOC_NODES.keys():
        for node in network.get(node_type, []):
            if "nodegroup" in node:
                node["nodegroup"] = f"nodegroup{node['nodegroup']}"


def _patch_node_groups_switch(nodes: typing.List[Node],
                              cluster_prefix: str,
                              node_switch: typing.Dict[str, str],
                              switches: typing.Dict[str, dict],
                              ) -> typing.Tuple[typing.List[Node], typing.List[NodeGroup]]:
    """
    Assigns a nodegroup to each worker/memory/mgmt node if it doesn't already have an assignment.
    If it already has an assignment, it validates that all nodes in the same group share the same switch.
    Additionally, the Nodegroup object is updated with SwitchConfig.
    """

    require_grouping = [n for n in nodes if n.role in {WORKER, MEMORY, MANAGEMENT}]

    node_to_group = {n.name: n.properties["group"] for n in require_grouping if "group" in n.properties}
    if node_to_group:
        # nodegroup assigned in the network doc
        if len(require_grouping) != len(node_to_group):
            raise ValueError(
                "invalid network configuration: "
                f"{len(node_to_group)} out of {len(require_grouping)} memx/worker/mgmt nodes were assigned groups."
                f" Must be all or none")
        switch_to_groups = {}
        group_to_switch = {}
        for n, s in node_switch.items():
            if n not in node_to_group:
                continue
            g = node_to_group[n]

            # handle case where switch has multiple groups
            if s not in switch_to_groups:
                switch_to_groups[s] = [g]
            elif g not in switch_to_groups[s]:
                switch_to_groups[s] = [g, *switch_to_groups[s]]

            # enforce groups mapped to a single switch
            if g in group_to_switch and group_to_switch[g] != s:
                raise ValueError(
                    f"invalid network configuration: group {g}'s nodes assigned to "
                    f"more than one switch (node={n}, switch={s})"
                )
            group_to_switch[g] = s

    else:
        # nodegroup not assigned in the network doc
        switch_to_groups = {s: [f"nodegroup{i}"] for i, s in enumerate(sorted(list(set(node_switch.values()))))}

    switch_configs: typing.Dict[str, SwitchConfig] = {}

    for node in require_grouping:
        switch_name = node_switch.get(node.name, None)
        if not switch_name:
            continue
        groups = switch_to_groups[switch_name]

        if "group" not in node.properties:
            node.properties["group"] = groups[0]

        if switch_name not in switch_configs:
            switch_configs[switch_name] = SwitchConfig.of(cluster_prefix, switches[switch_name])
        switch_configs[switch_name].update(node)

    return nodes, [
        NodeGroup(name=group, properties={"switch": sw}, switchConfig=switch_configs[sw] if switch_configs else None)
        for sw, groups in switch_to_groups.items() for group in groups
    ]


def list_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def _pad_digits(k: str) -> str:
    # sc-15ra17-s9 -> sc-0015ra0017-s009, for sort ordering
    return re.sub(r'\d+', lambda x: x.group(0).zfill(4) if len(x.group(0)) < 4 else x.group(0), k)


def _group_nodes_by_switch(network: dict, k: str) -> typing.Dict[str, typing.List[str]]:
    sn = {}
    for node in network.get(k, []):
        name = node.get("name", "unknown")
        switches = set([
            i["switch_name"] for i in node.get("interfaces", [])
        ])
        if not switches:
            raise ValueError(f"node {name} has no interfaces with 'switch' property, check network_doc")
        elif len(switches) > 1:
            raise ValueError(f"node {name} has connectivity to more than one switch: {switches}")
        else:
            switch = list(switches)[0]
        v = sn.get(switch, [])
        v.append(name)
        sn[switch] = v
    return {s: sorted(nodes, key=_pad_digits) for s, nodes in sn.items()}


def _patch_existing_ng_v2(
        network: dict,
        curr_cluster: Cluster,
        assigned_ng: typing.Set[str],
        nodes: typing.List[Node]
) -> typing.Tuple[typing.List[NodeGroup], typing.List[Node]]:
    """
    Find nodegroups with insufficient memx / wk. If there are any unassigned memx sharing the same switch, assign them
    to the nodegroup. If there are any unassigned wk sharing the same switch, assign them preferentially
    Return list of nodegroups carried forward from the prior reconfig, and list of nodes which still require assignment
    """
    old_ng_nodes = {ng.name: [] for ng in curr_cluster.groups}
    for node in curr_cluster.nodes:
        grp = None if not node.properties else node.properties.get("group", None)
        if grp and grp in old_ng_nodes:
            old_ng_nodes[grp].append(node)
    old_ng = {ng.name: ng for ng in curr_cluster.groups if ng.name in old_ng_nodes}

    seen_groups = {}
    unassigned_nodes = {n.name: n for n in nodes}
    ng_role_count = {(ng, MEMORY): 0 for ng in old_ng_nodes.keys()}
    ng_role_count.update({(ng, WORKER): 0 for ng in old_ng_nodes.keys()})
    # assumes prior config didn't change the switch that a nodegroup was assigned to
    for ng, prev_nodes in old_ng_nodes.items():
        for n in prev_nodes:
            if n.name not in unassigned_nodes:
                continue
            seen_groups[ng] = old_ng[ng]
            unassigned_nodes[n.name].properties["group"] = ng
            k = (ng, unassigned_nodes[n.name].role,)
            ng_role_count[k] = ng_role_count.get(k, 0) + 1
            del unassigned_nodes[n.name]

    # assign nodes sharing the same switch to nodegroups without enough memx/worker
    switch_mx = _group_nodes_by_switch(network, "memoryx_nodes")
    switch_wk = _group_nodes_by_switch(network, "worker_nodes")
    ng_needs_wk = []

    # Pack order: groups with some mx, groups which are unassigned, groups which have more mx, name.
    # Must pack groups with some mx first, otherwise there could be fragmentation: groups which never get full mx
    # Prefer to pack groups which are unassigned to sessions first to avoid changing sessions in progress
    # Prefer to pack groups with more mx first since they're more likely to be full at the end of the cycle
    mx_count_ng = sorted(
        [(c != 0, ng not in assigned_ng, c, ng) for (ng, role), c in ng_role_count.items() if role == MEMORY],
        reverse=True
    )
    for _, _, mx_count, ng in mx_count_ng:
        sw = old_ng[ng].properties.get("v2Group", None)
        candidates = switch_mx.get(sw, []).copy()
        while mx_count < MEMX_PER_POP_RACK_V2 and candidates:
            n = candidates.pop(0)
            if n in unassigned_nodes:
                unassigned_nodes[n].properties["group"] = ng
                del unassigned_nodes[n]
                mx_count += 1

        if ng_role_count[(ng, WORKER,)] == 0:  # needs a worker
            candidates = switch_wk.get(sw, []).copy()
            while candidates:
                n = candidates.pop(0)
                if n in unassigned_nodes:
                    unassigned_nodes[n].properties["group"] = ng
                    del unassigned_nodes[n]
                    break
            else:
                ng_needs_wk.append(ng)

    # failed to find a worker sharing the same switch, try to get a worker from anywhere
    for sw in sorted(switch_wk.keys(), key=_pad_digits):
        wks = switch_wk.get(sw, [])
        while ng_needs_wk and wks:
            worker = unassigned_nodes.get(wks.pop(0), None)
            if not worker:
                continue
            worker.properties["group"] = ng_needs_wk.pop(0)

    return list(seen_groups.values()), list(unassigned_nodes.values())


def _assign_node_groups_v2(
        network: dict, nodes: typing.List[Node], nodegroups: typing.List[NodeGroup]
) -> typing.List[NodeGroup]:
    """
    v2 nodegroup allocation:
    - split memx+worker sharing same leaf into groups of 12 + 1
    - split remaining workers into groups of 1 (v2 depop groups)
    for each nodegroup, assign the prop.v2Group -> leaf switch that group connection

    If a complete group cannot be made, assume this is during the initial bring up process and
    warn log but don't prohibit.
    """
    groups = {g.name: g for g in nodegroups}
    nodes = {n.name: n for n in nodes}
    ng_name_i = 0

    def new_ng_name() -> str:
        nonlocal ng_name_i
        nonlocal groups
        name = f"nodegroup{ng_name_i}"
        while name in groups:
            ng_name_i += 1
            name = f"nodegroup{ng_name_i}"
        return name

    def assign_wks(sw: str, wks: typing.List[Node]):
        for wk in wks:
            group = NodeGroup(name=new_ng_name(), properties={"v2Group": sw}, )
            groups[group.name] = group
            wk.properties["group"] = group.name

    switch_mx = _group_nodes_by_switch(network, "memoryx_nodes")
    switch_wk = _group_nodes_by_switch(network, "worker_nodes")

    ng_needs_wk = []
    for sw in sorted(switch_mx.keys(), key=_pad_digits):
        unassigned_mx = [nodes[n] for n in switch_mx[sw] if n in nodes]
        unassigned_wk = [nodes[n] for n in switch_wk.get(sw, []) if n in nodes]
        for mx_chunk in list_chunks(unassigned_mx, MEMX_PER_POP_RACK_V2):
            group = NodeGroup(
                name=new_ng_name(),
                properties={"v2Group": sw},
            )
            if len(mx_chunk) < MEMX_PER_POP_RACK_V2:
                logger.warning(f"insufficient memx for populated group {group.name}: {len(mx_chunk)} memx assigned")
            groups[group.name] = group
            for mx in mx_chunk:
                mx.properties["group"] = group.name
            if unassigned_wk:
                wk = unassigned_wk.pop(0)
                wk.properties["group"] = group.name
            else:
                ng_needs_wk.append(group.name)

        # update worker entry for final wk assignment
        switch_wk[sw] = [n.name for n in unassigned_wk]

    for sw in sorted(switch_wk.keys(), key=_pad_digits):
        # first, assign any memx ng which need a worker
        wks = switch_wk.get(sw, [])
        while ng_needs_wk and wks:
            group = ng_needs_wk.pop(0)
            worker = nodes[wks.pop(0)]
            worker.properties["group"] = group
        assign_wks(sw, [nodes[n] for n in switch_wk.get(sw, []) if n in nodes])

    return list(groups.values())


def _parse_cluster_properties(doc: dict) -> dict:
    """ Extract cluster.yaml::properties from network json doc """
    props = {}
    if is_v2_network(doc):
        props["topology"] = "v2"

    env = doc.get("environment")
    if not env:
        return props
    if "service_prefix" in env:
        props["serviceSubnet"] = env["service_prefix"]
    return props


def _add_nodegroup_system_affinity(systems: typing.List[System], nodegroups: typing.List[NodeGroup],
                                   curr_cluster_config: typing.Optional[Cluster] = None):
    nodegroup_to_system = {}
    used_systems = set()
    # If this is an existing cluster, we honor the systemAffinity assignment in the existing
    # cluster.yaml.
    if curr_cluster_config:
        system_names = set([system.name for system in systems])
        for group in curr_cluster_config.groups:
            if "systemAffinity" in group.properties:
                # If `systemAffinity` system is not in `systems` list any more, it means that
                # there is a system swap. We should not use this system for the nodegroup
                # any more.
                if group.properties["systemAffinity"] in system_names:
                    nodegroup_to_system[group.name] = group.properties["systemAffinity"]
                    used_systems.add(group.properties["systemAffinity"])

        for system in systems:
            if not system.nodegroup:
                continue
            if system.nodegroup not in nodegroup_to_system and system.name not in used_systems:
                nodegroup_to_system[system.nodegroup] = system.name
                used_systems.add(system.name)

    else:
        for system in systems:
            if not system.nodegroup:
                continue
            nodegroup_to_system[system.nodegroup] = system.name
            used_systems.add(system.name)

    for i, nodegroup in enumerate(nodegroups):
        system_name = ""
        if nodegroup.name in nodegroup_to_system:
            system_name = nodegroup_to_system[nodegroup.name]
        else:
            for system in systems:
                if system.name not in used_systems:
                    system_name = system.name
                    used_systems.add(system_name)
                    break
        if system_name != "":
            if not nodegroup.properties:
                nodegroup.properties = {}
            nodegroup.properties["systemAffinity"] = system_name


def _mark_controlplane_nodes(nodes: typing.List[Node], curr_cluster_config: typing.Optional[Cluster] = None):
    """ Mark relevant management nodes as control plane nodes. For a new cluster,
        mark the first 3 management nodes as controlplane nodes, assumes nodes already sorted.
        For an existing cluster, preserve the existing control plane nodes.
    """
    if not curr_cluster_config:
        # Mark the first 3 management nodes as control plane nodes.
        mgmt_nodes = [n for n in nodes if n.role == "management"]
        assert len(mgmt_nodes) <= 1 or len(mgmt_nodes) >= 3, "number of management nodes is 0 or 1 or not less than 3"
        for node in mgmt_nodes[:3]:
            node.properties["controlplane"] = "true"
    else:
        control_plane_nodes = []
        for node in curr_cluster_config.nodes:
            if node.role == "management" and "controlplane" in node.properties:
                control_plane_nodes.append(node.name)
        for node in [n for n in nodes if n.role == "management"]:
            if node.name in control_plane_nodes:
                node.properties["controlplane"] = "true"


def _mark_ceph_nodes(nodes: typing.List[Node], curr_cluster_config: typing.Optional[Cluster] = None):
    """ Mark relevant management nodes as ceph nodes. For a new cluster,
        mark the first 4 management nodes as ceph nodes, assumes nodes already sorted.
        For an existing cluster, preserve the existing ceph nodes.
    """
    if not curr_cluster_config:
        # Mark the first 4 management nodes as ceph nodes.
        mgmt_nodes = [n for n in nodes if n.role == "management"]
        assert len(mgmt_nodes) <= 1 or len(mgmt_nodes) >= 3, "number of management nodes is 0 or 1 or not less than 3"
        if len(mgmt_nodes) >= 3:
            for node in mgmt_nodes[:4]:
                node.properties["storage-type"] = "ceph"
    else:
        ceph_nodes = []
        for node in curr_cluster_config.nodes:
            if node.role == "management" and "storage-type" in node.properties:
                ceph_nodes.append(node.name)
        for node in [n for n in nodes if n.role == "management"]:
            if node.name in ceph_nodes:
                node.properties["storage-type"] = "ceph"


def _mark_nvme_of_targets(nodes: typing.List[Node], curr_cluster_config: typing.Optional[Cluster] = None):
    """
    For an existing cluster, preserve the existing nvme-of targets.
    """
    if curr_cluster_config:
        nvme_of_targets = {}
        nvme_of_capacities = {}
        for node in curr_cluster_config.nodes:
            if node.role == "management" and "nvme-of-target-id" in node.properties:
                nvme_of_targets[node.name] = node.properties["nvme-of-target-id"]
                nvme_of_capacities[node.name] = node.properties["nvme-per-disk-bytes"]
        for node in [n for n in nodes if n.role == "management"]:
            if node.name in nvme_of_targets:
                node.properties["nvme-of-target-id"] = nvme_of_targets[node.name]
                node.properties["nvme-per-disk-bytes"] = nvme_of_capacities[node.name]


def _mark_nvme_of_initiators(nodes: typing.List[Node], curr_cluster_config: typing.Optional[Cluster] = None):
    """
    For an existing cluster, preserve the existing nvme-of initiators.
    """
    if curr_cluster_config:
        nvme_of_initiators = {}
        for node in curr_cluster_config.nodes:
            if node.role == "memory" and "nvme-of-initiator" in node.properties:
                nvme_of_initiators[node.name] = node.properties["nvme-of-initiator"]
        for node in [n for n in nodes if n.role == "memory"]:
            if node.name in nvme_of_initiators:
                node.properties["nvme-of-initiator"] = nvme_of_initiators[node.name]


def _fix_nodegroup(nodes: typing.List[Node], curr_cluster_config: typing.Optional[Cluster] = None):
    """
    Fix nodes' nodegroup assignment based on the existing cluster.
    """
    if not curr_cluster_config:
        return

    node_to_nodegroup = {
        node.name: node.properties["group"]
        for node in curr_cluster_config.nodes
        if node.role not in [ACTIVATION, BROADCAST_REDUCE] and "group" in node.properties
    }

    for node in nodes:
        if node.name in node_to_nodegroup:
            node.properties["group"] = node_to_nodegroup[node.name]


def _add_manually_split_nodegroups(nodegroups: typing.List[NodeGroup],
                                   curr_cluster_config: typing.Optional[Cluster] = None):
    """
    Add manually split nodegroups into groups.
    """
    if not curr_cluster_config:
        return

    nodegroup_names = set([g.name for g in nodegroups])
    for group in curr_cluster_config.groups:
        if group.name not in nodegroup_names:
            nodegroups.append(group)


def _ensure_system_names(systems: typing.List[System]):
    """ Ensure system names lowercase and are capable of being k8s object names """
    name_re = re.compile(NAME_RE)
    for s in systems:
        s.name = s.name.lower()
        if name_re.match(s.name):
            continue
        raise ValueError(
            f"system {s.name} has an invalid name, must be a lowercase "
            f"RFC 1123 subdomain (e.g. match regex {NAME_RE})"
        )


def _parse_v2_groups(network: dict) -> typing.List[NodeGroup]:
    v2_groups = {}
    for sw in network.get("switches", []):
        if sw.get("tier") != "SP":
            v2_groups[sw["name"]] = NodeGroup(
                name=sw["name"],
                vlans=Vlan.from_switch(sw, _cluster_prefix(network)),
                properties={},
            )

    # not using "system_connections" because the port is at 1-12 instead of 0-11
    for cs in network.get("systems", []):
        system = cs['name']
        for iface in cs["interfaces"]:
            if not iface["name"].startswith("data"):
                continue
            switch = iface["switch_name"]
            port = int(iface["name"][4:])
            if switch not in v2_groups:
                raise ValueError(f"system interface {system}/{iface['name']} connected to switch {switch} "
                                 " that doesn't appear in .switches[] in network.json doc")
            v2_groups[switch].properties[f"{system}-{port}"] = ""
    return sorted(list(v2_groups.values()), key=lambda s: s.name)


def _cluster_prefix(doc: dict) -> str:
    v = doc.get("environment", {}).get("cluster_prefix")
    if not v:
        raise ValueError("field 'environment.cluster_prefix' in network json file can not be empty/none")
    return v


def _find_assigned_ng_k8s() -> typing.Set[str]:
    """
    Return list of nodegroups assigned to all namespaces in k8s.
    Implies these nodegroups' memberships must be preserved
    """
    try:
        stdout = subprocess.check_output("kubectl get nsr -ojson".split(" "))
        output = json.loads(stdout.decode())
        assigned_ng = set()
        for ng in output.get("items", []):
            assigned_ng = assigned_ng.union({*ng["status"].get("nodegroups", [])})
        return assigned_ng
    except subprocess.CalledProcessError:
        return set()


def _log_group_counts_v2(nodes: typing.List[Node], sys_count: int):
    mx_count = collections.Counter([(n.properties.get("group", "")) for n in nodes if n.role == MEMORY])
    wk_count = collections.Counter([(n.properties.get("group", "")) for n in nodes if n.role == WORKER])
    pop_count, depop_count, nonstandard = 0, 0, []
    for group, count in mx_count.items():
        wk = wk_count.get(group, 0)
        if count == MEMX_PER_POP_RACK_V2 and wk == 1:
            pop_count += 1
        else:
            nonstandard.append(f"{group}(mx={count},wk={wk})")
        if wk:
            del wk_count[group]
    for group, count in wk_count.items():
        if count == 1:
            depop_count += 1
        else:
            nonstandard.append(f"{group}(mx=0,wk={count})")
    logger.info(
        f"cluster has {pop_count} populated groups, {depop_count} depopulated groups, "
        f"{len(nonstandard)} non standard groups: [{nonstandard}]"
    )
    group_count = pop_count + depop_count + len(nonstandard)
    if sys_count > group_count:
        logger.warning(f"cluster has fewer nodegroups than systems, ({group_count} < {sys_count}). "
                       "Not all systems will be able to run jobs concurrently! Check cluster config")


def parse_cluster_config(network: dict, curr_cluster_config: typing.Optional[Cluster] = None) -> Cluster:
    """
    Parse a network.json file into a cluster-mgmt Cluster object.
    """
    is_v2 = is_v2_network(network)
    _normalize_nodegroup_names(network)
    nodes = _parse_nodes(network, is_v2)
    nodes = _filter_nodes(nodes)
    _sort_nodes(nodes)

    systems = _sort_systems(_parse_systems(network))

    v2_groups = None
    if not is_v2 or not any(n.role == ACTIVATION for n in nodes):
        # v1 network or v2 with no activation servers
        nodes, groups = _patch_node_groups_switch(
            nodes,
            _cluster_prefix(network),
            parse_nodes_switch_mapping(network),
            parse_switches(network)
        )
        # not used in 2.2+, should remove systemAffinity property in 2.4
        _add_nodegroup_system_affinity(systems, groups, curr_cluster_config)
        _fix_nodegroup(nodes, curr_cluster_config)
        _add_manually_split_nodegroups(groups, curr_cluster_config)

    else:
        # v2 network with activation servers
        preserve_ng_assign = _find_assigned_ng_k8s()
        if preserve_ng_assign and curr_cluster_config:
            old_ng, unassigned_nodes = _patch_existing_ng_v2(network, curr_cluster_config, preserve_ng_assign, nodes)
            groups = _assign_node_groups_v2(network, unassigned_nodes, old_ng)
        else:
            groups = _assign_node_groups_v2(network, nodes, [])
        _log_group_counts_v2(nodes, len(systems))

    # populate v2 group info even for v1 networks, for the purpose of `csctl cluster topology` query.
    # this should be safe, as all scheduling decisions should be guarded by the global topology property.
    v2_groups = _parse_v2_groups(network)

    _mark_controlplane_nodes(nodes, curr_cluster_config)
    _mark_ceph_nodes(nodes, curr_cluster_config)
    _mark_nvme_of_targets(nodes, curr_cluster_config)
    _mark_nvme_of_initiators(nodes, curr_cluster_config)
    _ensure_system_names(systems)

    return Cluster(
        name=network.get("name"),
        properties=_parse_cluster_properties(network),
        systems=systems,
        nodes=nodes,
        groups=groups,
        v2Groups=v2_groups,
    )
