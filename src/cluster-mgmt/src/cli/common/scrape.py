import json
import ipaddress
import logging
import typing
import re

from common.models import (
    bucket,
    BROADCAST_REDUCE,
    MEMORY,
    NODE_NAME_MAPPING,
    Cluster,
    NetworkInterface,
    Node,
    NodeGroup,
    System,
    SwitchConfig,
    _add_nodegroup_system_affinity,
    _mark_controlplane_nodes,
    _sort_nodes,
    _sort_systems,
)

from common import flat_map

logger = logging.getLogger("common.scrape")

# Cerebras system type naming conventions
_SYS_NAME_RE_TYPE = {
    re.compile(r'^system.*'): "cs2",
    re.compile(r'^xs1\d+'): "cs3",
    re.compile(r'^(vs|vh).*'): "verbier",
    re.compile(r'^(kapi|compilepod|wsperfdrop).*'): "mock"
}


def _scrape_system(system: str, ssh: typing.Callable[[str], str]) -> System:
    """
    Scrape system network info. System must be reachable and in an OK health
    state as a defense against gathering stale information
    """
    system_type = ""
    for pattern, t in _SYS_NAME_RE_TYPE.items():
        if pattern.match(system):
            system_type = t
            break
    if not system_type:
        logger.warning(f"could not infer system type from name, assuming mock system: {system}")
        system_type = "mock"

    if system_type == "mock":
        return System(
            name=system,
            type="cs2",
            controlAddress="~ws_compile_only~:0",
            managementAddress='',
        )

    system = system.replace("-", "")
    rv = System(name=system, type=system_type, controlAddress="", managementAddress="")
    rv.managementAddress = ssh("hostname -i").strip()

    n = json.loads(ssh("cs config network show --output-format json"))
    try:
        ctr_addrs = [c["value"] for c in n["dataStaticKeyVal"] if c["config"].startswith("Control IP")]
        if ctr_addrs:
            rv.controlAddress = ctr_addrs[0].split("/")[0] + ":9000"
    except:
        logger.warning("unable to parse config network show output, trying network show...")

    if not rv.controlAddress:
        n = json.loads(ssh("cs network show --output-format json"))
        ctr_addrs = [c["ipAddress"].split("/")[0] for c in n["data"] if c["ifaceName"] == "CTRL"]
        if ctr_addrs:
            rv.controlAddress = ctr_addrs[0] + ":9000"
    return rv


def _parse_nodes_from_cluster_doc(doc) -> typing.List[Node]:
    role_node = {
        NODE_NAME_MAPPING.get(k[:-6], k[:-6]): v for k, v in doc.items()
        if k.endswith("_hosts") and not k.startswith("user_")
    }

    def strip_name(host):
        return host.strip().replace(".cerebrassc.local", "")

    return flat_map(lambda x: x, [
        [Node(name=strip_name(host), hostname=strip_name(host), role=role) for host in hosts]
        for role, hosts in role_node.items()
    ])


def _scrape_node_network_interfaces(node: Node, ssh: typing.Callable[[str], str]):
    """Scrape NIC info. Try to find csPort subnet assoc. Only keep 100G links."""
    node.networkInterfaces = []
    # append directly if nics already parsed from netfile
    try:
        netinf = ssh('ip -j a')
    except Exception as e:
        raise RuntimeError(f"node scrape failed: {node.hostname} exec 'ip -j a': {e}")
    for inf in json.loads(netinf):
        if inf.get("mtu", 0) != 9000 or len(inf.get("addr_info", [])) == 0:
            continue
        addr_info = inf['addr_info'][0]
        if addr_info.get("scope") != "global":
            continue
        name, addr = addr_info['label'], addr_info['local']
        node.networkInterfaces.append(NetworkInterface(
            name=name,
            address=addr,
        ))
        if node.role not in [BROADCAST_REDUCE, MEMORY]:
            # only need the first 100G interface for non-br and non-memory nodes
            return


def _patch_node_cs_ports_rr(nodes: typing.List[Node]) -> typing.List[Node]:
    """for test clusters: assign csPorts round-robin to BR nodes"""
    brs = [node for node in nodes if node.role == BROADCAST_REDUCE]
    ports_total = sum(len(node.networkInterfaces) for node in brs)
    alloc, i, allocate = 0, 0, int(ports_total / 12)
    for br in brs:
        for inf in br.networkInterfaces:
            inf.csPort = i
            alloc = 0 if alloc + 1 == allocate else alloc + 1
            i = i if alloc else (i + 1) % 12
    return nodes


def _patch_node_groups_rr(nodes: typing.List[Node], n: int) -> typing.List[Node]:
    """round robin assignment is used in small clusters where there is only 1 switch"""
    role_nodes = {node.role: [] for node in nodes}
    for node in nodes:
        role_nodes[node.role].append(node)

    def set_group(role):
        for idx, node_bucket in enumerate(bucket(role_nodes.get(role, []), n)):
            for node in node_bucket:
                node.properties["group"] = f"nodegroup{idx}"

    set_group("worker")
    set_group("memory")
    set_group("management")
    return nodes


def _find_largest_subnet(exechost: typing.Callable[[str, str], str], host, inf) -> ipaddress.IPv4Network:
    """ Assuming each node has same routes, identify the largest subnet associated with 100G """
    rv = exechost(host, f"ip route show dev {inf}")
    subnets = [
        ipaddress.ip_network(r.split(" ")[0])
        for r in rv.split("\n")
        if "/" in r.split(" ")[0]
    ]
    if not subnets:
        raise ValueError(f"host {host} had no routes for interface {inf}, skipping switch config inference")
    sorted(subnets, key=lambda k: k.num_addresses)
    return subnets[-1]


def _find_virtual_range(
        exechost: typing.Callable[[str, str], str],
        nodes: typing.List[Node],
        subnet: ipaddress.IPv4Network,
) -> typing.Tuple[str,str]:
    """
    Identify the largest 100G IP of nodes, then probe the network for reachability of remaining IPs
    in the subnet. Colo racks are /22 so this command won't be that big.
    """
    host = nodes[0].hostname

    exechost(host, "rm -rf /tmp/addrs")
    max_ip = max([ipaddress.ip_address(intf.address) for n in nodes for intf in n.networkInterfaces])

    # search over a max of 256 addrs at a time
    for net in subnet.subnets(new_prefix=24):
        cmds = []
        for addr in net.hosts():
            if addr >= max_ip:
                cmds.append(f"{{ ping -c1 -w2 {addr} &>/dev/null && echo {addr} >> /tmp/addrs ; }}")
        if cmds:
            exechost(host, " & ".join(cmds) + " & wait")

    # used_addrs should be bounded by the bottom and top addr in the subnet
    used_addrs = [max_ip, subnet.broadcast_address]
    used_addrs.extend([ipaddress.ip_address(a) for a in exechost(host, "cat /tmp/addrs").split("\n") if a])
    used_addrs = sorted(used_addrs)

    # find the longest gap in used addrs
    lo, hi, max_dist = -1, -1, -1
    for i, a in enumerate(used_addrs[:-1]):
        dist = int(used_addrs[i+1]) - int(a)
        if max_dist < dist:
            max_dist, lo, hi = dist, a, used_addrs[i+1]

    # Assume for 1 cs cluster -> nginx + 1 compile job + 1 train job.
    # Train jobs can have arbitrarily number of pods but realistically, the
    # largest is 60 ACT + 22 WGT + CMD + CRD + CHF. Therefore 100 is a rough
    # estimate of the min required 100G IPs.
    min_required_pods = 100

    # Hack for singlebox clusters - we know they're a /22 and we know that ansible
    # allocates in steps of 16 and that there's likely no more than 22 nodes per
    # rack (1 mgmt, 8 workers, 12 memx, 1 usernode), so we can always choose a lower
    # bound starting virtual IP based on this logic.
    if subnet.prefixlen == 22:
        max_nodes = 22
        ip_alloc_interval = 16  # IT allocates in steps of 16, e.g. nodeA 10.0.0.0, nodeB 10.0.0.16, ...
        lower_bound = subnet.network_address + (max_nodes * ip_alloc_interval)
        if lo < lower_bound < (hi - min_required_pods):
            lo = lower_bound
            logger.info(f"setting virtual IP range to lower bound {lower_bound}")

    if max_dist - 2 < min_required_pods:
        raise ValueError(f"unable to find a virtual range of at least {min_required_pods} addresses")
    return str(lo+1), str(hi-1)


def _scrape_group_switchconfig(
        exechost: typing.Callable[[str, str], str],
        nodes: typing.List[Node],
        groups: typing.List[NodeGroup],
):
    if len(groups) != 1:
       raise ValueError("multiple node groups, skip inferring switch config for cluster")
    if len(nodes) == 1:
        # compile pods share racks with other compilepods which breaks the assumption of this scraping logic
        raise ValueError("single node cluster, skip inferring switch config")

    subnet = _find_largest_subnet(
        exechost,
        nodes[0].hostname,
        nodes[0].networkInterfaces[0].name,
    )
    vstart, vend = _find_virtual_range(exechost, nodes, subnet)

    replica_nic = {
        n.role: n.networkInterfaces[0].name
        for n in nodes
        if n.networkInterfaces
    }

    groups[0].switchConfig = SwitchConfig(
        subnet=str(subnet),
        gateway="0.0.0.0",
        parentnet=str(subnet),
        virtualStart=vstart,
        virtualEnd=vend,
        memoryNIC=replica_nic.get("memory"),
        workerNIC=replica_nic.get("worker"),
        managementNIC=replica_nic.get("management"),
    )
    logger.info(f"inferred switch config: {groups[0].switchConfig.to_json()}")


def _validate_system(system: System, groups: typing.List[NodeGroup]) -> System:
    """ Validate that CM addr of single box cluster is within subnet of switch. """
    if len(groups) == 1 and groups[0].switchConfig is not None:
        try:
            addr = ipaddress.ip_address(system.controlAddress.split(":")[0])
            subnet = ipaddress.ip_network(groups[0].switchConfig.subnet)
            assert addr in subnet, \
                f"invalid network: {system.name} cm addr was not contained in switch subnet {subnet}"
        except ValueError as e:
            logger.warning(f"unparsable subnet / cm addr: {e}")
    return system


def _replace_names(nodes: typing.List[Node], systems: typing.List[System], groups: typing.List[NodeGroup]):
    """
    Replace names for nodes/systems to a short name.
    This is useful when systemDB has domain names embedded in node names, but our deployment script is
    expecting a short name.
    """
    for node in nodes:
        dot_index = node.name.find(".")
        if dot_index != -1:
            node.name = node.name[:dot_index]

    for system in systems:
        dot_index = system.name.find(".")
        if dot_index != -1:
            system.name = system.name[:dot_index]

    for group in groups:
        dot_index = group.properties['systemAffinity'].find(".")
        if dot_index != -1:
            group.properties['systemAffinity'] = group.properties['systemAffinity'][:dot_index]


def scrape_cluster_config(cluster: dict, exechost: typing.Callable[[str, str], str], use_short_name: bool = False) -> Cluster:
    """
    Parse a devinfa mongo db cluster file into a cluster-mgmt Cluster object.
    Scrapes some information from hosts. Assigns CS ports using round-robin assignments.
    Best effort: may generate invalid config.
    Arguments:
        cluster: cluster object from devinfra/mongo
        exechost: ssh exec on (host,cmd) -> stdout
    """
    nodes = _parse_nodes_from_cluster_doc(cluster)
    for node in nodes:
        _scrape_node_network_interfaces(node, lambda cmd: exechost(node.name, cmd))
    _patch_node_cs_ports_rr(nodes)
    _sort_nodes(nodes)

    sys = cluster.get("systems", [])
    if not sys:
        logger.info("No systems in config, adding a kapi system to enable drop flow")
        sys.append("kapi0")

    _patch_node_groups_rr(nodes, len(sys))
    groups = [NodeGroup(name=f"nodegroup{i}") for i in range(len(sys))]
    try:
        _scrape_group_switchconfig(exechost, nodes, groups)
    except ValueError as e:
        logger.warning(f"failed to infer switch config, reason: {e}. Skipping switchconfig")

    systems = []
    for system_name in sys:
        system = _scrape_system(system_name, lambda cmd: exechost(system_name, cmd, is_system=True))
        systems.append(_validate_system(system, groups))
    systems = _sort_systems(systems)

    _add_nodegroup_system_affinity(systems, groups)
    _mark_controlplane_nodes(nodes)

    if use_short_name:
        _replace_names(nodes, systems, groups)

    return Cluster(
        name=cluster.get("name"),
        systems=systems,
        nodes=nodes,
        groups=groups,
    )
