import json
import logging
import pathlib
import re
import shlex
import subprocess
import typing
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

import jsonschema
import yaml

import common
import common.cluster
import common.models
from common.context import CliCtx
from common.models import Cluster

# Number of pings to send to validate connectivity.
_PING_COUNT = 5

# How long to wait for replies in ping test.
_PING_TTL_SECONDS = 1

# Maximum CS port.
_MAX_CS_PORT = 11

# Minimum number of NICs on a BR node.
_BR_MIN_NICS = 6

logger = logging.getLogger("cs_cluster.validate")


class _ProblemLog:
    """Tracks problems discovered while validating a config."""

    def __init__(self, initial_problems: Optional[List[str]] = None) -> None:
        self.problems = initial_problems or []

    def log(self, description: str):
        self.problems.append(description)


def validate_cluster_schema(cluster_doc: dict) -> Optional[jsonschema.ValidationError]:
    try:
        schema = json.loads(pathlib.Path(__file__).parent.resolve(
        ).joinpath("schemas").joinpath("cluster-schema.json").read_text())
        jsonschema.validate(cluster_doc, schema)
    except jsonschema.ValidationError as e:
        return e


def _get_cluster_config(controller: common.cluster.ClusterController, system_namespace: str) -> Cluster:
    """
    Returns the cluster configuration in use by the K8s cluster.
    Note that we use the cluster config that is actually deployed.
    :param ctx: the controller of the context.
    """
    _, out, _ = controller.exec(
        "kubectl get configmaps/cluster -o jsonpath='{.data.clusterConfiguration\\.yaml}' -n" + system_namespace,
    )
    return Cluster(**yaml.safe_load(out))


def _get_cluster_nodes(controller: common.cluster.ClusterController) -> Set[str]:
    """
    Returns the names of nodes in the k8s cluster.
    :param controller: the controller to access the k8s cluster.
    """
    _, out, _ = controller.exec(
        "kubectl get nodes -o jsonpath='{.items[*].metadata.name}'",
    )
    return set(out.split(' '))


def _get_cluster_nodes_to_labels(controller: common.cluster.ClusterController) -> Dict[str, Dict[str, Any]]:
    """
    Returns a mapping of node names to their labels.
    :param controller: the controller to access the k8s cluster.
    """
    _, out, _ = controller.exec("kubectl get nodes -o json")
    nodes = json.loads(out)
    return {
        node['metadata']['name']: node['metadata']['labels']
        for node in nodes['items']
    }


def _get_duplicate_addresses(cluster: Cluster) -> Sequence[Sequence[common.models.Node]]:
    """
    Returns nodes with duplicate IP addresses.
    :param cluster: the parsed cluster configuration.
    """
    ips_to_nodes: Dict[str, List[common.models.Node]] = {}
    for node in cluster.nodes:
        for interface in node.networkInterfaces:
            nodes_with_ip = ips_to_nodes.get(interface.address, [])
            ips_to_nodes[interface.address] = nodes_with_ip + [node]
    return [nodes for _, nodes in ips_to_nodes.items() if len(nodes) > 1]


def _get_unreachable_addresses(controller: common.cluster.ClusterController, cluster: Cluster) -> Sequence[str]:
    """
    Returns IPs of NICS unreachable from the master.
    :param controller: the CLI context's cluster controller.
    cluster: the parsed cluster configuration.
    """
    addresses = []
    for node in cluster.nodes:
        for iface in node.networkInterfaces:
            addresses.append(iface.address)
    _, stdout, _ = controller.exec(_get_unreachability_script(addresses))
    return stdout.splitlines()


def _get_unreachable_hosts(cluster: Cluster) -> Sequence[str]:
    """
    Returns hostnames of nodes unreachable from the machine the script is running on.
    cluster: the parsed cluster configuration.
    """
    try:
        cmd = _get_unreachability_script([n.hostname for n in cluster.nodes])
        stdout = subprocess.check_output(shlex.split(cmd))
    except subprocess.CalledProcessError as e:
        return str(e)
    return str(stdout, 'utf-8').splitlines()


def _get_unreachability_script(addresses: typing.List[str]) -> str:
    """This script writes unreachable addresses to stdout. We don't use "nice" shell features for portability."""
    unreachability_script = f"""
{
    "".join(
        [
            f"ping -W {_PING_TTL_SECONDS} -c {_PING_COUNT} {shlex.quote(addr)} >/dev/null 2>/dev/null & "
            f"pid_{n}=$!;"
            for n, addr in enumerate(addresses)
        ],
    )
    }
{
    "".join(
        [
            f"wait $pid_{n};"
            f"if [ $? -ne 0 ]; then echo {shlex.quote(addr)}; fi;"
            for n, addr in enumerate(addresses)
        ],
    )
    }
true;
    """.strip()
    return "sh -c {}".format(shlex.quote(unreachability_script))


def _get_duplicate_nodes(cluster: Cluster) -> Sequence[Sequence[common.models.Node]]:
    """
    Returns a sequence of nodes that appear multiple times in the cluster.
    :param cluster: the cluster to find duplicate nodes in.
    """
    node_names_to_node: Dict[str, List[common.models.Node]] = {}
    for node in cluster.nodes:
        node_names_to_node[node.name] = node_names_to_node.get(
            node.name,
            [],
        ) + [node]
    return [nodes for _, nodes in node_names_to_node.items() if len(nodes) > 1]


def _get_missing_node_roles(cluster: Cluster) -> Set[str]:
    """
    Returns the node roles that do not appear in the cluster.
    :param cluster: the cluster to look at roles in.
    """
    present_roles = set([node.role for node in cluster.nodes])
    if "any" in present_roles:
        return set()

    required_roles = {"management", "worker", "memory"}
    if len(cluster.systems) > 1:
        required_roles.add("broadcastreduce")

    return required_roles.difference(present_roles)


def _get_nodes_with_non_existant_csPorts(cluster: Cluster) -> Sequence[common.models.Node]:
    """
    Returns nodes containing NICs that reference CSPorts that do not exist.
    :param cluster: the cluster to look at NICs in.
    """
    nodes = []
    if cluster.is_v2_network():
        return nodes  # v2 doesn't assign csPort affinity
    for node in cluster.nodes:
        if node.role not in {"any", "broadcastreduce"}:
            continue
        for nic in node.networkInterfaces:
            if int(nic.csPort) > _MAX_CS_PORT:
                nodes.append(node)
    return nodes


def _get_underused_csPorts(cluster: Cluster) -> Set[int]:
    """
    Returns csPorts that are underused.
    A csPort "p" is underused if max_port_users(cluster) - num_users(p) >= 2
    where max_port_users(cluster) is the maximum number of users of a csPort,
    and num_users(p) is the number of NICs configured to use the port p.
    :param cluster: the cluster to check.
    """
    if cluster.is_v2_network():
        return set()  # v2 doesn't assign csPort affinity

    port_to_users = {n: 0 for n in range(_MAX_CS_PORT)}
    max_port_users = 0
    for node in cluster.nodes:
        if node.role not in {"any", "broadcastreduce"}:
            continue
        for nic in node.networkInterfaces:
            port_to_users[nic.csPort] = port_to_users.get(nic.csPort, 0) + 1
            max_port_users = max(max_port_users, port_to_users[nic.csPort])

    return {port for port, users in port_to_users.items() if max_port_users - users > 2}


def _get_br_nodes_connected_to_incorrect_csPorts(cluster: Cluster) -> Set[str]:
    """
    Returns BR nodes in the cluster that are not connected to 1 or 2 CS ports.
    :param cluster: the cluster to check.
    """
    badly_connected_nodes = set()
    for node in cluster.nodes:
        if node.role == "broadcastreduce":
            connected_ports = {nic.csPort for nic in node.networkInterfaces}
            if len(connected_ports) not in {1, 2}:
                badly_connected_nodes.add(node.name)
    return badly_connected_nodes


def _get_nodes_with_multi_nic_configs(cluster: Cluster) -> Sequence[common.models.Node]:
    """
    Returns nodes where NICs are defined > 1x.
    :param cluster: the cluster to look at NICs in.
    """
    nodes = []
    for node in cluster.nodes:
        nics = set()
        for nic in node.networkInterfaces:
            if nic.name in nics:
                nodes.append(node)
            nics.add(nic.name)
    return nodes


def _get_node_membership_inconsistencies(controller: common.cluster.ClusterController,
                                         cluster: Cluster,
                                         ) -> Tuple[Set[str], Set[str]]:
    """
    Returns nodes that appear in the config but not the k8s cluster, and nodes that
    appear in the k8s cluster but not in the config.
    :param controller: the controller to access the K8s cluster.
    :cluster: the cluster configuration.
    """
    k8s_nodes = _get_cluster_nodes(controller)
    config_nodes = set([node.name for node in cluster.nodes])
    return (
        config_nodes.difference(k8s_nodes),
        k8s_nodes.difference(config_nodes),
    )


def _get_br_nodes_without_enough_nics(cluster: Cluster) -> Sequence[common.models.Node]:
    """
    Returns nodes in the cluster that are designated BR, but don't have enough NICs.
    :param cluster: the cluster configuration.
    """
    return [n for n in cluster.nodes
            if n.role == "broadcastreduce" and
            len(n.networkInterfaces) < _BR_MIN_NICS]


def _check_node_to_group_assignment(cluster: Cluster) -> Dict[str, str]:
    """Ensure every worker/memx is in group, that group exists, and that non-worker/memx are not in groups"""
    if len(cluster.groups) == 0:
        # only check if there's groups in the cluster
        return dict()

    errors = {}  # node -> error message
    groups = {g.name for g in cluster.groups}
    for node in cluster.nodes:
        g = node.properties.get("group", None)
        if node.role in ["worker", "memory", "management", "any"]:
            if not g:
                errors[node.name] = f"missing membership: nodes of role {node.role} must be in a group"
            else:
                if g not in groups:
                    errors[node.name] = "missing group: node member of a non-existent group " + g
        elif g is not None:
            errors[node.name] = f"unexpected membership: nodes of role {node.role} must not be in a group"
    return errors


def _check_group_composition(cluster: Cluster) -> Dict[str, List[str]]:
    """Ensure group has at least one worker and one memx and that groups are unique"""
    errors = {}
    group_roles = {}
    for node in cluster.nodes:
        g = node.properties.get("group", None)
        if g is not None:
            role_counts = group_roles.get(g, {})
            role_counts[node.role] = role_counts.get(node.role, 0) + 1
            group_roles[g] = role_counts

    seen = set()
    for group in cluster.groups:
        if group.name in seen:
            errors[group.name] = [*errors.get(group.name, []), f"invalid groups: {group.name} appears more than once"]
            continue
        seen.add(group.name)
        counts = group_roles.get(group.name, {})
        if counts.get("worker", 0) < 1:
            errors[group.name] = ["invalid group composition: expected at least one worker node but got 0"]
        if counts.get("memory", 0) < 1 and not cluster.is_v2_network():
            errors[group.name] = [*errors.get(group.name, []),
                                  "invalid group composition: expected at least one memory node but got 0"]
    return errors


def _parse_ip_cmd_output(out: str) -> Dict[str, Optional[str]]:
    """
    Returns a mapping relating interface names to IP addresses.
    :param out: the output of an `ip link show` command.
    """
    interface_to_ip = {}
    links = re.split("^[0-9]+: ", out, flags=re.M)[1:]
    for output in links:
        name = output.split(':')[0].strip()
        ip_match = re.search(r"inet ([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)", output)
        interface_to_ip[name] = ip_match[1] if ip_match else None
    return interface_to_ip


def _get_nodes_with_inconsistent_links(cntrl: common.cluster.ClusterController,
                                       cluster: Cluster,
                                       ) -> List[Tuple[common.models.Node, str, str]]:
    """
    Returns nodes in the k8s cluster that have links whose configurations do not match cluster.
    Each returned value is a tuple (node, interface name, actual IP).
    :param cntrl: the controller to access the k8s cluster.
    :param cluster: the cluster configuration.
    """
    nodes: List[Tuple[common.models.Node, str, str]] = []
    node_to_cmd_results = cntrl.exec_cluster("ip addr show")
    node_name_to_node = {node.name: node for node in cluster.nodes}
    for node_name, (status, stdout, stderr) in node_to_cmd_results.items():
        if status != 0:
            logger.warning(
                f"Could not run command on {node_name}. Stderr: {stderr}.",
            )
            continue

        interface_to_ip = _parse_ip_cmd_output(stdout)
        configured_interfaces = node_name_to_node[node_name].networkInterfaces
        for iface in configured_interfaces:
            if interface_to_ip.get(iface.name) != iface.address:
                nodes.append(
                    (
                        node_name_to_node[node_name],
                        iface.name,
                        interface_to_ip.get(iface.name) or "",
                    ),
                )

    return nodes


def _get_nodes_with_inconsistent_role_labels(cntrl: common.cluster.ClusterController,
                                             cluster: Cluster,
                                             ) -> Set[str]:
    """
    Returns nodes in cluster whose kubernetes role label does not match the cluster's role.
    :param cntrl: the controller to access the k8s cluster.
    :param cluster: the cluster configuration.
    """
    nodes = set()
    node_to_labels = _get_cluster_nodes_to_labels(cntrl)
    for node in cluster.nodes:
        expect_role = f"k8s.cerebras.com/node-role-{node.role}"
        if expect_role not in node_to_labels.get(node.name, {}):
            nodes.add(node.name)
    return nodes


def _validate_cilium(cntrl: common.cluster.ClusterController) -> Tuple[bool, str]:
    """
    Returns if cilium is installed & running on the cluster.
    Under the hood, this invokes the `cilium connectivity test` program.
    :param cntrl: the controller to access the k8s cluster.
    :return: Tuple<succeeded, diagnosis message>
    """
    exit_code, output, _ = cntrl.exec("cilium connectivity test --hubble=false")
    return exit_code == 0, output


def _cilium_installed(cntrl: common.cluster.ClusterController) -> bool:
    """
    Returns if cilium is installed on the cluster.
    :param cntrl: the controller to access the k8s cluster.
    :return: is_cilium_installed
    """
    exit_code, _, _ = cntrl.exec("command cilium")
    return exit_code == 0


def static_validate_config(cluster: common.models.Cluster) -> List[str]:
    """
    Validates the cluster's configuration.
    Returns a list of problems detected..
    :param cluster: the cluster to validate.
    """
    problems = _ProblemLog()

    logger.info("Checking for duplicate addresses...")
    duplicates = _get_duplicate_addresses(cluster)
    if len(duplicates) != 0:
        problems.log(f"Duplicate addresses found in cluster: {duplicates}")

    logger.info("Checking for duplicate nodes...")
    duplicate_nodes = _get_duplicate_nodes(cluster)
    if len(duplicate_nodes) != 0:
        problems.log(f"Duplicate nodes found in cluster: {[n[0].name for n in duplicate_nodes]}")

    logger.info("Checking for missing roles...")
    missing_roles = _get_missing_node_roles(cluster)
    if len(missing_roles) != 0:
        problems.log(f"Missing node roles in cluster: {missing_roles}")

    logger.info("Checking for invalid csPorts...")
    invalid_csPorts = _get_nodes_with_non_existant_csPorts(cluster)
    if len(invalid_csPorts) != 0:
        problems.log(
            f"Nodes contain NICs with csPort > {_MAX_CS_PORT}: {invalid_csPorts}",
        )

    logger.info("Checking for underused csPorts...")
    underused_csPorts = _get_underused_csPorts(cluster)
    if len(underused_csPorts) > 0:
        problems.log(f"CS ports are under-utilized: {underused_csPorts}")

    logger.info(
        "Checking for BR nodes connected to an incorrect number csPorts...",
    )
    incorrectly_connected_br_nodes = _get_br_nodes_connected_to_incorrect_csPorts(
        cluster,
    )
    if len(incorrectly_connected_br_nodes) != 0:
        problems.log(
            f"These BR nodes must be connected to exactly 1 or 2 csPorts: {incorrectly_connected_br_nodes}",
        )

    logger.info("Checking for NICs configured multiple times...")
    nodes_with_multiple_nic_configs = _get_nodes_with_multi_nic_configs(
        cluster,
    )
    if len(nodes_with_multiple_nic_configs) != 0:
        problems.log(
            f"Nodes contain NICs with multiple configurations: {nodes_with_multiple_nic_configs}",
        )

    logger.info("Checking for BR nodes with insufficient NICs configured...")
    br_nodes_with_missing_nics = _get_br_nodes_without_enough_nics(cluster)
    if len(br_nodes_with_missing_nics) > 0:
        problems.log(
            f"BR nodes have less than {_BR_MIN_NICS} NICs: {br_nodes_with_missing_nics}"
        )

    logger.info("Checking for nodes assigned groups properly...")
    node_group_errors = _check_node_to_group_assignment(cluster)
    for node, err in node_group_errors.items():
        problems.log(f"node {node} / {err}")

    logger.info("Checking for groups assigned nodes properly...")
    group_node_errors = _check_group_composition(cluster)
    for group, err in group_node_errors.items():
        problems.log(f"group {group} / {err}")

    return problems.problems


def dynamic_validate_configmap(ctx: CliCtx, system_namespace: str) -> List[str]:
    """
    Validates the cluster configmap.
    Returns a list of problems detected.
    """
    cluster = _get_cluster_config(ctx.cluster_ctr, system_namespace)

    problems = _ProblemLog(initial_problems=static_validate_config(cluster))

    logger.info("Checking cluster for unreachable addresses...")
    unreachable_addresses = _get_unreachable_addresses(
        ctx.cluster_ctr,
        cluster,
    )
    if len(unreachable_addresses) != 0:
        problems.log(f"Found unreachable addresses: {unreachable_addresses}")

    logger.info("Checking cluster for unreachable hostnames...")
    unreachable_hosts = _get_unreachable_hosts(cluster)
    if unreachable_hosts:
        problems.log(f"Found unreachable hosts: {unreachable_hosts}")

    logger.info("Checking for cluster membership inconsistencies...")
    missing_nodes_in_k8s, missing_nodes_in_config = _get_node_membership_inconsistencies(
        ctx.cluster_ctr,
        cluster,
    )
    if len(missing_nodes_in_k8s) != 0:
        problems.log(
            f"Found nodes missing in kubernetes cluster: {missing_nodes_in_k8s}",
        )

    if len(missing_nodes_in_config) != 0:
        problems.log(
            f"Found nodes in kubernetes cluster with missing configuration: {missing_nodes_in_config}",
        )

    logger.info("Checking cluster for nodes with misconfigured NICs...")
    inconsistent_link_configs = _get_nodes_with_inconsistent_links(
        ctx.cluster_ctr,
        cluster,
    )
    if len(inconsistent_link_configs) != 0:
        for config in inconsistent_link_configs:
            problems.log(
                f"Inconsistent NIC config: node {config[0].name}, interface {config[1]}, found address {config[2]}",
            )

    logger.info("Checking cluster for nodes with misconfigured labels...")
    inconsistent_node_labels = _get_nodes_with_inconsistent_role_labels(
        ctx.cluster_ctr,
        cluster,
    )
    if len(inconsistent_node_labels) != 0:
        problems.log(
            f"Labels of {inconsistent_node_labels} do not match configured roles.",
        )

    logger.info("Checking Cilium. This may take a couple minutes...")
    if not ctx.in_kind_cluster and _cilium_installed(ctx.cluster_ctr):
        valid, diagnosis = _validate_cilium(ctx.cluster_ctr)
        if not valid:
            problems.log(f"Problem with cilium: {diagnosis}")
    elif not ctx.in_kind_cluster:
        problems.log("Not in kind cluster, but cilium command not found. "
                     "Make sure cilium is installed, or update validation "
                     "script.")

    return problems.problems


def summarize_issues(ctrl: common.cluster.ClusterController,
                     cluster: common.models.Cluster,
                     ) -> Dict[str, Iterable[str]]:
    """Returns a map relating issue names to an iterator of affected components."""

    def node_membership_inconsistencies(ctrl: common.cluster.ClusterController,
                                        cls: common.models.Cluster) -> Set[str]:
        a, b = _get_node_membership_inconsistencies(ctrl, cls)
        return a.union(b)

    def nic_inconsistencies(ctrl: common.cluster.ClusterController,
                            cls: common.models.Cluster) -> Set[str]:
        return {
            node.name for node, _, _ in _get_nodes_with_inconsistent_links(ctrl, cls)
        }

    issue_name_to_detector: Dict[
        str,
        Callable[
            [common.cluster.ClusterController, common.models.Cluster],
            Iterable[str],
        ],
    ] = {
        "cs_address_unreachable": _get_unreachable_addresses,
        "cs_inconsistent_membership": node_membership_inconsistencies,
        "cs_inconsistent_nic_config": nic_inconsistencies,
        "cs_inconsistent_labels": _get_nodes_with_inconsistent_role_labels,
    }

    return {
        issue_name: detector(ctrl, cluster)
        for issue_name, detector in issue_name_to_detector.items()
    }
