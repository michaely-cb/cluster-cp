from typing import Dict, List, Set
from common.models import NodeGroup

import common.models
from common.models import Node
from common.models import System
from common.models import Cluster
import common.validate
import pytest

# Nodes for creating test clusters.
node_a = common.models.Node(
    name="a",
    role="worker",
    networkInterfaces=[
        common.models.NetworkInterface(
            name="iface0",
            address="1.1.1.1",
            csPort=0,
        ),
    ],
)

node_b = common.models.Node(
    name="b",
    role="worker",
    networkInterfaces=[
        common.models.NetworkInterface(
            name="iface0",
            address="1.1.1.1",
            csPort=1,
        ),
        common.models.NetworkInterface(
            name="iface1",
            address="1.1.1.1",
            csPort=1,
        ),
    ],
)

node_bb = common.models.Node(
    name="b",
    role="broadcastreduce",
    networkInterfaces=[
        common.models.NetworkInterface(
            name="iface0",
            address="1.1.1.0",
            csPort=1,
        ),
        common.models.NetworkInterface(
            name="iface1",
            address="1.1.1.1",
            csPort=1,
        ),
        common.models.NetworkInterface(
            name="iface2",
            address="1.1.1.2",
            csPort=1,
        ),
    ],
)

node_c = common.models.Node(
    name="c",
    role="worker",
    networkInterfaces=[
        common.models.NetworkInterface(
            name="iface0",
            address="1.1.1.1",
            csPort=2,
        ),
    ],
)

node_d = common.models.Node(
    name="d",
    role="worker",
    networkInterfaces=[
        common.models.NetworkInterface(
            name="iface0",
            address="1.1.1.2",
            csPort=2,
        ),
    ],
)

node_any = common.models.Node(
    name="node-any",
    role="any",
    networkInterfaces=[
        common.models.NetworkInterface(
            name="iface0",
            address="1.1.1.2",
            csPort=42,
        ),
    ],
)

node_illegal_csPort = common.models.Node(
    name="node-illegal-cs-port",
    role="any",
    networkInterfaces=[
        common.models.NetworkInterface(
            name="iface0",
            address="1.1.1.2",
            csPort=42,
        ),
    ],
)


node_nic_multi_configured = common.models.Node(
    name="node-multi-configured",
    role="any",
    networkInterfaces=[
        common.models.NetworkInterface(
            name="iface0",
            address="1.1.1.2",
            csPort=10,
        ),
        common.models.NetworkInterface(
            name="iface0",
            address="1.1.1.1",
            csPort=9,
        ),
    ],
)

node_br_sufficient_nics = common.models.Node(
    name="node-br-enough-nics",
    role="broadcastreduce",
    networkInterfaces=[
        common.models.NetworkInterface(
            name="iface0",
            address="1.1.1.1",
            csPort=4,
        ),
        common.models.NetworkInterface(
            name="iface1",
            address="1.1.1.1",
            csPort=4,
        ),
        common.models.NetworkInterface(
            name="iface2",
            address="1.1.1.1",
            csPort=4,
        ),
        common.models.NetworkInterface(
            name="iface3",
            address="1.1.1.2",
            csPort=5,
        ),
        common.models.NetworkInterface(
            name="iface4",
            address="1.1.1.2",
            csPort=5,
        ),
        common.models.NetworkInterface(
            name="iface5",
            address="1.1.1.2",
            csPort=5,
        ),
    ],
)

node_br_insufficient_nics = common.models.Node(
    name="node-br-not-enough-nics",
    role="broadcastreduce",
    networkInterfaces=[],
)

cluster_with_group = common.models.Cluster(
    "valid composition",
    [],
    [
        common.models.Node("w0", "worker",
                           properties={"group": "ng0"}),
        common.models.Node("m0", "memory",
                           properties={"group": "ng0"}),
    ],
    groups=[
        NodeGroup(name="ng0"),
    ],
)


def _make_node(csPort: int = 0) -> common.models.Node:
    return common.models.Node(
        name="node-br-not-enough-nics",
        role="broadcastreduce",
        networkInterfaces=[
             common.models.NetworkInterface(
                 name="iface0",
                 address="1.1.1.1",
                 csPort=csPort,
             ),
        ],
    )


@pytest.mark.parametrize(
    "cluster, expected",
    [
        # Tests the empty cluster.
        (common.models.Cluster("", [], []), []),
        # A cluster with a single node.
        (
            common.models.Cluster("", [], [node_a]),
            [],
        ),
        # A cluster with a node that has interfaces with duplicate addresses.
        (
            common.models.Cluster("", [], [node_b]),
            [[node_b, node_b]],
        ),
        # A cluster with 2 nodes with duplicate addresses.
        (
            common.models.Cluster("", [], [node_a, node_c]),
            [[node_a, node_c]],
        ),
        # A cluster with 2 nodes with no duplicate addresses.
        (
            common.models.Cluster("", [], [node_a, node_d]),
            [],
        ),
    ],
)
def test_get_duplicate_ip_addresses(cluster: common.models.Cluster, expected: List[List[common.models.Node]]):
    duplicates = common.validate._get_duplicate_addresses(cluster)
    assert duplicates == expected


@pytest.mark.parametrize(
    "cluster, expected",
    [
        # Tests the empty cluster.
        (common.models.Cluster("", [], []), []),
        # A cluster that has one node has no duplicates.
        (
            common.models.Cluster("", [], [node_a]),
            [],
        ),
        # A cluster that has two duplicate nodes.
        (
            common.models.Cluster("", [], [node_a, node_a]),
            [[node_a, node_a]],
        ),
        # A 3-node cluster that has two duplicate nodes, and one other.
        (
            common.models.Cluster("", [], [node_a, node_b, node_a]),
            [[node_a, node_a]],
        ),
    ],
)
def test_get_duplicate_nodes(cluster: common.models.Cluster, expected: List[List[common.models.Node]]):
    duplicates = common.validate._get_duplicate_nodes(cluster)
    assert duplicates == expected


@pytest.mark.parametrize(
    "cluster, expected",
    [
        # Empty cluster test.
        (
            common.models.Cluster("", [], []),
            {"management", "memory", "worker"},
        ),
        # A node with an any role can do anything.
        (
            common.models.Cluster("", [], [node_any]),
            set(),
        ),
        # Missing a few nodes.
        (
            common.models.Cluster("", [], [node_a]),
            {"management", "memory"},
        ),
        # Cluster with 2+ CS2 requires BR
        (
                Cluster("",
                        [
                            System("s0", "cs2", "0.0.0.1", "0.0.0.2"),
                            System("s1", "cs2", "0.0.0.3", "0.0.0.4"),
                        ], [
                            Node("n0", "management"),
                            Node("n0", "memory"),
                            Node("n0", "worker"),
                        ]),
                {"broadcastreduce"},
        ),
    ],
)
def test_get_missing_roles(cluster: common.models.Cluster, expected: Set[str]):
    missing = common.validate._get_missing_node_roles(cluster)
    assert missing == expected


@pytest.mark.parametrize(
    "cluster, expected",
    [
        # Empty cluster test.
        (common.models.Cluster("", [], []), []),
        # Contains 1 invalid csPort.
        # node_any has an invalid csPort (42).
        (
            common.models.Cluster("", [], [node_any]),
            [node_any],
        ),
        # Contains several invalid csPorts.
        (
            common.models.Cluster("", [], [node_any, node_illegal_csPort]),
            [node_any, node_illegal_csPort],
        ),
    ],
)
def test_get_nodes_with_invalid_csPorts(cluster: common.models.Cluster, expected: List[common.models.Node]):
    nodes_with_invalid_ports = common.validate._get_nodes_with_non_existant_csPorts(
        cluster)
    assert nodes_with_invalid_ports == expected


@pytest.mark.parametrize(
    "cluster, expected",
    [
        # Empty cluster test.
        (common.models.Cluster("", [], []), []),
        # Cluster with 1 multi nic config error.
        (
            common.models.Cluster("", [], [node_nic_multi_configured]),
            [node_nic_multi_configured],
        ),
        # Cluster with 1 multi nic config error, and 1 correct node.
        (
            common.models.Cluster(
                "",
                [],
                [node_a, node_nic_multi_configured],
            ),
            [node_nic_multi_configured],
        ),
    ],
)
def test_get_nodes_with_multi_nic_configs(cluster: common.models.Cluster, expected: List[common.models.Node]):
    nodes = common.validate._get_nodes_with_multi_nic_configs(cluster)
    assert nodes == expected


@pytest.mark.parametrize(
    "cluster, expected",
    [
        (common.models.Cluster("", [], []), []),
        (common.models.Cluster("", [], [node_br_sufficient_nics]), []),
        (
            common.models.Cluster("", [], [node_br_insufficient_nics]),
            [node_br_insufficient_nics],
        ),
        (
            common.models.Cluster(
                "",
                [],
                [node_br_sufficient_nics, node_br_insufficient_nics],
            ),
            [node_br_insufficient_nics],
        ),
    ],
)
def test_get_br_nodes_without_enough_nics(cluster: common.models.Cluster, expected: List[common.models.Node]):
    nodes = common.validate._get_br_nodes_without_enough_nics(cluster)
    assert nodes == expected


@pytest.mark.parametrize(
    "cluster, expected",
    [
        # An empty cluster has no underused csPorts.
        (common.models.Cluster("", [], []), set()),
        # A cluster that has no underused csPorts.
        (common.models.Cluster("", [], [node_a]), set()),
        # A cluster that has many underused csPorts.
        # node_b uses port 1 2x.
        (
            common.models.Cluster("", [], [node_bb]),
            set(range(common.validate._MAX_CS_PORT)).difference({1}),
        ),
        # A large cluster with no underused csPorts.
        (
            common.models.Cluster(
                "",
                [],
                [_make_node(csPort=p)
                 for p in range(common.validate._MAX_CS_PORT)],
            ),
            set(),
        )
    ],
)
def test_get_underused_csPorts(cluster: common.models.Cluster, expected: Set[int]):
    underused_ports = common.validate._get_underused_csPorts(cluster)
    assert underused_ports == expected


@pytest.mark.parametrize(
    "cluster, expected",
    [
        # An empty cluster has no BR nodes connected to an incorrect number of ports.
        (common.models.Cluster("empty", [], []), set()),
        # This node is connected to a sufficient number of cs ports.
        (common.models.Cluster("2-good", [],
         [node_br_sufficient_nics, node_a]), set()),
        # This node is connected to an insufficient number of cs ports.
        (
            common.models.Cluster(
                "2-nodes-1-bad",
                [],
                [
                    node_br_insufficient_nics,
                    node_a,
                ],
            ),
            {"node-br-not-enough-nics"},
        ),
        # This cluster contains 1 node connected to an insufficient number of cs ports.
        (
            common.models.Cluster(
                "3-nodes-1-bad",
                [],
                [
                    node_br_insufficient_nics,
                    node_a,
                    node_br_sufficient_nics,
                ],
            ),
            {"node-br-not-enough-nics"},
        ),
    ],
)
def test_get_br_nodes_connected_to_incorrect_csPort(cluster: common.models.Cluster, expected: Set[str]):
    underconnected_nodes = common.validate._get_br_nodes_connected_to_incorrect_csPorts(
        cluster,
    )
    assert underconnected_nodes == expected


@pytest.mark.parametrize(
    "cluster, expected",
    [
        (
                common.models.Cluster(
                    "missing worker, memx, both",
                    [],
                    [
                        common.models.Node("w0", "worker",
                                           properties={"group": "ng0"}),
                        common.models.Node("m0", "memory",
                                           properties={"group": "ng1"}),
                    ],
                    groups=[
                        NodeGroup(name="ng0"),
                        NodeGroup(name="ng1"),
                        NodeGroup(name="ng2"),
                    ],
                ),
                {'ng0': ['invalid group composition: expected at least one memory node but got 0'],
                 'ng1': ['invalid group composition: expected at least one worker node but got 0'],
                 'ng2': ['invalid group composition: expected at least one worker node but got 0',
                         'invalid group composition: expected at least one memory node but got 0']}
                ,
        ),
        (
                cluster_with_group, {},
        ),
    ],
)
def test_check_group_composition(cluster: common.models.Cluster, expected: Dict[str, List[str]]):
    errors = common.validate._check_group_composition(cluster)
    assert errors == expected


@pytest.mark.parametrize(
    "cluster, expected",
    [
        (
                common.models.Cluster(
                    "worker, memx, missing groups",
                    [],
                    [
                        common.models.Node("w0", "worker",
                                           properties={"group": "ng0"}),
                        common.models.Node("w1", "worker"),
                        common.models.Node("m0", "memory",
                                           properties={"group": "ng0"}),
                        common.models.Node("m1", "memory"),
                        common.models.Node("m2", "memory",
                                           properties={"group": "ng1"}),
                        common.models.Node("a0", "any"),
                        common.models.Node("mgmt", "management"),
                        common.models.Node("br0", "broadcastreduce",
                                           properties={"group": "ng0"}),
                    ],
                    groups=[
                        NodeGroup(name="ng0"),
                    ],
                ),
                {'w1': 'missing membership: nodes of role worker must be in a group',
                 'm1': 'missing membership: nodes of role memory must be in a group',
                 'a0': 'missing membership: nodes of role any must be in a group',
                 'm2': 'missing group: node member of a non-existent group ng1',
                 'mgmt': 'missing membership: nodes of role management must be in a group',
                 'br0': 'unexpected membership: nodes of role broadcastreduce must not be in a group'},
        ),
        (
                cluster_with_group, {},
        ),
        (
                common.models.Cluster(
                    "no groups does not check",
                    [],
                    [
                        common.models.Node("w0", "worker"),
                        common.models.Node("w1", "worker"),
                        common.models.Node("m0", "memory"),
                        common.models.Node("m1", "memory"),
                    ],
                    groups=[],
                ),
                {},
        ),
    ],
)
def test_check_node_to_group_assignment(cluster: common.models.Cluster, expected: Dict[str, str]):
    errors = common.validate._check_node_to_group_assignment(cluster)
    assert errors == expected
