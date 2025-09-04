import pytest

from common.image import get_image_target_nodes
from common.models import Cluster, Node, System, NodeGroup


CLUSTER = Cluster(
    "",
    [
        System("s0", "cs2", "0.0.0.1"),
        System("s1", "cs2", "0.0.0.2"),
    ],
    [
        Node("n0", "memory", properties={"group": "g0"}),
        Node("n1", "memory", properties={"group": "g1"}),
        Node("n2", "broadcastreduce"),
        Node("n3", "management"),
    ],
    [
        NodeGroup("g0", {"systemAffinity": "s0"}),
        NodeGroup("g1", {"systemAffinity": "s1"})
    ]
)


@pytest.mark.parametrize(
    "cluster, systems, expected",
    [
        # No systems -> all nodes
        (
            CLUSTER,
            [],
            ["n3", "n0", "n1", "n2"]
        ),
        # single systems -> no BR nodes
        (
            CLUSTER,
            ["s1"],
            ["n3", "n1"]
        ),
        # multiple systems -> BR nodes
        (
            CLUSTER,
            ["s0", "s1"],
            ["n3", "n0", "n1", "n2"]
        ),
    ],
)
def test_get_target_nodes(cluster, systems, expected):
    vals = get_image_target_nodes(cluster, systems)
    assert vals[0] == expected[0], "mgmt node should be first"
    assert set(vals[1:]) == set(expected[1:])
