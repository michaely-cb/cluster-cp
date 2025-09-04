import pytest

from . import mock_devinfra_cluster
from common.context import CliCtx
from common.context import try_load_cluster_from_db
from common.models import Node


@pytest.mark.parametrize(
    "before, after",
    [
        # <name>.<role>
        (["d.a", "b.b", "c1.m", "c2.m", "a.a"], ["c1.m", "c2.m", "a.a", "b.b", "d.a"]),

        # already sorted
        (["m0.m", "m1.m", "m2.m", "a0.a", "a1.a"], ["m0.m", "m1.m", "m2.m", "a0.a", "a1.a"]),
    ],
)
def test_node_name_sorted(before, after):
    role = {v[0]: v for v in ["activation", "broadcastreduce", "management", "weight"]}
    to_nodes = lambda abbr: list(map(lambda t: Node(t[0], role[t[1]]), map(lambda n: n.split("."), abbr)))
    to_names = lambda abbr: list(map(lambda n: n.split(".")[0], abbr))

    assert to_names(after) == [n.name for n in CliCtx._nodes_ordered(to_nodes(before))]


def test_try_load_cluster_from_db():
    cluster = try_load_cluster_from_db("test-test", json=mock_devinfra_cluster)
    assert cluster is not None
    assert cluster.name == "test-test"
    assert [s.name for s in cluster.systems] == ["systemf1", "systemf2"]
    assert len(cluster.nodes) == 7
