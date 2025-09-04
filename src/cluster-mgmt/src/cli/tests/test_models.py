import json
from collections import Counter
from textwrap import dedent
from textwrap import indent
from typing import Dict

import pytest
import yaml

from common.models import BROADCAST_REDUCE, ACTIVATION, INFERENCEDRIVER
from common.models import Cluster
from common.models import NIC
from common.models import Node
from common.models import NodeGroup
from common.models import SwitchConfig
from common.models import System
from common.models import _add_nodegroup_system_affinity
from common.models import _ensure_system_names
from common.models import _parse_node
from common.models import _parse_systems
from common.models import _parse_v2_groups
from common.models import bucket
from common.models import parse_cluster_config
from common.scrape import _scrape_system
from common.scrape import _validate_system
from tests.test_cluster import _generate_network_doc_v2


@pytest.mark.parametrize(
    "items, n, expected",
    [
        (
                [1, 2, 3, 4, 5, 6], 0, [[1, 2, 3, 4, 5, 6]]
        ),
        (
                [1, 2, 3, 4, 5, 6], 1, [[1, 2, 3, 4, 5, 6]]
        ),
        (
                [1, 2, 3, 4, 5, 6], 2, [[1, 2, 3], [4, 5, 6]]
        ),
        (
                [1, 2, 3, 4, 5, 6], 4, [[1, 2], [3, 4], [5], [6]]
        ),
        (
                [1, 2, 3, 4, 5, 6], 6, [[1], [2], [3], [4], [5], [6]]
        ),
        (
                [1, 2, 3, 4, 5, 6], 7, [[1], [2], [3], [4], [5], [6], []]
        )
    ],
)
def test_buckets(items, n, expected):
    assert expected == bucket(items, n)


def test_node_hostname_default():
    assert "myname" == Node("myname", "any").hostname
    assert "host" == Node("myname", "any", "host").hostname
    assert "myname" == Node(**yaml.safe_load("name: myname\nrole: any")).hostname


def _gen_nics():
    return indent("\n".join([
        f"- name: nic-{i}\n  address: 10.0.0.{i}\n  csPort: 0"
        for i in range(6)
    ]), " " * 6)


_cluster_yaml = f"""
kind: cluster-configuration
name: myname
systems:
  - name: system0
    type: cs2
    managementAddress: 10.0.0.1
    managementUser: admin
    managementPassword: admin
    controlAddress: 10.0.0.2
  - name: system1
    type: cs2
    controlAddress: 10.0.0.3 # no mgmt address
groups:
  - name: ng-0
    properties:
      memory: 1TB
nodes:
  - name: br0
    role: broadcastreduce
    networkInterfaces:
{_gen_nics()}
    properties:
      group: ng-0
"""


def test_yaml_dump_asdict_reflexive():
    c = Cluster(**yaml.safe_load(_cluster_yaml))
    cr = Cluster(**yaml.safe_load(c.to_yaml()))
    # transform to dict using json serialization to avoid comparing objects
    assert json.loads(c.to_json()) == json.loads(cr.to_json())


@pytest.mark.parametrize(
    "obj,expected",
    [
        (
                Cluster(name="x", systems=[], nodes=[Node(name="y", role="z", hostname="y")]),
                dedent("""
                    kind: cluster-configuration
                    name: x
                    properties: {}
                    systems: []
                    nodes:
                    - name: y
                      role: z
                    """),
        ),
        (
                NodeGroup(name="x", properties={"group": "ng0"}),
                dedent("""
                    name: x
                    properties:
                      group: ng0
                    """),
        ),
        (
                Node(name="x", hostname="x"),
                "name: x",
        ),
        (
                Node(name="x", hostname="y"),
                "name: x\nhostname: y",
        ),
        (
                NodeGroup(name="x"),
                "name: x",
        ),
    ],
)
def test_asyaml(obj, expected: str):
    assert obj.to_yaml().strip() == expected.strip()


def test_parse_systems():
    net_json = {
        "systems": [{
            "name": "systemf69",
            "vip": "10.250.55.7/32",
            "management_address": "1.0.1.0",
            "management_user": "mgmtusers",
            "management_password": "P@_$w0rd!"
        }, {
            "name": "systemf70",
            "vip": "10.250.55.8/32",
        }]}
    sys = _parse_systems(net_json)
    assert 2 == len(sys)
    assert "1.0.1.0" == sys[0].managementAddress
    assert "mgmtusers" == sys[0].managementUser
    assert "P@_$w0rd!" == sys[0].managementPassword
    assert sys[1].managementAddress is None


def test_parse_cluster_config_16_system_network_file(net_cfg_16_system: dict):
    cluster = parse_cluster_config(net_cfg_16_system)

    assert 16 == len(cluster.systems)
    assert all([g.properties.get("systemAffinity", False) for g in cluster.groups])


@pytest.mark.parametrize(
    "systems,nodegroups,cluster_config,expected",
    [
        (
                [System("system0", "cs2", "0.0.0.1")],
                [NodeGroup("ng0")],
                None,
                [NodeGroup("ng0", properties={"systemAffinity": "system0"})],
        ),
        (
                [System("system0", "cs2", "0.0.0.1")],
                [NodeGroup("ng0", properties={"systemAffinity": "x", "foo": "bar"})],
                None,
                [NodeGroup("ng0", properties={"systemAffinity": "system0", "foo": "bar"})],
        ),
        (
                [System("system0", "cs2", "0.0.0.1"), System("system1", "cs2", "0.0.0.2")],
                [NodeGroup("ng0")],
                None,
                [NodeGroup("ng0", properties={"systemAffinity": "system0"})],
        ),
        (
                [],
                [NodeGroup("ng0")],
                None,
                [NodeGroup("ng0")],
        ),
        (
                [System("system0", "cs2", "0.0.0.1"), System("system1", "cs2", "0.0.0.2")],
                [NodeGroup("ng0"), NodeGroup("ng1"), NodeGroup("ng2")],
                Cluster(
                    name="x",
                    systems=[System("system0", "cs2", "0.0.0.1")],
                    nodes=[],
                    groups=[
                        NodeGroup("ng0", properties={"systemAffinity": "system0"}),
                    ]
                ),
                [
                    NodeGroup("ng0", properties={"systemAffinity": "system0"}),
                    NodeGroup("ng1", properties={"systemAffinity": "system1"}),
                    NodeGroup("ng2"),
                ],
        ),
        (
                [System("system1", "cs2", "0.0.0.1"), System("system2", "cs2", "0.0.0.2")],
                [NodeGroup("ng0"), NodeGroup("ng1"), NodeGroup("ng2")],
                Cluster(
                    name="x",
                    systems=[System("system0", "cs2", "0.0.0.1")],
                    nodes=[],
                    groups=[
                        NodeGroup("ng0", properties={"systemAffinity": "system0"}),
                    ]
                ),
                [
                    NodeGroup("ng0", properties={"systemAffinity": "system1"}),
                    NodeGroup("ng1", properties={"systemAffinity": "system2"}),
                    NodeGroup("ng2"),
                ],
        ),
    ],
)
def test_add_nodegroup_system_affinity(systems, nodegroups, cluster_config, expected):
    _add_nodegroup_system_affinity(systems, nodegroups, cluster_config)
    assert [e.to_yaml() for e in expected] == [n.to_yaml() for n in nodegroups]


def test_scrape_system_valid_config_show_output():
    def mock_ssh(cmd, *args, **kwargs) -> str:
        if cmd.startswith("hostname"):
            return "192.168.1.1"  # management address
        else:
            return json.dumps({  # cs config network show output
                "dataStaticKeyVal": [{
                    "config": "Control IP",
                    "value": "192.168.2.1/24"
                }]})

    result = _scrape_system("system1", mock_ssh)

    assert result.name == "system1"
    assert result.type == "cs2"
    assert result.managementAddress == "192.168.1.1"
    assert result.controlAddress == "192.168.2.1:9000"


def test_scrape_system_valid_network_show_output():
    def mock_ssh(cmd, *args, **kwargs) -> str:
        if cmd.startswith("hostname"):
            return "192.168.1.1"  # management address
        elif cmd.startswith("cs config network show"):
            return '{}'  # unparseable
        return json.dumps({  # cs network show output
            "data": [{
                "ifaceName": "CTRL",
                "ipAddress": "192.168.2.2/24"
            }]})

    result = _scrape_system("system1", mock_ssh)

    assert result.name == "system1"
    assert result.type == "cs2"
    assert result.managementAddress == "192.168.1.1"
    assert result.controlAddress == "192.168.2.2:9000"


def test_validate_system():
    s0 = System("system0", "cs2", "10.0.0.1:9000")
    s1 = System("system1", "cs2", "10.0.1.1:9000")
    s2 = System("wscompilepod", "cs2", "~wscompilepod:0~")
    ng0 = NodeGroup("ng0", {}, switchConfig=SwitchConfig(
        "10.0.0.0/24",
        "10.0.0.254",
        "10.0.0.0/24",
        "10.0.0.64",
        "10.0.0.232",
    ))
    # ok
    _validate_system(s0, [])
    _validate_system(s0, [ng0])
    _validate_system(s2, [ng0])

    # out of range
    with pytest.raises(AssertionError, match="cm addr was not contained"):
        _validate_system(s1, [ng0])


def test_scrape_system_unreachable_cs_cmds_fails():
    def mock_ssh(cmd, *args, **kwargs) -> str:
        raise Exception("cs command failed")

    with pytest.raises(Exception):
        _scrape_system("system1", mock_ssh)


def test_parse_json():
    c = Cluster(**json.loads(
        """
        {
          "kind": "cluster-configuration",
          "name": "test",
          "systems": [
            {
              "name": "kapi-0",
              "type": "cs2",
              "managementAddress": "8.8.8.8",
              "controlAddress": "~ws_kapi_model~:0"
            }
          ],
          "nodes": [
            {
              "name": "n0",
              "role": "management",
              "properties": {
                "group": "nodegroup0"
              }
            }
          ],
          "groups": [
            {
              "name": "nodegroup0",
              "switchConfig": {
                "gateway": "10.0.1.255",
                "subnet": "10.0.1.0/24",
                "parentnet": "10.0.0.0/20",
                "virtualStart": "10.0.1.46",
                "virtualEnd": "10.0.1.190",
                "memoryNIC": "eth0",
                "workerNIC": "eth1",
                "managementNIC": "eth2",
                "asn": "65001",
                "unrecognized": "foo"
               }
            }
          ],
          "v2Groups": [
            {
              "name": "sw001",
              "vlans": [{
                "name": "memx",
                "gateway": "10.0.1.255",
                "subnet": "10.0.1.0/24",
                "parentnet": "10.0.0.0/20",
                "virtualStart": "10.0.1.46",
                "virtualEnd": "10.0.1.190",
                "asn": "65001"
              }]
            }
          ]
        }
        """
    ))
    assert len(c.systems) == 1
    assert len(c.nodes) == 1
    assert len(c.groups) == 1
    assert c.groups[0].properties.get("test", "") == ""
    assert c.groups[0].switchConfig.gateway == "10.0.1.255"
    assert c.groups[0].switchConfig.asn == "65001"
    assert c.v2Groups[0].vlans[0].name == "memx"


@pytest.mark.parametrize(
    "switch_config,expected",
    [
        (
                # small subnet - doesn't make sense but should parse anyways
                {
                    "address": "10.0.0.255/28",
                    "virtual_addrs": {
                        "starting_address": "10.0.0.248/28",
                        "ending_address": "10.0.0.254/28",
                    }
                },
                {
                    "virtualStart": "10.0.0.248",
                    "virtualEnd": "10.0.0.254",
                }
        ),
        (
                # large subnet
                {
                    "address": "10.0.0.255/24",
                    "virtual_addrs": {
                        "starting_address": "10.0.0.64/24",
                        "ending_address": "10.0.0.254/24",
                    }
                },
                {
                    "virtualStart": "10.0.0.64",
                    "virtualEnd": "10.0.0.254",
                }
        ),
        (
                {
                    "address": "10.0.0.32/26",
                    "virtual_addrs": {
                        "starting_address": "10.0.0.42/26",
                        "ending_address": "10.0.0.55/26",
                    }
                },
                {
                    "virtualStart": "10.0.0.42",
                    "virtualEnd": "10.0.0.55",
                }
        ),
    ])
def test_switch_config_init(switch_config, expected):
    err = None
    try:
        sc = SwitchConfig.of("10.0.0.0/16", switch_config)
    except ValueError as e:
        err = e
    if expected is None:
        assert err is not None, "expected error not thrown"
    else:
        for field, expected_value in expected.items():
            assert getattr(sc, field) == expected_value, \
                f"Failed for {field}, wanted {expected_value} != {getattr(sc, field)}"


def test_ensure_system_names():
    sys_ok = [
        System(name="SystemX01", type="cs2", controlAddress="0.0.0.0"),
        System(name="systemx02", type="cs2", controlAddress="0.0.0.0")
    ]
    _ensure_system_names(sys_ok)
    assert sys_ok[0].name == "systemx01"
    assert sys_ok[1].name == "systemx02"

    with pytest.raises(ValueError):
        _ensure_system_names([System(name="system~x1", type="cs2", controlAddress="0.0.0.0")])

def test_parse_v1_br_group():
    # Difference with v2: 2 nics connect to different switches
    doc = {
        "name": "br0",
        "nodegroup": "ng0",
        "interfaces": [
            {
                "address": "10.251.32.28/23",
                "name": "eth100g1",
                "switch_name": "sc-r7rb11-400gsw",
                "switch_port": "Ethernet5/3",
                "gbps": 100
            },
            {
                "address": "10.251.32.30/23",
                "name": "eth100g3",
                "switch_name": "sc-r7rb14-400gsw",
                "switch_port": "Ethernet1/3",
                "gbps": 400
            }
        ],
    }

    nic_csport: Dict[NIC, int] = {
        NIC("br0", "10.251.32.28", "eth100g1"): 0,
        NIC("br0", "10.251.32.30", "eth100g3"): 1
    }
    node = _parse_node(doc, BROADCAST_REDUCE, nic_csport, is_v2=False)
    print(node.properties)
    assert "v2Group" not in node.properties
    assert len(node.networkInterfaces) == 2
    assert node.networkInterfaces[0].csPort == 0
    assert node.networkInterfaces[1].csPort == 1
    assert node.networkInterfaces[0].gbps is None
    assert node.networkInterfaces[1].gbps == 400, str(node.networkInterfaces[1])
    assert node.networkInterfaces[0].v2Group == "sc-r7rb11-400gsw"
    assert node.networkInterfaces[1].v2Group == "sc-r7rb14-400gsw"


def test_parse_v2_group_affinity():
    net_json = {
        "systems": [
            {
                "name": "s0",
                "interfaces": [
                    {
                        "name": "data0",
                        "switch_name": "sw0"
                    },
                    {
                        "address": "10.251.32.56/23",
                        "name": "data1",
                        "switch_name": "sw1"
                    }]

            },
            {
                "name": "s1",
                "interfaces": [
                    {
                        "name": "data0",
                        "switch_name": "sw0"
                    },
                    {
                        "name": "data1",
                        "switch_name": "sw0"
                    }]
            }
        ],
        "switches": [
            {"name": "sw0", "tier": "LF"},
            {"name": "sw1", "tier": "LF"},
        ],
        "environment": {
            "cluster_prefix": "10.251.0.0/16",
        }
    }

    leaf_groups = _parse_v2_groups(net_json)

    assert 2 == len(leaf_groups)
    for g in leaf_groups:
        if "sw0" in g.name:
            assert "sw0" == g.name
            assert "s0-0" in g.properties
            assert "s1-0" in g.properties
            assert "s1-1" in g.properties
        else:
            assert "sw1" == g.name
            assert "s0-1" in g.properties


def test_parse_v2_br_group():
    doc = {
        "name": "br0",
        "nodegroup": "ng0",
        "interfaces": [
            {
                "address": "10.251.32.28/23",
                "name": "eth100g1",
                "switch_name": "sc-r7rb14-400gsw",
                "switch_port": "Ethernet5/3",
                "gbps": 100
            },
            {
                "address": "10.251.32.30/23",
                "name": "eth100g3",
                "switch_name": "sc-r7rb14-400gsw",
                "switch_port": "Ethernet1/3",
                "gbps": 400
            }
        ],
    }

    node = _parse_node(doc, BROADCAST_REDUCE, {}, is_v2=True)
    assert node.properties["v2Group"] == "sc-r7rb14-400gsw"
    assert len(node.networkInterfaces) == 2
    assert node.networkInterfaces[0].csPort is None
    assert node.networkInterfaces[1].csPort is None
    assert node.networkInterfaces[0].gbps is None
    assert node.networkInterfaces[1].gbps == 400, str(node.networkInterfaces[1])


def test_parse_v2_ax_node():
    doc = {
        "name": "ax0",
        "interfaces": [
            {
                "address": "10.251.32.28/23",
                "name": "eth400g0",
                "switch_name": "sc-r7rb14-400gsw",
                "switch_port": "Ethernet5/3",
                "gbps": 400
            },
            {
                "address": "10.251.32.30/23",
                "name": "eth400g1",
                "switch_name": "sc-r7rb15-400gsw",
                "switch_port": "Ethernet1/3",
                "gbps": 400
            }
        ],
    }

    node = _parse_node(doc, ACTIVATION, {}, is_v2=True)
    assert node.properties.get("v2Group") is None
    assert len(node.networkInterfaces) == 2
    assert node.networkInterfaces[0].csPort is None
    assert node.networkInterfaces[1].csPort is None
    assert node.networkInterfaces[0].gbps == 400, str(node.networkInterfaces[0])
    assert node.networkInterfaces[1].gbps == 400, str(node.networkInterfaces[1])
    assert node.networkInterfaces[0].v2Group == "sc-r7rb14-400gsw"
    assert node.networkInterfaces[1].v2Group == "sc-r7rb15-400gsw"


def test_parse_v2_inferencedriver_node():
    doc = {
        "name": "id0",
        "interfaces": [
            {
                "address": "10.0.0.1/23",
                "name": "eth400g0",
                "switch_name": "sw0",
                "switch_port": "Ethernet1/1",
                "gbps": 400
            },
            {
                "address": "10.0.0.2/23",
                "name": "eth400g1",
                "switch_name": "sw0",
                "switch_port": "Ethernet1/5",
                "gbps": 400
            }
        ],
    }

    node = _parse_node(doc, INFERENCEDRIVER, {}, is_v2=True)
    assert node.properties.get("v2Group") is None
    assert len(node.networkInterfaces) == 2
    assert node.networkInterfaces[0].csPort is None
    assert node.networkInterfaces[1].csPort is None
    assert node.networkInterfaces[0].gbps == 400, str(node.networkInterfaces[0])
    assert node.networkInterfaces[1].gbps == 400, str(node.networkInterfaces[1])
    assert node.networkInterfaces[0].v2Group == "sw0"
    assert node.networkInterfaces[1].v2Group == "sw0"


def test_model_parse_v2(monkeypatch):
    monkeypatch.setattr("common.models._find_assigned_ng_k8s", set)

    netdoc = _generate_network_doc_v2()
    cluster = parse_cluster_config(netdoc, None)

    mx_per_group = Counter([n.properties["group"] for n in cluster.nodes if n.role == "memory"])
    assert set(mx_per_group.values()) == {12}, "expected all nodegroups with mx to have 12 memx"

    wk_per_group = Counter([n.properties["group"] for n in cluster.nodes if n.role == "worker"])
    assert set(wk_per_group.values()) == {1}, "expected all nodegroups with wk to have 1 workers"

    mg_v2group = set([n.properties["v2Group"] for n in cluster.nodes if n.role == "management"])
    assert len(mg_v2group) == 2, "expected 2 total mg v2 groups to be set"

    # cluster_yaml = cluster.to_yaml()
    # print(cluster.to_yaml())

    # simulate adding nodes after cluster is deployed - remove every other worker and every 12th memx
    # resulting doc should "fill in the gaps" of node groups, creating an equivalent network doc
    nodes, names = [], set()
    mx_i, wk_i = 0, 0
    for node in cluster.nodes:
        if node.role == "memory":
            mx_i += 1
            if mx_i - 1 % 12 == 0:
                continue
        if node.role == "worker":
            wk_i += 1
            if wk_i - 1 % 2 == 0 and node.properties["group"] != "nodegroup0":
                continue
        nodes.append(node)
        names.add(node.name)
    cluster.nodes = nodes
    monkeypatch.setattr("common.models._find_assigned_ng_k8s", lambda: {"nodegroup0"})
    # remove one mx so that the nodegroup0 which is assigned to a session does not get updated
    for i, node in enumerate(netdoc.get("memoryx_nodes")):
        if node["name"] not in names:
            netdoc.get("memoryx_nodes").pop(i)
            break
    cluster_incremental = parse_cluster_config(netdoc, curr_cluster_config=cluster)
    mx_per_group = Counter([n.properties["group"] for n in cluster_incremental.nodes if n.role == "memory"])
    assert mx_per_group.get("nodegroup0") == 11
    assert set(mx_per_group.values()) == {11, 12}
    wk_per_group = Counter([n.properties["group"] for n in cluster_incremental.nodes if n.role == "worker"])
    assert set(wk_per_group.values()) == {1}, "expected all nodegroups with wk to have 1 workers"

    # remove all the workers from an m-leaf, then create a config, ensure its memx groups have a worker
    # then re-create the config with the workers in the m-leaf and ensure that it was updated
    netdoc = _generate_network_doc_v2()
    workers_orig = netdoc["worker_nodes"].copy()
    workers_reduced = [w for w in netdoc["worker_nodes"] if w["interfaces"][0]["switch_name"] != "sw_lf_5"]
    netdoc["worker_nodes"] = workers_reduced
    cluster = parse_cluster_config(netdoc)
    role_group = Counter([(n.role, n.properties["group"],) for n in cluster.nodes if n.role in {"worker", "memory"}])
    for (role, group,), count in role_group.items():
        if role == "memory":
            assert role_group[("worker", group,)] == 1, f"expected a worker assigned to group {group}"

    # now add the workers back but put a nodegroup in-use: ensure that only 1 worker has a v2 group specified
    netdoc["worker_nodes"] = workers_orig
    monkeypatch.setattr("common.models._find_assigned_ng_k8s", lambda: {"nodegroup3"})
    cluster = parse_cluster_config(netdoc, curr_cluster_config=cluster)
    old_worker = [n for n in cluster.nodes if n.role == "worker" and n.properties["group"] == "nodegroup3"]
    assert old_worker
    assert old_worker[0].properties["v2Group"] == "sw_lf_4"
    mx_per_group = Counter([n.properties["group"] for n in cluster.nodes if n.role == "memory"])
    assert mx_per_group.get("nodegroup0") == 12, mx_per_group

    # remove the memx from a nodegroup and then see that they're added back preferentially rather than
    # creating a new nodegroup with memx but not workers
    monkeypatch.setattr("common.models._find_assigned_ng_k8s", lambda: {"nodegroup0", "nodegroup3"})
    cluster.nodes = [n for n in cluster.nodes if n.properties.get("group") != "nodegroup0" or n.role != "memory"]
    cluster = parse_cluster_config(netdoc, curr_cluster_config=cluster)
    mx_per_group = Counter([n.properties["group"] for n in cluster.nodes if n.role == "memory"])
    assert mx_per_group.get("nodegroup0") == 12, mx_per_group
