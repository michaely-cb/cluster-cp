import ipaddress

import pytest

from common import DockerImage
from common.models import parse_cluster_config, split_str_int, Cluster


@pytest.mark.parametrize(
    "image, repo, tag",
    [
        ("docker.io/lib/xyz:latest", "docker.io/lib/xyz", "latest"),
        ("registry.k8s.io/abc:marc-123", "registry.k8s.io/abc", "marc-123"),
        # handle trailing, leading whitespace
        (" registry.k8s.io/abc:marc-123", "registry.k8s.io/abc", "marc-123"),
        ("registry.k8s.io/abc:marc-123\n", "registry.k8s.io/abc", "marc-123"),
    ],
)
def test_image_valid(image, repo, tag):
    img = DockerImage.of(image)
    assert repo == img.repo
    assert tag == img.tag


@pytest.mark.parametrize(
    "image", [("docker.io/abc",), ("",), ],
)
def test_image_invalid(image):
    try:
        DockerImage.of(image)
    except ValueError:
        pass
    else:
        assert False, "expected exception was not thrown"


def _generate_node(
        role: str, rack, replica: int, ip: ipaddress.IPv4Address, switch_name=""
) -> dict:
    int_count = 1
    if role == "swarmx":
        int_count = 6
    elif role == "management":
        int_count = 2
    if not switch_name:
        switch_name = "sw-br-0" if role == "swarmx" else f"sw-memx-{rack}"
    return {
        "name": f"r{rack}-{role}{replica}",
        "interfaces": [
            {
                "address": str(ip + i) + "/24",
                "name": f"eth100g{i}",
                "switch_name": switch_name,
            }
            for i in range(int_count - 1, -1, -1)
        ],
        "nodegroup": rack,
    }


def _generate_network_doc_v1(system_count=4, worker_per=2, memx_per=2, mgmt_per=1, br=30) -> dict:
    """Generate a partially complete network doc to test network doc conversion"""
    parentnet = ipaddress.ip_network("10.0.0.0/20")
    memx_subnet = ipaddress.ip_network("10.0.0.0/24")
    br_subnet = ipaddress.ip_network("10.0.16.0/24")

    doc = {
        "switches": [],
        "worker_nodes": [],
        "memoryx_nodes": [],
        "management_nodes": [],
        "swarmx_nodes": [],
        "systems": [],
        "environment": {
            "cluster_prefix": str(parentnet),
            "service_prefix": "192.0.128.0/17"
        },
    }

    ip_memx = memx_subnet.network_address + 1
    ip_br = br_subnet.network_address + 1

    for i in range(system_count):
        doc["systems"].append(
            {
                "name": f"systemf{i}",
                "vip": str(ip_memx) + "/24",
                "interfaces": [
                    {
                        "address": str(ip_br + j) + "/24",
                        "name": f"data{j}",
                        "switch_name": "sw-br-0",
                    }
                    for j in range(12)
                ],
                "nodegroup": i,
            }
        )
        ip_br += 12
        ip_memx += 1
        for j in range(mgmt_per):
            doc["management_nodes"].append(_generate_node("management", i, j, ip_memx))
            ip_memx += 2
        for j in range(worker_per):
            doc["worker_nodes"].append(_generate_node("worker", i, j, ip_memx))
            ip_memx += 1
        for j in range(memx_per):
            doc["memoryx_nodes"].append(_generate_node("memory", i, j, ip_memx))
            ip_memx += 1
        doc["switches"].append(
            {
                "address": str(memx_subnet.broadcast_address - 1) + "/24",
                "asn": f"65001.{i}",
                "name": f"sw-memx-{i}",
                "prefix": str(memx_subnet),
                "tier": "AW",
                "virtual_addrs": {
                    "starting_address": str(memx_subnet.network_address + 128) + "/24",
                    "ending_address": str(memx_subnet.broadcast_address - 2)
                                      + "/24",
                },
            }
        )

        memx_subnet = ipaddress.ip_network(
            (memx_subnet.network_address + 256).exploded + "/24"
        )
        ip_memx = memx_subnet.network_address + 1

    for j in range(br):
        doc["swarmx_nodes"].append(_generate_node("swarmx", i, j, ip_br))
        ip_br += 6
    doc["switches"].append(
        {
            "address": str(br_subnet.broadcast_address - 1) + "/24",
            "asn": "65001.128",
            "name": "sw-br-0",
            "prefix": str(br_subnet),
            "tier": "BR",
        }
    )

    return doc


def _generate_network_doc_v2(systems=8, s_leafs=4, m_leafs=2, act_per=4, worker_per=4, br_per=6, memx_per=24, mgmt_per=2) -> dict:
    """Generate a partially complete network doc to test network doc conversion"""
    parentnet = ipaddress.ip_network("10.0.0.0/20")
    memx_parentnet = ipaddress.ip_network("10.0.0.0/21")
    memx_subnets = [s for s in memx_parentnet.subnets(new_prefix=24)][0:s_leafs + m_leafs]
    memx_next_ip = [nw.network_address + 1 for nw in memx_subnets]
    br_parentnet = ipaddress.ip_network("10.0.16.0/22")
    br_subnets = [s for s in br_parentnet.subnets(new_prefix=24)][0:s_leafs]
    br_next_ip = [nw.network_address + 1 for nw in br_subnets]

    doc = {
        "switches": [],
        "worker_nodes": [],
        "memoryx_nodes": [],
        "activation_nodes": [],
        "management_nodes": [],
        "swarmx_nodes": [],
        "systems": [],
        "environment": {
            "cluster_prefix": str(parentnet),
            "service_prefix": "192.0.128.0/17",
            "topology": "leaf_spine",
        },
    }

    # create s leafs switches
    for i in range(s_leafs):
        switch_name = f"sw_lf_{i}"
        doc["switches"].append(
            {
                "address": str(memx_next_ip[i]) + "/24",
                "asn": f"65001.{i}",
                "name": switch_name,
                "prefix": str(memx_subnets[i]),
                "tier": "LF",
                "virtual_addrs": {
                    "starting_address": str(memx_subnets[i].network_address + 128) + "/24",
                    "ending_address": str(memx_subnets[i].broadcast_address - 2) + "/24",
                },
                "swarmx_prefix": str(br_subnets[i]),
                "swarmx_vlan_address": f"{br_subnets[i].broadcast_address - 1}",
                "swarmx_asn": f"65002.{i}",
                "swarmx_virtual_addrs": {
                    "starting_address": str(br_subnets[i].network_address + 128),
                    "ending_address": str(br_subnets[i].broadcast_address - 2) + "/24"
                }
            }
        )
        memx_next_ip[i] += 1

    # add systems to sleafs
    for i in range(systems):
        doc["systems"].append(
            {
                "name": f"xs100{i}",
                "vip": str(br_next_ip[0]) + "/24",
                "interfaces": [],
                "nodegroup": i,
            }
        )
        br_next_ip[0] += 1
        for j in range(12):
            s_leaf_i = int(j / int(12 / s_leafs))
            a = br_next_ip[s_leaf_i]
            br_next_ip[s_leaf_i] += 1
            doc["systems"][-1]["interfaces"].append(
                {
                    "address": str(a) + "/24",
                    "name": f"data{j}",
                    "switch_name": f"sw_lf_{s_leaf_i}",
                }
            )

    # add activations + br to sleafs
    for i in range(s_leafs):
        li, ri = int(i / 2) * 2, int(i / 2) * 2 + 1
        left_switch, right_switch = f"sw_lf_{li}", f"sw_lf_{ri}"
        for j in range(act_per):
            doc["activation_nodes"].append({
                "name": f"act-{i}-{j}",
                "interfaces": [
                    {
                        "address": str(memx_next_ip[li]) + "/24",
                        "name": "eth100g0",
                        "switch_name": left_switch,
                    },
                    {
                        "address": str(memx_next_ip[ri]) + "/24",
                        "name": "eth100g1",
                        "switch_name": right_switch,
                    }
                ],
            })
            memx_next_ip[li] += 1
            memx_next_ip[ri] += 1
        for j in range(br_per):
            doc["swarmx_nodes"].append({
                "name": f"br-{i}-{j}",
                "interfaces": [
                    {
                        "address": str(br_next_ip[i]) + "/24",
                        "name": f"eth100g{k}",
                        "switch_name": f"sw_lf_{i}",
                    } for k in range(6)
                ],
            })
            br_next_ip[i] += 6

    # create m leafs switches
    for i in range(s_leafs, s_leafs + m_leafs):
        switch_name = f"sw_lf_{i}"
        doc["switches"].append(
            {
                "address": str(memx_next_ip[i] + 1) + "/24",
                "asn": f"65003.{i}",
                "name": switch_name,
                "prefix": str(memx_parentnet),
                "tier": "LF",
                "virtual_addrs": {
                    "starting_address": str(memx_subnets[i].network_address + 128) + "/24",
                    "ending_address": str(memx_subnets[i].broadcast_address - 2) + "/24",
                }
            }
        )
        memx_next_ip[i] += 1

        for j in range(mgmt_per):
            doc["management_nodes"].append(_generate_node("management", i, j, memx_next_ip[i], switch_name=switch_name))
            memx_next_ip[i] += 2
        for j in range(worker_per):
            doc["worker_nodes"].append(_generate_node("worker", i, j, memx_next_ip[i], switch_name=switch_name))
            memx_next_ip[i] += 1
        for j in range(memx_per):
            doc["memoryx_nodes"].append(_generate_node("memory", i, j, memx_next_ip[i], switch_name=switch_name))
            memx_next_ip[i] += 1
    return doc


def _remove_all_groups(doc: dict) -> dict:
    for k in [
        "memoryx_nodes",
        "worker_nodes",
        "management_nodes",
        "swarmx_nodes",
    ]:
        for node in doc[k]:
            del node["nodegroup"]
    return doc


def _remove_half_of_network_interfaces(doc: dict) -> dict:
    for k in ["memoryx_nodes", "worker_nodes", "management_nodes"]:
        seen_group = set()
        for node in doc[k]:
            if node["nodegroup"] in seen_group:
                continue
            seen_group.add(node["nodegroup"])
            node["interfaces"] = []
    # half swarmx have no interface, half have <6 interfaces (which should not result in node filtering)
    swarmx_count = len(doc["swarmx_nodes"])
    for i, node in enumerate(doc["swarmx_nodes"]):
        if i < swarmx_count / 2:
            node["interfaces"] = []
        else:
            node["interfaces"] = node["interfaces"][0:4]
    return doc


def _remove_all_but_one_mgmt(doc: dict) -> dict:
    doc["management_nodes"] = doc["management_nodes"][:1]
    return doc


def _mismatch_groups(doc: dict) -> dict:
    # make the generated network.json doc invalid by changing the nodegroup to
    # a switch which shouldn't own that nodegroup
    doc["worker_nodes"][0]["nodegroup"] = 1
    return doc


parse_network_doc_testcases = {
    "with_nodegroups": {
        "doc": _generate_network_doc_v1(),
        "expected_replica_counts": {
            "nodegroup0": {"worker": 2, "memory": 2, "management": 1, },
            "nodegroup1": {"worker": 2, "memory": 2, "management": 1, },
            "nodegroup2": {"worker": 2, "memory": 2, "management": 1, },
            "nodegroup3": {"worker": 2, "memory": 2, "management": 1, },
            "*": {"broadcastreduce": 30, },
        },
        "expected_controlplane_count": 3,
        "expected_switch_config": {
            "sw-memx-0": {
                "virtualStart": "10.0.0.128",
                "virtualEnd": "10.0.0.253",
            },
            "sw-memx-1": {
                "virtualStart": "10.0.1.128",
                "virtualEnd": "10.0.1.253",
            },
        },
    },
    "without_nodegroups": {
        "doc": _remove_all_groups(_generate_network_doc_v1()),
        "expected_replica_counts": {
            "nodegroup0": {"worker": 2, "memory": 2, "management": 1, },
            "nodegroup1": {"worker": 2, "memory": 2, "management": 1, },
            "nodegroup2": {"worker": 2, "memory": 2, "management": 1, },
            "nodegroup3": {"worker": 2, "memory": 2, "management": 1, },
            "*": {"broadcastreduce": 30, },
        },
        "expected_controlplane_count": 3,
    },
    "single_mgmt_node": {
        "doc": _remove_all_but_one_mgmt(_generate_network_doc_v1()),
        "expected_controlplane_count": 1,
    },
    "filter_missing_interfaces": {
        "doc": _remove_half_of_network_interfaces(_generate_network_doc_v1()),
        "expected_replica_counts": {
            "nodegroup0": {"worker": 1, "memory": 1, },
            "nodegroup1": {"worker": 1, "memory": 1, },
            "nodegroup2": {"worker": 1, "memory": 1, },
            "nodegroup3": {"worker": 1, "memory": 1, },
            "*": {"broadcastreduce": 15, },
        },
    },
    "mismatch_nodegroup_error": {
        "doc": _mismatch_groups(_generate_network_doc_v1()),
        "error": ValueError,
    },
}


@pytest.mark.parametrize(
    "testcase",
    list(parse_network_doc_testcases.values()),
    ids=list(parse_network_doc_testcases.keys()),
)
def test_parse_network_doc(testcase):
    error = None
    try:
        cluster = parse_cluster_config(testcase["doc"])
    except Exception as e:
        error = e

    if "error" in testcase:
        assert isinstance(
            error, testcase["error"]
        ), f"expected {testcase['error']} was not thrown"
        return

    assert error is None, f"unexpected error thrown: {error}"
    assert "topology" not in cluster.properties.get("properties", {})

    if "expected_replica_counts" in testcase:
        actual_replica_counts = {}
        for node in cluster.nodes:
            group = node.properties.get("group", "*")
            group_counts = actual_replica_counts.get(group, {})
            group_counts[node.role] = group_counts.get(node.role, 0) + 1
            actual_replica_counts[group] = group_counts
        assert (
                actual_replica_counts == testcase['expected_replica_counts']
        ), "parsed nodes group->replica counts did not match expected replica counts"

    assert cluster.properties["serviceSubnet"] == "192.0.128.0/17"

    if "expected_switch_config" in testcase:
        sw = {ng.properties['switch']: ng.switchConfig for ng in cluster.groups}
        for name, values in testcase["expected_switch_config"].items():
            s = sw[name]
            for k, v in values.items():
                assert (
                        getattr(s, k) == v
                ), f"expected switch {name}.{k} == {v} but was {getattr(s, k)}"

    if "expected_controlplane_count" in testcase:
        cpnodes = [n for n in cluster.nodes if
                   n.role == "management" and n.properties.get("controlplane", "") == "true"]
        assert len(cpnodes) == testcase[
            "expected_controlplane_count"], f"expected {testcase['expected_controlplane_count']}, got {len(cpnodes)}"

    for n in cluster.nodes:
        is_sorted = all(
            (a.csPort, a.name,) <= (b.csPort, b.name,) for a, b in zip(n.networkInterfaces, n.networkInterfaces[1:]))
        assert is_sorted, f"node {n.name} did not have sorted nics: {n.networkInterfaces}"


def test_split_nodegroups():
    # clear nodes from last 2 nodegroups
    net_doc = _generate_network_doc_v1(6, 2, 12, 1, 30)
    net_doc["worker_nodes"] = [n for n in net_doc["worker_nodes"] if n["nodegroup"] < 4]
    net_doc["memoryx_nodes"] = [n for n in net_doc["memoryx_nodes"] if n["nodegroup"] < 4]
    net_doc["management_nodes"] = [n for n in net_doc["management_nodes"] if n["nodegroup"] < 4]
    cluster = parse_cluster_config(net_doc)
    assert len(cluster.groups) == 4

    # split, ensuring there are now 2 populated and 4 depopulated groups
    old, new = cluster.split_groups()
    assert old == ["nodegroup3", "nodegroup2"]
    assert new == ["nodegroup4", "nodegroup5"]

    # split again...
    old, new = cluster.split_groups(2)
    assert old == ["nodegroup1", "nodegroup0"]
    assert new == ["nodegroup6", "nodegroup7"]

    # split again...
    old, new = cluster.split_groups(2)
    assert old == []
    assert new == []

    # try again but with some groups having only 8 memx
    def name_index(node) -> int:
        return split_str_int(node["name"])[1]

    net_doc = _generate_network_doc_v1(6, 2, 12, 1, 30)
    net_doc["worker_nodes"] = [n for n in net_doc["worker_nodes"] if n["nodegroup"] < 4]
    net_doc["memoryx_nodes"] = [n for n in net_doc["memoryx_nodes"] if
                                n["nodegroup"] < 4 and (n["nodegroup"] > 1 or name_index(n) < 8)]
    net_doc["management_nodes"] = [n for n in net_doc["management_nodes"] if n["nodegroup"] < 4]

    cluster = parse_cluster_config(net_doc)
    assert len(cluster.groups) == 4
    assert len([n.name for n in cluster.nodes if n.properties.get("group") == "nodegroup0" and n.role == "memory"]) == 8
    old, new = cluster.split_groups()
    assert old == ["nodegroup1", "nodegroup0"]
    assert new == ["nodegroup4", "nodegroup5"]


def test_keep_systemAffinity():
    # Test maintaining systemAffinity in existing cluster.yaml
    num_systems = 6
    net_doc = _generate_network_doc_v1(num_systems, 2, 12, 1, 30)
    cluster_config = {
        "name": "my_cluster",
        "systems": [],
        "nodes": [],
        "groups": [],
    }
    for i in range(num_systems):
        cluster_config["groups"].append({
            "name": f"nodegroup{i}",
            "properties": {
                "systemAffinity": f"systemf{(i + 1) % num_systems}"
            }
        })

    cluster = parse_cluster_config(net_doc, Cluster(**cluster_config))
    for i in range(len(cluster.groups)):
        group = cluster.groups[i]
        assert group.properties["systemAffinity"] == f"systemf{(i + 1) % num_systems}", \
            str(f"nodegroup{i} should have systemAffinity set to systemf{(i + 1) % num_systems}, "
                f"seeing {group.properties['systemAffinity']}")


def test_system_swap():
    # Test system swap in existing cluster.yaml
    num_systems = 6
    net_doc = _generate_network_doc_v1(num_systems, 2, 12, 1, 30)
    cluster_config = {
        "name": "my_cluster",
        "systems": [],
        "nodes": [],
        "groups": [],
    }

    net_doc["systems"][0]["name"] = f"systemf{num_systems}"
    for i in range(num_systems):
        cluster_config["groups"].append({
            "name": f"nodegroup{i}",
            "properties": {
                "systemAffinity": f"systemf{i}"
            }
        })

    cluster = parse_cluster_config(net_doc, Cluster(**cluster_config))
    assert cluster.groups[0].properties["systemAffinity"] == f"systemf{num_systems}", \
        str(f"nodegroup0 should have systemAffinity set to systemf{num_systems}, seeing "
            f"{cluster.groups[0].properties['systemAffinity']}")


def test_keep_control_plane_nodes():
    # Test keeping control plane nodes in existing cluster.yaml
    num_systems = 6
    net_doc = _generate_network_doc_v1(num_systems, 2, 12, 1, 30)
    cluster_config = {
        "name": "my_cluster",
        "systems": [],
        "nodes": [],
        "groups": [],
    }
    control_plane_nodes = set()
    for rack in range(int(num_systems / 2)):
        cluster_config["nodes"].append({
            "name": f"r{rack * 2}-management0",
            "role": "management",
            "properties": {
                "controlplane": "true",
                "group": f"nodegroup{rack * 2}",
            }
        })
        control_plane_nodes.add(f"r{rack * 2}-management0")

    cluster = parse_cluster_config(net_doc, Cluster(**cluster_config))
    for node in cluster.nodes:
        if node.name in control_plane_nodes:
            assert node.properties["controlplane"] == "true", \
                f"node {node.name} is a control plane node"
        else:
            assert "controlplane" not in node.properties or node.properties["controlplane"] != "true", \
                f"node {node.name} is not a control plane node"


def test_keep_ceph_nodes():
    # Test keeping ceph nodes in existing cluster.yaml
    num_systems = 6
    net_doc = _generate_network_doc_v1(num_systems, 2, 12, 1, 30)
    cluster_config = {
        "name": "my_cluster",
        "systems": [],
        "nodes": [],
        "groups": [],
    }
    ceph_nodes = set()
    for rack in range(int(num_systems / 2)):
        cluster_config["nodes"].append({
            "name": f"r{rack * 2}-management0",
            "role": "management",
            "properties": {
                "storage-type": "ceph",
                "group": f"nodegroup{rack * 2}",
            }
        })
        ceph_nodes.add(f"r{rack * 2}-management0")

    cluster = parse_cluster_config(net_doc, Cluster(**cluster_config))
    for node in cluster.nodes:
        if node.name in ceph_nodes:
            assert node.properties["storage-type"] == "ceph", \
                f"node {node.name} is a ceph node"
        else:
            assert "storage-type" not in node.properties or node.properties["storage-type"] != "ceph", \
                f"node {node.name} is not a ceph node"


def test_keep_nvme_of_nodes():
    # Test keeping nvme-of targets and initiators in existing cluster.yaml
    num_systems = 6
    net_doc = _generate_network_doc_v1(num_systems, 2, 12, 1, 30)
    cluster_config = {
        "name": "my_cluster",
        "systems": [],
        "nodes": [],
        "groups": [],
    }
    targets = {}
    initiators = {}
    for rack in range(int(num_systems / 2)):
        cluster_config["nodes"].append({
            "name": f"r{rack * 2}-management0",
            "role": "management",
            "properties": {
                "nvme-of-target-id": f"{rack * 2}",
                "nvme-per-disk-bytes": "999",
                "group": f"nodegroup{rack * 2}",
            }
        })
        targets[f"r{rack * 2}-management0"] = f"{rack * 2}"

        cluster_config["nodes"].append({
            "name": f"r{rack * 2}-memory0",
            "role": "memory",
            "properties": {
                "nvme-of-initiator": "",
                "group": f"nodegroup{rack * 2}",
            }
        })
        initiators[f"r{rack * 2}-memory0"] = ""

    cluster = parse_cluster_config(net_doc, Cluster(**cluster_config))
    for node in cluster.nodes:
        if node.name in targets:
            assert node.properties["nvme-of-target-id"] == targets[node.name], \
                f"node {node.name} is an nvme-of target"
            assert node.properties["nvme-per-disk-bytes"] == "999", \
                f"node {node.name} should have 999 bytes disk space"
        elif node.name in initiators:
            assert node.properties["nvme-of-initiator"] == "", \
                f"node {node.name} is an nvme-of initiator"
        else:
            assert "nvme-of-target-id" not in node.properties and "nvme-of-initiator" not in node.properties, \
                f"node {node.name} is neither an nvme-of target nor initiator"
