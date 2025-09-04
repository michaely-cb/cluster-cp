import pytest

from common.models import parse_nic_csport


def test_parse_nic_csport_16_system_net_json(net_cfg_16_system: dict):
    """ Quick and dirty test that parses a full net.json file of a 16 CS2 cluster.
    """
    nic_csport = parse_nic_csport(net_cfg_16_system)
    num_br = len(net_cfg_16_system.get("swarmx_nodes", []))

    csport_nic = {i: [] for i in range(12)}
    for nic, port in nic_csport.items():
        csport_nic[port].append(nic)

    assert set([len(n) for n in csport_nic.values()]) == {num_br/2}, \
        f"expecting {num_br/2} NICs per each CS port"
    assert len(set([nic for nic in nic_csport])) == num_br * 6, \
        "expecting 6 nics per BR node"


_addr_ctr = 0


def _swarmx(name, switch_a, switch_b=None) -> dict:
    global _addr_ctr
    switches = [switch_a] * 6 if not switch_b else ([switch_a] * 3) + ([switch_b] * 3)
    rv = {"name": name,
          "interfaces": [{"name": f"i{i}", "switch_name": s, "address": f"127.0.0.{_addr_ctr + i}"}
                         for i, s in enumerate(switches)]}
    _addr_ctr += 6
    return rv


def _switches(*names):
    return {"switches": [{"name": name} for name in names]}


def _system(name, *switches):
    expand, s = int(12 / len(switches)), []
    for switch in switches:
        s += [switch] * expand
    return {
        "name": name,
        "interfaces": [{"name": f"data{i}", "switch_name": s}
                       for i, s in enumerate(s)]
    }


@pytest.mark.parametrize(
    "doc",
    [
        (  # 8cs cluster with 6 switches, ports 0,1 -> s0, 2,3 -> s1, etc...
                {"swarmx_nodes":
                     [_swarmx(f"br{i}", f"s{int(i / 2)}") for i in range(12)] +
                     [_swarmx(f"br{12 + i}", f"s{i}") for i in range(6)],
                 "systems": [_system(f"cs{i}", *[f"s{int(j / 2)}" for j in range(12)]) for i in range(8)],
                 **_switches(*[f"s{i}" for i in range(6)])
                 }
        ),
        (  # 2 cs2 cluster, 3 switches, ports 0,1,2,3 -> s0, 4,5,6,7 -> s2, etc...
                {"swarmx_nodes": [_swarmx(f"br{i}", f"s{int(i / 2)}") for i in range(6)],
                 "systems": [_system(f"cs{i}", *[f"s{int(j / 4)}" for j in range(12)]) for i in range(2)],
                 **_switches(*[f"s{i}" for i in range(3)])
                 }
        ),
    ],
)
def test_parse_nic_csport_shared_switches(doc):
    nic_csport = parse_nic_csport(doc)
    node_port = {nic.node: {} for nic, port in nic_csport.items()}
    for nic, port in nic_csport.items():
        node_port[nic.node][port] = node_port[nic.node].get(port, 0) + 1
    for node, port_count in node_port.items():
        assert len(port_count) in {1, 2}, "nodes expected to have 1 or 2 CS ports"
        for port, count in port_count.items():
            assert count in {3, 6}, "nodes expected to have 3 or 6 NICS assigned to a port"


def _doc_strange_nic_cs_port_balance():
    doc = {
        "swarmx_nodes": [_swarmx(f"br{i}", f"s{int(i / 2)}") for i in range(6)],
        "systems": [_system(f"cs{i}", *[f"s{int(j / 4)}" for j in range(12)]) for i in range(2)],
        **_switches(*[f"s{i}" for i in range(3)])
    }
    doc["swarmx_nodes"][3]["interfaces"][0]["switch_name"] = doc["swarmx_nodes"][0]["interfaces"][-1]["switch_name"]
    return doc


@pytest.mark.parametrize(
    "doc, error",
    [
        (
                # cs2's map port 0 to different switches
                {"swarmx_nodes": [_swarmx("br0", "s0")],
                 "systems": [_system("cs0", "s0"), _system("cs1", "s1")],
                 **_switches("s0", "s1"), },
                "expected exactly 1 switch connected with same CS port"
        ),
    ],
)
def test_parse_nic_csport_assert(doc, error):
    try:
        print(parse_nic_csport(doc))
    except AssertionError as e:
        assert error in str(e)
    else:
        assert False, f"expected assertion error not thrown: {error}"
