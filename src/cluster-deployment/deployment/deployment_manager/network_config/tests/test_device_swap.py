import argparse
import json
import pathlib
import shutil

from deployment_manager.network_config.cli import AddNode, RemoveNode, RunPlacer
from deployment_manager.network_config.tests.network_builder import nano_stamp_builder

ASSETS_PATH = pathlib.Path(__file__).parent / "assets"


def test_node_swap(tmp_path):

    builder = nano_stamp_builder()
    builder.call_allocate_tiers(tmp_path)
    builder.call_placer(tmp_path)

    builder_nw_cfg = tmp_path / "network.json"
    test_nw_cfg = tmp_path / "network_config.json"
    shutil.copy(builder_nw_cfg, test_nw_cfg)

    test_node_name = "memoryx-net006-0"
    test_node_role = "memoryx_nodes"

    def _find_test_node_in_cfg():
        for n in json.loads(test_nw_cfg.read_text()).get("memoryx_nodes", []):
            if n['name'] == test_node_name:
                return n
        else:
            return None

    original_node_obj = _find_test_node_in_cfg()
    assert original_node_obj is not None, f"Node {test_node_name} not found"

    RemoveNode()(argparse.Namespace(
        name=test_node_name,
        config=str(test_nw_cfg),
    ))
    assert _find_test_node_in_cfg() is None, f"Node {test_node_name} not removed"

    AddNode()(argparse.Namespace(
        section=test_node_role,
        name=test_node_name,
        management_addr=None,
        nodegroup=None,
        username=None,
        password=None,
        rack=None,
        rack_unit=None,
        stamp=None,
        config=str(test_nw_cfg),
    ))
    assert _find_test_node_in_cfg() is not None, f"Node {test_node_name} not added"

    # manually update interfaces to simulate LLDP
    nw_cfg_obj = json.loads(test_nw_cfg.read_text())
    test_switch_name = None
    for n in nw_cfg_obj[test_node_role]:
        if n['name'] == test_node_name:
            n['interfaces'] = []
            for i in original_node_obj['interfaces']:
                n['interfaces'].append({
                    "name": i['name'],
                    "switch_name": i['switch_name'],
                    "switch_port": i['switch_port']
                })
                if test_switch_name is None:
                    test_switch_name = i['switch_name']
    test_nw_cfg.write_text(json.dumps(nw_cfg_obj))

    RunPlacer()(argparse.Namespace(
        no_switches=False,
        no_vips=False,
        no_xconnect=False,
        no_interfaces=False,
        config=str(test_nw_cfg),
    ))

    # compare IPs of memx nodes connected to the same switch
    # as the test node
    src_nw_cfg = json.loads(builder_nw_cfg.read_text())
    dest_nw_cfg = json.loads(test_nw_cfg.read_text())

    dest_interfaces = dict()
    for n in dest_nw_cfg.get(test_node_role, []):
        dest_interfaces[n['name']] = {i['name']: i['address'] for i in n['interfaces'] if i['switch_name'] == test_switch_name}

    for n in src_nw_cfg.get(test_node_role, []):
        for i in n['interfaces']:
            if i['switch_name'] != test_switch_name:
                continue
            assert i['address'] == dest_interfaces[n['name']][i['name']], f"IP address for {n['name']} changed after swap from {i['address']} to {dest_interfaces[n['name']][i['name']]}"
