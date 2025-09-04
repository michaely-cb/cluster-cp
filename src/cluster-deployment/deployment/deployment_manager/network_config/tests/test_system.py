from pathlib import Path
import pytest
from deployment_manager.network_config.tasks.system import update_system_port_switch
from deployment_manager.network_config.common.context import NetworkCfgDoc
from deployment_manager.network_config.cli.system import safe_load_system_connections
from deployment_manager.network_config.tests.network_builder import nano_stamp_builder, cs4_nano_stamp_builder


def test_system_iface_from_tier():
    builders = [nano_stamp_builder(), cs4_nano_stamp_builder()]

    def gather_system_interfaces(builder):
        return {s['name']: s['interfaces'] for s in builder.network["systems"]}

    def delete_system_switch_names(builder, name=""):
        remove_switch_names = []
        for sys in builder.network["systems"]:
            if not name or name == sys["name"]:
                infs = []
                for inf in sys["interfaces"]:
                    inf = inf.copy()
                    del inf["switch_name"]
                    infs.append(inf)
                sys["interfaces"] = infs
            remove_switch_names.append(sys)
        builder.network["systems"] = remove_switch_names

    for builder in builders:
        nw_doc = NetworkCfgDoc(builder.network)
        orig_infs = gather_system_interfaces(builder)
        # delete the switch names and then call update, check that they're equivalent
        delete_system_switch_names(builder)
        assert orig_infs != gather_system_interfaces(builder)
        update_system_port_switch(nw_doc, [])
        assert gather_system_interfaces(builder) == orig_infs

        delete_system_switch_names(builder, "xs00000")
        delete_system_switch_names(builder, "xs00001")
        update_system_port_switch(nw_doc, ["xs00000"])
        new_infs = gather_system_interfaces(builder)
        assert new_infs["xs00000"] == orig_infs["xs00000"]
        assert new_infs["xs00001"] != orig_infs["xs00001"]



def test_partial_system_ifaces(tmp_path):
    builder = nano_stamp_builder()
    network_doc = NetworkCfgDoc(builder.network)
    builder.call_allocate_tiers(tmp_path, vip_count=5)
    # remove a switch_name from one of the system_connections
    # and call placer and generate config
    conn = network_doc.raw()["system_connections"].pop()
    system_names = [s['name'] for s in network_doc.raw().get('systems', []) if s['name'] != conn['system_name']]
    update_system_port_switch(network_doc, system_names)
    builder.call_placer(tmp_path)
    builder.call_generate_cfg(tmp_path)


def test_load_system_connections(tmp_path):
    # invalid format
    p = Path(tmp_path) / "invalid.json"
    p.write_text('[{"system_name": "xxx"}]')
    with pytest.raises(ValueError):
        safe_load_system_connections(str(p), {"xxx"})

    # should filter
    p = Path(tmp_path) / "valid.json"
    p.write_text('[{"system_name": "n0", "system_port": "Port 1", "switch_name": "n1", "switch_port": "et20"}]')
    out = safe_load_system_connections(str(p), {"n0"})
    assert len(out) == 0

    # ok
    out = safe_load_system_connections(str(p), {"n0", "n1"})
    assert len(out) == 1
