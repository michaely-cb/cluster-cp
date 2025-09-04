import pytest

from deployment_manager.network_config.cli.sanity import sanity_check
from deployment_manager.network_config.common import SystemModels
from deployment_manager.network_config.placer import SwitchVLANPlacer
from deployment_manager.network_config.schema.utils import is_spine_switch
from deployment_manager.network_config.tests.network_builder import LeafSpineNetworkBuilder, nano_stamp_builder


def test_placer_single_switch_cluster(tmp_path):
    # simulate a single switch cluster with 4 cs, similar to EEPC customer set up
    builder = LeafSpineNetworkBuilder(0)
    builder.add_stamp(system_count=4, sleaf_count=1, sx_per_leaf=8)
    builder.add_mleaf(mx=24, wk=4, mg=1, us=0, append=True)

    # invoke CLI commands to complete network configuration
    builder.network["environment"]["cluster_prefix"] = "10.0.0.0/21"
    builder.call_allocate_tiers(tmp_path, aw_size=1024, br_size=64, vip_count=8, aw_physical=196, system_prefix_size=128)
    builder.call_placer(tmp_path)

    result = sanity_check(builder.network)
    assert result.is_ok(), f"sanity check failed: {result}"


def test_placer_single_switch_cs4_cluster(tmp_path):
    # simulate a single switch cluster with 4 cs4s, similar to EEPC customer set up
    builder = LeafSpineNetworkBuilder(0)
    builder.add_stamp(system_count=4, sleaf_count=1, sx_per_leaf=8, system_model=SystemModels.CS4.value)
    builder.add_mleaf(mx=24, wk=4, mg=1, us=0, append=True)

    # invoke CLI commands to complete network configuration
    builder.network["environment"]["cluster_prefix"] = "10.0.0.0/16"
    builder.call_allocate_tiers(tmp_path, aw_size=1024, br_size=64, vip_count=8, aw_physical=196, system_prefix_size=256)
    builder.call_placer(tmp_path)

    result = sanity_check(builder.network)
    assert result.is_ok(), f"sanity check failed: {result}"


def test_placer_mb306(tmp_path):
    # simulate a 2 spine, 3 sleaf, 1mleaf cluster
    builder = LeafSpineNetworkBuilder(2)
    builder.add_stamp(system_count=18, sleaf_count=3, sx_per_leaf=12)
    builder.add_mleaf(mx=60, wk=18, mg=4, us=4)

    # invoke CLI commands to complete network configuration
    builder.network["environment"]["cluster_prefix"] = "10.0.0.0/19"
    builder.call_allocate_tiers(tmp_path, aw_size=1024, br_size=256, vip_count=128, aw_physical=196)

    # simulate a bug where AW vlan offset was used in v2 topology
    for sw in builder.network["switches"]:
        if sw["tier"] == "LF":
            sw[SwitchVLANPlacer.VLAN_NAME] = SwitchVLANPlacer.AW_BR_VLAN_START
            break

    builder.call_placer(tmp_path)

    result = sanity_check(builder.network)
    assert result.is_ok(), f"sanity check failed: {result}"

    for sw in builder.network["switches"]:
        if sw["tier"] == "LF":
            assert sw[SwitchVLANPlacer.VLAN_NAME] % 10 == SwitchVLANPlacer.AW_BR_VLAN_START % 10


def test_placer_cg6(tmp_path):
    # simulate CG6 (which is the first part of a larger cluster of total 8 CGs
    builder = LeafSpineNetworkBuilder(4, sx_interface_count=4, xconnect_per_spine=4)
    builder.add_stamp(system_count=68, sleaf_count=12, sx_per_leaf=12)
    builder.add_mleaf(mx=24, wk=0, mg=2, us=0) # mxxl
    for j in range(2):  # mxl
        builder.add_mleaf(mx=24, wk=0, mg=2, us=0)
    for j in range(2):  # base mx
        builder.add_mleaf(mx=36, wk=34, mg=1, us=0)
    builder.add_mleaf(mx=0, wk=0, mg=0, us=36)  # usernode rack

    # invoke CLI commands to complete network configuration
    builder.network["environment"]["cluster_prefix"] = "10.0.0.0/12"
    builder.call_allocate_tiers(tmp_path, aw_size=1024, br_size=64, system_prefix_size=256, vip_count=1024, aw_physical=196)
    builder.call_placer(tmp_path)

    result = sanity_check(builder.network, node_expected_nic_count={"SX": 4})
    assert result.is_ok(), f"sanity check failed: {result}"


def test_placer_inference_cluster(tmp_path):
    # simulate an inference-like cluster
    builder = LeafSpineNetworkBuilder(4)
    builder.add_stamp(system_count=88, sleaf_count=6, sx_per_leaf=0, id_per_leaf=2,
                      round_robin_node_links=False, leaf_per_rack=6)
    builder.add_mleaf(mg=24, us=2)

    # invoke CLI commands to complete network configuration
    builder.network["environment"]["cluster_prefix"] = "10.0.0.0/16"
    builder.call_allocate_tiers(tmp_path,
                                aw_size=2048, br_size=0,
                                vip_count=128, aw_physical=196,
                                system_prefix_size=512, xconn_count=2096)
    builder.call_placer(tmp_path)

    result = sanity_check(builder.network)
    assert result.is_ok(), f"sanity check failed: {result}"

    # simulate adding a second cluster to the same network
    builder.add_cluster()
    builder.call_placer(tmp_path)
    result = sanity_check(builder.network)
    assert result.is_ok(), f"sanity check failed: {result}"
    assert builder.network["clusters"][1]["data_network"]["vip"], "VIP not allocated for second cluster"


@pytest.mark.skip("takes a long time to run")
def test_placer_cgx_544_systems(tmp_path):
    builder = LeafSpineNetworkBuilder(4, sx_interface_count=4, xconnect_per_spine=4)
    for i in range(8):
        builder.add_stamp(system_count=68, sleaf_count=12, sx_per_leaf=12)
    for i in range(2):
        for j in range(6):  # mxxl
            builder.add_mleaf(mx=24, wk=0, mg=2, us=0)
        for j in range(6):  # mxl
            builder.add_mleaf(mx=24, wk=0, mg=2, us=0)
        for j in range(8):  # base mx
            builder.add_mleaf(mx=36, wk=34, mg=1, us=0)

    # invoke CLI commands to complete network configuration
    builder.network["environment"]["cluster_prefix"] = "10.0.0.0/12"
    builder.call_allocate_tiers(tmp_path, aw_size=1024, br_size=64, system_prefix_size=256, vip_count=1024, aw_physical=196)
    builder.call_placer(tmp_path)

    result = sanity_check(builder.network, node_expected_nic_count={"SX": 4})
    assert result.is_ok(), f"sanity check failed: {result}"



def test_place_after_switch_removal(tmp_path):
    """ Test spine replacement flow """
    builder = LeafSpineNetworkBuilder(6)
    builder.add_stamp(system_count=4, sleaf_count=1, sx_per_leaf=9)
    builder.add_stamp(system_count=4, sleaf_count=1, sx_per_leaf=9)
    builder.add_mleaf(mx=12, wk=2, mg=4, us=1)
    builder.call_allocate_tiers(tmp_path)
    builder.call_placer(tmp_path)

    def collect_vlans(network) -> dict:
        switch_vlan = {}
        for sw in network['switches']:
            switch_vlan[f"{sw['name']}:vlan"] = sw['vlan']
            if "system_vlan" in sw:
                switch_vlan[f"{sw['name']}:system_vlan"] = sw['system_vlan']
            if "swarmx_vlan" in sw:
                switch_vlan[f"{sw['name']}:swarmx_vlan"] = sw['swarmx_vlan']
        return switch_vlan

    def find_changed_vlans(before: dict, after: dict) -> list:
        rv = []
        for vlan_key, vlan_id in after.items():
            if vlan_key not in before:
                continue
            if before[vlan_key] != vlan_id:
                rv.append(f"{vlan_key} changed from {before[vlan_key]} to {vlan_id}")
        return rv

    # test placer re-run after device removal / add. Remove half the spines, then re-add spines
    vlans_before = collect_vlans(builder.network)
    spine_names = [s["name"] for s in builder.spines]
    xconnect_count = len(builder.network["xconnect"]["connections"])
    spines0 = spine_names[0:len(spine_names) // 2]
    builder.call_remove_resources(tmp_path, switch_names=spines0)
    assert len(builder.network["xconnect"]["connections"]) == xconnect_count // 2
    assert len([s for s in builder.network["switches"] if s["tier"] == "SP"]) == len(spine_names) - len(spines0)
    builder.call_placer(tmp_path)
    vlans_after0 = collect_vlans(builder.network)
    assert not find_changed_vlans(vlans_before, vlans_after0)

    builder.add_spines(3)
    for sw in builder.network["switches"]:
        if sw["tier"] == "LF":
            builder.add_xconnects(sw)
            builder.add_xconnects(sw)
    builder.call_placer(tmp_path)

    vlans_after1 = collect_vlans(builder.network)
    assert not find_changed_vlans(vlans_before, vlans_after1)
    assert not find_changed_vlans(vlans_after0, vlans_after1)

    result = sanity_check(builder.network)
    assert result.is_ok(), f"sanity check failed: {result}"
    builder.call_generate_cfg(tmp_path)

    # remove some other resources for coverage
    memx_names = [n["name"] for n in builder.network["memoryx_nodes"]]
    system_names = [n["name"] for n in builder.network["systems"]]
    builder.call_remove_resources(tmp_path, node_names=memx_names[0:5], system_names=system_names[0:2])
    builder.call_placer(tmp_path)

    assert len(builder.network["memoryx_nodes"]) == len(memx_names) - 5
    assert len(builder.network["systems"]) == len(system_names) - 2
    assert len(builder.network["system_connections"]) == (len(system_names) - 2) * 12


def test_rack_addition(tmp_path):
    builder = nano_stamp_builder()
    builder.call_allocate_tiers(tmp_path, vip_count=5)
    builder.call_placer(tmp_path)

    prefixes = dict(
        switches=dict(),
        xconnect=dict()
    )
    for s in builder.network['switches']:
        if is_spine_switch(s):
            continue
        prefixes['switches'][s['name']] = {
            'prefix': s['prefix'],
            'system_prefix': s['system_prefix'],
            'swarmx_prefix': s['swarmx_prefix'],
            'virtual_addrs': s['virtual_addrs'],
        }
    for xc in builder.network['xconnect']['connections']:
        prefixes['xconnect'][xc['name']] = xc['prefix']

    builder.add_mleaf(mx=12, wk=2, mg=4, us=1)
    builder.add_stamp()
    builder.call_placer(tmp_path)
    builder.add_stamp()
    builder.call_placer(tmp_path)

    for s in builder.network['switches']:
        if is_spine_switch(s):
            continue
        # existing switch
        if s['name'] in prefixes['switches']:
            assert s['prefix'] == prefixes['switches'][s['name']]['prefix'], \
                (f"Prefix for {s['name']} has changed from "
                f"{prefixes['switches'][s['name']]['prefix']} to {s['prefix']}")
            assert s['system_prefix'] == prefixes['switches'][s['name']]['system_prefix'], \
                (f"System prefix for {s['name']} has changed from "
                 f"{prefixes['switches'][s['name']]['system_prefix']} to {s['system_prefix']}")
            assert s['swarmx_prefix'] == prefixes['switches'][s['name']]['swarmx_prefix'], \
                (f"Swarmx prefix for {s['name']} has changed from "
                 f"{prefixes['switches'][s['name']]['swarmx_prefix']} to {s['swarmx_prefix']}")
            assert s['virtual_addrs'] == prefixes['switches'][s['name']]['virtual_addrs'], \
                (f"Virtual addr range for {s['name']} has changed from "
                 f"{prefixes['switches'][s['name']]['virtual_addrs']} to {s['virtual_addrs']}")
        else:
            assert s.get('prefix') is not None, f"Prefix not allocated for {s['name']}"
            assert s.get('system_prefix') is not None, f"System prefix not allocated for {s['name']}"
            assert s.get('swarmx_prefix') is not None, f"Swarmx prefix not allocated for {s['name']}"
            assert s.get('virtual_addrs') is not None, f"Virtual addr range not allocated for {s['name']}"


def test_place_virtual_addr_range_immutable(tmp_path):
    # test adding more nodes to the cluster in an existing rack, growing the physical address reservation too large
    # and triggering the virtual address allocation to fail
    builder = LeafSpineNetworkBuilder(1)
    builder.add_mleaf(mx=12)
    builder.call_allocate_tiers(tmp_path)
    for tier in builder.network["tiers"]:
        tier["min_physical"] = 24  # have to set this outside the allocator since it's overridden with min default
    builder.call_placer(tmp_path)

    # add another node which should exceed the virtual address range
    builder.add_mleaf(mx=1, append=True)
    with pytest.raises(AssertionError) as e:
        builder.call_placer(tmp_path)
    assert "virtual address range falls outside the allowed virtual address range" in str(e.value)
