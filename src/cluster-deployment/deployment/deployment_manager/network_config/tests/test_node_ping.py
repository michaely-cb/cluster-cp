import json

from deployment_manager.network_config.common.context import NetworkCfgDoc, OBJ_TYPE_SR, OBJ_TYPE_SY, NetworkCfgCtx
from deployment_manager.network_config.tasks.node import NodePingTestNetworkTask, DEFAULT_MTU
from deployment_manager.network_config.tasks.node_ping import NodePingTestGenerator, PingTestKind
from deployment_manager.network_config.tests.network_builder import LeafSpineNetworkBuilder, nano_stamp_builder


def test_ping_generator(tmp_path):
    # create an 8 system cluster with 2 stamps
    builder = LeafSpineNetworkBuilder(1)
    builder.add_stamp(system_count=4, sleaf_count=1, sx_per_leaf=2)
    builder.add_stamp(system_count=4, sleaf_count=1, sx_per_leaf=2)
    builder.add_mleaf(mx=12, wk=4, mg=4, us=1)

    # invoke CLI commands to complete network configuration
    builder.network["environment"]["cluster_prefix"] = "10.0.0.0/19"
    builder.call_allocate_tiers(tmp_path, aw_size=1024, br_size=256, vip_count=128, aw_physical=196)
    builder.call_placer(tmp_path)

    fp = f"{tmp_path}/network_config.json"
    with open(fp, 'w') as f:
        f.write(json.dumps(builder.network))
    doc = NetworkCfgDoc.from_file(fp)


    # try generating ping tests at various replication factors for targets with everything selected
    for factor in (1,2,3,):
        gen = NodePingTestGenerator(doc, tests_per_dst_node_if=factor, tests_per_dst_system_if=factor)

        expected_node_ping_count = 0
        actual_node_ping_count = 0
        for name in doc.get_names_by_type(OBJ_TYPE_SR):
            expected_node_ping_count += len(doc.get_raw_object(name).get("interfaces", []))
            actual_node_ping_count += len(gen.get_tests(name, [PingTestKind.NODE]))
        expected_node_ping_count *= factor
        assert actual_node_ping_count == expected_node_ping_count, f"expected {expected_node_ping_count} node tests at factor {factor}, got {actual_node_ping_count}"

        expected_sys_ping_count = 0
        actual_sys_ping_count = 0
        for name in doc.get_names_by_type(OBJ_TYPE_SY):
            expected_sys_ping_count += len(doc.get_raw_object(name).get("interfaces")) + 1  # + VIP
        expected_sys_ping_count *= factor
        for name in doc.get_names_by_type(OBJ_TYPE_SR):
            actual_sys_ping_count += len(gen.get_tests(name, [PingTestKind.SYSTEM]))
        assert expected_sys_ping_count == actual_sys_ping_count, f"expected {expected_sys_ping_count} sys tests at factor {factor}, got {actual_sys_ping_count}"

    # try generating ping tests for a subset of targets
    node_names = doc.get_names_by_type(OBJ_TYPE_SR)
    src_names, dst_names = node_names[0:-6], node_names[-6:]
    gen = NodePingTestGenerator(doc, include_src=src_names, include_dst=dst_names, tests_per_dst_node_if=2)
    expected_node_ping_count = 0
    actual_node_ping_count = 0
    for name in node_names:
        if name in dst_names:
            expected_node_ping_count += len(doc.get_raw_object(name).get("interfaces", []))
        actual_node_ping_count += len(gen.get_tests(name, [PingTestKind.NODE]))
    expected_node_ping_count *= 2
    assert expected_node_ping_count == actual_node_ping_count, f"expected {expected_node_ping_count}, got {actual_node_ping_count}"

    # ensure at down source interfaces are ignored
    down_src_if, dst_if_count = set(), 0
    for name in node_names:
        ifs = doc.get_raw_object(name).get("interfaces")
        ifs[0]["state"] = "down"
        key = f"{name}:{ifs[0]['name']}"
        if name in src_names:
            down_src_if.add(key)
        elif name in dst_names:  # set 1 dst down - all dsts should be included regardless of state
            ifs[0]["state"] = "down"
            dst_if_count += len(ifs)
    gen = NodePingTestGenerator(doc, include_src=src_names, include_dst=dst_names, tests_per_dst_node_if=1, exclude_down_src_interfaces=True)
    expected_node_ping_count = dst_if_count
    actual_node_ping_count = 0
    for name in node_names:
        tests = gen.get_tests(name, [PingTestKind.NODE])
        for t in tests:
            assert f"{t.src_name}:{t.src_if}" not in down_src_if
        actual_node_ping_count += len(tests)
    assert actual_node_ping_count == expected_node_ping_count

    # ensure the vlan test produces test for each node vlan - 2 sx, 3 mx = 4 * 5
    expected_vlan_tests = 4 * 5
    actual_vlan_tests = 0
    gen = NodePingTestGenerator(doc)
    for name in doc.get_names_by_type(OBJ_TYPE_SR):
        actual_vlan_tests += len(gen.get_tests(name, [PingTestKind.VLAN]))
    assert expected_vlan_tests == actual_vlan_tests


def test_node_mtu_ping_check(tmp_path):
    """ Test MTU options specified correctly depending on dst type """
    builder = nano_stamp_builder()
    builder.network["environment"]["cluster_prefix"] = "10.0.0.0/19"
    builder.call_allocate_tiers(tmp_path, aw_size=1024, br_size=256, vip_count=128, aw_physical=196)
    builder.call_placer(tmp_path)
    fp = f"{tmp_path}/network_config.json"
    with open(fp, 'w') as f:
        f.write(json.dumps(builder.network))

    with NetworkCfgCtx(fp) as ctx:
        sr_names = ctx.network_config.get_names_by_type(OBJ_TYPE_SR)
        name = sr_names[0]
        gen = NodePingTestGenerator(ctx.network_config, include_src=[name], include_dst=[name])

        # default case
        task = NodePingTestNetworkTask(ctx, name, gen, [PingTestKind.NODE], mtu=DEFAULT_MTU)
        assert " -s 8122" in task.get_mtu_opts("system")
        assert " -s 8972" in task.get_mtu_opts("node")
        assert " -s 8972" in task.get_mtu_opts("gateway")
        assert " -s 8972" in task.get_mtu_opts("vlan")

        # override case
        task = NodePingTestNetworkTask(ctx, name, gen, [PingTestKind.NODE], mtu=4000)
        assert " -s 3972" in task.get_mtu_opts("system")
        assert " -s 3972" in task.get_mtu_opts("node")
        assert " -s 3972" in task.get_mtu_opts("gateway")
        assert " -s 3972" in task.get_mtu_opts("vlan")

        # none case
        task = NodePingTestNetworkTask(ctx, name, gen, [PingTestKind.NODE], mtu=0)
        assert "" == task.get_mtu_opts("system")
        assert "" == task.get_mtu_opts("node")
        assert "" == task.get_mtu_opts("gateway")
        assert "" == task.get_mtu_opts("vlan")