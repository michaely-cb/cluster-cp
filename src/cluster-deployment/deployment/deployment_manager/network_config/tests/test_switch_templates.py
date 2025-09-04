import pathlib

import pytest

from deployment_manager.network_config.cli.sanity import sanity_check
from deployment_manager.network_config.tests.network_builder import ARISTA_SWITCH_BASE, DELL_SWITCH_BASE, \
    EC_SWITCH_BASE, \
    HP_SWITCH_BASE, JUNIPER_SWITCH_BASE, \
    nano_stamp_builder


@pytest.mark.parametrize("switch_base", [
    ARISTA_SWITCH_BASE,
    DELL_SWITCH_BASE,
    EC_SWITCH_BASE,
    HP_SWITCH_BASE,
    JUNIPER_SWITCH_BASE,
], ids=["AR", "DL", "EC", "HP", "JU"])
@pytest.mark.parametrize("vrf", [
    "cs1data",
    "tenant0"
])
def test_switch_templates(tmp_path, switch_base, vrf):
    builder = nano_stamp_builder(switch_base=switch_base)
    if vrf != "cs1data":
        builder.network["environment"]["vrf_name"] = vrf

    # invoke CLI commands to complete network configuration
    builder.call_allocate_tiers(tmp_path)
    builder.call_placer(tmp_path)

    result = sanity_check(builder.network)
    assert result.is_ok(), f"sanity check failed: {result}"

    # test ability to write configs
    files = builder.call_generate_cfg(tmp_path)

    # check that vrf name is 'tenant0' not cs1data in the generated files
    switch_files = [f for f in files if f.endswith("l3_config.json")]
    assert switch_files
    for f in switch_files:
        doc = pathlib.Path(f).read_text()
        assert vrf in doc
        if vrf != "cs1data":
            assert "cs1data" not in doc

    # debug
    # Path("/tmp/network.json").write_text(json.dumps(builder.network))
    # os.mkdir("/tmp/config")
    # builder.call_generate_cfg(Path("/tmp/config"))
