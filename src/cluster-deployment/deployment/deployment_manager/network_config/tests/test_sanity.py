import json
import pathlib
from importlib import resources

from deployment_manager.network_config.cli.sanity import (
    check_duplicate_ips,
    check_xconn_count,
    sanity_check,
    system_switch_check, NetworkDocCheckResult
)
from deployment_manager.network_config.tests.network_builder import nano_stamp_builder
import pytest

ASSETS_PATH = pathlib.Path(__file__).parent / "assets"


def test_sanity_system_connections():
    doc = json.loads((ASSETS_PATH / "network_config_mb21.json").read_text())

    errors = system_switch_check(doc)
    assert not errors, f"expected no errors, got: {errors}"

    # swap a system's ports and expect that that system is reported in an error
    infs = doc['systems'][0]['interfaces']
    d0 = [i for i in infs if i['name'] == 'data0'][0]
    d2 = [i for i in infs if i['name'] == 'data2'][0]
    d0_switch = d0['switch_name']
    d2_switch = d2['switch_name']
    d0['switch_name'] = d2_switch

    errors = system_switch_check(doc)
    assert 1 == len(errors), f"expected 1 error, got: {errors}"

    d2['switch_name'] = d0_switch
    errors = system_switch_check(doc)
    assert 2 == len(errors), f"expected 2 errors, got: {errors}"


def test_sanity_cross_connects():
    doc = json.loads((ASSETS_PATH / "network_config_mb21.json").read_text())

    result = NetworkDocCheckResult()

    check_xconn_count(doc, result)
    assert result.is_ok(), f"expected no errors, got {result}"

    # ensure that the check works for leaf and spine as well
    for s in doc.get("switches", []):
        if s["tier"] == "AW":
            s["tier"] = "LF"
        else:
            s["tier"] = "SP"
    doc["environment"]["topology"] = "leaf_spine"
    result = NetworkDocCheckResult()
    check_xconn_count(doc, result)
    assert result.is_ok(), f"expected no errors, got {result}"

    # reduce one cross-connect and ensure error is reported
    del doc['xconnect']['connections'][0]
    result = NetworkDocCheckResult()
    check_xconn_count(doc, result)
    assert not result.is_ok(), "No error reported for unequal cross-connects"

    # remove cross-connects and ensure error is reported
    doc['xconnect']['connections'] = []
    result = NetworkDocCheckResult()
    check_xconn_count(doc, result)
    assert not result.is_ok(), "No error reported for insufficient cross-connects"


def test_sanity_vrange(tmp_path):
    builder = nano_stamp_builder()
    builder.call_allocate_tiers(tmp_path)
    builder.call_placer(tmp_path)

    # sanity check with generous tier defaults
    result = sanity_check(builder.network)
    assert result.is_ok(), f"sanity check failed: {result}"

    # add a second cluster to the mix and ensure that the sanity check fails due to insufficient virtual ranges
    builder.add_cluster()
    result = sanity_check(builder.network)
    assert result.is_fatal(), f"sanity check failed with second cluster: {result}"
    assert len(result.allocation_errors) > 0, "expected allocation errors but got none"

    # sanity check with too small of virtual ranges
    builder = nano_stamp_builder()
    builder.call_allocate_tiers(tmp_path, aw_size=256)
    builder.call_placer(tmp_path)
    result = sanity_check(builder.network)
    assert not result.is_ok(), "sanity check succeeded but should have failed"
    assert result.is_fatal(), "sanity check succeeded but should have failed"
    assert result.allocation_errors[0].type == "insufficient_virtual_addresses", \
        f"expected virtual range errors but got {result.allocation_errors[0].type}"


def test_sanity_ax_connections(tmp_path):
    builder = nano_stamp_builder()
    builder.call_allocate_tiers(tmp_path)
    builder.call_placer(tmp_path)

    # sanity check with correct ax connections
    result = sanity_check(builder.network)
    assert result.is_ok(), f"sanity check failed but should have succeeded, got: {result}"

    # change switch_name of one ax connection
    builder.network["activation_nodes"][0]["interfaces"][0]["switch_name"] = "err_switch"
    result = sanity_check(builder.network)
    assert not result.is_ok(), "sanity check succeeded but should have failed"
    assert result.device_errors[0].results[0].type == "unequal_ax_connections", \
        f"expected unequal connection errors but got {result.device_errors[0].results[0].type}"


def test_sanity_duplicate_ips():
    doc = json.loads((ASSETS_PATH / "network_config_mb21.json").read_text())

    dups = check_duplicate_ips(doc)
    assert not dups, f"Unexpected duplicate IPs {dups}"

    doc['memoryx_nodes'][0]['interfaces'][1]['address'] = doc['memoryx_nodes'][0]['interfaces'][0]['address']

    dups = check_duplicate_ips(doc)
    assert dups, f"Duplicate IPs expected, but not found"

    result = sanity_check(doc)
    assert not result.is_ok(), "Sanity check succeeded but should have failed"
    assert result.allocation_errors, f"Expected allocation errors, but got {result.allocation_errors}"


@pytest.mark.skip("placeholder: user responsible for running this test with their own network config")
def test_sanity_real_cluster():
    txt = resources.files(__package__).joinpath("assets", "network_config_cg3.json").read_text()
    doc = json.loads(txt)
    result = sanity_check(doc)
    assert result.is_ok(), f"sanity check failed: {result}"