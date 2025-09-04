import json
import logging
import pathlib

import yaml

from deployment_manager.tests.conftest import MockCluster
from deployment_manager.common.yamlhandler import CustomYamlLoader
import pytest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def device_cli_cluster():
    with MockCluster("device_cli") as cluster:
        cluster.must_exec_root("/project/install.sh")
        yield cluster


def test_device_cli_edit_device(device_cli_cluster, tmp_path):
    """ generate devtest cluster dnsmasq """
    cluster = device_cli_cluster

    cluster.must_exec_root(
        "cscfg profile create device_cli_test "
        "--inventory_file /host/config/devtest_inventory.csv "
        "--config_input /host/config/devtest_input.yml"
    )

    # edit a property for yaml response
    out = cluster.must_exec_root("cscfg device edit sc-r1rb1-s8 -oyaml -p "
                                 "management_info.mac=10:10:10:10:10:10 management_info.ip=1.1.1.1")
    repr = yaml.safe_load(out)[0]
    assert repr["properties"]["management_info"]["mac"] == "10:10:10:10:10:10"

    out = cluster.must_exec_root("cscfg device edit sc-r1rb1-s8 -ojson -p "
                                 "management_info.mac=10:10:10:10:10:11")

    repr = json.loads(out)[0]
    assert repr["properties"]["management_info"]["mac"] == "10:10:10:10:10:11"

    # clear property
    out = cluster.must_exec_root("cscfg device edit sc-r1rb1-s8 -ojson -p "
                                 "management_info.mac=")
    repr = json.loads(out)[0]
    assert "mac" not in repr["properties"]["management_info"]

    # ensure cannot edit switch props
    rv, _, _ = cluster.exec("rootserver", "cscfg device edit sc-r1rb1-s8 -p "
                                          "switch_info.subnet=1.1.1.0/24")
    assert rv != 0

    # ensure cannot edit invalid prop
    rv, _, _ = cluster.exec("rootserver", "cscfg device edit sc-r1rb1-s8 -p "
                                          "management_info.ip.x=1.1.1.1")
    assert rv != 0

    # ensure cannot edit unknown device
    rv, _, _ = cluster.exec("rootserver", "cscfg device edit sc-r1rb1-s888 -p "
                                          "management_info.ip=1.1.1.1")
    assert rv != 0

    cluster.must_exec_root("cscfg profile delete device_cli_test")


def test_device_cli_update_device_file(device_cli_cluster, tmp_path):
    """ generate devtest cluster dnsmasq """
    cluster = device_cli_cluster
    cluster.must_exec_root(
        "cscfg profile create update_file "
        "--inventory_file /host/config/devtest_inventory.csv "
        "--config_input /host/config/devtest_input.yml"
    )

    # edit a property for yaml response
    out = cluster.must_exec_root("cscfg device edit sc-r1rb1-s8 -ojson -p "
                                 "management_info.mac=10:10:10:10:10:10 management_info.ip=1.1.1.1")
    d = json.loads(out)
    orig_props = d[0]["properties"]
    d[0]["properties"] = {
        "management_info": {"mac": "10:10:10:10:10:11", "ip": None}
    }
    cluster.write_file_contents("rootserver", pathlib.Path("/device.json"), json.dumps(d))
    out = cluster.must_exec_root(f"bash -c 'cscfg device update - -oyaml </device.json'")
    d = yaml.load(out, Loader=CustomYamlLoader)
    assert "10:10:10:10:10:11" == d[0]["properties"]["management_info"]["mac"]
    assert "ip" not in d[0]["properties"]["management_info"]

    props = d[0]["properties"]
    del orig_props["management_info"]
    del props["management_info"]
    assert props == orig_props, "properties other than management_info should not have been updated"

    out = cluster.must_exec_root(f"cscfg device update /device.json -oyaml")
    d = yaml.load(out, Loader=CustomYamlLoader)
    assert d == [], "no-op update should not have listed results"

    cluster.must_exec_root("cscfg profile delete update_file")
