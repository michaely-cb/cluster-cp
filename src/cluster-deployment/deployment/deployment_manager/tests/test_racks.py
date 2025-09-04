import logging
import pytest
import yaml
from pathlib import Path
from deployment_manager.tests.conftest import MockCluster
from deployment_manager.common.yamlhandler import CustomYamlLoader

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def rack_cluster():
    with MockCluster("rack_cli") as cluster:
        cluster.must_exec_root("/project/install.sh")
        yield cluster


def test_rack_cli_add_show_remove(rack_cluster: MockCluster, tmp_path: Path):
    cluster = rack_cluster

    cluster.must_exec_root(
        "cscfg profile create rack_cli_test "
        "--inventory_file /host/config/devtest_inventory.csv "
        "--config_input /host/config/devtest_input.yml"
    )

    correct_racks = {"/host/files/correct_racks.yaml": ["net001", "net002"]}
    incorrect_racks = {"/host/files/incorrect_racks.yaml": ["properties.location.stamp", "properties.location.rack"]}
    correct_elevations = {"/host/files/correct_elevations.yaml": ["correct_elevation1", "correct_elevation2"]}

    for filename, keywords in incorrect_racks.items():
        rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks add {filename}")
        assert rc == 1, f"add shouldn't have succeeded but did: {err}"
        for keyword in keywords:
            assert keyword in err

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory elevations show")
    assert "incorrect_elevation" not in out

    for filename, keywords in correct_elevations.items():
        rc, out, err = cluster.exec("rootserver", f"cscfg inventory elevations add {filename}")
        assert rc == 0, f"add should have succeeded but failed: {err}"
        for keyword in keywords:
            assert keyword in out

    for filename, keywords in correct_racks.items():
        rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks add {filename}")
        assert rc == 0, f"add racks failed {err}"
        for keyword in keywords:
            assert keyword in out

    for filename, keywords in correct_racks.items():
        rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks add {filename}")
        assert rc == 1, f"duplicate add should have failed but succeeded: {err}"

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks show -o yaml")
    assert rc == 0, f"show failed: {err}"
    doc = yaml.load(out, Loader=CustomYamlLoader)
    assert doc[0]["elevation_name"] == 'correct_elevation1'
    assert doc[1]["properties"]["location"]["stamp"] == 'cg9'

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks remove incorrect_rack1")
    assert rc == 1, f"attempting to remove elevation that doesn't exist should have failed but succeeded: {err}"

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks remove net002")
    assert rc == 0, f"remove elevation failed {err}"

    rc, out, err = cluster.exec("rootserver", f"cscfg device add net001-sxx-sr01 SR SX -p location.rack=net001")
    assert rc == 0, f"add device failed {err}"

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks remove net001")
    assert rc == 1, f"remove rack with corresponding devices succeeded when it should have failed {err}"
    assert "net001-sxx-sr01" in out

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks sync-show")
    assert rc == 0, f"sync-show failed {err}"
    devices = ["net001-sxx-sr01", "net001-sxx-sr02", "net001-sxx-sr03", "net001-sxx-sr04", "net001-ax-sr01", "net001-ax-sr02", "net001-ax-sr03", "net001-ax-sr04"]
    for device in devices:
        assert device in out

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks sync")
    assert rc == 1, f"sync devices suceeded when there is an unaddressed conflict {err}"

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks sync -m=skip -y")
    assert rc == 0, f"sync devices failed {err}"
    rc, out, err = cluster.exec("rootserver", f"cscfg device show")
    assert rc == 0, f"device show failed {err}"
    for device in devices:
        assert device in out

    rc, out, err = cluster.exec("rootserver", f"cscfg device show -f name=net001-sxx-sr01 -o yaml")
    assert rc == 0, f"net001-sxx-sr01 show failed {err}"
    doc = yaml.load(out, Loader=CustomYamlLoader)
    assert "location" not in doc[0]

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks sync -m=overwrite -y")
    assert rc == 0, f"sync devices failed {err}"
    rc, out, err = cluster.exec("rootserver", f"cscfg device show -f name=net001-sxx-sr01 -o yaml")
    assert rc == 0, f"net001-sxx-sr01 show failed {err}"
    doc = yaml.load(out, Loader=CustomYamlLoader)
    assert doc[0]["properties"]["location"]["rack"] == 'net001'

    cluster.must_exec_root("cscfg profile delete elevation_cli_test")
