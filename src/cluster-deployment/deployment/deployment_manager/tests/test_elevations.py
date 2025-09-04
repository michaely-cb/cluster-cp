import logging
import pytest
import yaml
from pathlib import Path
from deployment_manager.tests.conftest import MockCluster
from deployment_manager.common.yamlhandler import CustomYamlLoader

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def elevation_cluster():
    with MockCluster("elevation_cli") as cluster:
        cluster.must_exec_root("/project/install.sh")
        yield cluster


def test_elevation_cli_add_show_remove(elevation_cluster: MockCluster, tmp_path: Path):
    cluster = elevation_cluster

    cluster.must_exec_root(
        "cscfg profile create elevation_cli_test "
        "--inventory_file /host/config/devtest_inventory.csv "
        "--config_input /host/config/devtest_input.yml"
    )

    incorrect_elevations = {"/host/files/incorrect_elevations.yaml": ["device_specs.0.type", "device_specs.0.role", "device_specs.0.count", "device_specs"]}
    correct_elevations = {"/host/files/correct_elevations.yaml": ["correct_elevation1", "correct_elevation2"]}
    correct_racks = {"/host/files/correct_racks.yaml": ["net001"]}

    for filename, keywords in incorrect_elevations.items():
        rc, out, err = cluster.exec("rootserver", f"cscfg inventory elevations add {filename}")
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

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory elevations show -o yaml")
    assert rc == 0, f"show failed: {err}"
    doc = yaml.load(out, Loader=CustomYamlLoader)
    assert doc[0]["device_specs"][0]["properties"]["vendor"]["name"] == 'DL'
    assert doc[1]["device_specs"][0]["count"] == 2

    for filename, keywords in correct_elevations.items():
        rc, out, err = cluster.exec("rootserver", f"cscfg inventory elevations add {filename}")
        assert rc == 1, f"duplicate add should have failed but succeeded: {err}"

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory elevations remove incorrect_elevation1")
    assert rc == 1, f"attempting to remove elevation that doesn't exist should have failed but succeeded: {err}"

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory elevations remove correct_elevation2")
    assert rc == 0, f"remove elevation failed {err}"
    
    for filename, keywords in correct_racks.items():
        rc, out, err = cluster.exec("rootserver", f"cscfg inventory racks add {filename}")
        assert rc == 0, f"add racks failed {err}"

    rc, out, err = cluster.exec("rootserver", f"cscfg inventory elevations remove correct_elevation1")
    assert rc == 1, f"remove elevation with corresponding racks succeeded when it should have failed {err}"
    assert "net001" in out

    cluster.must_exec_root("cscfg profile delete elevation_cli_test")