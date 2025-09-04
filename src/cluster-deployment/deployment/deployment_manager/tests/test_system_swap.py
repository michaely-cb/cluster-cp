import json
import logging
import pathlib
import pytest
import textwrap
import yaml
from typing import Tuple

from deployment_manager.common.models import K_SYSTEM_ROOT_PASSWORD, K_SYSTEM_ADMIN_PASSWORD
from deployment_manager.tests.conftest import MockCluster

logger = logging.getLogger(__name__)


def _configure_system_swap_cluster(cluster: MockCluster, system_ip: str):
    cluster.must_exec_root(f"bash -c 'echo \"{system_ip} systemf001 >>/etc/hosts\"'")
    cluster.must_exec_root("/project/install.sh")
    cfg = yaml.safe_load(cluster.must_exec_root("cat /host/config/input.yml"))
    cfg["mgmt_network_int_config"]["ip_allocation"]["allocation_strategy"] = "v1"
    cluster.write_file_contents("rootserver", pathlib.Path("/workspace/input.yml"), yaml.safe_dump(cfg))

    # test with backwards compatibility -e argument based command passing
    cluster.must_exec_root(
        "cscfg -e 'profile create mocktest "
        "--inventory_file /host/config/inventory.csv --config_input /workspace/input.yml'"
    )


def _update_cscfg_input_password(cluster: MockCluster, **kwargs):
    # ideally, we'd be able to update the input.yaml file programmatically to avoid all this hassle-
    cfg_path = "/opt/cerebras/cluster-deployment/meta/mocktest/input.yml"
    cfg = cluster.must_exec_root(f"cat {cfg_path}")
    cfg = yaml.safe_load(cfg)
    cfg["secrets_provider"] = {
        "provider": "embedded",
        "data": {
            K_SYSTEM_ROOT_PASSWORD: kwargs.get("root", "root"),  # docker container root pw is root
            K_SYSTEM_ADMIN_PASSWORD: kwargs.get("admin", "foobar"),  # we'll update the container's admin user to foobar
        }
    }
    cluster.write_file_contents("rootserver", pathlib.Path(cfg_path), yaml.safe_dump(cfg))


@pytest.fixture
def system_swap_cluster(request) -> Tuple[MockCluster, str]:
    cluster = MockCluster(request.node.name, mocks_dir="system_swap")
    system_ip = cluster.add_mock_server_container("systemf001")

    with cluster as cluster:
        _configure_system_swap_cluster(cluster, system_ip)
        yield cluster, system_ip


def test_system_swap_no_alias_name(system_swap_cluster):
    cluster, system_ip = system_swap_cluster

    cluster.must_exec_root(f"cp /host/config/network_config.json /opt/cerebras/cluster-deployment/meta/mocktest/")
    cluster.must_exec_root(f"mkdir -p /etc/hosts.d/")
    cluster.write_file_contents("rootserver", pathlib.Path("/etc/hosts.d/hosts.conf"), textwrap.dedent("""
    172.0.0.1 systemf000
    172.0.0.4 systemf004
    """))

    # password of root and admin user should be updated
    _update_cscfg_input_password(cluster)
    cluster.must_exec("systemf001", "useradd -m -s /bin/bash admin")
    cluster.must_exec("systemf001", "bash -c 'passwd admin --stdin <<< somethingrandom'")
    cluster.must_exec_root("cscfg device edit -f name=systemf001 -y "
                           "-p management_credentials.password=xxx management_credentials.user=root")

    cluster.must_exec_root(
        "cscfg run_flow swap_system systemf000,systemf001 "
        " --skip-tasks ValidateSystemLinks,UpdateCBInfra -y",
        environment=["K8_TMP_PATH=/home/"],
    )
    # check that the uploaded cs2 config was correct
    sysnet_cfg = json.loads(cluster.must_exec("systemf001", "cat /workspace/uploaded_files/updateFile.json"))
    for port, portval in sysnet_cfg["data"].items():
        if port == "default":
            continue
        assert portval["inet"].startswith("10.250.248."), f"port {port} had incorrect IP: {portval}"

    # check that dns masq hosts were updated
    out = cluster.must_exec_root("cat /etc/hosts.d/hosts.conf")
    assert f"{system_ip} systemf001" in out

    # check the mgmt IP was updated in clusteryaml
    clustercfg = cluster.must_exec_root("cat /home/clusteryaml")
    doc = yaml.safe_load(clustercfg)
    for s in doc["systems"]:
        if s["name"] == "systemf001":
            assert s["managementAddress"] == system_ip
            break
    else:
        assert False, f"failed to find new system in cluster.yaml: {clustercfg}"

    # check that admin user password was updated
    doc = json.loads(cluster.must_exec_root("cscfg device show -f name=systemf001 -ojson"))
    assert doc[0]["properties"]["management_credentials"]["password"] == "root"

    # check passing invalid systems fails
    rv, stdout, _ = cluster.exec("rootserver", "cscfg run_flow swap_system yyy,xxx -y "
                                               "--skip-tasks PreflightCheck ")
    assert rv != 0, f"cscfg should have exited with non-zero: {stdout}"

    # check passing pingable old system fails
    input_path = pathlib.Path("/opt/cerebras/cluster-deployment/meta/mocktest/input.yml")
    cfg = yaml.safe_load(cluster.must_exec_root(f"cat {input_path}"))
    cfg["mgmt_network_int_config"]["ip_allocation"]["allocation_strategy"] = "v2"
    cluster.write_file_contents("rootserver", input_path, yaml.safe_dump(cfg))

    # test Precheck - passing an unpingable new system fails and pingable old system fails
    rv, stdout, stderr = cluster.exec(
        "rootserver",
        "cscfg -v 1 run_flow swap_system systemf001,systemf002 -y",
        environment=["K8_TMP_PATH=/home/"],
    )
    assert rv != 0
    assert "PreflightCheck: fail" in stderr
    assert "systemf001 to swap out but they are pingable" in stderr
    assert "systemf002 to swap in but failed to respond to ping" in stderr


def test_system_swap_alias_name(system_swap_cluster):
    cluster, system_ip = system_swap_cluster

    cluster.must_exec_root(f"cp /host/config/network_config.json /opt/cerebras/cluster-deployment/meta/mocktest/")
    cluster.must_exec_root("sed -i 's/systemf000/systemf001/' /opt/cerebras/cluster-deployment/meta/mocktest/network_config.json")
    cluster.must_exec_root(f"bash -c 'echo \"{system_ip} systemf000\" >> /etc/hosts'")  # so it passes ping check
    # make the system look like it's an aliased system
    cluster.must_exec_root("cscfg device edit -f name=systemf001 -y -p "
                           "management_info.name=hardware_name management_credentials.password=root management_credentials.user=root")

    cluster.must_exec_root(
        "cscfg run_flow swap_system systemf001,systemf000 "
        " --skip-tasks ValidateSystemLinks,UpdateLegacyDnsmasqConf,UpdateCBInfra,UpdatePassword -y",
        environment=["K8_TMP_PATH=/home/"],
    )
    # now there should be the old system present but with the updated name
    doc = json.loads(cluster.must_exec_root("cscfg device show -f name=systemf001 -ojson"))
    assert doc[0]["properties"]["management_info"]["name"] == "systemf000"
    # and systemf002 must not exist
    doc = json.loads(cluster.must_exec_root("cscfg device show -f name=systemf000 -ojson"))
    assert not doc

    nw_doc = json.loads(cluster.must_exec_root("cat /opt/cerebras/cluster-deployment/meta/mocktest/network_config.json"))
    sys = nw_doc["systems"][0]
    assert sys["name"] == "systemf001"
    assert sys["default_hostname"] == "systemf000"

    # add a system record in non-aliased mode and then attempt to swap a system with that non-aliased name with the
    # aliased system - this should fail
    cluster.must_exec_root("cscfg device add sys_fake SY CS -p management_info.ip=0.0.0.0")
    rv, stdout, stderr = cluster.exec("rootserver", "cscfg run_flow swap_system systemf001,sys_fake -y")
    assert rv != 0
    assert "already exists as a non-aliased device record" in stderr, f"unexpected error: {stderr}"


def test_dev_exec_cmds(system_swap_cluster):
    # exec commands - saves the time of having to bring up/down 2 docker containers though
    cluster, system_ip = system_swap_cluster

    # avoid duplication of system IP for worker and systemf001
    cluster.must_exec_root("cscfg device edit systemf001 -p management_info.ip=1.1.1.1")
    cluster.must_exec_root(f"cscfg device add worker SR WK "
                           "-p management_credentials.user=root "
                           "   management_credentials.password=root "
                           f"  management_info.ip={system_ip}")
    
    # test single command mode
    output = cluster.must_exec_root("cscfg dev exec worker -- echo helloworld")
    assert "helloworld" in output

    # test batch mode
    cluster.write_file_contents("rootserver", pathlib.Path("/home/script.sh"), "echo hello\necho goodbye\n")
    output = cluster.must_exec_root("cscfg dev exec --file /home/script.sh worker")
    assert "hello" in output
    assert "goodbye" in output

    # test output dir
    cluster.must_exec_root("cscfg dev exec --file /home/script.sh --output_dir . worker")
    output = cluster.must_exec_root("cat worker")
    assert "hello" in output
    assert "goodbye" in output
    
    # Remove worker device to avoid conflicts in other tests
    cluster.must_exec_root("cscfg device remove worker")
    cluster.must_exec_root(f"cscfg device edit systemf001 -p management_info.ip={system_ip}")

def test_change_system_password(system_swap_cluster):
    cluster, system_ip = system_swap_cluster

    _update_cscfg_input_password(cluster)

    # create an admin user with password foo, then update it to set it to foobar and verify we can ssh to it with the new password
    cluster.must_exec_root(f"cscfg device edit -f name=systemf001 -y -p management_credentials.user=admin management_credentials.password=notcorrect")
    cluster.must_exec("systemf001", "useradd -m -s /bin/bash admin")
    cluster.must_exec("systemf001", "bash -c 'passwd admin --stdin <<< foo'")
    cluster.must_exec_root(f"cscfg system password set -f name=systemf001 --user admin -y")

    # see that we can connect to the system using the admin user with admin password
    out = cluster.must_exec_root(f"cscfg device show -f name=systemf001 -ojson")
    assert json.loads(out)[0]["properties"]["management_credentials"]["password"] == "foobar", "password was not updated in DB"
    out = cluster.must_exec_root("cscfg dev exec -f name=systemf001 -y -- echo loggedinsuccess")
    assert "loggedinsuccess" in out

    cluster.must_exec_root(f"cscfg system password reset -f name=systemf001 --user admin -y")
    out = cluster.must_exec_root(f"cscfg device show -f name=systemf001 -ojson")
    assert json.loads(out)[0]["properties"]["management_credentials"]["password"] == "admin", "password was not updated in DB"
    out = cluster.must_exec_root("cscfg dev exec -f name=systemf001 -y -- echo loggedinsuccess")
    assert "loggedinsuccess" in out
