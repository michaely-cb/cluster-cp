import json
import logging
import os

import pytest
import yaml

from deployment_manager.tests.conftest import MockCluster

logger = logging.getLogger(__name__)


def test_health_commands():
    cluster = MockCluster("validator")
    device_name = "rootserver"

    with cluster:
        cluster.must_exec_root("/project/install.sh")
        cluster.must_exec_root(
            "cp /host/files/mock_lease /var/lib/dnsmasq/dnsmasq.leases"
        )
        cluster.must_exec_root(
            "cscfg profile create mocktest "
            "--inventory_file /host/config/inventory.csv --config_input /host/config/input.yml"
        )
        cluster.must_exec_root("cscfg server health show")
        cluster.must_exec_root("cscfg server health show -o yaml")
        cluster.must_exec_root("cscfg server health check")
        cluster.must_exec_root("bash -c 'cscfg server health show | grep OK'")
        cluster.must_exec_root(
            f"cscfg server health force-update -f name={device_name} --attr ipmi_access --status OK"
        )
        json_string = cluster.must_exec_root("cscfg server health show -o json")
        device_health = json.loads(json_string)
        for device_status in device_health:
            if device_status["name"] == device_name:
                break

        for status in device_status["healthStatuses"]:
            if status["name"] == "ipmi_access":
                assert status["status"] == "OK"

        cluster.must_exec_root("cscfg system health show")
        cluster.must_exec_root("cscfg system health show -o yaml")
        cluster.must_exec_root("cscfg system health check")
        cluster.must_exec_root(
            f"cscfg system health force-update -f name=cs1 --attr system_health --status OK"
        )
        cluster.must_exec_root(
            f"cscfg system health force-update -f name=cs1 --attr system_link --status OK"
        )
        cluster.must_exec_root("bash -c 'cscfg system health show | grep OK'")
        json_string = cluster.must_exec_root("cscfg system health show -o json")
        device_health = json.loads(json_string)

        for device_status in device_health:
            if device_status["name"] == "cs1":
                for s in device_status["healthStatuses"]:
                    if s["name"] == "system_health":
                        assert s["status"] == "OK"
                    elif s["name"] == "system_link":
                        assert s["status"] == "OK"

        # check pb2 master config generation
        cluster.must_exec_root(
            "cscfg deploy update --name sc-r1rb1-s5 --stage NETWORK_PUSH --status COMPLETED -y"
        )
        cluster.must_exec_root(
            "cscfg deploy update --name rootserver --stage NETWORK_PUSH --status COMPLETED -y"
        )
        cluster.must_exec_root(
            f"cscfg switch health force-update -f name=sc-r1rb1-100gsw32 --attr mgmt_access --status OK"
        )
        cluster.must_exec_root(
            f"cscfg switch health force-update -f name=sc-r1rb1-100gsw32 --attr hostname --status OK"
        )
        cluster.must_exec_root(
            f"cscfg switch health force-update -f name=sc-r1rb1-100gsw32 --attr switch_lease --status OK"
        )
        cluster.must_exec_root(
            f"cscfg switch health force-update -f name=sc-r1rb1-100gsw32 --attr gateway_ping --status OK"
        )
        cluster.must_exec_root(
            f"cscfg dev generate_master_config --output /tmp/master_config.yml --no"
        )

        yaml_string = cluster.must_exec_root(f"cat /tmp/master_config.yml")
        yaml_object = yaml.safe_load(yaml_string)
        assert len(yaml_object["servers"]) == 2
        assert len(yaml_object["switches"]) == 1
        assert len(yaml_object["systems"]) == 1


@pytest.mark.skip("Skip as it depends on a real server availability")
def test_health_check():
    cluster = MockCluster("validator", has_network=False)

    with cluster:
        cluster.must_exec_root("/project/install.sh")
        cluster.must_exec_root(
            "cscfg profile create mocktest "
            "--config_input /host/config/pb1-input.yml'"
        )
        cluster.must_exec_root(
            "cscfg device import_inventory /host/config/pb1.csv -y"
        )
        cluster.must_exec_root("cscfg server health check")
        cluster.must_exec_root("cscfg server health show -o yaml")
        cluster.must_exec_root("cscfg server update_mac_address")
        json_string = cluster.must_exec_root(
            "cscfg device show -o json --filter name=cs303-wse001-sx-sr03"
        )
        dev_prop = json.loads(json_string)[0]["properties"]
        assert dev_prop["ipmi_info"]["mac"] == "a8:3c:a5:38:30:a8"
        assert dev_prop["management_info"]["mac"] == "c4:cb:e1:c1:7b:40"

        cluster.must_exec_root(
            "cscfg device update sc-r1rb1-1gsw --properties "
            "'subnet_info.gateway=172.28.8.254'"
        )
        cluster.must_exec_root(
            "cscfg device update sc-r15ra8-1gsw-dell --properties "
            "'subnet_info.gateway=172.28.87.254'"
        )

        cluster.must_exec_root("cscfg switch health check")

        json_string = cluster.must_exec_root("cscfg switch health show -ojson")
        json_obj = json.loads(json_string)

        assert json_obj[0]["health"] == "WARNING"
        assert json_obj[1]["health"] == "WARNING"


@pytest.mark.skip("Skip as it depends on a real cluster availability")
def test_health_mb303():
    assert "CEREBRAS_PASSWORD" in os.environ.keys(), "Set ENV CEREBRAS_PASSWORD"

    cluster = MockCluster("validator", has_network=False)
    SSH_CMD = (
        f"sshpass -p {os.environ['CEREBRAS_PASSWORD']} "
        f"ssh -o StrictHostKeyChecking=no root@cs303-wse001-mg-sr01"
    )
    with cluster:
        cluster.must_exec_root("/project/install.sh")
        cluster.must_exec_root(
            f"bash -c '{SSH_CMD} cat /opt/cerebras/cluster-deployment/meta/mb303/input.yml | tee /tmp/input.yml'"
        )
        cluster.must_exec_root(
            f"bash -c '{SSH_CMD} cat /var/lib/dnsmasq/dnsmasq.leases | tee /var/lib/dnsmasq/dnsmasq.leases'"
        )
        cluster.must_exec_root(
            "cscfg -e 'create_profile mockmb303 "
            "--config_input /tmp/input.yml'"
        )
        cluster.must_exec_root("cscfg change_profile mockmb303")
        cluster.must_exec_root(
            f"bash -c '{SSH_CMD} cscfg device show -oyaml"
            f" | tee /tmp/inventory'"
        )
        cluster.must_exec_root("bash -c 'cat /tmp/inventory | cscfg device update -f-'")
        cluster.must_exec_root("cscfg server health check")