import json
import logging
import os
import pytest
import yaml

from deployment_manager.tests.conftest import MockCluster

logger = logging.getLogger(__name__)

CLUSTER_IP_PROFILE = [
    ("172.28.107.1", "mb307"),
    ("172.28.224.1", "mb303"),
]


@pytest.mark.skip("Skip as it depends on a real cluster availability")
@pytest.mark.parametrize("mgmt_ip, profile", CLUSTER_IP_PROFILE)
def test_ipmi_mb(mgmt_ip, profile):
    assert "CEREBRAS_PASSWORD" in os.environ.keys(), "Set ENV CEREBRAS_PASSWORD"

    cluster = MockCluster("validator", has_network=False)
    SSH_CMD = (
        f"sshpass -p {os.environ['CEREBRAS_PASSWORD']} "
        f"ssh -o StrictHostKeyChecking=no root@{mgmt_ip}"
    )
    with cluster:
        cluster.must_exec_root("/project/install.sh")
        cluster.must_exec_root(
            f"bash -c '{SSH_CMD} cat /opt/cerebras/cluster-deployment/meta/{profile}/input.yml | tee /tmp/input.yml'"
        )
        cluster.must_exec_root(
            f"bash -c '{SSH_CMD} cat /var/lib/dnsmasq/dnsmasq.leases | tee /var/lib/dnsmasq/dnsmasq.leases'"
        )
        cluster.must_exec_root(
            f"cscfg profile create mock{profile} "
            f"--config_input /tmp/input.yml"
        )
        cluster.must_exec_root(f"cscfg profile change mock{profile}")

        cluster.must_exec_root(
            f"bash -c '{SSH_CMD} cscfg device show -oyaml"
            f" | tee /tmp/inventory'"
        )
        cluster.must_exec_root(
            "bash -c 'cscfg device update - </tmp/inventory'"
        )
        if profile == "mb303":
            cluster.must_exec_root(
                f"cscfg server ipmi create_user "
                f"--filter name=cs303-wse001-sx-sr07 "
                f"--username readonly --password readonly --role ReadOnly -y"
            )
