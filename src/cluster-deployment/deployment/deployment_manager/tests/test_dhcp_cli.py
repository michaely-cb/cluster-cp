import logging
from pathlib import Path

from deployment_manager.tests.conftest import MockCluster

logger = logging.getLogger(__name__)


ASSETS_DIR = Path(__file__).parent / "assets"


def test_dhcp_conf_v1(tmp_path):
    """ generate devtest cluster dnsmasq """
    with MockCluster("dnsmasq_v1") as cluster:
        cluster.must_exec_root("/project/install.sh")
        cluster.must_exec_root(
            "cscfg profile create devtest "
            "--inventory_file /host/config/devtest_inventory.csv "
            "--config_input /host/config/devtest_input.yml"
        )
        cluster.must_exec_root("mkdir -p /home/dnsmasq_v1")

        cluster.must_exec_root("cscfg dhcp update --dryrun", environment={"OUTPUT_DIR": "/home/dnsmasq_v1"})
        dnsmasq_doc = cluster.must_exec_root("cat /home/dnsmasq_v1/sc-r1rb1-s4/dnsmasq.conf")
        expected_doc = (ASSETS_DIR / "devtest_dnsmasq.conf").read_text()
        assert dnsmasq_doc == expected_doc, "generated dnsmasq doc differed from expected"

        hosts_doc = cluster.must_exec_root("cat /home/dnsmasq_v1/sc-r1rb1-s4/hosts.conf")
        assert "172.28.8.19 mgmt-node" in hosts_doc
        assert "172.28.8.132 sc-r1rb1-s5-ipmi" in hosts_doc
        assert "172.28.0.140 sc-r1rb1-1gsw" in hosts_doc
        assert "172.28.0.141 sc-r1rb1-100gsw32" in hosts_doc
