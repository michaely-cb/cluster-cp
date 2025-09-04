import ipaddress
import json
import logging

import pytest

from deployment_manager.tools.dhcp import models
from deployment_manager.tools.dhcp.interface import KeaDhcpProvider
from deployment_manager.tools.dhcp.models import IpRange
from deployment_manager.tools.dhcp.utils import approximate_parentnet

logger = logging.getLogger(__name__)


root_ip = ipaddress.ip_address("172.16.0.1")


def _make_dhcp_config() -> models.DhcpConfig:
    cfg = models.DhcpConfig(
        domain="cerebras.local",
        ip=root_ip,
        default_reserved_lease_duration=60*60*24,
        default_dynamic_lease_duration=60 * 2,
        listen_interfaces=["eth0", "eth1"],
        ntp_servers=[str(root_ip)],
        dns_servers=["1.1.1.1", "8.8.8.8"],
        tftp_path="/var/tftp",
        static_hosts=[models.Host(ip=root_ip, hostname="mgmt-node")]
    )
    sn0 = models.Subnet(
        name="net001",
        subnet=ipaddress.ip_network("172.16.0.0/22"),
        gateway="172.16.0.254",
        hosts=[models.Host(ip=ipaddress.ip_address("172.16.0.2"), mac="00:00:00:00:00:01", hostname="srv1", client_class="type_sr"),
               models.Host(ip=ipaddress.ip_address("172.16.0.65"), mac="00:00:00:00:00:02", hostname="srv1-ipmi", client_class="type_sr"),
               models.Host(ip=ipaddress.ip_address("172.16.0.66"), dns_name="xyzipminame", hostname="srv2-ipmi", client_class="type_sr"),
               models.Host(ip=ipaddress.ip_address("172.16.0.67"), mac="", dns_name=None, hostname="empty-mac", client_class="type_sr"),
               models.Host(ip=ipaddress.ip_address("172.16.0.127"), mac="00:00:00:00:00:03", hostname="dell-sw0",
                           dhcp_options={66: "172.10.0.1", 67: "dell_switch.px.0"}, client_class="type_sw"),
               models.Host(ip=ipaddress.ip_address("172.16.0.126"), mac="00:00:00:00:00:04", hostname="sys-0", tags=["system"], client_class="type_sy"),
               ]
    )
    sn0.dynamic_range = IpRange(ipaddress.ip_address("172.16.0.129"), ipaddress.ip_address("172.16.0.253"))

    sn1 = models.Subnet(
        name="net002",
        gateway="172.16.4.126",
        subnet=ipaddress.ip_network("172.16.4.0/25"),
        hosts=[models.Host(ip=ipaddress.ip_address("172.16.4.125"), mac="00:00:00:00:00:05", hostname="dell-sw1", client_class="type_sw")]
    )
    sn2 = models.Subnet(
        name="net003",
        gateway="172.16.5.254",
        lease_duration=60*60,
        subnet=ipaddress.ip_network("172.16.5.0/24")
    )
    sn2.dynamic_range = IpRange(
        start_ip=ipaddress.ip_address("172.16.5.10"),
        end_ip=ipaddress.ip_address("172.16.5.20"),
        comment="dynamic range",
    )

    cfg.subnets = [sn0, sn1, sn2]
    return cfg


def test_dnsmasq_dhcp_conf():
    """ basic smoke test for generating dnsmasq configs """
    cfg = _make_dhcp_config()
    cfg_doc = models.render_dnsmasq_config(cfg)
    hosts_doc = models.render_dnsmasq_hosts(cfg)

    with open("/tmp/dnsmasq.conf", 'w') as f:  # for debug
        f.write(cfg_doc)

    # a better test would load these into a container and user dnsmasq --test
    assert cfg_doc
    assert f"interface=eth0" in cfg_doc
    assert f"interface=eth1" in cfg_doc
    assert "dhcp-host=set:type_sr,00:00:00:00:00:02,172.16.0.65,srv1-ipmi" in cfg_doc
    assert "dhcp-host=set:type_sr,xyzipminame,172.16.0.66,srv2-ipmi" in cfg_doc
    assert "172.16.0.67" not in cfg_doc, "IP for 172.16.0.67 should not exist since MAC is empty string and dns name is None"

    assert hosts_doc
    assert f"{root_ip} mgmt-node" in hosts_doc

    logger.info(f"dnsmasq.conf\n{cfg_doc}")
    logger.info(f"hosts\n{hosts_doc}")


def test_kea_dhcp_conf():
    """ basic smoke test for generating kea configs """
    cfg = _make_dhcp_config()
    cfg_doc = models.render_kea_dhcp_config(cfg)

    doc = json.loads(cfg_doc)
    subnets = doc["Dhcp4"]["subnet4"]
    assert 3 == len(subnets)
    subnet0 = subnets[0]
    assert 2 == len(subnet0["pools"])

    ddns_vars = KeaDhcpProvider._generate_ddns_vars(cfg)
    ddns_doc = models.render_kea_dhcp_ddns_config(ddns_vars)

    ddns_vars["kea_tsig_key"] = "xxx"
    ddns_vars["kea_tsig_name"] = "default"
    ddns_doc = models.render_kea_dhcp_ddns_config(ddns_vars)
    ddns_doc = json.loads(ddns_doc)
    arpas = [
        ".".join(dd["name"].split(".")[:3])
        for dd in ddns_doc["DhcpDdns"]["reverse-ddns"]["ddns-domains"]
    ]
    assert arpas == ['0.16.172', '1.16.172', '2.16.172', '3.16.172', '4.16.172', '5.16.172']


def test_approximate_parenetnet():
    assert "10.0.0.0/23" == str(approximate_parentnet(["10.0.0.0/24", "10.0.1.0/26"]))
    assert "10.0.0.0/24" == str(approximate_parentnet(["10.0.0.0/24"]))
    with pytest.raises(ValueError):
        approximate_parentnet(["10.0.1.0/24", "172.0.0.0/12"])