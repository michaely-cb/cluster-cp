import dataclasses
import ipaddress
import logging
from typing import Dict, List, Tuple

from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device
from deployment_manager.tools.dhcp import CSX_MAC_MATCHER, DEFAULT_CEREBRAS_INTERNAL_DOMAIN, DEFAULT_TFTP_PATH
from deployment_manager.tools.dhcp.models import DhcpConfig, Host, Subnet
from deployment_manager.tools.dhcp.utils import find_host_ip, find_root_server_ip, find_interfaces_in_network, \
    approximate_parentnet
from deployment_manager.tools.utils import from_duration

logger = logging.getLogger(__name__)


def _v1_scan_hosts(profile: str, root_server: str) -> Tuple[List[Host], List[Host]]:
    """
    Returns: dynamic hosts, static hosts
    """
    dhcp_ips = []

    for d in Device.get_servers(profile):
        if d.name == root_server:
            continue

        client_class = f"type_{d.device_type.lower()}"

        # IPMI
        ipmi_ip = d.get_prop(props.prop_ipmi_info_ip)
        ipmi_mac = d.get_prop(props.prop_ipmi_info_mac)
        ipmi_name = d.get_prop(props.prop_ipmi_info_name)
        if ipmi_ip is not None and (bool(ipmi_mac) or bool(ipmi_name)):
            default_alias = d.name + "-ipmi"
            dhcp_ips.append(Host(
                ip=ipmi_ip,
                hostname=ipmi_name,
                mac=ipmi_mac,
                dns_name=ipmi_name,
                aliases=[] if default_alias == ipmi_name else [default_alias],
                client_class=client_class,
            ))
        else:
            logger.warning(f"device {profile}/{d.name} IPMI did not have an ip ({ipmi_ip}) "
                           f"or ipmi mac/dnsname ({ipmi_mac}/{ipmi_name})")

        # MGMT
        mgmt_ip = d.get_prop(props.prop_management_info_ip)
        mgmt_mac = d.get_prop(props.prop_management_info_mac)
        if mgmt_ip is not None and bool(mgmt_mac):
            dhcp_ips.append(Host(
                hostname=d.name,
                ip=mgmt_ip,
                mac=mgmt_mac,
                client_class=client_class,
            ))
        else:
            logger.warning(f"device {profile}/{d.name} mgmt did not have an ip ({mgmt_ip}) or mac ({mgmt_ip})")

    # systems are assigned using a mac matcher that is statically added
    # switches are statically assigned
    static_ips = []

    def append_static_host(d: Device, hosts: List[Host]):
        mgmt_ip = d.get_prop(props.prop_management_info_ip)
        if mgmt_ip is not None:
            hosts.append(Host(
                ip=mgmt_ip,
                hostname=d.name,
            ))
        else:
            logger.warning(f"device {profile}/{d.name} mgmt did not have an ip ({mgmt_ip})")
    for d in Device.get_systems(profile):
        append_static_host(d, static_ips)
    for d in Device.get_switches(profile):
        append_static_host(d, static_ips)

    # special host entry for mgmt-node. PXE boot uses "mgmt-node.cerebras.internal" to resolve
    # early boot files during OS install. Since root server will be hosting tftp server, add it as mgmt-node
    root_server_ip = find_host_ip(profile, root_server)
    static_ips.append(Host(hostname=root_server, ip=root_server_ip, aliases=["mgmt-node", "mgmt-node.cerebras.internal"],))

    return dhcp_ips, static_ips


def _get_v1_dhcp_conf(profile: str, cfg: dict) -> Tuple[DhcpConfig, Dict[str, DhcpConfig]]:
    """
    Legacy method of creating dnsmasq config. Read subnets from the input.yaml file and then group
    devices into those subnets. Makes the assumption of 1 CSX per rack which is matched on by the
    3 byte prefix.
    """
    root_server, root_ip = find_root_server_ip(profile, cfg)

    mgmt_doc = cfg["mgmt_network_int_config"]
    lease_duration = from_duration(mgmt_doc.get("default_lease_time", "1h"))
    dynamic_lease_duration = from_duration(mgmt_doc.get("default_dynamic_lease_time", "10m"))
    subnets = {ipaddress.ip_network(sn): [] for sn in mgmt_doc.get("mgmt_ip_blocks", [])}
    dhcp_hosts, static_hosts = _v1_scan_hosts(profile, root_server)
    for host in dhcp_hosts:
        for sn, sn_hosts in subnets.items():
            if host.ip in sn:
                sn_hosts.append(host)
                break
        else:
            logger.warning(f"host {host.hostname} with ip {host.ip} did not fall into an IP block. "
                           "It will not be added to DHCP config but will be added to DNS")
            continue

    gateway_index = mgmt_doc.get("mgmt_gateway_nth_address_per_rack", -1)

    def calc_gateway(sn: ipaddress.IPv4Network, offset: int) -> str:
        if abs(offset) > sn.num_addresses:
            raise ValueError("illegal value for 'mgmt_gateway_nth_address_per_rack': "
                             f"offset larger than subnet size ({sn.prefixlen})")
        if offset > 0:
            return str(sn.network_address + offset)
        return str(sn.broadcast_address + offset)

    root_cfg = DhcpConfig(
        domain=cfg["basic"].get("domain", DEFAULT_CEREBRAS_INTERNAL_DOMAIN),
        dns_servers=mgmt_doc.get("mgmt_dns", []),
        ntp_servers=[str(root_ip)],
        tftp_path=DEFAULT_TFTP_PATH,
        default_reserved_lease_duration=lease_duration,
        default_dynamic_lease_duration=dynamic_lease_duration,
        ip=root_ip,
        listen_interfaces=find_interfaces_in_network(approximate_parentnet(list(subnets.keys()))),
        static_hosts=static_hosts,
    )
    for i, sn in enumerate(subnets.keys()):
        root_cfg.subnets.append(
            Subnet(
                name=f"subnet{i}",
                subnet=sn,
                gateway=calc_gateway(sn, gateway_index),
                hosts=subnets[sn],
            )
        )
        # TODO: this is a hack to statically assign the IP of the cs2. Get this from the config/db
        if not cfg["mgmt_network_int_config"].get("disable_csx_ip_assignment", False):
            root_cfg.subnets[-1].hosts.append(
                Host(ip=sn.network_address + 122, mac=CSX_MAC_MATCHER, tags=["cs2"], client_class="type_sy")
            )

    mirror_cfgs = {}
    for mirror in cfg["basic"].get("mirror_dhcp_servers", []):
        mirror_ip = find_host_ip(profile, mirror)
        mirror_cfgs[mirror] = dataclasses.replace(root_cfg, tftp_path="", ip=mirror_ip)
        mirror_cfgs[mirror].dns_servers = [str(mirror_ip), *mgmt_doc.get("mgmt_dns", [])]
        mirror_cfgs[mirror].ntp_servers = [str(mirror_ip)]
    return root_cfg, mirror_cfgs
