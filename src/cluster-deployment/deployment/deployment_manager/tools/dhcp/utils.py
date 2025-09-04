import ipaddress
import logging
import pathlib
import subprocess
from typing import Dict, Tuple, List

from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device

logger = logging.getLogger(__name__)


def find_root_server_ip(profile: str, cfg: dict) -> Tuple[str, ipaddress.IPv4Address]:
    root_server = cfg["basic"]["root_server"]
    # ip may be specified outside the inventory since deploy node not in inventory
    root_ip = cfg["basic"].get("root_server_ip")
    if root_ip:
        root_ip = ipaddress.ip_address(root_ip)
    else:
        root_ip = find_host_ip(profile, root_server)
    return root_server, root_ip


def find_host_ip(profile: str, name: str) -> ipaddress.IPv4Address:
    d = Device.get_device(name, profile)
    if d is None:
        raise ValueError(f"device {profile}/{name} was not found")
    ip = d.get_prop(props.prop_management_info_ip)
    if ip is None:
        raise ValueError(f"device {profile}/{name}::{props.prop_management_info_ip} was not found")
    return ip


def approximate_parentnet(subnet_list: List[ipaddress.IPv4Network]) -> ipaddress.IPv4Network:
    """ Calculate the smallest supernet containing the given subnet list """
    networks = [ipaddress.ip_network(subnet, strict=False) for subnet in subnet_list]
    min_ip = min(network.network_address for network in networks)
    max_ip = max(network.broadcast_address for network in networks)

    combined_network = ipaddress.ip_network(f"{min_ip}/{min_ip.max_prefixlen}", strict=False)
    while not (combined_network.network_address <= min_ip and combined_network.broadcast_address >= max_ip):
        combined_network = combined_network.supernet()
    if combined_network.prefixlen == 0:
        raise ValueError(f"no common subnet found for IP range {min_ip} to {max_ip}")
    return combined_network


def find_interfaces_in_network(parentnet: ipaddress.IPv4Network) -> List[str]:
    """ Get interface names which have an address in parentnet """
    try:
        out = subprocess.check_output([
            'bash',
            '-c',
            'ip -j a | jq -r \'.[] | select(.addr_info[].scope == "global") |'
            '[.ifname, (.addr_info[]|select(.family == "inet")|.local)] | @csv\''
        ]).decode()
        lines = out.strip().replace('"', '').splitlines()
        interfaces = set()
        for line in lines:
            parts = line.split(",")
            for addr in parts[1:]:
                if ipaddress.ip_address(addr) in parentnet:
                    interfaces.add(parts[0])
        return sorted(list(interfaces))
    except Exception as e:
        logger.warning(f"unable to determine local IP addresses: {e}")
        return []
