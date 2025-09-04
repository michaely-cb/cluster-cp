import abc
import csv
import dataclasses
import datetime
import glob
import ipaddress
import logging
import pathlib
import socket
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from deployment_manager.tools.dhcp.generate_v1 import _get_v1_dhcp_conf
from deployment_manager.tools.dhcp.generate_v2 import _get_v2_dhcp_conf
from deployment_manager.tools.dhcp.models import DhcpConfig, render_dnsmasq_config, render_dnsmasq_hosts, \
    render_kea_dhcp_config, render_kea_dhcp_ddns_config
from deployment_manager.tools.powerdns import PowerDNSZoneManager
from deployment_manager.tools.utils import ReprFmtMixin, exec_cmd

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class LeaseEntry(ReprFmtMixin):
    expire_time: int
    mac: str
    ip: ipaddress.IPv4Address
    host: str
    device_id: str = ""

    @property
    def sort_key(self):
        return self.ip

    @classmethod
    def table_header(cls) -> list:
        return ["mac", "ip", "host", "device_id", "expire_time"]

    def to_table_row(self) -> list:
        exp_dt = datetime.datetime.fromtimestamp(self.expire_time, datetime.timezone.utc)
        return [self.mac, str(self.ip), self.host, self.device_id, exp_dt.strftime("%Y-%m-%dT%H:%M:%S%z")]

    def to_dict(self) -> dict:
        return {
            "expire_time": self.expire_time, "mac": self.mac,
            "ip": str(self.ip), "host": self.host, "device_id": self.device_id
        }


class DhcpProvider(abc.ABC):

    def __init__(self, profile: str, cfg: dict):
        self._profile = profile
        self._cfg = cfg

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @abc.abstractmethod
    def generate_configs(self) -> Dict[str, List[Tuple[Path, str]]]:
        """ Generate config files for the dhcp provider
        Args
            profile: profile name
            cfg: profile config (input.yaml)
        Returns
            Dict of HOST -> List of [path the file should be written to be effective, rendered config doc]
        """
        raise NotImplementedError("generate_configs not implemented by this provider")

    def update_dns(self):
        """ Update a DNS server with DHCP reservation information """
        pass

    @abc.abstractmethod
    def get_leases(self, lease_file: Optional[Path] = None) -> List[LeaseEntry]:
        raise NotImplementedError("get_leases not implemented by this provider")

    def expire_lease(self, hosts: List[str]):
        raise NotImplementedError("expire leases is not implemented for this provider")

    def _generate_dhcp_conf(self) -> Tuple[DhcpConfig, Dict[str, DhcpConfig]]:
        """
        Extract dnsmasq configs from master config
        Args:
            cfg: input.yaml dict
        Returns: (root server dnsmasq config, dict[mirror_server_name] -> mirror_server_config)
        """
        generation_method = self._cfg["mgmt_network_int_config"].get(
            "ip_allocation", {}).get("allocation_strategy", "v1").lower()

        if generation_method == "v1":
            return _get_v1_dhcp_conf(self._profile, self._cfg)
        elif generation_method == "v2":
            return _get_v2_dhcp_conf(self._profile, self._cfg)

        raise ValueError(f"unknown dnsmasq generation method: {generation_method}")


class DnsmasqDhcpProvider(DhcpProvider):
    LEASE_FILE = pathlib.Path("/var/lib/dnsmasq/dnsmasq.leases")
    HOSTS_PATH = pathlib.Path("/etc/hosts.d/hosts.conf")
    CONF_PATH = pathlib.Path("/etc/dnsmasq.conf")
    name = "dnsmasq"

    def generate_configs(self) -> Dict[str, List[Tuple[Path, str]]]:
        """ Returns list of paths to dnsmasq config files and path to hosts file """
        root_cfg, mirror_cfgs = self._generate_dhcp_conf()

        root_server = self._cfg["basic"]["root_server"]
        rv = {
            root_server: [
                (self.CONF_PATH, render_dnsmasq_config(root_cfg)),
                (self.HOSTS_PATH, render_dnsmasq_hosts(root_cfg)),
            ]
        }
        for mirror_server, mirror_cfg in mirror_cfgs.items():
            rv[mirror_server] = [
                (self.CONF_PATH, render_dnsmasq_config(mirror_cfg)),
                (self.HOSTS_PATH, render_dnsmasq_hosts(mirror_cfg)),
            ]
        return rv

    def get_leases(self, lease_file: Optional[Path] = None) -> List[LeaseEntry]:
        if not lease_file:
            lease_file = self.LEASE_FILE
        if not lease_file.is_file():
            return []
        doc = self.LEASE_FILE.read_text()
        rv = []
        for line in doc.splitlines():
            # 1714843562 3c:ec:ef:7a:a5:86 172.28.8.243 sc-r1rb1-s9 01:3c:ec:ef:7a:a5:86
            if not line or line.startswith("#"):
                continue
            tokens = line.strip().split(" ")
            try:
                rv.append(LeaseEntry(
                    expire_time=int(tokens[0]),
                    mac=tokens[1],
                    ip=ipaddress.ip_address(tokens[2]),
                    host=tokens[3],
                    device_id=tokens[4],
                ))
            except Exception as e:
                logger.warning(f"failed to parse lease file line: '{line}', {e}")
        return rv

    def expire_lease(self, hosts: List[str]):
        def force_expire():
            for name in hosts:
                exec_cmd(f"sed -i '/\\b{name}\\b/d' {self.LEASE_FILE}")
            exec_cmd("systemctl restart dnsmasq")

        # if old leases are present, force expire leases from previous system names
        leases = self.get_leases()
        for lease in leases:
            if lease.host in hosts:
                force_expire()
                break


class KeaDhcpProvider(DhcpProvider):
    LEASE_FILE_GLOB = "/var/lib/kea/kea-leases4.csv*"
    CONF_PATH = pathlib.Path("/etc/kea/kea-dhcp4.conf")
    DDNS_PATH = pathlib.Path("/etc/kea/kea-dhcp-ddns.conf")
    name = "kea"

    @staticmethod
    def _generate_ddns_vars(cfg: DhcpConfig) -> dict:
        # break subnets into /24's for reverse dns
        reverse_dns = []

        def add24(sn: ipaddress.IPv4Network):
            parts = str(sn).split(".")
            reverse_dns.append(".".join([parts[2], parts[1], parts[0]]))

        for s in cfg.subnets:
            if s.subnet.prefixlen >= 24:
                add24(s.subnet)
            else:
                for sn in s.subnet.subnets(new_prefix=24):
                    add24(sn)

        return {
            "domain": cfg.domain,
            "dns_ip": cfg.dns_servers[0],
            "reverse_dns": sorted(list(set(reverse_dns))),
        }

    def generate_configs(self) -> Dict[str, List[Tuple[Path, str]]]:
        """ Returns list of paths to kea config files """
        root_cfg, mirror_cfgs = self._generate_dhcp_conf()

        ddns_vars = KeaDhcpProvider._generate_ddns_vars(root_cfg)
        tname = self._cfg["mgmt_network_int_config"]["dhcp"].get("kea_tsig_name", None)
        tkey = self._cfg["mgmt_network_int_config"]["dhcp"].get("kea_tsig_key", None)
        if tname and tkey:
            ddns_vars["kea_tsig_name"] = tname
            ddns_vars["kea_tsig_key"] = tkey
        elif tname or tkey:
            logger.warning("kea_tsig_name and kea_tsig_key must be set together to be applied")

        root_server = self._cfg["basic"]["root_server"]
        rv = {
            root_server: [
                (self.CONF_PATH, render_kea_dhcp_config(root_cfg)),
                (self.DDNS_PATH, render_kea_dhcp_ddns_config(ddns_vars))
            ]
        }

        # do post-check
        if mirror_cfgs:
            logger.warning("ignoring mirror servers for kea dhcp provider")
        if not root_cfg.listen_interfaces:
            logger.warning(
                "no listen_interfaces found, check input.yaml::mgmt_network_int_config.ip_allocations.parentnet")

        missing_mac = []
        for sn in root_cfg.subnets:
            for host in sn.hosts:
                if not host.mac:
                    missing_mac.append(host.mac)
        if missing_mac:
            raise RuntimeError(f"invalid kea configuration generated: missing MAC address for hosts: {missing_mac}")
        return rv

    def update_dns(self):
        enable = self._cfg["mgmt_network_int_config"]["dhcp"].get("powerdns_enable", False)
        if not enable:
            logger.warning("not updating powerdns, set powerdns configs in input.yml::mgmt_network_int_config.dhcp")
            return

        host = self._cfg["mgmt_network_int_config"]["dhcp"].get("powerdns_api_url", None)
        key = self._cfg["mgmt_network_int_config"]["dhcp"].get("powerdns_api_key", None)
        if not host or not key:
            raise ValueError("missing 'powerdns_host' or 'powerdns_api_key' configs")
        domain = self._cfg["basic"]["domain"]

        root_cfg, _ = self._generate_dhcp_conf()
        updates = defaultdict(list)
        with PowerDNSZoneManager(host, key, domain) as mgr:
            for host in root_cfg.static_hosts:
                if not host.hostname or "." in host.hostname:  # skip names like 'mgmt-node.cerebras.local'
                    continue
                if mgr.add_a_record(host.hostname, str(host.ip)):
                    updates[f"{host.hostname}.{domain}"].append(str(host.ip))
            for sn in root_cfg.subnets:
                for host in sn.hosts:
                    if not host.hostname or "." in host.hostname:  # skip names like 'mgmt-node.cerebras.local'
                        continue
                    if mgr.add_a_record(host.hostname, str(host.ip)):
                        updates[f"{host.hostname}.{domain}"].append(str(host.ip))
                    for alias in host.aliases:
                        fqdn = f"{host.hostname}.{domain}"
                        if mgr.add_cname_record(alias, fqdn + "."):
                            updates[fqdn].append(str(host.ip))
        logger.info(f"updated {len(updates)} dns records for {domain}")

        if updates:
            timeout, interval = 120, 5
            logger.info(f"checking dns cache updated for max of {timeout} seconds")
            last_check = time.time()
            timeout_time = last_check + timeout
            while updates and last_check < timeout_time:
                time.sleep(max(0.0, interval - (time.time() - last_check)))

                logger.debug(f"awaiting update of {len(updates)} records: {', '.join(updates.keys())}")
                next_updates = {}
                for name, addrs in updates.items():
                    try:
                        addr = socket.gethostbyname(name)
                        if addr not in addrs:
                            next_updates[name] = addrs
                    except socket.gaierror:
                        next_updates[name] = addrs
                updates = next_updates
                last_check = time.time()

            if updates:
                logger.warning(f"waited {timeout} seconds for dns cache updates but {len(updates)} records "
                               "returned NXDOMAIN. You may need to decrease the authoritative dns server's neg "
                               "caching time")
            else:
                logger.info("cache updated")


    def get_leases(self, lease_file: Optional[Path] = None) -> List[LeaseEntry]:
        if lease_file:
            lease_files = [lease_file]
        else:
            lease_files = [pathlib.Path(p) for p in glob.glob(self.LEASE_FILE_GLOB)]
        lease_entries = {}
        for f in lease_files:
            if not f.is_file():
                continue
            with open(f, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    try:
                        lease_entry = LeaseEntry(
                            expire_time=int(row['expire']),
                            mac=row['hwaddr'],
                            ip=ipaddress.ip_address(row['address']),
                            host=row['hostname'],
                            device_id=row.get('client_id', '')
                        )
                        if lease_entry.ip in lease_entries:
                            if lease_entries[lease_entry.ip].expire_time < lease_entry.expire_time:
                                lease_entries[lease_entry.ip] = lease_entry
                        else:
                            lease_entries[lease_entry.ip] = lease_entry
                    except ValueError as e:
                        logger.warning(f"failed to parse lease file line: '{row}', {e}")
        return list(lease_entries.values())

    def expire_lease(self, hosts: List[str]):
        logger.warning("expire_lease not implemented for kea")


def get_provider(profile: str, cfg: dict) -> DhcpProvider:
    provider_name = cfg["mgmt_network_int_config"].get("dhcp", {}).get("provider", "dnsmasq")
    if provider_name == "dnsmasq":
        return DnsmasqDhcpProvider(profile, cfg)
    elif provider_name == "kea":
        return KeaDhcpProvider(profile, cfg)
    else:
        raise RuntimeError("unknown dhcp provider specified at "
                           f"input.yml::mgmt_network_int_config.dhcp.provider={provider_name}")
