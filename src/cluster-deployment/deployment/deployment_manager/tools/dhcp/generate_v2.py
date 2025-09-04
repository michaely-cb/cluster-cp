import dataclasses
import ipaddress
import logging
from typing import Dict, List, Optional, Tuple

from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device
from deployment_manager.tools.dhcp import CSX_MAC_MATCHER, DEFAULT_CEREBRAS_INTERNAL_DOMAIN, DEFAULT_TFTP_PATH
from deployment_manager.tools.dhcp.generate_v1 import find_host_ip
from deployment_manager.tools.dhcp.models import DhcpConfig, IpRange, Host, MacTag, Subnet
from deployment_manager.tools.dhcp.utils import find_root_server_ip, find_interfaces_in_network
from deployment_manager.tools.ip_allocator import PREFIX_SETTINGS
from deployment_manager.tools.utils import from_duration

logger = logging.getLogger(__name__)

@dataclasses.dataclass(frozen=True)
class _GenerateCtx:
    profile: str
    root_server: str
    tftp_server: str
    deploy_subnets: Dict[ipaddress.IPv4Network, Subnet] = dataclasses.field(default_factory=dict)


def _get_boot_opts(host: Host, d: Device) -> Tuple[Dict[int, str], Dict[int, str]]:
    """ Get device DHCP options tagged to that specific device """
    options = {}
    vs_options = {}
    opts = {
        43: props.prop_network_config_dhcp_option_43,
        66: props.prop_network_config_dhcp_option_66,
        67: props.prop_network_config_dhcp_option_67,
        240: props.prop_network_config_dhcp_option_240,
    }
    for opt_number, opt_prop in opts.items():
        opt_val = d.get_prop(opt_prop)
        if opt_val:
            if opt_number == 43:
                for i, subopt in enumerate(opt_val.split(',')):
                    if subopt:
                        vs_options[i] = subopt
            else:
                options[opt_number] = opt_val

    bootp_boot_file = d.get_prop(props.prop_network_config_dhcp_boot_file)
    if bootp_boot_file and 67 not in options:
        options[67] = bootp_boot_file

    if options:
        host.set_tag = d.name

    return options, vs_options


def _get_hosts(ctx: _GenerateCtx, d: Device) -> List[Host]:
    hosts = []
    client_class = f"type_{d.device_type.lower()}"

    # MGMT
    mgmt_ip = d.get_prop(props.prop_management_info_ip)
    if mgmt_ip:
        mgmt_mac = d.get_prop(props.prop_management_info_mac) or ""
        dns_name = d.get_prop(props.prop_management_info_name) or ""
        hostname, dnsname = (dns_name, d.name,) if dns_name else (d.name, "",)
        host = Host(hostname=hostname, ip=mgmt_ip, mac=mgmt_mac, client_class=client_class,)
        if mgmt_mac:
            opts, vs_opts = _get_boot_opts(host, d)
            if opts:
                host.dhcp_options = opts
            if vs_opts:
                host.dhcp_vs_options = vs_opts
        if dnsname:
            host.aliases.append(dnsname)
        hosts.append(host)

    if d.device_type != "SR":
        return hosts

    # IPMI
    ipmi_ip = d.get_prop(props.prop_ipmi_info_ip)
    ipmi_mac = d.get_prop(props.prop_ipmi_info_mac)
    ipmi_dnsname = d.get_prop(props.prop_ipmi_info_name)
    ipmi_preferred_name = d.name + "-ipmi"
    if ipmi_ip is not None and (ipmi_mac is not None or ipmi_dnsname is not None):
        if ipmi_dnsname and ipmi_dnsname != ipmi_preferred_name:
            # the A record will point to the default dnsname while the cname for host-ipmi -> default dnsname
            host = Host(ip=ipmi_ip, hostname=ipmi_dnsname, mac=ipmi_mac, dns_name=ipmi_dnsname, aliases=[ipmi_preferred_name], client_class=client_class,)
        else:
            # the A record will point to host-ipmi
            host = Host(ip=ipmi_ip, hostname=ipmi_preferred_name, mac=ipmi_mac, dns_name=ipmi_dnsname, client_class=client_class,)
        hosts.append(host)
    else:
        logger.warning(f"device {ctx.profile}/{d.name} IPMI did not have an ip ({ipmi_ip}) "
                       f"or ipmi mac/dnsname ({ipmi_mac}/{ipmi_dnsname})")
    return hosts


def _get_subnet(ctx: _GenerateCtx, d: Device) -> Optional[Subnet]:
    # RACK SUBNET INFO
    sw_tag = f"lan_{d.name}"

    sn = d.get_prop(props.prop_subnet_info_subnet)
    gateway = d.get_prop(props.prop_subnet_info_gateway)
    lease_time = d.get_prop(props.prop_subnet_info_lease_time)
    lease_time = 0 if not lease_time else from_duration(lease_time)
    if sn and gateway and sn.prefixlen in PREFIX_SETTINGS:
        prefix_settings = PREFIX_SETTINGS.get(sn.prefixlen)
        subnet = Subnet(
            name=sw_tag,
            subnet=sn,
            gateway=gateway,
            lease_duration=lease_time,
            dynamic_range=IpRange(
                start_ip=sn.broadcast_address + prefix_settings.dynamic_start_offset,
                end_ip=sn.broadcast_address + prefix_settings.dynamic_end_offset,
                comment="dynamic range"
            ),
        )
    else:
        logger.warning(f"mgmt switch {ctx.profile}/{d.name} did not have {props.prop_subnet_info_subnet} "
                       f"or {props.prop_subnet_info_gateway} assigned")
        subnet = None
    return subnet


def _scan_devices(ctx: _GenerateCtx) -> Tuple[List[Subnet], List[Host]]:
    """
    Extract DHCP options for each rack (ip ranges, gateway, boot options, etc). If the rack does not have a MG
    switch associated with it or that MG switch does not have a subnet associated with it, then consider that rack's
    hosts as static hosts.
    Returns:
        List of subnets
        List of static hosts
    """
    ip_host = {}
    subnets = {}

    def add_hosts(hosts: List[Host]):
        for h in hosts:
            if h.ip in ip_host:
                logger.error(f"duplicate value for {props.prop_management_info_ip} or {props.prop_ipmi_info_ip}={h.ip}")
            else:
                ip_host[h.ip] = h

    def add_subnet(subnet: Subnet):
        assert subnet.subnet not in subnets, f"duplicate value for {props.prop_subnet_info_subnet}={subnet.subnet}"
        subnets[subnet.subnet] = subnet

    for sn in ctx.deploy_subnets.values():
        add_subnet(sn)

    for device in Device.get_all(ctx.profile):
        # MG switches usually have a subnet managed by the deployment manager. MA and FE switches might depending
        # on the data center configuration.
        if device.device_type == "SW" and device.device_role in ("MG", "MA", "FE"):
            subnet = _get_subnet(ctx, device)
            if not subnet:
                if device.device_role == "MG":
                    logger.warning(
                        f"management switch {device.name} did not have a property subnet_info.subnet assigned. "
                        "Connected devices may not appear in DHCP. You may need to run `device assign_mgmt_ips`.")
            else:
                add_subnet(subnet)
        hosts_ = _get_hosts(ctx, device)
        add_hosts(hosts_)

    static_hosts = []
    # find subnet association of non-static hosts, bucket static hosts
    sorted_subnets: List[Subnet] = sorted(list(subnets.values()), key=lambda sn: sn.subnet)
    sorted_hosts: List[Host] = sorted(list(ip_host.values()), key=lambda h: h.ip)
    subnet_i, host_i = 0, 0
    while host_i < len(sorted_hosts):
        host = sorted_hosts[host_i]
        host_i += 1
        while subnet_i < len(sorted_subnets) and sorted_subnets[subnet_i].subnet.broadcast_address < host.ip:
            subnet_i += 1
        if host.is_static or subnet_i >= len(sorted_subnets) or host.ip not in sorted_subnets[subnet_i].subnet:
            static_hosts.append(host)
        else:
            sorted_subnets[subnet_i].hosts.append(host)

    # sanity check subnet values for potential overlap
    for subnet_i in range(1, len(sorted_subnets)):
        l, r = sorted_subnets[subnet_i - 1], sorted_subnets[subnet_i]
        if l.subnet.overlaps(r.subnet):
            raise AssertionError(f"subnet {l.name}/{l.subnet} overlaps with {r.name}/{r.subnet}")

    return sorted_subnets, static_hosts


def _get_v2_dhcp_conf(profile: str, cfg: dict) -> Tuple[DhcpConfig, Dict[str, DhcpConfig]]:
    """
    Creates dnsmasq file. Supports the following features
    - deploy_subnets: special subnets containing the mg switches
    - dhcp_boot options per host: allowing fine-grained control of PXE/ZTP boot per node/switch
    - dynamic ranges: within each mg switch subnet, create 2 dynamic ranges: a CSX range for
            CS devices, and a generic `debug` range for any device without a dhcp-host entry.
            Deploy subnets only get a debug dynamic range.
    - mirror dhcp servers: config generated for mirrors strips out any dynamic assignment capabilities
            and boot files to prevent possible conflicting IP assignments / incorrectly configured boot
            servers
    """
    root_server, root_ip = find_root_server_ip(profile, cfg)

    mgmt_doc = cfg["mgmt_network_int_config"]
    default_lease_duration = from_duration(mgmt_doc.get("default_lease_time", "1h"))
    dynamic_lease_duration = from_duration(mgmt_doc.get("default_dynamic_lease_time", "10m"))
    parentnet = ipaddress.ip_network(mgmt_doc.get("ip_allocation", {}).get("parentnet"))

    # deploy network
    extents = mgmt_doc.get("ip_allocation", {}).get("network_extents", [])
    deploy_subnets = {}
    for e in extents:
        if e["name"] == "deploy_mgmt":
            for i, subnet in enumerate(sorted([ipaddress.ip_network(sn) for sn in e["subnets"]])):
                deploy_gw = subnet.broadcast_address - 1
                prefix_settings = PREFIX_SETTINGS.get(subnet.prefixlen)
                if prefix_settings:
                    dyn_range = IpRange(
                        start_ip=subnet.broadcast_address + prefix_settings.dynamic_start_offset,
                        end_ip=subnet.broadcast_address + prefix_settings.dynamic_end_offset,
                        comment="deploy network dynamic range",
                    )
                else:
                    dyn_range = None
                deploy_subnets[subnet] = Subnet(
                    name=f"deploy_mgmt_{i}",
                    subnet=subnet,
                    gateway=str(deploy_gw),
                    dynamic_range=dyn_range,
                )

    ctx = _GenerateCtx(profile=profile, root_server=root_server,
                       tftp_server=str(root_ip), deploy_subnets=deploy_subnets)

    subnets, static_hosts = _scan_devices(ctx)

    # special host entry for mgmt-node. The iPXE embedded boot script uses "mgmt-node.cerebras.internal" to resolve
    # early boot files during OS install. Since root server will be hosting tftp server, add it as mgmt-node
    for alias in ("mgmt-node", "mgmt-node.cerebras.internal", root_server):
        static_hosts.append(Host(hostname=alias, ip=root_ip))

    dns_servers = mgmt_doc.get("mgmt_dns", [])
    root_cfg = DhcpConfig(
        domain=cfg["basic"].get("domain", DEFAULT_CEREBRAS_INTERNAL_DOMAIN),
        dns_servers=dns_servers,
        ntp_servers=[str(root_ip)],
        tftp_path=DEFAULT_TFTP_PATH,
        ip=root_ip,
        listen_interfaces=find_interfaces_in_network(parentnet),
        default_reserved_lease_duration=default_lease_duration,
        default_dynamic_lease_duration=dynamic_lease_duration,
        mac_tags=[MacTag(set_tag="csx", mac=CSX_MAC_MATCHER)],
        subnets=sorted(subnets, key=lambda sn: sn.subnet),
        static_hosts=sorted(static_hosts, key=lambda h: h.ip),
    )

    # mirrors are similar to root but removes the host-specific and global dhcp boot options
    mirror_dns = [root_ip] + mgmt_doc.get("mgmt_dns", []).copy() # set root as upstream to resolve dynamic IPs
    mirror_cfgs = {}
    for mirror in cfg["basic"].get("mirror_dhcp_servers", []):
        mirror_ip = find_host_ip(profile, mirror)
        mirror_cfg = dataclasses.replace(
            root_cfg,
            dns_servers=mirror_dns,
            ntp_servers=[str(mirror_ip), str(root_ip)],
            tftp_path="",
            ip=mirror_ip,
            listen_interfaces=[]  # HACK: dnsmasq will listen on all interfaces when none are specified
        )

        # remove dynamic host options to prevent diverging IP assignments
        mirror_cfg.subnets = [
            dataclasses.replace(s, dynamic_range=None)
            for s in mirror_cfg.subnets
        ]
        mirror_cfgs[mirror] = mirror_cfg

    return root_cfg, mirror_cfgs
