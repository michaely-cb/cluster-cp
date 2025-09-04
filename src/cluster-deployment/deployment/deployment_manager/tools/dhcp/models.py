import dataclasses
import ipaddress
import jinja2
import json
import logging
from pathlib import Path
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Host:
    ip: ipaddress.IPv4Address
    mac: Optional[str] = None
    hostname: Optional[str] = None
    aliases: List[str] = dataclasses.field(default_factory=list)  # other names to associate an A record
    dns_name: Optional[str] = None  # device default DNS name
    dhcp_options: Dict[int, str] = dataclasses.field(default_factory=dict)
    # Vendor specific options (DHCP option 43)
    dhcp_vs_options: Dict[int, str] = dataclasses.field(default_factory=dict)
    tags: List[str] = dataclasses.field(default_factory=list)
    client_class: str = "type_unset"

    def encode_vs_options(self, colon_separated=False) -> str:
        """
        Encode vendor options (43) according to RFC2132 section 8.4
        Returns:
            str: A hexadecimal string representing the encapsulated options.
        """
        hex_string = ""
        for code, value in sorted(self.dhcp_vs_options.items()):
            code_byte = code.to_bytes(1, byteorder="big")
            value_bytes = value.encode("ascii")
            length = len(value_bytes)
            length_byte = length.to_bytes(1, byteorder="big")
            hex_string += code_byte.hex() + length_byte.hex() + value_bytes.hex()
        if colon_separated:
            return ':'.join(hex_string[i:i+2] for i in range(0, len(hex_string), 2))
        else:
            return hex_string

    @property
    def is_static(self) -> bool:
        """ True if host doesn't contain enough information to create an ip->host association for DHCP IP assignment """
        return not self.mac and not self.dns_name

    def fmt_tags(self) -> str:
        return ",".join([f"tag:{t}," for t in self.tags])


@dataclasses.dataclass
class IpRange:
    start_ip: ipaddress.IPv4Address  # inclusive
    end_ip: ipaddress.IPv4Address  # inclusive
    comment: str = ""


@dataclasses.dataclass
class MacTag:
    """ Tag devices based on a MAC address or address pattern """
    mac: str
    set_tag: str


@dataclasses.dataclass
class Subnet:
    """
    Specifies a subnet with an optional dynamic portion of the range.
    dnsmasq supports setting tags for multiple sub-range but this representation
    is just simple enough to support our needs
    """
    name: str
    subnet: ipaddress.IPv4Network
    gateway: str = ""  # gateway IP address, DHCP option 3
    lease_duration: Optional[int] = None
    dynamic_range: Optional[IpRange] = None

    hosts: List[Host] = dataclasses.field(default_factory=list)

    @property
    def has_dynamic(self) -> bool:
        return self.dynamic_range is not None

    @property
    def has_static(self) -> bool:
        return len(self.hosts) > 0

    @property
    def subnet_id(self) -> int:
        """ Unique ID of subnet - required for kea. Has to be a stable, unique int32 """
        return int(self.subnet.network_address)


@dataclasses.dataclass
class DhcpConfig:
    """ An opinionated set of dnsmasq configs for use in templating """
    domain: str
    ip: ipaddress.IPv4Address
    listen_interfaces: List[str]
    dns_servers: List[str] = dataclasses.field(default_factory=list)
    ntp_servers: List[str] = dataclasses.field(default_factory=list)
    tftp_path: Optional[str] = None
    default_reserved_lease_duration: int = 24 * 60 * 60  # 24 hours
    default_dynamic_lease_duration: int = 60 * 60  # 1 hour
    subnets: List[Subnet] = dataclasses.field(default_factory=list)
    mac_tags: List[MacTag] = dataclasses.field(default_factory=list)
    static_hosts: List[Host] = dataclasses.field(default_factory=list)  # hosts with static IP - 100G switches

    @property
    def client_classes(self) -> List[str]:
        cc = {"type_sr", "type_sw", "type_sy"}
        for sn in self.subnets:
            for host in sn.hosts:
                cc.add(host.client_class)
        return sorted(list(cc))


def render_dnsmasq_config(conf_vars: DhcpConfig) -> str:
    """
    Render a single dnsmasq config file to replace /etc/dnsmasq.conf
    In the future, we should take advantage of `dhcp-hostsfile` which allows
    reloading host definitions without restarting dnsmasq
    Args:
        conf_vars: dnsmasq configs

    Returns: rendered template
    """
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(Path(__file__).parent.parent.parent / "templates"),
        autoescape=jinja2.select_autoescape(['j2']),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    t = env.get_template('dnsmasq.conf.j2')
    return t.render(config=conf_vars)


def render_dnsmasq_hosts(conf_vars: DhcpConfig) -> str:
    """
    Create a hosts file from all devices that don't already have a dhcp-host entry

    TODO: ideally aliases should be CNAME records in dnsmasq. Tested on CG3 - seems there's some performance issue
    that causes 100% cpu utilization using the following template:
    {% if host.hostname %}
    {% for alias in host.aliases %}
    cname={{ alias }}.{{ config.domain }}.,{{ host.hostname }}.{{ config.domain }}.
    {% endfor %}
    {% endif %}
    Ultimately, the CNAME record is nice-to-have so I didn't root cause it.

    Args:
        conf_vars: dnsmasq configs
    Returns: rendered template
    """
    hosts = []
    for subnet in conf_vars.subnets:
        for h in subnet.hosts:
            if not h.ip:
                continue
            if h.hostname:
                hosts.append((h.ip, h.hostname,))
            for alias in h.aliases:
                hosts.append((h.ip, alias,))
    for h in conf_vars.static_hosts:
        hosts.append((h.ip, h.hostname,))
        for alias in h.aliases:
            hosts.append((h.ip, alias,))

    # sort final output
    hosts = sorted(hosts, key=lambda h: h[0])
    output = [f"{h[0]} {h[1]}" for h in hosts]
    return "\n".join(output) + "\n"


def render_kea_dhcp_config(conf_vars: DhcpConfig) -> str:
    """
    Render a single kea config file to replace /etc/kea/kea-dhcp4.conf
    Args:
        conf_vars: dhcp configs

    Returns: rendered template
    """
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(Path(__file__).parent.parent.parent / "templates"),
        autoescape=jinja2.select_autoescape(['j2']),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    t = env.get_template('kea-dhcp4.conf.j2')
    render = t.render(config=conf_vars)
    try:
        # reformat JSON doc for consistency - the templated json isn't well formatted
        return json.dumps(json.loads(render), indent=2)
    except json.decoder.JSONDecodeError as e:
        debug_path = '/tmp/kea-dhcp4-fail.json'
        with open(debug_path, 'w') as f:
            f.write(render)
        raise RuntimeError(f"kea templating failed to produce valid JSON, inspect '{debug_path}'")


def render_kea_dhcp_ddns_config(v: dict) -> str:
    """
    Returns: rendered template
    """
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(Path(__file__).parent.parent.parent / "templates"),
        autoescape=jinja2.select_autoescape(['j2']),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    t = env.get_template('kea-dhcp-ddns.conf.j2')
    render = t.render(v)
    try:
        # reformat JSON doc for consistency - the templated json isn't well formatted
        return json.dumps(json.loads(render), indent=2)
    except json.decoder.JSONDecodeError as e:
        debug_path = '/tmp/kea-dhcp-ddns-fail.json'
        with open(debug_path, 'w') as f:
            f.write(render)
        raise RuntimeError(f"kea templating failed to produce valid JSON, inspect '{debug_path}'")
