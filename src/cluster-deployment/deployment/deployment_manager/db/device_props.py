import dataclasses
import ipaddress
import typing
from typing import Dict, Optional, Union, Type, Any
from deployment_manager.tools.utils import parse_mac


class ASN(int):
    pass

class MAC(str):
    pass

def mac(v: Union[None,int,str]) -> Optional[MAC]:
    """ Parse a MAC, or something that looks like an MAC. No validation """
    if v is None:
        return None
    return parse_mac(v)

def asn(v: Union[None,int,str]) -> Optional[ASN]:
    """ Parse an ASN, or something that looks like an ASN. No validation """
    if v is None:
        return None
    if isinstance(v, int):
        return ASN(v)
    if "." in v:
        a, b = v.split(".", maxsplit=2)
        return ASN(int(a) << 16 + int(b))
    return ASN(int(v))


def parse_bool(v: str) -> bool:
    normalized = v.strip().lower()
    true_values = {'true', '1', 't', 'yes', 'y', 'enabled'}
    false_values = {'false', '0', 'f', 'no', 'n', 'disabled'}
    if normalized in true_values:
        return True
    elif normalized in false_values:
        return False
    else:
        raise ValueError(f"Invalid boolean value: {v}")


DevicePropValueType = Union[Type[str], Type[int], Type[ipaddress.IPv4Address], Type[ipaddress.IPv4Network], Type[ASN]]


@dataclasses.dataclass(frozen=True)
class DeviceProp:
    """
    Well-known device properties. The device_properties table includes both
    version information about software running on the device as well as more
    general properties of the device that aren't expected to change frequently or
    at all.
    """
    name: str
    attr: str

    default_value: Optional[Any] = None
    value_type: DevicePropValueType = str
    comment: str = ""

    title: str = ""

    sensitive: bool = False  # passwords, keys, etc

    def __post_init__(self):
        if not self.title:
            object.__setattr__(self, "title", f"{self.name}.{self.attr}")

    def validate_value(self, value: str) -> typing.Tuple[bool, str]:
        """Validate value using both type casting and regex pattern"""
        if value is None:
            return True, ""
        # Try type validation first (this will catch invalid IPs)
        if self.cast_value(value) is None and self.value_type != str:            
            msg = ''
            if self.value_type == MAC:
                msg = f"Expected format like xx:xx:xx:xx:xx:xx (valid numbers 0-9, letters a-f)."
            elif self.value_type == ipaddress.IPv4Address:
                msg = f"Expected format like x.x.x.x (numbers 0-255)."
            elif self.value_type == ipaddress.IPv4Network:
                msg = f"Expected format like x.x.x.x/x. This is not a valid subnet mask or CIDR notation."
            return False, f"Expected prop {self.name}.{self.attr}={value} to have type '{self.value_type}' {msg}"

        return True, ""
            
    def cast_value(self, v: Optional[str]) -> Optional[Any]:
        """ cast prop value to expected prop value type """
        if v is None or self.value_type == str:
            return v
        try:
            if self.value_type == int:
                return int(v)
            if self.value_type == ipaddress.IPv4Address:
                return ipaddress.ip_address(v)
            if self.value_type == ipaddress.IPv4Network:
                return ipaddress.ip_network(v)
            if self.value_type == ASN:
                return asn(v)
            if self.value_type == MAC:
                return mac(v)
            if self.value_type == bool:
                return parse_bool(v)
            return None
        except:
            return None

    def __str__(self):
        return f"{self.name}.{self.attr}"

    def __eq__(self, other):
        if not isinstance(other, DeviceProp):
            return False
        return other.name == self.name and other.attr == self.attr

    def __hash__(self):
        return hash(str(self))


# Generic Props
prop_bios_vendor = DeviceProp("bios", "vendor")
prop_bios_version = DeviceProp("bios", "version")
prop_deploy_tag = DeviceProp("deploy", "tag", default_value="",
                             comment="Arbitrary tag for grouping devices for a particular deployment")
prop_location_rack = DeviceProp("location", "rack", default_value="1", title="Rack")
prop_location_unit = DeviceProp("location", "unit", default_value="1", title="Unit", comment="ordinal of device type/role within rack")
prop_location_position = DeviceProp("location", "position", comment="top-most slot occupied by the device. Slot relative to top of rack")
prop_location_size = DeviceProp("location", "size", comment="rack units occupied")
prop_location_node_group = DeviceProp("location", "node_group", title="Node group", comment="logical grouping of node with other nodes")
prop_location_stamp = DeviceProp("location", "stamp", default_value="", title="Stamp")
prop_management_credentials_user = DeviceProp("management_credentials", "user", title="Root username")
prop_management_credentials_password = DeviceProp("management_credentials", "password", title="Root password", sensitive=True)
prop_management_info_name = DeviceProp("management_info", "name", comment="Default hostname requested over DNS")
prop_management_info_ip = DeviceProp("management_info", "ip", value_type=ipaddress.IPv4Address, title="MGMT port IP")
prop_management_info_gateway = DeviceProp("management_info", "gateway", value_type=ipaddress.IPv4Address)
prop_management_info_mac = DeviceProp("management_info", "mac", title="MGMT port MAC", value_type=MAC)
prop_network_config_roce = DeviceProp("network_config", "roce", "disabled")
prop_network_config_dhcp_option_43 = DeviceProp("network_config", "dhcp_option_43", comment="DHCP option 43. Vendor-specific")
prop_network_config_dhcp_option_66 = DeviceProp("network_config", "dhcp_option_66", comment="DHCP option 66. TFTP server name")
prop_network_config_dhcp_option_67 = DeviceProp("network_config", "dhcp_option_67", comment="DHCP option 67. Boot file")
prop_network_config_dhcp_option_240 = DeviceProp("network_config", "dhcp_option_240", comment="DHCP option 240. ztd-provision-url")
prop_network_config_dhcp_boot_file = DeviceProp("network_config", "dhcp_boot_file", comment="Path to BOOTP boot file. Prefer option 67 instead")
prop_vendor_name = DeviceProp("vendor", "name", title="Vendor", comment="vendor shorthand (AR,CR,CS,DL,EC,HP,JU,SM)")
prop_vendor_model = DeviceProp("vendor", "model", title="Model", comment="device model description")
prop_vendor_serial = DeviceProp("vendor", "serial", title="Serial Number", comment="device serial number")
prop_console_ip = DeviceProp("console", "ip", comment="Console server IP")
prop_console_username = DeviceProp("console", "username", comment="Console server username, like 'root:port16'")
prop_console_password = DeviceProp("console", "password", title="Console server password", comment="Console server password", sensitive=True)

# Server-specific props
prop_ipmi_credentials_user = DeviceProp("ipmi_credentials", "user", title="IPMI username")
prop_ipmi_credentials_password = DeviceProp("ipmi_credentials", "password", title="IPMI password", sensitive=True)
prop_ipmi_info_name = DeviceProp("ipmi_info", "name", title="IPMI default DNS", comment="default hostname requested over DNS")
prop_ipmi_info_ip = DeviceProp("ipmi_info", "ip", title="IPMI IP", value_type=ipaddress.IPv4Address)
prop_ipmi_info_mac = DeviceProp("ipmi_info", "mac", title="IPMI MAC", value_type=MAC)


# deprecated - use ClusterDevice.controlplane instead
prop_kubernetes_controlplane = DeviceProp("kubernetes", "controlplane", title="K8S controlplane nodes",
                                    comment="Indicates that a node is a k8s controlpane node",
                                    value_type=bool, default_value=False)
prop_kubernetes_ignore = DeviceProp("kubernetes", "ignore", title="Ignore for k8s flag",
                                    comment="Ignore device when generating network_config for k8s if set",
                                    value_type=bool, default_value=False)

# Switch-specific props
prop_switch_info_firmware_version = DeviceProp("switch_info", "firmware_version")
prop_switch_info_model = DeviceProp("switch_info", "model")
prop_switch_info_loopback_ip = DeviceProp("switch_info", "loopback_ip",
                                          value_type=ipaddress.IPv4Address,
                                          comment="Switch loopback IP, allocated from deploy_mgmt subnet")
prop_switch_info_deploy_subnet = DeviceProp("switch_info", "deploy_subnet",
                                            value_type=ipaddress.IPv4Network,
                                            comment="Deploy network (network_extents.name==deploy_mgmt) this switch "
                                                    "belongs to. Used for IP allocation")
prop_switch_info_mac_verified = DeviceProp("switch_info", "mac_verified",
                                           default_value=False, value_type=bool,
                                           comment="management_info.mac has been verified by from DNSMASQ lease file. "
                                                   "(vendor ingestion may report an off-by-one MAC address)")
prop_switch_info_connected_racks = DeviceProp("switch_info", "connected_racks",
                                              default_value="", value_type=str,
                                              comment="comma-separated list of location.rack values which this switch "
                                                      "connects to, in addition to its own rack. Used in cases where a "
                                                      "mgmt switches connect to multiple racks"
                                              )

# Mg Switch network config
prop_subnet_info_subnet = DeviceProp("subnet_info", "subnet",
                                     value_type=ipaddress.IPv4Network,
                                     comment="IP network, e.g. 10.0.1.0/24")
prop_subnet_info_prefixlen = DeviceProp("subnet_info", "prefixlen",
                                        value_type=int,
                                        default_value=None,
                                        comment="Prefix len for IP allocator to assign. Ignored if subnet already assigned")
prop_subnet_info_gateway = DeviceProp("subnet_info", "gateway",
                                      value_type=ipaddress.IPv4Address,
                                      comment="MG switch gateway IP within subnet_info.subnet")
prop_subnet_info_asn = DeviceProp("subnet_info", "asn", value_type=ASN)
prop_subnet_info_lease_time = DeviceProp("subnet_info", "lease_time", comment="override cluster default lease time")


def _asdict(*props_args, **props_kwargs) -> Dict[str, DeviceProp]:
    d = {}
    for p in props_args:
        if isinstance(p, dict):
            d.update(p)
        elif isinstance(p, DeviceProp):
            d[str(p)] = p
    for k, v in props_kwargs.items():
        d[k] = v
    return d


# TODO: add property for bios_config and package version
#       Update _assert_property_attribute when bios_config and/or package
#       version are added

GENERIC_DEVICE_PROPS = _asdict(
    prop_bios_vendor,
    prop_bios_version,
    prop_deploy_tag,
    prop_location_rack,
    prop_location_unit,
    prop_location_position,
    prop_location_size,
    prop_location_node_group,
    prop_location_stamp,
    prop_management_credentials_user,
    prop_management_credentials_password,
    prop_management_info_ip,
    prop_management_info_mac,
    prop_network_config_dhcp_option_43,
    prop_network_config_dhcp_option_66,
    prop_network_config_dhcp_option_67,
    prop_network_config_dhcp_option_240,
    prop_network_config_dhcp_boot_file,
    prop_network_config_roce,
    prop_vendor_name,
    prop_vendor_model,
    prop_vendor_serial,
)
SERVER_DEVICE_PROPS = _asdict(
    GENERIC_DEVICE_PROPS,
    prop_ipmi_credentials_user,
    prop_ipmi_credentials_password,
    prop_ipmi_info_name,
    prop_ipmi_info_ip,
    prop_ipmi_info_mac,
    prop_kubernetes_controlplane,
    prop_kubernetes_ignore,
)

SWITCH_DEVICE_PROPS = _asdict(
    GENERIC_DEVICE_PROPS,
    prop_console_ip,
    prop_console_password,
    prop_console_username,
    prop_kubernetes_ignore,
    prop_subnet_info_asn,
    prop_subnet_info_gateway,
    prop_subnet_info_lease_time,
    prop_subnet_info_prefixlen,
    prop_subnet_info_subnet,
    prop_switch_info_connected_racks,
    prop_switch_info_deploy_subnet,
    prop_switch_info_firmware_version,
    prop_switch_info_loopback_ip,
    prop_switch_info_mac_verified,
    prop_switch_info_model,
)

SYSTEM_DEVICE_PROPS = _asdict(
    GENERIC_DEVICE_PROPS,
    prop_management_info_name,
    prop_console_ip,
    prop_console_username,
    prop_console_password,
    prop_kubernetes_ignore,
)

CONSOLE_DEVICE_PROPS = _asdict(
    GENERIC_DEVICE_PROPS,
)

PERIPHERAL_DEVICE_PROPS = _asdict(
    GENERIC_DEVICE_PROPS,
)

ALL_DEVICE_PROPS: Dict[str, DeviceProp] = _asdict(
    GENERIC_DEVICE_PROPS,
    SERVER_DEVICE_PROPS,
    SWITCH_DEVICE_PROPS,
    SYSTEM_DEVICE_PROPS,
)

# Props specific to the inventory/vendor file
INVENTORY_DEVICE_PROPS = _asdict(
    prop_ipmi_credentials_user,
    prop_ipmi_credentials_password,
    prop_ipmi_info_name,
    prop_ipmi_info_ip,
    prop_ipmi_info_mac,
    prop_location_rack,
    prop_location_unit,
    prop_location_stamp,
    prop_location_node_group,
    prop_management_credentials_user,
    prop_management_credentials_password,
    prop_management_info_ip,
    prop_management_info_mac,
    prop_vendor_name,
)


ENFORCE_UNIQUE_PROPS = {
    "ip": [prop_management_info_ip,
           prop_ipmi_info_ip,
           prop_switch_info_loopback_ip,
           prop_subnet_info_gateway,
           prop_management_info_gateway],
    "mac": [prop_management_info_mac,
            prop_ipmi_info_mac],
    "subnet": [prop_subnet_info_subnet, ],
    "name": [prop_management_info_name],
}

MGMT_INFO_NAME = _asdict(
    prop_management_info_name
)

AttrDict = Dict[str, Optional[str]]
PropAttrDict = Dict[str, AttrDict]
PropDict = Dict[DeviceProp, Optional[typing.Any]]


def _to_propattr_dict(props: Dict[str, DeviceProp]) -> PropAttrDict:
    rv = {}
    for prop in props.values():
        attr_dict = rv.get(prop.name, {})
        attr_dict[prop.attr] = prop.default_value
        rv[prop.name] = attr_dict
    return rv


GENERIC_DEVICE_PROP_DICT = _to_propattr_dict(GENERIC_DEVICE_PROPS)
SERVER_DEVICE_PROP_DICT = _to_propattr_dict(SERVER_DEVICE_PROPS)
SWITCH_DEVICE_PROP_DICT = _to_propattr_dict(SWITCH_DEVICE_PROPS)
SYSTEM_DEVICE_PROP_DICT = _to_propattr_dict(SYSTEM_DEVICE_PROPS)
CONSOLE_DEVICE_PROP_DICT = _to_propattr_dict(CONSOLE_DEVICE_PROPS)
PERIPHERAL_DEVICE_PROP_DICT = _to_propattr_dict(PERIPHERAL_DEVICE_PROPS)
ALL_DEVICE_PROP_DICT = _to_propattr_dict(ALL_DEVICE_PROPS)
INVENTORY_DEVICE_PROPS_DICT = _to_propattr_dict(INVENTORY_DEVICE_PROPS)


def expand_prop_dict(d: PropDict) -> PropAttrDict:
    rv = {}
    for k, v in d.items():
        attrs = rv.get(k.name, {})
        attrs[k.attr] = str(v) if v is not None else v
        rv[k.name] = attrs
    return rv


def get_props_for_device_type(device_type: str) -> typing.Dict[str, DeviceProp]:
    if device_type == "SR":
        return SERVER_DEVICE_PROPS
    elif device_type == "SW":
        return SWITCH_DEVICE_PROPS
    elif device_type == "SY":
        return SYSTEM_DEVICE_PROPS
    elif device_type == "CN":
        return CONSOLE_DEVICE_PROPS
    elif device_type == "PR":
        return PERIPHERAL_DEVICE_PROPS
    return GENERIC_DEVICE_PROPS


def get_propattr_dict_for_device_type(device_type: str) -> PropAttrDict:
    if device_type == "SR":
        return SERVER_DEVICE_PROP_DICT
    elif device_type == "SW":
        return SWITCH_DEVICE_PROP_DICT
    elif device_type == "SY":
        return SYSTEM_DEVICE_PROP_DICT
    elif device_type == "CN":
        return CONSOLE_DEVICE_PROP_DICT
    elif device_type == "PR":
        return PERIPHERAL_DEVICE_PROP_DICT
    return GENERIC_DEVICE_PROP_DICT


def device_type_has_property(device_type: str, property: str) -> bool:
    return property in get_propattr_dict_for_device_type(device_type)


def get_property_attrs(prop_name: str) -> AttrDict:
    return ALL_DEVICE_PROP_DICT.get(prop_name, {})


def must_get_prop_attr_default_value(name: str, attribute: str) -> Optional[str]:
    """
    Check property name and attributes is well-known. Asserts if no match.
    Returns default value otherwise
    """
    # bios_config property is an unknown at this time, so will make an exception for it here
    if name == "bios_config":
        return None
    # For backward compatibility with package version, skipping all version attributes
    if attribute == "version":
        return None

    assert name in ALL_DEVICE_PROP_DICT, \
        f"Device property '{name}' is not a well-known property name"
    assert attribute in ALL_DEVICE_PROP_DICT[name], \
        f"Device property.attribute {name}.{attribute} is not a well-known property.attribute"

    return ALL_DEVICE_PROP_DICT[name][attribute]
