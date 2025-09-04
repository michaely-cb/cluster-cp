import ipaddress
import logging
import pathlib
from collections import defaultdict
from typing import Optional, Tuple, Any, Dict, List

from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles, DeploymentDeviceVendors
from deployment_manager.db.models import Device, get_rack_to_mg_switch
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.switch.templates import (DataSwitchZTPTemplate, MgmtSwitchZTPTemplate,
                                                       MgmtSwitchStartupConfigTemplate, DataSwitchStartupConfigTemplate)

logger = logging.getLogger(__name__)


HTTP_DIR = pathlib.Path("/var/www/html/")
TFTP_DIR = pathlib.Path("/var/ftpd/")
SWITCH_MGMT_VLAN = 4000
# originally LOCAL/NODE ASN were the same: 4294967295 but Juniper doesn't support 4294967295 or peers with same AS as local AS
MGMT_NODE_PEER_GROUP_DEFAULT_LOCAL_ASN = 4294967294 # AS of switch
MGMT_NODE_PEER_GROUP_DEFAULT_NODE_ASN = 4294967293  # AS of nodes / kube-vip
# default switch password
SWITCH_SHA = "$6$Bui4D71CsKJbUhqY$EfcE4D1UGAz3rlCGjbLPt8vFslHxq3thpzdjoK4O/Qcx3XGf0a5/1ijVjrkFKMHbN.IyaUOwChjg8pYNAUl3O."
SWITCH_MD5 = "ffd193192720781b87bfe169f240a750"


def _get_switch_prop(switch_props, prop):
    val = switch_props.get(prop.name, {}).get(prop.attr)
    if val is not None:
        return prop.cast_value(val)
    return None


def _require_switch_prop(switch_props, prop):
    val = _get_switch_prop(switch_props, prop)
    if val is None:
        raise ValueError(f"property {prop} was not set")
    return val


def _require_cfg_prop(cfg: dict, *path) -> Any:
    d = cfg
    for ele in path:
        if ele not in d:
            raise ValueError(f"required property input.yml::{'.'.join(path)} was not set")
        d = d[ele]
    return d


def _get_deploy_mgmt_subnet_gw(cfg: dict, deploy_subnet: Optional[str]) -> Tuple[ipaddress.IPv4Network, ipaddress.IPv4Address]:
    extents = _require_cfg_prop(cfg, "mgmt_network_int_config", "ip_allocation", "network_extents")
    subnet, my_subnet = None, None
    if deploy_subnet:
        my_subnet = ipaddress.IPv4Network(deploy_subnet)
    for e in extents:
        if e["name"] == "deploy_mgmt":
            subnets = [ipaddress.IPv4Network(s) for s in e["subnets"]]
            if my_subnet and my_subnet not in subnets:
                raise ValueError(f"switch was assigned a switch_info.deploy_subnet property={my_subnet} but "
                                 "that subnet did not exist in ip_allocation.network_extents['deploy_mgmt']")
            if not my_subnet:
                my_subnet = subnets[0]
            break
    if not my_subnet:
        raise ValueError("no network extent 'deploy_mgmt' configured - cannot determine deploy subnet")

    return my_subnet, my_subnet.broadcast_address - 1


def get_mgmt_fw_image(vendor: str) -> Optional[pathlib.Path]:
    if vendor == DeploymentDeviceVendors.ARISTA.value:
        return pathlib.Path("/var/www/html/EOS64-4.32.0F.swi")
    elif vendor == DeploymentDeviceVendors.DELL.value:
        return pathlib.Path("/var/www/html/OS10.bin")
    elif vendor == DeploymentDeviceVendors.JUNIPER.value:
        return pathlib.Path("/var/www/html/junos-install-ex-arm-64.tgz")
    else:
        return None


def get_data_fw_image(vendor: str) -> Optional[pathlib.Path]:
    if vendor == DeploymentDeviceVendors.ARISTA.value:
        return pathlib.Path("/var/www/html/EOS64-4.33.1F.swi")
    elif vendor == DeploymentDeviceVendors.JUNIPER.value:
        return pathlib.Path("/var/ftpd/junos-evo-install-qfx-ms-x86-64-23.4R2-S3.11-EVO.iso")
    else:
        return None


def generate_mgmt_switch_ztp_config(switch_name, switch_props, cfg, root_server_ip) -> Dict[str, str]:
    """ Returns file type (ztp, startup-config) to file contents """
    asn_agg_0 = _require_cfg_prop(cfg, "mgmt_network_int_config", "asn_agg_0")
    asn_agg_1 = cfg["mgmt_network_int_config"].get("asn_agg_1")
    if asn_agg_1 is None:
        logger.info(
            "input.yml property 'mgmt_network_int_config.asn_agg_1' not set. "
            "Using the ASN for first aggregate switch"
        )
        asn_agg_1 = asn_agg_0

    try:
        mgmt_group_local_asn = _require_cfg_prop(cfg, "mgmt_network_int_config", "mgmt_vip", "router_asn")
    except ValueError:
        mgmt_group_local_asn = MGMT_NODE_PEER_GROUP_DEFAULT_LOCAL_ASN

    try:
        mgmt_group_node_asn = _require_cfg_prop(cfg, "mgmt_network_int_config", "mgmt_vip", "node_asn")
    except ValueError:
        mgmt_group_node_asn = MGMT_NODE_PEER_GROUP_DEFAULT_NODE_ASN

    switch_mgmt_sn, switch_mgmt_gw = _get_deploy_mgmt_subnet_gw(
        cfg, switch_props.get("switch_info", {}).get("deploy_subnet", None)
    )

    vendor = _require_switch_prop(switch_props, props.prop_vendor_name)
    switch_addr = _require_switch_prop(switch_props, props.prop_subnet_info_gateway)
    switch_subnet: ipaddress.IPv4Network = _require_switch_prop(switch_props, props.prop_subnet_info_subnet)
    switch_cidr = f"{switch_addr}/{switch_subnet.prefixlen}"

    mgmt_addr = _require_switch_prop(switch_props, props.prop_management_info_ip)
    assert ipaddress.IPv4Address(mgmt_addr) in switch_mgmt_sn, "switch mgmt_addr must be in switch subnet"
    mgmt_cidr = f"{mgmt_addr}/{switch_mgmt_sn.prefixlen}"

    template_args = dict(
        tftp_server_ip=root_server_ip,
        http_server_ip=root_server_ip,
        switch_name=switch_name,
        switch_sha=SWITCH_SHA,
        switch_md5=SWITCH_MD5,
        mgmt_vlan=SWITCH_MGMT_VLAN,
        switch_cidr=switch_cidr,
        ip_helper_address=root_server_ip,
        loopback_ip=str(_require_switch_prop(switch_props, props.prop_switch_info_loopback_ip)),
        switch_mgmt_cidr=mgmt_cidr,
        switch_mgmt_gw=switch_mgmt_gw,
        switch_asn=_require_switch_prop(switch_props, props.prop_subnet_info_asn),
        mgmt_subnet=_require_cfg_prop(cfg, "mgmt_network_int_config", "ip_allocation", "parentnet"),
        asn_agg_0=asn_agg_0,
        asn_agg_1=asn_agg_1,
        local_asn=mgmt_group_local_asn,
        mgmt_node_asn=mgmt_group_node_asn,
    )
    fw_image = get_mgmt_fw_image(vendor)
    if fw_image is not None:
        assert fw_image.exists(), f"Firmware image {fw_image.name} for {switch_name} not found"
        template_args['fw_image'] = fw_image.name

    return dict(
        ztp=MgmtSwitchZTPTemplate(vendor).substitute(**template_args),
        startup_config=MgmtSwitchStartupConfigTemplate(vendor).substitute(**template_args),
    )


def generate_data_switch_ztp_config(
        switch_name: str, switch_props: dict, mg_switch_props: dict, root_server_ip: str
) -> Dict[str, str]:
    """ Returns file type (ztp, startup-config) to file contents """
    vendor = _require_switch_prop(switch_props, props.prop_vendor_name)
    model = _get_switch_prop(switch_props, props.prop_vendor_model)
    switch_gw = _require_switch_prop(mg_switch_props, props.prop_subnet_info_gateway)
    switch_sn = _require_switch_prop(mg_switch_props, props.prop_subnet_info_subnet)

    switch_mgmt_ip = _require_switch_prop(switch_props, props.prop_management_info_ip)
    switch_mgmt_cidr = f"{switch_mgmt_ip}/{switch_sn.prefixlen}"

    template_args = dict(
        tftp_server_ip=root_server_ip,
        http_server_ip=root_server_ip,
        switch_name=switch_name,
        switch_sha=SWITCH_SHA,
        switch_md5=SWITCH_MD5,
        switch_mgmt_cidr=switch_mgmt_cidr,
        switch_mgmt_gw=switch_gw,
    )
    fw_image = get_data_fw_image(vendor)
    if fw_image is not None:
        assert fw_image.exists(), f"Firmware image {fw_image.name} for {switch_name} not found"
        template_args['fw_image'] = fw_image.name

    return dict(
        ztp=DataSwitchZTPTemplate(vendor, model=model).substitute(**template_args),
        startup_config=DataSwitchStartupConfigTemplate(vendor).substitute(**template_args),
    )


def _get_output_dir(device: Device) -> pathlib.Path:
    """ different vendors / models will either use TFTP or HTTP """
    vendor = device.get_prop(props.prop_vendor_name)
    is_mgmt = device.device_role == "MG"
    if vendor == "DL":
        return HTTP_DIR
    if vendor == "JU" and is_mgmt:
        # juniper 100G switches didn't obey the HTTP-PORT suboption in option 43 but 1G mgmt switches did
        return HTTP_DIR
    return TFTP_DIR


def setup_ztp(profile: str, switches: List[Device], output_dir: Optional[str]=None) -> Dict[str, str]:
    cg = ConfGen(profile)
    root_server = Device.get_device(cg.get_root_server(), profile)
    profile_cfg = cg.parse_profile()

    root_server_ip = cg.get_root_server_ip()
    if root_server_ip is None:
        msg = "Root server IP not found"
        logger.error(msg)
        return {s.name: msg for s in switches}

    # if root server is on infrastructure node, then will look for ztp ip
    if root_server.device_role == DeploymentDeviceRoles.INFRA.value:
        mgmt_ztp_ip = profile_cfg["basic"].get("switch_ztp_ip", None)
        if mgmt_ztp_ip is None:
            msg = (
                "Root server is an infra node and requires config 'basic.switch_ztp_ip' to be set. "
                "Please update this config in input.yaml with the IP which switches will use to contact this server"
            )
            logger.error(msg)
            return {s.name: msg for s in switches}
    else:
        logger.info(f"Using root server ip ({root_server_ip}) as switch ZTP IP")
        mgmt_ztp_ip = root_server_ip

    dry_run = False
    override_dir = None
    if output_dir:
        override_dir = pathlib.Path(output_dir)
        override_dir.mkdir(parents=True, exist_ok=True)
        dry_run = True
    else:
        HTTP_DIR.mkdir(parents=True, exist_ok=True)
        TFTP_DIR.mkdir(parents=True, exist_ok=True)

    rack_mg_sw = get_rack_to_mg_switch(profile)

    def clear_dhcp_options(device):
        device.set_prop(props.prop_network_config_dhcp_boot_file, None)
        device.set_prop(props.prop_network_config_dhcp_option_43, None)
        device.set_prop(props.prop_network_config_dhcp_option_66, None)
        device.set_prop(props.prop_network_config_dhcp_option_67, None)
        device.set_prop(props.prop_network_config_dhcp_option_240, None)

    failures = 0
    retval = defaultdict(str)
    for s in switches:
        is_mgmt = s.device_role == DeploymentDeviceRoles.MANAGEMENT.value
        try:
            if is_mgmt:
                output = generate_mgmt_switch_ztp_config(s.name, s.get_prop_attr_dict(), profile_cfg, mgmt_ztp_ip)
            else:
                # get the data switch's mgmt switch
                rack = s.get_prop(props.prop_location_rack)
                mg_switch = rack_mg_sw.get(rack)
                if not mg_switch:
                    failures += 1
                    retval[s.name] = (
                        f"Failed to generate configuration for {s.name}: rack {rack} has no mg switches. "
                        f"Please change the properties.location.rack of {s.name} to the rack where this switch "
                        "is cabled."
                    )
                    continue
                mg_switch_props = mg_switch.get_prop_attr_dict()
                output = generate_data_switch_ztp_config(
                    s.name, s.get_prop_attr_dict(), mg_switch_props, root_server_ip
                )
        except ValueError as e:
            retval[s.name] = f"Failed to generate configuration for {s.name}: {e}"
            clear_dhcp_options(s)
            failures += 1
        else:
            for file_kind, contents in output.items():
                # Dell switches need their ZTP config to be served over HTTP
                od = override_dir if override_dir else _get_output_dir(s)
                outfile = od / f"{s.name}.{file_kind}"
                outfile.write_text(contents)
                if file_kind == "ztp":
                    if not dry_run:
                        if s.get_prop(props.prop_vendor_name) == "DL":
                            s.set_prop(
                                props.prop_network_config_dhcp_option_240,
                                f'http://{root_server_ip}:8080/{outfile.name}'
                            )
                        else:
                            s.set_prop(props.prop_network_config_dhcp_option_67, outfile.name)
                    else:
                        clear_dhcp_options(s)
                else:
                    if not dry_run:
                        if s.get_prop(props.prop_vendor_name) == "JU":
                            if is_mgmt:
                                image_file = get_mgmt_fw_image(DeploymentDeviceVendors.JUNIPER.value).name
                            else:
                                image_file = get_data_fw_image(DeploymentDeviceVendors.JUNIPER.value).name
                            if od == HTTP_DIR:
                                s.set_prop(
                                    props.prop_network_config_dhcp_option_43,
                                    f'{image_file},{outfile.name},,http,,8080'
                                )
                            else:
                                s.set_prop(
                                    props.prop_network_config_dhcp_option_43,
                                    f'{image_file},{outfile.name},,,,'
                                )
                            s.set_prop(
                                props.prop_network_config_dhcp_option_66,
                                root_server_ip
                            )
                            s.set_prop(props.prop_network_config_dhcp_option_67, outfile.name)

    return retval
