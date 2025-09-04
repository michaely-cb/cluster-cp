from typing import Dict

from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceTypes, DeploymentDeviceRoles
from deployment_manager.db.models import Device
from deployment_manager.tools.switch.interface import SwitchInfo


def device_to_switchinfo(d: Device) -> SwitchInfo:      
    if d.device_type != DeploymentDeviceTypes.SWITCH.value:
        raise ValueError(f"this command requires type=SW")

    vendor = d.get_prop(props.prop_vendor_name)
    addr = str(d.get_prop(props.prop_management_info_ip))
    user = d.get_prop(props.prop_management_credentials_user, include_default=False)
    pw = d.get_prop(props.prop_management_credentials_password, include_default=False)
    if not addr or not user or not pw or not vendor:
        raise ValueError(f"{d.name} missing one of {props.prop_vendor_name}, {props.prop_management_info_ip}, "
                         f"{props.prop_management_credentials_user}, "
                         f"{props.prop_management_credentials_password}")
    return SwitchInfo(d.name, addr, vendor, user, pw)
