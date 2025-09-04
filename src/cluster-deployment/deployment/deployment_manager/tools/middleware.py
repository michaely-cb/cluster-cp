from functools import cached_property
from typing import Optional, List

from deployment_manager.common import models
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceTypes
from deployment_manager.db.models import Device, Cluster
from deployment_manager.network_config.common.context import ObjCred, AppCtx
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.switch.interface import SwitchOSFamily
from deployment_manager.tools.switch.switchctl import SwitchInfo


def must_lookup_switch_info(profile: str, name: str) -> SwitchInfo:
    d = Device.get_device(name, profile)
    if d is None:
        raise ValueError(f"switch {name} was not found")

    vendor = d.get_prop(props.prop_vendor_name)

    # TODO: CR 'credo' media converter devices should be converted to a different device type like "PR" peripherals
    # originally, HPE switches were used as media converters but Credo became the standard and credo is a L1 device,
    # not a switch
    if d.device_type != DeploymentDeviceTypes.SWITCH.value:
        raise ValueError(f"device {name} was a {d.device_type}/{d.device_role} "
                         "but this command requires type=SW")

    addr = str(d.get_prop(props.prop_management_info_ip))
    user = d.get_prop(props.prop_management_credentials_user, include_default=False)
    pw = d.get_prop(props.prop_management_credentials_password, include_default=False)

    switch_os = SwitchOSFamily.NOT_SET
    if vendor == "DL":
        switch_os = SwitchOSFamily.DELL_OS10 if d.device_role == "MG" else SwitchOSFamily.DELL_SONIC
    
    if vendor == "HP":
        if d.device_role == "SX":
            switch_os = SwitchOSFamily.HPE_COMWARE
        else:
            switch_os = SwitchOSFamily.HPE

    if not addr or not user or not pw or not vendor:
        raise ValueError(f"{d.name} missing one of {props.prop_vendor_name}, {props.prop_management_info_ip}, "
                         f"{props.prop_management_credentials_user}, "
                         f"{props.prop_management_credentials_password}")
    return SwitchInfo(name, addr, vendor, user, pw, os_family=switch_os)


def lookup_switch_info(profile: str, name: str) -> Optional[SwitchInfo]:
    try:
        return must_lookup_switch_info(profile, name)
    except ValueError:
        return None


def get_obj_cred(profile: str, name: str) -> Optional[ObjCred]:
    d = Device.get_device(profile_name=profile, name=name)
    if not d:
        return None
    addr = d.get_prop(props.prop_management_info_ip)
    user = d.get_prop(props.prop_management_credentials_user)
    pw = d.get_prop(props.prop_management_credentials_password)
    return ObjCred(host=str(addr) or name, username=user, password=pw)


def must_get_obj_cred(profile: str, name: str) -> ObjCred:
    creds = get_obj_cred(profile, name)
    if not creds:
        raise ValueError(f"invalid state: no credentials found for {name}")
    return creds


class AppCtxImpl(AppCtx):
    """ Larger application context to pass down into network_config packages."""

    def __init__(self, profile: str):
        self._profile = profile

    @cached_property
    def _cluster_config(self):
        return ConfGen(self._profile).load_config()

    def get_mgmt_network_config(self) -> models.MgmtNetworkConfig:
        return self._cluster_config.mgmt_network_int_config

    def get_clusters(self) -> List[models.Cluster]:
        rv = []
        for db_obj in Cluster.objects.filter(profile__name=self._profile):
            rv.append(models.Cluster(name=db_obj.name, mgmt_vip=db_obj.management_vip))
        return rv

    def get_obj_creds(self, name: str) -> Optional[ObjCred]:
        return get_obj_cred(self._profile, name)
