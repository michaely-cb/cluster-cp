import datetime
import logging
import typing
import yaml
from typing import Optional, List

from deployment_manager.conftool.schema.cluster_config import ClusterConfigData
from deployment_manager.conftool.schema.cluster_types import DEVICE_TYPES_SHORT
from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device

from deployment_manager.common import models

logger = logging.getLogger(__name__)


def write_master_config(config_input_fp: str,
                        devices: List[Device],
                        master_output_fp: str = f"master-config.yml",
                        meta: Optional[dict] = None):
    with open(config_input_fp) as fp:
        doc = yaml.safe_load(fp)

    config = models.ClusterDeploymentConfig.model_validate(doc)
    master_config = ClusterConfigData().update_from_clusterconfig(config)

    load_vendor_info(master_config, devices)

    # Add meta data
    master_config.add_meta("created", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if meta:
        for k, v in meta.items():
            master_config.add_meta(k, v)

    master_config.save_yaml(master_output_fp)


def load_vendor_info(cluster_config: ClusterConfigData, devices: typing.List[Device]):
    """ Update the config from the database models """
    for device in devices:
        if device.device_type not in DEVICE_TYPES_SHORT:
            logger.warning(f"skip processing unhandled device type {device.device_type}")
            continue

        if device.device_type == "SR":
            _load_server(cluster_config, device)
        elif device.device_type == "SW":
            _load_switch(cluster_config, device)
        elif device.device_type == "SY":
            _load_system(cluster_config, device)
        else:
            raise AssertionError("programming error")


def _load_server(cluster_config, device, validate: bool = False):
    addr = str(device.get_prop(props.prop_management_info_ip) or "")
    server_obj = {
        "rackUnit": {
            "rackNum": device.get_prop(props.prop_location_rack),
            "unitNum": int(device.get_prop(props.prop_location_unit) or 0),
        },
        "role": device.device_role,
        "stamp": device.get_prop(props.prop_location_stamp) or "default",
        "nodeGroup": int(device.get_prop(props.prop_location_node_group) or -1),
        "name": device.name,
        "address": addr,
        "vendor": device.get_prop(props.prop_vendor_name),
        "rootLogin": {
            "username": device.get_prop(props.prop_management_credentials_user),
            "password": device.get_prop(props.prop_management_credentials_password),
        }
    }

    cluster_config.add_or_update_device("server", server_obj)

    # Check for root server ip
    cluster_config.check_update_root_server_info(server_name=device.name, server_ip=addr)


def _load_system(cluster_config, device, validate: bool = False):
    system_vlan = device.name in cluster_config.data()["network"]["clusterNetwork"].get("systemsWithVlan", [])


    system_obj = {
        "rackUnit": {
            "rackNum": device.get_prop(props.prop_location_rack),
            "unitNum": int(device.get_prop(props.prop_location_unit) or 0),
        },
        "role": device.device_role,
        "stamp": device.get_prop(props.prop_location_stamp) or "default",
        "nodeGroup": int(device.get_prop(props.prop_location_node_group) or -1),
        "name": device.name,
        "login": {
            "username": device.get_prop(props.prop_management_credentials_user),
            "password": device.get_prop(props.prop_management_credentials_password),
        },
        "ports": [
            {
                "name": "MGMT",
                "physicalName": "MGMT",
                "networkType": "L2",
                "dnsName": device.name,
                "address": str(device.get_prop(props.prop_management_info_ip) or ""),
                "mac": device.get_prop(props.prop_management_info_mac) or "null",
            }
        ],
        "systemVlan": system_vlan,
    }

    # Add default hostname if available
    default_hostname = device.get_prop(props.prop_management_info_name)
    if default_hostname:
        system_obj["defaultHostname"] = default_hostname

    cluster_config.add_or_update_device("system", system_obj, validate=validate)


def _load_switch(cluster_config, device, validate: bool = False):
    if device.device_role not in ("MX", "SX", "LF", "SP"):  # skip non-100G+ switches
        return

    switch_obj = {
        "rackUnit": {
            "rackNum": device.get_prop(props.prop_location_rack),
            "unitNum": int(device.get_prop(props.prop_location_unit) or 0),
        },
        "ip": str(device.get_prop(props.prop_management_info_ip) or ""),
        "role": device.device_role,
        "stamp": device.get_prop(props.prop_location_stamp) or "default",
        "name": device.name,
        "vendor": device.get_prop(props.prop_vendor_name),
        "login": {
            "username": device.get_prop(props.prop_management_credentials_user),
            "password": device.get_prop(props.prop_management_credentials_password),
        },
    }

    cluster_config.add_or_update_device("switch", switch_obj, validate=validate)
