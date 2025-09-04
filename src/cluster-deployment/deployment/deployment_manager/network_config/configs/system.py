#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Configuration builders
"""

import json

from .base import (
    _ConfigFileWriterBase,
    _ConfigAggregateBase,
    MTU_DEFAULT,
    MTU_MIN,
    MTU_MAX_WITH_OVERHEAD
)
from ..schema import JSIPInterface
from ..utils import find_item_named


class SystemNetworkConfigWriter(_ConfigFileWriterBase):
    """ Write the system network configuration
    """
    config_class_name = "systems"
    config_file_name = "/var/lib/tftpboot/network-cfg.json"
    config_file_mode = 0o644

    IFACE_TYPES = ("control", "data", )

    @property
    def config_item_name(self):
        return self.name

    def __init__(self, network_config, system_name, force_dcqcn=None):
        self.name = system_name
        is_leaf_spine = network_config.get("environment", {}).get("topology") == "leaf_spine"
        enable_dcqcn = network_config.get("environment", {}).get("systemEnableDcqcn", False)
        dcqcn_obj = {"enable_dcqcn_sender": 1, "enable_dcqcn_receiver": 1}
        if force_dcqcn is not None:
            dcqcn = {} if not force_dcqcn else dcqcn_obj
        else:
            dcqcn = {} if not enable_dcqcn else dcqcn_obj

        self.network_cfg = {
            "cm_daemon": {
                "port": 9000,
                **dcqcn,
            },
            "control": {
                "default": {
                },
                "virtual": {
                }
            },
            "data": {
                "default": {
                }
            }
        }

        system_obj = find_item_named(network_config["systems"], system_name)
        system_vlan = True if (system_obj.get("system_vlan") or is_leaf_spine) else False

        self.network_cfg["control"]["virtual"]["inet"] = \
            str(JSIPInterface(system_obj["vip"]))

        for iface_obj in system_obj["interfaces"]:
            switch_obj = find_item_named(network_config["switches"],
                                         iface_obj["switch_name"])

            for iftype in self.IFACE_TYPES:
                if iface_obj["name"].startswith(iftype):
                    section = iftype
                    index = int(iface_obj["name"][len(iftype):]) + 1
                    break

            else:
                raise ValueError(
                    f'Invalid interface name: {iface_obj["name"]}')

            mtu = int(switch_obj.get("mtu", MTU_DEFAULT))
            if (mtu < MTU_MIN) or (mtu > MTU_MAX_WITH_OVERHEAD):
                raise ValueError(f"Invalid MTU: {mtu}")
            # Cap mtu at MTU_DEFAULT
            mtu = min(mtu, MTU_DEFAULT)

            if system_vlan:
                gateway = str(JSIPInterface(switch_obj["system_vlan_address"]).ip)
            else:
                gateway = str(JSIPInterface(switch_obj["address"]).ip)
            self.network_cfg[section][str(index)] = dict(
                mtu=int(mtu),
                gateway=gateway,
                inet=str(JSIPInterface(iface_obj["address"]))
            )

    def output_config(self):
        return json.dumps(self.network_cfg, sort_keys=True, indent=4,
                          separators=(',', ': '))


class AllSystemsConfigWriter(_ConfigAggregateBase):
    """ Write configuration files for all systems
    """
    def __init__(self, network_config):
        self._config_instances = [
            SystemNetworkConfigWriter(network_config, system_obj["name"])
            for system_obj in network_config.get("systems", list())
        ]

    def config_instances(self):
        return self._config_instances
