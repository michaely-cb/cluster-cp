#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Placer classes for nodes
"""

from .base import _BaseItemKeyPlacer
from ..schema import \
    JSIPNetwork, \
    JSIPInterface, \
    NetworkConfigSchema
from ..common.context import NetworkCfgDoc


class InterfacePlacer(_BaseItemKeyPlacer):
    """ Place node and system interfaces on their switch subnets
    """
    entry_sections = NetworkConfigSchema.node_sections + ("systems",)

    item_key = "address"

    def __init__(self, network_config: NetworkCfgDoc):
        self._network_config = network_config
        self.is_leaf_spine = (network_config.raw().get("environment", {}).get("topology") == "leaf_spine")

        self._resource_extents = dict()
        for switch_obj in network_config.raw().get("switches"):
            # Spine switches will only have leaf switches connected to them
            if switch_obj["tier"] == "SP":
                continue
            network = JSIPNetwork(switch_obj["prefix"])
            interfaces = network.interfaces()
            gateway = JSIPInterface(switch_obj["address"])
            # This is used as a subnet. Remove the network, broadcast
            # and gateway addresses.
            del interfaces[0]
            del interfaces[-1]
            interfaces.remove(gateway)

            vrange = switch_obj.get("virtual_addrs")
            if vrange:
                vstart = interfaces.index(JSIPInterface(vrange["starting_address"]))
                vend = interfaces.index(JSIPInterface(vrange["ending_address"]))
                del interfaces[vstart:vend + 1]

            self._resource_extents[switch_obj["name"]] = interfaces

    def resource_extent(self, item_obj):
        return self._resource_extents[item_obj["switch_name"]]

    @staticmethod
    def resource_object(value):
        return JSIPInterface(value)

    def items_iterator(self):
        for entry_section in self.entry_sections:
            for node_obj in self._network_config.raw().get(entry_section, list()):
                if entry_section == "systems":
                    if node_obj.get("system_vlan", "false") == "true" or self.is_leaf_spine:
                        # Systems addresses in the system VLAN will be placed by
                        # SystemVLANInterfacePlacer
                        continue
                if entry_section == "swarmx_nodes" and self.is_leaf_spine:
                    # Swarmx addresses in the swarmx VLAN will be placed by
                    # SwarmxVLANInterfacePlacer
                    continue
                for interface_obj in node_obj.get("interfaces", list()):
                    if interface_obj["switch_name"] in self._resource_extents:
                        yield interface_obj


class SystemVLANInterfacePlacer(_BaseItemKeyPlacer):
    """ Place system interfaces on their switch subnets
    """
    entry_sections = ("systems",)

    item_key = "address"

    def __init__(self, network_config: NetworkCfgDoc):
        self._network_config = network_config
        self.is_leaf_spine = (network_config.raw().get("environment", {}).get("topology") == "leaf_spine")

        self._resource_extents = dict()
        for switch_obj in network_config.raw().get("switches"):
            if not switch_obj.get("system_prefix"):
                continue
            network = JSIPNetwork(switch_obj["system_prefix"])
            interfaces = network.interfaces()
            gateway = JSIPInterface(switch_obj["system_vlan_address"])
            # This is used as a subnet. Remove the network, broadcast
            # and gateway addresses.
            del interfaces[0]
            del interfaces[-1]
            interfaces.remove(gateway)

            self._resource_extents[switch_obj["name"]] = interfaces

    def resource_extent(self, item_obj):
        return self._resource_extents[item_obj["switch_name"]]

    @staticmethod
    def resource_object(value):
        return JSIPInterface(value)

    def items_iterator(self):
        for entry_section in self.entry_sections:
            for node_obj in self._network_config.raw().get(entry_section, list()):
                if node_obj.get("system_vlan", "false") == "false" and not self.is_leaf_spine:
                    # Systems addresses not in the system VLAN will be placed by
                    # InterfacePlacer
                    continue
                for interface_obj in node_obj.get("interfaces", list()):
                    if interface_obj["switch_name"] in self._resource_extents:
                        yield interface_obj


class SwarmxVLANInterfacePlacer(_BaseItemKeyPlacer):
    """ Place swarmx interfaces on their switch subnets
    """
    entry_sections = ("swarmx_nodes",)

    item_key = "address"

    def __init__(self, network_config: NetworkCfgDoc):
        self._network_config = network_config

        self._resource_extents = dict()
        for switch_obj in network_config.raw().get("switches"):
            if not switch_obj.get("swarmx_prefix"):
                continue
            network = JSIPNetwork(switch_obj["swarmx_prefix"])
            interfaces = network.interfaces()
            gateway = JSIPInterface(switch_obj["swarmx_vlan_address"])
            # This is used as a subnet. Remove the network, broadcast
            # and gateway addresses.
            del interfaces[0]
            del interfaces[-1]
            interfaces.remove(gateway)

            vrange = switch_obj.get("swarmx_virtual_addrs")
            if vrange:
                vstart = interfaces.index(JSIPInterface(vrange["starting_address"]))
                vend = interfaces.index(JSIPInterface(vrange["ending_address"]))
                del interfaces[vstart:vend + 1]

            self._resource_extents[switch_obj["name"]] = interfaces

    def resource_extent(self, item_obj):
        return self._resource_extents[item_obj["switch_name"]]

    @staticmethod
    def resource_object(value):
        return JSIPInterface(value)

    def items_iterator(self):
        for entry_section in self.entry_sections:
            for node_obj in self._network_config.raw().get(entry_section, list()):
                for interface_obj in node_obj.get("interfaces", list()):
                    if interface_obj["switch_name"] in self._resource_extents:
                        yield interface_obj
