#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Placer classes for switches
"""
import ipaddress
from collections import defaultdict
from ipaddress import ip_network
from typing import List

from .allocator import SubnetReservationManager
from .base import _BaseItemKeyPlacer
from ..common.context import OBJ_TYPE_SY, OBJ_TYPE_SR, NetworkCfgDoc, K_POOL_CLASS_SY, K_POOL_CLASS_SX, K_POOL_CLASS_MX, \
    K_POOL_CLASS_XC, _VlanConsts
from ..schema import (
    ASN4,
    ASN4Extent,
    JSIPNetwork,
    NetworkConfigSchema
)

OBJ_TYPE_ROLE_VLAN_CLASSES = {
    (OBJ_TYPE_SY, OBJ_TYPE_SY): (K_POOL_CLASS_SY, K_POOL_CLASS_SX,),  # some v1 clusters (cg1) put systems in the SX vlan

    (OBJ_TYPE_SR, "swarmx"): (K_POOL_CLASS_SX,),

    (OBJ_TYPE_SR, "activation"): (K_POOL_CLASS_MX,),
    (OBJ_TYPE_SR, "management"): (K_POOL_CLASS_MX,),
    (OBJ_TYPE_SR, "inferencedriver"): (K_POOL_CLASS_MX,),
    (OBJ_TYPE_SR, "memoryx"): (K_POOL_CLASS_MX,),
    (OBJ_TYPE_SR, "worker"): (K_POOL_CLASS_MX,),
}


def place_xconnect_prefixes(network_config: NetworkCfgDoc):
    """ Assign /31's to xconnects """
    connections = network_config.raw().get("xconnect", {}).get("connections", [])
    unallocated_candidates = [
        conn for conn in connections
        if not conn.get("prefix")
    ]
    if not unallocated_candidates:
        return

    starting_prefix = network_config.raw().get("xconnect", {}).get("starting_prefix", None)
    if starting_prefix is None:
        preferred_reservation_prefixlen = 31
    else:
        preferred_reservation_prefixlen = int(starting_prefix.split("/")[1])

    subnet_manager = SubnetReservationManager(network_config)
    reserved_prefixes = subnet_manager.allocate(
        31, len(unallocated_candidates), K_POOL_CLASS_XC, reserve_prefixlen=preferred_reservation_prefixlen
    )
    for i, conn in enumerate(unallocated_candidates):
        conn["prefix"] = str(reserved_prefixes[i])


def place_switch_prefixes(cfg: NetworkCfgDoc, vlan_consts: _VlanConsts):
    """ Assign subnets to switches. This will never unassign an existing subnet/gateway. """
    candidate_switches = [
        s for s in
        sorted(cfg.raw().get("switches", []), key=lambda x: (x["tier_pos"], x["name"]))
        if s['tier'] != 'SP' and (
                s.get(vlan_consts.prefix_key) is None or
                s.get(vlan_consts.gateway_key) is None
        )
    ]

    if not candidate_switches:
        return

    gw_index = cfg.gateway_index()
    subnet_manager = SubnetReservationManager(cfg)

    # first allocate any provided prefixes
    unallocated_candidates = []
    for switch in candidate_switches:
        if not switch.get(vlan_consts.provided_prefix_key):
            unallocated_candidates.append(switch)
            continue

        prefix = ipaddress.ip_network(switch[vlan_consts.provided_prefix_key])
        err = subnet_manager.allocate_subnet(prefix, vlan_consts.tier_class_map[switch["tier"]])
        if err:
            raise ValueError(
                f"unable to allocate provided prefix {prefix} for switch {switch['name']}: {err}"
            )
        switch[vlan_consts.prefix_key] = str(prefix)
        switch[vlan_consts.gateway_key] = f"{list(prefix.hosts())[gw_index]}/{prefix.prefixlen}"

    # second, allocate any unspecified prefixes but only if their corresponding tier had a prefix from this class
    # assigned to it. This prevents system_prefixes from being allocated to memx_swarmx topology BR switches which
    # existed prior to the introduction of system_prefixes
    class_to_prefixlen = {}
    for tier in cfg.raw().get("tiers"):
        if vlan_consts.tier_prefix_key in tier:
            vlan_class = vlan_consts.tier_class_map[tier["name"]]
            class_to_prefixlen[vlan_class] = ip_network(tier[vlan_consts.tier_prefix_key]).prefixlen

    class_to_switch = defaultdict(list)
    for switch in candidate_switches:
        if switch["tier"] not in vlan_consts.tier_class_map:
            continue

        # TODO: optimize here to not allocate a vlan to a switch with no neighbors in that vlan
        vlan_class = vlan_consts.tier_class_map[switch["tier"]]
        class_to_switch[vlan_class].append(switch)

    class_to_subnets = {}
    for class_key in sorted(class_to_switch.keys()):
        if class_key in class_to_prefixlen:
            class_to_subnets[class_key] = subnet_manager.allocate(
                class_to_prefixlen[class_key], len(class_to_switch[class_key]), class_key
            )
    for class_key, subnets in class_to_subnets.items():
        for i, switch in enumerate(class_to_switch.get(class_key, [])):
            subnet = subnets[i]
            switch[vlan_consts.prefix_key] = str(subnet)
            switch[vlan_consts.gateway_key] = f"{list(subnet.hosts())[gw_index]}/{subnet.prefixlen}"


class SwitchVirtualAddrPlacer:
    """
    Place switch virtual address ranges by inspecting the number of interfaces connected to a switch and reserving
    a range of addresses. Checks if the requested number of physical addresses has been preserved.

    NOTES
    The number of virtual addresses needed is a function of the number and type of node types in a subnet. Ideally,
    we'd know the final number of nodes per subnet before running this function, but clusters grow incrementally
    without a view of the final topology.

    Therefore, a better way to size the virtual ranges would be to calculate the min required virtual range size and
    grow the virtual range from the high order addresses while the physical grows from the low end. However, this
    strategy would require multus node configuration during incremental deploy to re-generate NAD config on all nodes
    during incremental deploy to avoid the risk of running out of virtual addresses
    """
    DEFAULT_GATEWAY_IDX = -1

    def __init__(self, network_config: NetworkCfgDoc):
        self.gateway_idx = self.DEFAULT_GATEWAY_IDX
        if network_config.raw().get("environment"):
            if "gateway_index" in network_config.raw().get("environment"):
                self.gateway_idx = int(network_config.raw().get("environment")["gateway_index"])

        self._switches = {
            s['name']: s for s in network_config.raw().get("switches", [])
            if s['tier'] not in ('SP', 'BR')
        }

        # Get total number of interfaces connected to this switch
        # which need an address, grouped by vlan
        self._interface_count = dict(
            prefix=defaultdict(int),
            swarmx_prefix=defaultdict(int)
        )
        for obj_type in [*NetworkConfigSchema.node_sections, *NetworkConfigSchema.system_sections]:
            for obj in network_config.raw().get(obj_type, []):
                for i in obj.get("interfaces", []):
                    if i["switch_name"] not in self._switches:
                        continue
                    switch = self._switches[i["switch_name"]]
                    key = "prefix"
                    if obj_type == "swarmx_nodes" and switch.get("swarmx_prefix"):
                        key = "swarmx_prefix"
                    elif obj_type in NetworkConfigSchema.system_sections:
                        if switch.get("system_prefix"):
                            # The system vlan does not have a virtual address range
                            # so we can ignore this interface
                            continue
                        elif switch.get("swarmx_prefix"):
                            key = "swarmx_prefix"
                    self._interface_count[key][i["switch_name"]] += 1

        self._tier_min_physical = dict()
        for tier_obj in network_config.raw().get("tiers", []):
            tier_name = tier_obj["name"]
            self._tier_min_physical[tier_name] = int(tier_obj.get("min_physical", 0))

    def _set_switch_virtual_addresses(self, item_obj):
        switch_prefix = JSIPNetwork(item_obj["prefix"])
        gateway = ipaddress.ip_address(item_obj["address"].split("/")[0])

        # If there are virtual addresses, allocate from the end
        # after addresses have been allocated for interfaces
        # connected to physical ports

        # default vlan virtual range
        reserve_addr_count = max(
            self._interface_count["prefix"].get(item_obj["name"], 0),  # actually allocated
            self._tier_min_physical.get(item_obj["tier"], 0)  # default reservation for physical interfaces
        )

        addrs = list(switch_prefix.hosts())

        # TODO: this assumes the gateway index is near the end of the range - add some arbitrary check but this but should be enforced as a default
        gw_index = addrs.index(gateway)
        assert gw_index != -1, f"gateway address {gateway} not found in {switch_prefix}"
        assert len(addrs) - gw_index < 6, f"gateway address {gateway} is not within the last 5 addresses of {switch_prefix}"

        addrs = addrs[reserve_addr_count:gw_index]
        new_start = addrs[0]
        new_end = addrs[-1]
        old_val = item_obj.get("virtual_addrs")
        if not old_val:
            item_obj["virtual_addrs"] = {
                "starting_address": f"{new_start}/{switch_prefix.prefixlen}",  # inclusive
                "ending_address": f"{new_end}/{switch_prefix.prefixlen}",  # inclusive
            }
        else:
            # preserve the virtual range in the document. It's OK if the calculated range contains the range found in
            # the document as sometimes custom ranges are hand-crafted for test purposes
            old_start = ipaddress.ip_address(old_val["starting_address"].split("/")[0])
            old_end = ipaddress.ip_address(old_val["ending_address"].split("/")[0])
            min_allowed, max_allowed = addrs[0], addrs[-1]
            if old_start < min_allowed or old_end > new_end:
                raise AssertionError(
                    f"error placing {item_obj['name']}: existing virtual address range falls outside the allowed virtual "
                    f"address range (allowed={min_allowed},{max_allowed}, current={old_start},{old_end}). "
                    "This is probably due to virtual address range sizes being allocated too small during configuration "
                    "or due to manual edits. You must carefully ensure multus is not using virtual addresses and "
                    "hand-edit the network_config to fix this."
                )

    def __call__(self, *args, **kwargs):
        for item_obj in self._switches.values():
            self._set_switch_virtual_addresses(item_obj)


class SwitchASNPlacer(_BaseItemKeyPlacer):
    """ Place switch ASNs

    The BR tier is the spine layer of a Clos spine-leaf network.
    All switches at this tier get the same ASN so that BR tiers
    aren't advertised to each other.

    Note, this is designed for a single BR switch tier only.
    """
    item_key = "asn"

    def __init__(self, network_config: NetworkCfgDoc):
        self._items = \
            sorted(network_config.raw().get("switches", []),
                   key=lambda x: x["tier_pos"])

        self._resource_extent = ASN4Extent()

        for tier_obj in network_config.raw().get("environment").get("cluster_asns", []):
            self._resource_extent += ASN4Extent(
                tier_obj["starting_asn"], tier_obj["count"])

        # The first ASN is reserved for the BR tier
        if self._br_switch_exists():
            self.br_tier_asn = self._resource_extent.pop(0)

    def _br_switch_exists(self):
        for item_obj in self._items:
            if item_obj["tier"] == "BR":
                return True
        return False

    def resource_extent(self, item_obj):
        if item_obj["tier"] == "BR":
            return ASN4Extent(self.br_tier_asn, 1)

        return self._resource_extent

    def resource_object(self, value):
        return ASN4(value)

    def items_iterator(self):
        for item in self._items:
            yield item


class SwitchVLANPlacer:
    """
    Place switch vlans

    Note: it's not necessary to place different vlans for each switch since we're using routed connections between
    switches. Additionally, SP switches don't use vlans at all so shouldn't place vlans
    """
    AW_BR_VLAN_START = 11
    MEMX_VLAN_START = 12  # e.g. AW_BR_VLAN_START v2 network starting offset
    SYSTEM_VLAN_START = 13
    SWARMX_VLAN_START = 15

    VLAN_INTERVAL = 10

    AW_TIER = "AW"
    BR_TIER = "BR"
    LF_TIER = "LF"
    SP_TIER = "SP"

    VLAN_NAME = "vlan"
    SYSTEM_VLAN_NAME = "system_vlan"
    SWARMX_VLAN_NAME = "swarmx_vlan"

    def __init__(self, network_config):
        self._topology = network_config.raw().get("environment", {}).get("topology", "memx_swarmx")
        self._aw_switches = []  # AW
        self._not_aw_switches = []  # LF, SP, BR
        self._lf_switches = []
        self._switches = []

        for s in network_config.raw().get("switches", []):
            bucket = self._aw_switches if s["tier"] == self.AW_TIER else self._not_aw_switches
            bucket.append(s)
            if s["tier"] == self.LF_TIER:
                self._lf_switches.append(s)
            self._switches.append(s)

    @staticmethod
    def alloc_vlan(vlan_name: str, vlan_start: int, switches: List[dict]):
        """ Take the highest number vlan and add 10 for a new vlan. Sort by name for determinism """
        assert 10 <= vlan_start < 20
        interval = 10
        offset = vlan_start % interval

        last_vlan = max([offset] + [s[vlan_name] for s in switches if vlan_name in s])
        assert last_vlan % interval == offset, f"largest vlan for {vlan_name} was {last_vlan} but should have been {offset} modulo {interval}"

        need_alloc = sorted([s for s in switches if vlan_name not in s], key=lambda s: s['name'])
        for switch_obj in need_alloc:
            val = last_vlan + interval
            switch_obj[vlan_name] = val
            last_vlan = val

    def __call__(self):
        actual_mx_vlan_start = [s[self.VLAN_NAME] for s in self._switches if self.VLAN_NAME in s]
        if actual_mx_vlan_start:
            # xxx: due to a bug, some V2 clusters are using the AW_BR_VLAN_START - just accept that as the default as
            # there's no real harm in using a different offset
            mx_vlan_start = (max(actual_mx_vlan_start) % 10) + 10
        else:
            # AW or BR vlan for V1 and MX vlan for V2
            mx_vlan_start = self.AW_BR_VLAN_START if self._topology == "memx_swarmx" else self.MEMX_VLAN_START
        SwitchVLANPlacer.alloc_vlan(self.VLAN_NAME, mx_vlan_start, self._aw_switches)
        SwitchVLANPlacer.alloc_vlan(self.VLAN_NAME, mx_vlan_start, self._not_aw_switches)
        # all switches get a system vlan
        SwitchVLANPlacer.alloc_vlan(self.SYSTEM_VLAN_NAME, self.SYSTEM_VLAN_START, self._switches)
        # leaf switches get a swarmx vlan
        SwitchVLANPlacer.alloc_vlan(self.SWARMX_VLAN_NAME, self.SWARMX_VLAN_START, self._lf_switches)
