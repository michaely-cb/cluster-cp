#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.


import ipaddress

from .allocator import K_POOL_CLASS_VIP, SubnetReservationManager
from ..common.context  import NetworkCfgDoc

def place_cluster_vips(network_config: NetworkCfgDoc):
    # for each cluster, allocate a VIP. If it's the first cluster, also populate the cluster_data_vip field for
    # backwards compatibility
    for i, cluster in enumerate(network_config.raw().get("clusters") or []):
        cluster["data_network"] = cluster.get("data_network", {})
        data_nw = cluster["data_network"]
        if not "vip" in data_nw:
            vips_prefix = ipaddress.ip_network(network_config.raw().get("vips")["prefix"])
            subnet_manager = SubnetReservationManager(network_config)
            vips = subnet_manager.allocate(32, 1, K_POOL_CLASS_VIP, vips_prefix.prefixlen)
            vip = vips[0]
            data_nw["vip"] = str(vip)
            if i == 0:
                network_config.raw()["cluster_data_vip"] = {"vip": str(vip)}


def place_system_vips(network_config: NetworkCfgDoc):
    candidates = [
        system for system in (network_config.raw().get("systems") or [])
        if not system.get("vip")
    ]
    if not candidates:
        return

    vips_prefix = ipaddress.ip_network(network_config.raw().get("vips")["prefix"])
    subnet_manager = SubnetReservationManager(network_config)
    vips = subnet_manager.allocate(32, len(candidates), K_POOL_CLASS_VIP, vips_prefix.prefixlen)
    for system, vip in zip(candidates, vips):
        system.update({"vip": str(vip)})

