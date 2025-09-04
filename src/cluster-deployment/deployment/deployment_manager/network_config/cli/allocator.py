#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved

"""
Allocate network extents
"""
import ipaddress
import math

from .base import \
    SubCommandBase, \
    UsesNetworkConfig
from ..placer.allocator import SubnetReservationManager, K_POOL_CLASS_MX, K_POOL_CLASS_SX, K_POOL_CLASS_SY, \
    K_POOL_CLASS_VIP, K_POOL_CLASS_XC
from deployment_manager.network_config.common.context import NetworkCfgDoc

def reserve_xconn_prefixlen(xconn_count: int) -> int:
    """
    Small optimization to allocate xconnects in large continuous blocks if the number of xconnects
    is large. This is to avoid fragmentation of the xconnect space.
    """
    if xconn_count >= 512:
        return 23
    elif xconn_count >= 256:
        return 24
    elif xconn_count >= 128:
        return 25
    return 31


@UsesNetworkConfig(save=True)
class BuildTierAllocations(SubCommandBase):
    """
    Build the network extents for allocation from a prefix

    We have two topologies for the cluster's 100G network: the default memx-swarmx, or leaf-spine. In the default
    topology swarmx nodes and systems are connected to swarmx (BR) switches and all other nodes are connected to memx
    (AW) switches. In the leaf-spine topology all nodes and systems are connected to leaf (LF) switches and they in turn
    are connected to spine (SP) switches.

    Tier allocation divides the network into segments "tiers" for the placer to further divide for each device and
    interfaces.

    The allocator creates the following segments:
    0 AW subnets (ax, mx, mg, us, wk, virtual_addrs)
    1 BR subnets (sx, virtual_addrs)
    2 system subnets
    3 vips
    4 cross-connects (AW/BR in memx-swarmx and LF/SP in leaf-spine)
    And assigns tiers AW=0 BR=1,2 in memx-swarmx and LF=0,1,2 in leaf-spine
    """

    # The allocator doesn't try to be particularly smart and just relies
    # on input to guide allocation strategy.
    #
    # This should probably be a separate module inside of the package
    # instead of being stuffed into a command class.
    sub_command = 'allocate_tiers'

    LEAF_SPINE_AW_MIN_PHYSICAL = 142  # 2*48 MEMX + 2*18 WK + 2*2 MG + 2*3 ACT
    LEAF_SPINE_BR_MIN_PHYSICAL = 68   # 6*6  BR + BUFFER
    MEMX_SWARMX_MIN_PHYSICAL = 48

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-p', '--cluster_prefix', required=True,
                            help='Cluster prefix to allocate from')

        parser.add_argument('-t', '--topology', default='memx_swarmx',
                            choices=['memx_swarmx', 'leaf_spine'],
                            help='Specify network topology of the cluster')

        # Will be distributed across memx switches based on aw_count in the
        # memx_swarmx topology and leaf switches based on leaf_count in the
        # leaf_spine topology
        parser.add_argument('--aw_prefix', required=False,
                            help='Prefix to allocate AW IPs from')

        # This argument is needed if aw_prefix is not specified. This along
        # with aw_count/leaf_count will be used to carve out a subnet from cluster_prefix
        # if aw_prefix is not specified.
        parser.add_argument('--aw_size', type=int, required=False,
                            choices=(256, 512, 1024),
                            help='Size of each AW tier allocation')

        parser.add_argument('--aw_count', type=int, required=False,
                            help='Number of AW subnets to allocate')

        # Will be distributed across swarmx switches based on br_count in the
        # memx_swarmx topology and leaf switches based on leaf_count in the
        # leaf_spine topology
        parser.add_argument('--br_prefix', required=False,
                            help='Prefix to allocate BR IPs from')

        # This argument is needed if br_prefix is not specified. This along
        # with br_count will be used to carve out a subnet from
        parser.add_argument('--br_size', type=int, required=False,
                            choices=(32, 64, 128, 256),
                            help='Size of each BR tier allocation')

        parser.add_argument('--br_count', type=int, required=False,
                            help='Number of BR subnets to allocate')

        # This is needed for leaf_spine topology. Spine count is not needed
        # since we don't need to allocate subnets for spine switches. We need
        # to allocate AW, BR and system subnets for each leaf switch
        parser.add_argument('--leaf_count', type=int, required=False,
                            help='Number of leaf switches')

        # This is the minimum number of interfaces to reserve for physical
        # connections to memx/leaf switches
        parser.add_argument('--aw_physical', type=int, default=0,
                            help=(
                                "Minimum number of physical interfaces"
                                " per memx/leaf switch"
                            ))
        # This is the minimum number of interfaces to reserve for physical
        # connections to swarmx/leaf switches for swarmx nodes
        parser.add_argument('--br_physical', type=int, default=0,
                            help=(
                                "Minimum number of physical interfaces"
                                " per swarmx/leaf switch"
                            ))

        parser.add_argument('--vip_count', type=int, required=True,
                            help='Number of VIPs')

        parser.add_argument('--vip_prefix', required=False,
                            help='Prefix to allocate VIPs from')

        parser.add_argument('--xconn_prefix', required=False,
                            help='Prefix to allocate cross-connects from')

        parser.add_argument('--xconn_count', type=int, required=False,
                            help='Number of cross connections')

        parser.add_argument('--system_prefix', required=False,
                            help='Prefix to allocate system IPs from')

        parser.add_argument('--system_prefix_size', type=int, default=128,
                            help='Size of system IP allocation range')

    def __call__(self, args):
        args.network_config["tiers"] = list()
        args.network_config["environment"]["topology"] = args.topology
        if not args.network_config["environment"].get("subnet_pools"):
            # normally set in set_network_config_environment but the tests might not call this
            args.network_config["environment"]["subnet_pools"] = [
                {
                    "description": "cluster prefix",
                    "subnets": [args.cluster_prefix],
                }
            ]
        doc = NetworkCfgDoc(args.network_config)
        subnet_manager = SubnetReservationManager(doc)

        if args.system_prefix and not args.system_prefix_size:
            raise ValueError(
                "system_prefix_size must be specified along with system_prefix"
            )

        is_leaf_spine = (args.topology == "leaf_spine")

        if is_leaf_spine and not args.leaf_count:
            raise ValueError(
                "leaf_count must be specified for the leaf_spine topology"
            )
        if not is_leaf_spine and not args.aw_count:
            raise ValueError(
                "aw_count must be specified for the memx_swarmx topology"
            )

        if is_leaf_spine:
            aw_count = args.leaf_count
            br_count = args.leaf_count
            # err on the side of caution
            aw_physical = max(args.aw_physical, self.LEAF_SPINE_AW_MIN_PHYSICAL)
            br_physical = max(args.br_physical, self.LEAF_SPINE_BR_MIN_PHYSICAL)
        else:
            aw_count = args.aw_count
            br_count = args.br_count
            aw_physical = max(args.aw_physical, self.MEMX_SWARMX_MIN_PHYSICAL)
            br_physical = max(args.br_physical, self.MEMX_SWARMX_MIN_PHYSICAL)

        # AW prefix
        prefixlen = 32 - math.ceil(math.log(args.aw_size, 2))
        prefixcount = 1 << (32 - prefixlen)
        aw_addr_count = prefixcount - 3
        if aw_addr_count <= 0:
            raise ValueError('AW prefix allocation too small')
        if not args.aw_prefix:
            memx_prefixes = subnet_manager.allocate(prefixlen, aw_count, K_POOL_CLASS_MX)
            memx_starting_prefix = str(memx_prefixes[0])
        else:
            memx_starting_prefix = ipaddress.ip_network(args.aw_prefix)
            err = subnet_manager.allocate_subnet(memx_starting_prefix, K_POOL_CLASS_MX)
            if err:
                raise ValueError(f"AW prefix {args.aw_prefix} is not valid: {err}")
            memx_starting_prefix = str(memx_starting_prefix)

        # swarmx prefix
        swarmx_starting_prefix = None
        swarmx_addr_count = 0
        if args.br_size:
            prefixlen = 32 - math.ceil(math.log(args.br_size, 2))
            prefixcount = 1 << (32 - prefixlen)
            swarmx_addr_count = prefixcount - 3
            if swarmx_addr_count <= 0:
                raise ValueError('BR prefix allocation too small')

            if not args.br_prefix:
                sx_prefixes = subnet_manager.allocate(prefixlen, br_count, K_POOL_CLASS_SX)
                swarmx_starting_prefix = str(sx_prefixes[0])
            else:
                sx_starting_prefix = ipaddress.ip_network(args.br_prefix)
                err = subnet_manager.allocate_subnet(sx_starting_prefix, K_POOL_CLASS_SX)
                if err:
                    raise ValueError(f"AW prefix {args.aw_prefix} is not valid: {err}")
                swarmx_starting_prefix = str(sx_starting_prefix)

        # systems
        system_starting_prefix = None
        if args.system_prefix or is_leaf_spine:
            system_prefixlen = 32 - math.ceil(math.log(args.system_prefix_size, 2))
            if not args.system_prefix:
                sy_prefixes = subnet_manager.allocate(system_prefixlen, aw_count, K_POOL_CLASS_SY)  # TODO: aw_count doesn't make sense here
                system_starting_prefix = str(sy_prefixes[0])
            else:
                system_base_subnet = ipaddress.ip_network(args.system_prefix)
                subnet_manager.allocate_subnet(system_base_subnet, K_POOL_CLASS_SY)
                system_starting_prefix = str(system_base_subnet)

        if is_leaf_spine:
            tier_extent = dict(name="LF",
                               starting_prefix=memx_starting_prefix,
                               count=aw_count,
                               virtual_physical=aw_addr_count,
                               swarmx_virtual_physical=swarmx_addr_count,
                               min_physical=aw_physical,
                               swarmx_min_physical=br_physical)
            if swarmx_starting_prefix:
                tier_extent["swarmx_starting_prefix"] = swarmx_starting_prefix
            if system_starting_prefix:
                tier_extent["system_starting_prefix"] = system_starting_prefix
            args.network_config["tiers"].append(tier_extent)
        else:
            aw_tier = dict(name="AW",
                           starting_prefix=memx_starting_prefix,
                           count=aw_count,
                           virtual_physical=aw_addr_count,
                           min_physical=aw_physical)
            args.network_config["tiers"].append(aw_tier)
            if swarmx_starting_prefix:
                br_tier = dict(name="BR",
                               starting_prefix=swarmx_starting_prefix,
                               swarmx_virtual_physical=swarmx_addr_count,
                               swarmx_min_physical=br_physical,
                               count=br_count)
                if system_starting_prefix:
                    br_tier["system_starting_prefix"] = system_starting_prefix
                args.network_config["tiers"].append(br_tier)

        # Management and system VIPs
        if "vips" not in args.network_config:
            args.network_config["vips"] = dict()
        if "cluster_data_vip" not in args.network_config:
            args.network_config["cluster_data_vip"] = dict()

        prefixlen = 32 - math.ceil(math.log(args.vip_count, 2))
        if not args.vip_prefix:
            vip_subnets = subnet_manager.allocate(prefixlen, 1, K_POOL_CLASS_VIP)
            vip_prefix = str(vip_subnets[0])
        else:
            vip_subnet = ipaddress.ip_network(args.vip_prefix)
            if len(list(vip_subnet)) < args.vip_count:
                raise ValueError(f"Insufficient prefix size for {args.vip_count} VIPs")
            err = subnet_manager.allocate_subnet(vip_subnet, K_POOL_CLASS_VIP)
            if err:
                raise ValueError(f"VIP prefix {args.vip_prefix} is not valid: {err}")
            vip_prefix = str(vip_subnet)
        args.network_config["vips"]["prefix"] = vip_prefix

        # XConnect
        if args.xconn_count:
            if "xconnect" not in args.network_config:
                args.network_config["xconnect"] = dict()

            args.network_config["xconnect"]["count"] = args.xconn_count

            if args.xconn_prefix:
                xconn_subnet = ipaddress.ip_network(args.xconn_prefix)
                if len(list(xconn_subnet)) < args.xconn_count*2:
                    raise ValueError(f"Insufficient prefix size for {args.xconn_count} VIPs")
                err = subnet_manager.allocate_subnet(xconn_subnet, K_POOL_CLASS_XC)
                if err:
                    raise ValueError(f"xconnect prefix {args.vip_prefix} is not valid: {err}")
                xconn_prefix = f"{xconn_subnet.network_address}/31"
            else:
                # small optimization - prefer to allocate xconnects in contiguous blocks
                prefixlen = reserve_xconn_prefixlen(args.xconn_count)
                count, rem = divmod(args.xconn_count * 2, math.pow(2, 32 - prefixlen))
                if rem:
                    count += 1
                xconn_prefixes = subnet_manager.allocate(prefixlen, int(count), K_POOL_CLASS_XC)
                xconn_prefix = str(xconn_prefixes[0])

            args.network_config["xconnect"]["starting_prefix"] = xconn_prefix
