import ipaddress
import json
import logging
from collections import defaultdict
from typing import List, Dict, Tuple

from deployment_manager.network_config.deployment.base import DeploymentConfig, DeployCmd
from deployment_manager.network_config.placer.allocator import K_POOL_CLASS_MX, K_POOL_CLASS_SX, \
    K_POOL_CLASS_SY, K_POOL_CLASS_XC, K_POOL_CLASS_VIP, coalesce_subnets, SubnetPool, \
    SubnetAllocator, find_unallocated_subnets, RESV_POOL_KEY, ENV_POOL_KEY
from deployment_manager.network_config.schema.schema import SCHEMA

logger = logging.getLogger(__name__)



def _amend_tier_subnets(network_config: dict, tier_allocations: Dict[Tuple[str, str], List[ipaddress.IPv4Network]]):
    """ Collect all the subnet allocations from the network config file. This looks at the actual device allocations
    and not the extents since the extents might be overlapping with one another due to bugs.
    Returns
        Dict of subnet class to list of subnets in that class
    """

    def _add_or_extend(tier_key: Tuple[str, str], subnet: ipaddress.IPv4Network):
        if tier_key not in tier_allocations:
            tier_allocations[tier_key] = []
        for existing_subnet in tier_allocations[tier_key]:
            if subnet.subnet_of(existing_subnet):
                return
        # search every other subnet in the class and see if it overlaps - if it does, raise an error
        for other_tier_key, existing_subnets in tier_allocations.items():
            for existing_subnet in existing_subnets:
                if other_tier_key == tier_key:
                    if subnet.subnet_of(existing_subnet):
                        return  # already present
                elif subnet.overlaps(existing_subnet):
                    raise RuntimeError(f"overlapping subnets in {other_tier_key} and {tier_key}: {subnet} overlaps {existing_subnet}")
        tier_allocations[tier_key].append(subnet)

    # switch vlan prefixes
    for s in network_config.get("switches", list()):
        for prefix_key in ("prefix", "swarmx_prefix", "system_prefix",):
            if prefix_key in s:
                _add_or_extend((s['tier'], prefix_key,), ipaddress.ip_network(s[prefix_key]))

    # VIP prefix - has to be inferred from the system VIPs
    for system in network_config.get("systems", []):
        if "vip" in system:
            vip = ipaddress.ip_network(system.get("vip"))
            _add_or_extend(("vips", "vip"), vip)
    data_vip = network_config.get("cluster_data_vip", {}).get("vip")
    if data_vip:
        _add_or_extend(("vips", "vip"), ipaddress.ip_network(data_vip))

    # xconn prefixes
    xconn = network_config.get("xconnect", {})
    for c in xconn.get("connections", []):
        if "prefix" not in c:
            continue
        _add_or_extend(("xconnect", "prefix",), ipaddress.ip_network(c["prefix"]))


def _expand_subnet(subnet: ipaddress.IPv4Network, count: int) -> List[ipaddress.IPv4Network]:
    rv = []
    cursor = subnet
    for _ in range(count):
        rv.append(cursor)
        cursor = ipaddress.ip_network(str(cursor.broadcast_address + 1) + "/" + str(cursor.prefixlen))
    return coalesce_subnets(rv)


def _collect_tier_subnets(network_config: dict) -> Dict[Tuple[str, str], List[ipaddress.IPv4Network]]:
    rv = {}
    for tier in network_config.get("tiers", []):
        name = tier["name"]
        if "prefix" in tier:
            rv[(name, "prefix")] = _expand_subnet(ipaddress.ip_network(tier["prefix"]), tier["count"])
        if "swarmx_prefix" in tier:
            rv[(name, "swarmx_prefix")] = _expand_subnet(ipaddress.ip_network(tier["swarmx_prefix"]), tier["count"])
        if "system_prefix" in tier:
            rv[(name, "system_prefix")] = _expand_subnet(ipaddress.ip_network(tier["system_prefix"]), tier["count"])

    # VIP and xconnect prefixes are handled separately
    if "vips" in network_config and "prefix" in network_config['vips']:
        vip_prefix = ipaddress.ip_network(network_config['vips']['prefix'])
        rv[("vips", "vip")] = [vip_prefix]

    xconnect = network_config.get("xconnect", {})
    if xconnect and xconnect.get("count") and "starting_prefix" in xconnect:
        xconnect_prefix = ipaddress.ip_network(xconnect["starting_prefix"])
        rv[("xconnect", "prefix")] = _expand_subnet(xconnect_prefix, xconnect["count"])

    return rv


def find_subnet_pools(network_cfg: dict) -> List[SubnetPool]:
    cluster_prefix = ipaddress.ip_network(network_cfg["environment"]["cluster_prefix"])

    tier_subnets = _collect_tier_subnets(network_cfg)
    _amend_tier_subnets(network_cfg, tier_subnets)

    # now coalesce all the subnets into their reservation class
    sw_vlan_mapping = {
        # v1
        ("AW", "prefix"): K_POOL_CLASS_MX,
        ("BR", "prefix"): K_POOL_CLASS_SX,
        ("BR", "system_prefix"): K_POOL_CLASS_SY,
        # v2
        ("LF", "prefix"): K_POOL_CLASS_MX,
        ("LF", "swarmx_prefix"): K_POOL_CLASS_SX,
        ("LF", "system_prefix"): K_POOL_CLASS_SY,
        # shared
        ("xconnect", "prefix"): K_POOL_CLASS_XC,
        ("vips", "vip"): K_POOL_CLASS_VIP,
    }
    reservation = {}
    all_reserved_subnets = []
    for key, subnets in tier_subnets.items():
        subnets = coalesce_subnets(sorted(subnets))
        reservation[sw_vlan_mapping[key]] = SubnetPool(
            subnets,
            [sw_vlan_mapping[key]],
            f"system {sw_vlan_mapping[key]} reservations",
        )
        all_reserved_subnets.extend(subnets)

    # finally, sanity check that the subnets are all within the cluster prefix
    SubnetAllocator([SubnetPool([cluster_prefix])], all_reserved_subnets)

    # now get any subnets that are in between the reserved subnets and allocate them to the subnet with the closest
    # broadcast address to prevent fragmentation
    all_subnets = []
    for key, pool in reservation.items():
        all_subnets.extend([(sn, pool.permit_classes[0],) for sn in pool.subnets])
    all_subnets.sort(key=lambda x: x[0])
    unallocated_subnets = find_unallocated_subnets(cluster_prefix, [s[0] for s in all_subnets])
    for i in range(len(unallocated_subnets) - 1, -1, -1):
        if unallocated_subnets[i].prefixlen >= 27: # trim the tail if larger than /27
            unallocated_subnets = unallocated_subnets[:i]
            break
    else:
        unallocated_subnets = []

    # now allocate the unallocated subnets to the closest pool
    subnet_cursor, unalloc_cursor = 0, 0
    additions = defaultdict(list)
    while subnet_cursor < len(all_subnets) and unalloc_cursor < len(unallocated_subnets):
        cursor_subnet, cursor_key = all_subnets[subnet_cursor]
        free_subnet = unallocated_subnets[unalloc_cursor]
        if cursor_subnet.broadcast_address + 1 == free_subnet.network_address:
            unalloc_cursor += 1
            additions[cursor_key].append(free_subnet)
            subnet_cursor += 1
            all_subnets.insert(subnet_cursor, (free_subnet,cursor_key, ))
        elif cursor_subnet.broadcast_address + 1 < free_subnet.network_address:
            subnet_cursor += 1
        else:
            unalloc_cursor += 1
    for key, subnets in additions.items():
        reservation[key].subnets.extend(subnets)
        reservation[key].subnets = coalesce_subnets(reservation[key].subnets)

    return list(reservation.values())


class MigrateNetworkSchemaTask(DeployCmd):
    name = "Migrate network schema latest format"

    def __init__(self, deployment_config: DeploymentConfig, **kwargs):
        super().__init__(deployment_config, **kwargs)
        self.network_config_fp = deployment_config.network_config_filename

    def run(self, dry_run=False) -> Tuple[int, None, str]:
        with open(self.network_config_fp, 'r') as f:
            doc = json.load(f)

        modified = False

        # 1/ Migrate subnet allocator to reservation based format
        if not RESV_POOL_KEY in doc:
            # signal that this migration has not been performed - perform the migration
            pools = find_subnet_pools(doc)
            doc[RESV_POOL_KEY] = [p.todict() for p in pools]
            doc["environment"][ENV_POOL_KEY] = [SubnetPool(
                subnets=[ipaddress.ip_network(doc["environment"]["cluster_prefix"])],
                description="cluster prefix",
            ).todict()]
            modified = True

        if not dry_run and modified:
            SCHEMA.save_json(doc, self.network_config_fp)

        return 0, None, "cluster schema migrated"