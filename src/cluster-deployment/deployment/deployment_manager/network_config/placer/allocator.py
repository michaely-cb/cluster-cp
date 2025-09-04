import dataclasses
import ipaddress
import itertools
import logging
import math
from typing import List, Tuple, Callable

from deployment_manager.network_config.common.context import K_POOL_CLASS_MX, K_POOL_CLASS_SX, K_POOL_CLASS_SY,  \
    K_POOL_CLASS_XC, K_POOL_CLASS_VIP, NetworkCfgDoc

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SubnetPool:
    subnets: List[ipaddress.IPv4Network]
    # list of subnet classes that can use the range, empty = permit all
    permit_classes: List[str] = dataclasses.field(default_factory=list)
    description: str = ""

    def todict(self) -> dict:
        return {
            "subnets": [str(subnet) for subnet in self.subnets],
            "permit_classes": self.permit_classes,
            "description": self.description,
        }

    @classmethod
    def fromdict(cls, data: dict):
        return cls(
            subnets=[ipaddress.ip_network(subnet) for subnet in data["subnets"]],
            permit_classes=data.get("permit_classes", []),
            description=data.get("description", ""),
        )


def coalesce_subnets(subnets: List[ipaddress.IPv4Network]) -> List[ipaddress.IPv4Network]:
    """
    Coalesce subnets if they are adjacent, have the same prefix length, and their combination forms a new subnet that
    exactly contains both. Expects input to be non-overlapping.
    Returns
        A new list of coalesced subnets
    """
    rv = []
    for subnet in sorted(subnets):
        while rv and (
                rv[-1].prefixlen == subnet.prefixlen and
                rv[-1].broadcast_address + 1 == subnet.network_address and
                rv[-1].supernet(prefixlen_diff=1).network_address == rv[-1].network_address
        ):
            subnet = rv.pop().supernet(prefixlen_diff=1)
        rv.append(subnet)
        if len(rv) > 1 and rv[-1].network_address < rv[-2].broadcast_address:
            raise AssertionError("invalid state: overlapping subnets")
    return rv


def find_unallocated_subnets(
        supernet: ipaddress.IPv4Network,
        allocated_subnets: List[ipaddress.IPv4Network],
) -> List[ipaddress.IPv4Network]:
    """
    Find unallocated subnets between the allocated subnets and the cluster prefix. Assumes that the allocated subnets are
    sorted and non-overlapping. The supernet must contain all the allocated subnets.
    Think of it as supernet - allocated_subnets.
    Returns
        A list of unallocated subnets from the supernet
    """
    if not allocated_subnets:
        return [supernet]

    unallocated_subnets = []

    if allocated_subnets[0].network_address != supernet.network_address:
        unallocated_subnets.extend(
            ipaddress.summarize_address_range(supernet.network_address, allocated_subnets[0].network_address - 1))

    for i in range(len(allocated_subnets) - 1):
        start = allocated_subnets[i].broadcast_address + 1
        end = allocated_subnets[i + 1].network_address - 1
        if start < end:
            unallocated_subnets.extend(ipaddress.summarize_address_range(start, end))

    if allocated_subnets[-1].broadcast_address != supernet.broadcast_address:
        unallocated_subnets.extend(ipaddress.summarize_address_range(allocated_subnets[-1].broadcast_address + 1,
                                                                     supernet.broadcast_address))

    return unallocated_subnets


@dataclasses.dataclass
class _SubnetAllocation:
    pool: ipaddress.IPv4Network
    origin_subnet: ipaddress.IPv4Network
    subnet_parts_taken: List[ipaddress.IPv4Network]  # parts taken from origin
    subnets_parts_remaining: List[ipaddress.IPv4Network]  # parts remaining in origin


class SubnetAllocator:

    def __init__(self, pools: List[SubnetPool], allocated_subnets: List[ipaddress.IPv4Network]):
        self._subnet_pool = {
            subnet: pool for pool in pools for subnet in pool.subnets
        }
        self._subnet_allocations = {subnet: [] for subnet in self._subnet_pool}
        self._subnet_unallocated = {}

        # enforce that pools must not overlap
        all_pool_subnets = sorted([s for pool in pools for s in pool.subnets])
        for i, subnet in enumerate(all_pool_subnets):
            if i > 0:
                if all_pool_subnets[i - 1].broadcast_address > subnet.network_address:
                    raise ValueError(f"invalid state: overlapping pool subnets, {all_pool_subnets[i - 1]} and {subnet}")

        # place subnets into their allocated pools
        allocated_subnets = sorted(allocated_subnets)
        for i, subnet in enumerate(allocated_subnets):
            if i > 0:
                if allocated_subnets[i - 1].broadcast_address > subnet.network_address:
                    raise ValueError(
                        f"invalidate state: overlapping subnet allocations, {allocated_subnets[i - 1]} and {subnet}")

            for pool in self._subnet_allocations:
                if subnet.subnet_of(pool):
                    self._subnet_allocations[pool].append(subnet)
                    break
            else:
                raise ValueError(f"subnet {subnet} not in any pool")

        for pool in list(self._subnet_allocations.keys()):
            self._subnet_allocations[pool] = coalesce_subnets(self._subnet_allocations[pool])

        for pool, subnets in self._subnet_allocations.items():
            self._subnet_unallocated[pool] = find_unallocated_subnets(pool, subnets)

        self._pending_commits = None

    def _allocate_uncommitted(self, prefixlen: int, count: int, pool_class: str = "") -> List[_SubnetAllocation]:
        if not (0 < prefixlen <= 32):
            raise ValueError(f"invalid prefix length: {prefixlen}")

        if count < 0:
            raise ValueError(f"count must be >= 0: {count}")

        if count == 0:
            return []

        # Sort free subnets by their class restrictions, then prefixlen, then by their network address.
        # Then choose the first one that fits do this until the count is satisfied
        candidates = []
        for pool, unallocated_subnets in self._subnet_unallocated.items():
            permit_classes = self._subnet_pool[pool].permit_classes
            if permit_classes:
                if pool_class not in permit_classes:
                    continue
                class_match_score = len(permit_classes)  # try to take the most specific pool first
            else:
                class_match_score = 1000  # least preference to the pool with no class restrictions

            for subnet in unallocated_subnets:
                if subnet.prefixlen <= prefixlen:
                    candidates.append((class_match_score, 32 - subnet.prefixlen, subnet, pool,))
        candidates = sorted(candidates, key=lambda x: (x[0], x[1], x[2].network_address))

        allocations = []
        need_count = count
        for _, _, subnet, pool in candidates:
            parts = list(subnet.subnets(new_prefix=prefixlen))
            allocations.append(_SubnetAllocation(pool, subnet, parts[:need_count], parts[need_count:]))
            need_count -= len(parts[:need_count])
            if need_count == 0:
                break
        return allocations

    def allocate(self, prefixlen: int, count: int, pool_class: str = "") -> List[ipaddress.IPv4Network]:
        """
        Allocate subnets of the given prefixlen from the unallocated subnets in the pool. Always allocates to a pool
        with the most specific class restriction first. If no class is specified, allocate from pools with no class
        restrictions.
        Arguments:
            prefixlen: The prefix length of the subnets to allocate
            count: The number of subnets to allocate
            pool_class: The class of the pool to allocate from. If empty, allocate only from pools with no class
        Returns:
            A list of allocated subnets. May return fewer than requested if not enough subnets are available.
        """
        return self._commit_allocations(
            self._allocate_uncommitted(prefixlen, count, pool_class)
        )

    def allocate_uncommitted(
            self, prefixlen: int, count: int, pool_class: str = ""
    ) -> Tuple[List[ipaddress.IPv4Network], Callable[[], List[ipaddress.IPv4Network]]]:
        """
        Same as allocate, but does not commit the allocations. Returns a list of allocated subnets and a commit
        function to allow deferring the allocation if the caller decides that the allocatable subnets are not sufficient.
        """
        allocations = self._allocate_uncommitted(prefixlen, count, pool_class)
        self._pending_commits = allocations

        def commit_fn() -> List[ipaddress.IPv4Network]:
            assert self._pending_commits is allocations, "usage error: allocations pending commits have changed"
            rv = self._commit_allocations(allocations)
            self._pending_commits = None
            return rv

        return sorted([sn for alloc in self._pending_commits for sn in alloc.subnet_parts_taken]), commit_fn

    def allocate_subnet(self, subnet: ipaddress.IPv4Network, permit_class: str = "") -> str:
        """ Allocate a specific subnet from the pool. Ensures that the subnet is in the pool and not already allocated.
        Returns
            non-empty string if error, empty string if success
        """

        def _find_allocation():
            for pool, subnets in self._subnet_unallocated.items():
                if not subnet.subnet_of(pool):
                    continue
                for i, candidate in enumerate(subnets):
                    if subnet.subnet_of(candidate):
                        # break out this is a subnet of a larger subnet, so we need to split it
                        parts = list(candidate.subnets(new_prefix=subnet.prefixlen))
                        i = parts.index(subnet)
                        return _SubnetAllocation(pool, candidate, [parts[i]], parts[0:i] + parts[i + 1:])
                else:
                    raise ValueError(f"subnet or part of subnet already allocated in pool {pool}")
            raise ValueError(f"subnet not in any pool")

        try:
            alloc = _find_allocation()
        except ValueError as e:
            return str(e)

        pool_permits = self._subnet_pool[alloc.pool].permit_classes
        if permit_class and pool_permits and permit_class not in pool_permits:
            return f"subnet {subnet} requested for class '{permit_class}' exists in pool {alloc.pool} with class restrictions {pool_permits}"
        elif pool_permits and not permit_class:
            return f"subnet {subnet} requested exists in pool {alloc.pool} with class restrictions {pool_permits}"
        self._commit_allocations([alloc])
        return ""

    def _commit_allocations(self, allocations: List[_SubnetAllocation]) -> List[ipaddress.IPv4Network]:
        """ Update allocator state for allocated subnets. """
        final_subnets = []
        affected_pools = set()
        for allocation in allocations:
            i = self._subnet_unallocated[allocation.pool].index(allocation.origin_subnet)
            self._subnet_unallocated[allocation.pool] = (
                self._subnet_unallocated[allocation.pool][:i] +
                allocation.subnets_parts_remaining +
                self._subnet_unallocated[allocation.pool][i + 1:]
            )
            self._subnet_allocations[allocation.pool].extend(allocation.subnet_parts_taken)
            final_subnets.extend(allocation.subnet_parts_taken)
            affected_pools.add(allocation.pool)
        for pool in affected_pools:
            self._subnet_allocations[pool] = coalesce_subnets(self._subnet_allocations[pool])
            self._subnet_unallocated[pool] = coalesce_subnets(self._subnet_unallocated[pool])
        return final_subnets

    def get_unallocated_subnets(self) -> List[ipaddress.IPv4Network]:
        return [subnet for pool in self._subnet_unallocated.values() for subnet in pool]


def _collect_subnet_allocations(network_config: NetworkCfgDoc) -> dict:
    """ returns a dict of class to subnets allocated to that class """
    sw_vlan_mapping = {
        # v1
        ("AW", "prefix"): K_POOL_CLASS_MX,
        ("BR", "prefix"): K_POOL_CLASS_SX,
        ("BR", "system_prefix"): K_POOL_CLASS_SY,
        # v2
        ("LF", "prefix"): K_POOL_CLASS_MX,
        ("LF", "swarmx_prefix"): K_POOL_CLASS_SX,
        ("LF", "system_prefix"): K_POOL_CLASS_SY,
    }

    subnets = {
        K_POOL_CLASS_MX: [],
        K_POOL_CLASS_SX: [],
        K_POOL_CLASS_SY: [],
        K_POOL_CLASS_XC: [],
        K_POOL_CLASS_VIP: [],
    }
    # switch vlan prefixes
    for s in network_config.raw().get("switches", list()):
        for prefix_key in ("prefix", "swarmx_prefix", "system_prefix",):
            if prefix_key in s:
                subnets[sw_vlan_mapping[(s['tier'], prefix_key)]].append(ipaddress.ip_network(s[prefix_key]))

    # VIP prefix - has to be inferred from the system VIPs
    vips = []
    for system in network_config.raw().get("systems", []):
        if "vip" in system:
            vips.append(ipaddress.ip_network(system.get("vip")))
    data_vip = network_config.raw().get("cluster_data_vip", {}).get("vip")
    if data_vip:
        vips.append(ipaddress.ip_network(data_vip))
    subnets[K_POOL_CLASS_VIP] = vips

    # xconn prefixes
    xconn = network_config.raw().get("xconnect", {})
    for c in xconn.get("connections", []):
        if "prefix" not in c:
            continue
        subnets[K_POOL_CLASS_XC].append(ipaddress.ip_network(c['prefix']))
    return subnets

ENV_POOL_KEY = "subnet_pools"
RESV_POOL_KEY = "reserved_subnet_pools"

class SubnetReservationManager:
    """
    Manages reservations of subnets from the environment.subnet_pool. It's not terribly useful for switch vlan subnet
    allocations since the allocations are self-evident in the switch config, but it's somewhat useful for blocking off
    subnets for VIPS and xconnects.

    Updates the `reserved_subnet_pools` in the network config by allocating requests from subnets in the
    `reserved_subnet_pools` pools and if not enough subnets are available, allocating new subnets from the
    `environment.subnet_pools` pools to a reserved_subnet_pools entry first.
    """

    def __init__(self, network_cfg: NetworkCfgDoc):
        self._network_cfg = network_cfg

        # track environment network pools - this class changes these as they are user-input
        global_pools = []
        for extent in self._network_cfg.raw()["environment"][ENV_POOL_KEY]:
            global_pools.append(SubnetPool.fromdict(extent))

        # track system reservation pools - this class can amend these
        self._class_to_system_pool = {}
        for extent in self._network_cfg.raw().get(RESV_POOL_KEY, []):
            pool = SubnetPool.fromdict(extent)
            assert len(pool.permit_classes) == 1, "only one class per pool is supported"
            assert pool.permit_classes[0] not in self._class_to_system_pool, f"duplicate class {pool.permit_classes[0]} in {RESV_POOL_KEY}"
            self._class_to_system_pool[pool.permit_classes[0]] = pool

        self._cluster_subnet_pools_allocator = SubnetAllocator(
            global_pools, [subnet for pool in self._class_to_system_pool.values() for subnet in pool.subnets]
        )

        self._reserved_subnet_pools_allocator = SubnetAllocator(
            list(self._class_to_system_pool.values()),
            list(itertools.chain.from_iterable(_collect_subnet_allocations(self._network_cfg).values())),
        )

    def allocate(
            self,
            prefixlen: int,
            count: int,
            pool_class: str = "",
            reserve_prefixlen: int = 0,
    ) -> List[ipaddress.IPv4Network]:
        """
        Allocate subnets of the given prefixlen from the unallocated subnets in the pool.
        Arguments:
            prefixlen: The prefix length of the subnets to allocate
            count: The number of subnets to allocate
            pool_class: The class of the pool to allocate from. If empty, allocate only from pools with no class
            reserve_prefixlen: The prefix length of the subnets to allocate from the reservation pool. If 0, use the
                same prefixlen as the requested subnets.
        Returns:
            A list of allocated subnets.
        """
        allocatable, commit = self._reserved_subnet_pools_allocator.allocate_uncommitted(prefixlen, count, pool_class)
        if len(allocatable) == count:
            return commit()

        # if not enough subnets were allocated, try to allocate from the reservation pool
        required_reservation_count = count - len(allocatable)
        if reserve_prefixlen < 1:
            reserve_prefixlen = prefixlen
        elif reserve_prefixlen < prefixlen:
            # calculate the number of reserve_prefixlen subnets required to fit the missing prefixlen subnets
            reserve_count = (2 ** (32 - prefixlen)) * required_reservation_count
            reserve_count /= (2 ** (32 - reserve_prefixlen))
            required_reservation_count = math.ceil(reserve_count)
        elif reserve_prefixlen > prefixlen:
            raise ValueError(f"reserve_prefixlen {reserve_prefixlen} must be <= prefixlen {prefixlen}")

        reservable, commit = self._cluster_subnet_pools_allocator.allocate_uncommitted(reserve_prefixlen, required_reservation_count, pool_class)
        if len(reservable) < required_reservation_count:
            raise ValueError(
                f"could only allocate {len(reservable)} of {required_reservation_count} required subnets of prefixlen "
                f"{prefixlen} from class '{pool_class}'. Please expand environment.{ENV_POOL_KEY}"
            )
        reservable = commit()
        self._update_reservations(reservable, pool_class)

        rv = self._reserved_subnet_pools_allocator.allocate(prefixlen, count, pool_class)
        assert len(rv) == count, str(
            f"failed to allocate {count} subnets of prefixlen {prefixlen} from "
            f"class '{pool_class}' after expanding {RESV_POOL_KEY}"
        )
        return sorted(rv, key=lambda x: x.network_address)

    def allocate_subnet(self, subnet: ipaddress.IPv4Network, permit_class: str = "") -> str:
        """ Allocate a specific subnet from the pool. Ensures that the subnet is in the pool and not already allocated.
        Returns
            non-empty string if error, empty string if success
        """
        error = self._reserved_subnet_pools_allocator.allocate_subnet(subnet, permit_class)
        if not error or "subnet not in any pool" != error:
            return error

        # try to allocate from the environment network extents
        error = self._cluster_subnet_pools_allocator.allocate_subnet(subnet, permit_class)
        if error:
            return error

        self._update_reservations([subnet], permit_class)
        return self._reserved_subnet_pools_allocator.allocate_subnet(subnet, permit_class)

    def _update_reservations(self, subnets: List[ipaddress.IPv4Network], pool_class: str):
        # now update our state with the allocated subnets
        reservation_pool = self._class_to_system_pool.get(pool_class)
        if reservation_pool:
            reservation_pool.subnets = coalesce_subnets(reservation_pool.subnets + subnets)
        else:
            self._class_to_system_pool[pool_class] = SubnetPool(
                coalesce_subnets(subnets), [pool_class], f"system {pool_class} reservations"
            )

        # sync the new system reservation pool to the network config
        self._network_cfg.raw()[RESV_POOL_KEY] = [
            self._class_to_system_pool[class_key].todict()
            for class_key in sorted(self._class_to_system_pool.keys())
        ]

        # reinitialize the subnet allocator with the new pools and previous allocations
        self._reserved_subnet_pools_allocator = SubnetAllocator(
            list(self._class_to_system_pool.values()),
            itertools.chain.from_iterable(self._reserved_subnet_pools_allocator._subnet_allocations.values()),
        )