import bisect
import ipaddress
import logging
import math
from dataclasses import dataclass
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PrefixSettings:
    prefix: int

    # Dynamic range is the IP range that the DHCP server can issue leases to without prior reservation.
    #
    # Offsets from broadcast address are inclusive so, /24 start=-4, end=-2 expands to .251, .252, .253.
    #
    # Note: During cscfg management IP allocation, the dynamic IP range cannot be used for automatic
    # IP assignment since the dynamic IP range might be in-use by some other device. However, if a device
    # like a system gets a dynamic IP, we can make a reservation in the dynamic range which will then not
    # be re-leased by the DHCP server
    dynamic_start_offset: int
    dynamic_end_offset: int


__PREFIX_SETTINGS = [
    PrefixSettings(prefix=27, dynamic_start_offset=-16, dynamic_end_offset=-3), # 13 reservable, 14 dynamic
    PrefixSettings(prefix=26, dynamic_start_offset=-20, dynamic_end_offset=-3), # 41 reservable, 18 dynamic
    PrefixSettings(prefix=25, dynamic_start_offset=-40, dynamic_end_offset=-9), # 85 reservable, 32 dynamic
    PrefixSettings(prefix=24, dynamic_start_offset=-40, dynamic_end_offset=-9), # 213 reservable, 32 dynamic
    PrefixSettings(prefix=23, dynamic_start_offset=-208, dynamic_end_offset=-9), # 303 reservable, 100 dynamic

    # These prefix lens are not commonly used. Usually at customer sites with non-standard setups.
    # The dynamic range size could be variable, but leave it small and static for simplicity until the need arises
    *[
        PrefixSettings(prefix=prefix, dynamic_start_offset=-200, dynamic_end_offset=-9)
        for prefix in range(22, 15, -1)
    ]
]
PREFIX_SETTINGS = {s.prefix: s for s in __PREFIX_SETTINGS}


def _check_supported_prefixlen(prefixlen: int):
    if prefixlen not in PREFIX_SETTINGS:
        raise ValueError(f"subnet mask size ({prefixlen}) must be in "
                         f"{', '.join([str(i) for i in PREFIX_SETTINGS.keys()])}")


def _reserved_dynamic_ips(sn: ipaddress.IPv4Network) -> List[ipaddress.IPv4Address]:
    prefix_setting = PREFIX_SETTINGS.get(sn.prefixlen)
    if not prefix_setting:
        logger.warning(f"not reserving dynamic IPs for subnet {sn}: unhandled prefixlen: {sn.prefixlen}")
        return []
    return [
        ipaddress.IPv4Address(i)
        for i in range(
            int(sn.broadcast_address + prefix_setting.dynamic_start_offset),
            int(sn.broadcast_address + prefix_setting.dynamic_end_offset)
        )
    ]


class NetworkExtent:
    def __init__(self, subnets: List[ipaddress.IPv4Network], prefixlen: int=0):
        """
        Args:
            subnets: subnets from which network sized prefixlen can be allocated from
            prefixlen: default prefix len assigned to a subnet assigned from this extent
        """
        self.subnets: List[ipaddress.IPv4Network] = subnets
        self.prefixlen = prefixlen

    def __contains__(self, item):
        if isinstance(item, (ipaddress.IPv4Network,)):
            for sn in self.subnets:
                if sn.supernet_of(item):
                    return True
        if isinstance(item, (ipaddress.IPv4Address,)):
            for sn in self.subnets:
                if item in sn:
                    return True
        return False


class NetworkExtentAllocation(NetworkExtent):

    def __init__(self, subnets: List[ipaddress.IPv4Network], prefixlen: int):
        super().__init__(subnets, prefixlen)
        self.free_subnets: Dict[ipaddress.IPv4Network, List[ipaddress.IPv4Network]] = {
            network: [network] for network in self.subnets
        }

    def set_alloc(self, alloc: ipaddress.IPv4Network) -> bool:
        """ allocate alloc to this extent, returning true if the alloc was contained in this extent """
        alloc_nw, remaining_segments = None, None
        for network, segments in self.free_subnets.items():
            for i, segment in enumerate(segments):
                if segment.supernet_of(alloc):
                    alloc_nw = network
                    remainder = sorted(list(segment.address_exclude(alloc)))
                    remaining_segments = segments[0:i] + remainder + segments[i+1:]
                    break
            if alloc_nw is not None:
                break
        if alloc_nw is not None and remaining_segments is not None:
            self.free_subnets[alloc_nw] = remaining_segments
            return True
        return False

    def next_alloc(self, prefixlen=0) -> Optional[ipaddress.IPv4Network]:
        """ find the next alloc of length prefixlen and return it or None if no segment can be found with this length """
        if prefixlen < 1 and self.prefixlen > 0:
            prefixlen = self.prefixlen
        elif prefixlen < 1:
            raise ValueError("missing required arg 'prefixlen'")

        alloc, alloc_nw, remaining_segments = None, None, None
        for network in self.subnets:
            for i, segment in enumerate(self.free_subnets.get(network, [])):
                if segment.prefixlen <= prefixlen:
                    alloc = next(segment.subnets(new_prefix=prefixlen))
                    remaining_segments = sorted(list(segment.address_exclude(alloc)))
                    alloc_nw = network
                    remaining_segments = self.free_subnets[network][0:i] + remaining_segments + self.free_subnets[network][i+1:]
                    break
            if alloc_nw is not None:
                break
        if alloc is None:
            return None
        self.free_subnets[alloc_nw] = remaining_segments
        return alloc


class IPAllocator:
    """
    Container for IP allocation state of a profile. `next_*` methods find free IPs/networks within a subnet.

    Assumes an allocation strategy where a subnet is assigned per rack and within the subnet, there is a range of
    dynamically assigned IPs and ranges of statically assigned IPs.
    """

    def __init__(self,
                 parentnet: ipaddress.IPv4Network,
                 default_prefixlen: int,
                 named_extents: Dict[str, NetworkExtent],
                 asn_start: int,
                 asn_end: int,
                 used_subnet: List[ipaddress.IPv4Network],
                 used_asn: List[int],
                 used_ip: List[ipaddress.IPv4Address]):
        # config related
        self.parentnet = parentnet
        self.subnet_prefixlen = default_prefixlen
        if default_prefixlen < parentnet.prefixlen:
            raise ValueError(
                f"subnet mask size ({default_prefixlen}) cannot be larger than parentnet size ({self.parentnet})")
        _check_supported_prefixlen(default_prefixlen)

        self._named_extents: Dict[str, NetworkExtentAllocation] = {}
        default_extent = NetworkExtentAllocation([parentnet], self.subnet_prefixlen)
        for name, extent in named_extents.items():
            if name == "default":
                raise ValueError("invalid named extent: name 'default' is reserved and cannot be specified")
            self._named_extents[name] = NetworkExtentAllocation(extent.subnets, extent.prefixlen)
            for subnet in extent.subnets:
                if not default_extent.set_alloc(subnet):
                    raise ValueError(f"invalid named extent: {name} contained subnet {subnet} "
                                     f"which was not contained in {parentnet}")

        self._named_extents["default"] = default_extent

        self.asn_start = asn_start
        self.asn_end = asn_end
        self.used_asn = sorted(used_asn)

        # reserve IPs for subnets - network addr, broadcast addr and
        # dynamic ranges if this is the default network extent
        ips = used_ip.copy()
        for sn in used_subnet:
            ips.append(sn.network_address)
            ips.append(sn.broadcast_address)
            for name, extent in self._named_extents.items():
                if sn in extent:
                    extent.set_alloc(sn)
                    if name in ("default", "deploy_mgmt"):
                        ips.extend(_reserved_dynamic_ips(sn))
                    break
            else:
                raise ValueError(f"subnet {sn} should have fallen into some network extent. "
                                 f"Is this used subnet a child of {self.parentnet}?")

        self.used_ip = sorted(list(set(ips)))

    def next_asn(self) -> int:
        """
        Returns:
            Next free ASN
        """
        asn = self.asn_start
        # find the next free asn, slow but there's only a few...
        i = 0
        while i < len(self.used_asn) and asn > self.used_asn[i]:
            i += 1
        while i < len(self.used_asn):
            if asn == self.used_asn[i]:
                i += 1
                asn += 1
            else:
                break
        if asn >= self.asn_end:
            raise ValueError(f"ASNs exhausted: [{self.asn_start}, {self.asn_end}]")
        self.used_asn.insert(i, asn)
        return asn

    def next_subnet(self, extent_name="default", prefixlen=None) -> ipaddress.IPv4Network:
        """
        Finds the next free subnet of size subnet_prefixlen within the requested pool. Auto-reserves the dynamic IPs.
        Assumes subnets are assigned sequentially within an extent
        Args:
            extent_name: Name of the extent to allocate the subnet from
            prefixlen: optional prefixlen of requested subnet
        Returns:
            Allocated subnet
        Raises:
            ValueError: if no subnet available
        """
        if prefixlen:
            _check_supported_prefixlen(prefixlen)
        else:
            prefixlen = self.subnet_prefixlen

        extent = self._named_extents.get(extent_name)
        if not extent:
            raise ValueError(f"unknown extent name {extent_name}")

        subnet = extent.next_alloc(prefixlen)
        if not subnet:
            raise ValueError(f"network extent {extent_name} is out of free space to allocate /{prefixlen} subnets")

        used_ips = [subnet.network_address]
        if extent_name == "default":
            used_ips.extend(_reserved_dynamic_ips(subnet))
        used_ips.append(subnet.broadcast_address)

        insertat = bisect.bisect_left(self.used_ip, subnet.network_address)
        self.used_ip = self.used_ip[0:insertat] + used_ips + self.used_ip[insertat:]

        return subnet

    def next_ip(self, subnet: ipaddress.IPv4Network, preferred_offset=0) -> ipaddress.IPv4Address:
        """
        Allocate an IP from a subnet
        Args:
            subnet: subnet to allocate from
            preferred_offset: preferred offset within the subnet. A negative value will search before the given offset
                from the broadcast address while a positive value searches after the given offset from the network addr

        Returns:
            Allocated IP
        Raises:
            ValueError: if there's no available IPs starting from the given offset
        """
        if not self.parentnet.supernet_of(subnet):
            raise ValueError(f"attempted to assign from a subnet that was not meant for allocation: {subnet}")

        if abs(preferred_offset) > math.pow(2, 32 - subnet.prefixlen):
            raise ValueError(f"preferred_offset ({preferred_offset}) must not be > network size (/{subnet.prefixlen})")

        if preferred_offset < 0:
            lo_i = bisect.bisect_left(self.used_ip, subnet.network_address)
            target = subnet.broadcast_address + preferred_offset
            target_i = bisect.bisect_left(self.used_ip, target)
            while target_i > lo_i and self.used_ip[target_i] == target:
                target_i -= 1
                target -= 1
                if target <= subnet.network_address:
                    raise ValueError(f"subnet {subnet} exhausted starting from preferred offset {preferred_offset}")
        else:
            hi_i = bisect.bisect_left(self.used_ip, subnet.broadcast_address)
            target = subnet.network_address + preferred_offset
            target_i = bisect.bisect_left(self.used_ip, target)
            while target_i < hi_i and self.used_ip[target_i] == target:
                target_i += 1
                target += 1
                if target >= subnet.broadcast_address:
                    raise ValueError(f"subnet {subnet} exhausted starting from preferred offset {preferred_offset}")
        bisect.insort(self.used_ip, target)
        return target

    def next_extent_ip(self, extent_name: str) -> ipaddress.IPv4Address:
        """ Get the next free IP in an extent, in the laziest way possible """
        extent = self._named_extents.get(extent_name)
        if not extent:
            raise ValueError(f"unknown extent name {extent_name}")

        for subnet in extent.subnets:
            try:
                return self.next_ip(subnet)
            except ValueError:
                continue
        raise ValueError(f"named extent {extent_name} has no free IPs to allocate")
