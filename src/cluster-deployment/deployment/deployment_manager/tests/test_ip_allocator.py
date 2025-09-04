import bisect
import ipaddress
from ipaddress import ip_network, ip_address

import pytest

from deployment_manager.tools.ip_allocator import IPAllocator, NetworkExtent, NetworkExtentAllocation


def test_network_extent_allocation():
    alloc = NetworkExtentAllocation([
        ipaddress.IPv4Network("10.0.0.0/24"),
        ipaddress.IPv4Network("10.0.8.0/23"),
    ], 25)
    assert alloc.set_alloc(ipaddress.IPv4Network("10.0.0.128/26"))
    assert alloc.next_alloc() == ipaddress.IPv4Network("10.0.0.0/25")
    assert ipaddress.IPv4Network("10.0.0.0/25") in alloc

    assert alloc.next_alloc() == ipaddress.IPv4Network("10.0.8.0/25")
    assert alloc.next_alloc() == ipaddress.IPv4Network("10.0.8.128/25")
    assert alloc.next_alloc() == ipaddress.IPv4Network("10.0.9.0/25")
    assert alloc.next_alloc() == ipaddress.IPv4Network("10.0.9.128/25")
    assert alloc.next_alloc() is None
    assert alloc.next_alloc(prefixlen=26) == ipaddress.IPv4Network("10.0.0.192/26")


def test_ip_allocator():
    named_extents = {
        "loopback": NetworkExtent([ip_network("10.1.0.0/25")], prefixlen=25),
        "deploy_mgmt": NetworkExtent([ip_network("10.1.0.128/25"), ip_network("10.1.1.0/25")], prefixlen=25),
        "reserved": NetworkExtent([ip_network("10.1.2.0/25"), ip_network("10.1.3.0/25")], prefixlen=25),
        "uplinks": NetworkExtent([ip_network("10.1.2.128/28")], prefixlen=31),
    }

    alloc = IPAllocator(
        parentnet=ip_network("10.1.0.0/21"),  # last IP: 10.1.7.255
        named_extents=named_extents,
        default_prefixlen=24,
        asn_start=300,
        asn_end=308,
        used_subnet=[ip_network("10.1.4.0/25")],
        used_asn=[200, 300, 400],
        used_ip=[ip_address("10.1.0.0"), ip_address("10.1.2.1")]
    )

    for i in range(5, 8):
        sn = alloc.next_subnet()
        assert sn == ip_network(f"10.1.{i}.0/24")
    with pytest.raises(ValueError):
        alloc.next_subnet()

    for i in range(7):
        asn = alloc.next_asn()
        assert alloc.asn_start < asn < alloc.asn_end
    with pytest.raises(ValueError):
        alloc.next_asn()

    ips_alloced = 2
    for i in range(1, 255):
        addr = ip_address(f"10.1.5.{i}")
        if addr not in alloc.used_ip:
            if i % 2 == 1:
                bisect.insort(alloc.used_ip, addr)
                ips_alloced += 1
        else:
            ips_alloced += 1

    sn = ip_network("10.1.5.0/24")
    ip = alloc.next_ip(sn)
    assert ip == ip_address("10.1.5.2")
    ip = alloc.next_ip(sn)
    assert ip == ip_address("10.1.5.4")
    ip = alloc.next_ip(sn, preferred_offset=-1)
    assert ip == ip_address("10.1.5.254")
    ip = alloc.next_ip(sn, preferred_offset=-1)
    assert ip == ip_address("10.1.5.252")
    ips_alloced += 4

    for i in range(1, (256 - ips_alloced) + 1):
        assert alloc.next_ip(sn) in sn

    assert sorted(alloc.used_ip) == alloc.used_ip

    with pytest.raises(ValueError) as e:
        alloc.next_ip(sn)
    assert "exhausted" in str(e.value)

    with pytest.raises(ValueError) as e:
        alloc.next_ip(sn, preferred_offset=-1)
    assert "exhausted" in str(e.value)

    loopback = alloc.next_extent_ip("loopback")
    assert str(loopback) == "10.1.0.1"
