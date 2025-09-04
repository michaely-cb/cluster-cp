import bisect
import ipaddress
import json
import pathlib

import pytest

from deployment_manager.network_config.common.context import K_POOL_CLASS_VIP, K_POOL_CLASS_XC, K_POOL_CLASS_MX, \
    K_POOL_CLASS_SX, K_POOL_CLASS_NONE
from deployment_manager.network_config.deployment.schema_migration import find_subnet_pools
from deployment_manager.network_config.placer.allocator import SubnetPool, SubnetAllocator, coalesce_subnets, \
    SubnetReservationManager, find_unallocated_subnets
from deployment_manager.network_config.placer.allocator import RESV_POOL_KEY, ENV_POOL_KEY
from deployment_manager.network_config.common.context import NetworkCfgDoc

def test_coalesce_sorted_nonoverlapping_subnets():
    val = coalesce_subnets([
        # aligned
        ipaddress.ip_network("10.0.0.18/31"),
        ipaddress.ip_network("10.0.0.16/31"),

        # out of order, aligned
        ipaddress.ip_network("10.0.0.0/31"),
        ipaddress.ip_network("10.0.0.2/31"),
        ipaddress.ip_network("10.0.0.4/31"),
        ipaddress.ip_network("10.0.0.6/31"),

        # unaligned
        ipaddress.ip_network("10.0.0.34/31"),
        ipaddress.ip_network("10.0.0.36/31"),
    ])
    assert val == [ipaddress.ip_network("10.0.0.0/29"),
                   ipaddress.ip_network("10.0.0.16/30"),
                   ipaddress.ip_network("10.0.0.34/31"),
                   ipaddress.ip_network("10.0.0.36/31")]


def test_find_unallocated_subnets():
    supernet = ipaddress.ip_network("10.0.0.0/24")
    allocated_subnets = [
        ipaddress.ip_network("10.0.0.64/26"),
        ipaddress.ip_network("10.0.0.128/27"),
        ipaddress.ip_network("10.0.0.176/28"),
    ]
    unallocated = find_unallocated_subnets(supernet, allocated_subnets)

    expected_unallocated = [
        ipaddress.ip_network("10.0.0.0/26"),
        ipaddress.ip_network("10.0.0.160/28"),
        ipaddress.ip_network("10.0.0.192/26"),
    ]

    assert unallocated == expected_unallocated, f"Expected {expected_unallocated}, but got {unallocated}"

    full = coalesce_subnets(sorted(allocated_subnets + expected_unallocated))
    assert len(full) == 1, f"Expected 1 subnet, but got {len(full)}"
    assert full[0] == supernet, f"Expected {supernet}, but got {full[0]}"

    # test no unallocated subnets
    unallocated = find_unallocated_subnets(supernet, [])
    assert unallocated == [supernet], f"Expected {supernet}, but got {unallocated}"

    # test all unallocated subnets
    unallocated = find_unallocated_subnets(supernet, sorted(allocated_subnets + expected_unallocated))
    assert unallocated == [], f"Expected [], but got {unallocated}"


def test_network_subnet_allocator():
    # 3 pools with different classes
    pools = [
        SubnetPool(
            subnets=[ipaddress.ip_network("10.0.0.0/22")],
            description="classless"
        ),
        SubnetPool(
            subnets=[ipaddress.ip_network("10.1.0.0/24")],
            description="class A",
            permit_classes=["a"],
        ),
        SubnetPool(
            subnets=[ipaddress.ip_network("10.2.0.0/24")],
            description="class A B",
            permit_classes=["a", "b"],
        ),

        SubnetPool(
            subnets=[ipaddress.ip_network("10.3.0.0/24")],
            description="class C",
            permit_classes=["c"],
        ),
    ]
    allocated_subnets = [
        ipaddress.ip_network("10.0.0.0/25"),  # Disjoint, member of classless
        ipaddress.ip_network("10.1.0.0/26"),  # Adjacent to next, member of class A
        ipaddress.ip_network("10.1.0.64/26"),
    ]

    allocator = SubnetAllocator(pools, allocated_subnets)

    def verify_subnets(subnets, expected_prefixlen):
        assert all(subnet.prefixlen == expected_prefixlen for subnet in new_subnets)
        assert all(subnet not in allocated_subnets for subnet in new_subnets)
        for subnet in new_subnets:
            # Ensure the new subnets are within the pool
            assert any(subnet.subnet_of(sn) for pool in pools for sn in pool.subnets)
            # Ensure no overlap with existing allocations
            for already_allocated in allocated_subnets:
                assert not subnet.overlaps(already_allocated)


    # verify that we can't commit an old uncommitted allocation
    _, commit_a = allocator.allocate_uncommitted(prefixlen=26, count=2, pool_class="a")
    _, commit_b = allocator.allocate_uncommitted(prefixlen=26, count=2, pool_class="c")
    with pytest.raises(AssertionError) as e:
        commit_a()

    # Verify the allocated subnets
    new_subnets = allocator.allocate(prefixlen=26, count=2)
    assert len(new_subnets) == 2
    verify_subnets(new_subnets, 26)

    # allocate /31's in class b
    new_subnets = allocator.allocate(prefixlen=31, count=128, pool_class="b")
    assert len(new_subnets) == 128
    assert all(sn.subnet_of(ipaddress.ip_network("10.2.0.0/24")) for sn in new_subnets)
    verify_subnets(new_subnets, 31)

    # allocate /31's in class b, which spills over to the classless pool
    new_subnets = allocator.allocate(prefixlen=31, count=8, pool_class="b")
    assert len(new_subnets) == 8
    assert all(sn.subnet_of(ipaddress.ip_network("10.0.0.0/22")) for sn in new_subnets)
    verify_subnets(new_subnets, 31)

    # try to allocate classless 24's but exceed the limit
    new_subnets = allocator.allocate(prefixlen=24, count=3)
    assert len(new_subnets) == 2
    verify_subnets(new_subnets, 24)

    # try re-allocating, should be 0
    new_subnets = allocator.allocate(prefixlen=24, count=3)
    assert len(new_subnets) == 0

    # try allocating a singular subnet
    error = allocator.allocate_subnet(ipaddress.ip_network("10.3.0.0/25"), "c")
    assert not error
    # try allocating a singular subnet that overlaps
    error = allocator.allocate_subnet(ipaddress.ip_network("10.3.0.0/27"), "c")
    assert error
    # try allocating a singular subnet that is not in the pool
    error = allocator.allocate_subnet(ipaddress.ip_network("10.254.0.0/24"))
    assert error
    # try allocating a singular subnet that doesn't match the class
    error = allocator.allocate_subnet(ipaddress.ip_network("10.3.0.128/25"), "d")
    assert error


def test_subnet_reservation_manager():
    # Create a minimal network config with environment extents and system reserved extents
    cluster_prefix = ipaddress.ip_network("10.0.0.0/16")
    cluster_pool = ipaddress.ip_network("10.0.16.0/20")

    network_config = {
        "environment": {
            "cluster_prefix": str(cluster_prefix),
            ENV_POOL_KEY: [
                {
                    "description": "cluster prefix",
                    "subnets": [str(cluster_pool)]
                }
            ]
        },
        RESV_POOL_KEY: [
            {
                "description": "system vip reservations",
                "subnets": ["10.0.17.0/24"],
                "permit_classes": [K_POOL_CLASS_VIP]
            },
            {
                "description": "system xconnect reservations",
                "subnets": ["10.0.18.0/24"],
                "permit_classes": [K_POOL_CLASS_XC]
            }
        ],
        "switches": [{"tier": "LF", "address": "10.0.19.0/24"}],
        "systems": [],
        "xconnect": {
            "connections": [
                {"prefix": "10.0.18.0/30"}
            ]
        },
    }

    nw_doc = NetworkCfgDoc(network_config)
    mgr = SubnetReservationManager(nw_doc)

    # Test allocation from existing pool
    vip_subnets = mgr.allocate(prefixlen=32, count=10, pool_class=K_POOL_CLASS_VIP)
    assert len(vip_subnets) == 10
    assert all(subnet.prefixlen == 32 for subnet in vip_subnets)
    assert all(subnet.subnet_of(ipaddress.ip_network("10.0.17.0/24")) for subnet in vip_subnets)

    # Exhaust VIP pool and force allocation from environment pool
    remaining_vips = mgr.allocate(prefixlen=28, count=20, pool_class=K_POOL_CLASS_VIP)
    assert len(remaining_vips) == 20

    # Verify that reserved_subnet_pools was updated with new allocations
    updated_vip_pools = [
        pool for pool in network_config[RESV_POOL_KEY]
        if "vip" in pool["permit_classes"]
    ]
    assert len(updated_vip_pools) == 1
    assert len(updated_vip_pools[0]["subnets"]) > 1  # Should have added new subnets

    # Test specific subnet allocation
    specific_subnet = ipaddress.ip_network("10.0.21.0/24")
    error = mgr.allocate_subnet(specific_subnet, K_POOL_CLASS_MX)
    assert not error, "should not have been allocated to MX pool"
    error = mgr.allocate_subnet(specific_subnet, K_POOL_CLASS_MX)
    assert error, "should have been allocated to MX pool"
    error = mgr.allocate_subnet(ipaddress.ip_network("10.0.21.128/25"), K_POOL_CLASS_SX)
    assert error, "should have been allocated to MX pool"
    error = mgr.allocate_subnet(ipaddress.ip_network("10.1.0.0/25"))
    assert error, "should not be available for allocation - outside cluster prefix"

    sx_subnets = mgr.allocate(25, 1, K_POOL_CLASS_SX)
    assert sx_subnets, "should have been allocated to SX pool"

    # Test allocation failure due to insufficient space
    with pytest.raises(ValueError, match="could only allocate"):
        # Try to allocate more than available in the whole network
        mgr.allocate(prefixlen=22, count=5, pool_class=K_POOL_CLASS_MX)

    # try allocating all the remaining subnets
    prefixlen = 21
    for vlan_class in [K_POOL_CLASS_NONE, K_POOL_CLASS_VIP, K_POOL_CLASS_XC, K_POOL_CLASS_SX, K_POOL_CLASS_SX]:
        while prefixlen <= 32:
            try:
                mgr.allocate(prefixlen=prefixlen, count=1, pool_class=vlan_class)
            except ValueError as e:
                prefixlen += 1

    # collect all the reserved_subnets and check they don't overlap
    all_reserved_subnets = sorted([
        ipaddress.ip_network(sn) for pool in network_config[RESV_POOL_KEY] for sn in pool["subnets"]
    ])
    for i in range(len(all_reserved_subnets) - 1):
        assert not all_reserved_subnets[i].overlaps(all_reserved_subnets[i + 1]), \
            f"overlapping subnets: {all_reserved_subnets[i]} and {all_reserved_subnets[i + 1]}"
    assert [cluster_pool] == coalesce_subnets(all_reserved_subnets), "entire cluster_pool should be allocated"

def test_migrate_allocations_real_cluster():
    doc_text = (pathlib.Path(__file__).parent / "assets" / "network_config_mb21.json").read_text()
    doc = json.loads(doc_text)

    pools = find_subnet_pools(doc)
    cluster_prefix = ipaddress.ip_network(doc["environment"]["cluster_prefix"])
    subnets = []
    for pool in pools:
        assert all(subnet.subnet_of(cluster_prefix) for subnet in pool.subnets)
        for subnet in pool.subnets:
            i = bisect.bisect_left(subnets, subnet)
            lo, hi = i - 1, i
            if lo >= 0 and subnets:
                assert not subnet.overlaps(subnets[lo]), f"{subnet} overlaps {subnets[lo]} in pool {pool.description}"
            if hi < len(subnets):
                assert not subnet.overlaps(subnets[hi]), f"{subnet} overlaps {subnets[hi]} in pool {pool.description}"
            subnets.insert(i, subnet)

    doc["environment"][ENV_POOL_KEY] = [
        {"description": "cluster_prefix", "subnets": [cluster_prefix]}
    ]
    doc[RESV_POOL_KEY] = [
        pool.todict() for pool in pools if pool.permit_classes
    ]

    nw_doc = NetworkCfgDoc(doc)
    mgr = SubnetReservationManager(nw_doc)
    # try allocating a few subnets
    subnets = mgr.allocate(prefixlen=31, count=2, pool_class=K_POOL_CLASS_XC)
    assert len(subnets) == 2
    assert all(subnet.prefixlen == 31 for subnet in subnets)