import logging
import os
import pytest
from ipaddress import ip_network

from deployment_manager.tools.powerdns import PowerDNSZoneManager

logger = logging.getLogger(__name__)

"""
Test powerdns API - note this exists to test locally. Not worth the effort to
set up a powerdns and run an integration test against it.

If testing, use colo dns test domain against colo powerdns admin api
"""

TEST_DOMAIN = "test.cerebrascloud.com"
api_url = "http://colo-util-00.cerebrassc.local:9193/"
api_key = os.getenv("DM_POWERDNS_API_KEY")

if not api_key:
    pytest.skip("powerdns test skipped because DM_POWERDNS_API_KEY not set", allow_module_level=True)


def test_powerdns():
    subnet = ip_network("10.253.0.0/24")
    addrs = list(subnet.hosts())

    with PowerDNSZoneManager(api_url, api_key, TEST_DOMAIN) as mgr:
        mgr.add_a_record(f"host0", str(addrs[0]))
        mgr.add_cname_record(f"host0-alias", f"host0")


    with PowerDNSZoneManager(api_url, api_key, TEST_DOMAIN) as mgr:
        # ensure cache hit
        assert not mgr.add_a_record(f"HOST0", str(addrs[0])), "should have a cache entry"
        assert not mgr.add_cname_record(f"HOST0-alias", f"HOST{0}"), "should have a cache entry"

        # update record, ensure updated
        assert mgr.add_a_record(f"host0", str(addrs[1]))

    with PowerDNSZoneManager(api_url, api_key, TEST_DOMAIN) as mgr:
        # ensure cache hit
        assert not mgr.add_a_record(f"host0", str(addrs[1]))

        for i in range(2):
            # ensure non-exist delete OK
            mgr.remove_a_record(f"host{i}")
            mgr.remove_cname_record(f"host{i}-alias")




