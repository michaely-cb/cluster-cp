import os
import sys

import pytest

sys.path.insert(0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../../deployment/deployment_manager/tools/pb1')))

from ipmi import create_ipmi_client, IPMI, FanSpeedMode


def run_ipmi(ipmi: IPMI, mutate=False):
    bios = ipmi.get_bios_settings()

    if mutate:
        ipmi.update_bios_config_privilege(ipmi._username)
        ipmi.create_or_update_user("readonly", "readonly", "ReadOnly")

    bios = ipmi.get_bios_settings()
    print(bios)
    health = ipmi.get_server_aggregate_health()
    print(health)
    mgmt_mac = ipmi.find_mgmt_mac_address()
    print(mgmt_mac)
    processor = ipmi.get_server_processor_details()
    print(processor)

@pytest.mark.skip
def test_dell_bios():
    # mb-hostio sc-r14ra7-s26
    ipmi = create_ipmi_client("172.28.103.155", "root", "calvin")
    run_ipmi(ipmi)
    ipmi.update_bios_settings({"BootMode":"Bios"})
    ipmi.reboot_server("ForceRestart")
    ipmi.onetimeboot_from_pxe()
    ipmi.reboot_server("ForceRestart")
    ipmi.close()

def test_dell():
    # mb-303 cs303-wse001-sx-sr03
    ipmi = create_ipmi_client("172.28.153.132", "root", "PV4YWMMNYY34")
    run_ipmi(ipmi)
    ipmi.set_fan_speed(FanSpeedMode.HIGH)
    ipmi.set_fan_speed(FanSpeedMode.DEFAULT)
    ipmi.close()

@pytest.mark.skip
def test_supermicro():
    # devtest - sc-r1rb1-s4-ipmi
    ipmi = create_ipmi_client("172.28.8.132", "ADMIN", "Admin123")
    ipmi.create_or_update_user("readonly", "USER_admin123", "ReadOnly")
    run_ipmi(ipmi)
    ipmi.close()


def test_hpe():
    # perfdrop3 - sc-r14ra19-s1-ipmi
    ipmi = create_ipmi_client("172.28.141.32", "ADMIN", "Admin123")
    ipmi.update_bios_settings({"NetworkBootRetry":"Disabled"})
    run_ipmi(ipmi)
    ipmi.close()
