import ipaddress

import pytest
from django.test.testcases import TestCase

from deployment_manager.db import device_props as props
from deployment_manager.db.device_props import expand_prop_dict, prop_bios_vendor, prop_location_position
from deployment_manager.db.models import DeploymentProfile, Device, DeploymentDeviceTypes, DeploymentDeviceRoles, \
    get_props_values, get_devices_with_prop, HealthState
from deployment_manager.tools.pb1.health_check import (
    filter_degraded_devices,
    filter_critical_devices,
    update_device_health, get_device_health_all)


@pytest.mark.skip
class DbTest(TestCase):
    """
    Django models tests. Should not be run through pytest as they require additional setup steps
    which are launched through django's manage script. Run make -C $GITTOP/src/cluster_deployment/deployment unittest
    to execute
    """

    def setUp(self):
        dp = DeploymentProfile.create("test", "test", True)
        Device.add("sys0", DeploymentDeviceTypes.SYSTEM, DeploymentDeviceRoles.SYSTEM, dp)
        Device.add("sys1", DeploymentDeviceTypes.SYSTEM, DeploymentDeviceRoles.SYSTEM, dp)
        Device.add("sr0", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.MANAGEMENT, dp)
        d = Device.add("sr1", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.MEMORYX, dp)
        d.set_prop(props.prop_management_info_ip, "172.168.0.1")
        d.set_prop(props.prop_location_rack, "wse001")
        d = Device.add("sr2", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.ACTIVATION, dp)
        d.set_prop(props.prop_management_info_ip, "172.168.0.2")
        d.set_prop(props.prop_location_rack, "wse001")
        d = Device.add("sw0", DeploymentDeviceTypes.SWITCH, DeploymentDeviceRoles.SWARMX, dp)
        d.set_prop(props.prop_management_info_ip, "172.168.0.3")
        d.set_prop(props.prop_subnet_info_subnet, "172.168.0.0/25")
        d.set_prop(props.prop_subnet_info_asn, "6500.651")
        d.set_prop(props.prop_location_rack, "wse001")
        d = Device.add("sw1", DeploymentDeviceTypes.SWITCH, DeploymentDeviceRoles.LEAF, dp)
        d.set_prop(props.prop_vendor_name, "AR")
        d.set_prop(props.prop_management_info_ip, "172.168.0.4")
        d.set_prop(props.prop_management_credentials_user, "admin")
        d.set_prop(props.prop_management_credentials_password, "admin000")
        d.set_prop(props.prop_location_rack, "wse001")
        d = Device.add("sw2", DeploymentDeviceTypes.SWITCH, DeploymentDeviceRoles.SPINE, dp)
        d.set_prop(props.prop_vendor_name, "AR")

    def test_deployment_profile(self):
        dp = DeploymentProfile.get_profile("test")
        assert len(dp.systems()) == 2
        assert len(dp.servers()) == 3
        assert len(dp.switches()) == 3

    def test_devices_duplicates_not_allowed(self):
        dp = DeploymentProfile.get_profile("test")
        with self.assertRaises(ValueError):
            Device.add("sr2", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.MEMORYX, dp)
        Device.add("sr2", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.ACTIVATION, dp)
        assert len(dp.servers()) == 3

    def test_devices_get_servers(self):
        sr = Device.get_servers("test", device_role=[DeploymentDeviceRoles.MEMORYX, DeploymentDeviceRoles.MANAGEMENT])
        assert len(sr) == 2

        sr = Device.get_servers("test", device_role=[DeploymentDeviceRoles.MEMORYX, DeploymentDeviceRoles.MANAGEMENT],
                                server_name="sr0")
        assert len(sr) == 1

        sr = Device.get_servers("test", device_role=DeploymentDeviceRoles.MEMORYX, server_name="sr0")
        assert len(sr) == 0

        sr = Device.get_servers("test", device_role=DeploymentDeviceRoles.MEMORYX)
        assert len(sr) == 1

        sr = Device.get_servers("test", device_role=DeploymentDeviceRoles.MEMORYX, server_name=["sr0", "sr1"])
        assert len(sr) == 1

    def test_devices_update_props(self):
        sr = Device.get_servers("test", device_role=[DeploymentDeviceRoles.MANAGEMENT])
        assert len(sr) == 1
        sr = sr[0]
        sr.batch_update_properties(expand_prop_dict({
            prop_bios_vendor: "AR",
            prop_location_position: "10",
        }))

        pa = sr.get_prop_attr_dict()
        assert pa[prop_bios_vendor.name][prop_bios_vendor.attr] == "AR"
        assert pa[prop_location_position.name][prop_location_position.attr] == "10"

        sr.batch_update_properties(expand_prop_dict({
            prop_bios_vendor: None,
            prop_location_position: "11",
        }))
        pa = sr.get_prop_attr_dict()
        assert pa.get(prop_bios_vendor.name, {}).get(prop_bios_vendor.attr) is None
        assert pa[prop_location_position.name][prop_location_position.attr] == "11"

        sr.batch_update_properties(expand_prop_dict({
            prop_bios_vendor: "DL",
        }))
        pa = sr.get_prop_attr_dict()
        assert pa[prop_bios_vendor.name][prop_bios_vendor.attr] == "DL"

    def test_get_props(self):
        ips = get_props_values("test", props.prop_management_info_ip)
        assert len(ips) == 4
        assert isinstance(ips[0], ipaddress.IPv4Address)

        asns = get_props_values("test", props.prop_subnet_info_asn)
        assert len(asns) == 1
        assert isinstance(asns[0], props.ASN)

        sn = get_props_values("test", props.prop_subnet_info_subnet)
        assert len(sn) == 1
        assert isinstance(sn[0], ipaddress.IPv4Network)

        ds = get_devices_with_prop("test", props.prop_location_rack, "wse001")
        assert len(ds) == 4

    def test_server_health(self):
        sr0 = Device.get_device("sr0", "test")
        health_records = get_device_health_all(sr0)

        # initial state should have all status == "UNKNOWN"
        for _, status in health_records.items():
            assert status["status"] == "UNKNOWN"

        # should have 3 degraded devices
        assert filter_degraded_devices(Device.get_all("test", device_type="SR"), "SR").count() == 3
        update_device_health("test", "sr1", "ipmi_access", HealthState.OK)
        update_device_health("test", "sr1", "ipmi_lease", HealthState.OK)
        update_device_health("test", "sr1", "server_health", HealthState.OK)
        update_device_health("test", "sr1", "mgmt_link", HealthState.OK)

        # sr1 should be OK now.
        sr1 = Device.get_device("sr1", "test")
        health_records = get_device_health_all(sr1)
        for _, status in health_records.items():
            assert status["status"] == "OK"

        update_device_health("test", "sr2", "ipmi_access", HealthState.WARNING)
        update_device_health("test", "sr2", "ipmi_lease", HealthState.WARNING)
        update_device_health("test", "sr2", "server_health", HealthState.WARNING)
        update_device_health("test", "sr2", "mgmt_link", HealthState.WARNING)

        # should have 2 degraded devices
        assert filter_degraded_devices(Device.get_all("test", device_type="SR"), "SR").count() == 2

        # should have 1 critical devices
        assert filter_critical_devices(Device.get_all("test", device_type="SR"), "SR").count() == 1

    def test_system_health(self):
        sys0 = Device.get_device("sys0", "test")
        health_records = get_device_health_all(sys0)

        # initial state should have all status == "UNKNOWN"
        for _, status in health_records.items():
            assert status["status"] == "UNKNOWN"

        # should have 2 degraded devices - sys0 and sys1
        assert filter_degraded_devices(Device.get_all("test", device_type="SY"), "SY").count() == 2

        update_device_health("test", "sys0", "system_health", HealthState.OK)
        update_device_health("test", "sys0", "system_link", HealthState.OK)

        # sys0 should be OK now.
        sys0 = Device.get_device("sys0", "test")
        health_records = get_device_health_all(sys0)
        for _, status in health_records.items():
            assert status["status"] == "OK"

        update_device_health("test", "sys1", "system_health", HealthState.WARNING)
        update_device_health("test", "sys1", "system_link", HealthState.CRITICAL)

        # should have 1 degraded device
        assert filter_degraded_devices(Device.get_all("test", device_type="SY"), "SY").count() == 1

        # should have 1 critical device
        assert filter_critical_devices(Device.get_all("test", device_type="SY"), "SY").count() == 1
