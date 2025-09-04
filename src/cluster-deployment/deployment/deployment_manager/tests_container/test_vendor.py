import pathlib
from unittest import TestCase

import pytest

import deployment_manager.tools.vendor as vendor
from deployment_manager.tools.vendor import process_vendor_file

ASSET_PATH = pathlib.Path(__file__).parent.parent / "tests/assets/"


#@pytest.mark.skip
class VendorIngestionTest(TestCase):

    def test_parse_vendor_csv(self):
        vendor_file = ASSET_PATH / "config/vendor_ingestion-inventory.csv"
        expected = len(vendor_file.read_text().strip().split("\n")) - 1

        devices = process_vendor_file(vendor_file, "cgx-")
        assert len(devices) == expected, f"expected {expected} devices, got {len(devices)}: {devices}"

        # names unique
        names = {d["name"]: 0 for d in devices}
        for d in devices:
            names[d["name"]] += 1
        assert len(names) == expected, f"expected names to be unique, some were repeated {names}"

        # No None names
        assert "None" not in names, "name assignment included 'None' value"

        non_xs = [d["name"] for d in devices if d["name"].startswith("cgx")]
        assert len(non_xs) == expected - 3, "expecting non-system and non-mediaconverter names to start with cgx-"

    def test_parse_vendor_csv_validation(self):
        sw_base = {"location.rack": "wse001", "type": "SW", "role": "MG", "vendor.name": "juniper"}
        # require mac or subnet+ip
        err = vendor._check_prop_spec(sw_base, vendor.require_properties_type["SW"])
        self.assertNotEqual(err, "")

        # require mac or subnet+ip
        sw = sw_base.copy()
        sw["management_info.mac"] = "00:00:00:00:00:00:00"
        err = vendor._check_prop_spec(sw, vendor.require_properties_type["SW"])
        self.assertEqual(err, "")

        sw = sw_base.copy()
        sw["subnet_info.subnet"] = "10.0.0.0/24"
        err = vendor._check_prop_spec(sw, vendor.require_properties_type["SW"])
        self.assertEqual(err, "at least one of (management_info.mac or all of (subnet_info.subnet and management_info.ip))")
        sw["management_info.ip"] = "10.0.0.1"
        err = vendor._check_prop_spec(sw, vendor.require_properties_type["SW"])
        self.assertEqual(err, "")
