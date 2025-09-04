from collections import defaultdict
from typing import List, Tuple
import pathlib
import pytest
import tempfile
from unittest.mock import patch
from django.test.testcases import TestCase

import deployment_manager.db.device_props as props
from deployment_manager.tools.switch.ztp_config import (
    generate_mgmt_switch_ztp_config,
    generate_data_switch_ztp_config
)


def props_to_dict(p: List[Tuple[props.DeviceProp, str]]) -> dict:
    d = defaultdict(dict)
    for p, v in p:
        d[p.name][p.attr] = v
    return d

@pytest.mark.skip
class SwitchZtpTest(TestCase):
    """
    Test mgmt and data switch ZTP config generation
    """
    switch_name = "test-switch"

    def setUp(self):
        self.mg_switch_props = self._generate_mg_switch_props()
        self.profile_cfg = self._generate_profile_config()

    def _generate_mg_switch_props(self):
        prop_vals = [
            (props.prop_subnet_info_gateway, "172.20.20.254"),
            (props.prop_subnet_info_subnet, "172.20.20.0/24"),
            (props.prop_management_info_ip, "172.20.20.2"),
            (props.prop_subnet_info_asn, "65001"),
            (props.prop_switch_info_loopback_ip, "1.0.0.1"),
            (props.prop_vendor_name, "AR")
        ]
        return props_to_dict(prop_vals)

    def _generate_data_switch_props(self):
        prop_vals = [
            (props.prop_management_info_ip, "172.20.20.3"),
            (props.prop_switch_info_loopback_ip, "1.0.0.1"),
            (props.prop_vendor_name, "AR")
        ]
        return props_to_dict(prop_vals)

    def _generate_profile_config(self):
        mgmt_network_int_config = {
            "ip_allocation": {
                "parentnet": "172.20.0.0/16",
                "network_extents": [
                    {"name": "deploy_mgmt", "subnets": ["172.20.20.0/24"]},
                ],
            },
            "asn_agg_0": "65123",
            "asn_agg_1": "65124",
        }
        return {
            "mgmt_network_int_config": mgmt_network_int_config
        }

    @patch('deployment_manager.tools.switch.ztp_config.get_mgmt_fw_image')
    def test_generate_mgmt_switch_ztp_config(self, mock_get_mgmt_fw_image):
        # Mock the firmware path to use the temporary file
        temp_dir = tempfile.TemporaryDirectory()
        test_fw_path = pathlib.Path(temp_dir.name) / "EOS64-4.32.0F.swi"
        test_fw_path.touch()
        mock_get_mgmt_fw_image.return_value = test_fw_path

        
        out = generate_mgmt_switch_ztp_config(self.switch_name, self.mg_switch_props, self.profile_cfg, "172.20.20.253")
        assert out["ztp"] and out["startup_config"], f"Unexpected error encountered in mgmt switch ZTP config, no output"

        switch_props = self._generate_mg_switch_props()
        del switch_props["switch_info"]["loopback_ip"]
        with self.assertRaises(ValueError):
            generate_mgmt_switch_ztp_config(self.switch_name, switch_props, self.profile_cfg, "172.20.20.253")

        profile_cfg = self._generate_profile_config()
        del profile_cfg["mgmt_network_int_config"]["asn_agg_0"]
        with self.assertRaises(ValueError):
            generate_mgmt_switch_ztp_config(self.switch_name, self.mg_switch_props, profile_cfg, "172.20.20.253")
        
        temp_dir.cleanup()

    @patch('deployment_manager.tools.switch.ztp_config.get_data_fw_image')
    def test_generate_mgmt_switch_ztp_config(self, mock_get_mgmt_fw_image):
        # Mock the firmware path to use the temporary file
        temp_dir = tempfile.TemporaryDirectory()
        test_fw_path = pathlib.Path(temp_dir.name) / "EOS64-4.32.0F.swi"
        test_fw_path.touch()
        mock_get_mgmt_fw_image.return_value = test_fw_path

        out = generate_data_switch_ztp_config(
            self.switch_name, self._generate_data_switch_props(), self.mg_switch_props, "172.20.20.253"
        )
        assert out["ztp"] and out["startup_config"], f"Unexpected error encountered in data switch ZTP config, empty output {out}"

        switch_props = self._generate_mg_switch_props()
        del switch_props["subnet_info"]["subnet"]
        with self.assertRaises(ValueError):
            generate_data_switch_ztp_config(
                self.switch_name, self._generate_data_switch_props(), switch_props, "172.20.20.253"
            )
        
        temp_dir.cleanup()
