import pathlib
import pytest
import tempfile
import yaml
from django.test.testcases import TestCase
from typing import Optional

from deployment_manager.conftool.confgen import write_master_config
from deployment_manager.db.models import DeploymentProfile, Device
from deployment_manager.tools.config import ConfGen

asset_root = pathlib.Path(__file__).parent.parent / "tests" / "assets"


@pytest.mark.skip
class DbInventoryTest(TestCase):
    """
    Test import of inventory.csv to Device database
    """

    tags = ["container_only"]

    test_profile = "test-inv"
    test_input_yml = "/host/config/pb1-input.yml"
    test_inventory_csv = "/host/config/test-inventory.csv"
    test_inventory_update_csv = "/host/config/test-inventory-update.csv"
    
    def setUp(self):
        dp = DeploymentProfile.create(self.test_profile, self.test_profile, True)
        self.cg = ConfGen(profile=self.test_profile)
    
    def test_new_import(self) -> None:
        """ 
        Test importing into a new database profile
        """
        self.cg.update_inventory_from_file(filename=self.test_inventory_csv)
        csv = self.cg.inventory_file_to_dict(filename=self.test_inventory_csv)
        
        for device, info in csv.items():
            d:Optional[Device] = Device.get_device(name=device, profile_name=self.test_profile)
            assert d is not None, f"{device} did not get inserted into db"
            prop = d.get_prop_attr_dict()
            for k,v in info.items():
                if k in ["device_role", "device_type"]:
                    assert getattr(d, k) == v, f"{device}.{k} has the wrong value"
                else:
                    for attr, value in v.items():
                        assert prop[k][attr] == value, f"{device}.{k}.{attr} has the wrong value" 

    def test_update_import(self) -> None:
        """
        Test importing into an existing Device table
        Test both cases of information change and new devices
        """
        # Initial import
        self.cg.update_inventory_from_file(filename=self.test_inventory_csv)
        
        # Update import
        self.cg.update_inventory_from_file(filename=self.test_inventory_update_csv)
        
        csv_original = self.cg.inventory_file_to_dict(filename=self.test_inventory_csv)
        csv_update = self.cg.inventory_file_to_dict(filename=self.test_inventory_update_csv)
        
        # Check existing entries
        for device, info in csv_original.items():
            d:Optional[Device] = Device.get_device(name=device, profile_name=self.test_profile)
            assert d is not None, f"{device} did not get inserted into db"
            prop = d.get_prop_attr_dict()
            if device in csv_update:
                for k,v in csv_update[device].items():
                    if k in ["device_role", "device_type"]:
                        assert getattr(d, k) == v, f"{device}.{k} does not have the updated value"
                    else:
                        for attr, value in v.items():
                            assert prop[k][attr] == value, f"{device}.{k}.{attr} does not have the updated value" 
            else:
                for k,v in info.items():
                    if k in ["device_role", "device_type"]:
                        assert getattr(d, k) == v, f"{device}.{k} has the wrong value"
                    else:
                        for attr, value in v.items():
                            assert prop[k][attr] == value, f"{device}.{k}.{attr} has the wrong value" 
        
        # Check for new devices
        new_devices = []
        for device, info in csv_update.items():
            if device not in csv_original:
                new_devices.append((device, info))
        
        for device, info in new_devices:
            d:Optional[Device] = Device.get_device(name=device, profile_name=self.test_profile)
            assert d is not None, f"{device} did not get inserted into db"
            prop = d.get_prop_attr_dict()
            for k,v in info.items():
                if k in ["device_role", "device_type"]:
                    assert getattr(d, k) == v, f"{device}.{k} has the wrong value"
                else:
                    for attr, value in v.items():
                        assert prop[k][attr] == value, f"{device}.{k}.{attr} has the wrong value" 
                
    def test_write_master_config(self):
        tmp_file = tempfile.mktemp("master_config")
        self.cg.update_inventory_from_file(filename=self.test_inventory_csv)
        write_master_config(self.test_input_yml, Device.get_all("test-inv"), tmp_file)
        doc = yaml.safe_load(pathlib.Path(tmp_file).read_text())
        self.assertEqual(5, len(doc["servers"]))
        self.assertEqual(1, len(doc["systems"]))
        self.assertEqual(1, len(doc["switches"]))
        self.assertEqual("stamp-0", doc["servers"][0]["stamp"])
        self.assertEqual("default", doc["servers"][-1]["stamp"])
