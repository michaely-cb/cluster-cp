import os
import sys
import shutil
import pathlib



sys.path.insert(0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../../deployment/')))

GITTOP = pathlib.Path(os.getenv("GITTOP"))
DEPLOYMENT_PATH = "src/cluster_deployment/"
TESTS_PATH = "deployment/deployment_manager/tests/"
ASSETS_PATH= GITTOP.joinpath(DEPLOYMENT_PATH + "/" + TESTS_PATH + "assets")

from deployment_manager.tools.validator import (
    IpmiValidator, OsValidator, NicValidator
)
from deployment_manager.tools.config import (
    ConfGen,
)

def test_validator(tmp_path):
    src_master_file = ASSETS_PATH / "devtest_master_config.yaml"
    dest_master_file = tmp_path / "master_config.yml"
    
    shutil.copy(src_master_file, dest_master_file)
    
    cg = ConfGen("validator", tmp_path)
    devices = [dev for dev in cg.get_devices("dict")["servers"]]
    ipmi_validator=IpmiValidator(cg, 2)
    results = ipmi_validator.validate(devices)
    print(results)
    
    os_validator=OsValidator(cg, 2)
    results = os_validator.validate(devices)
    print(results)
    
    nic_validator=NicValidator(cg, 2)
    results = nic_validator.validate(devices)
    print(results)
    
    





