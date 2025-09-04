import os
import sys
import yaml


sys.path.insert(0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../../os-provision/scripts')))

import extract_config

def test_run():
    with open("in/config.yaml") as infile:
        data = yaml.safe_load(infile)
    print(extract_config.config_to_ipmi_list(data))

