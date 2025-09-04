from deployment_manager.cli.switch.link.link import LinkDetectMacsRepr, sort_devs

def test_detect_macs():

  expected_sw_port_dev = {
    "sw1": {
        "Ethernet1": ("srv1", "@ipmi"),
        "Ethernet2": ("srv1", "@mgmt"),
        "Ethernet3": ("srv2", "@ipmi_mgmt"),
        "Ethernet4": ("srv3", "@ipmi"),
        "Ethernet5": ("srv3", "@mgmt"),
        "Ethernet6": ("srv4", "@ipmi"),
        "Ethernet7": ("srv4", "@mgmt"),
        "Ethernet8": ("srv5", "@mgmt"),
    },
  }

  cscfg_device_macs = {
    "srv1": {
        "@ipmi": "AA:BB:CC:DD:EE:11",
        "@mgmt": "AA:BB:CC:DD:EE:12",
    },
    "srv2": {
        "@ipmi": "AA:BB:CC:DD:EE:21",
        "@mgmt": "AA:BB:CC:DD:EE:22",
    },
    "srv3": {
        "@ipmi": "AA:BB:CC:DD:EE:31",
        "@mgmt": "AA:BB:CC:DD:EE:32",
    },
    "srv4": {
        "@ipmi": "AA:BB:CC:DD:EE:41",
        "@mgmt": "AA:BB:CC:DD:EE:42",
    },
  }

  mac_table = {
    "sw1": {
      "Ethernet1": ["AA:BB:CC:DD:EE:11"],
      "Ethernet2": ["AA:BB:CC:DD:EE:12"],
      "Ethernet3": ["AA:BB:CC:DD:EE:21", "AA:BB:CC:DD:EE:22"],
      "Ethernet6": ["AA:BB:CC:DD:EE:42"],
      "Ethernet7": ["AA:BB:CC:DD:EE:41"],
      "Ethernet8": ["AA:BB:CC:DD:EE:51"],
    },
  }

  matches, notfound, changes, additions = sort_devs(expected_sw_port_dev, cscfg_device_macs, mac_table)

  exp_matches = [
    LinkDetectMacsRepr(
        dst_name="srv1",
        dst_if="@ipmi",
        match_state="MATCH",
        current_mac="AA:BB:CC:DD:EE:11",
        actual_macs=["AA:BB:CC:DD:EE:11"],
        src_name="",
        src_if="",
        link_state="",
    ),
    LinkDetectMacsRepr(
        dst_name="srv1",
        dst_if="@mgmt",
        match_state="MATCH",
        current_mac="AA:BB:CC:DD:EE:12",
        actual_macs=["AA:BB:CC:DD:EE:12"],
        src_name="",
        src_if="",
        link_state="",
    ),
    LinkDetectMacsRepr(
        dst_name="srv2",
        dst_if="@ipmi",
        match_state="MATCH",
        current_mac="AA:BB:CC:DD:EE:21",
        actual_macs=["AA:BB:CC:DD:EE:21"],
        src_name="",
        src_if="",
        link_state="",
    ),
    LinkDetectMacsRepr(
        dst_name="srv2",
        dst_if="@mgmt",
        match_state="MATCH",
        current_mac="AA:BB:CC:DD:EE:22",
        actual_macs=["AA:BB:CC:DD:EE:22"],
        src_name="",
        src_if="",
        link_state="",
    ),
  ]

  exp_notfound = [
    LinkDetectMacsRepr(
        dst_name="srv3",
        dst_if="@ipmi",
        match_state="MISSING",
        current_mac="",
        actual_macs=[],
        src_name="sw1",
        src_if="Ethernet4",
        link_state="",
    ),
    LinkDetectMacsRepr(
        dst_name="srv3",
        dst_if="@mgmt",
        match_state="MISSING",
        current_mac="",
        actual_macs=[],
        src_name="sw1",
        src_if="Ethernet5",
        link_state="",
    ),
  ]
  
  exp_changes = [
    LinkDetectMacsRepr(
        dst_name="srv4",
        dst_if="@ipmi",
        match_state="MISMATCH",
        current_mac="AA:BB:CC:DD:EE:41",
        actual_macs=["AA:BB:CC:DD:EE:42"],
        src_name="",
        src_if="",
        link_state="",
    ),
    LinkDetectMacsRepr(
        dst_name="srv4",
        dst_if="@mgmt",
        match_state="MISMATCH",
        current_mac="AA:BB:CC:DD:EE:42",
        actual_macs=["AA:BB:CC:DD:EE:41"],
        src_name="",
        src_if="",
        link_state="",
    ),
  ]

  exp_additions = [
    LinkDetectMacsRepr(
      dst_name="srv5",
      dst_if="@mgmt",
      match_state="NEW",
      current_mac="",
      actual_macs=["AA:BB:CC:DD:EE:51"],
      src_name="",
      src_if="",
      link_state="",
    ),
  ]  

  assert matches == exp_matches
  assert notfound == exp_notfound
  assert changes == exp_changes
  assert additions == exp_additions
