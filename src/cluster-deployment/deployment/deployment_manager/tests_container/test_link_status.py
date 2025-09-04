import pytest
from django.test.testcases import TestCase

from typing import Dict, List

from deployment_manager.cli.switch.link.reprs import LinkNeighStatusRepr, LINK_STATE_MATCH, LINK_STATE_UNRECOGNIZED, LINK_STATE_MISSING
from deployment_manager.cli.switch.interface import MacAddrTableEntry
from deployment_manager.cli.switch.link.link import _PortRef, generate_eth_statuses, get_mac_addr_tables, mark_missing_links
from deployment_manager.db.models import DeploymentDeviceRoles, DeploymentDeviceTypes, DeploymentProfile, Device, Link
from deployment_manager.db import device_props as props

@pytest.mark.skip
class DbTest(TestCase):

  def setUp(self):
    
    # create a profile
    profile = DeploymentProfile.create("test", "test", True)

    # function to add MG links to the DB
    def gen_link(src_device, src_if, src_if_role, dst_name, dst_if, dst_if_role, speed):
      l = Link(src_device=src_device, src_if=src_if, src_if_role=src_if_role, dst_device=None, dst_name=dst_name, dst_if=dst_if, dst_if_role=dst_if_role, speed=speed)
      l.save()
      return l

    # add switches to the DB
    self.switches = [
      Device.add("mg_sw_1", DeploymentDeviceTypes.SWITCH, DeploymentDeviceRoles.MANAGEMENT, profile),
    ]

    # add servers to the DB
    self.servers = [
      Device.add("sr_1", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.USER, profile),
      Device.add("sr_2", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.USER, profile),
    ]

    # add properties to the servers
    self.servers[0].set_prop(props.prop_ipmi_info_mac, "AA:BB:CC:DD:EE:11")
    self.servers[0].set_prop(props.prop_management_info_mac, "AA:BB:CC:DD:EE:12")
    self.servers[1].set_prop(props.prop_ipmi_info_mac, "AA:BB:CC:DD:EE:21")
    self.servers[1].set_prop(props.prop_management_info_mac, "AA:BB:CC:DD:EE:22")

    self.links = [
      # add MANAGEMENT links to the DB
      gen_link(self.switches[0], "Eth1/1", "mgmt", "sr_1", "if1", "ipmi", "1"),
      gen_link(self.switches[0], "Eth2/1", "mgmt", "sr_1", "if2", "mgmt", "1"),
      gen_link(self.switches[0], "Eth3/1", "mgmt", "sr_2", "if1", "ipmi_mgmt", "1"),
      # add a missing link
      gen_link(self.switches[0], "Eth5/1", "mgmt", "sr_3", "if1", "mgmt", "1"),
    ] 

  # TEST
  def test_generate_eth_statuses(self):

    profile = DeploymentProfile.get_profile("test")

    # Get all the links from the database
    all_db_links = Link.objects.all() 

    mac_addr_tables: List[MacAddrTableEntry] = [
      MacAddrTableEntry("mg_sw_1", "Eth1/1", "AA:BB:CC:DD:EE:11"),
      MacAddrTableEntry("mg_sw_1", "Eth2/1", "AA:BB:CC:DD:EE:12"),
      MacAddrTableEntry("mg_sw_1", "Eth3/1", "AA:BB:CC:DD:EE:21"),
      MacAddrTableEntry("mg_sw_1", "Eth3/1", "AA:BB:CC:DD:EE:22"),
      # unrecognized device
      MacAddrTableEntry("mg_sw_1", "Eth7/1", "AA:BB:CC:DD:EE:77"),
      # VLAN with many devices
      MacAddrTableEntry("mg_sw_1", "Eth8/1", "AA:BB:CC:DD:EE:80"),
      MacAddrTableEntry("mg_sw_1", "Eth8/1", "AA:BB:CC:DD:EE:81"),
      MacAddrTableEntry("mg_sw_1", "Eth8/1", "AA:BB:CC:DD:EE:82"),
    ]

    mgmt_macs : Dict[str, _PortRef] = {
        "AA:BB:CC:DD:EE:11": _PortRef("sr_1", "if1", "ipmi"),
        "AA:BB:CC:DD:EE:12": _PortRef("sr_1", "if2", "mgmt"),
        "AA:BB:CC:DD:EE:21": _PortRef("sr_2", "if1", "ipmi"),
        "AA:BB:CC:DD:EE:22": _PortRef("sr_2", "if1", "mgmt"), 
    }

    exp_link_interface_info = {
          'sr_1':
              {
                'ipmi': ('if1', 'mg_sw_1', 'Eth1/1'),
                'mgmt': ('if2', 'mg_sw_1', 'Eth2/1'),
              },
          'sr_2':
              {
                'ipmi': ('if1', 'mg_sw_1', 'Eth3/1'),
                'mgmt': ('if1', 'mg_sw_1', 'Eth3/1'),
              },
          'sr_3':
              {
                'mgmt': ('if1', 'mg_sw_1', 'Eth5/1'),
              },
          }

    _ , link_interface_info = get_mac_addr_tables(profile, None, all_db_links)

    assert link_interface_info == exp_link_interface_info

    all_interface_status = {
          'mg_sw_1:Eth1/1': True,
          'mg_sw_1:Eth2/1': True,
          'mg_sw_1:Eth3/1': True,
          'mg_sw_1:Eth4/1': True,
        }

    exp_eth_link_statuses = [
      LinkNeighStatusRepr(
                "mg_sw_1", "Eth1/1", "sr_1", "if1",
                "ETH", "sr_1", "if1",
                LINK_STATE_MATCH, "import", True, "", ""
                ),
      LinkNeighStatusRepr(
                "mg_sw_1", "Eth2/1", "sr_1", "if2",
                "ETH", "sr_1", "if2",
                LINK_STATE_MATCH, "import", True, "", ""
                ),
      LinkNeighStatusRepr(
                "mg_sw_1", "Eth3/1", "sr_2", "if1",
                "ETH", "sr_2", "if1",
                LINK_STATE_MATCH, "import", True, "", ""
                ),
      LinkNeighStatusRepr(
                "mg_sw_1", "Eth3/1", "sr_2", "if1",
                "ETH", "sr_2", "if1",
                LINK_STATE_MATCH, "import", True, "", ""
                ),
      LinkNeighStatusRepr(
                "mg_sw_1", "Eth7/1", "", "",
                "ETH", "???", "AA:BB:CC:DD:EE:77",
                LINK_STATE_UNRECOGNIZED, "", "", "", ""
                ),
      LinkNeighStatusRepr(
                "mg_sw_1", "Eth8/1", "", "",
                "ETH", "", "VLAN with many devices",
                LINK_STATE_UNRECOGNIZED, "", "", "", ""
                ),
    ]

    eth_link_statuses = generate_eth_statuses(None, mac_addr_tables, mgmt_macs, link_interface_info, all_interface_status)

    exp_missing_link_statuses = [
      LinkNeighStatusRepr(
                "mg_sw_1", "Eth5/1", "sr_3", "if1",
                "ETH", "", "",
                LINK_STATE_MISSING, "import", "", "", ""
                ),
    ]

    missing_link_statuses = mark_missing_links(link_interface_info)

    assert eth_link_statuses == exp_eth_link_statuses
    
    assert missing_link_statuses == exp_missing_link_statuses
