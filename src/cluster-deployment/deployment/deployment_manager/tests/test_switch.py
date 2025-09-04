import json
import textwrap
import unittest
import unittest.mock

from pathlib import Path

from deployment_manager.tools.ssh import ExecMixin
from deployment_manager.tools.switch.arista import AristaPortInterface
from deployment_manager.tools.switch.dell_os10 import DellOS10PortInterface, DellOS10SwitchCtl
from deployment_manager.tools.switch.dell_sonic import DellSonicSwitchCtl, parse_dell_sonic_table
from deployment_manager.tools.switch.edgecore import EdgecorePortInterface, EdgecoreSwitchCtl, \
    parse_edgecore_sonic_table
from deployment_manager.tools.switch.hpe import HpePortInterface, HpeOnyxSwitchCtl, HpeSwitchCtl
from deployment_manager.tools.switch.interface import BreakoutMode, GBPS, PortInterface, SwitchInterface, \
    SwitchOSFamily, \
    _PortSpeedAccum, get_breakout_commands, parse_transceiver
from deployment_manager.tools.switch.juniper import JuniperPortInterface, JuniperSwitchCtl
from deployment_manager.tools.switch.switchctl import AristaSwitchCtl
from deployment_manager.tools.switch.utils import parse_sonic_structured_output

ASSETS_DIR = Path(__file__).parent / "assets"


def test_port_interface_parsing():
    testcases = [
        ("et-0/1/2:3", JuniperPortInterface(1,2,3), None),
        ("et-0/1/2", JuniperPortInterface(1, 2, None), None),
        ("xe-0/1/2:3", JuniperPortInterface(1,2,3,prefix="xe"), None),

        ("Eth1", AristaPortInterface(None, 0, None), "Ethernet1"),
        ("Ethernet1", AristaPortInterface(None, 0, None), None),
        ("Ethernet1/2", AristaPortInterface(None, 0, 1), None),
        ("Ethernet1/2/3", AristaPortInterface(0, 1, 2), None),

        ("ethernet1/1/1", DellOS10PortInterface(0), None),

        ("Ethernet1", EdgecorePortInterface(1), None),
        ("Eth16", EdgecorePortInterface(16), "Ethernet16"),

        ("Eth1/1", HpePortInterface(0), None),
    ]

    for name, expected, expected_name in testcases:
        if isinstance(expected, PortInterface):
            expected_name = expected_name if expected_name else name
            actual = expected.from_str(name)
            assert actual == expected, f"expected {expected.__class__.__name__} to parse '{name}' as {expected} but got {actual}"
            assert str(expected) == expected_name, f"expected string for {expected.__class__.__name__} to be '{expected_name}' but got {str(expected)}"
        else:  # shouldn't parse case
            actual = expected.from_str(name)
            assert actual is None, f"expected {expected.__name__} to be unable to parse '{name}' but returned '{actual}'"


def test_create_arista_interface_speed_actions():
    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.exec.return_value = (0, (ASSETS_DIR / "files" / "arista_show_interfaces.json").read_text(), "")
    cfg_infs = {
        "Eth1/1": 100,
        "Eth1/3": 100,
        "Eth2/1": 100,
        "Eth3/1": 400,
        "Eth5/1": 100,
        "Eth16/1": 100,
        "Ma1": 10,
    }
    ctl = AristaSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    actual_force, errors = ctl.get_breakout_commands(cfg_infs, force=True)
    enter, exit = ['en', 'terminal length 0', 'terminal dont-ask', 'config'], ['exit', 'write memory', ]
    txr_diag_cmds = ["exit", "exit", "bash", "sleep 3", "exit", "config",]
    sleep_long = ['en', 'bash', 'sleep 60', 'exit', ]
    sleep_short = ['en', 'bash', 'sleep 10', 'exit', ]
    expected = [
        *[*enter,
          'interface ethernet 1/1', 'speed auto 100g-2', 'exit',
          'interface ethernet 2/1', 'speed auto 100g-2', 'exit',
          'hardware speed-group 5 serdes 25g', 'interface ethernet 5/1', 'speed 100g-4', 'exit',
          *exit,
          *sleep_long],
        *[*enter,
          'interface ethernet 1/3', 'speed auto 100g-2', 'exit',
          'interface ethernet 2/3', 'speed auto 100g-2', 'exit',
          'interface ethernet 5/1', 'transceiver diag simulate removed', *txr_diag_cmds, 'interface ethernet 5/1', 'no transceiver diag simulate removed', 'exit',
          *exit,
          *sleep_short],
        *[*enter,
          'interface ethernet 1/5', 'speed auto 100g-2', 'exit',
          'interface ethernet 2/5', 'speed auto 100g-2', 'exit',
          *exit,
          *sleep_short],
        *[*enter,
          'interface ethernet 1/7', 'speed auto 100g-2', 'exit',
          'interface ethernet 2/7', 'speed auto 100g-2', 'exit',
          *exit,
          *sleep_short],
        *[*enter,
          'interface ethernet 1/1', 'transceiver diag simulate removed', *txr_diag_cmds, 'interface ethernet 1/1', 'no transceiver diag simulate removed', 'exit',
          'interface ethernet 2/1', 'transceiver diag simulate removed', *txr_diag_cmds, 'interface ethernet 2/1', 'no transceiver diag simulate removed', 'exit',
          *exit]
    ]
    assert len(expected) == len(actual_force)
    for i, actual in enumerate(actual_force):
        assert actual == expected[i], f"command at index {i} did not match, actual={actual}, expected={expected[i]}"
    assert errors
    assert 'unable to parse input interface name Ma1' in errors[0]
    assert 'port for interface Ethernet16/1 was not found' in errors[1]

    # without force, the 2/1 interface, which is already configured, should not be reconfigured
    actual_noforce, errors = ctl.get_breakout_commands({"Eth2/1": 100, "Eth5/1": 100,}, force=False)
    expected = [
        *[*enter,
          'hardware speed-group 5 serdes 25g', 'interface ethernet 5/1', 'speed 100g-4', 'exit',
          *exit,
          *sleep_long],
        *[*enter,
          'interface ethernet 5/1', 'transceiver diag simulate removed', *txr_diag_cmds, 'interface ethernet 5/1', 'no transceiver diag simulate removed', 'exit',
          *exit]
    ]
    assert actual_noforce == expected


def test_switch_get_breakout_commands():
    mock_ctl = unittest.mock.Mock()
    mock_ctl.get_max_port_speed_gbps.return_value = 800
    mock_ctl.list_interfaces.return_value = [
        SwitchInterface(
            switch="mock_switch",
            name="Ethernet1",
            alias="",
            desc="",
            up=True,
            status="up",
            speed=400 * GBPS,  # Current mode is 1x400G
            transceiver="2x400g-dr4",
            raw={},
        )
    ]
    mock_vendor_cmds = unittest.mock.Mock()
    mock_vendor_cmds.parse_interface.return_value = AristaPortInterface.from_str("Ethernet1/1")
    mock_vendor_cmds.get_port_breakout_cmds.return_value = [["BREAKOUT_800G_1"]]
    mock_vendor_cmds.get_enter_config_mode_cmds.return_value = ["ENTER"]
    mock_vendor_cmds.get_exit_config_mode_cmds.return_value = ["EXIT"]
    mock_vendor_cmds.get_sleep_commands.return_value = ["SLEEP"]
    mock_ctl.vendor_cmds.return_value = mock_vendor_cmds

    if_speed = {"Ethernet1": 800}  # Target mode is 1x800G

    commands, warnings = get_breakout_commands(mock_ctl, if_speed)

    expected_commands = [ "ENTER", "BREAKOUT_800G_1", "EXIT", ]
    assert commands == expected_commands, f"Expected commands: {expected_commands}, but got: {commands}"
    assert not warnings, f"Expected no warnings, but got: {warnings}"

    assert mock_vendor_cmds.get_port_breakout_cmds.call_args_list[0].args[1:] == (BreakoutMode.Mode400X1, BreakoutMode.Mode800X1)


def test_arista_list_interfaces_err_stats():
    exec = unittest.mock.Mock(spec=ExecMixin)
    clock_doc = (ASSETS_DIR / "files" / "arista_show_clock.json").read_text()
    inf_doc = (ASSETS_DIR / "files" / "arista_show_interfaces2.json").read_text()
    txr_doc = (ASSETS_DIR / "files" / "arista_show_txr.json").read_text()
    phy_doc = (ASSETS_DIR / "files" / "arista_show_phy_diag.json").read_text()
    exec.exec.side_effect = [(0, doc, "") for doc in [clock_doc, inf_doc, txr_doc, phy_doc]]
    ctl = AristaSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    stats = ctl.list_interfaces_err_stats()
    assert len(stats) == 5
    for i, stat in enumerate(stats):
        if i == 3:
            assert stat.min_rx_power == -30.0
        else:
            assert stat.min_rx_power > 0
        assert stat.min_tx_power > 0
        assert stat.tail_phys_errs > 0
        assert stat.state_duration > 0
        assert stat.up
    interface_names = sorted(["Ethernet25/1", "Ethernet25/3", "Ethernet25/5", "Ethernet25/7", "Ethernet2/1/1"])
    assert interface_names == sorted([i.name for i in stats]), "expected interface names not present"

def test_arista_sflow_status():
    exec = unittest.mock.Mock(spec=ExecMixin)
    status_doc = (ASSETS_DIR / "files" / "arista_show_sflow.json").read_text()
    sflow_interfaces_doc = (ASSETS_DIR / "files" / "arista_show_sflow_interfaces.json").read_text()
    exec.exec.side_effect = [(0, doc, "") for doc in [status_doc, sflow_interfaces_doc]]
    ctl = AristaSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    status_singleton = ctl.sflow_status()
    assert len(status_singleton) == 1
    status = status_singleton[0]
    assert status.src == "mock"
    assert status.enabled == True
    assert status.sampling_rate == 65536
    assert status.poll_interval == 2
    assert status.agent_ip == "172.28.211.240"
    assert status.src_ip == "10.250.227.254"
    assert len(status.dsts) == 2
    assert ("10.250.228.33", 6344) in status.dsts
    assert ("10.250.228.34", 6343) in status.dsts
    assert len(status.interfaces) == 8
    for intf, enabled in status.interfaces.items():
        if intf == "Ethernet1/1":
            assert not enabled
        else:
            assert enabled
    assert status.sample_cnt == 12345678
    row = status.to_table_row()
    assert row[0] == "mock"
    assert row[1] == "True"
    assert row[2] == "65536"
    assert row[3] == "2"
    assert row[4] == "172.28.211.240"
    assert row[5] == "10.250.227.254"
    assert row[6] == "10.250.228.33:6344,10.250.228.34"
    assert row[7] == "Eth 2/1,2/3,2/5,2/7,3/1,4/1,5/1"
    assert row[8] == "12345678"
    assert row[9] == "Displaying counters that may be stale"

def test_arista_get_mac_addr_table():
    get_mac_addr_table_doc = (ASSETS_DIR / "files" / "arista_get_mac_addr_table.json").read_text()

    def mock_get_mac_addr_table_call(*args, **kwargs):
        return 0, get_mac_addr_table_doc, ""

    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.exec = mock_get_mac_addr_table_call

    ctl = AristaSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    entries = ctl.get_mac_addr_table()
    assert len(entries) == 3, "Invalid number of entries detected"
    # first entry
    assert entries[0].switch == "mock", "Wrong switch name"
    assert entries[0].name == "Port-Channel106", "Wrong interface name"
    assert entries[0].mac == "00:00:5e:00:01:29", "Wrong mac address"
    # second entry
    assert entries[1].switch == "mock", "Wrong switch name"
    assert entries[1].name == "Port-Channel106", "Wrong interface name"
    assert entries[1].mac == "00:13:c6:03:5f:37", "Wrong mac address"
    # third entry
    assert entries[2].switch == "mock", "Wrong switch name"
    assert entries[2].name == "Port-Channel106", "Wrong interface name"
    assert entries[2].mac == "b4:0c:25:e2:80:12", "Wrong mac address"

def test_juniper_list_interfaces():
    shell = unittest.mock.MagicMock()
    shell.__enter__.return_value = shell
    infs_doc = (ASSETS_DIR / "files" / "juniper_interfaces.json").read_text()
    chassis_doc = (ASSETS_DIR / "files" / "juniper_chassis.json").read_text()
    desc_doc = (ASSETS_DIR / "files" / "juniper_descriptions.json").read_text()
    shell.exec.side_effect = [infs_doc, chassis_doc, desc_doc]
    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.shell_session.return_value = shell

    ctl = JuniperSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    infs = ctl.list_interfaces()
    assert len(infs) == 7
    assert infs[0].up
    assert parse_transceiver(infs[0].transceiver).kind == "800g-dr8"
    assert infs[0].normalized_port == 0
    assert infs[0].normalized_channel == 0
    assert infs[1].up
    assert infs[1].normalized_port == 0
    assert infs[1].normalized_channel == 1
    assert parse_transceiver(infs[1].transceiver).kind == "800g-dr8"
    assert not infs[2].up
    assert infs[-1].desc == "desc"


def test_juniper_lldp():
    lldp_doc = (ASSETS_DIR / "files" / "juniper_lldp.json").read_text()

    def mock_lldp_call(*args, **kwargs):
        return 0, lldp_doc, ""

    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.exec = mock_lldp_call

    ctl = JuniperSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    neighbors = ctl.list_lldp_neighbors()
    assert len(neighbors) == 3, "Invalid number of neighbors detected"
    for n in neighbors:
        assert "mgmt" not in n.src_if, "Management interface included in output"
        assert n.src_if, "Source interface not detected"
        assert n.dst_name, "Destination not detected"
        assert n.dst_if, "Destination interface not detected"

def test_edgecore_lldp():
    lldp_doc = (ASSETS_DIR / "files" / "edgecore_lldp.json").read_text()

    def mock_lldp_call(*args, **kwargs):
        return 0, lldp_doc, ""
    
    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.exec = mock_lldp_call

    ctl = EdgecoreSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec

    neighbors = ctl.list_lldp_neighbors()

    assert len(neighbors) == 1, "Invalid number of neighbors detected"

    for n in neighbors:
        assert "mgmt" not in n.src_if, "Management interface included in output"
        assert n.src_if, "Source interface not detected"
        assert n.dst_name, "Destination not detected"
        assert n.dst_if, "Destination interface not detected"


def test_dell_sonic_lldp():
    lldp_doc = (ASSETS_DIR / "files" / "dell_sonic_lldp.json").read_text()

    def mock_lldp_call(*args, **kwargs):
        return 0, lldp_doc, ""

    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.exec = mock_lldp_call

    ctl = DellSonicSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec

    neighbors = ctl.list_lldp_neighbors()

    assert len(neighbors) == 1, "Invalid number of neighbors detected"

    for n in neighbors:
        assert "mgmt" not in n.src_if, "Management interface included in output"
        assert n.src_if, "Source interface not detected"
        assert n.dst_name, "Destination not detected"
        assert n.dst_if, "Destination interface not detected"
    

def test_juniper_get_mac_addr_table():
    get_mac_addr_table_doc = (ASSETS_DIR / "files" / "juniper_get_mac_addr_table.json").read_text()

    def mock_get_mac_addr_table_call(*args, **kwargs):
        return 0, get_mac_addr_table_doc, ""

    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.exec = mock_get_mac_addr_table_call

    ctl = JuniperSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    entries = ctl.get_mac_addr_table()
    assert len(entries) == 3, "Invalid number of entries detected"
    # first entry
    assert entries[0].switch == "mock", "Wrong switch name"
    assert entries[0].name == "ge-0/0/33", "Wrong interface name"
    assert entries[0].mac == "c4:cb:e1:dc:e6:c6", "Wrong mac address"
    # second entry
    assert entries[1].switch == "mock", "Wrong switch name"
    assert entries[1].name == "ae0", "Wrong interface name"
    assert entries[1].mac == "e4:5e:cc:60:3a:00", "Wrong mac address"
    # third entry
    assert entries[2].switch == "mock", "Wrong switch name"
    assert entries[2].name == "ae0", "Wrong interface name"
    assert entries[2].mac == "e4:5e:cc:60:9e:00", "Wrong mac address"

def test_juniper_sflow_status():
    sflow_doc = (ASSETS_DIR / "files" / "juniper_show_sflow.json").read_text()
    sflow_collector_doc = (ASSETS_DIR / "files" / "juniper_show_sflow_collector.json").read_text()
    sflow_interface_doc = (ASSETS_DIR / "files" / "juniper_show_sflow_interface.json").read_text()
    infs_doc = (ASSETS_DIR / "files" / "juniper_interfaces.json").read_text()
    chassis_doc = (ASSETS_DIR / "files" / "juniper_chassis.json").read_text()
    desc_doc = (ASSETS_DIR / "files" / "juniper_descriptions.json").read_text()

    shell = unittest.mock.MagicMock()
    shell.__enter__.return_value = shell
    shell.exec.side_effect = [sflow_doc, sflow_collector_doc, sflow_interface_doc, infs_doc, chassis_doc, desc_doc]
    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.shell_session.return_value = shell
    ctl = JuniperSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    status_singleton = ctl.sflow_status()

    assert len(status_singleton) == 1
    status = status_singleton[0]
    assert status.src == "mock"
    assert status.enabled == True
    assert status.sampling_rate == 65536
    assert status.poll_interval == 2
    assert status.agent_ip == "172.19.133.104"
    assert status.src_ip == "10.4.35.254"
    assert len(status.dsts) == 2
    assert ("10.4.16.1", 6343) in status.dsts
    assert ("10.4.20.1", 6344) in status.dsts
    assert status.sample_cnt == 154128
    assert len(status.interfaces) == 7
    enabled_intfs = [intf for intf, enbld in status.interfaces.items() if enbld]
    exp_enabled_intfs = ["et-0/0/0:1", "et-0/0/1:0", "et-0/0/1:1", "et-0/0/3"]
    for intf in exp_enabled_intfs:
        assert intf in enabled_intfs
    for intf in enabled_intfs:
        assert intf in exp_enabled_intfs

    row = status.to_table_row()
    assert row[0] == "mock"
    assert row[1] == "True"
    assert row[2] == "65536"
    assert row[3] == "2"
    assert row[4] == "172.19.133.104"
    assert row[5] == "10.4.35.254"
    assert row[6] == "10.4.16.1,10.4.20.1:6344"
    assert row[7] == "et-0/0/0:1,et-0/0/1:0,et-0/0/1:1,et-0/0/3"
    assert row[8] == "154128"
    assert row[9] == ""



def test_parse_sonic_table_dell():
    table = textwrap.dedent("""
    sonic# show interface transceiver summary
    --------------------------------------------------------------------------------------------------------------------------------------
    Interface    Name                                    Vendor            Part No.          Serial No.        QSA Adapter       Qualified
    --------------------------------------------------------------------------------------------------------------------------------------
    Eth1/1       QSFP56-DD 400GBASE-DR4                  Intel Corp        SPTSHP2PMCDF      CRDR233404SWP     N/A               False
    Eth1/2       QSFP56-DD 400GBASE-DR4                  Intel Corp        SPTSHP2PMCDF      CRDR2334043EP     N/A               False
    Eth1/3       QSFP56-DD 400GBASE-DR4                  Intel Corp        SPTSHP2PMCDF      CRDR2333075HP     N/A               False
    Eth1/4       QSFP56-DD 400GBASE-DR4                  Intel Corp        SPTSHP2PMCDF      CRDR2335027EP     N/A               False
    """)
    rows = parse_dell_sonic_table(table)
    assert len(rows) == 4
    assert rows[0] == {"Interface": "Eth1/1", "Name": "QSFP56-DD 400GBASE-DR4", "Vendor": "Intel Corp",
                       "PartNo.": "SPTSHP2PMCDF", "SerialNo.": "CRDR233404SWP", "QSAAdapter": "N/A",
                       "Qualified": "False"}


def test_parse_sonic_table_edgecore():
    table = textwrap.dedent("""
admin@sc-r7ra18-400gsw-ec:~$ show int status
  Interface                            Lanes    Speed    MTU    Oper FEC            Alias    Vlan    Oper    Admin    ProtoDown    Eff Admin                                             Type    Asym PFC    Oper Speed
-----------  -------------------------------  -------  -----  ----------  ---------------  ------  ------  -------  -----------  -----------  -----------------------------------------------  ----------  ------------
  Ethernet0  130,131,132,133,134,135,136,137     400G   9100          rs      Eth1(Port1)   trunk      up       up        False           up  QSFP-DD Double Density 8X Pluggable Transceiver         N/A          400G
  Ethernet8  138,139,140,141,142,143,144,145     400G   9100          rs      Eth2(Port2)  routed      up       up        False           up  QSFP-DD Double Density 8X Pluggable Transceiver         N/A          400G
 Ethernet16  146,147,148,149,150,151,152,153     400G   9100          rs      Eth3(Port3)  routed    down       up        False           up                                              N/A         N/A          400G
 Ethernet24  154,155,156,157,158,159,160,161     400G   9100          rs      Eth4(Port4)  routed    down       up        False           up                                              N/A         N/A          400G
    """)
    rows = parse_edgecore_sonic_table(table)
    assert len(rows) == 4
    assert rows[0]["Interface"] == "Ethernet0"
    assert rows[0]["Oper"] == "up"


def test_parse_sonic_structured_output():
    out = """
Ethernet220: SFP EEPROM detected
        Application Advertisement:
                1: 400GAUI-8 C2M (Annex 120E) | 400GBASE-DR4 (Cl 124)
                2: 100GAUI-2 C2M (Annex 135G) | 100GBASE-DR (Cl 140)
                3: IB HDR (Arch.Spec.Vol.2) | 400GBASE-DR4 (Cl 124)
                4: IB HDR (Arch.Spec.Vol.2) | 100GBASE-DR (Cl 140)
        Connector: MPOx12

Ethernet230: SFP EEPROM detected
        Application Advertisement:
                1: 400GAUI-8 C2M (Annex 120E) | 400GBASE-DR4 (Cl 124)
                2: 100GAUI-2 C2M (Annex 135G) | 100GBASE-DR (Cl 140)
                3: IB HDR (Arch.Spec.Vol.2) | 400GBASE-DR4 (Cl 124)
                4: IB HDR (Arch.Spec.Vol.2) | 100GBASE-DR (Cl 140)
        Connector: MPOx12

Ethernet224: SFP EEPROM Not detected

Ethernet228: SFP EEPROM Not detected
"""
    rv = parse_sonic_structured_output(out)
    assert len(rv) == 4
    assert len(rv["Ethernet220"]) == 2
    assert len(rv["Ethernet220"]["Application Advertisement"]) == 4


def test_hp_list_interfaces():
    shell = unittest.mock.MagicMock()
    shell.__enter__.return_value = shell
    shell.exec.side_effect = [
        "en",
        "no-paging",
        (ASSETS_DIR / "files" / "hp_show_interface_status.json").read_text(),
        (ASSETS_DIR / "files" / "hp_show_interface_transceiver.json").read_text(),
    ]
    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.shell_session.return_value = shell
    ctl = HpeOnyxSwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    infs = ctl.list_interfaces()
    assert len(infs) == 3
    inf0 = infs[0]
    assert inf0.name == "Eth1/1"
    assert inf0.normalized_port == 0
    assert inf0.speed == 100 * GBPS
    assert inf0.transceiver == "200GBASE-CR4"
    assert inf0.up

    inf1 = infs[1]
    assert inf1.name == "Eth1/2"
    assert inf1.normalized_port == 1
    assert not inf1.up

    inf2 = infs[2]
    assert inf2.name == "Eth1/3"
    assert inf2.transceiver == ""
    assert not inf2.up


def test_breakout_estimate():
    accum = _PortSpeedAccum(800)
    assert accum.estimate_current_mode(0) == BreakoutMode.Mode800X1

    accum.port_speed_current[1] = 100
    accum.port_inf_count_current[1] = 8
    assert accum.estimate_current_mode(1) == BreakoutMode.Mode100X8

    accum.port_speed_current[2] = 400
    accum.port_inf_count_current[2] = 2
    assert accum.estimate_current_mode(2) == BreakoutMode.Mode400X2

    accum.port_speed_current[3] = 800
    accum.port_inf_count_current[3] = 1
    assert accum.estimate_current_mode(3) == BreakoutMode.Mode800X1

    # unknown mode - default to native
    accum.port_speed_current[4] = 100
    accum.port_inf_count_current[4] = 6
    assert accum.estimate_current_mode(4) == BreakoutMode.Mode800X1


def test_hostname_parsing():
    def make_exec_mock(*output: str):
        shell = unittest.mock.MagicMock()
        shell.__enter__.return_value = shell
        shell.exec.side_effect = output
        exec = unittest.mock.Mock(spec=ExecMixin)
        exec.shell_session.return_value = shell
        return exec

    expected = "test-123"

    # output is copied from a real terminal session with this switch vendor
    arista_json = json.dumps({"hostname": expected, "fqdn": expected + ".foobar"}, indent=2)
    juniper_json = json.dumps({"system-information": [{"host-name": [{"data": expected}]}]}, indent=2)

    test_cases = {
        ("DL", SwitchOSFamily.DELL_SONIC,): (DellSonicSwitchCtl, [f"login message\nhostname\n{expected}\nprompt>"]),
        ("DL", SwitchOSFamily.DELL_OS10): (DellOS10SwitchCtl, [f"login message\nsystem hostname\n{expected}\nprompt>"]),
        ("EC", SwitchOSFamily.EDGECORE_SONIC): (EdgecoreSwitchCtl, [f"login message$\nhostname\n{expected}\nprompt$"]),
        ("HP", SwitchOSFamily.HPE): (HpeOnyxSwitchCtl, [f"login message\n", f"show running-config | include hostname\n\n  hostname {expected}\nprompt>"]),
        ("AR", SwitchOSFamily.ARISTA_EOS): (AristaSwitchCtl, ["", f"login message\nshow hostname | json\n{arista_json}\n{{master: 0}}\nprompt>"]),
        ("JU", SwitchOSFamily.JUNIPER_JUNOS): (JuniperSwitchCtl, [f"login message\nshow system information | display json | no-more\n{juniper_json}\nprompt>"]),
    }
    for (vendor, switch_os), (ctl_cls, cmd_output) in test_cases.items():
        try:
            ctl = ctl_cls("test", expected, "test", "test")
            ctl._exec = make_exec_mock(*cmd_output)
            hostname = ctl.get_hostname()
        except Exception as e:
            assert False, f"vendor {vendor} os {switch_os} raised an exception: {e}"
        assert hostname == expected, f"vendor {vendor} hostname didn't match expected: {hostname} != {expected}"


def test_dell_os10_list_interfaces():
    lldp_doc = (ASSETS_DIR / "files" / "dell_os10_interfaces_detail.xml").read_text()

    def mock_lldp_call(*args, **kwargs):
        return 0, lldp_doc, ""

    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.exec = mock_lldp_call

    ctl = DellOS10SwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    infs = ctl.list_interfaces()

    assert len(infs) == 4
    assert infs[0].up
    assert infs[0].speed == 1

    assert not infs[1].up
    assert infs[1].normalized_port == 39

    assert infs[2].up
    assert infs[2].speed == 10


def test_dell_os10_lldp():
    lldp_doc = (ASSETS_DIR / "files" / "dell_os10_lldp.xml").read_text()

    def mock_lldp_call(*args, **kwargs):
        return 0, lldp_doc, ""

    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.exec = mock_lldp_call

    ctl = DellOS10SwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    neighbors = ctl.list_lldp_neighbors()
    assert len(neighbors) == 3, "Invalid number of neighbors detected"
    for n in neighbors:
        assert "mgmt" not in n.src_if, "Management interface included in output"
        assert n.src_if, "Source interface not detected"
        assert n.dst_name, "Destination not detected"
        assert n.dst_if, "Destination interface not detected"
        assert n.dst_if_mac, "Destination mac not detected"

def test_dell_get_mac_addr_table():
    get_mac_addr_table_doc = (ASSETS_DIR / "files" / "dell_get_mac_addr_table.xml").read_text()

    def mock_get_mac_addr_table_call(*args, **kwargs):
        return 0, get_mac_addr_table_doc, ""

    exec = unittest.mock.Mock(spec=ExecMixin)
    exec.exec = mock_get_mac_addr_table_call

    ctl = DellOS10SwitchCtl("mock", "mock", "mock", "mock")
    ctl._exec = exec
    entries = ctl.get_mac_addr_table()
    assert len(entries) == 3, "Invalid number of entries detected"
    # first entry
    assert entries[0].switch == "mock", "Wrong switch name"
    assert entries[0].name == "ethernet1/1/48", "Wrong interface name"
    assert entries[0].mac == "00:0a:9c:68:6b:5b", "Wrong mac address"
    # second entry
    assert entries[1].switch == "mock", "Wrong switch name"
    assert entries[1].name == "ethernet1/1/46", "Wrong interface name"
    assert entries[1].mac == "00:0a:9c:68:6b:ff", "Wrong mac address"
    # third entry
    assert entries[2].switch == "mock", "Wrong switch name"
    assert entries[2].name == "ethernet1/1/47", "Wrong interface name"
    assert entries[2].mac == "00:0a:9c:68:6c:2c", "Wrong mac address"

def test_switch_model_support():
    exec = unittest.mock.Mock(spec=ExecMixin)
    shell = unittest.mock.MagicMock()
    shell.__enter__.return_value = shell
    exec.shell_session.return_value = shell
    switches = [
        (
            AristaSwitchCtl,
            [
                ("""Arista DCS-7060DX5-64S-F
Hardware version: 11.05
Serial number: FGN241202EX""", "7060DX5", True),
                ("", "", False),
            ]
        ),
        (
            HpeSwitchCtl,
            [
                ("MSN3700cM", "3700", True),
                ("""                                 ^
 % Unrecognized command found at '^' position.""", "12908", True),
                ("", "", False),
            ]
        ),
        (
            JuniperSwitchCtl,
            [
                ("""Hostname: net001-lf-sw01
Model: qfx5240-64od
Junos: 23.4R2-S3.11-EVO""", "qfx5240-64od", True),
                ("", "", False),
            ]
        ),
        (
            DellSonicSwitchCtl,
            [
                ("""
SONiC Software Version: SONiC-OS-4.2.1-Enterprise_Premium
Product: Enterprise SONiC Distribution by Dell Technologies
Distribution: Debian 10.13
Kernel: 5.10.0-21-amd64
Config DB Version: version_4_2_2
Build commit: f63532edf6
Build date: Fri Apr 12 03:33:02 UTC 2024
Built by: sonicbld@bld-lvn-csg-01

Platform: x86_64-dell_z9664f-r0
HwSKU: DellEMC-Z9664f-O64
ASIC: broadcom""", "Z9664f", True),
                ("", "", False),
            ]
        ),
        (
            EdgecoreSwitchCtl,
            [
                ("""
SONiC Software Version: SONiC.Edgecore-SONiC_20240722_085018_ec202111_b657_hsdk_6.5.23_6
Distribution: Debian 11.10
Kernel: 5.10.0-8-2-amd64
Build commit: 1a169dde5
Build date: Mon Jul 22 14:50:14 UTC 2024
Built by: ubuntu@ip-10-5-1-181

Platform: x86_64-accton_as9736_64d-r0
HwSKU: Accton-AS9736-64D
ASIC: broadcom""", "AS9736", True),
                ("", "", False),
            ]
        ),
    ]

    for sctl, cases in switches:
        ctl = sctl("mock", "mock", "mock", "mock")
        ctl._exec = exec
        for model_out, expected_model, supported in cases:
            exec.exec.return_value = (0, model_out, "")
            # for HPE
            shell.exec.side_effect = [
                "en",
                model_out
            ]
            model = ctl.get_model()
            assert model == expected_model, f"Expected model {expected_model}, got {model}"
            assert supported == ctl.model_support().is_supported_model(model), f"is_supported_model = {not supported}, should be {supported}"

