from unittest.mock import MagicMock

from deployment_manager.tools.pb1.bios_settings import DellBiosSettings, SMBiosSettings, HpeBiosSettings
from deployment_manager.tools.pb1.ipmi import IPMI


def test_get_bios_settings_dell():
    dell_bios = DellBiosSettings()
    assert dell_bios.get_settings("id") == dell_bios.get_settings("ax"), "ID and AX should have the same settings"

    # smoke test dell
    for role in ["ax", "id", "mx", "sx", "mg", "us", "wk"]:
        settings = dell_bios.get_settings(role)
        assert isinstance(settings, dict), f"Settings for {role} should be a dict"


def test_get_bios_settings_supermicro():
    ipmi = MagicMock(IPMI)
    ipmi.get_system = MagicMock(return_value={"Model": "as-1115hs-tnr"})

    bios = SMBiosSettings(ipmi)
    settings = bios.get_settings("mx")
    assert settings["settings.xml"].endswith("config/mx/sm_mx_bios.xml"), "Settings should be for MX"


def test_get_bios_settings_hpe():
    # smoke test
    hpe_bios = HpeBiosSettings()
    for role in ["ax", "id", "mx", "sx", "mg", "us", "wk"]:
        settings = hpe_bios.get_settings(role)
        assert isinstance(settings, dict), f"Settings for {role} should be a dict"