import abc
import json
import logging
from abc import abstractmethod
from typing import Dict

from pkg_resources import resource_string, resource_filename

from deployment_manager.tools.pb1.ipmi import IPMI

logger = logging.getLogger(__name__)


class BiosSettings(abc.ABC):
    @abstractmethod
    def get_settings(self, role: str) -> Dict:
        """ Get settings dict for IPMI BIOS update """
        raise NotImplementedError(f"{self.__class__}::get not implemented")

    def check_supported_model(self, ipmi, role: str):
        """ Check if the device model is supported for the given role """
        pass

    @abstractmethod
    def vendor(self) -> str:
        raise NotImplementedError(f"{self.__class__}::get_vendor not implemented")


class DellBiosSettings(BiosSettings):
    COMMON_SETTINGS = dict(BootMode="Bios")
    ROLE_SETTINGS = {
        "ax": "./config/ax/dell_ax_bios.json",
        "id": "./config/ax/dell_ax_bios.json",  # same as AX
        "mx": "./config/mx/dell_mx_bios.json",
        "sx": "./config/sx/dell_sx_bios.json",
    }

    def vendor(self):
        return "DL"

    def get_settings(self, role: str) -> Dict:
        settings = self.COMMON_SETTINGS.copy()
        if role in self.ROLE_SETTINGS:
            settings.update(json.loads(
                resource_string(__name__, self.ROLE_SETTINGS[role])
            ))
        return settings


class HpeBiosSettings(BiosSettings):
    COMMON_SETTINGS = dict(
        NetworkBootRetry="Disabled",
        BootMode="LegacyBios",
        NetworkBootRetryCount=1,
    )
    ROLE_SETTINGS = {
        "mx": "./config/mx/hpe_mx_bios.json",
        "sx": "./config/sx/hpe_sx_bios.json",
    }

    def vendor(self):
        return "HP"

    def get_settings(self, role: str) -> Dict:
        settings = self.COMMON_SETTINGS.copy()
        if role in self.ROLE_SETTINGS:
            settings.update(json.loads(
                resource_string(__name__, self.ROLE_SETTINGS[role])
            ))
        return settings

    def check_supported_model(self, ipmi, role: str):
        if role == "mx":
            self.check_supported_mx(ipmi)
        elif role == "sx":
            self.check_supported_sx(ipmi)

    def check_supported_mx(self, ipmi):
        system_info = ipmi.get_system()
        cpu_info = system_info["ProcessorSummary"]["Model"]
        sku_info = system_info["SKU"]

        if ("7713P" not in cpu_info) and ("7543P" not in cpu_info):
            raise Exception(
                f"Unsupported CPU {cpu_info} for {self.vendor()} MX"
            )

        if "P21163-B21" not in sku_info:
            raise Exception(
                f"Unsupported SKU {sku_info} for {self.vendor()} MX"
            )

    def check_supported_sx(self, ipmi):
        system_info = ipmi.get_system()
        cpu_info = system_info["ProcessorSummary"]["Model"]
        sku_info = system_info["SKU"]

        if "7513" not in cpu_info:
            raise Exception(
                f"Unsupported CPU {cpu_info} for {self.vendor()} SX"
            )

        if "P21163-B21" not in sku_info:
            raise Exception(
                f"Unsupported SKU {sku_info} for {self.vendor()} SX"
            )


class SMBiosSettings(BiosSettings):
    """ Supermicro is only used in one production cluster role today, the XXL memoryx """
    MINIMAL_SETTINGS_XML = "config/sm_minimal_bios.xml"
    MXXL_GOLDEN_SETTINGS_XML = "config/mx/sm_mx_bios.xml"

    def __init__(self, ipmi: IPMI):
        self._ipmi = ipmi

    def vendor(self):
        return "SM"

    def get_settings(self, role: str) -> Dict:
        settings = {"settings.xml": resource_filename(__name__, self.MINIMAL_SETTINGS_XML)}
        if role != "mx":
            return settings

        # mxxl have special requirements for bios settings
        system_info = self._ipmi.get_system()
        chassis_model = system_info["Model"].replace(" ", "").lower()
        if "as-1115hs-tnr" == chassis_model:
            settings = {"settings.xml": resource_filename(__name__, self.MXXL_GOLDEN_SETTINGS_XML)}
        else:
            # We only use supermicro for mxxl otherwise dell
            logger.warning(f"Unknown supermicro chassis model '{chassis_model}'. "
                           "Using minimal supermicro bios config. This may not work!")

        return settings

class LenovoBiosSettings(BiosSettings):
    COMMON_SETTINGS = dict(
       BootModes_SystemBootMode="LegacyMode",
       Memory_NUMANodesperSocket="NPS1"
    )
    ROLE_SETTINGS = {
        "sx": "./config/sx/lenovo_sx_bios.json",
    }

    def vendor(self):
        return "LN"

    def get_settings(self, role: str) -> Dict:
        settings = self.COMMON_SETTINGS.copy()
        if role in self.ROLE_SETTINGS:
            settings.update(json.loads(
                resource_string(__name__, self.ROLE_SETTINGS[role])
            ))
        return settings
    



def get_bios_settings(device_role: str, ipmi: IPMI) -> dict:
    role = device_role.lower()
    vendor = ipmi.get_vendor()
    if vendor == "HP":
        bios_settings = HpeBiosSettings()
    elif vendor == "DL":
        bios_settings = DellBiosSettings()
    elif vendor == "SM":
        bios_settings = SMBiosSettings(ipmi)
    elif vendor == "LN":
        bios_settings = LenovoBiosSettings()
    else:
        raise ValueError(
            f"Unsupported vendor {vendor} for BIOS settings update"
        )

    bios_settings.check_supported_model(ipmi, role)

    return bios_settings.get_settings(role)
