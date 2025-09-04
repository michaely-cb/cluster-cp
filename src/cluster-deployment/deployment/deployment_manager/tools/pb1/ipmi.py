import abc
import contextlib
import logging
import re
import time
import typing
from enum import Enum
from functools import cached_property
from typing import List, Optional

import redfish
import redfish.rest.v1
from redfish.rest import v1 as redfish_v1

from deployment_manager.tools.utils import exec_cmd

logging.getLogger("redfish").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

IPMI_REBOOT_ACTIONS = {"On", "ForceOff", "GracefulShutdown", "ForceRestart", "Nmi", "PushPowerButton", "GracefulRestart"}


class FanSpeedMode(Enum):
    DEFAULT = 0
    HIGH = 1


class IPMIError(RuntimeError):
    def __init__(self, message, extended_err: Optional[dict] = None):
        super().__init__()
        self.message = message
        self._extended_err: Optional[dict] = extended_err

    def __str__(self):
        return f"{self.message}, extended error={self._extended_err}"


def check_ok(resp: redfish.rest.v1.RestResponse, fail_msg: str):
    if 200 <= resp.status < 300:
        return
    extended_err = None
    try:
        extended_err = resp.dict['error']['@Message.ExtendedInfo']
    except KeyError:
        pass
    raise IPMIError(f"{resp.request.method} {resp.request.path} "
                    f"returned code {resp.status}: {fail_msg}", extended_err)


class IPMI(abc.ABC):
    """
    IPMI "intelligent platform management interface" base class. Vendor specific
    IPMI classes should override this base class and override methods taking
    advantage of vendor specific redfish functions.
    """

    def __init__(self, addr: str, username: str, password: str, rf_client: redfish.rest.v1.HttpClient=None):
        self._addr = addr
        self._username = username
        self._password = password

        self._logger: logging.Logger = logger.getChild(f"BMC[{self._addr}]")
        self._rf = rf_client
        if self._rf is None:
            self._rf: redfish.rest.v1.HttpClient = redfish.redfish_client(
                self._addr, username=self._username, password=self._password
            )
            self._rf.login()
        self._jobs_uri = "/redfish/v1/JobService/Jobs"

    def close(self):
        try:
            self._rf.logout()
        except Exception as e:
            logger.warning(f"ipmi {self._addr} user {self._username} failed to log out cleanly: {e}")

    def _must_get(self, uri) -> redfish.rest.v1.RestResponse:
        resp = self._rf.get(uri)
        if resp.status == 401:
            # long held session can expire
            logger.debug(f"GET {self._addr}/{uri} failed with 401. Trying to login and retry")
            self._rf.login()
            resp = self._rf.get(uri)
        check_ok(resp, f"failed to get uri {uri}")
        self._logger.debug(f"{uri} -> {resp.text}")
        return resp

    @abc.abstractmethod
    def get_vendor(self) -> str:
        raise NotImplemented("get_vendor not implemented")

    @cached_property
    def _bios_uri(self):
        return self._system_uri + "/Bios"

    @cached_property
    def _manager_uri(self):
        resp = self._must_get("/redfish/v1/Managers")
        return resp.dict["Members"][0]["@odata.id"]

    @cached_property
    def _system_uri(self):
        resp = self._must_get("/redfish/v1/Systems")
        return resp.dict["Members"][0]["@odata.id"]

    @cached_property
    def _chassis_uri(self):
        resp = self._must_get("/redfish/v1/Chassis")
        return resp.dict["Members"][0]["@odata.id"]

    @cached_property
    def _power_uri(self):
        return self._chassis_uri + "/Power"

    def _find_account_with_username(self, username) -> typing.Tuple[str, dict]:
        """
        Args:
            username:

        Returns: uri of account, account body
        """
        usernames = set()
        resp = self._must_get("/redfish/v1/AccountService/Accounts")
        for item in resp.dict["Members"]:
            r = self._must_get(item["@odata.id"])
            if r.dict["UserName"] == username:
                return item["@odata.id"], r.dict
            usernames.add(r.dict["UserName"])
        raise IPMIError(f"failed to find account with username {username}. Found: {usernames}")

    def _create_bios_settings_dict(self, settings: typing.Dict[str, str]) -> typing.Dict[str, str]:
        """
        Args:
            settings (typing.Dict[str, str]): bios setting dict

        Returns: bios setting dict that will need to be applied
        """
        bios_data = self._must_get(self._bios_uri)
        update = {}
        for k, v in settings.items():
            cur_value = bios_data.dict.get('Attributes', {}).get(k)
            if not cur_value:
                self._logger.warning(f"cannot find {k} in bios registry")

            if v != cur_value:
                update[k] = v

        return update

    def get_bios_settings(self) -> dict:
        """ Get BIOS configuration dict. "Attributes" contains the dict of bios settings """
        response = self._must_get(self._system_uri + "/Bios")
        return response.dict

    def update_bios_config_privilege(self, username):
        """
        Update the IMPI user to have privilege to modify BIOS configurations
        Args:
            username: IPMI account username
        """
        # TODO: override for Dell, Supermicro
        pass

    def onetimeboot_from_pxe(self):
        """
        Initialize PXE boot
        """
        raise NotImplementedError("onetimeboot_from_pxe")

    def get_bios_boot_order(self) -> List[str]:
        """
        Returns: List of Boot devices
        """
        pass

    def is_boot_order_hdd(self) -> bool:
        """
        Returns true if HDD is higher than PXE in boot order
        """
        pass

    def update_bios_settings(self, settings: typing.Dict[str, str], bios_password: Optional[str] = None):
        """
        Args:
            settings: k/v pairs to update
            bios_password: Optional bios password
        """
        pass

    def wait_for_bios_settings_update(self, timeout: int = 900):
        """
        Args:
            timeout: Time in seconds to wait for completion
        """
        logger.info(f"Waiting for BIOS settings update for {timeout} seconds")
        time.sleep(timeout)

    def get_system(self) -> dict:
        """
        Returns: /redfish/v1/Systems/<1> dict
        """
        return self._must_get(self._system_uri).dict

    def get_server_power_state(self) -> str:
        response = self._must_get(self._system_uri)
        return response.dict.get('PowerState', 'Unknown')

    def get_server_processor_details(self) -> str:
        resp = self._must_get(self._system_uri + "/Processors")
        resp = self._must_get(resp.dict["Members"][0]["@odata.id"])
        return resp.dict.get('Model', 'unknown')

    def get_server_aggregate_health(self) -> typing.Tuple[bool, dict]:
        """
        Returns:
            bool: health is OK
            dict: health_status details
        """
        response = self._must_get(self._system_uri)
        # TODO: extract more fields with subcomponent health
        status = response.dict["Status"]
        return "OK" == status["Health"], status

    def reset_ipmi(self):
        """ Force reset. Mimics pushing the physical reset power button """
        pass

    def reboot_server(self, reboot_action: str):
        """ Reboot the server """
        if reboot_action not in IPMI_REBOOT_ACTIONS:
            raise ValueError(f"invalid reboot option: {reboot_action}")

        resp = self._must_get(self._system_uri)
        reboot_target = resp.dict['Actions']['#ComputerSystem.Reset']['target']
        body = {
            "Action": 'ComputerSystem.Reset',
            'ResetType': reboot_action,
        }
        resp = self._rf.post(reboot_target, body=body)
        check_ok(resp, "failed to reboot system")

    def find_mgmt_mac_address(self) -> str:
        """ Server MAC address """
        resp = self._must_get(self._system_uri + "/EthernetInterfaces")

        mgmt_macs = []
        for adapter in resp.dict['Members']:
            adapter_resp = self._must_get(adapter["@odata.id"])
            if adapter_resp.dict["LinkStatus"] == "LinkUp":
                mgmt_macs.append(adapter_resp.dict["MACAddress"].lower())

        if len(mgmt_macs) == 0:
            raise IPMIError("mgmt link not up")
        elif len(mgmt_macs) > 2:
            raise IPMIError("discovered more than two mgmt ports")

        mgmt_macs.sort()
        return mgmt_macs[0]

    def find_ipmi_mac_address(self) -> Optional[str]:
        """ IPMI mac address """
        resp = self._must_get(self._manager_uri + "/EthernetInterfaces")
        for adapter in resp.dict['Members']:
            adapter_resp = self._must_get(adapter["@odata.id"])
            if adapter_resp.dict["Status"]["State"] != "Enabled":
                continue
            return adapter_resp.dict["MACAddress"].lower()
        return None

    def one_psu_ok(self) -> bool:
        """ Check if at least 1 Power Supply Unit is OK """
        resp = self._must_get(self._power_uri)
        power_supplies = resp.dict['PowerSupplies']
        for ps in power_supplies:
            status = ps["Status"]
            if status["Health"] == "OK" and status["State"] == "Enabled":
                return True

        return False

    def set_ipmi_nic_dedicated(self):
        """
        Some vendors support a shared bmc mode where a single BMC NIC is shared
        between 4 physical servers.
        After performing this operation, reset_bmc must be performed for the
        operation to complete.
        """
        pass

    def set_ipmi_nic_shared(self):
        """
        Set ipmi NIC shared with MGMT NIC
        """
        pass

    def check_ipmi_nic_shared(self) -> Optional[bool]:
        """
        Returns: True if the bmc interface is in shared mode.
        """
        return False

    def set_fan_speed(self, mode: FanSpeedMode) -> bool:
        """
        Set fan speed. Different vendors will have different ways of configuring this. Try to generalize to
        "default" and "high" modes where default is the vendor default and high is the max possible speed.
        Args:
            mode: FanSpeedMode

        Returns: true if modified
        """
        raise NotImplementedError(f"set_fan_speed not implemented for {self.__class__}")

    def create_or_update_user(self, username: str, password: str, role: str):
        """
        Create IPMI user with role Administrator, Operator, or ReadOnly
        """
        if role not in ["Administrator", "Operator", "ReadOnly"]:
            raise ValueError(f"Unsupported role {role}")
        if self._username == username:
            raise ValueError("must not update IPMI client's current user")

        return self._create_or_update_user(username, password, role)

    def _create_or_update_user(self, username: str, password: str, role: str):
        """
        Create IPMI readonly user
        """
        raise NotImplemented("not implemented for IPMI vendor")

    def set_host_header_check(self, enable: bool) -> bool:
        """
        Enforce that the hostname in the IPMI request matches the IPMI device's hostname
        """
        raise NotImplementedError(f"set_host_header_check not implemented for {self.__class__}")

    def clear_system_event_log(self):
        """
        Clear the system event log
        """
        raise NotImplementedError(f"clear_system_event_log not implemented for {self.__class__}")

class LenovoIPMI(IPMI):
    """
    Lenovo IPMI controller
    See also https://pubs.lenovo.com/xcc3-restapi/xcc3_restapi_book.pdf
    """

    def get_vendor(self):
        return "LN"

    def find_mgmt_mac_address(self) -> str:
        """ Server MAC address """
        resp = self._must_get(self._system_uri + "/EthernetInterfaces")

        mgmt_macs = []
        for adapter in resp.dict['Members']:
            adapter_resp = self._must_get(adapter["@odata.id"])
            d = adapter_resp.dict
            desc = d.get("Description", "").lower()
            if "100g" in desc or "host network" in desc:
                continue
            if d["LinkStatus"] == "LinkUp":
                mgmt_macs.append(d["MACAddress"].lower())

        if len(mgmt_macs) == 0:
            raise IPMIError("mgmt link not up")
        elif len(mgmt_macs) > 2:
            raise IPMIError("discovered more than two mgmt ports")

        mgmt_macs.sort()
        return mgmt_macs[0]

    def _create_or_update_user(self, username: str, password: str, role: str):
        account_service_uri = "/redfish/v1/AccountService"
        account_collection_uri = account_service_uri + "/Accounts"

        # https://pubs.lenovo.com/xcc3-restapi/xcc3_restapi_book.pdf (Page 24)

        # list accounts, identify if one already exists with our name
        users_list = self._must_get(account_collection_uri)
        existing_user_uri = None
        for account in users_list.dict.get("Members", []):
            user_uri = account.get("@odata.id")
            user = self._must_get(user_uri)
            if user.dict.get("UserName") == username:
                existing_user_uri = user_uri
                break

        body = {
            "RoleId": role,
            "UserName": username,
            "Password": password,
            "Enabled": True
        }

        if existing_user_uri:
            resp = self._rf.delete(existing_user_uri)
            check_ok(resp, f"failed to remove account {username}")
        resp = self._rf.post(account_collection_uri, body=body)
        check_ok(resp, f"failed to create account {username}")

    def update_bios_settings(self, settings: typing.Dict[str,str], bios_password: Optional[str]=None):
        """ Update BIOS settings """

        update = self._create_bios_settings_dict(settings)

        if not update:
            return

        if bios_password:
            self._rf.bios_password = bios_password

        bios_data = self._must_get(self._bios_uri)
        bios_settings_uri = bios_data.obj['@Redfish.Settings']['SettingsObject']['@odata.id']
        body = {'Attributes': update}
        resp = self._rf.patch(bios_settings_uri, body=body)
        check_ok(resp, f"failed to update bios setting {update}")
    

    def get_bios_boot_order(self) -> List[str]:
        resp = self._must_get(self._system_uri)
        return resp.dict.get("Boot", {}).get("BootOrder", [])

    def clear_system_event_log(self):
        body = {"Action": "ClearLog"}
        resp = self._rf.post(self._system_uri + "/LogServices/SEL/Actions/LogService.ClearLog", body=body)
        check_ok(resp, "failed to clear system event log")

    def check_ipmi_nic_shared(self) -> Optional[bool]:
        """
        Returns: True if the bmc interface is in shared mode.
        """
        resp = self._must_get(self._manager_uri + "/EthernetInterfaces/NIC")
        nic_mode = resp.dict.get("Oem", {}).get("Lenovo", {}).get("InterfaceNicMode", {}).get("NicMode")
        if nic_mode is not None:
            return nic_mode.lower() == "shared"
        return False

    def onetimeboot_from_pxe(self):
        """ Initialize PXE boot """
        system_uri = self._system_uri
        body = {"Boot": {"BootSourceOverrideTarget": "Pxe", "BootSourceOverrideEnabled": "Once", }}
        resp = self._rf.patch(system_uri, body=body)
        check_ok(resp, "failed to set onetimeboot from pxe")

    def reset_ipmi(self):
        """ Force reset. Mimics pushing the physical reset power button """
        resp = self._must_get(self._manager_uri)
        reset_ilo_uri = resp.dict['Actions']['#Manager.Reset']['target']
        resp = self._rf.post(reset_ilo_uri, body={'Action': 'ForceRestart'})
        check_ok(resp, "failed to ForceRestart ipmi")

        """ IPMI mac address """
        resp = self._must_get(self._manager_uri + "/EthernetInterfaces/NIC")
        if resp.status == 404:
            return None
        if resp.status != 200:
            raise IPMIError(f"failed to get IPMI MAC address: {resp.status}")
        return resp.dict.get("MACAddress", "").lower() or None
    
    def set_ipmi_nic_dedicated(self):
        cmd = f"ipmitool -H {self._addr} -U {self._username} -P {self._password} -I lanplus raw 0xc 1 1 0xc0 1 0"
        exec_cmd(cmd, throw=True, logcmd=False)
    
    def set_ipmi_nic_shared(self):
        cmd = f"ipmitool -H {self._addr} -U {self._username} -P {self._password} -I lanplus raw 0xc 1 1 0xc0 2 0"
        exec_cmd(cmd, throw=True, logcmd=False)
    
class SMIPMI(IPMI):
    """ SuperMicro IPMI controller
    See Also https://www.supermicro.com/manuals/other/redfish-user-guide-4-0
    """

    def get_vendor(self):
        return "SM"

    def find_mgmt_mac_address(self) -> str:
        """ Server MAC address """
        resp = self._must_get(self._system_uri + "/EthernetInterfaces")

        mgmt_macs = []
        for adapter in resp.dict['Members']:
            adapter_resp = self._must_get(adapter["@odata.id"])
            d = adapter_resp.dict
            desc = d.get("Description", "").lower()
            if "100g" in desc or "host network" in desc:
                continue
            # avoid doing health check for supermicro - seen in devtest that
            # links can be Disabled but still accessible on the network
            mgmt_macs.append(d["MACAddress"].lower())

        if len(mgmt_macs) == 0:
            raise IPMIError("mgmt link not up")
        elif len(mgmt_macs) > 2:
            raise IPMIError("discovered more than two mgmt ports")

        mgmt_macs.sort()
        return mgmt_macs[0]

    def onetimeboot_from_pxe(self):
        """
        Initialize pxe boot for supermicro server since SM requires special IPMI license to enable redfish support
        """
        # local exec of ipmitool
        cmd = f"ipmitool -H {self._addr} -U {self._username} -P {self._password}"
        exec_cmd(f"{cmd} -I lanplus chassis power off", throw=True, logcmd=False)
        exec_cmd(f"{cmd} -I lanplus chassis bootdev pxe", throw=True, logcmd=False)

    def reset_ipmi(self):
        """ Force reset. Mimics pushing the physical reset power button """
        resp = self._must_get(self._manager_uri)
        reset_ilo_uri = resp.dict['Actions']['#Manager.Reset']['target']
        resp = self._rf.post(reset_ilo_uri, body={'Action': 'ForceRestart'})
        check_ok(resp, "failed to ForceRestart ipmi")

    def _create_or_update_user(self, username: str, password: str, role: str):
        account_service_uri = "/redfish/v1/AccountService"
        account_collection_uri = account_service_uri + "/Accounts"

        # https://www.supermicro.com/manuals/other/redfish-ref-guide-html/Content/general-content/account-service.htm
        # We don't have correct license installed in Colo
        # Not licensed to perform this request. The following licenses DCMS  were needed

        # list accounts, identify if one already exists with our name
        users_list = self._must_get(account_collection_uri)
        existing_user_uri = None
        for account in users_list.dict.get("Members", []):
            user_uri = account.get("@odata.id")
            user = self._must_get(user_uri)
            if user.dict.get("UserName") == username:
                existing_user_uri = user_uri
                break

        body = {
            "RoleId": role,
            "UserName": username,
            "Password": password,
            "Enabled": True
        }

        if existing_user_uri:
            resp = self._rf.patch(existing_user_uri, body=body)
            check_ok(resp, f"failed to update account {username}")
        else:
            resp = self._rf.post(account_collection_uri, body=body)
            check_ok(resp, f"failed to create account {username}")

    def set_fan_speed(self, mode: FanSpeedMode) -> bool:
        manager_res = self._must_get(self._manager_uri)
        fan_uri = None
        for k, v in manager_res.dict.get("Oem", {}).get("Supermicro", {}).items():
            if k == "FanMode":
                fan_uri = v.get("@odata.id", None)
                break
        if not fan_uri:
            logger.warning("unable to find fan URI in supermicro Oem output, using default")
            fan_uri = "/redfish/v1/Managers/1/Oem/Supermicro/FanMode"
        res = self._must_get(fan_uri)

        mode_map = {
            "FullSpeed": FanSpeedMode.HIGH, "Optimal": FanSpeedMode.DEFAULT,
            FanSpeedMode.HIGH: "FullSpeed", FanSpeedMode.DEFAULT: "Optimal",
        }
        # handle older version of SM API which uses "standard" instead of "optimal"
        if "Standard" in res.dict.get("Mode@Redfish.AllowableValues", []):
            mode_map[FanSpeedMode.DEFAULT] = "Standard"
            mode_map["Standard"] = FanSpeedMode.DEFAULT

        current_mode = res.dict.get("Mode", "Unknown")
        if mode_map.get(current_mode) == mode:
            return False
        res = self._rf.patch(fan_uri, body={"Mode": mode_map[mode]})
        check_ok(res, "failed to update fan mode")
        return True

    def get_bios_boot_order(self) -> List[str]:
        resp = self._must_get(f"{self._system_uri}/Oem/Supermicro/FixedBootOrder")
        return resp.dict.get("FixedBootOrder", [])

    def is_boot_order_hdd(self) -> bool:
        for device in self.get_bios_boot_order():
            if device.startswith("Hard"):
                return True
            if device.startswith("Network"):
                return False
        else:
            return False

    def update_bios_settings(self, settings: typing.Dict[str, str], bios_password: Optional[str] = None):
        """
        Supermicro requires updating through the special SUM tool. Their redfish API does not support configuring
        all BIOS parameters such as boot mode (UEFI versus Legacy). Requires reboot afterwards to take effect.
        Args:
            settings: a dict with a single arg 'settings.xml' which is the path to the xmlfile for the SUM tool
            bios_password: Ignored
        """
        if "settings.xml" not in settings:
            raise ValueError("supermicro BIOS update requires a single argument 'settings.xml'")
        exec_cmd(f"/opt/bin/sum -i {self._addr} -u {self._username} -p {self._password} -c ChangeBiosCfg "
                 f"--file {settings['settings.xml']} --skip_unknown --skip_bbs", throw=True)

    def clear_system_event_log(self):
        """
        Clear the chassis intrusion sensor detection
        """
        # clear chassis intrusion sensor
        body = {
            "PhysicalSecurity": {
            "IntrusionSensor": "Normal"
            }
        }
        resp = self._rf.patch(self._chassis_uri, body=body)
        check_ok(resp, "failed to clear chassis intrusion sensor")


class DellIPMI(IPMI):
    """ Dell IPMI controller """

    def get_vendor(self):
        return "DL"

    def reset_ipmi(self):
        """ Force reset. Mimics pushing the physical reset power button """
        resp = self._must_get(self._manager_uri)
        reset_ilo_uri = resp.dict['Actions']['#Manager.Reset']['target']
        resp = self._rf.post(reset_ilo_uri, body={'ResetType': 'GracefulRestart'})
        check_ok(resp, "failed to GracefulRestart ipmi")

    def onetimeboot_from_pxe(self):
        """ Initialize PXE boot """
        # NOTE: if there's already an OS installed you should DISABLE network boot on the mellanox NICS since the
        # Flexboot loader will try the first mellanox card, fail, and then revert to HDD boot instead of trying the next
        # NIC. There are some options for disabling PCIE slot device network boot (e.g. Slot8NicBoot1) but docs say this
        # only works in UEFI mode:
        # https://servermanagementportal.ext.hpe.com/docs/redfishservices/ilos/ilo6/ilo6_166/ilo6_bios_resourcedefns166/#bios
        # An alternative is to wipe or corrupt the boot partition on the HDD
        system_uri = self._system_uri
        body = {"Boot": {"BootSourceOverrideTarget": "Pxe", "BootSourceOverrideEnabled": "Once", }}
        resp = self._rf.patch(system_uri, body=body)
        check_ok(resp, "failed to set onetimeboot from pxe")

    def get_bios_boot_order(self) -> List[str]:
        resp = self._must_get(self._system_uri)
        return resp.dict.get("Boot", {}).get("BootOrder", [])

    def is_boot_order_hdd(self) -> bool:
        for device in self.get_bios_boot_order():
            if device.startswith("Hard"):
                return True
            if device.startswith("NIC"):
                return False
        else:
            return False

    def _create_boot_order(self) -> str:
        """
        Seen cases where HDD was not a boot device.
        Try to set the HDD as first boot device, embedded nic as second, nics as remaining
        """

        resp = self._rf.get(self._system_uri + "/BootOptions")
        if 200 != resp.status:
            self._logger.warning(f"failed to get boot options: {resp}")
            return ""
        opts = []
        for opt in resp.dict["Members"]:
            opts.append(opt["@odata.id"].split("/")[-1])

        def priority(o: str):
            if o.startswith("HardDisk"):
                return 0
            if o.startswith("NIC.Embedded"):
                return 1
            if o.startswith("NIC"):
                return 2
            return 3

        return ",".join(sorted(opts, key=lambda o: (priority(o), o)))


    def update_bios_settings(self, settings: typing.Dict[str, str], bios_password: Optional[str]=None):
        """ Update BIOS settings """
        if not "SetBootOrderEn" in settings:
            override_order = self._create_boot_order()
            if override_order:
                # Note: in UEFI boot mode, the boot options will be different than in BIOS mode.
                # In practice, the HDD will be first option on switching to BIOS mode but in rare cases, if the
                # initial OS install fails because the HDD was not set as the first boot device on changing to
                # BIOS mode, then this override should fix the issue
                if not "HardDisk" in override_order:
                    logger.warning(f"{self._addr} 'HardDisk' not in boot options ({override_order}). Skipping updating boot order")
                else:
                    settings["SetBootOrderEn"] = override_order

        update = self._create_bios_settings_dict(settings)

        if not update:
            return

        if bios_password:
            self._logger.warning("bios password will be ingored for Dell servers")
        self._logger.info(f"updating BIOS settings: {update}")

        bios_data = self._must_get(self._bios_uri)
        bios_settings_uri = bios_data.obj['@Redfish.Settings']['SettingsObject']['@odata.id']
        body = {'Attributes': update}
        body.update({"@Redfish.SettingsApplyTime": {"ApplyTime": "OnReset"}})
        resp = self._rf.patch(bios_settings_uri, body=body)
        check_ok(resp, f"failed to update bios setting {update}")

    def _get_bios_update_job_id(self) -> Optional[str]:
        resp = self._must_get(self._jobs_uri)
        job_id = None
        for m in resp.dict["Members"]:
            job_uri = m["@odata.id"]
            job_resp = self._must_get(job_uri)
            if "ConfigBIOS" in job_resp.dict["Name"]:
                # Experiments showed that there was just one instance of this type
                # of job irrespective of the number of times bios config was updated
                job_id = job_uri.split('/')[-1]
                break
        return job_id

    def wait_for_bios_settings_update(self, timeout: int = 360):
        job_id = self._get_bios_update_job_id()
        if job_id is None:
            logger.warning(f"Unable to get information about BIOS settings update status. Waiting for {timeout}s")
            time.sleep(timeout)
        else:
            retry_interval = timeout // 10
            end_time = time.time() + timeout
            perc = 0
            while time.time() < end_time:
                time.sleep(retry_interval)
                try:
                    resp = self._must_get(f"{self._jobs_uri}/{job_id}")
                except (redfish.rest.RetriesExhaustedError, IPMIError) as e:
                    self._logger.warning(f"{self._addr}: failed to get job status: {e.__class__.__name__} - {e}")
                    continue
                # Not sure how failure is reported so in failure case
                # we will wait for timeout
                perc = resp.dict["PercentComplete"]
                if perc == 100:
                    break
            else:
                raise IPMIError(f"BIOS settings update did not complete in {timeout}s, {perc}% completed")

    def find_mgmt_mac_address(self) -> str:
        """ Server MAC address """
        resp = self._must_get(self._system_uri + "/EthernetInterfaces")

        mgmt_macs = []
        for adapter in resp.dict['Members']:
            adapter_resp = self._must_get(adapter["@odata.id"])
            if "Embedded" not in adapter_resp.dict["Description"]:
                continue

            if adapter_resp.dict["LinkStatus"] == "LinkUp":
                mgmt_macs.append(adapter_resp.dict["MACAddress"].lower())

        if len(mgmt_macs) == 0:
            raise IPMIError("mgmt link not up")
        elif len(mgmt_macs) > 2:
            raise IPMIError("discovered more than two mgmt ports")

        mgmt_macs.sort()
        return mgmt_macs[0]

    def get_server_aggregate_health(self) -> typing.Tuple[bool, dict]:
        """
        Returns:
            bool: health is OK
            dict: health_status details
        """
        response = self._must_get(self._system_uri)
        status = response.dict["Status"]

        try:
            response = self._must_get(self._system_uri + "/Oem/Dell/DellRollupStatus")
            unhealthy_items = {}
            for member in response.dict["Members"]:
                member_health = member["RollupStatus"]
                if member_health != "Ok":
                    unhealthy_items[member["SubSystem"]] = member_health

            return "OK" == status["Health"], unhealthy_items
        except Exception as e:
            # TODO: to support Dell R6515 health check
            return "OK" == status["Health"], {}

    def set_fan_speed(self, mode: FanSpeedMode) -> bool:
        if mode == FanSpeedMode.DEFAULT:
            target = {
                "ThermalSettings.1.ThermalProfile": "Default Thermal Profile Settings",
                "ThermalSettings.1.FanSpeedOffset": "Off",
                "ThermalSettings.1.MinimumFanSpeed": 255  # "Default"
            }
        elif mode == FanSpeedMode.HIGH:
            target = {
                "ThermalSettings.1.ThermalProfile": "Maximum Performance",
                "ThermalSettings.1.FanSpeedOffset": "Max",  # direct max amount of fan to PCIe cards
                "ThermalSettings.1.MinimumFanSpeed": 50
            }
        else:
            raise ValueError(f"invalid fan mode: {mode}")
        return self._set_attribute(target, False)

    def check_ipmi_nic_shared(self) -> Optional[bool]:
        active_nic =  self._get_attribute("CurrentNIC.1.ActiveNIC", True)
        if active_nic.lower() != "dedicated".lower():
            return True
        else:
            return False

    def set_ipmi_nic_dedicated(self):
        settings = {"NIC.1.Selection": "Dedicated"}
        self._set_attribute(settings, True)

    def set_ipmi_nic_shared(self):
        settings = {"NIC.1.Selection": "LOM1"}
        self._set_attribute(settings, True)

    def _create_or_update_user(self, username: str, password: str, role: str):
        # Dell idrac has 16 possible user slots - use the last 3 for the roles we support
        role_to_id = {
            "Administrator": 14,
            "Operator": 15,
            "ReadOnly": 16
        }
        user_id = role_to_id[role]

        account_service_uri = "/redfish/v1/AccountService"
        account_collection_uri = account_service_uri + f"/Accounts/{str(user_id)}"

        # check if the username exists, and delete the user if its userid != user_id
        for aid in range(1, 17):
            uri = account_service_uri + f"/Accounts/{aid}"
            user = self._must_get(uri)
            if user.dict.get("UserName") == username and aid != user_id:
                body = {"UserName": "", "Enabled": False}
                resp = self._rf.patch(uri, body=body)
                check_ok(resp, f"failed to delete existing user with same name at {uri}")
                break

        body = {
            "RoleId": role,
            "UserName": username,
            "Password": password,
            "Enabled": True
        }

        resp = self._rf.patch(account_collection_uri, body=body)
        check_ok(resp, f"failed to create account {username} with id {user_id}")

    def set_host_header_check(self, enable: bool):
        """

        By default, iDRAC8 checks the HTTP or HTTPS Host Header and compares
        it to the defined 'DNSRacName' and 'DNSDomainName'.
        When the values do not match, the iDRAC refuses the HTTP or
        HTTPS connection.

        This function enable or disable web server host header check

        Args:
            enable (bool): enable/disable

        """
        option = "Enabled" if enable else "Disabled"
        settings = {
            "WebServer.1.HostHeaderCheck": option
        }
        self._set_attribute(settings, True)

    def get_host_header_check(self) -> bool:
        """
        Check if host header check is enabled.

        return True if enabled
        """
        status = self._get_attribute("WebServer.1.HostHeaderCheck", True)
        if status == "Enabled":
            return True
        elif status == "Disabled":
            return False
        else:
            raise IPMIError(f"Unrecognized host header check status: {status}")

    def _set_attribute(self, settings: typing.Dict[str, str], is_idrac) -> bool:
        """
        Set iDrac or System attributes

        Args:
            settings (typing.Dict[str, str]): Settings
            is_idrac (bool): set idrac attribute if True otherwise set system attribute

        Returns:
            bool: True if settings applied
        """
        target = "iDRAC" if is_idrac else "System"
        attr_uri = self._manager_uri + f"/Oem/Dell/DellAttributes/{target}.Embedded.1"
        attr_data = self._must_get(attr_uri)
        attr_settings_uri = attr_data.obj['@Redfish.Settings']['SettingsObject']['@odata.id']
        attr_dict = self._must_get(attr_uri).obj["Attributes"]

        patch_body = {}
        for k, v in settings.items():
            if attr_dict[k] != v:
                patch_body[k] = v

        if patch_body:
            patch_body = {"Attributes": patch_body}
            patch_body.update({"@Redfish.SettingsApplyTime":{"ApplyTime":"Immediate"}})
            resp = self._rf.patch(attr_settings_uri, body=patch_body)
            check_ok(resp, f"failed to update idrac setting {settings}")
            return True
        return False

    def _get_attribute(self, attr: str, is_idrac: bool) -> str:
        """
        Get iDrac or System attributes
        Args:
            attr (str): attribute name
            is_idrac (bool): set idrac attribute if True otherwise set system attribute

        Returns:
            str: attribute value
        """
        target = "iDRAC" if is_idrac else "System"
        attr_uri = self._manager_uri + f"/Oem/Dell/DellAttributes/{target}.Embedded.1"
        attr_data = self._must_get(attr_uri)
        return attr_data.obj["Attributes"][attr]

    def clear_system_event_log(self):
        """
        Clear the system event log
        """
        # clear system event log
        body = {"Action": "ClearLog"}
        resp = self._rf.post(self._manager_uri + "/LogServices/Sel/Actions/LogService.ClearLog", body=body)
        check_ok(resp, "failed to clear system event log")


class HPEIPMI(IPMI):
    """ Hewlett-Packard IPMI controller """

    @property
    def _resource_directory(self) -> List[dict]:
        try:
            resp = self._must_get("/redfish/v1/")
            resource_uri = resp.obj.Oem.Hpe.Links.ResourceDirectory['@odata.id']
        except KeyError:
            raise IPMIError(f"ResourceDirectory not present in BMC info on server {self._addr}")

        response = self._must_get(resource_uri)
        return response.dict["Instances"]

    @cached_property
    def _system_uri(self) -> str:
        for instance in self._resource_directory:
            if '#ComputerSystem.' in instance['@odata.type']:
                return instance['@odata.id']
        raise IPMIError(f"#ComputerSystem type not in redfish resources: {self._resource_directory}")

    @cached_property
    def _manager_uri(self) -> str:
        for instance in self._resource_directory:
            if '#Manager.' in instance['@odata.type']:
                return instance['@odata.id']
        raise IPMIError(f"#Manager type not in redfish resources: {self._resource_directory}")

    @property
    def _bmc_eth_interfaces_uri(self) -> Optional[str]:
        for instance in self._resource_directory:
            if '#EthernetInterfaceCollection.' in instance['@odata.type'] and 'Managers' in instance['@odata.id']:
                return instance['@odata.id']
        return None

    @property
    def _server_eth_interfaces_uri(self) -> Optional[str]:
        for instance in self._resource_directory:
            if 'BaseNetworkAdapterCollection' in instance['@odata.type'] and 'System' in instance['@odata.id']:
                return instance['@odata.id']
        raise IPMIError("failed to find BaseNetworkAdapterCollection")

    def _query_eth_data(self) -> Optional[dict]:
        eth_data = {}
        ethernet_uri = self._bmc_eth_interfaces_uri
        if not ethernet_uri:
            return None
        eth_resp = self._must_get(ethernet_uri)
        for inf_details in eth_resp.dict['Members']:
            eth_id = inf_details['@odata.id']
            inf_resp = self._must_get(eth_id)
            eth_data[inf_details['@odata.id']] = inf_resp.dict
        return eth_data

    def get_vendor(self) -> str:
        return "HP"

    def update_bios_config_privilege(self, username):
        url, resp_body = self._find_account_with_username(username)
        has_priv = resp_body.get("Oem", {}).get("Hpe", {}).get("Privileges", {}).get("HostBIOSConfigPriv", False)
        if has_priv:
            self._logger.debug("skip updating HostBIOSConfigPriv since it's already set")
            return
        body = {"Oem": {"Hpe": {"Privileges": {"HostBIOSConfigPriv": True}}}}
        resp = self._rf.patch(url, body=body)
        check_ok(resp, f"failed to update account {username} HostBIOSConfigPriv")

    def get_bios_boot_order(self) -> List[str]:
        resp = self._must_get(self._system_uri)
        return resp.dict.get("Boot", {}).get("BootOrder", [])

    def is_boot_order_hdd(self) -> bool:
        for device in self.get_bios_boot_order():
            if device.startswith("Hdd"):
                return True
            if device.startswith("Pxe"):
                return False
        else:
            return False

    def onetimeboot_from_pxe(self):
        """ Initialize PXE boot """
        system_uri = self._system_uri
        body = {"Boot":{"BootSourceOverrideTarget":"Pxe"}}
        resp = self._rf.patch(system_uri, body=body)
        check_ok(resp, "failed to set onetimeboot from pxe")

    def update_bios_settings(self, settings: typing.Dict[str,str], bios_password: Optional[str]=None):
        """ Update BIOS settings """

        update = self._create_bios_settings_dict(settings)

        if not update:
            return

        if bios_password:
            self._rf.bios_password = bios_password

        bios_data = self._must_get(self._bios_uri)
        bios_settings_uri = bios_data.obj['@Redfish.Settings']['SettingsObject']['@odata.id']
        body = {'Attributes': update}
        resp = self._rf.patch(bios_settings_uri, body=body)
        check_ok(resp, f"failed to update bios setting {update}")

    def get_server_aggregate_health(self) -> typing.Tuple[bool, dict]:
        response = self._must_get(self._system_uri)
        health = response.dict["Oem"]["Hpe"]["AggregateHealthStatus"]

        critical_items = [
            "BiosOrHardwareHealth",
            "Fans",
            "Memory",
            "Network",
            "PowerSupplies",
            "Processors",
            "Storage",
            "Temperatures"
        ]
        unhealthy_items = {}
        for item in critical_items:
            if item not in health.keys():
                continue
            item_health = health[item]["Status"]["Health"]
            if item_health != "OK":
                unhealthy_items[item] = item_health

        # some server don't have this field
        if "AggregateServerHealth" in health.keys():
            return "OK" in health.get("AggregateServerHealth"), unhealthy_items
        else:
            return True if len(unhealthy_items) == 0 else False, unhealthy_items

    def reset_ipmi(self):
        """ Force reset """
        resp = self._must_get(self._manager_uri)

        reset_ilo_uri = resp.obj['Actions']['#Manager.Reset']['target']
        resp = self._rf.post(reset_ilo_uri, body={'Action': 'ForceRestart'})
        check_ok(resp, "failed to ForceRestart")

    def reboot_server(self, reboot_action: str):
        """ Reboot the server """
        if reboot_action not in IPMI_REBOOT_ACTIONS:
            raise ValueError(f"invalid reboot option: {reboot_action}")

        resp = self._must_get(self._system_uri)

        reboot_target = resp.obj['Actions']['#ComputerSystem.Reset']['target']
        body = {
            "Action": 'ComputerSystem.Reset',
            'ResetType': reboot_action,
        }
        resp = self._rf.post(reboot_target, body=body)
        check_ok(resp, "failed to reboot system")

    def find_mgmt_mac_address(self) -> str:
        """ Server MAC address """
        eth_adapters_uri = self._server_eth_interfaces_uri
        resp = self._must_get(eth_adapters_uri)

        mgmt_macs, candidates = [], []
        for adapter in resp.dict['Members']:
            adapter_uri = adapter["@odata.id"]
            adapter_resp = self._must_get(adapter_uri)
            card_name = adapter_resp.dict["Name"].lower()
            if re.match(r'.*connect-?x.*', card_name):  # mellanox secondary card should be avoided
                continue
            for i, port in enumerate(adapter_resp.dict['PhysicalPorts']):
                if port["LinkStatus"] == "LinkUp":
                    mgmt_macs.append(port["MacAddress"])
                if i == 0:
                    candidates.append(port["MacAddress"])

        if not mgmt_macs and len(candidates) == 1:
            # when OS is installed already, sometimes the link appears down when it is actually up
            # this isn't perfect since the mgmt link could be plugged into the second port but if we
            # don't guess then we'll have to debug later anyways
            return candidates[0]

        if len(mgmt_macs) == 0:
            raise IPMIError("mgmt link not up")
        elif len(mgmt_macs) > 2:
            raise IPMIError("discovered more than two mgmt ports")

        mgmt_macs.sort()
        return mgmt_macs[0]

    def find_ipmi_mac_address(self) -> Optional[str]:
        """ BMC mac address """
        ethernet_data = self._query_eth_data()
        if not ethernet_data:
            return None
        for iface in ethernet_data:
            if ethernet_data[iface]['LinkStatus'] == "LinkUp":
                return ethernet_data[iface].get('MACAddress').lower()
        return None

    def set_ipmi_nic_shared(self):
        eth_data = self._query_eth_data()
        if not eth_data:
            self._logger.warning("eth collection URI not preset, not setting to dedicated mode")
            return

        for interface, data in eth_data.items():
            if "Shared" in data.get('Name'):
                body = {"Oem": {"Hpe": {"SharedNetworkPortOptions": {"NIC": "FlexibleLOM/OCP"}}}}
                body.update({"InterfaceEnabled": True})
                resp = self._rf.patch(interface, body=body)
                check_ok(resp, f"failed to set interface {interface} to shared mode")
                self._logger.debug(f"enabled interface {interface} in shared mode")

    def set_ipmi_nic_dedicated(self):
        eth_data = self._query_eth_data()
        if not eth_data:
            self._logger.warning("eth collection URI not preset, not setting to dedicated mode")
            return

        for interface, data in eth_data.items():
            if "Dedicated" in data.get('Name'):
                resp = self._rf.patch(interface, body={"InterfaceEnabled": True})
                check_ok(resp, f"failed to set interface {interface} to dedicated mode")
                self._logger.debug(f"enabled interface {interface} in dedicated mode")

    def check_ipmi_nic_shared(self) -> Optional[bool]:
        eth_data = self._query_eth_data()
        if not eth_data:
            self._logger.warning("unable to query eth data, unable to check if nic in shared mode")
            return None

        for interface, data in eth_data.items():
            name = data.get('Name', "")
            if "Shared" in name and data.get('InterfaceEnabled', False):
                self._logger.info(f"interface enabled for {name}")
                return True
        return False

    def _create_or_update_user(self, username: str, password: str, role: str):
        account_service_uri = "/redfish/v1/AccountService"
        account_collection_uri = account_service_uri + "/Accounts"

        # list accounts, identify if one already exists with our name
        users_list = self._must_get(account_collection_uri)
        existing_user_uri = None
        for account in users_list.dict.get("Members", []):
            user_uri = account.get("@odata.id")
            user = self._must_get(user_uri)
            if user.dict.get("UserName") == username:
                existing_user_uri = user_uri
                break

        body = {
            "RoleId": role,
            "UserName": username,
            "Password": password,
        }

        if existing_user_uri:
            # PATCH didn't work on some machines tested, got AlreadyExists. Removing/adding works
            resp = self._rf.delete(existing_user_uri)
            check_ok(resp, f"failed to remove account {username}")
        resp = self._rf.post(account_collection_uri, body=body)
        check_ok(resp, f"failed to create account {username}")


def create_ipmi_client(addr: str, username: str, password: str, vendor="", timeout: int=60) -> IPMI:
    https_addr = addr if addr.startswith("http") else f"https://{addr}"
    attempt, end_time = 0, time.time() + timeout
    while True:
        try:
            # IPMI occasionally fails to login especially after a reboot
            rf = redfish.redfish_client(https_addr,
                                        username=username,
                                        password=password,
                                        timeout=10,   # each request, not just login
                                        max_retry=3)  # each request, not just login, will retry max of 3 times
            rf.login()
            break
        except redfish_v1.InvalidCredentialsError as e:
            # some errors are not retryable
            raise IPMIError(f"invalid credentials for ipmi {addr} user {username} to IPMI: {e}")
        except Exception as e:
            attempt += 1
            wait_time = min(attempt * 2, 10)

            if time.time() + wait_time > end_time:
                msg = f"failed to login to ipmi {addr} to IPMI after {attempt} attempts and {timeout} seconds"
                logger.warning(msg)
                raise IPMIError(msg)
            logger.debug(f"IPMI login attempt {attempt} failed, retrying in {wait_time}s ({e.__class__.__name__}::{e})")
            time.sleep(wait_time)

    if not vendor:
        # infer the vendor from Oem field
        oem_mapping = {"supermicro": "SM", "dell": "DL", "hpe": "HP" }
        for k in rf.root["Oem"].keys():
            if k.lower() in oem_mapping:
                vendor = oem_mapping[k.lower()]
                break
        else:
            if rf.root.get("UUID", "").startswith("000000"):
                # Supermicro can return an empty OEM
                logger.warning(f"{username}@{addr} OEM field not specified in redfish response, assuming Supermicro: {rf.root}")
                vendor = "SM"

    if not vendor:
        raise IPMIError(f"unable to find vendor implementation for ipmi: root info = {rf.root}")

    if vendor == "SM":
        return SMIPMI(addr, username, password, rf_client=rf)
    elif vendor == "HP":
        return HPEIPMI(addr, username, password, rf_client=rf)
    elif vendor == "DL":
        return DellIPMI(addr, username, password, rf_client=rf)
    elif vendor == "LN":
        return LenovoIPMI(addr, username, password, rf_client=rf)

    raise AssertionError(f"passed in unimplemented IPMI vendor: {vendor}")


@contextlib.contextmanager
def with_ipmi_client(
        addr: str, username: str, password: str, vendor: str = "", timeout: int = 60
) -> typing.Generator[IPMI, None, None]:
    ipmi = create_ipmi_client(addr, username, password, vendor, timeout)
    try:
        yield ipmi
    finally:
        ipmi.close()
