from concurrent.futures import ThreadPoolExecutor

import logging
from tabulate import tabulate
from typing import Dict, Optional

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import DeploymentDeviceTypes
from deployment_manager.db.models import Device
from deployment_manager.tools.pb1.device_status import DeviceStatus, Status
from deployment_manager.tools.pb1.ipmi import (FanSpeedMode, with_ipmi_client)
from deployment_manager.tools.pb1.server_task import ServerPowerAction
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)

IPMI_THREADPOOL_PARALLELISM = 20


def show_task_status(result: Dict[str, DeviceStatus]):
    header = ["device", "status", "message"]
    rows = []
    for device, status in result.items():
        rows.append([device, status.status.value, status.message])

    print(tabulate(rows, header))


def show_device_status(
    status: Dict[str, str], status_attr: Optional[str] = None
):
    if status_attr:
        header = ["device", status_attr]
    else:
        header = ["device"]
    rows = []
    for device, message in status.items():
        if status_attr:
            rows.append([device, message])
        else:
            rows.append([device])

    print(tabulate(rows, header))


class ServerPower(SubCommandABC):
    """
    Use IPMI redfish API to power on/off/cycle of the server
    """

    name = "power"

    def construct(self):
        self.parser.add_argument(
            "--action",
            help="specify power action",
            type=str,
            required=True,
            choices=["on", "off", "cycle"],
        )
        self.add_arg_noconfirm()
        self.add_arg_filter(required=True)

    def run(self, args):
        targets = self.filter_devices(
            args,
            device_type=DeploymentDeviceTypes.SERVER.value,
            query_set=Device.get_all(self.profile),
        )

        def check_power_state(device: Device) -> str:
            try:
                with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                    state = ipmi.get_server_power_state()
                    return state
            except Exception as e:
                return f"{e}"

        with ThreadPoolExecutor(IPMI_THREADPOOL_PARALLELISM) as e:
            results = e.map(check_power_state, targets)
            show_device_status(
                dict(zip([d.name for d in targets], results)), "power"
            )

        if not args.noconfirm and not prompt_confirm("Continue?"):
            return 0

        logger.info(f"Powering {args.action}....")
        action = ServerPowerAction(self.profile, args.action)
        result = action.run([d.name for d in targets])
        show_task_status(result)


class SetHostHeaderCheck(SubCommandABC):
    """
    Enable or disable host header check for Dell idrac. Allows Dell idrac to
    accept API calls on hostnames which are different from the hostname
    assigned over DHCP, e.g. 'net001-mx-sr01-ipmi' versus default 'idracabc0123'

    Example: server ipmi set_idrac_host_check DISABLE -f name=wse001-mg-sr01
    """

    name = "set_idrac_host_check"

    def construct(self):
        self.parser.add_argument(
            "option",
            choices=["ENABLE", "DISABLE"],
            help="Enable or disable host header check",
        )
        self.add_arg_noconfirm()
        self.add_arg_filter(required=True)

    def run(self, args):
        targets = self.filter_devices(
            args,
            device_type=DeploymentDeviceTypes.SERVER.value,
            query_set=Device.get_all(self.profile),
        )
        opt = True if args.option == "ENABLE" else False

        def check_host_header_status(device: Device) -> str:
            try:
                with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                    if ipmi.get_vendor() != "DL":
                        return "N/A (not idrac)"
                    enabled = ipmi.get_host_header_check()
                    return "ENABLE" if enabled else "DISABLE"
            except Exception as e:
                return f"{e}"

        with ThreadPoolExecutor(IPMI_THREADPOOL_PARALLELISM) as e:
            results = e.map(check_host_header_status, targets)
            show_device_status(dict(zip([d.name for d in targets], results)), "mode")

        if not args.noconfirm and not prompt_confirm("Continue?"):
            return 0

        def set_host_header_status(device: Device) -> DeviceStatus:
            try:
                with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                    updated = ipmi.set_host_header_check(opt)
                    return DeviceStatus(Status.OK,
                                        f"host header check {args.option} {'updated' if updated else 'unchanged'}")
            except NotImplementedError as e:
                return DeviceStatus(Status.OK, f"{e}")
            except Exception as e:
                return DeviceStatus(Status.FAILED, f"IPMI exception: {e}")

        logger.info(f"Setting idrac host check {args.option}")

        with ThreadPoolExecutor(IPMI_THREADPOOL_PARALLELISM) as e:
            results = e.map(set_host_header_status, targets)
            show_task_status(dict(zip([d.name for d in targets], results)))


class SetFanSpeed(SubCommandABC):
    """ Use IPMI to set fan speed. May not be implemented on all server vendors. """

    name = "set_fan_speed"

    def construct(self):
        self.parser.add_argument(
            "--mode",
            help="specify fan speed",
            type=str,
            choices=[FanSpeedMode.HIGH.name, FanSpeedMode.DEFAULT.name],
            required=True,
        )

        self.add_arg_noconfirm()
        self.add_arg_filter(required=True)

    def run(self, args):
        targets = self.filter_devices(
            args,
            device_type=DeploymentDeviceTypes.SERVER.value,
            query_set=Device.get_all(self.profile),
        )

        mode = FanSpeedMode.DEFAULT if args.mode == FanSpeedMode.DEFAULT.name else FanSpeedMode.HIGH

        def set_fan_speed(device: Device) -> DeviceStatus:
            try:
                with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                    updated = ipmi.set_fan_speed(mode)
                    return DeviceStatus(Status.OK, f"mode {mode.name} {'updated' if updated else 'unchanged'}")
            except NotImplementedError as e:
                return DeviceStatus(Status.OK, f"{e}")
            except Exception as e:
                return DeviceStatus(Status.FAILED, f"{e}")

        show_device_status(dict([(d.name, "") for d in targets]))

        if not args.noconfirm and not prompt_confirm("Continue?"):
            return 0

        logger.info(f"Setting fan speed {mode.name}")
        with ThreadPoolExecutor(IPMI_THREADPOOL_PARALLELISM) as e:
            results = e.map(set_fan_speed, targets)
            show_task_status(dict(zip([d.name for d in targets], results)))


class CreateUser(SubCommandABC):
    """
    Create or update an IPMI User.

    Limitations:
    - There is a maximum number of IPMI users, different for each vendor. This call does not check the max is reached
    - Dell has 16 user slots. This call maps Admin, ReadOnly, Operator role to the last 3 slots so creating 2
      ReadOnly users is not possible in the current scheme
    """

    name = "create_user"

    def construct(self):
        self.parser.add_argument(
            "--username", help="IPMI account username", type=str, required=True
        )

        self.parser.add_argument(
            "--password", help="IPMI account password", type=str, required=True
        )

        self.parser.add_argument(
            "--role",
            help="IPMI account role",
            type=str,
            choices=["Administrator", "ReadOnly", "Operator"],
            required=True,
        )

        self.add_arg_noconfirm()
        self.add_arg_filter(required=True)

    def run(self, args):
        targets = self.filter_devices(
            args,
            device_type=DeploymentDeviceTypes.SERVER.value,
            query_set=Device.get_all(self.profile),
        )

        def create_user(device: Device) -> DeviceStatus:
            try:
                with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                    ipmi.create_or_update_user(args.username, args.password, args.role)
                    return DeviceStatus(Status.OK, "")
            except Exception as e:
                return DeviceStatus(Status.FAILED, f"{e}")

        show_device_status(dict([(d.name, "") for d in targets]))
        if not args.noconfirm and not prompt_confirm("Continue?"):
            return 0
        logger.info(f"Creating {args.role} user {args.username}")
        with ThreadPoolExecutor(IPMI_THREADPOOL_PARALLELISM) as e:
            results = e.map(create_user, targets)
            show_task_status(dict(zip([d.name for d in targets], results)))


class SetNicMode(SubCommandABC):
    """
    Set IPMI NIC connectivity mode.
    In shared mode, IPMI and MGMT will be sharing the same physical port
    In dedicated mode, IPMI and MGMT will have their own dedicated
    physical port
    """

    name = "set_nic_mode"

    def construct(self):

        self.parser.add_argument(
            "--mode",
            help="specify ipmi mode",
            type=str,
            choices=["DEDICATED", "SHARED"],
            required=True,
        )

        self.add_arg_noconfirm()
        self.add_arg_filter(required=True)

    def run(self, args):
        targets = self.filter_devices(
            args,
            device_type=DeploymentDeviceTypes.SERVER.value,
            query_set=Device.get_all(self.profile),
        )

        def check_ipmi_mode(device: Device) -> str:
            try:
                with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                    shared = ipmi.check_ipmi_nic_shared()
                    return "SHARED" if shared else "DEDICATED"
            except Exception as e:
                return f"{e}"

        with ThreadPoolExecutor(IPMI_THREADPOOL_PARALLELISM) as e:
            results = e.map(check_ipmi_mode, targets)
            show_device_status(dict(zip([d.name for d in targets], results)), "mode")

        if not args.noconfirm and not prompt_confirm("Continue?"):
            return 0

        def set_ipmi_mode(device: Device) -> DeviceStatus:
            try:
                with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                    if args.mode == "SHARED":
                        ipmi.set_ipmi_nic_shared()
                        if ipmi.get_vendor() == "HP":
                            ipmi.reset_ipmi()
                        return DeviceStatus(Status.OK, "")
                    elif args.mode == "DEDICATED":
                        ipmi.set_ipmi_nic_dedicated()
                        if ipmi.get_vendor() == "HP":
                            ipmi.reset_ipmi()
                        return DeviceStatus(Status.OK, "")
                    else:
                        assert False, f"Unsupported mode {args.mode}"
            except Exception as e:
                return DeviceStatus(Status.FAILED, f"{e}")

        logger.info(f"Setting IPMI mode {args.mode}")
        with ThreadPoolExecutor(IPMI_THREADPOOL_PARALLELISM) as e:
            results = e.map(set_ipmi_mode, targets)
            show_task_status(dict(zip([d.name for d in targets], results)))

class ClearLogs(SubCommandABC):
    """
    Clear IPMI system event logs
    """
    
    name = "clear_logs"
    
    def construct(self):
        self.add_arg_noconfirm()
        self.add_arg_filter(required=True)
    
    def run(self, args):
        targets = self.filter_devices(
            args,
            device_type=DeploymentDeviceTypes.SERVER.value,
            query_set=Device.get_all(self.profile),
        )
        
        def clear_logs(device: Device) -> DeviceStatus:
            try:
                with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                    ipmi.clear_system_event_log()
                return DeviceStatus(Status.OK, "Logs cleared")
            except Exception as e:
                return DeviceStatus(Status.FAILED, f"{e}")
        
        show_device_status(dict([(d.name, "") for d in targets]))
        
        if not args.noconfirm and not prompt_confirm("clear event logs? y/n"):
            return 0
            
        with ThreadPoolExecutor(IPMI_THREADPOOL_PARALLELISM) as e:
            results = e.map(clear_logs, targets)
            show_task_status(dict(zip([d.name for d in targets], results)))

class IpmiAction(SubCommandABC):
    """
    IPMI related actions
    """

    SUB_CMDS = [ServerPower, SetFanSpeed, CreateUser, SetNicMode, SetHostHeaderCheck, ClearLogs]

    name = "ipmi"

    def construct(self):
        subparsers = self.parser.add_subparsers(dest="action", required=True)
        for ssc in self.SUB_CMDS:
            m = ssc(
                subparsers, profile=self.profile, cli_instance=self.cli_instance
            )
            m.build()

    def run(self, args):
        if hasattr(args, "func"):
            args.func(args)
