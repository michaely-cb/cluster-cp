import abc
import csv
import io
import json
import logging
import re
import typing
from concurrent.futures import ThreadPoolExecutor
from importlib import resources
from typing import Dict, List

import paramiko

from deployment_manager.db.models import Device
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.utils import exec_remote_cmd, ping_check
import deployment_manager.db.device_props as props
from .device_status import DeviceStatus, Status
from ..ssh import SSHConn

logger = logging.getLogger(__name__)

class DeviceValidator(abc.ABC):
    """
    Validate device configurations.
    """

    def __init__(self, profile: str, parallelism=20):
        self._profile = profile
        self._parallelism = parallelism

    def get_device(self, device_name: str) -> Device:
        device = Device.get_device(device_name, self._profile)
        if not device:
            raise ValueError(f"can't find device {device_name}")
        return device

    def validate(self, targets: List[str]) -> Dict:
        """
        Args:
            targets (List[str]): target devices to be validated
        """
        result = {}

        with ThreadPoolExecutor(max_workers=self._parallelism) as executor:
            future_to_device = {
                executor.submit(self._run, tgt): tgt for tgt in targets
            }
            for f, device in future_to_device.items():
                try:
                    status, msg = f.result()
                    dev_status = DeviceStatus(status, msg)
                    result[device] = dev_status
                except Exception as e:
                    result[device] = DeviceStatus(Status.FAILED, f"failed: {e}")
        return result

    @abc.abstractmethod
    def _run(self, target: str) -> typing.Tuple[Status, str]:
        raise NotImplementedError


class CascadeValidator(DeviceValidator):
    """
    Run multiple validators
    """

    def __init__(
        self,
        cg: ConfGen,
        parallelism=1,
        validators: typing.List[typing.Type[DeviceValidator]] = [],
    ):
        super().__init__(cg, parallelism)
        self._validators = [
            validator_cls(cg, parallelism) for validator_cls in validators
        ]

    def _run(self, target: str) -> typing.Tuple[Status, str]:
        for validator in self._validators:
            status, msg = validator._run(target)
            if status == Status.FAILED:
                return Status.FAILED, msg

        return Status.OK, ""


class IpmiValidator(DeviceValidator):
    """
    Validate IPMI connectivity.

    This validator will ping and check login info for given devices' IPMI
    """

    def __init__(self, cg: ConfGen, parallelism=1, only_ping=False):
        """
        Args:
            cg (ConfGen): master config
            parallelism (int, optional): num of threads. Defaults to 1.
            ping_check (bool, optional): only perform ping. Defaults to False.
        """
        super().__init__(cg, parallelism)
        self._only_ping = only_ping

    def _run(self, target: str) -> typing.Tuple[Status, str]:
        device = self.get_device(target)
        ipmi_ip, ipmi_username, ipmi_password, vendor = device.get_ipmi_client_args()

        reachable = ping_check(ipmi_ip)
        if not reachable:
            return Status.FAILED, "ping check failed"

        if self._only_ping:
            return Status.OK, "ping check passed"

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            ssh.connect(
                ipmi_ip,
                username=ipmi_username,
                password=ipmi_password,
                disabled_algorithms={
                    'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']
                },
                look_for_keys=False,
            )
            return Status.OK, ""
        except Exception as e:
            return Status.FAILED, f"{e}"
        finally:
            ssh.close()


class OsValidator(DeviceValidator):
    """
    Validate OS installation

    This validator will ping and try to login each device
    """

    def _run(self, target: str) -> typing.Tuple[Status, str]:
        device = self.get_device(target)
        name = device.name
        username = device.get_prop(props.prop_management_credentials_user)
        password = device.get_prop(props.prop_management_credentials_password)
        reachable = ping_check(name)
        if not reachable:
            return Status.FAILED, f"failed to ping {name}"

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try_ssh_params = [
            {"look_for_keys": True, "password": None},  # Attempt 1: Key-based login
            {"look_for_keys": False, "password": "root"},  # Attempt 2: OS default password
            {"look_for_keys": False, "password": password},  # Attempt 3: Provided password
        ]

        err_msg = ""
        for i, params in enumerate(try_ssh_params):
            try:
                ssh.connect(
                    name,
                    port=22,
                    username=username,
                    password=params["password"],
                    timeout=5,
                    allow_agent=False,
                    look_for_keys=params["look_for_keys"],
                )
                return Status.OK, f"ssh {username}@{name} succeeded"
            except Exception as e:
                err_msg = f"failed to ssh {username}@{name}, got {e.__class__.__name__}: {e}"
            finally:
                ssh.close()
        return Status.FAILED, err_msg


class NicValidator(DeviceValidator):
    """
    Validate 100G NIC status

    This class will check:
    1. NIC name changed to `eth100g[0-6]` for non mgmt node
    2. NIC link status
    3. NIC count
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        cg = ConfGen(self._profile)
        self._role_nic_count = cg.get_expected_role_nic_counts()
        self._get_nic_state_path = str(resources.files(__package__).joinpath("resources", "get_nic_state.sh"))


    def _check_netdev(self, netdev, role) -> typing.Tuple[bool, str]:
        # check nic name pattern
        if role != "MG":
            nic_name = netdev["ifname"]
            if not re.match(r"eth\d+00g\d+", nic_name):
                return False, nic_name

        # check link status
        if netdev["operstate"] != "UP":
            return False, f"{netdev['ifname']}:{netdev['operstate']}"

        return True, ""

    def _run(self, target: str) -> typing.Tuple[Status, str]:
        device = self.get_device(target)
        name = device.name
        role = device.device_role
        username = device.get_prop(props.prop_management_credentials_user)
        password = device.get_prop(props.prop_management_credentials_password)

        expect_count = self._role_nic_count[role]

        try:
            with SSHConn(name, username, password) as conn:
                conn.scp_file(self._get_nic_state_path, "/tmp/get_nic_state.sh")
                rv, stdout, stderr = conn.exec("bash /tmp/get_nic_state.sh")
                conn.exec("rm -f /tmp/get_nic_state.sh")
        except Exception as e:
            return Status.FAILED, f"failed to exec get_nic_state.sh on {name}: {e}"

        if rv != 0:
            return Status.FAILED, f"failed to run get_nic_state.sh on {name}: {stderr}"

        # parse the csv output
        up, down = [], []
        for if_info in csv.DictReader(io.StringIO(stdout), delimiter=','):
            if int(if_info["max_speed_mbps"]) < 100_000:
                continue
            if if_info["operstate"].strip().lower() != "up":
                down.append(if_info['name'])
            else:
                up.append(if_info['name'])

        down_warning = f". Note there were {len(down)} down interfaces: {','.join(down)}" if down else ""

        if len(up) == expect_count:
            return Status.OK, f"found {expect_count} expected NICs{down_warning}"
        elif len(up) > expect_count:
            warning_msg = f"warning: up NIC count greater than expected ({len(up)}>{expect_count} ){down_warning}"
            return Status.OK, f"{warning_msg}"
        else:
            error_msg = f"error: up NIC count less than expected ({len(up)}<{expect_count}). DOWN: {','.join(down)}"
            return Status.FAILED, f"{error_msg}"
