import logging
import os
import pathlib
import stat
import tempfile
import time
from datetime import datetime, timezone

from paramiko import AuthenticationException
from typing import Dict, List, Tuple

from deployment_manager.db import device_props as props

from .device_status import Status
from .device_task import ParallelDeviceTask, DeviceTask
from .ipmi import IPMIError, with_ipmi_client
from ..config import ConfGen
from ...db.models import Device
from ...tools.utils import exec_remote_cmd
from ...tools.ssh import SSHConn

logger = logging.getLogger(__name__)

class ServerPowerAction(ParallelDeviceTask):
    """
    Send a power command to servers.
    """

    def __init__(
        self, profile: str, action: str, parallelism: int = 20, retry: int = 3, wait: bool = False
    ):
        super().__init__(profile, parallelism)
        self._retry = retry
        self._wait = wait

        action_to_ipmi_action = {
            "cycle": "ForceRestart",
            "on": "On",
            "off": "ForceOff",
        }

        self._ipmi_action = action_to_ipmi_action.get(action)
        if not self._ipmi_action:
            raise ValueError(
                f"invalid action: {action}. Valid options: {action_to_ipmi_action.keys()}"
            )

    def _run(self, target) -> Tuple[Status, str]:
        device = Device.get_device(target, self._profile)

        with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
            err = ""
            for i in range(0, self._retry):
                try:
                    # wait 3 seconds. ipmi may not always respond to
                    # get_server_power_state query if the session just got
                    # created
                    time.sleep(3)
                    if (
                        self._ipmi_action == "ForceOff"
                        and ipmi.get_server_power_state().lower() == "off"
                    ):
                        return Status.OK, "already off" if i == 0 else f"turned off in {i} retries"
                    if (
                        self._ipmi_action == "On"
                        and ipmi.get_server_power_state().lower() == "on"
                    ):
                        return Status.OK, "already on" if i == 0 else f"turned on in {i} retries"

                    # Add another 3 seconds wait after power state checking
                    time.sleep(3)
                    ipmi.reboot_server(self._ipmi_action)
                    if self._wait:
                        desired_state = "off" if self._ipmi_action == "ForceOff" else "on"
                        logger.info(f"Waiting for {target} to power {desired_state}")
                        end_time = time.time() + 60
                        while time.time() < end_time:
                            time.sleep(3)
                            if ipmi.get_server_power_state().lower() == desired_state:
                                return Status.OK, ""
                        return Status.FAILED, f"failed to turn {desired_state} in 60s"
                    else:
                        return Status.OK, ""
                except Exception as e:
                    err += f"{e}"
                    time.sleep(10)
                    continue
            return Status.FAILED, f"failed after {i} retries. Error: {err}"

    def __str__(self) -> str:
        return f"{super().__str__()} act:{self._ipmi_action}"


class PowerOnServersInBatch(DeviceTask):
    """
    Power on server in batches
    """

    def __init__(
        self, profile: str, batch_size: int = 1, batch_interval: int = 120
    ):
        """
        Args:
            profile (str): profile
            batch_size (int, optional): batch size. Defaults to 1.
            batch_interval (int, optional): time (second) inverval
                                            between batches. Defaults to 120.
        """
        super().__init__(profile)
        self._batch_size = batch_size
        self._batch_interval = batch_interval

    def run(self, targets: List[str]) -> Dict:
        total_num_targets = len(targets)
        cur_on = 0
        server_list = targets.copy()
        power_action = ServerPowerAction(self._profile, "on")
        results = {}
        while server_list:
            batch, server_list = (
                server_list[:self._batch_size],
                server_list[self._batch_size:],
            )
            cur_on += len(batch)
            logger.info(
                f"Powering on {cur_on} of {total_num_targets}. Last: {batch[-1] if batch else ''}"
            )
            results.update(power_action.run(batch))
            time.sleep(self._batch_interval)
        return results

    def __str__(self) -> str:
        return (
            f"{super().__str__()} batch size:{self._batch_size} "
            f"interval:{self._batch_interval}"
        )


class WaitForOSInstall(ParallelDeviceTask):
    """
    Wait for the new OS install to complete by checking that the time written during OS install (/boot/os-install-time)
    is greater than the time when the OS install started.
    """

    MAX_OS_INSTALL_TIME_DIFF = 3600

    def __init__(self, profile: str,
                 parallelism: int = 20,
                 retry_period: int = 30,
                 timeout: int = 900,
                 not_before: int = 0):
        # poll every 'retry_period' seconds for a max of 'timeout' seconds
        super().__init__(profile, parallelism)
        self._retry_period = retry_period
        self._timeout = timeout
        self._not_before = not_before

    def _run(self, target) -> Tuple:
        device = Device.get_device(target, self._profile)
        username = password = "root" # OS still has the image default password

        start_time = int(time.time())
        not_before = self._not_before if self._not_before > 0 else start_time - self.MAX_OS_INSTALL_TIME_DIFF
        time_limit = start_time + self._timeout
        last_error = None
        unreachable = False

        while time.time() < time_limit:
            cmd = 'date -f /boot/os-install-time "+%s"' # read file and convert to unix time
            ret, stdout, err = exec_remote_cmd(cmd, device.name, username, password)
            if ret == 0 and stdout:
                unreachable = False
                try:
                    actual_install_time = int(stdout.strip())
                    if actual_install_time >= not_before:
                        return Status.OK, f"ready after {int(time.time()) - start_time} seconds"
                    else:
                        not_before_dt = datetime.fromtimestamp(not_before, timezone.utc)
                        actual_dt = datetime.fromtimestamp(actual_install_time, timezone.utc)
                        cmd = "fdisk -l | grep 'Linux LVM' | awk '{print $1}' | sed 's/p\?[0-9]\+$//'"
                        ret, out, err = exec_remote_cmd(cmd, device.name, username, password, throw=True)
                        root_disk = out.strip()
                        return Status.FAILED, str(
                            f"Read /boot/os-install-time from {device.name} as {actual_dt} which is before "
                            f"{not_before_dt} indicating that the installed OS is not the expected OS image. "
                            f"You may need to wipe the root disk (ssh root@{device.name} blkdiscard {root_disk}) and retry."
                        )

                except Exception as e:
                    last_error = "Missing or corrupt OS install time"
                    logger.debug(
                        f"Logged into {device.name} but failed to read and parse /boot/os-install-time: {e}"
                    )
                    pass
            else:
                unreachable = True
                last_error = f"target unreachable or login failed, {err}"
                logger.debug(
                    f"failed poll OS install time for {device.name}: {last_error}"
                )
            time.sleep(self._retry_period)

        # There have been cases where the node gets stuck in a boot loop because
        # it's boot device continues to be PXE as opposed to HDD.
        if unreachable:
            try:
                with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                    if not ipmi.is_boot_order_hdd():
                        last_error = (
                            f"{last_error}. HDD is not above PXE in the device boot order."
                            "Please log in to the server console and update the boot order from the BIOS screen"
                        )
            except IPMIError as e:
                last_error = f"{last_error}. Failed to connect to IPMI to get more information, {e}"

        return Status.FAILED, f"failed after {int(time.time() - start_time)} seconds: {last_error}"

    def __str__(self) -> str:
        return (
            f"{super().__str__()} retry_period:{self._retry_period} "
            f"timeout:{self._timeout} not_before:{self._not_before}"
        )

class CopySSHKeys(ParallelDeviceTask):
    """
    Copy the root server's SSH keys to the cluster nodes.

    Note: in the future as part of security hardening, the k8s management nodes should have their own key copied
    to the authorized_keys of the cluster nodes.
    """

    SSH_KEYDIR = "/root/.ssh/"

    def __init__(self, profile: str, parallelism: int = 20):
        super().__init__(profile, parallelism)
        cg = ConfGen(profile)
        self._root_node = cg.get_root_server()

        self._ssh_key_file_xfr_details = []
        self._ssh_authorized_key_xfr_details = []
        _pub_key_contents = []
        path = pathlib.Path(self.SSH_KEYDIR)
        for entry in os.scandir(path):
            if entry.name.startswith("id_") and entry.is_file():
                if not entry.is_symlink():
                    uid = entry.stat().st_uid
                    gid = entry.stat().st_gid
                    mode = stat.S_IMODE(entry.stat().st_mode)
                    self._ssh_key_file_xfr_details.append((entry.path, entry.path, uid, gid, mode,))
                if entry.name.endswith(".pub"):
                    with open(entry.path, "r") as f:
                        _pub_key_contents.append(f.read().strip())

        with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
            temp_file.write("\n".join(_pub_key_contents))
            temp_file_path = temp_file.name

        self._ssh_authorized_key_xfr_details.append(
            (temp_file_path, self.SSH_KEYDIR + "authorized_keys", 0, 0, 0o644,)
        )


    def _run(self, target) -> Tuple:
        if self._root_node == target:
            return Status.OK, ""

        device = Device.get_device(target, self._profile)
        auth_pairs = [("root", "root")]  # if OS still has the image default password
        user = device.get_prop(props.prop_management_credentials_user)
        password = device.get_prop(props.prop_management_credentials_password)
        auth_pairs.append((user, password,))  # if the new password was set

        for username, password in auth_pairs:
            try:
                with SSHConn(device.name, username, password) as conn:
                    ret, _, err = conn.exec(f"rm -fr {self.SSH_KEYDIR}")
                    if ret != 0:
                        return Status.FAILED, f"Failed to remove old .ssh directory: {err}"

                    ret, _, err = conn.exec(f"mkdir -p {self.SSH_KEYDIR}")
                    if ret != 0:
                        return Status.FAILED, f"Failed to create new .ssh directory: {err}"

                    conn.scp_files(self._ssh_authorized_key_xfr_details + self._ssh_key_file_xfr_details)
            except AuthenticationException:
                # Try the next password
                pass

            except Exception as e:
                return Status.FAILED, f"Error during ssh session: {e}"

        return Status.OK, ""

    def __str__(self) -> str:
        return (
            f"{super().__str__()}"
        )
