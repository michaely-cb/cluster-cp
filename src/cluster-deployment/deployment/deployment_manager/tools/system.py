from concurrent.futures import ThreadPoolExecutor

import concurrent
import dataclasses
import json
import logging
import os
import requests
import time
import typing
import urllib3
from requests.auth import HTTPBasicAuth
from typing import Optional, Tuple
import paramiko

from deployment_manager.tools.ssh import SSHConn, make_prompt_detector
from deployment_manager.tools.utils import ReprFmtMixin, exec_cmd, exec_remote_cmd, strip_json_dict
from deployment_manager.common.constants import MAX_WORKERS

# Disable warning about insecure connections.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)
_logger = logger  # so there can be a reference inside the SystemCtl.__init__ method


LINKS_ALL_UP = "ALL UP"


@dataclasses.dataclass
class SystemStatus(ReprFmtMixin):
    name: str
    status: str = "Unknown"
    version: str = "Unknown"
    build_id: str = "Unknown"
    link_status: str = "Unknown"
    dcqcn: str = "Unknown"
    error: str = ""

    @property
    def sort_key(self):
        return self.name

    @classmethod
    def table_header(cls) -> list:
        return ["System Name", "Status", "SW version", "Build ID", "Network Links", "DCQCN", "Error"]

    def to_table_row(self) -> list:
        return [self.name, self.status, self.version, self.build_id, self.link_status, self.dcqcn, self.error]


@dataclasses.dataclass
class PasswordChange(ReprFmtMixin):
    name: str  # system name
    username: str
    status: str  # updated|unchanged|error
    messsage: str = ""

    @classmethod
    def table_header(cls) -> list:
        return ["name", "username", "status", "message"]

    def to_table_row(self) -> list:
        return [self.name, self.username, self.status, self.messsage]

    @property
    def sort_key(self):
        return self.name, self.username,

def change_system_password(
        hostname: str,
        login_as: str, try_login_passwords: typing.List[str],
        target_user: str, target_password: str,
) -> PasswordChange:
    """
    Use `login_as` user to change `target_user`'s password to target_password. Supports trying multiple passwords for
    `try_login_password`. Note this isn't a part of SystemCtl since it doesn't assume the `admin` user which is
    the assumption for the SystemCtl API calls
    Args:
        login_as: user who will do the password update - usually root
        try_login_passwords: passwords to attempt to login for `login_as`'s user
        target_user: user to change the pw of
        target_password: pw to set target_user's login
    """
    rv = PasswordChange(hostname, target_user, "")

    try:
        for i, try_pw in enumerate(try_login_passwords):
            try:
                with SSHConn(hostname, login_as, try_pw if try_pw else None) as conn:
                    # little optimization to avoid changing password if already set correctly
                    if login_as != target_user or target_password != try_pw:
                        with conn.shell_session() as sh:
                            # NOTE: setting a password to empty string doesn't work on all flavors of linux,
                            # just happens to work for the one running on the system (BusyBox v1.29.3 at time of writing)
                            sh.prompt_detect = make_prompt_detector()
                            sh.exec(f"passwd {target_user}")
                            sh.exec(target_password, disable_logging=True)
                            sh.exec(target_password, disable_logging=True)
                        rv.status = "updated"
                    else:
                        rv.status = "unchanged"

                if rv.status == "updated":
                    # verify that the new password indeed works
                    try:
                        with SSHConn(hostname, target_user, target_password if target_password else None) as conn:
                            rv.messsage = f"ok: validated {target_user} password updated"
                    except Exception as e:
                        rv.status = "error"
                        rv.messsage = f"failed to login as {target_user} after password update. This may be a problem with the update routine. Check logs and contact cluster-deployment team."
                return rv

            except paramiko.AuthenticationException:
                logger.debug(f"failed to login to {hostname} as {login_as} with password index {i}")

    except Exception as e:
        rv.status = "error"
        rv.messsage = f"failed with unexpected exception: {e}"
        return rv
    # none of the password worked...
    rv.status = "error"
    rv.messsage = f"unable to login as {login_as} in order to change {target_user}'s password"
    return rv


class SystemCtl:
    """ CS2 API Wrapper """

    _STANDBY_ACTIVATE_TIMEOUT = 45 * 60

    def __init__(self, hostname: str, username: str, password: str, logger=None):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.logger = logger if logger is not None else _logger
        # TODO: also create an SSH session, use ctxmgr
        self._session = requests.Session()

    def _exec_remote_cmd(self, cmd, timeout=30, throw=False) -> Tuple[int, str, str]:
        """ Execute command on remote shell using ssh """
        return exec_remote_cmd(
            cmd,
            host=self.hostname, username=self.username, password=self.password,
            timeout=timeout, throw=throw
        )

    def get_date(self) -> str:
        failure_count = 0
        while failure_count < 3:
            try:
                _, date, _ = self._exec_remote_cmd("system date", throw=True)
                return date.strip()
            except Exception as e:
                # ssh connection may fail, retry a few times
                self.logger.warning(f"Error: {e}")
                failure_count += 1
            time.sleep(30)
        else:
            raise RuntimeError(f"{self.hostname}: failed to fetch system date")

    def get_version(self) -> Tuple[Optional[str], Optional[str]]:
        """ Get the CS image version and build id of the system """

        cmd = "software show --detailed --output-format json"
        ret, stdout, stderr = self._exec_remote_cmd(cmd)
        out = json.loads(stdout)

        for comp in out['components']:
            if comp['shortName'] == 'itb-sm':
                return comp['version'], comp['buildid']

        return None, None

    def _await_install(self, timeout=5400):
        """
        Poll the system until install completes.
        Args:
            timeout: time to wait for the install to finish in sections
        Raises:
            RuntimeError: if the system fails to install for any reason or times out
        """
        cmd = "software show --output-format json"

        self.logger.info(f"Polling {self.hostname} for update status...")

        # Time out after 90 mins
        expire_time = time.time() + timeout
        while time.time() < expire_time:
            try:
                ret, stdout, stderr = self._exec_remote_cmd(cmd)
                out = json.loads(stdout)
                percent = out["upgradeDetails"]["progress"]
                status = out["upgradeDetails"]["status"]
                phase = out["upgradeDetails"]["phase"]

                self.logger.info(f"{self.hostname} image upgrade {percent}% done (status={status} phase={phase})")

                if percent == 100 and status == 'SUCCESS' and phase == 'DONE':
                    self.logger.info(f"{self.hostname} update finished successfully")
                    return
                elif percent == 100 and status == 'FAILED' and phase == 'DONE':
                    raise RuntimeError(f"{self.hostname} image update failed: {out['upgradeDetails']}")
                else:
                    time.sleep(60)

            except:
                # This can happen during the update
                time.sleep(30)
                continue

        self.logger.error(f"{self.hostname} update timed out")
        raise RuntimeError(f"{self.hostname} update timed out")

    def update_image(self, image: str):
        """
        Args:
            image: path to image file
        """
        self.logger.info(f"Updating {self.hostname} with {image}")

        image_file = os.path.basename(image)

        # Put system into standby mode before the update.  This eliminates harmless
        # system alert messages when it is in degraded mode.
        # This is better than the PRE_INSTALL_STANDBY step during the update.
        # It slowly ramps down the power, thereby reducing the stress on the
        # hardware.  The downside is that it takes longer, 15 mins on average,
        # than the average 5 mins in PRE_INSTALL_STANDBY step.
        self.standby()

        # Upload image to the system using http
        url = f"https://{self.hostname}/upgradeBundle/{image_file}"
        with open(image, 'rb') as file_data:
            response = requests.put(
                url,
                data=file_data,
                auth=HTTPBasicAuth(self.username, self.password),
                verify=False,
                timeout=30
            )
            if response.status_code != 200:
                raise RuntimeError(f"Failed to upload image {image} to system {self.hostname}: status_code {response.status_code} {response.text} ")

        # Start the software update
        cmd = f"entersupportmode --noConfirm; software upgrade {image_file} --no-confirm --force"
        status, stdout, stderr = self._exec_remote_cmd(cmd, timeout=120)
        self.logger.info(f"{self.hostname} {cmd} -> {status} | {stdout} | {stderr}")
        if status != 0:
            raise RuntimeError(f"Failed to upgrade software using {image_file}: {stdout} {stderr}")

        self._await_install()

    def update_network_config(self, config_filepath: str) -> Tuple[bool, str]:
        """
        Returns:
            Tuple[bool, str]: True if system accepted the config update. Non-empty error message in case of failure
        """
        session = requests.Session()
        with open(config_filepath, 'rb') as file_handle:
            files = [
                ('UpdateParams',
                 ("Parameters.json", json.dumps({"UpdateType": "network-cfg"}), 'application/json')),
                ('updateFile', ("network-cfg.json", file_handle))
            ]
            url = f"https://{self.hostname}/redfish/v1/UpdateService"
            response = session.post(
                url, files=files, verify=False,
                auth=requests.auth.HTTPBasicAuth(self.username, self.password)
            )

        if response.status_code != 202:
            return False, f"POST network_config failed: {self.hostname} :: {response.status_code} :: {response.text}"
        return True, ""

    def _await_state_change_completed(self, from_time: str, timeout: int, state="activate") -> str:
        """
        Check if the standby or activate command is completed or failed
        Returns a non-empty string if the await times out or otherwise fails
        """
        self.logger.info(f"{self.hostname} awaiting {state} for max {timeout} seconds")
        state_search = "EventActivate" if state == "activate" else "EventStandby"

        cmd = f"events show --output-format json --event-type '{state_search}*' --from-time {from_time}"
        consecutive_failures = 0
        not_after = time.time() + timeout
        while consecutive_failures < 3 and time.time() < not_after:
            ret, stdout, stderr = self._exec_remote_cmd(cmd)
            if ret != 0:
                self.logger.warning(f"failed to query system {self.hostname} events: {stdout}, {stderr}")
                consecutive_failures += 1
            else:
                consecutive_failures = 0
                out = json.loads(stdout)
                for event in out['events']:
                    if event['type'].endswith("Completed"):
                        return ""
                    elif event['type'].endswith("Failed") or event['type'].endswith("Timedout"):
                        return f"system reported {state} failed"
            time.sleep(30)
        if time.time() >= not_after:
            return f"timed out out awaiting {state}"
        return f"failed to query system 3 times over 30 seconds"

    def standby(self):
        """
        Put system into standby mode, wait until it completes
        Raises
            RuntimeError: if the system fails to standby for any reason
        """
        start_time = self.get_date()
        self._exec_remote_cmd("system standby --no-confirm", throw=True)
        err = self._await_state_change_completed(
            start_time, timeout=SystemCtl._STANDBY_ACTIVATE_TIMEOUT, state="standby"
        )
        if err:
            raise RuntimeError(err)
        self.logger.info(f"{self.hostname} in standby")

    def activate(self):
        """
        Activate the system
        Raises
            RuntimeError: if the system fails to activate for any reason
        """
        self.logger.info(f"Activating {self.hostname}")
        start_time = self.get_date()

        self._exec_remote_cmd("system activate --no-confirm")
        err = self._await_state_change_completed(
            start_time, timeout=SystemCtl._STANDBY_ACTIVATE_TIMEOUT, state="activate"
        )
        if err:
            raise RuntimeError(err)
        self.logger.info(f"{self.hostname} activated")

    def network_reload(self):
        """
        Reload system network config
        """
        with SSHConn(self.hostname, self.username, self.password) as conn:
            rv, stdout, stderr = conn.exec("entersupportmode --noConfirm; network reload")
            if rv != 0:
                raise RuntimeError(stderr)
            self.logger.info(f"Network reloaded on {self.hostname}")

    def update_syscfg(self, configs: dict):
        """
        Places the system in standby and updates the config. Does not re-activate if the system was in activate.
        Args:
            configs: dict of k/v pairs of system configs to target values
        Raises
            RuntimeError: if configs were not in target state after applying them
        """
        if not self.is_system_standby():
            self.standby()

        outputs = []
        errors = []
        with SSHConn(self.hostname, self.username, self.password) as conn:
            for k, v in configs.items():
                rv, stdout, stderr = conn.exec(f"entersupportmode --noConfirm; config syscfg set --key {k} --value {v}")
                if rv != 0:
                    outputs.append(stdout)

            # validate settings applied
            rv, settings, stderr = conn.exec("entersupportmode --noConfirm; config syscfg show --output-format json")
            if rv == 0:
                jstart, jend = settings.index("{"), settings.rindex("}")
                doc = json.loads(settings[jstart:jend + 1])
                actual_values = {
                    o["parameter"]: str(o["value"]) for o in doc["systemCfgValues"]
                }
                for k, v in configs.items():
                    if actual_values.get(k) != str(v):
                        errors.append(f"failed to apply {k}={v}, actual={actual_values.get(k)}")
            else:
                errors.append(settings)
        if errors:
            raise RuntimeError(", ".join(errors) + f"ssh session output: {' '.join(outputs)}")

    def check_version(self, img_version, img_buildid):
        new_sys_version, new_sys_buildid = self.get_version()
        if new_sys_version == img_version and new_sys_buildid == img_buildid:
            self.logger.info(
                f"\nSuccessfully updated {self.hostname} system version to {new_sys_version}, build id {new_sys_buildid}"
            )
        else:
            self.logger.error(
                f"\nERROR: Failed to update {self.hostname}.  Current system version {new_sys_version}, build id {new_sys_buildid}"
            )
            raise RuntimeError(f"Failed to update {self.hostname}")

    def get_system_show(self):
        cmd = "system show --detailed --output-format json"
        _, stdout, _ = self._exec_remote_cmd(cmd, throw=True)
        return json.loads(stdout)

    def check_status(self):
        out = self.get_system_show()
        status = out["systemHealth"]
        self.logger.info(f"{self.hostname} health status: {status}")
        if status != "OK":
            # Dump out detailed report if status is not OK.
            cmd = "system show --detailed"
            ret, stdout, stderr = self._exec_remote_cmd(cmd)
            self.logger.info(f"\n{stdout}")

    def check_network(self):
        ret, stdout, stderr = self._exec_remote_cmd("network show")
        self.logger.info(f"{stdout}")

    def is_system_standby(self):
        return self.get_system_show()["systemHealth"] == "STANDBY"

    def is_system_activated(self):
        return self.get_system_show()["systemHealth"] in ("OK", "DEGRADED",)

    def network_show(self, json=False) -> str:
        args = "" if not json else "--output-format=json"
        _, stdout, _ = self._exec_remote_cmd(f"network show {args}", throw=True)
        return stdout

    def _get_network_cfg_doc(self) -> dict:
        """ Read the network config json directly from the file system """
        with SSHConn(self.hostname, self.username, self.password) as conn:
            with conn.shell_session() as sh:
                sh.prompt_detect = lambda x: True
                sh.exec("entersupportmode --noConfirm")
                time.sleep(1)
                sh.exec("dev admin shell")
                time.sleep(0.5)
                sh.read()  # clear read buffer
                network_json = sh.exec("cat /var/lib/tftpboot/network-cfg.json")
                time.sleep(1)
                network_json += sh.read()
                return strip_json_dict(network_json)

    def is_dcqcn_enabled(self) -> bool:
        """ Check network config file for DCQCN enabled """
        doc = self._get_network_cfg_doc()
        cm = doc.get("cm_daemon", {})
        if cm.get("enable_dcqcn_sender", 0) == 1 and cm.get("enable_dcqcn_receiver", 0) == 1:
            return True
        return False

    def update_service(self, image, force, skip_activate):
        """ Update CS2 with new image """

        sys_version, sys_buildid = self.get_version()
        if sys_version is None:
            self.logger.info(f"Failed to get system {self.hostname} OS")
            raise ValueError(f"Failed to get system {self.hostname} OS")

        self.logger.info(f"{self.hostname} system version {sys_version}, build id {sys_buildid}")

        img_version, img_buildid = parse_image_version(image)
        self.logger.info(f"Image version {img_version}, build id {img_buildid}")
        if img_version is None:
            self.logger.error(f"Invalid image file {image}")
            raise ValueError(f"Invalid image file {image}")

        if (
                (not force)
                and (sys_version == img_version)
                and (sys_buildid == img_buildid)
        ):
            self.logger.info(f"{self.hostname} already has the image installed.")
            return 0

        self.update_image(image)

        if skip_activate:
            self.logger.info(f"Skipping system activation")
        else:
            self.activate()

        # Post update sanity checks
        self.check_version(img_version, img_buildid)
        self.check_status()
        self.check_network()

        return 0

    def get_down_links(self, throw=True) -> typing.List[str]:
        """ Check if there are any link down """
        cmd = "network show --output-format json"
        _, stdout, _ = self._exec_remote_cmd(cmd, throw=throw)
        out = json.loads(stdout)

        down_links = []
        if out["mgmt"]["linkState"] != "UP":
            down_links.append(out["mgmt"]["ifaceName"])
        down_links.extend([link["ifaceName"] for link in out["data"] if link["linkState"] != "UP"])
        return down_links

    def get_system_version(self, throw=True) -> Tuple[Optional[str], Optional[str]]:
        """ Get the CS image version and build id of the system """
        cmd = "software show --detailed --output-format json"
        rv, stdout, _ = self._exec_remote_cmd(cmd, throw=throw)
        if stdout is None:
            return None, None
        out = json.loads(stdout)

        for comp in out['components']:
            if comp['shortName'] == 'itb-sm':
                return comp['version'], comp['buildid']

        return None, None

    def get_config_network(self, throw=True) -> Optional[dict]:
        """ Get the systems network config """
        cmd = "config network show --output-format json"
        rv, stdout, _ = self._exec_remote_cmd(cmd, throw=throw)
        if stdout is None:
            return None
        out = json.loads(stdout)
        return out

    def get_network_show(self) -> dict:
        cmd = "network show --output-format json"
        rv, stdout, stderr = self._exec_remote_cmd(cmd, throw=True)
        if rv != 0 or not stdout:
            raise RuntimeError(f"unexpected response from 'network show': {rv} :: {stdout} :: {stderr}")
        return json.loads(stdout)

    def get_system_health_status(self, throw=True) -> Optional[str]:
        """ Get the systems health """
        cmd = "system show --detailed --output-format json"
        rv, stdout, _ = self._exec_remote_cmd(cmd, throw=throw)
        if stdout is None:
            return None
        out = json.loads(stdout)
        return out.get("systemHealth")

    def check_system_health(self, check_link_state=True) -> str:
        """ Check system health: ensure links up, hostname correctly configured, health status is OK """
        # check accessibility and hostname
        try:
            net_doc = self.get_config_network(throw=False)
            actual_hostname = net_doc.get("mgmtDhcp", {}).get("hostname", "NOT_FOUND")
            if actual_hostname != self.hostname:
                return f"system {self.hostname} name does not match actual hostname '{actual_hostname}'"
        except Exception as e:
            return f"failed to get network status from system {self.hostname}: {e}"

        # check system health status
        system_status = self.get_system_health_status(throw=False)
        acceptable_health_states = ("DEGRADED", "OK", "STANDBY")
        if system_status not in acceptable_health_states:
            return (
                f"system {self.hostname} health is {system_status}. "
                f"System's health need to be in {acceptable_health_states} to continue."
            )

        if check_link_state:
            down_links = self.get_down_links()
            if down_links:
                return f"system {self.hostname} had {len(down_links)} links down: {', '.join(down_links)}"

        return ""

    def get_system_status(self) -> SystemStatus:
        """ Query the system, return state in status object """
        status = SystemStatus(self.hostname)
        try:
            health = self.get_system_health_status()
            if health is not None:
                status.status = health

            sys_version, sys_buildid = self.get_system_version()
            if sys_version is not None:
                status.version, status.build_id = sys_version, sys_buildid

            down_links = self.get_down_links()
            if down_links:
                status.link_status = " ".join(down_links) + " DOWN"
            else:
                status.link_status = LINKS_ALL_UP

            status.dcqcn = "Enabled" if self.is_dcqcn_enabled() else "Disabled"
        except Exception as e:
            status.error = str(e)

        return status


def parse_image_version(system_image_file: str) -> Tuple[Optional[str], Optional[str]]:
    """ Extract the version and build id of the image file """
    system_image_file = os.path.basename(system_image_file)

    # Image file must start with "CS1-"
    if not system_image_file.startswith('CS1-'):
        return None, None

    # version is between "CS1-" and the second hyphen '-'
    second_hyphen = system_image_file[4:].find('-')
    if second_hyphen == -1:
        return None, None
    second_hyphen += 4

    version = system_image_file[4:second_hyphen]

    # build id is between the second hyphen and ".tar.gz"
    end_index = system_image_file.find('.tar.gz')
    if end_index == -1:
        return None, None

    buildid = system_image_file[second_hyphen + 1: end_index]

    return version, buildid

def perform_group_action(sys: typing.List[SystemCtl], method: str, *args, **kwargs) -> typing.Dict[str, typing.Any]:
    """ Call methods on all systems - TODO: share code with switch/switchctl.py::perform_group_action """
    result = {}

    def exec_one(sys) -> typing.Any:
        if hasattr(sys, method):
            return getattr(sys, method)(*args, **kwargs)
        return f"Programmer error: object {sys} does not have attribute '{method}'"

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(exec_one, s): s for s in sys}
        done, _ = concurrent.futures.wait(list(futures.keys()), return_when=concurrent.futures.ALL_COMPLETED)
        for future in done:
            sys_name = futures[future].hostname
            try:
                result[sys_name] = future.result()
            except Exception as e:
                result[sys_name] = f"Threw exception {e}"

    return result
