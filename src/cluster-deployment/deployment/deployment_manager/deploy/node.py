import dataclasses
import datetime
import logging
import os
import shutil
import time
from abc import ABCMeta, abstractmethod
from functools import cached_property
from typing import Tuple

import deployment_manager.db.device_props as props
from deployment_manager.cli.cmd_cluster_mgmt import resolve_pkg_path, update_cluster_pkg
from deployment_manager.common.models import DEFAULT_METRICS_PASSWORD, DEFAULT_METRICS_USERNAME, ClusterDeploymentConfig
from deployment_manager.db.const import (
    DeploymentDeviceTypes, DeploymentStageStatus,
    DeviceDeploymentStages,
)
from deployment_manager.db.models import (
    DeploymentCtx,
    Device, QuerySet, )
from deployment_manager.deploy.common import FATAL_EC, DEPLOYMENT_SUCCESSFUL, DEPLOYMENT_SKIP
from deployment_manager.tools import middleware
from deployment_manager.tools.config import (
    ConfGen,
)
from deployment_manager.tools.const import (CLUSTER_DEPLOYMENT_BASE, CLUSTER_YAML, DEPLOYMENT_PACKAGES)
from deployment_manager.tools.pb1.ansible_task import (ConfigureFreeIpa, ConfigureNics, DeployServers, LockdownNodes)
from deployment_manager.tools.pb1.ansible_task import (ConfigureNfsMount)
from deployment_manager.tools.pb1.bios_task import UpdateBiosSettings, WaitForBiosSettingsUpdate
from deployment_manager.tools.pb1.device_status import Status
from deployment_manager.tools.pb1.device_task import WaitTask, SerialTask
from deployment_manager.tools.pb1.ipmi_task import CreateMetricsUser, DisableHostCheck, InitializePxeBoot
from deployment_manager.tools.pb1.server_task import ServerPowerAction, PowerOnServersInBatch, WaitForOSInstall, \
    CopySSHKeys
from deployment_manager.tools.pb1.validator import (NicValidator, OsValidator, IpmiValidator)
from deployment_manager.tools.ssh import SSHConn
from deployment_manager.tools.utils import exec_cmd, generate_secure_string

logger = logging.getLogger(__name__)

REBOOT_WAIT_TIME = 420


class Deploy(metaclass=ABCMeta):
    """ Base class for deployment push buttons
    """

    cwd: str = CLUSTER_DEPLOYMENT_BASE
    env: dict = None
    _targets: list = None

    def __init__(self, profile, write_to_deploy_log:bool=True):
        self.profile = profile
        self._cg = ConfGen(profile)
        self.profile_config = self._cg.parse_profile()
        self.log_file = f"{self._cg.log_dir}/{type(self).name()}.log"
        self.cfg_dir = self._cg.config_dir
        self.master_file = self._cg.master_file
        self.network_config_file = self._cg.network_config_file
        self._ctx = DeploymentCtx.get_ctx(self.profile)
        self.write_to_deploy_log = write_to_deploy_log

        if self._ctx is None:
            raise RuntimeError(
                f"Profile {self.profile} not found"
            )

    @cached_property
    def global_config(self) -> ClusterDeploymentConfig:
        return self._cg.load_config()

    def write_to_log_ts(self, msg):
        self.write_to_log(f"{datetime.datetime.now()}: {msg}")

    def write_to_log(self, msg):
        if not self.write_to_deploy_log:
            return

        with open(self.log_file, "a") as f:
            f.write(str(msg))
            f.write("\n")

    def exec_cmd(self, cmdstr, cwd=None, env=None, stream=False, logcmd=True) -> Tuple[int, str, str]:
        """
            Execute the shell cmd.

            If cwd is None, define from self.cwd
            If env is None, define from self.env
        """
        if cwd is None:
            cwd = self.cwd
        if env is None:
            env = self.env

        self.write_to_log_ts(cmdstr)
        ret, out, err = exec_cmd(
            cmdstr,
            cwd=cwd,
            env=env,
            stream=stream,
            logcmd=logcmd
        )

        self.write_to_log(f"STDOUT:\n{out}")
        self.write_to_log(f"STDERR:\n{err}")
        return ret, out, err

    def exec_func(self, func, args=None, env=None):
        """
        Execute func with args
        Set env variables to env, self.env if env is None
        """
        self.write_to_log_ts(f"{func.__name__} - {args}")
        environ = env or self.env
        if environ:
            orig_environ = dict()
            for k, v in environ.items():
                orig_environ[k] = os.environ.get(k)
                os.environ[k] = v
        try:
            ret = func(args)
            self.write_to_log(ret)
        finally:
            # restore environ
            if environ:
                for k, v in orig_environ.items():
                    if k not in os.environ:
                        continue
                    if v is None:
                        del os.environ[k]
                    else:
                        os.environ[k] = v
        return ret

    def exec_ansible_playbook(self, cmdstr):
        rtn, out, err = self.exec_cmd(f"ansible-playbook {cmdstr}")
        if rtn:
            logger.error(f"Ansible failed.\nExecute 'show logs --logname {self.name()}' for details")
            logger.info("")
            printline = False
            matches = ["TASK", "fatal:", "failed:"]
            for line in out.split("\n"):
                if "PLAY RECAP" in line:
                    printline = True
                if printline or any(x in line for x in matches):
                    logger.info(f"\t{line}")
            return FATAL_EC
        return DEPLOYMENT_SUCCESSFUL

    @abstractmethod
    def name(cls):
        return "deploy"

    # abstracted execute_get_targets because it is required.  precheck and validate are not.
    @abstractmethod
    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        """
        Define the list of devices that is going to be deployed on

        Returns True if there is atleast one target, False otherwise
        """
        return False

    @abstractmethod
    def precheck(self, args):
        """ Run precheck for this push button step
        """
        return DEPLOYMENT_SUCCESSFUL

    @abstractmethod
    def execute(self, args):
        """ Execute this push button
        """
        return DEPLOYMENT_SUCCESSFUL

    @abstractmethod
    def validate(self, args):
        """ Validate deployment part done by this push button
        """
        return DEPLOYMENT_SUCCESSFUL


class DeployOsProvisionBase(Deploy):
    """ Base class for deployment from os-provision
    """
    cwd = f"{CLUSTER_DEPLOYMENT_BASE}/os-provision"

    def __init__(self, profile, write_to_deploy_log:bool=True):
        super().__init__(profile, write_to_deploy_log=write_to_deploy_log)

    @classmethod
    def name(cls):
        return "deploy_os_provision_base"

    def precheck(self, args):
        return DEPLOYMENT_SUCCESSFUL

    def execute(self, args):
        return DEPLOYMENT_SUCCESSFUL

    def validate(self, args):
        return DEPLOYMENT_SUCCESSFUL

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if force or not d.os_installed():
                self._targets.append(d.name)
        return bool(self._targets)

    def copy_master_config_to_os_provision(self):
        """
        Copy the profile master-config.yml file to os-provision directory
        """
        ret, _, _ = exec_cmd(
            f"cp {self.master_file} {CLUSTER_DEPLOYMENT_BASE}/os-provision/master-config.yaml"
        )
        if ret != 0:
            logger.error("Could not copy master config")
            return FATAL_EC
        return 0


OS_BATCH_SIZE_ENV = "DEPLOY_MGR_OS_INSTALL_BATCH_SIZE"
OS_BATCH_SIZE_PER_MBPS = 1000  # 1 OS install per batch for 1000 MBPS interface speed


class DeployOs(DeployOsProvisionBase):
    """ Push button 1 (install OS)
    """

    def __init__(self, profile):
        super().__init__(profile)

    def _get_os_installation_parallelism(self):
        if OS_BATCH_SIZE_ENV in os.environ.keys():
            logger.info(
                f"Found {OS_BATCH_SIZE_ENV}. "
                f"OS installation batch size is {os.environ[OS_BATCH_SIZE_ENV]}"
            )
            return int(os.environ[OS_BATCH_SIZE_ENV])

        root_server_name = self.profile_config["basic"]["root_server"]
        root_server = Device.get_device(root_server_name, self.profile)
        if root_server is None:
            dhcp_main_ip = self.profile_config["basic"]["root_server_ip"]
        else:
            # sanity check
            config_root_server_ip = self.profile_config["basic"].get("root_server_ip")
            dhcp_main_ip = str(root_server.get_prop(props.prop_management_info_ip))
            if config_root_server_ip and dhcp_main_ip != config_root_server_ip:
                raise ValueError(
                    "inconsistent root server ip detected. config input:"
                    f"{config_root_server_ip}, inventory: {dhcp_main_ip}"
                )

        # get NIC name
        cmd = (
            f"ip -j a |"
            f" jq -r --arg ADDR {dhcp_main_ip} "
            f"'.[]|select(.addr_info[].local == $ADDR)|.ifname'"
        )
        _, ifname, _ = exec_cmd(cmd)
        ifname = ifname.strip("\n")

        def get_nic_speed(nic: str) -> int:
            try:
                _, out, _ = exec_cmd(
                    f'ethtool {nic} | grep Speed | tr -d -c "[:digit:]"'
                )
                return int(out)
            except Exception as e:
                return 0

        calc_batch_size = max(get_nic_speed(ifname) // OS_BATCH_SIZE_PER_MBPS, 1)
        logger.info(f"OS installation batch size is {calc_batch_size}. Override with env {OS_BATCH_SIZE_ENV}")
        return calc_batch_size

    @classmethod
    def name(cls):
        return "install_os"

    def precheck_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if d.ipmi_configured() and not force:
                continue

            # always skip root server even in force mode
            if d.name == self.profile_config["basic"]["root_server"]:
                continue

            self._targets.append(d.name)
        return bool(self._targets)

    def precheck(self, args) -> int:
        def fail(d, msg):
            d.update_stage(
                DeviceDeploymentStages.IPMI_CONFIGURATION.value,
                DeploymentStageStatus.INCOMPLETE.value,
                msg,
            )

        do_ipmi_validate = []
        for name in self._targets:
            d = Device.get_device(name, self.profile)
            if d.get_prop(props.prop_management_info_mac) is None:
                fail(d,
                    "missing management mac. Run 'cscfg server "
                    "update_mac_address' or add manually with 'cscfg device edit'"
                )
            elif not d.has_health_records:
                fail(d, "missing health scan. "
                        f"Run 'cscfg server health check -f name={d.name}'")
            elif d.has_critical_health:
                fail(d,"critical health. "
                       f"Run 'cscfg server health check -f name={d.name}' or force update health")
            else:
                do_ipmi_validate.append(d.name)

        # TODO: arguably we shouldn't have this extra validate - we should just re-run the
        # health check for any devices that reported bad health
        ipmi_validator = IpmiValidator(self.profile, 20)
        ipmi_results = ipmi_validator.validate(do_ipmi_validate)

        for server, status in ipmi_results.items():
            d = Device.get_device(server, self.profile)
            if status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.IPMI_CONFIGURATION.value,
                    DeploymentStageStatus.COMPLETED.value
                )
            else:
                fail(d, status.message)
        return DEPLOYMENT_SUCCESSFUL


    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []

        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if not force:
                if not d.ipmi_configured() or d.os_installed():
                    continue

                if d.deployment_status == DeploymentStageStatus.STARTED.value:
                    continue

            if d.name == self.profile_config["basic"]["root_server"]:
                logger.warning(
                    f"root server {d.name} must have os installed, skipping"
                )
                continue

            self._targets.append(d.name)
        return bool(self._targets)

    def execute(self, args):
        if args.skip_os:
            return DEPLOYMENT_SKIP

        if not self._targets:
            logger.info(f"{type(self).name()} has nothing to configure")
            return DEPLOYMENT_SKIP

        os_parallelism = self._get_os_installation_parallelism()
        logger.info(
            f"OS installation parallelism is {os_parallelism}"
        )

        basic_config = self.profile_config.get("basic", {})
        ipmi_metrics_username = basic_config.get("ipmi_metrics_username", DEFAULT_METRICS_USERNAME)
        ipmi_metrics_password = basic_config.get("ipmi_metrics_password", DEFAULT_METRICS_PASSWORD)

        tasks = [
            # IPMI configuration
            DisableHostCheck(self.profile),
            WaitTask(self.profile, 60),  # give some time for idrac to reconfigureΩ (only necessary for dell)
            CreateMetricsUser(self.profile, ipmi_metrics_username, ipmi_metrics_password),

            # OS installation
            ServerPowerAction(self.profile, "off", wait=True),  # power off all servers
            UpdateBiosSettings(self.profile),                   # update bios
            ServerPowerAction(self.profile, "on"),              # power on all servers
            WaitForBiosSettingsUpdate(self.profile),
            ServerPowerAction(self.profile, "off", wait=True),     # power off all servers
            ServerPowerAction(self.profile, "on", wait=True),      # power on all servers
            ServerPowerAction(self.profile, "off", wait=True),     # power off all servers
            InitializePxeBoot(self.profile),                       # pxe boot
            WaitTask(self.profile, 20),                            # wait for 20s to power off servers
            PowerOnServersInBatch(self.profile, os_parallelism),
            WaitTask(self.profile, 300),                        # wait at least 5m for post-boot install tasks to complete
            WaitForOSInstall(self.profile, not_before=int(time.time())), # Poll for os-install-time file to appear, max 15m
        ]
        install_os_tasks = SerialTask(self.profile, tasks)
        server_results = install_os_tasks.run(self._targets)
        for server, status in server_results.items():
            d = Device.get_device(server, self.profile)
            if status.status == Status.FAILED:
                d.update_stage(
                    DeviceDeploymentStages.OS_INSTALLATION.value,
                    DeploymentStageStatus.FAILED.value,
                    status.message
                )
            elif status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.OS_INSTALLATION.value,
                    DeploymentStageStatus.STARTED.value
                )

        return DEPLOYMENT_SUCCESSFUL

    def validate_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if not force:
                if not d.ipmi_configured() or d.os_installed():
                    continue

            if force or (
                d.deployment_status == DeploymentStageStatus.STARTED.value
                or d.deployment_status == DeploymentStageStatus.INCOMPLETE.value
            ):
                self._targets.append(d.name)
        return bool(self._targets)

    def validate(self, args):
        if not self._targets:
            logger.info(f"{type(self).name()} has nothing to validate")
            return DEPLOYMENT_SKIP

        os_validator = OsValidator(self.profile, 20)
        server_results = os_validator.validate(self._targets)

        for server, status in server_results.items():
            d = Device.get_device(server, self.profile)

            if status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.OS_INSTALLATION.value,
                    DeploymentStageStatus.COMPLETED.value
                )
            else:
                d.update_stage(
                    DeviceDeploymentStages.OS_INSTALLATION.value,
                    DeploymentStageStatus.INCOMPLETE.value,
                    status.message
                )

        return DEPLOYMENT_SUCCESSFUL


class DeployNodes(DeployOsProvisionBase):
    """ Push button 1 (servers) """

    def __init__(self, profile, write_to_deploy_log: bool = True):
        super().__init__(profile, write_to_deploy_log=write_to_deploy_log)
        root_server_name = self.profile_config["basic"]["root_server"]
        self._root_server = Device.get_device(root_server_name, self.profile)

    @classmethod
    def name(cls):
        return "deploy_nodes"

    def precheck_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        return self.execute_get_targets(devices, force = force)

    def precheck(self, args):
        return DEPLOYMENT_SUCCESSFUL

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if not force:
                if (
                    not d.os_installed() or
                    d.provisioned() or
                    d.deployment_status == DeploymentStageStatus.STARTED.value
                ):
                    continue

            self._targets.append(d.name)

        return bool(self._targets)

    def execute(self, args):
        if not self._targets:
            logger.info(f"{self.name()} has nothing to configure")
            return DEPLOYMENT_SKIP

        tasks = [
            CopySSHKeys(self.profile),   # Copy SSH keys and create authorized_hosts on new servers
            DeployServers(self.profile), # Run ansible tasks
        ]
        deploy_node_tasks = SerialTask(self.profile, tasks)

        server_results = deploy_node_tasks.run(self._targets)
        for server, status in server_results.items():
            d = Device.get_device(server, self.profile)
            if status.status == Status.FAILED:
                d.update_stage(
                    DeviceDeploymentStages.PROVISIONING.value,
                    DeploymentStageStatus.FAILED.value,
                    status.message
                )
            elif status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.PROVISIONING.value,
                    DeploymentStageStatus.STARTED.value
                )

        return DEPLOYMENT_SUCCESSFUL

    def validate_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if not force:
                if not d.os_installed() or d.provisioned():
                    continue

            if force or d.deployment_status in (
                    DeploymentStageStatus.STARTED.value,
                    DeploymentStageStatus.INCOMPLETE.value,
            ):
                self._targets.append(d.name)
        return bool(self._targets)

    def validate(self, args):
        if not self._targets:
            logger.info(f"{type(self).name()} has nothing to validate")
            return DEPLOYMENT_SKIP

        # TODO: Need to add some validation to check that changes are successful.
        # Execute only checks that ostools did not have return code

        os_validator = OsValidator(self.profile, 20)
        server_results = os_validator.validate(self._targets)

        for server, status in server_results.items():
            d = Device.get_device(server, self.profile)
            if d.deployment_status == DeploymentStageStatus.INCOMPLETE.value:
                continue

            if status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.PROVISIONING.value,
                    DeploymentStageStatus.COMPLETED.value
                )
            else:
                d.update_stage(
                    DeviceDeploymentStages.PROVISIONING.value,
                    DeploymentStageStatus.INCOMPLETE.value,
                    status.message
                )
        return 0


class DeployNics(DeployOsProvisionBase):
    """ Push button 1 (configure 100g nics)
    """

    def __init__(self, profile):
        super().__init__(profile)

    @classmethod
    def name(cls):
        return "configure_nics"

    def precheck_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        return self.execute_get_targets(devices, force = force)

    def precheck(self, args):
        if not self._targets:
            logger.info(f"{type(self).name()} has nothing to precheck")
            return DEPLOYMENT_SKIP
        else:
            logger.info(
                f"{self.name()} found targets: {' '.join(self._targets)}"
            )

        return DEPLOYMENT_SUCCESSFUL

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if not force:
                if not d.provisioned() or d.nics_collected():
                    continue

                if d.deployment_status in (
                        DeploymentStageStatus.STARTED.value,
                        DeploymentStageStatus.INCOMPLETE.value
                ):
                    continue

            self._targets.append(d.name)
        return bool(self._targets)

    def execute(self, args):
        if not self._targets:
            logger.info(f"{type(self).name()} has nothing to execute")
            return DEPLOYMENT_SKIP

        configure_nics = ConfigureNics(self.profile)
        results = configure_nics.run(self._targets)

        for server, status in results.items():
            d = Device.get_device(server, self.profile)
            if status.status == Status.FAILED:
                d.update_stage(
                    DeviceDeploymentStages.NIC_COLLECTION.value,
                    DeploymentStageStatus.FAILED.value,
                    status.message
                )
            elif status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.NIC_COLLECTION.value,
                    DeploymentStageStatus.STARTED.value,
                    status.message
                )

        return DEPLOYMENT_SUCCESSFUL

    def validate_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if not force:
                if not d.provisioned() or d.nics_collected():
                    continue

            if force or (
                d.deployment_status == DeploymentStageStatus.STARTED.value
                or d.deployment_status == DeploymentStageStatus.INCOMPLETE.value
            ):
                self._targets.append(d.name)
        return bool(self._targets)

    def validate(self, args):
        if not self._targets:
            logger.info(f"{type(self).name()} has nothing to configure")
            return DEPLOYMENT_SKIP

        nic_validator = NicValidator(self.profile, parallelism=20)
        nic_results = nic_validator.validate(self._targets)

        for server, status in nic_results.items():
            d = Device.get_device(server, self.profile)
            if status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.NIC_COLLECTION.value,
                    DeploymentStageStatus.COMPLETED.value,
                    status.message
                )
            else:
                d.update_stage(
                    DeviceDeploymentStages.NIC_COLLECTION.value,
                    DeploymentStageStatus.INCOMPLETE.value,
                    status.message
                )

        return DEPLOYMENT_SUCCESSFUL


class DeployFreeIpa(DeployOsProvisionBase):
    """ Push button 2.5 deploy FreeIPA
    """
    freeipa_config: dict = {}
    config_input: dict = {}

    def __init__(self, profile):
        super().__init__(profile)
        self.pkg_dir = DEPLOYMENT_PACKAGES
        self.config_input = self._cg.parse_profile()

    @classmethod
    def name(cls):
        return "deploy_freeipa"

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if not force and (not d.network_pushed() or d.freeipa_installed()):
                continue

            self._targets.append(d.name)
        return bool(self._targets)

    def precheck(self, args):
        # Check if deploy freeipa is enabled
        freeipa_config = self.config_input.get("freeipa", {})
        if not freeipa_config.get("enabled", False):
            logger.info("Skipping, FreeIPA is not enabled in input.yml")
            return DEPLOYMENT_SKIP

        if "domain" not in self.config_input["basic"]:
            logger.error("input.yaml::basic.domain field must be defined")
            return FATAL_EC

        # check for required configs
        missing = []
        for k in ["master_username", "master_password",
                  "ipa_master_nis_domain", "ipa_master_server", "ipa_master_domain"]:
            if k not in freeipa_config:
                missing.append(k)
        if missing:
            logger.error(f"input.yaml::freeipa missing required fields: {', '.join(missing)}")
            return FATAL_EC

        return DEPLOYMENT_SUCCESSFUL

    def execute(self, args):
        if not self._targets:
            logger.info("Skipping, no eligible servers selected")
            return DEPLOYMENT_SKIP

        # Check if deploy freeipa is enabled
        freeipa_config = self.config_input.get("freeipa", {})
        if not freeipa_config.get("enabled", False):
            logger.info("Skipping, FreeIPA is not enabled in input.yml")
            return DEPLOYMENT_SKIP

        configure_freeipa = ConfigureFreeIpa(self.profile)
        results = configure_freeipa.run(self._targets)

        all_failed = True
        for server, status in results.items():
            d = Device.get_device(server, self.profile)
            if status.status == Status.FAILED:
                d.update_stage(
                    DeviceDeploymentStages.FREEIPA.value,
                    DeploymentStageStatus.FAILED.value,
                    status.message
                )
            elif status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.FREEIPA.value,
                    DeploymentStageStatus.COMPLETED.value,
                    status.message
                )
                all_failed = False

        if all_failed:
            logger.warning(
                "All FreeIPA deployments failed. Check the ansible logs for full details. "
                "Note: this commonly happens when the enroller credentials expiring. "
                f"Please log in to the FreeIPA web interface ({self.global_config.freeipa.ipa_master_server}) or "
                f"run 'kinit {self.global_config.freeipa.master_username}' on a server which is enrolled into "
                f"FreeIPA to refresh the password. Then update {self.cfg_dir}/input.yml"
            )

        return DEPLOYMENT_SUCCESSFUL

    def validate(self, args):
        # Validation check is built into the ansible script so this is a no-op
        return DEPLOYMENT_SUCCESSFUL


class DeployNfsMounts(DeployOsProvisionBase):
    """ Push button 2.5 deploy NfsMount
    """
    mount_config: dict = {}
    config_input: dict = {}

    def __init__(self, profile):
        super().__init__(profile)
        self.pkg_dir = DEPLOYMENT_PACKAGES
        self.config_input = self._cg.parse_profile()

    @classmethod
    def name(cls):
        return "deploy_nfs_mounts"

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if not force:
                if d.nfs_mounted():
                    continue

                if not d.network_pushed():
                    continue

                if d.deployment_status == DeploymentStageStatus.STARTED.value:
                    continue

            self._targets.append(d.name)
        return bool(self._targets)

    def precheck(self, args):
        # Check if deploy nfs_mount is enabled
        mount_config = self.config_input.get("nfs_mount", {})
        if not mount_config.get("enabled", False):
            logger.info("Skipping, nfs_mount is not enabled in input.yml")
            return DEPLOYMENT_SKIP

        # check for required configs
        missing = []
        for k in [
            "u_names",
            "u_ids",
            "u_passwords",
            "g_names",
            "g_ids",
            "server_paths",
            "mount_paths",
        ]:
            if k not in mount_config:
                missing.append(k)
        if missing:
            logger.error(f"nfs_mount input config missing required keys: {', '.join(missing)}")
            return FATAL_EC

        return DEPLOYMENT_SUCCESSFUL

    def execute(self, args):
        if not self._targets:
            logger.info("Skipping, no eligible servers selected")
            return DEPLOYMENT_SKIP

        # Check if deploy nfs_mount is enabled
        mount_config = self.config_input.get("nfs_mount", {})
        if not mount_config.get("enabled", False):
            logger.info("Skipping, nfs_mount is not enabled in input.yml")
            return DEPLOYMENT_SKIP

        configure_nfs_mount = ConfigureNfsMount(self.profile)
        results = configure_nfs_mount.run(self._targets)

        for server, status in results.items():
            d = Device.get_device(server, self.profile)
            if status.status == Status.FAILED:
                d.update_stage(
                    DeviceDeploymentStages.NFS_MOUNT.value,
                    DeploymentStageStatus.INCOMPLETE.value,
                    status.message
                )
            elif status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.NFS_MOUNT.value,
                    DeploymentStageStatus.STARTED.value,
                    status.message
                )

        return DEPLOYMENT_SUCCESSFUL

    def validate_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if not force:
                if d.deployment_stage != DeviceDeploymentStages.NFS_MOUNT.value:
                    continue

                if d.nfs_mounted():
                    continue

            if force or d.deployment_status == DeploymentStageStatus.STARTED.value:
                self._targets.append(d.name)
        return bool(self._targets)

    def validate(self, args):
        # Check if deploy nfs_mount is enabled
        mount_config = self.config_input.get("nfs_mount", {})
        if not mount_config.get("enabled", False):
            logger.info("Skipping, nfs_mount is not enabled in input.yml")
            return DEPLOYMENT_SKIP

        if not self._targets:
            logger.info("Skipping, no eligible servers selected")
            return DEPLOYMENT_SKIP

        for device_name in self._targets:
            device = Device.get_device(device_name, self.profile)
            try:
                err = ""
                with SSHConn(*dataclasses.astuple(middleware.must_get_obj_cred(self.profile, device_name))) as conn:
                    for mount in mount_config["mount_paths"]:
                        rtn, stdout, stderr = conn.exec(f"sh -c 'df -Th {mount} | grep nfs'")
                        if rtn:
                            err = f"mount {mount} not found on {device.name}, {stdout} {stderr}"
                            break
                if err:
                    device.update_stage(
                        DeviceDeploymentStages.NFS_MOUNT.value,
                        DeploymentStageStatus.INCOMPLETE.value,
                        err,
                    )
                else:
                    device.update_stage(
                        DeviceDeploymentStages.NFS_MOUNT.value,
                        DeploymentStageStatus.COMPLETED.value
                    )
            except Exception as e:
                device.update_stage(
                    DeviceDeploymentStages.NFS_MOUNT.value,
                    DeploymentStageStatus.INCOMPLETE.value,
                    f"unexpected error during validation: {e}",
                )
                logger.exception("Unexpected error during validation")

        return DEPLOYMENT_SUCCESSFUL


class DeployK8s(Deploy):
    """ Push button 3 (k8s cluster)
    """

    def __init__(self, profile):
        super().__init__(profile)
        self.pkg_dir = DEPLOYMENT_PACKAGES
        root_server_name = self.profile_config.get("basic", {}).get("root_server", None)
        assert root_server_name, "root_server not found in input.yaml::basic.root_server"
        self._root_server = Device.get_device(
            root_server_name, self.profile
        )

    @classmethod
    def name(cls):
        return "deploy_k8s"

    def precheck(self, args):
        # check if a single cluster-pkg can be found
        try:
            resolve_pkg_path()
        except ValueError as e:
            logger.error(str(e))
            return FATAL_EC
        return DEPLOYMENT_SUCCESSFUL

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        # TODO: Assumng that the k8s deployment is on the rootserver
        self._targets = []
        if not self.skip():
            self._targets = [self._root_server.name]
        return bool(self._targets)

    def skip(self):
        if not self._root_server.deployment_stage:
            return False

        if self._root_server.deployment_stage > DeviceDeploymentStages.NETWORK_PUSH.value:
            return True

        if self._root_server.network_pushed():
            return True

        return False

    def execute(self, args):
        # Make a backup copy and delete the cluster.yaml file first
        # since csadm.sh does incremental updates
        now = datetime.datetime.now()
        if os.path.exists(CLUSTER_YAML):
            shutil.copyfile(CLUSTER_YAML, f"{CLUSTER_YAML}.{now.strftime('%Y%m%d_%H%M%S')}")
            os.unlink(CLUSTER_YAML)

        pkg_path = resolve_pkg_path()
        update_cluster_pkg(pkg_path, self.network_config_file, no_confirm=True)
        return DEPLOYMENT_SUCCESSFUL

    def validate(self, args):
        return DEPLOYMENT_SUCCESSFUL


class DeployLockdownNodes(DeployOsProvisionBase):
    """ Remove root login capabilities. FreeIPA must be installed """

    def __init__(self, profile):
        super().__init__(profile)
        self.pkg_dir = DEPLOYMENT_PACKAGES
        self.config_input = self._cg.parse_profile()

    @classmethod
    def name(cls):
        return "deploy_lockdown"

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SERVER.value):
            if not force:
                if d.check_stage_completed(DeviceDeploymentStages.LOCKDOWN.value):
                    continue

                if not d.freeipa_installed():
                    continue

            self._targets.append(d.name)
        return bool(self._targets)

    def precheck(self, args):
        return DEPLOYMENT_SUCCESSFUL

    def execute(self, args):
        if not self._targets:
            logger.info("Skipping, no eligible servers selected")
            return DEPLOYMENT_SKIP

        for host in self._targets:
            pw = generate_secure_string()
            device = Device.get_device(host, profile_name=self.profile)
            device.set_prop(props.prop_management_credentials_password, pw)

        lockdown_task = LockdownNodes(self.profile)
        results = lockdown_task.run(self._targets)

        for server, status in results.items():
            d = Device.get_device(server, self.profile)
            if status.status == Status.FAILED:
                d.update_stage(
                    DeviceDeploymentStages.LOCKDOWN.value,
                    DeploymentStageStatus.INCOMPLETE.value,
                    status.message
                )
            elif status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.LOCKDOWN.value,
                    DeploymentStageStatus.COMPLETED.value,
                    status.message
                )

        return DEPLOYMENT_SUCCESSFUL

    def validate_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        return False

    def validate(self, args):
        return DEPLOYMENT_SKIP

