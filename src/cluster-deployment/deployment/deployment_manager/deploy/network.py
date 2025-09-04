import abc
import argparse
import dataclasses
import json
import logging
import os
from typing import List, Optional

from tabulate import tabulate

import deployment_manager.network_config.cli.sanity as nw_deploy_sanity
import deployment_manager.network_config.deploy as nw_deploy
from deployment_manager.common.lock import LOCK_NETWORK, with_lock
from deployment_manager.db.const import (
    DeploymentDeviceRoles, DeploymentDeviceTypes, DeploymentStageStatus,
    DeviceDeploymentStages,
)
from deployment_manager.db.models import Device, QuerySet
from deployment_manager.deploy.common import DEPLOYMENT_SKIP, DEPLOYMENT_SUCCESSFUL, Deploy, FATAL_EC
from deployment_manager.tools.const import (CLUSTER_DEPLOYMENT_BASE)
from deployment_manager.tools.master_config import generate_pb2_master_config
from deployment_manager.tools.middleware import AppCtxImpl

logger = logging.getLogger(__name__)


class DeployNetwork(Deploy, abc.ABC):
    def __init__(self, profile):
        super().__init__(profile)
        self.cfg_dir = self._cg.config_dir
        self.master_file = self._cg.master_file
        self.network_config_file = self._cg.network_config_file
        self.env = os.environ
        self.env["PYTHONPATH"] = self.env.get("PYTHONPATH", "") + f":{CLUSTER_DEPLOYMENT_BASE}/network_config"
        self.env["NODE_PASSWORD"] = self.profile_config["passwords"]["node_password"]
        self.env["SWITCH_PASSWORD"] = self.profile_config["passwords"]["switch_password"]
        self._app_ctx = AppCtxImpl(self.profile)


class DeployNetworkGenerate(DeployNetwork):
    """ Push button 2 (network) config generation
    """

    def __init__(self, profile, dev_command_network_config_file=None):
        super().__init__(profile)
        self._target_devices = []
        self.dev_command_network_config_file = dev_command_network_config_file

    @classmethod
    def name(cls):
        return "deploy_network_generate"

    def _generate_master_config(self, devices: QuerySet[Device]) -> List[str]:
        self._target_devices = generate_pb2_master_config(self.profile, self.master_file, devices=devices)
        root_server_name = self.profile_config["basic"]["root_server"]
        self._root_server = Device.get_device(root_server_name, self.profile)
        return [d.name for d in self._target_devices]

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = self._generate_master_config(devices)
        return bool(self._targets)

    def precheck_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        return self.execute_get_targets(devices=devices, force=force)

    def validate_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        return self.execute_get_targets(devices=devices, force=force)

    def precheck(self, args):
        ret, out = nw_deploy.cmd_precheck(self.master_file)
        if ret:
            logger.error("\n" + tabulate([[e.device, e.error] for e in out], ["Device", "Error"]))
            return FATAL_EC
        return 0

    @with_lock(LOCK_NETWORK)
    def execute(self, args):
        if not self.dev_command_network_config_file and \
           not self._root_server.network_initialized() and \
           not os.path.isfile(self.network_config_file):
            ret, err = nw_deploy.cmd_init(
                config_file=self.master_file,
                output_file=self.network_config_file,
                app_ctx=self._app_ctx,
            )
            if ret:
                logger.error(err)
                self._root_server.update_stage(
                    DeviceDeploymentStages.NETWORK_INIT.value,
                    DeploymentStageStatus.FAILED.value
                )
                return ret
            else:
                self._root_server.update_stage(
                    DeviceDeploymentStages.NETWORK_INIT.value,
                    DeploymentStageStatus.COMPLETED.value
                )

        ret, err = nw_deploy.cmd_generate(
            master_config_path=self.master_file,
            network_config_path=self.dev_command_network_config_file or self.network_config_file,
            artifacts_dir=f"{self.cfg_dir}/config",
            app_ctx=self._app_ctx,
        )
        if ret:
            logger.error(err)

        return ret

    def validate(self, args, update_status=True):
        with open(self.network_config_file) as f:
            nw_config = json.load(f)
        node_min_data_nic_count = self._cg.get_expected_role_nic_counts()
        result = nw_deploy_sanity.sanity_check(nw_config, node_min_data_nic_count)
        logger.debug(f"network sanity result: {json.dumps(result.asdict())}")

        if result.allocation_errors:
            # these must immediately fail the deployment
            logger.error(
                "Network configuration sanity check detected fatal allocation errors:\n"
                f"{json.dumps([dataclasses.asdict(r) for r in result.allocation_errors], indent=2)}"
            )
            return FATAL_EC

        def update_device_stage(d: Device, error_msg: str = ""):
            if not update_status:
                return
            if not error_msg:
                d.update_stage(
                    DeviceDeploymentStages.NETWORK_GENERATE.value,
                    DeploymentStageStatus.COMPLETED.value
                )
            else:
                d.update_stage(
                    DeviceDeploymentStages.NETWORK_GENERATE.value,
                    DeploymentStageStatus.FAILED.value,
                    msg=error_msg
                )

        errors_by_device = {}
        for result in result.device_errors:
            errors_by_device[result.name] = result.results

        has_fatal_errors = False
        for d in self._target_devices:
            device_result = errors_by_device.get(d.name, [])
            fatal_errors = [err for err in device_result if err.fatal]
            non_fatal_errors = [err for err in device_result if not err.fatal]
            if fatal_errors:
                error_msg = f"{len(fatal_errors)} fatal validation errors: " + "; ".join(f"{e.type}: {e.message}" for e in fatal_errors)
                logger.error(f"Device {d.name} has {error_msg}")
                update_device_stage(d, error_msg)
                has_fatal_errors = True
            elif non_fatal_errors:
                logger.warning(f"Device {d.name} has non-fatal validation errors: " + "; ".join(f"{e.type}: {e.message}" for e in non_fatal_errors))

            if not fatal_errors:
                update_device_stage(d)

        if has_fatal_errors:
            logger.info(f"Network configuration validation failed for some devices")
            return FATAL_EC

        return DEPLOYMENT_SUCCESSFUL


class DeployNetworkPush(DeployNetwork):
    """ Push button 2 (network) config push
    """

    def __init__(self, profile):
        super().__init__(profile)
        self.switches = None
        self.systems = None
        self.servers = None

    @classmethod
    def name(cls):
        return "deploy_network_push"

    def _collect_switches(self, devices: QuerySet, force=False) -> List[Device]:
        query_set = devices.filter(
            device_type=DeploymentDeviceTypes.SWITCH.value,
            device_role__in=(DeploymentDeviceRoles.SPINE.value, DeploymentDeviceRoles.LEAF.value,
                             DeploymentDeviceRoles.SWARMX.value, DeploymentDeviceRoles.MEMORYX.value,))
        switches = []
        for s in query_set:
            if force or (s.network_generate() and not s.network_pushed()):
                switches.append(s)
        return switches

    def _collect_systems(self, devices: QuerySet, force=False) -> List[Device]:
        query_set = devices.filter(device_type=DeploymentDeviceTypes.SYSTEM.value)
        systems = []
        for s in query_set:
            if force or (s.network_generate() and not s.network_pushed()):
                systems.append(s)
        return systems

    def _collect_servers(self, devices: QuerySet, force=False) -> List[Device]:
        query_set = devices.filter(device_type=DeploymentDeviceTypes.SERVER.value).exclude(device_role="IN")
        servers = []
        for s in query_set:
            if force or (s.network_generate() and not s.network_pushed()):
                servers.append(s)
        return servers

    def precheck(self, args):
        return DEPLOYMENT_SUCCESSFUL

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self.systems = self._collect_systems(devices=devices, force=force)
        self.servers = self._collect_servers(devices=devices, force=force)
        self.switches = self._collect_switches(devices=devices, force=force)
        self._targets = [s.name for s in self.switches + self.systems + self.servers]
        return bool(self._targets)

    @with_lock(LOCK_NETWORK)
    def execute(self, args):
        if not self._targets:
            logger.info("Skipping, no devices to configure")
            return DEPLOYMENT_SKIP

        # switches
        if self.switches:
            ret, err = self.exec_func(nw_deploy.cmd_push_switches, argparse.Namespace(
                config_file=self.master_file,
                network_config=self.network_config_file,
                artifacts_dir=f"{self.cfg_dir}/config",
                names=[s.name for s in self.switches]
            ))
            if ret != 0:
                logger.error(f"Failed to push to switches - {err}")
                return FATAL_EC

        if self.systems:
            ret, err = nw_deploy.cmd_push_systems(
                names=[s.name for s in self.systems],
                config_file=self.master_file,
                artifacts_dir=f"{self.cfg_dir}/config",
                network_config=self.network_config_file,
                app_ctx=self._app_ctx,
            )
            if ret != 0:
                logger.error(f"Failed to push to systems - {err}")
                return FATAL_EC

        if self.servers:
            ret, err = nw_deploy.cmd_push_nodes(
                config_file=self.master_file,
                artifacts_dir=f"{self.cfg_dir}/config",
                network_config=self.network_config_file,
                names=[s.name for s in self.servers],
                app_ctx=self._app_ctx,
            )
            if ret != 0:
                logger.error(f"Failed to push to servers - {err}")
                return FATAL_EC
        return 0

    def validate_get_targets(self, devices: Optional[QuerySet[Device]] = None, force: bool = False) -> bool:
        return self.execute_get_targets(devices=devices, force=force)

    def _mark_completed(self, device):
        device.update_stage(
            DeviceDeploymentStages.NETWORK_PUSH.value,
            DeploymentStageStatus.COMPLETED.value
        )

    def _mark_failed(self, device, msg=""):
        device.update_stage(
            DeviceDeploymentStages.NETWORK_PUSH.value,
            DeploymentStageStatus.FAILED.value,
            msg=msg
        )

    def validate(self, args):
        servers = self.servers or []
        systems = self.systems or []
        switches = self.switches or []

        if servers:
            node_errs = nw_deploy.validate_nodes(self.network_config_file, [s.name for s in servers], self._app_ctx)
            for node, err in node_errs.items():
                if err and node in servers:
                    self._mark_failed(Device.get_device(node, self.profile), err)

            servers = [s for s in servers if not node_errs.get(s.name)]

        if systems:
            sys_errs = nw_deploy.validate_systems(self.network_config_file, [s.name for s in systems], self._app_ctx)

            for sys, err in sys_errs.items():
                if err and sys in systems:
                    self._mark_failed(Device.get_device(sys, self.profile), err)

            systems = [s for s in systems if not sys_errs.get(s.name)]

        if switches:
            sw_errs = nw_deploy.validate_switches(self.network_config_file, [s.name for s in switches], self._app_ctx)

            for sw, err in sw_errs.items():
                if err and sw in switches:
                    self._mark_failed(Device.get_device(sw, self.profile), err)

            switches = [s for s in switches if not sw_errs.get(s.name)]

        for d in servers + systems + switches:
            self._mark_completed(d)

        return DEPLOYMENT_SUCCESSFUL
