import logging
from deployment_manager.deploy.common import DEPLOYMENT_SKIP, DEPLOYMENT_SUCCESSFUL, Deploy, FATAL_EC
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed
from deployment_manager.db.const import (
    DeploymentDeviceTypes, DeploymentStageStatus,
    DeviceDeploymentStages, DeploymentDeviceRoles
)
from deployment_manager.db.models import (
    Device, QuerySet, )
from typing import List, Dict, Union
from deployment_manager.client.flow_controller import BelimoFlowController
from deployment_manager.tools.utils import ping_check, tcp_connection_check
from deployment_manager.tools.flow_controller_util import (
    create_client,
    get_fc_config_updates,
    put_fc_config_updates,
    check_fc_flow_rate,
    MAX_WORKERS, TIMEOUT)
from deployment_manager.tools.pb1.device_status import Status


logger = logging.getLogger(__name__)

class DeployFlowController(Deploy):
    """ Flow controller for deployment tasks. This class is responsible for managing the execution of deployment tasks
    in a controlled manner, ensuring that tasks are executed in the correct order and with the necessary dependencies.
    """

    name = "Flow Controller"
    def __init__(self, profile):
        super().__init__(profile)
    
    @classmethod
    def name(cls):
        return "deploy_flow_controller"
    
    def precheck_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.PERIPHERAL.value, device_role=DeploymentDeviceRoles.VALVECTRL.value, profile__name=self.profile):
            if d.os_installed() and not force:
                continue

            self._targets.append(d.name)
        return bool(self._targets)
    
    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.PERIPHERAL.value, device_role=DeploymentDeviceRoles.VALVECTRL.value, profile__name=self.profile):
            if not force:
                if d.deployment_status != DeploymentStageStatus.STARTED.value and d.deployment_status != DeploymentStageStatus.INCOMPLETE.value:
                    continue
            targets.append(d.name)
        self._targets = targets 
        return bool(self._targets)
    
    def precheck(self, args):
        def check_connectivity(target):
            try:
                device = Device.get_device(target, self.profile)
                bl_cli = create_client(device)
                ping_ok = ping_check(bl_cli._addr)
                
                if not ping_ok:
                    logger.error(f"Ping check failed for {target}")
                    device.update_stage(
                        DeviceDeploymentStages.OS_INSTALLATION.value,
                        DeploymentStageStatus.NOT_STARTED.value,
                        f"Ping check failed",
                    )
                    return False

                tcp_ok = tcp_connection_check(bl_cli._addr)
                if not tcp_ok:
                    logger.error(f"TCP check failed for {target}")
                    device.update_stage(
                        DeviceDeploymentStages.OS_INSTALLATION.value,
                        DeploymentStageStatus.NOT_STARTED.value,
                        f"TCP check failed",
                    )
                    return False

                device.update_stage(
                    DeviceDeploymentStages.OS_INSTALLATION.value,
                    DeploymentStageStatus.STARTED.value,
                    f"Connectivity check passed"
                )

            except Exception as e:
                logger.error(f"Error checking connectivity for {target}: {e}")
                return False
        

        rv = DEPLOYMENT_SUCCESSFUL
        try:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(check_connectivity, target) for target in self._targets]
                for future in as_completed(futures, timeout=TIMEOUT):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error in connectivity check: {e}")

        except ValueError as e:
            logger.error(e)
            rv = FATAL_EC
        return rv
    
    def execute(self, args):
        # Initial config here
        candidates = {}
        clients = [create_client(Device.get_device(target, self.profile)) for target in self._targets]
        updates = get_fc_config_updates(clients)
        for name, update in updates.items():
             if not isinstance(update, str):
                candidates[name] = update
        
        results = put_fc_config_updates(clients, candidates)
        for name, status in results.items():
            device = Device.get_device(name, self.profile)
            # If no error during update, mark as completed
            if status.status == Status.OK:
                device.update_stage(
                    DeviceDeploymentStages.OS_INSTALLATION.value,
                    DeploymentStageStatus.COMPLETED.value,
                    status.message
                )
            elif status.status == Status.FAILED:
                device.update_stage(
                    DeviceDeploymentStages.OS_INSTALLATION.value,
                    DeploymentStageStatus.INCOMPLETE.value,
                    status.message
                )

    
        return DEPLOYMENT_SUCCESSFUL

    def validate(self, args):
        return DEPLOYMENT_SUCCESSFUL