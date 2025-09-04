import logging

from deployment_manager.db.const import (
    DeploymentStageStatus,
    DeviceDeploymentStages,
)
from deployment_manager.db.models import (
    Device, QuerySet, )
from deployment_manager.deploy.common import DEPLOYMENT_SUCCESSFUL, DEPLOYMENT_SKIP, FATAL_EC
from deployment_manager.deploy.node import DeployOsProvisionBase
from deployment_manager.tools.pb1.ansible_task import (ConfigureRootServer)
from deployment_manager.tools.pb1.device_status import Status

logger = logging.getLogger(__name__)

class DeployRootServer(DeployOsProvisionBase):
    """ Push button 1 deploy root server
    """
    def __init__(self, profile):
        super().__init__(profile)
        rs_name = self.profile_config["basic"].get("root_server", "")
        self._root_server = Device.get_device(rs_name, self.profile)

    @classmethod
    def name(cls):
        return "deploy_root_server"

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        if devices and self._root_server.name not in [d.name for d in devices]:
            return False
        if force or not self.skip():
            self._targets = [self._root_server.name]
        return bool(self._targets)

    def skip(self):
        if not self._root_server.deployment_stage:
            return False

        if self._root_server.deployment_stage > DeviceDeploymentStages.IPMI_CONFIGURATION.value:
            return True

        if self._root_server.ipmi_configured():
            return True

        return False

    def precheck(self, args):
        return DEPLOYMENT_SUCCESSFUL

    def execute(self, args):
        if not self._targets:
            logger.info("Skipping, no devices to execute")
            return DEPLOYMENT_SKIP

        configure_root_server = ConfigureRootServer(
            self.profile
        )
        result = configure_root_server.run(self._targets)
        result = result[self._targets[0]]
        if result.status != Status.OK:
            self._root_server.update_stage(
                DeviceDeploymentStages.IPMI_CONFIGURATION.value,
                DeploymentStageStatus.FAILED.value,
                msg=result.message,
            )
            return FATAL_EC
        return DEPLOYMENT_SUCCESSFUL

    def validate(self, args):
        if not self._targets:
            logger.info("Skipping, no device to validate")
            return DEPLOYMENT_SKIP

        if self._root_server.device_role == "IN":
            self._root_server.update_stage(
                DeviceDeploymentStages.NETWORK_PUSH.value,
                DeploymentStageStatus.COMPLETED.value
            )
        else:
            self._root_server.update_stage(
                DeviceDeploymentStages.OS_INSTALLATION.value,
                DeploymentStageStatus.COMPLETED.value
            )
        return DEPLOYMENT_SUCCESSFUL