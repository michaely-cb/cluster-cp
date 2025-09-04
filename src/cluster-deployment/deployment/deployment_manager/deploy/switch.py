import logging

from deployment_manager.db.const import DeploymentDeviceTypes, DeploymentStageStatus, DeviceDeploymentStages
from deployment_manager.db.models import Device, QuerySet
from deployment_manager.deploy.common import Deploy, DEPLOYMENT_SUCCESSFUL, DEPLOYMENT_SKIP
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.dhcp import interface as dhcp
from deployment_manager.deploy.utils import run_dhcp_updates
from deployment_manager.tools.pb1.device_status import Status
from deployment_manager.tools.pb1.device_task import WaitTask
from deployment_manager.tools.pb1.validator import OsValidator
from deployment_manager.tools.switch.ztp_config import setup_ztp


logger = logging.getLogger(__name__)


class DeploySwitchZtp(Deploy):
    """ ZTP on switches
    """

    def __init__(self, profile):
        super().__init__(profile)
        self._target_devices = []

    @classmethod
    def name(cls):
        return "switch_ztp"

    def precheck_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        return self.execute_get_targets(devices, force=force)

    def precheck(self, args) -> int:
        return DEPLOYMENT_SUCCESSFUL

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SWITCH.value):
            if not force:
                if d.provisioned():
                    continue
            self._targets.append(d.name)

        return bool(self._targets)

    def execute(self, args) -> int:
        if not self._targets:
            # TODO: For this and other deployment targets in general,
            # we need to provide device-wise information indicating why devices were ignored
            logger.info(f"{self.name()} has nothing to do on given targets")
            return DEPLOYMENT_SKIP

        # generate config
        switch_devices = Device.get_switches(self.profile).filter(name__in=self._targets)
        ztp_results = setup_ztp(self.profile, switch_devices)
        failures = 0
        for s in switch_devices:
            err = ztp_results[s]
            if err:
                failures += 1
                s.update_stage(
                    DeviceDeploymentStages.PROVISIONING.value,
                    DeploymentStageStatus.FAILED.value,
                    err
                )

        # update dhcp
        if failures < switch_devices.count():
            cg = ConfGen(self.profile)
            cfg = cg.parse_profile()
            root_server = cfg["basic"]["root_server"]
            dhcp_provider = dhcp.get_provider(self.profile, cfg)
            server_configs = dhcp_provider.generate_configs()
            err = run_dhcp_updates(
                    self.profile,
                    dhcp_provider,
                    server_configs,
                    mirror_servers=cfg["basic"].get("mirror_dhcp_servers", []),
            )
            for s in switch_devices:
                if not ztp_results[s]:
                    if err:
                        s.update_stage(
                            DeviceDeploymentStages.PROVISIONING.value,
                            DeploymentStageStatus.FAILED.value,
                            err
                        )
                    else:
                        s.update_stage(
                            DeviceDeploymentStages.PROVISIONING.value,
                            DeploymentStageStatus.STARTED.value
                        )

        return DEPLOYMENT_SUCCESSFUL

    def validate_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        self._targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SWITCH.value):
            if not force:
                if d.provisioned():
                    continue
            if force or d.deployment_status in (
                    DeploymentStageStatus.STARTED.value,
                    DeploymentStageStatus.INCOMPLETE.value,
            ):
                self._targets.append(d.name)
        return bool(self._targets)

    def validate(self, args):
        if not self._targets:
            logger.info(f"{self.name()} has nothing to validate")
            return DEPLOYMENT_SKIP

        # Wait for a total of 15mins over 3 retries
        attempt = 0
        wt = WaitTask(self.profile, 300)
        failures = 0
        while attempt < 5:
            if failures:
                logger.info(f"ZTP not complete on {failures} out of {len(self._targets)} switches")
            failures = 0
            logger.info(f"ZTP validation attempt {attempt+1}")
            os_validator = OsValidator(self.profile, 20)
            switch_results = os_validator.validate(self._targets)

            for switch, status in switch_results.items():
                d = Device.get_device(switch, self.profile)

                if status.status == Status.OK:
                    d.update_stage(
                        DeviceDeploymentStages.PROVISIONING.value,
                        DeploymentStageStatus.COMPLETED.value
                    )
                    self._targets.remove(switch)
                else:
                    d.update_stage(
                        DeviceDeploymentStages.PROVISIONING.value,
                        DeploymentStageStatus.INCOMPLETE.value,
                        status.message
                    )
                    failures += 1
            if not failures:
                break
            else:
                attempt += 1
                wt.run(self._targets)
        return 0
