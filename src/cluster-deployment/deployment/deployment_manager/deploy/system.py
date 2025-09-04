import logging
import os
import pathlib
from typing import List

from deployment_manager.common.models import well_known_secrets, K_SYSTEM_ROOT_PASSWORD, K_SYSTEM_ADMIN_PASSWORD
from deployment_manager.db import device_props as props
from deployment_manager.db.const import (
    DeploymentDeviceTypes, DeploymentStageStatus,
    DeviceDeploymentStages,
)
from deployment_manager.db.models import (
    Device, QuerySet, )
from deployment_manager.deploy.common import Deploy, DEPLOYMENT_SUCCESSFUL, FATAL_EC
from deployment_manager.tools.pb1 import system_task
from deployment_manager.tools.pb1.device_status import Status
from deployment_manager.tools.pb1.device_task import SerialTask
from deployment_manager.tools.pb1.system_task import SystemPasswordTask
from deployment_manager.tools.secrets import create_secrets_provider
from deployment_manager.tools.system import SystemCtl

logger = logging.getLogger(__name__)

_sys_image_path_env = "DM_SYSTEM_IMAGE_PATH"
_pkg_dir = pathlib.Path(os.getenv("DEPLOYMENT_PACKAGES", "../tools"))

def find_system_image_candidate() -> pathlib.Path:
    """ Need to find one definitive system image to install with. Search deployment packages for a single image if
    the user has not specified an override.
    """

    # allow for overrides using envvar
    override = os.getenv(_sys_image_path_env)
    if override:
        override = pathlib.Path(override)
        if not override.is_file():
            raise ValueError(f"specified {_sys_image_path_env}={override} but file did not exist")
        return override

    image_candidates = [p for p in _pkg_dir.glob("CS1-*") if p.name.endswith(".tar.gz")]

    msg = f"Please ensure exactly one CS1-*.tar.gz image is present in {_pkg_dir} or specify an image with envvar {_sys_image_path_env}"
    if len(image_candidates) > 1:
        raise ValueError(f"more than one system image discovered. {msg}")
    elif not image_candidates:
        raise ValueError(f"no system image found. {msg}")
    return image_candidates.pop()


def generate_sysctl(profile: str, name: str) -> SystemCtl:
    d = Device.get_device(name, profile)
    if not d:
        raise ValueError(f"unable to find Device entry for system {name}")
    return SystemCtl(
        d.name,
        d.get_prop(props.prop_management_credentials_user),
        d.get_prop(props.prop_management_credentials_password)
    )


class DeploySystems(Deploy):
    """
    Push button 1 for systems. Step one changes the system root and admin passwords if specified in the input.yml.
    Step two updates datacenter specific settings, also optionally specified in input.yml. Finally, step three
    updates the system image.

    To specify the image to update the system with, a single image file must be in /opt/cerebras/cluster-deployment/packages
    It is typically named CS1-<version>-<build_id>.tar.gz and comes from /cb/artifacts/releases/Cerebras_CS1-x.x.x/components/itb-sm/
    directory. Alternatively, an override path may be specified with envvar DM_SYSTEM_IMAGE_PATH.
    """
    parallelism = 32

    def __init__(self, profile):
        super().__init__(profile)
        self.status = []
        self._targets: List[str] = []

    @classmethod
    def name(cls):
        return "deploy_systems"

    def precheck_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        return self.execute_get_targets(devices, force = force)

    def precheck(self, args):
        rv = DEPLOYMENT_SUCCESSFUL
        try:
            image = find_system_image_candidate()
            logger.info(
                f"Systems will update to image: {image.absolute()}\n"
                f"To use another image, replace the current image in {_pkg_dir} or set envvar {_sys_image_path_env}"
            )
        except ValueError as e:
            logger.error(e)
            rv = FATAL_EC
        return rv

    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        targets = []
        for d in devices.filter(device_type=DeploymentDeviceTypes.SYSTEM.value):
            if not d.os_installed() or force:
                targets.append(d.name)
        self._targets = targets
        return bool(self._targets)

    def execute(self, args):
        if not self._targets:
            return DEPLOYMENT_SUCCESSFUL

        # generate sysctls on each task as the admin password can change. This uses the potentially updated password
        sysctl_fn = lambda name: generate_sysctl(self.profile, name)

        tasks = []

        # update passwords to cluster specific passwords
        provider = create_secrets_provider(self.profile_config)
        try_root_pws = list(
            {provider.get(K_SYSTEM_ROOT_PASSWORD), well_known_secrets().get(K_SYSTEM_ROOT_PASSWORD)}
        )

        # this logic assumes the passwords are set correctly if the well known secrets match the requested ones
        # Ideally we would check but this is purposeful to avoid handling when root SSH is disabled, see CS-1272
        req_admin_pw = provider.get(K_SYSTEM_ADMIN_PASSWORD)
        req_root_pw = provider.get(K_SYSTEM_ROOT_PASSWORD)
        if req_admin_pw != well_known_secrets().get(K_SYSTEM_ADMIN_PASSWORD):
            tasks.append(
                SystemPasswordTask(self.profile, try_root_pws, "admin", provider.get(K_SYSTEM_ADMIN_PASSWORD)),
            )
        if req_root_pw != well_known_secrets().get(K_SYSTEM_ROOT_PASSWORD):
            tasks.append(
                SystemPasswordTask(self.profile, try_root_pws, "root", provider.get(K_SYSTEM_ROOT_PASSWORD)),
            )

        # update global syscfg settings if needed
        syscfgs = self.profile_config.get("basic", {}).get("global_system_configs", {})
        if syscfgs:
            tasks.append(system_task.SystemConfigUpdateTask(self.profile, sysctl_fn, syscfgs))

        image = find_system_image_candidate()
        image_path = str(image.absolute())
        logger.info(f"Installing {image_path} on {len(self._targets)} systems")
        tasks.extend([
            system_task.SystemImageUpdateTask(self.profile, sysctl_fn, image_path),
            system_task.SystemActivateTask(self.profile, sysctl_fn)
        ])

        parent_task = SerialTask(self.profile, tasks)
        results = parent_task.run(self._targets)
        for name, status in results.items():
            d = Device.get_device(name, self.profile)
            if status.status == Status.FAILED:
                d.update_stage(
                    DeviceDeploymentStages.OS_INSTALLATION.value,
                    DeploymentStageStatus.FAILED.value,
                    status.message
                )
            elif status.status == Status.OK:
                d.update_stage(
                    DeviceDeploymentStages.OS_INSTALLATION.value,
                    DeploymentStageStatus.COMPLETED.value
                )
        return DEPLOYMENT_SUCCESSFUL

    def validate(self, args):
        # TODO: run the system health check here to ungate network deployment
        return DEPLOYMENT_SUCCESSFUL
