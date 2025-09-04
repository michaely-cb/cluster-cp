import logging
import pathlib
from typing import List, Optional

from deployment_manager.cli.health import HealthRepr
from deployment_manager.conftool.confgen import write_master_config
from deployment_manager.db.const import DeploymentDeviceTypes, DeploymentDeviceRoles
from deployment_manager.db.models import Device, QuerySet, HealthState
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.pb1.health_check import (
    get_device_health_all,
)

logger = logging.getLogger(__name__)

def generate_pb2_master_config(
    profile: str, output_path: str, exclude_devices: List[str] = (), devices: Optional[QuerySet[Device]] = None
) -> List[Device]:
    cg = ConfGen(profile)

    included = []
    deployed_devices = 0
    new_servers = 0
    new_switches = 0
    new_systems = 0
    unhealthy_server = []
    unhealthy_switch = []
    unhealthy_system = []

    if devices is None:
        devices = Device.get_all(profile)
    query_set = devices.exclude(name__in=exclude_devices)

    for device in query_set:

        # skip infra nodes
        if device.device_role == DeploymentDeviceRoles.INFRA.value:
            continue

        # collect all previously-deployed devices
        # for switch/servers/devices
        if device.network_generate():
            included.append(device)
            deployed_devices += 1
            continue

        # add new servers in the deploy process with good health
        if device.device_type == DeploymentDeviceTypes.SERVER.value and device.nics_collected():
            health_statuses = get_device_health_all(device)
            health_repr = HealthRepr.from_dict(device.name, health_statuses)
            if (
                health_repr.overall == HealthState.OK
                or health_repr.overall == HealthState.WARNING
            ):
                included.append(device)
                new_servers += 1
            else:
                unhealthy_server.append(device.name)

        # add all new healthy switches
        elif device.device_type == DeploymentDeviceTypes.SWITCH.value:
            if device.device_role not in (  # 100G+ switches
                    DeploymentDeviceRoles.MEMORYX.value,
                    DeploymentDeviceRoles.SWARMX.value,
                    DeploymentDeviceRoles.LEAF.value,
                    DeploymentDeviceRoles.SPINE.value,
            ):
                continue

            health_statuses = get_device_health_all(device)
            health_repr = HealthRepr.from_dict(device.name, health_statuses)
            if (
                health_repr.overall == HealthState.OK
                or health_repr.overall == HealthState.WARNING
            ):
                included.append(device)
                new_switches += 1
            else:
                unhealthy_switch.append(device.name)

        # add all new healthy systems
        elif device.device_type == DeploymentDeviceTypes.SYSTEM.value:
            health_statuses = get_device_health_all(device)
            health_repr = HealthRepr.from_dict(device.name, health_statuses)
            if health_repr.overall == HealthState.OK:
                included.append(device)
                new_systems += 1
            else:
                unhealthy_system.append(device.name)

    if exclude_devices or unhealthy_switch or unhealthy_system:
        logger.warning(f"Skipped {len(exclude_devices)} excluded devices, "
                              f"{len(unhealthy_server)} unhealthy servers, {len(unhealthy_switch)} unhealthy switches, "
                              f"{len(unhealthy_system)} unhealthy systems.")
    logger.info(
        f"Found {len(included)} devices for PB2, "
        f"deployed devices: {deployed_devices}, new servers: {new_servers}, "
        f"new switches: {new_switches}, new systems: {new_systems}"
    )

    # write master config
    write_master_config(
        cg.input_file,
        included,
        str(pathlib.Path(output_path).absolute())
    )
    return included
