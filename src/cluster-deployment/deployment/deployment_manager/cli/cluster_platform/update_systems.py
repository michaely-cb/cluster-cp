import logging
import os
import paramiko
import tempfile
import time
from threading import Thread
from typing import Dict, List, Tuple

from deployment_manager.cli.cluster_platform.update_status import UpdateStatus
from deployment_manager.cli.system.status import SystemStatus
from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device
from deployment_manager.tools.system import SystemCtl, perform_group_action
from deployment_manager.tools.utils import (
    exec_cmd,
    prompt_confirm,
)

logger = logging.getLogger(__name__)


def _update_system(system, username, password, image, force, skip_activate, log_file):
    """Update a system and send the outputs to log_file"""
    # system specific logger
    sys_logger = logging.getLogger(system)
    sys_logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_file)
    sys_logger.addHandler(fh)

    system = SystemCtl(system, username, password, logger=sys_logger)
    system.update_service(image, force, skip_activate)


def _update_systems(
    systems: List[Device],
    image: str,
    username: str,
    password: str,
    force: bool,
    skip_activate: bool,
):
    """Update the systems in parallel"""

    # Return 0 if all systems updated successfully, 1 otherwise.

    tmp_dir = tempfile.mkdtemp(prefix="update_systems_")

    logger.info(
        f"Updating {len(systems)} systems. This will take about an hour. The detailed progress can be found in the log files under {tmp_dir}."
    )

    threads = []
    for s in systems:
        system_name = s.name
        logger.info(f"Updating system {system_name} ...")
        log_file = os.path.join(tmp_dir, f"{system_name}.log")

        t = Thread(
            target=_update_system,
            args=(
                system_name,
                username
                or s.get_prop(
                    props.prop_management_credentials_user, include_default=False
                )
                or "admin",
                password
                or s.get_prop(
                    props.prop_management_credentials_password, include_default=False
                )
                or "",
                image,
                force,
                skip_activate,
                log_file,
            ),
        )
        t.start()
        threads.append(t)
        # Wait a little bit to prevent overloading the 1G network
        time.sleep(20)

    success_updates = 0
    failed_updates = 0
    failed_systems = []
    in_progress_systems = [s.name for s in systems]
    joined = [False] * len(systems)
    while True:
        for i, t in enumerate(threads):
            if joined[i]:
                continue

            if not t.is_alive():
                # systems[i] just finished, report status
                t.join()
                joined[i] = True
                s = systems[i].name
                in_progress_systems.remove(s)
                log_file = os.path.join(tmp_dir, f"{s}.log")
                ret, _, _ = exec_cmd(
                    f"grep -E 'Successfully updated | already has the image installed' {log_file}",
                    logcmd=False
                )
                if ret:
                    logger.info(f"Failed to update system {s}. Update log:")
                    logger.info(open(log_file, "r").read())
                    failed_systems.append(s)
                    failed_updates += 1
                else:
                    logger.info(f"Successfully updated system {s}")
                    success_updates += 1

        in_progress = len(systems) - failed_updates - success_updates
        if in_progress == 0:
            # All done
            break
        else:
            if in_progress > 3:
                logger.info(
                    f"{success_updates} systems updated successfully, {failed_updates} failed, {in_progress} in progress ..."
                )
            else:
                # Show the last few systems
                logger.info(
                    f"{success_updates} systems updated successfully, {failed_updates} failed, {in_progress} ({', '.join(in_progress_systems)}) in progress ..."
                )
            time.sleep(60)

    if failed_updates:
        logger.info(f"Failed to update {', '.join(failed_systems)}")

    return failed_systems


# Core function
def update_systems(
    image: str,
    systems: List[Device],
    no_confirm: bool,
    force_update: bool,
    skip_activate: bool,
    username: str,
    password: str,
) -> Tuple[int, Dict[str, UpdateStatus]]:
    """
    Update CS systems with a new system image

    Parameters:
        image: system image file for update
        systems: List of systems to update.
        no_confirm: Do not prompt user before updating the systems
        force_update: Force update the systems even when they already have the image installed.
        skip_activate: Do not activate the systems after the update.
        username: username for system login (optional)
        password: password for system login (optional)

    Returns:
        (int, Dict[str, UpdateStatus]):
            Overall status and each system's UpdateStatus enum
            For overall status: 0 if OK, 1 otherwise
    """
    if not os.path.isfile(image):
        # More robust check?
        logger.info(f"{image} is not a valid file")
        return (1, {})

    system_names = [s.name for s in systems]

    if not no_confirm:
        print(
            f"Updating {len(systems)} systems:\n  " + "\n  ".join(system_names)
        )
        print("Please ensure there are no running jobs on these systems.")
        if not prompt_confirm("Proceed to update systems (y/n):"):
            print("Abort updating systems.")
            return (0, {})
    else:
        logger.info(f"Updating {len(systems)} system(s) ...")

    failed_systems = _update_systems(
        systems, image, username, password, force_update, skip_activate
    )
    if len(failed_systems) > 0:
        logger.error(
            f"Failed to update {len(failed_systems)} out of {len(systems)} systems!"
        )
    else:
        logger.info(f"Successfully updated {len(systems)} systems.")

    # Show the system summary
    ctls = [
        SystemCtl(
            d.name,
            username
            or d.get_prop(props.prop_management_credentials_user, include_default=False)
            or "admin",
            password
            or d.get_prop(
                props.prop_management_credentials_password, include_default=False
            )
            or "",
            logger=logger,
        )
        for d in systems
    ]
    results = perform_group_action(ctls, SystemCtl.get_system_status.__name__)
    logger.info(
        SystemStatus.format_reprs(
            [v for v in results.values() if not isinstance(v, str)]
        )
    )
    errs = [v for v in results.values() if isinstance(v, str)]
    if errs:
        logger.error(f"errors while retrieving status: {errs}")

    ret = {}
    for s in system_names:
        if s in failed_systems:
            ret[s] = UpdateStatus.FAILED
        else:
            ret[s] = UpdateStatus.OK
    return (0, ret)
