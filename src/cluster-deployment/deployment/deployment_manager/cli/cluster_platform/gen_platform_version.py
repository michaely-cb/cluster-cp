import logging
import os

from deployment_manager.db.models import Device
import deployment_manager.db.device_props as props
from deployment_manager.tools.ssh import SSHConn
from deployment_manager.tools.utils import (
    TaskRunner,
    exec_cmd,
    exec_remote_cmd
)

logger = logging.getLogger(__name__)


# Utility functions for generating platform-version.json file on the servers.
# Used by ClusterPlatformGenPlatformVersion and ClusterPlatformUpdateServers.


def gen_platform_version(server: str, role: str, vendor: str) -> int:
    """Generate platform-version.json on the given server. Return 0 if succeed, -1 otherwise"""

    # FIXME: add cronjob to the server to refresh the platform-version.json file
    # periodically.
    dir_path = os.path.dirname(os.path.realpath(__file__))
    package_manifest = os.path.join(
        dir_path, "../../platform/manifests/data/package_manifest.json"
    )
    config_manifest = os.path.join(
        dir_path, "../../platform/manifests/data/config_manifest.json"
    )
    cbcore_compat = os.path.join(
        dir_path, "../../platform/manifests/data/cbcore_compatibility.json"
    )
    platform_script = os.path.join(
        dir_path, "../../platform/manifests/platform_version.py"
    )

    tmp_dir = "/tmp/platform-version"
    ret, _, _ = exec_remote_cmd(f"mkdir -p {tmp_dir}", server, "root")
    if ret:
        logger.error(f"Failed to create temp directory {tmp_dir} on {server}")
        return -1

    with SSHConn(server, "root") as conn:
        conn.scp_file(package_manifest, os.path.join(tmp_dir, os.path.basename(package_manifest)))
        conn.scp_file(config_manifest, os.path.join(tmp_dir, os.path.basename(config_manifest)))
        conn.scp_file(cbcore_compat, os.path.join(tmp_dir, os.path.basename(cbcore_compat)))
        conn.scp_file(platform_script, os.path.join(tmp_dir, os.path.basename(platform_script)))

    cmd = f"python3 {tmp_dir}/platform_version.py --package {tmp_dir}/package_manifest.json --config {tmp_dir}/config_manifest.json --cbcore {tmp_dir}/cbcore_compatibility.json --output /opt/cerebras/platform-version.json --role {role} --vendor {vendor}"
    ret, _, _ = exec_remote_cmd(f"echo '{cmd}' > {tmp_dir}/rerun; chmod a+x {tmp_dir}/rerun", server, "root")
    if ret:
        logger.error(f"Failed to create {tmp_dir}/rerun file on {server}")
        return -1

    ret, _, _ = exec_remote_cmd(cmd, server, "root")
    if ret:
        logger.error(f"Failed to generate platform-version.json file on {server}")
        return -1

    # Keep the temp directory for now to help local debugging and rerunning.
    # ret, _, _ = exec_remote_cmd(f"rm -rf {tmp_dir}", server, "root")
    # if ret:
    #     logger.error(f"Failed to delete temp directory {tmp_dir} on {server}")
    #     return -1

    return 0


def gen_platform_versions(servers: [Device]) -> int:
    """Generate platform-version.json on the given list of servers. Return the number of failed servers."""

    logger.info(f"Generating platform-version.json file on {len(servers)} servers ...")

    tr = TaskRunner(gen_platform_version)
    arg_iter = [(s.name,
                 s.device_role,
                 s.get_prop(props.prop_vendor_name)
                ) for s in servers]
    results = tr.run(arg_iter)

    failures = []
    for i, s in enumerate(servers):
        if results[i] != 0:
            failures.append(s.name)

    logger.info(f"{len(servers) - len(failures)} succeeded, {len(failures)} failed.")
    if len(failures) != 0:
        logger.info(f"Failing ones: {failures}")

    return len(failures)
