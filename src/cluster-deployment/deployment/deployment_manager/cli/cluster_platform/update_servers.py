import logging
import os
import re
import socket
import tempfile
import yaml
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Dict, List, Tuple

from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles
from deployment_manager.db.models import Device
from deployment_manager.cli.cluster_platform.update_status import UpdateStatus
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.network_config_tool import NetworkConfigTool
from deployment_manager.tools.utils import (
    TaskRunner,
    exec_cmd,
    exec_remote_cmd,
    prompt,
)

logger = logging.getLogger(__name__)


def _get_100g_interfaces(server):
    """Get the 100G interface names of a server"""
    cmd = "netstat -i | grep 9000 | awk '{print $1}' | sort"
    ret, out, err = exec_remote_cmd(cmd, server, "root")
    if ret != 0:
        logger.error(
            f"Error in getting the 100G interface names on {server}: exit code: {ret}, stdout: {out}, stderr: {err}"
        )
        return []

    return out.split()


def _collect_100g_interfaces(servers):
    """Collect the 100G interfaces of all servers"""
    tr = TaskRunner(_get_100g_interfaces)
    arg_iter = [(s,) for s in servers]
    results = tr.run(arg_iter)

    server_interfaces = dict()
    for i, interfaces in enumerate(results):
        name = servers[i]
        server_interfaces[name] = interfaces
    return server_interfaces


def _collect_expected_100g_interfaces(profile, servers):
    """Collect expected 100G interfaces from network_config.json"""
    server_set = set(servers)

    nwt = NetworkConfigTool.for_profile(profile)
    config = nwt.nw_config()

    server_interfaces = dict()

    # Collect the 100G interface names from network_config.json
    for key, data in config.items():
        # Iterate all types of nodes
        if key.endswith("_nodes"):
            for node in data:
                node_name = node["name"]
                if node_name in server_set:
                    interfaces = []
                    for i in node.get("interfaces", []):
                        interfaces.append(i["name"])
                    interfaces.sort()
                    server_interfaces[node_name] = interfaces
    return server_interfaces


def _validate_100g_interfaces(profile, servers, curr_server_interfaces) -> int:
    """Validate if the current 100G interfaces match the ones defined in network_config.json"""
    # Return 0 if the interfaces match, -1 otherwise
    expected_interfaces = _collect_expected_100g_interfaces(profile, servers)

    network_good = True
    for server, interfaces in expected_interfaces.items():
        curr_interfaces = curr_server_interfaces[server]
        for i in interfaces:
            if i not in curr_interfaces:
                network_good = False
                logger.warning(f"Missing interface {i} in {server}")

    if network_good:
        return 0
    else:
        return -1


def _create_inventory(hosts_yaml, servers, all_ceph_nodes, cluster_servers):
    """Generate the inventory hosts.yaml for ansible run"""
    ceph_nodes = {}
    non_ceph_nodes = {}

    for s in servers:
        role, vendor = cluster_servers[s]
        if s in all_ceph_nodes:
            ceph_nodes[s] = {"role": role, "vendor": vendor}
        else:
            non_ceph_nodes[s] = {"role": role, "vendor": vendor}

    data = {
        "all": {
            "children": {
                "non_ceph_nodes": {"hosts": non_ceph_nodes},
                "ceph_nodes": {"hosts": ceph_nodes},
            }
        }
    }
    f = open(hosts_yaml, "w")
    yaml.dump(data, stream=f, default_flow_style=False)


def _get_100g_ip():
    """Get the IP of the 100G connection"""

    # Extract the IP from the first network name with mtu 9000
    cmd = "ifconfig `ifconfig | grep -m 1 'mtu 9000' | awk -F'[: ]+' '{ print $1 }'` | grep 'inet ' | awk '{print $2}'"
    ret, out, _ = exec_cmd(cmd=cmd, logcmd=False)
    if ret != 0:
        # Fail to get 100G IP, fallback to hostname, which uses 1G network
        return socket.gethostname()
    else:
        return out.strip()


def _create_vars(profile, vars_yaml, control_plane_node, patch_dir, k8s_running, force):
    """Generate the vars.yaml file containing all necessary variables for ansible run"""

    cg = ConfGen(profile)
    cfg = cg.parse_profile()

    data = {
        "deploy_mgr_host": socket.gethostname(),
        "deploy_mgr_host_100g": _get_100g_ip(),
        "control_plane_node": control_plane_node,
        "patch_dir": patch_dir,
        "k8s_running": k8s_running,
        "ansible_python_interpreter": "/usr/bin/python3.8",
        "force": force,
        "configure_log_storage": cfg["basic"].get("configure_log_storage", False),
    }
    f = open(vars_yaml, "w")
    yaml.dump(data, stream=f, default_flow_style=False)


def _parse_ansible_result(ansible_log):
    """Parse the PLAY RECAP section in the ansible log file and report success/failure/unreachable nodes"""
    # Better way would be to parse the ansible .json output, but ansible
    # can't dump normal log to stdout and json to a file at the same time.
    successes = []
    failures = []
    unreachables = []

    reach_recap = False
    with open(ansible_log, "r") as log:
        for line in log:
            if not reach_recap and "PLAY RECAP" in line:
                reach_recap = True
            elif reach_recap:
                _, rest = line.split("|", 1)
                host, summary = rest.split(":", 1)
                host = host.strip()

                if host == "127.0.0.1":
                    # Skip localhost, if any
                    continue

                # Unreachable hosts have higher precedence than the failed ones.
                if re.search("unreachable=[1-9]", summary):
                    unreachables.append(host)
                elif re.search("failed=[1-9]", summary):
                    failures.append(host)
                else:
                    successes.append(host)

    return successes, failures, unreachables


def _get_platform_version(server):
    """Extract the package version from /opt/cerebras/platform-version.json"""
    cmd = "jq -r '.package' /opt/cerebras/platform-version.json"
    ret, out, err = exec_remote_cmd(cmd, server, "root")
    if ret != 0:
        logger.error(
            f"Error in getting package version on {server}: exit code: {ret}, stdout: {out}, stderr: {err}"
        )
        return ""
    return out.strip()


def _collect_platform_versions(servers):
    """Collect the platform version of all servers"""
    tr = TaskRunner(_get_platform_version)
    arg_iter = [(s,) for s in servers]
    results = tr.run(arg_iter)

    server_version = dict()
    for i, version in enumerate(results):
        name = servers[i]
        server_version[name] = version
    return server_version


def _validate_update(servers, orig_server_interfaces) -> int:
    """Simple validation after the update"""
    # Return 0 if validation passes, -1 otherwise

    logger.info("Validating server updates ...")

    # Checks:
    # 1. platform version is created and populated properly, this catches
    #    any package errors.
    # 2. check 100G network interface, this validates if the Mellanox driver
    #    is properly installed.

    server_versions = _collect_platform_versions(servers)
    packages_good = True
    for server, version in server_versions.items():
        if version == "unknown":
            packages_good = False
            logger.error(
                f"Unknown platform version on {server} after server update. Please check the /opt/cerebras/platform-version.json file on {server} for package discrepancies."
            )

    if packages_good:
        logger.info("All platform versions validated")

    new_server_interfaces = _collect_100g_interfaces(servers)

    network_good = True
    for server, orig_interfaces in orig_server_interfaces.items():
        new_interfaces = new_server_interfaces[server]
        for i in orig_interfaces:
            if i not in new_interfaces:
                network_good = False
                logger.error(
                    f"Missing network interface {i} in {server} after server update"
                )

    if network_good:
        logger.info("All 100G network interfaces validated.")

    if packages_good and network_good:
        return 0
    else:
        return -1


def _create_reboot_scripts(
    reboot_script, is_ceph_node, control_plane_node, role, vendor
):
    """Generate the scripts to reboot the ansible host node"""

    post_reboot_script = os.path.join(
        os.path.dirname(reboot_script), "post_" + os.path.basename(reboot_script)
    )

    post_reboot_log = Path(post_reboot_script).with_suffix(".log")

    # Reboot script is simple enough to write it out directly
    with open(reboot_script, "w") as file:
        file.write("#!/bin/bash\n\n")

        if is_ceph_node:
            file.write("# Disable ceph balancing\n")
            ceph_cmd = (
                "kubectl -n rook-ceph exec deploy/rook-ceph-tools -- ceph osd set noout; "
                "kubectl -n rook-ceph exec deploy/rook-ceph-tools -- ceph osd set norebalance"
            )
            if not control_plane_node or control_plane_node == "127.0.0.1":
                file.write(ceph_cmd + "\n\n")
            else:
                file.write(f"ssh control_plane_node '{ceph_cmd}'\n\n")

        file.write(
            "# Run post reboot script only once by appending a job to crontab\n"
            f"(crontab -l 2>/dev/null; echo \"@reboot {post_reboot_script} > {post_reboot_log} 2>&1 && crontab -l | grep -v '@reboot {post_reboot_script}' | crontab -\") | crontab -\n\n"
        )

        file.write('echo "Rebooting node ..."\n' "systemctl reboot\n\n")
    os.chmod(reboot_script, 0o755)

    # Post-reboot script is more involved, use jinja2 to make it more
    # manageable.
    template_dir = os.path.join(
        os.path.dirname(__file__),
        "..",
        "templates",
    )
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template("post_reboot.sh.j2")
    output = template.render(
        role=role,
        vendor=vendor,
        is_ceph_node=is_ceph_node,
        control_plane_node=control_plane_node,
        reboot_node=socket.gethostname(),
    )
    with open(post_reboot_script, "w") as file:
        file.write(output)
    os.chmod(post_reboot_script, 0o755)


# Core function for updating servers
def update_servers(
    profile,
    control_plane_node: str,
    servers: List[str],
    patch_dir: str,
    no_confirm: bool,
    force: bool,
    verbose: bool,
) -> Tuple[int, Dict[str, UpdateStatus]]:
    """
    Apply security patches to cluster servers

    Parameters:
        profile: Cluster profile for querying server properties
        control_plane_node:
            Control plane node managing the servers(nodes) to be updated. Empty
            if the servers are being migrated in blue/green flow.
        servers: List of servers to be updated.
        patch_dir: The directory containing the security patch tar ball.
        no_confirm: Do not prompt user before updating and rebooting the servers.
        force: Force install the Mellanox driver even when it is already up-to-date.
        verbose: Enable verbose output.

    Returns:
        (int, Dict[str, UpdateStatus]):
            Overall status and each server's UpdateStatus enum
            For overall status: 0 if OK, 1 otherwise
    """

    if len(servers) == 0:
        logger.error("No servers are provided.")
        return (1, {})

    tmp_dir = tempfile.mkdtemp(prefix="update_servers_")
    server_file = os.path.join(tmp_dir, "servers.txt")
    with open(server_file, "w") as f:
        for s in servers:
            f.write(f"{s}\n")

    k8s_running = True if control_plane_node else False

    # Find all ceph nodes
    all_ceph_nodes = {}
    if control_plane_node:
        cmd = "set -o pipefail; kubectl get nodes -oname -lstorage-type=ceph | sed 's|node/||g'"
        ret, out, _ = exec_remote_cmd(cmd, control_plane_node, "root", timeout=30)
        if ret == 0:
            all_ceph_nodes = set(out.splitlines())

    logger.info(
        f"Applying all patches and Mellanox driver on {len(servers)} server(s). See {server_file} for full list of servers."
    )

    cg = ConfGen(profile)
    cfg = cg.parse_profile()
    if cfg["basic"].get("configure_log_storage", False):
        logger.info(
            "Also configuring log storage on these nodes. This will clear node local weight streaming logs if this is the first time this has been run."
        )

    if not no_confirm:
        proceed = prompt(msg="Proceed to update servers (y/n): ")
        if not proceed.lower().startswith("y"):
            logger.info("Abort updating servers.")
            return (0, {})

    # Collect current 100G network interfaces, check if they match the ones
    # defined in network_config.json.
    orig_network_interfaces = _collect_100g_interfaces(servers)
    _validate_100g_interfaces(profile, servers, orig_network_interfaces)

    if control_plane_node:
        # Mark nodes to error state to prevent them from being scheduled.
        remote_server_file = "/tmp/remote-servers.txt"
        cmd = f"scp {server_file} root@{control_plane_node}:{remote_server_file}"
        ret, _, _ = exec_cmd(cmd, logcmd=False)
        if ret != 0:
            logger.error(
                f"Failed to copy {server_file} to control plane node {control_plane_node}"
            )
            return (1, {})

        # FIXME: there is a race condition that some jobs may get started on
        # these nodes since the check.
        cmd = f"csadm.sh batch-update-nodes {remote_server_file} error cluster-upgrade-in-progress"
        ret, out, err = exec_remote_cmd(cmd, control_plane_node, "root", timeout=60)
        if ret != 0:
            msg = (
                f"Failed to update node status to maintenance mode. The failing command is \n  {cmd}\n"
                + f"with logs: {out} {err}"
                + "Once resolved, please rerun the update_servers command."
            )
            logger.error(f"{msg}")
            return (1, {})

    playbook = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "platform",
        "apply_all_updates.yaml",
    )

    cluster_servers = {
        s.name: (s.device_role, s.get_prop(props.prop_vendor_name))
        for s in Device.get_servers(profile)
        if s.device_role != DeploymentDeviceRoles.INFRA.value
    }

    inventory_file = os.path.join(tmp_dir, "hosts.yaml")
    _create_inventory(inventory_file, servers, all_ceph_nodes, cluster_servers)

    vars_file = os.path.join(tmp_dir, "vars.yaml")
    _create_vars(profile, vars_file, control_plane_node, patch_dir, k8s_running, force)

    verbose = "-v" if verbose else ""

    ansible_log = os.path.join(tmp_dir, "ansible.log")

    # TODO: tune parallelism
    cmd = f"ansible-playbook -f 250 {verbose} -i {inventory_file} {playbook} --extra-vars='@{vars_file}'"
    ansible_env = os.environ.copy()
    ansible_env["ANSIBLE_HOST_KEY_CHECKING"] = "False"
    # Change default ANSIBLE_REMOTE_TMP from ~/.ansible/tmp to /tmp, in case
    # sudo root doesn't have write permission to ~ directory.
    ansible_env["ANSIBLE_REMOTE_TMP"] = "/tmp"
    ansible_env["ANSIBLE_LOG_PATH"] = ansible_log
    ansible_env["ANSIBLE_CACHE_PLUGIN"] = "jsonfile"
    ansible_env["ANSIBLE_CACHE_PLUGIN_CONNECTION"] = os.path.join(tmp_dir, "fact_cache")
    ansible_env["ANSIBLE_CACHE_PLUGIN_TIMEOUT"] = "86400"
    ret, _, _ = exec_cmd(cmd, env=ansible_env, stream=True)

    successes, failures, unreachables = _parse_ansible_result(ansible_log)

    successes.sort()
    failures.sort()
    unreachables.sort()

    results = {}
    for s in successes:
        results[s] = UpdateStatus.OK
    for s in failures:
        results[s] = UpdateStatus.FAILED
    for s in unreachables:
        results[s] = UpdateStatus.UNREACHABLE

    logger.info(f"Successfully updated {len(successes)} servers.")
    if len(failures):
        logger.info(f"Failed to update {len(failures)} servers: {failures}")

    if len(unreachables):
        logger.info(f"Failed to connect to {len(unreachables)} servers: {unreachables}")

    success_file = os.path.join(tmp_dir, "success.txt")
    remote_success_file = "/tmp/success.txt"
    with open(success_file, "w") as f:
        for node in successes:
            f.write(f"{node}\n")

    if control_plane_node:
        # mark nodes back to schedulable if current node is marked as down by upgrade
        # there's a risk if current node needs reboot, can watch and optimize
        cmd = f"scp {success_file} root@{control_plane_node}:{remote_success_file}"
        ret, _, _ = exec_cmd(cmd, logcmd=False)
        if ret != 0:
            logger.error(
                f"Failed to copy {success_file} to control plane node {control_plane_node}"
            )
            return (1, results)

        cmd = f"csadm.sh batch-update-nodes {remote_success_file} ok cluster-upgrade-in-progress"
        ret, out, err = exec_remote_cmd(cmd, control_plane_node, "root", timeout=60)
        if ret != 0:
            msg = (
                f"Failed to update node status back to ok mode. The failing command is \n  {cmd}\n"
                + f"with logs: {out} {err}"
                + f"Once resolved, please rerun the command\n  {cmd}"
            )
            if control_plane_node != "127.0.0.1":
                msg += f"\non control plane node {control_plane_node}."

            logger.error(f"{msg}")
            return (1, results)

    validate_status = _validate_update(servers, orig_network_interfaces)

    if validate_status == 0:
        logger.info("Validation passed successfully")
    else:
        logger.error("Validation failed. Please check the log.")

    # Check if reboot on ansible server itself is needed and prompt user
    # for reboot.
    ret, _, _ = exec_cmd("needs-restarting -r", logcmd=False)
    if ret:
        reboot_script = "/tmp/reboot.sh"
        hostname = socket.gethostname()
        is_ceph_node = hostname in all_ceph_nodes
        role, vendor = cluster_servers[hostname]
        _create_reboot_scripts(
            reboot_script, is_ceph_node, control_plane_node, role, vendor
        )
        if no_confirm:
            logger.info("Rebooting, with log file /tmp/post_reboot.log ...")
            exec_cmd(reboot_script)
        else:
            logger.info("")
            logger.info("IMPORTANT: The patch requires a reboot on this server.")
            logger.info(
                "Enter 'y' to reboot this server immediately, or 'n' to reboot this server manually later."
            )
            reboot = prompt(msg="Reboot now? (y/n): ")
            if reboot.lower().startswith("y"):
                logger.info("Rebooting, with log file /tmp/post_reboot.log ...")
                exec_cmd(reboot_script)
            else:
                logger.info(
                    f"When ready, please run '{reboot_script}' to reboot this server. Reboot log file is /tmp/post_reboot.log."
                )

    return (0, results)
