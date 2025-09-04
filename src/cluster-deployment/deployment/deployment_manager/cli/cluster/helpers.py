#!/usr/bin/env python
"""
Helper functions for cluster upgrade operations.

This module contains utility functions that support the cluster upgrade CLI commands
but are not CLI commands themselves. This includes functions for credential management,
upgrade/batch lookups, and other shared utilities.
"""

import glob
import json
import logging
import os
from typing import Optional, Tuple
import json
from dataclasses import dataclass

from deployment_manager.cli.cluster_platform.update_servers import update_servers
from deployment_manager.cli.cluster_platform.update_status import UpdateStatus
from deployment_manager.cli.cluster_platform.update_systems import update_systems
from deployment_manager.db import device_props as props
from deployment_manager.db.models import (
    ClusterUpgrade,
    ClusterUpgradeBatch,
    UpgradeBatchStatus,
    UpgradeProcessStatus,
    UpgradeDeviceStatus,
)
from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node
from deployment_manager.db.models import Device

logger = logging.getLogger(__name__)

class ClusterUpgradeWrapper:
    """
    A wrapper class for ClusterUpgrade.
    """
    def __init__(self, upgrade_process: ClusterUpgrade):
        self.upgrade_process = upgrade_process

        db_update_only = os.environ.get("DB_UPDATE_ONLY", False)
        print(f"DB_UPDATE_ONLY: {db_update_only}")
        if db_update_only:
            return

        # Check if upgrade package directory exists
        if not os.path.isdir(self.upgrade_process.upgrade_pkg_path):
            raise RuntimeError(f"Upgrade package directory {self.upgrade_process.upgrade_pkg_path} does not exist")

        matches = glob.glob(f'{self.upgrade_process.upgrade_pkg_path}/cluster-package-*.tar.gz')
        if not matches:
            raise RuntimeError(f"No cluster package found in {self.upgrade_process.upgrade_pkg_path}")
        elif len(matches) > 1:
            raise RuntimeError(f"Multiple cluster packages found in {self.upgrade_process.upgrade_pkg_path}: {matches}")

        self.cluster_pkg_tarball = matches[0]
        self.cluster_pkg_path = self.cluster_pkg_tarball[:-len(".tar.gz")]

        matches = glob.glob(f'{self.upgrade_process.upgrade_pkg_path}/CS1-*.tar.gz')
        if not matches:
            raise RuntimeError(f"No system image package found in {self.upgrade_process.upgrade_pkg_path}")
        elif len(matches) > 1:
            raise RuntimeError(f"Multiple system image package found in {self.upgrade_process.upgrade_pkg_path}: {matches}")
        self.system_img_tarball = matches[0]

        matches = glob.glob(f'{self.upgrade_process.upgrade_pkg_path}/Cerebras-patches-*.tar.gz')
        if not matches:
            raise RuntimeError(f"No security patch package found in {self.upgrade_process.upgrade_pkg_path}")
        elif len(matches) > 1:
            raise RuntimeError(f"Multiple security patch packages found in {self.upgrade_process.upgrade_pkg_path}: {matches}")
        self.security_patch_tarball = matches[0]

    def __repr__(self):
        return str(
            f"ClusterUpgradeWrapper(upgrade_process={self.upgrade_process}, cluster_pkg_tarball={self.cluster_pkg_tarball}, "
            f"cluster_pkg_path={self.cluster_pkg_path}, system_img_tarball={self.system_img_tarball}, security_patch_tarball={self.security_patch_tarball})"
        )


def confirm_quiesce_completed(batch, force=False):
    """
    Confirm that a batch has completed quiescing before proceeding.
    
    Args:
        batch: The ClusterUpgradeBatch to check
        force: If True, skip the check and log a warning
        
    Returns:
        True if batch can proceed (QUIESCE_COMPLETED or force=True), False otherwise
    """
    if not force and batch.status != UpgradeBatchStatus.QUIESCE_COMPLETED.value:
        logger.error(f"Batch {batch.id} is in status '{batch.status}', expected QUIESCE_COMPLETED.")
        logger.info("Use --force to skip prerequisite checks for testing.")
        return False
    elif force:
        logger.warning(f"[FORCE MODE] Skipping batch status check. Batch {batch.id} is in status '{batch.status}'.")
    return True


def fetch_ceph_credentials_to_memory(profile: str, cluster_name: str) -> Tuple[str, str]:
    """
    Fetch Ceph credentials from cluster's deployment node directly to memory.
    
    Args:
        profile: The deployment profile name
        cluster_name: The cluster name to fetch credentials from
        
    Returns:
        A tuple of (ceph_conf_content, keyring_content)
        
    Raises:
        RuntimeError: If credential fetch fails or validation fails
    """
    logger.info(f"Fetching Ceph credentials from cluster {cluster_name} to memory...")

    try:
        # Get the deployment node for this cluster
        dnode = get_k8s_lead_mgmt_node(profile, cluster_name)
        user = dnode.get_prop(props.prop_management_credentials_user)
        password = dnode.get_prop(props.prop_management_credentials_password)

        with KubernetesCtl(profile, dnode.name, user, password) as kctl:
            # Fetch ceph.conf
            logger.debug("Fetching ceph.conf from rook-ceph-tools...")
            ret, ceph_conf_content, err = kctl.run(
                "kubectl exec deploy/rook-ceph-tools -nrook-ceph -- cat /etc/ceph/ceph.conf")
            if ret != 0:
                raise RuntimeError(f"Failed to fetch ceph.conf: {err}")

            # Fetch keyring
            logger.debug("Fetching keyring from rook-ceph-tools...")
            ret, keyring_content, err = kctl.run(
                "kubectl exec deploy/rook-ceph-tools -nrook-ceph -- ceph auth get client.admin")
            if ret != 0:
                raise RuntimeError(f"Failed to fetch keyring: {err}")

            # Basic validation
            if "mon_host" not in ceph_conf_content:
                raise RuntimeError("ceph.conf missing mon_host")
            if "[client.admin]" not in keyring_content:
                raise RuntimeError("keyring corrupt")

            logger.info(f"Successfully fetched Ceph credentials from cluster {cluster_name}")
            return ceph_conf_content, keyring_content

    except Exception as e:
        logger.error(f"Failed to fetch credentials from cluster {cluster_name}: {e}")
        raise RuntimeError(f"Credential fetch failed: {e}")


def get_ongoing_upgrade_ids() -> list[int]:
    """
    Returns a list of IDs for all ongoing (non-terminal) ClusterUpgrade processes.
    """
    terminal_statuses = [
        UpgradeProcessStatus.COMPLETED.value,
        UpgradeProcessStatus.CANCELLED.value,
        UpgradeProcessStatus.FAILED.value,
    ]
    return list(
        ClusterUpgrade.objects.exclude(status__in=terminal_statuses)
                              .values_list("id", flat=True)
    )

def get_implicit_upgrade_id() -> Optional[int]:
    """
    Helper function to find an implicit active upgrade ID.
    
    Finds a unique, active (not completed or failed) ClusterUpgrade process.
    
    Returns:
        The upgrade ID if one is found, otherwise None.
        
    Side effects:
        Logs error messages if no upgrade is found or multiple upgrades exist.
    """
    # Instead of listing all active statuses, exclude terminal states
    terminal_statuses = [
        UpgradeProcessStatus.COMPLETED.value,
        UpgradeProcessStatus.CANCELLED.value,
        UpgradeProcessStatus.FAILED.value,
    ]

    active_upgrades = ClusterUpgrade.objects.exclude(status__in=terminal_statuses)
    count = active_upgrades.count()

    if count == 0:
        logger.error(
            "No active (not COMPLETED or FAILED) upgrade process found. "
            "Please specify --upgrade-id or create a new upgrade process."
        )
        return None
    if count > 1:
        ids = list(active_upgrades.values_list('id', flat=True))
        logger.error(
            f"Multiple active upgrade processes found (IDs: {ids}). "
            "Please specify --upgrade-id to select one."
        )
        return None

    implicit_upgrade = active_upgrades.first()
    logger.info(f"Automatically using active upgrade process with ID {implicit_upgrade.id}.")
    return implicit_upgrade.id


def get_implicit_batch_id() -> Optional[int]:
    """
    Helper function to find an implicit active batch ID.
    
    Finds a unique, active (not completed or failed) ClusterUpgradeBatch.
    
    Returns:
        The batch ID if one is found, otherwise None.
        
    Side effects:
        Logs error messages if no batch is found or multiple batches exist.
    """
    # Instead of listing all active statuses, exclude terminal states
    terminal_statuses = [
        UpgradeBatchStatus.MIGRATE_COMPLETED.value,
        UpgradeBatchStatus.FAILED.value,
    ]

    ongoing_upgrade_ids = get_ongoing_upgrade_ids()
    active_batches = (
        ClusterUpgradeBatch.objects
        .filter(upgrade_process_id__in=ongoing_upgrade_ids)
        .exclude(status__in=terminal_statuses)
    )
    count = active_batches.count()

    if count == 0:
        logger.error(
            "No active (not MIGRATE_COMPLETED or FAILED) batch found. "
            "Please specify --batch-id or create a new batch."
        )
        return None
    if count > 1:
        ids = list(active_batches.values_list('id', flat=True))
        logger.error(
            f"Multiple active batches found (IDs: {ids}). "
            "Please specify --batch-id to select one."
        )
        return None

    implicit_batch = active_batches.first()
    logger.info(f"Automatically using active batch with ID {implicit_batch.id}.")
    return implicit_batch.id


def get_upgrade_process(upgrade_id_arg: Optional[int],
                        select_for_update: bool = True) -> Tuple[Optional[ClusterUpgradeWrapper], int]:
    """
    Helper function to get upgrade process by ID or implicit active upgrade.
    
    Args:
        upgrade_id_arg: Explicit upgrade ID, or None to find implicit upgrade
        select_for_update: Whether to use select_for_update() for locking
        
    Returns:
        A tuple of (upgrade_process, error_code) where error_code is 1 if failed, 0 if success.
    """
    upgrade_id_to_use = upgrade_id_arg
    if upgrade_id_to_use is None:
        upgrade_id_to_use = get_implicit_upgrade_id()
        if upgrade_id_to_use is None:
            return None, 1

    try:
        if select_for_update:
            upgrade_process = ClusterUpgrade.objects.select_for_update().get(id=upgrade_id_to_use)
        else:
            upgrade_process = ClusterUpgrade.objects.get(id=upgrade_id_to_use)
        return ClusterUpgradeWrapper(upgrade_process), 0
    except ClusterUpgrade.DoesNotExist:
        logger.error(f"Upgrade process with ID {upgrade_id_to_use} not found.")
        return None, 1


def get_batch_and_upgrade_process(batch_id_arg: Optional[int], select_for_update: bool = True) -> Tuple[
    Optional[ClusterUpgradeWrapper], Optional[ClusterUpgradeBatch], int]:
    """
    Helper function to get both batch and upgrade process using batch_id or implicit active batch.
    
    Args:
        batch_id_arg: Explicit batch ID, or None to find implicit batch
        select_for_update: Whether to use select_for_update() for locking
        
    Returns:
        A tuple of (upgrade_process, batch, error_code) where error_code is 1 if failed, 0 if success.
    """
    batch_id_to_use = batch_id_arg
    if batch_id_to_use is None:
        batch_id_to_use = get_implicit_batch_id()
        if batch_id_to_use is None:
            return None, None, 1

    try:
        if select_for_update:
            batch = ClusterUpgradeBatch.objects.select_for_update().select_related('upgrade_process').get(id=batch_id_to_use)
        else:
            batch = ClusterUpgradeBatch.objects.select_related('upgrade_process').get(id=batch_id_to_use)
        upgrade_wrapper = ClusterUpgradeWrapper(batch.upgrade_process)
        return upgrade_wrapper, batch, 0
    except ClusterUpgradeBatch.DoesNotExist:
        logger.error(f"Batch with ID {batch_id_to_use} not found.")
        return None, None, 1


def get_manifest_filename(upgrade_pkg_path: str, kctl) -> str:
    """
    Determine which manifest file should be supplied to the upgrade command.
    Behaviour:
    1. If one or more files matching ``manifest.retry.*.json`` exist in the
       *upgrade package* directory, the function returns the most recently
       modified retry-manifest.
    2. A ``manifest.retry.*.json`` is created only when a previous
       ``csadm.sh install <manifest>`` invocation terminates with an error.
    3. After successful runs of ``csadm.sh install``, all ``manifest.retry.*.json``
       files are deleted from the directory to avoid accidentally using an
       outdated or incorrect manifest in future runs.
    4. After unsuccessful runs of ``csadm.sh install``, manifest.retry.*.json are created
       and the function returns the most recently created retry-manifest.
    5. After unsuccessful runs of ``csadm.sh install``, the upgrade_cluster_control_plane_nodes or
       upgrade_cluster_batch_devices function does not delete the retry manifest files. This is to
       preserve the retry manifest files for a retry attempt.
    6. If no retry manifest files are found, the function returns the canonical ``manifest.json``.
    7. If the upgrade package directory does not exist, the function raises an exception
    Args:
        upgrade_pkg_path: Absolute path to the upgrade package directory on the
            remote host.
        kctl: Active ``KubernetesCtl`` connection used to run shell commands on
            the deployment node.
    Returns:
        str: The manifest filename that should be passed to ``csadm.sh install``.
    Raises:
        RuntimeError: If the upgrade package directory does not exist.
    """
    # Check if upgrade package directory exists
    ret, _, stderr = kctl.run(f"test -d {upgrade_pkg_path}", raise_exc=False)
    if ret != 0:
        raise RuntimeError(f"Upgrade package directory {upgrade_pkg_path} does not exist")

    # List manifest.retry.*.json files on remote host
    ret, cmd_stdout, _ = kctl.run(f"ls -t {upgrade_pkg_path}/manifest.retry*.json 2>/dev/null", raise_exc=False)
    if ret != 0 or not cmd_stdout:
        logger.info(f"No retry manifest files in {upgrade_pkg_path}, using default manifest.json")
        return "manifest.json"

    # Get the most recent file (ls -t sorts by modification time, newest first)
    retry_files = [line.strip() for line in cmd_stdout.strip().split('\n') if line.strip()]
    # Get just the filename from the full path
    latest_retry_file = retry_files[0]  # First file is most recent due to ls -t
    filename = os.path.basename(latest_retry_file)
    logger.info(f"Found retry manifest file: {filename}")
    return filename

def upgrade_pb3(
    profile: str,
    cluster_mgmt_node: str,
    user: str,
    password: str,
    operation: str,
    cluster_pkg_path: str,
    force_new_install_version: str = None
)-> None:
    """
    Helper function to run PB3 upgrade in a given cluster.
    Raises an exception if the upgrade fails.
    """
    with KubernetesCtl(profile, cluster_mgmt_node, user, password) as kctl:
        # Run upgrade on management node
        logger.info(f"Running csadm.sh install on management node: {cluster_mgmt_node}")
        # Determine which manifest file to use
        manifest_filename = get_manifest_filename(cluster_pkg_path, kctl)
        target_k8s_version = os.environ.get("K8S_UPGRADE_VERSION", "")

        remove_only = ""
        if operation == "remove":
            remove_only = "--remove-only"
        cmd = f"./csadm.sh install {manifest_filename} --yes --update-config {remove_only}"
        if force_new_install_version:
            # skip update config which will enter into incremental flow check
            cmd = (f"export K8S_INSTALL_VERSION={force_new_install_version}; "
                   f"bash k8s/k8_init.sh teardown_cluster --yes; ./csadm.sh install {manifest_filename} {remove_only} --yes")
        elif target_k8s_version:
            cmd = (f"export K8S_UPGRADE_VERSION={target_k8s_version}; "
                   f"./csadm.sh install {manifest_filename} --yes --update-config {remove_only}")
        try:
            ret, _, stderr = kctl.run(
                f"cd {cluster_pkg_path}; {cmd}"
            )
            if ret != 0:
                return False, f"Failed to upgrade cluster on node {cluster_mgmt_node}: {stderr}"

            # Delete all retry manifest files
            del_ret, _, del_stderr = kctl.run(f"rm -f {cluster_pkg_path}/manifest.retry*.json")
            if del_ret != 0:
                logger.warning(f"Failed to delete retry manifest files in {cluster_pkg_path}: {del_stderr}")
            logger.info(f"csadm.sh install completed successfully on {cluster_mgmt_node}")
            return True, ""
        except Exception as e:
            logger.error(f"csadm.sh install failed on {cluster_mgmt_node}: {str(e)}")
            raise RuntimeError(f"Failed to upgrade cluster on node {cluster_mgmt_node}: {str(e)}")

def upgrade_cluster_control_plane_nodes(
        profile: str,
        cluster_name: str,
        cluster_pkg_path: str,
        security_patch_path: str,
        security_patch_servers: Optional[list[Device]] = None,
        force_new_install_version: str = None) -> Tuple[bool, str]:
    """
    Helper function to upgrade cluster control plane nodes with security patch + pb3.
    
    The function flow:
    1. If security_patch_check_only is False:
       - Gets deployment node for the cluster
       - Returns error if deployment node not found
       - Gets user and password credentials from deployment node
    2. If security_patch_servers list is not empty:
       - Applies security patches to the servers
       - Returns error if any node fails patching
    3. Finally calls upgrade_pb3 with the collected credentials
    
    Returns (success, error_message) where success is True if the upgrade succeeded.
    """
    db_only = os.environ.get("DB_ONLY", False)
    logger.info(f"db_only: {db_only}")
    dnode = None
    user = None
    password = None
    if not db_only:
        logger.info(f"Getting deployment node for cluster {cluster_name}")
        dnode = get_k8s_lead_mgmt_node(profile, cluster_name=cluster_name)
        if dnode is None:
            return False, f"No deployment node found in profile '{profile}'."

        user = dnode.get_prop(props.prop_management_credentials_user)
        password = dnode.get_prop(props.prop_management_credentials_password)

    if security_patch_servers:
        security_patch_server_names = [s.name for s in security_patch_servers]
        _, failed_nodes, unreachable_nodes, unknown_nodes = _security_patch_servers(
            profile, dnode if force_new_install_version == "" else None,
            security_patch_server_names, security_patch_path
        )
        failed_device_names = failed_nodes + unreachable_nodes + unknown_nodes

        # Command fails if ANY nodes failed
        if len(failed_device_names) > 0:
            return False, (f"Security patching failed for {len(failed_device_names)} nodes: "
                           f"{', '.join(failed_device_names)}")
        logger.info(f"Security patching of nodes completed")

    return upgrade_pb3(profile, dnode.name, user, password, "", cluster_pkg_path, force_new_install_version)

def _security_patch_servers(profile, dnode, node_names, patch_dir) -> Tuple[list[str], list[str], list[str], list[str]]:
    """
    Updates server nodes via security patch and processes the results, logging status and returning categorized lists.

    Args:
        profile (str): The deployment profile.
        dnode: Deployment node object.
        node_names (list): List of node names to patch.
        control_plane_only (bool): Whether to patch only control plane nodes.

    Returns:
        tuple: (successful_nodes, failed_nodes, unreachable_nodes, unknown_nodes) Lists of node names by status
    """
    ret, node_status = update_servers(
        profile=profile,
        control_plane_node=dnode.name if dnode else "",
        servers=node_names,
        patch_dir=patch_dir,
        no_confirm=True,
        force=False,
        verbose=True,
    )

    if ret != 0:
        logger.warning(f"update_servers returned non-zero code: {ret}")

    successful_nodes = []
    failed_nodes = []
    unreachable_nodes = []
    unknown_nodes = []

    for node_name in node_names:
        if node_name in node_status:
            status = node_status[node_name]
            if status == UpdateStatus.OK:
                successful_nodes.append(node_name)
            elif status == UpdateStatus.FAILED:
                failed_nodes.append(node_name)
            elif status == UpdateStatus.UNREACHABLE:
                unreachable_nodes.append(node_name)
        else:
            # Device not in results - assume failure
            unknown_nodes.append(node_name)

    # Log structured results
    logger.info(f"Security patching results for cluster:")
    logger.info(f"  ✓ Successful nodes ({len(successful_nodes)}): "
                f"{', '.join(successful_nodes) if successful_nodes else 'None'}")

    if failed_nodes:
        logger.warning(f"  ✗ Failed nodes ({len(failed_nodes)}): {', '.join(failed_nodes)}")
    if unreachable_nodes:
        logger.warning(f"  ⚠ Unreachable nodes ({len(unreachable_nodes)}): {', '.join(unreachable_nodes)}")
    if unknown_nodes:
        logger.error(f"  ? Unknown status nodes ({len(unknown_nodes)}): {', '.join(unknown_nodes)}")

    return successful_nodes, failed_nodes, unreachable_nodes, unknown_nodes


def upgrade_systems(systems, image_path) -> tuple[bool, list[str]]:
    """
    Helper function to patch/upgrade systems in a batch using update_systems.
    Updates the ClusterUpgradeBatchDevice status and logs results.
    Returns (success, failed_device_names) where success is True if any systems succeeded.
    """
    system_devices = [s.device for s in systems]
    ret, system_status = update_systems(
        image=image_path,
        systems=system_devices,
        no_confirm=True,
        force_update=False,
        skip_activate=False,
        username="",
        password="",
    )

    successful_systems = []
    failed_systems = []
    unreachable_systems = []
    unknown_systems = []

    for batch_device in systems:
        device_name = batch_device.device.name
        if device_name in system_status:
            status = system_status[device_name]
            if status == UpdateStatus.OK:
                batch_device.current_step = "security_patched_system"
                batch_device.status = UpgradeDeviceStatus.IN_PROGRESS.value
                batch_device.message = ""
                successful_systems.append(device_name)
            elif status == UpdateStatus.FAILED:
                batch_device.current_step = "system_patch_failed"
                batch_device.status = UpgradeDeviceStatus.FAILED.value
                batch_device.message = f"System patching failed for {device_name}"
                failed_systems.append(device_name)
            elif status == UpdateStatus.UNREACHABLE:
                batch_device.current_step = "system_patch_unreachable"
                batch_device.status = UpgradeDeviceStatus.FAILED.value
                batch_device.message = f"System {device_name} was unreachable during patching"
                unreachable_systems.append(device_name)
        else:
            batch_device.current_step = "system_patch_unknown_error"
            batch_device.status = UpgradeDeviceStatus.FAILED.value
            batch_device.message = f"No status returned for system {device_name}"
            unknown_systems.append(device_name)
        batch_device.save()

    logger.info(f"  ✓ Successful systems ({len(successful_systems)}): "
                f"{', '.join(successful_systems) if successful_systems else 'None'}")
    if failed_systems:
        logger.warning(f"  ✗ Failed systems ({len(failed_systems)}): {', '.join(failed_systems)}")
    if unreachable_systems:
        logger.warning(f"  ⚠ Unreachable systems ({len(unreachable_systems)}): {', '.join(unreachable_systems)}")
    if unknown_systems:
        logger.error(f"  ? Unknown status systems ({len(unknown_systems)}): {', '.join(unknown_systems)}")

    failed_device_names = failed_systems + unreachable_systems + unknown_systems
    return len(successful_systems) > 0, failed_device_names


def update_security_patch_status(nodes, successful_nodes, failed_nodes, unreachable_nodes, unknown_nodes):
    for batch_device in nodes:
        device_name = batch_device.device.name
        if device_name in successful_nodes:
            batch_device.current_step = "security_patched_node"
            batch_device.status = UpgradeDeviceStatus.IN_PROGRESS.value
            batch_device.message = ""
            successful_nodes.append(device_name)
        elif device_name in failed_nodes:
            batch_device.current_step = "security_patch_failed"
            batch_device.status = UpgradeDeviceStatus.FAILED.value
            batch_device.message = f"Security patching failed for node {device_name}"
            failed_nodes.append(device_name)
        elif device_name in unreachable_nodes:
            batch_device.current_step = "security_patch_unreachable"
            batch_device.status = UpgradeDeviceStatus.FAILED.value
            batch_device.message = f"Node {device_name} was unreachable during security patching"
            unreachable_nodes.append(device_name)
        else:
            # Device not in results - assume failure
            batch_device.current_step = "security_patch_unknown_error"
            batch_device.status = UpgradeDeviceStatus.FAILED.value
            batch_device.message = f"No status returned for node {device_name}"
            unknown_nodes.append(device_name)
        batch_device.save()


def save_cluster_node_and_system_count(profile: str, cluster_name: str,
                                       node_count: int, system_count: int) -> Tuple[bool, Optional[str]]:
    """
    Write the node count to pkg-properties.yaml
    
    Args:
        profile: The deployment profile name
        cluster_name: The cluster name to write the node count to
        node_count: Number of nodes to write to the file
        system_count: Number of systems to write to the file
        
    Returns:
        A tuple of (success, error_message) where success is True if the operation succeeded.
    """
    if node_count < 1:
        return False, "Node count must be at least 1"

    try:
        # Get the deployment node for this cluster
        dnode = get_k8s_lead_mgmt_node(profile, cluster_name)
        if dnode is None:
            return False, f"No deployment node found in profile '{profile}' for cluster '{cluster_name}'"

        user = dnode.get_prop(props.prop_management_credentials_user)
        password = dnode.get_prop(props.prop_management_credentials_password)

        with KubernetesCtl(profile, dnode.name, user, password) as kctl:
            properties_file = "/opt/cerebras/cluster/pkg-properties.yaml"

            # Check if properties file exists, if not create basic structure
            ret, _, stderr = kctl.run(f"touch {properties_file}", raise_exc=False)
            if ret != 0:
                return False, f"Failed to create properties file: {stderr}"

            # Update the nodeCount property using yq
            if node_count > 0:
                ret, _, stderr = kctl.run(f"yq eval '.properties.nodeCount = {node_count}' -i {properties_file}")
                if ret != 0:
                    return False, f"Failed to update nodeCount in properties file: {stderr}"

            # Update the systemCount property using yq
            if system_count > 0:
                ret, _, stderr = kctl.run(f"yq eval '.properties.systemCount = {system_count}' -i {properties_file}")
                if ret != 0:
                    return False, f"Failed to update systemCount in properties file: {stderr}"

            logger.info(f"Successfully wrote node count {node_count} and system count {system_count} "
                        f"for cluster {cluster_name} on {dnode.name}")
            return True, None

    except Exception as e:
        logger.error(f"Failed to set node count on cluster {cluster_name}: {e}")
        return False, f"Exception occurred: {e}"


def clear_cluster_node_and_system_count(profile: str, cluster_name: str) -> Tuple[bool, Optional[str]]:
    """
    Clear the node count and system count from pkg-properties.yaml
    
    Args:
        profile: The deployment profile name
        cluster_name: The cluster name to delete the node count file from
        
    Returns:
        A tuple of (success, error_message) where success is True if the operation succeeded.
    """
    try:
        # Get the deployment node for this cluster
        dnode = get_k8s_lead_mgmt_node(profile, cluster_name)
        if dnode is None:
            return False, f"No deployment node found in profile '{profile}' for cluster '{cluster_name}'"

        user = dnode.get_prop(props.prop_management_credentials_user)
        password = dnode.get_prop(props.prop_management_credentials_password)

        with KubernetesCtl(profile, dnode.name, user, password) as kctl:
            properties_file = "/opt/cerebras/cluster/pkg-properties.yaml"
            # Check if properties file exists, if not create basic structure
            ret, _, _ = kctl.run(f"test -f {properties_file}")
            if ret != 0:
                logger.warning(f"Properties file {properties_file} does not exist on cluster {cluster_name}")
                return True, None

            # Remove the nodeCount property using yq
            ret, _, stderr = kctl.run(f"yq eval 'del(.properties.nodeCount)' -i {properties_file}")
            if ret != 0:
                return False, f"Failed to clear nodeCount in properties file: {stderr}"

            # Remove the systemCount property using yq
            ret, _, stderr = kctl.run(f"yq eval 'del(.properties.systemCount)' -i {properties_file}")
            if ret != 0:
                return False, f"Failed to clear systemCount in properties file: {stderr}"

            logger.info(f"Successfully cleared node count and system count from cluster {cluster_name}")
            return True, None

    except Exception as e:
        logger.error(f"Failed to clear node count from cluster {cluster_name}: {e}")
        return False, f"Exception occurred: {e}"

@dataclass
class UpgradeCreds:
    """Class to handle deployment node credentials for source and destination clusters."""
    
    profile: object
    upgrade_process: object
    skip_src_cluster: bool = False
    skip_dest_cluster: bool = False
    
    def __post_init__(self):
        """Initialize credentials after dataclass instantiation."""
        if not self.skip_src_cluster:
            self.dnode_source_cluster = get_k8s_lead_mgmt_node(self.profile, self.upgrade_process.source_cluster.name)
            self.user_source_cluster = self.dnode_source_cluster.get_prop(props.prop_management_credentials_user)
            self.password_source_cluster = self.dnode_source_cluster.get_prop(props.prop_management_credentials_password)
        if not self.skip_dest_cluster:
            self.dnode_dest_cluster = get_k8s_lead_mgmt_node(self.profile, self.upgrade_process.dest_cluster.name)
            self.user_dest_cluster = self.dnode_dest_cluster.get_prop(props.prop_management_credentials_user)
            self.password_dest_cluster = self.dnode_dest_cluster.get_prop(props.prop_management_credentials_password)

def get_ceph_nodes(profile: str, cluster_name: str) -> list[str]:
    """
    Get the list of Ceph nodes in the cluster.

    Args:
        profile: The deployment profile name
        cluster_name: The cluster name to fetch Ceph nodes from

    Returns:
        A list of Ceph node names.

    Raises:
        RuntimeError: if fetching fails.
    """
    dnode = get_k8s_lead_mgmt_node(profile, cluster_name)
    if dnode is None:
        raise RuntimeError(f"No deployment node found in profile '{profile}' for cluster '{cluster_name}'")

    user = dnode.get_prop(props.prop_management_credentials_user)
    password = dnode.get_prop(props.prop_management_credentials_password)

    with KubernetesCtl(profile, dnode.name, user, password) as kctl:
        try:
            _, out, _ = kctl.run(f"kubectl get nodes -lstorage-type=ceph -ojson")
            data = json.loads(out)
            ceph_nodes = [
                item["metadata"]["name"]
                for item in data.get("items", [])
            ]
        except Exception as e:
            logger.error(f"Failed to get ceph nodes from mgmt node {dnode.name} in cluster {cluster_name}: {e}")
            raise RuntimeError(f"Failed to fetch Ceph nodes from mgmt node {dnode.name} in cluster {cluster_name}: {e}")
        return ceph_nodes


def remove_thanos_config(profile, upgrade_process, properties_path, logger, skip_src_cluster=False, skip_dest_cluster=False):
    """
    Remove a Thanos query GRPC service in the source cluster + remove extra store in dest cluster.

    Args:
        profile: The deployment profile name
        upgrade_process: The ClusterUpgrade model instance
        properties_path: Path to the properties YAML file
        logger: Logger instance
        skip_src_cluster: Whether to skip source cluster operations
        skip_dest_cluster: Whether to skip dest cluster operations
    """

    if not skip_src_cluster:
        # Delete the Thanos query GRPC service from source cluster
        source_cluster = upgrade_process.source_cluster
        source_node = get_k8s_lead_mgmt_node(profile, source_cluster.name)
        source_user = source_node.get_prop(props.prop_management_credentials_user)
        source_password = source_node.get_prop(props.prop_management_credentials_password)
        with KubernetesCtl(profile, source_node.name, source_user, source_password) as source_kctl:
            logger.info("Removing Thanos query GRPC service from source cluster...")
            try:
                source_kctl.run("kubectl delete service thanos-query-grpc-external -n prometheus --ignore-not-found")
                logger.info("Successfully deleted Thanos query GRPC service")
            except Exception as e:
                logger.warning(f"Failed to delete Thanos query GRPC service: {e}")

    # Remove Thanos extraStore property from destination cluster
    if not skip_dest_cluster:
        dest_cluster = upgrade_process.dest_cluster
        dest_node = get_k8s_lead_mgmt_node(profile, dest_cluster.name)
        dest_user = dest_node.get_prop(props.prop_management_credentials_user)
        dest_password = dest_node.get_prop(props.prop_management_credentials_password)
        with KubernetesCtl(profile, dest_node.name, dest_user, dest_password) as kctl:
            logger.info(f"Removing Thanos extraStore property from {properties_path}")
            _, extra_store_output, _ = kctl.run(
                "yq '.properties.prometheus.thanos.extraStore // \"\"' " + properties_path
            )
            extra_store = extra_store_output.strip().replace('"', '')
            if not extra_store:
                return

            kctl.run(f"yq -i 'del(.properties.prometheus.thanos.extraStore)' {properties_path}")
            logger.info(f"Successfully removed Thanos extraStore property from {properties_path}")
            _, output, _ = kctl.run("kubectl -n prometheus get deployment thanos-query -o json")
            deployment = json.loads(output)
            # Find the container with thanos
            for container in deployment["spec"]["template"]["spec"]["containers"]:
                if container["name"] == "thanos-query":
                    args = container["args"]
                    endpoint_arg_index = None
                    for i, arg in enumerate(args):
                        if arg.startswith("--endpoint=") and extra_store in arg:
                            endpoint_arg_index = i
                            break
                    if endpoint_arg_index is not None:
                        # Create a patch to remove this specific argument by index
                        patch = f'[{{"op": "remove", "path": "/spec/template/spec/containers/0/args/{endpoint_arg_index}"}}]'
                        kctl.run(
                            f'kubectl -n prometheus patch deployment thanos-query --type=json -p=\'{patch}\''
                        )
                        logger.info(
                            f"Removed store endpoint for source cluster from thanos-query deployment"
                        )
                    else:
                        logger.info(
                            "No source cluster endpoint found in thanos-query args, no changes needed"
                        )

