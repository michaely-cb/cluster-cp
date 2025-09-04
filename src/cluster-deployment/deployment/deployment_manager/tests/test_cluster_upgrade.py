import pytest

from deployment_manager.db.models import UpgradeBatchStatus
from deployment_manager.cli.cluster_platform.update_status import UpdateStatus
from .conftest import SQLITE_CMD
from deployment_manager.cli.cluster.helpers import upgrade_cluster_control_plane_nodes, upgrade_systems

def set_device_status(mock_cluster_fixture, device_id, status=UpdateStatus.FAILED):
    """Helper to set a device's status in db_clusterupgradebatchdevice table"""
    
    if device_id is None or not isinstance(device_id, int):
        raise ValueError("device_id must be provided and must be an integer for set_device_status")
    
    if isinstance(status, UpdateStatus):
        status_str = status.name
    else:
        raise ValueError(f"status must be a string or UpdateStatus enum, got {type(status)}")
    mock_cluster_fixture.must_exec_root(
        f"{SQLITE_CMD} \"UPDATE db_clusterupgradebatchdevice SET status = '{status_str}' WHERE id = {device_id};\"")

def set_batch_status(mock_cluster_fixture, batch_id, status):
    """Helper to set a batch's status in db_clusterupgradebatch table"""
    if batch_id is None or not isinstance(batch_id, int):
        raise ValueError("batch_id must be provided and must be an integer")
    mock_cluster_fixture.must_exec_root(
        f"{SQLITE_CMD} \"UPDATE db_clusterupgradebatch SET status = '{status}' WHERE id = {batch_id};\"")

def test_cluster_upgrade_help(mock_cluster_fixture):
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade --help")
    assert result.returncode in [0, 1]
    assert "usage: cluster upgrade" in result.stdout
    assert "create" in result.stdout
    assert "upgrade_dest_cluster" in result.stdout


def test_cluster_upgrade_batch_create_help(mock_cluster_fixture):
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --help")
    assert "--upgrade-id" in result.stdout
    assert "--session" in result.stdout
    assert "--device-list" in result.stdout


def test_cluster_upgrade_create_missing_args(mock_cluster_fixture):
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade create --source blue")
    assert result.returncode != 0
    assert "the following arguments are required: --dest, --upgrade-pkg-path" in result.stderr


def test_cluster_upgrade_batch_create_missing_args(mock_cluster_fixture):
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --upgrade-id 1", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode != 0
    assert "one of the arguments --device-list --device-list-file --session is required" in result.stderr


def test_cluster_upgrade_create_happy_path(mock_cluster_fixture):
    cluster_create_cmds = [
        "cluster add blue-cluster -d blue.example.com -m blue.example.com",
        "cluster add green-cluster -d green.example.com -m green.example.com",
        "device add blue_cluster-control-plane-1 SR MG -p kubernetes.controlplane=true -p management_credentials.password=root",
        "device add green_cluster-control-plane-1 SR MG -p kubernetes.controlplane=true -p management_credentials.password=root",
        "cluster device edit --cluster blue-cluster -f name=blue_cluster-control-plane-1 --controlplane=true -y",
        "cluster device edit --cluster green-cluster -f name=green_cluster-control-plane-1 --controlplane=true -y",
    ]
    for cmd in cluster_create_cmds:
        result = mock_cluster_fixture.exec_cscfg(cmd)
        assert result.returncode == 0, \
            f"Failed to execute command: {cmd}, stdout: {result.stdout}, stderr: {result.stderr}"

    # Start the upgrade
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert "Executing ClusterUpgradeCreateCmd with" in result.stdout
    assert "Successfully created new upgrade" in result.stdout
    assert result.returncode == 0

    # View upgrade status
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show")
    assert "blue-cluster" in result.stdout
    assert "green-cluster" in result.stdout
    assert "NOT_STARTED" in result.stdout

    # Upgrade Destination cluster
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show")
    assert "DEST_CLUSTER_UPGRADED" in result.stdout

    # Prepare Data for migration
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0, f"Failed to run prepare_data: {result.stdout}"
    assert "Successfully completed data preparation for upgrade process" in result.stdout

    # Check status - should still show DEST_CLUSTER_UPGRADED since data prep doesn't change overall status
    # Also verify that data preparation status is COMPLETED
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show -ojson")
    assert result.returncode == 0
    import json
    upgrade_data = json.loads(result.stdout)
    assert len(upgrade_data) == 1
    upgrade = upgrade_data[0]
    assert upgrade["status"] == "DEST_CLUSTER_UPGRADED"
    assert upgrade["data_preparation_status"] == "COMPLETED"
    
    # Run prepare_data again to test that it can be run multiple times
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0, f"Failed to run prepare_data: {result.stdout}"
    assert "Successfully completed data preparation for upgrade process" in result.stdout
    
    # Verify status is still COMPLETED
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show -ojson")
    assert result.returncode == 0
    upgrade_data = json.loads(result.stdout)
    assert len(upgrade_data) == 1
    upgrade = upgrade_data[0]
    assert upgrade["data_preparation_status"] == "COMPLETED"

    # Adding devices to the cluster
    result = mock_cluster_fixture.exec_cscfg("device add foo SR IX")
    assert "New device added successfully" in result.stdout
    result = mock_cluster_fixture.exec_cscfg("device add bar SY CS")
    assert "New device added successfully" in result.stdout
    result = mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=foo")
    assert result.returncode == 0, f"Failed to edit device: {result.stdout}"
    result = mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=bar")
    assert result.returncode == 0, f"Failed to edit device: {result.stdout}"

    # Create a batch
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade batch create --device-list foo,bar", environment=["DB_UPDATE_ONLY=true"]
    )
    assert "Successfully created new batch 1" in result.stdout

    # Taint + Quiesce + Update nodes
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch taint", environment=["DB_UPDATE_ONLY=true"])
    assert "Tainting 2 devices in batch 1" in result.stdout
    assert "Adding server node foo to the batch to taint." in result.stdout
    assert "System node bar to be tainted (DB update only):" in result.stdout
    assert "Successfully completed tainting for batch 1" in result.stdout

    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show", environment=["DB_UPDATE_ONLY=true"])
    assert "TAINT_COMPLETED" in result.stdout

    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch quiesce -y", environment=["DB_UPDATE_ONLY=true"])
    assert "Quiescing workloads on 2 devices in batch 1" in result.stdout
    assert "Successfully completed quiescing for batch 1" in result.stdout

    # Temporary disable security patch mock test -stevenl
    # result = mock_cluster_fixture.exec_cscfg(
    #     "cluster upgrade batch security_patch_nodes"
    # )
    # assert "Applying security patches to 2 server nodes" in result.stdout
    # assert "Successfully completed security patching for 2 nodes" in result.stdout

    # Migrate the batch
    result = mock_cluster_fixture.exec_cscfg("cluster device show --cluster blue-cluster")
    assert "bar" in result.stdout
    assert "foo" in result.stdout
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch migrate -y", environment=["DB_UPDATE_ONLY=true"])
    assert "Batch 1 migrated successfully" in result.stdout
    result = mock_cluster_fixture.exec_cscfg("cluster device show --cluster green-cluster")
    assert "bar" in result.stdout
    assert "foo" in result.stdout

    # Upgrade Source cluster
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_src_cluster", environment=["DB_UPDATE_ONLY=true"])
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show")
    assert "SOURCE_CLUSTER_UPGRADED" in result.stdout

    # End the upgrade
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade end_upgrade", environment=["DB_UPDATE_ONLY=true"])
    assert "Upgrade process with ID 1 completed successfully." in result.stdout
    # Verify data preparation status is displayed
    assert "Data preparation status for upgrade process 1:" in result.stdout
    # Verify no warning is shown since data preparation was completed
    assert "WARNING: Data preparation has never been completed" not in result.stdout
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show")
    assert "COMPLETED" in result.stdout


def test_cluster_upgrade_end_warning_without_data_prep(mock_cluster_fixture):
    """Test that end_upgrade shows warning when data preparation was never completed"""
    cluster_create_cmds = [
        "cluster add blue-cluster -d blue.example.com -m blue.example.com",
        "cluster add green-cluster -d green.example.com -m green.example.com",
        "device add blue_cluster-control-plane-1 SR MG -p kubernetes.controlplane=true -p management_credentials.password=root",
        "device add green_cluster-control-plane-1 SR MG -p kubernetes.controlplane=true -p management_credentials.password=root",
        "cluster device edit --cluster blue-cluster -f name=blue_cluster-control-plane-1 --controlplane=true -y",
        "cluster device edit --cluster green-cluster -f name=green_cluster-control-plane-1 --controlplane=true -y",
    ]
    for cmd in cluster_create_cmds:
        result = mock_cluster_fixture.exec_cscfg(cmd)
        assert result.returncode == 0

    # Start the upgrade

    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    
    # Upgrade Destination cluster (but skip data preparation)
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    assert "Successfully completed destination cluster upgrade" in result.stdout

    # Add devices and create a batch
    result = mock_cluster_fixture.exec_cscfg("device add foo SR IX")
    assert result.returncode == 0
    result = mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=foo")
    assert result.returncode == 0

    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list foo --force",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0

    # Complete the batch migration workflow
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch taint", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch quiesce", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch migrate -y", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0

    # Upgrade Source cluster
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_src_cluster", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0

    # End the upgrade - should show warning since we never ran prepare_data
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade end_upgrade", environment=["DB_UPDATE_ONLY=true"])
    assert "Upgrade process with ID" in result.stdout and "completed successfully." in result.stdout
    # Verify data preparation status shows NOT_STARTED
    assert "Data preparation status for upgrade process" in result.stdout
    # Verify warning is shown
    assert "WARNING: Data preparation has never been completed successfully for this upgrade!" in result.stdout
    assert "This means no data synchronization (CephFS volumes, registry images, K8s state) has been performed." in result.stdout
    assert "Consider running 'cluster upgrade prepare_data' before completing the upgrade." in result.stdout

# --- Tests for ClusterUpgradeShowCmd ---

def test_cluster_upgrade_show_help(mock_cluster_fixture):
    """Test the help output for cluster upgrade show command."""
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --help")
    assert "--upgrade-id" in result.stdout
    assert "--devices" in result.stdout
    assert "--error-only" in result.stdout
    assert "Show all devices across all batches" in result.stdout
    assert "Show only failed devices" in result.stdout


def test_cluster_upgrade_show_not_found(mock_cluster_fixture):
    """Test showing upgrades when none exist."""
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show")
    assert result.returncode == 0
    assert "No upgrade processes found" in result.stdout


def test_cluster_upgrade_show_happy_case(mock_cluster_fixture):
    """Test showing upgrades with and without --upgrade-id when an upgrade exists."""
    # Create clusters and upgrade
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    
    # Create upgrade
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    
    # Test showing all upgrades (without --upgrade-id)
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show")
    assert result.returncode == 0
    assert "blue-cluster" in result.stdout
    assert "green-cluster" in result.stdout
    assert "NOT_STARTED" in result.stdout
    assert "1" in result.stdout  # Upgrade ID should be shown
    
    # Test showing specific upgrade (with --upgrade-id)
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --upgrade-id 1")
    assert result.returncode == 0
    assert "blue-cluster" in result.stdout
    assert "green-cluster" in result.stdout
    assert "NOT_STARTED" in result.stdout
    assert "1" in result.stdout  # Upgrade ID should be shown


def test_cluster_upgrade_show_specific_upgrade_not_found(mock_cluster_fixture):
    """Test showing a specific upgrade that doesn't exist."""
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --upgrade-id 999")
    assert result.returncode == 1
    assert "Upgrade process with ID 999 not found" in result.stderr

def test_cluster_upgrade_show_error_only_without_devices_no_upgrade_id(mock_cluster_fixture):
    """Test that --error-only without --devices and without --upgrade-id errors out."""
    # Create clusters and upgrade
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    # Try --error-only without --devices
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --error-only")
    assert result.returncode == 1
    assert "--error-only must be used with --devices" in result.stderr


def test_cluster_upgrade_show_error_only_without_devices_with_upgrade_id(mock_cluster_fixture):
    """Test that --error-only without --devices but with --upgrade-id errors out."""
    # Create clusters and upgrade
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    # Try --error-only without --devices but with --upgrade-id
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --upgrade-id 1 --error-only")
    assert result.returncode == 1
    assert "--error-only must be used with --devices" in result.stderr

def test_cluster_upgrade_show_devices_without_upgrade_id(mock_cluster_fixture):
    """Test that --devices works without --upgrade-id (uses implicit active upgrade)."""
    # Create clusters and upgrade
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    # Add devices and create a batch
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list device1", environment=["DB_UPDATE_ONLY=true"])
    # Show devices without --upgrade-id
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --devices", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "=== Device Upgrade Status Table ===" in result.stdout
    assert "device1" in result.stdout
    assert "PENDING" in result.stdout


def test_cluster_upgrade_show_error_only_without_upgrade_id(mock_cluster_fixture):
    """Test that --devices --error-only works without --upgrade-id (uses implicit active upgrade)."""
    # Create clusters and upgrade
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    # Add devices and create a batch
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list device1", environment=["DB_UPDATE_ONLY=true"])
    # Mark device as failed
    set_device_status(mock_cluster_fixture, device_id=1, status=UpdateStatus.FAILED)
    # Show failed devices without --upgrade-id
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --devices --error-only", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "=== Device Upgrade Status Table ===" in result.stdout
    assert "device1" in result.stdout
    assert "FAILED" in result.stdout


def test_cluster_upgrade_show_devices_not_found(mock_cluster_fixture):
    """Test showing devices when no batches have been created yet."""
    # Create clusters and upgrade
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    
    # Create upgrade
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    
    # Add devices to source cluster
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("device add device2 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device2")
    
    # Show devices before any batches are created
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --upgrade-id 1 --devices", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "=== Device Upgrade Status Table ===" in result.stdout
    assert "No devices found for upgrade process 1" in result.stdout
    assert "device1" not in result.stdout
    assert "device2" not in result.stdout

def test_cluster_upgrade_show_error_only_not_found(mock_cluster_fixture):
    """Test showing failed devices when no batches have been created yet."""
    # Create clusters and upgrade
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    
    # Create upgrade
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    
    # Add devices to source cluster
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("device add device2 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device2")
    
    # Show devices before any batches are created
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --upgrade-id 1 --devices --error-only", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "=== Device Upgrade Status Table ===" in result.stdout
    assert "No failed devices found for upgrade process 1" in result.stdout
    assert "device1" not in result.stdout
    assert "device2" not in result.stdout

def test_cluster_upgrade_show_devices_with_upgrade_id_happy_case(mock_cluster_fixture):
    """Test that --devices works without --upgrade-id (uses implicit active upgrade)."""
    # Create clusters and upgrade
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    # Add devices and create a batch
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list device1", environment=["DB_UPDATE_ONLY=true"])
    # Show devices without --upgrade-id
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --upgrade-id 1 --devices", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "=== Device Upgrade Status Table ===" in result.stdout
    assert "device1" in result.stdout
    assert "PENDING" in result.stdout


def test_cluster_upgrade_show_error_only_with_upgrade_id_happy_case(mock_cluster_fixture):
    """Test that --devices --error-only works without --upgrade-id (uses implicit active upgrade)."""
    # Create clusters and upgrade
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    # Add devices and create a batch
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list device1", environment=["DB_UPDATE_ONLY=true"])
    # Mark device as failed
    set_device_status(mock_cluster_fixture, device_id=1, status=UpdateStatus.FAILED)
    # Show failed devices without --upgrade-id
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade show --upgrade-id 1 --devices --error-only", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "=== Device Upgrade Status Table ===" in result.stdout
    assert "device1" in result.stdout
    assert "FAILED" in result.stdout

# --- Tests for ClusterUpgradeBatchShowCmd ---

def test_cluster_upgrade_batch_show_help(mock_cluster_fixture):
    """Test the help output for cluster upgrade batch show command."""
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --help")
    assert "--upgrade-id" in result.stdout
    assert "--batch-id" in result.stdout
    assert "--devices" in result.stdout
    assert "--error-only" in result.stdout
    assert "Show all devices in the batch" in result.stdout
    assert "Show only failed devices" in result.stdout


def test_cluster_upgrade_batch_show_not_found(mock_cluster_fixture):
    """Test showing batches when none exist."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --upgrade-id 1", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "No batches found for upgrade process 1" in result.stdout


def test_cluster_upgrade_batch_show_specific_batch_not_found(mock_cluster_fixture):
    """Test showing a specific batch that doesn't exist."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 999", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 1
    assert "Batch with ID 999 not found" in result.stderr


def test_cluster_upgrade_batch_show_batches_only(mock_cluster_fixture):
    """Test showing batches without device flags."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    mock_cluster_fixture.exec_cscfg("device add device1 SR IX")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list device1", environment=["DB_UPDATE_ONLY=true"])
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --upgrade-id 1", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "BatchID" in result.stdout
    assert "UpgradeID" in result.stdout
    assert "Status" in result.stdout
    assert "1" in result.stdout


def test_cluster_upgrade_batch_show_devices_happy_case(mock_cluster_fixture):
    """Test showing devices when batch has devices."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("device add device2 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device2")
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list device1,device2", environment=["DB_UPDATE_ONLY=true"])
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1 --devices",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "=== Batch Device Status Table ===" in result.stdout
    assert "device1" in result.stdout
    assert "device2" in result.stdout
    assert "PENDING" in result.stdout
    assert "Device Name" in result.stdout
    assert "Type" in result.stdout
    assert "Role" in result.stdout
    assert "Status" in result.stdout


def test_cluster_upgrade_batch_show_error_only_happy_case(mock_cluster_fixture):
    """Test showing only failed devices when some devices have failed."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("device add device2 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device2")
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list device1,device2", environment=["DB_UPDATE_ONLY=true"])
    # Mark device1 as failed
    set_device_status(mock_cluster_fixture, device_id=1, status=UpdateStatus.FAILED)
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1 --devices --error-only", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "=== Batch Device Status Table ===" in result.stdout
    assert "device1" in result.stdout
    assert "FAILED" in result.stdout
    assert "device2" not in result.stdout


def test_cluster_upgrade_batch_show_error_only_not_found(mock_cluster_fixture):
    """Test showing failed devices when no devices have failed."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list device1", environment=["DB_UPDATE_ONLY=true"])
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1 --devices --error-only",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "=== Batch Device Status Table ===" in result.stdout
    assert "No failed devices found in batch 1" in result.stdout


def test_cluster_upgrade_batch_show_error_only_without_devices(mock_cluster_fixture):
    """Test that --error-only without --devices errors out."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list device1", environment=["DB_UPDATE_ONLY=true"])
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1 --error-only",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 1
    assert "--error-only must be used with --devices" in result.stderr


def test_cluster_upgrade_batch_show_error_only_without_batch_id(mock_cluster_fixture):
    """Test --devices and --error-only without batch-id (should use active batch)."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    mock_cluster_fixture.exec_cscfg("device add device1 SR MG")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=device1")
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list device1", environment=["DB_UPDATE_ONLY=true"])
    # Should use active batch
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --devices --error-only",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "=== Batch Device Status Table ===" in result.stdout
    assert "device1" in result.stdout or "No failed devices found" in result.stdout

# --- Tests for ClusterUpgradeBatchAddDeviceCmd and ClusterUpgradeBatchRemoveDeviceCmd ---

def test_cluster_upgrade_batch_add_and_remove_device(mock_cluster_fixture):
    """Test adding and then removing devices from a batch."""
    # Setup: Create clusters, upgrade process, devices, and a batch
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    mock_cluster_fixture.exec_cscfg("device add foo SR IX")
    mock_cluster_fixture.exec_cscfg("device add bar SR IX")
    mock_cluster_fixture.exec_cscfg("device add baz SR IX")
    mock_cluster_fixture.exec_cscfg("device add qux SR IX")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=foo")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=bar")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=baz")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=qux")

    # 0. Create a batch with device 'bar'
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list bar --force", environment=["DB_UPDATE_ONLY=true"])

    # 1. Add 1 device to the batch
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch add_device --batch-id 1 --device-list foo",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "Successfully added 1 devices to batch 1" in result.stdout
    assert "foo" in result.stdout

    # Verify both devices are in the batch
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1 --devices",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert "foo" in result.stdout
    assert "bar" in result.stdout

    # 2. Add 2 more devices to the batch
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch add_device --batch-id 1 --device-list baz,qux",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "Successfully added 2 devices to batch 1" in result.stdout
    assert "baz" in result.stdout
    assert "qux" in result.stdout

    # Verify devices are in the batch
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1 --devices",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert "foo" in result.stdout
    assert "bar" in result.stdout
    assert "baz" in result.stdout
    assert "qux" in result.stdout

    # 3. Remove 1 device from the batch
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch remove_device --batch-id 1 --device-list foo -y",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "Successfully removed 1 devices from batch 1" in result.stdout
    assert "foo" in result.stdout

    # Verify foo is no longer in the batch but others remain
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1 --devices",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert "foo" not in result.stdout
    assert "bar" in result.stdout
    assert "baz" in result.stdout
    assert "qux" in result.stdout

    # 4. Remove 2 devices from the batch
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch remove_device --batch-id 1 --device-list bar,baz -y",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "Successfully removed 2 devices from batch 1" in result.stdout
    assert "bar" in result.stdout
    assert "baz" in result.stdout

    # Verify devices are no longer in the batch
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1 --devices",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert "foo" not in result.stdout
    assert "bar" not in result.stdout
    assert "baz" not in result.stdout
    assert "qux" in result.stdout

def test_cluster_upgrade_batch_add_device_error_cases(mock_cluster_fixture):
    """Test error cases for add_device command."""
    # Setup
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    mock_cluster_fixture.exec_cscfg("device add foo SR IX")
    mock_cluster_fixture.exec_cscfg("device add bar SR IX")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=foo")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=bar")
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list foo --force", environment=["DB_UPDATE_ONLY=true"])

    # Test: Add mix of existing and non-existent devices
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch add_device --batch-id 1 --device-list bar,nonexistent",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 1
    assert "Successfully added 1 devices to batch 1" in result.stdout
    assert "Failed to add 1 devices to batch 1" in result.stderr
    assert "bar" in result.stdout
    assert "nonexistent" in result.stderr

    # Test: Add device that's already in batch (should be skipped)
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch add_device --batch-id 1 --device-list foo",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "already in batch 1. Skipping." in result.stdout

    # Test: Add empty device list
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch add_device --batch-id 1 --device-list ''",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 1
    assert "No valid device names provided in device list" in result.stderr

def test_cluster_upgrade_batch_remove_device_error_cases(mock_cluster_fixture):
    """Test error cases for remove_device command."""
    # Setup
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    mock_cluster_fixture.exec_cscfg("device add foo SR IX")
    mock_cluster_fixture.exec_cscfg("device add bar SR IX")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=foo")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=bar")
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list foo,bar --force", environment=["DB_UPDATE_ONLY=true"])

    # Test: Remove mix of existing and non-existent devices
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch remove_device --batch-id 1 --device-list foo,nonexistent -y",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 1
    assert "Successfully removed 1 devices from batch 1" in result.stdout
    assert "Failed to remove 1 devices from batch 1" in result.stderr
    assert "foo" in result.stdout
    assert "nonexistent" in result.stderr

    # Test: Remove device not in batch
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch remove_device --batch-id 1 --device-list foo -y",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 1
    assert "Failed to remove 1 devices from batch 1" in result.stderr
    assert "not in batch 1" in result.stderr

    # Test: Remove empty device list
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch remove_device --batch-id 1 --device-list '' -y",
                                             environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 1
    assert "No valid device names provided in device list" in result.stderr

# --- Tests for ClusterUpgradeHealthCheckCmd ---

def test_cluster_upgrade_health_check_help(mock_cluster_fixture):
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade health_check --help")
    assert result.returncode in [0, 1]
    assert "usage: cluster upgrade health_check" in result.stdout
    assert "--cluster-name" in result.stdout

def test_cluster_upgrade_health_check_cluster_not_found(mock_cluster_fixture):
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade health_check --cluster-name non-existent")
    assert result.returncode == 1
    assert "Cluster 'non-existent' not found in profile" in result.stderr


# --- Additional tests for cluster upgrade create/cancel edge cases ---
def test_cluster_upgrade_create_blocked_if_ongoing(mock_cluster_fixture):
    """Test that creating a new upgrade is blocked when one is already ongoing."""
    # Create clusters
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")

    # Start first upgrade
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0

    # Attempt to start second upgrade
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 1
    assert "Upgrade ID: 1" in result.stdout

def test_cluster_upgrade_cancel_terminal_state(mock_cluster_fixture):
    """Test that canceling a terminal upgrade returns without changes."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")

    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )

    # Force complete the upgrade
    mock_cluster_fixture.exec_cscfg("cluster upgrade end_upgrade --force", environment=["DB_UPDATE_ONLY=true"])

    # Try to cancel
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade cancel --upgrade-id 1", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "already in terminal status" in result.stdout

def test_cluster_upgrade_cancel_not_started(mock_cluster_fixture):
    """Test that canceling an upgrade in NOT_STARTED state proceeds without prompt."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")

    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )

    result = mock_cluster_fixture.exec_cscfg("cluster upgrade cancel --upgrade-id 1", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "has been successfully cancelled" in result.stdout

def test_cluster_upgrade_cancel_with_prompt_force(mock_cluster_fixture):
    """Test canceling an upgrade in-progress using prompt/force."""
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")

    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])

    # Use force flag to bypass confirmation
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade cancel --upgrade-id 1 --force", environment=["DB_UPDATE_ONLY=true"])
    assert result.returncode == 0
    assert "has been successfully cancelled" in result.stdout

def test_cluster_upgrade_batch_migrate_state_machine(mock_cluster_fixture):
    """Test the batch migration state machine with DB update only mode.
    Validates normal state transitions."""
    
    # Define test cases: (initial_state, expected_completed_state)
    test_cases = [
        (UpgradeBatchStatus.QUIESCE_COMPLETED, UpgradeBatchStatus.UPDATE_DEVICE_ASSOCIATION_COMPLETED),
        (UpgradeBatchStatus.MIGRATE_STARTED, UpgradeBatchStatus.UPDATE_DEVICE_ASSOCIATION_COMPLETED),
        (UpgradeBatchStatus.UPDATE_DEVICE_ASSOCIATION_COMPLETED, UpgradeBatchStatus.MIGRATE_OUT_COMPLETED),
        (UpgradeBatchStatus.MIGRATE_OUT_COMPLETED, UpgradeBatchStatus.SECURITY_PATCH_SERVERS_COMPLETED),
        (UpgradeBatchStatus.SECURITY_PATCH_SERVERS_COMPLETED, UpgradeBatchStatus.UPGRADE_SYSTEMS_COMPLETED),
        (UpgradeBatchStatus.UPGRADE_SYSTEMS_COMPLETED, UpgradeBatchStatus.MIGRATE_IN_COMPLETED),
        (UpgradeBatchStatus.MIGRATE_IN_COMPLETED, UpgradeBatchStatus.MIGRATE_COMPLETED),
    ]

    # Setup: Create clusters and upgrade process
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")

    # add devices
    mock_cluster_fixture.exec_cscfg(
        f"device add blue-cluster-control-plane-1 SR MG "
        f"-p kubernetes.controlplane=true -p management_credentials.password=root"
    )
    mock_cluster_fixture.exec_cscfg(
        f"device add green-cluster-control-plane-1 SR MG "
        f"-p kubernetes.controlplane=true -p management_credentials.password=root"
    )

    # add cluster devices
    mock_cluster_fixture.exec_cscfg(
        f"cluster device edit --cluster blue-cluster -f "
        f"name=blue-cluster-control-plane-1 --controlplane=true -y"
    )
    mock_cluster_fixture.exec_cscfg(
        f"cluster device edit --cluster green-cluster -f "
        f"name=green-cluster-control-plane-1 --controlplane=true -y"
    )

    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )

    # Add test devices
    mock_cluster_fixture.exec_cscfg("device add server1 SR MG")
    mock_cluster_fixture.exec_cscfg("device add system1 SY CS")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=server1")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=system1")

    # Setup upgrade process and create batch
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list server1,system1", environment=["DB_UPDATE_ONLY=true"])
    
    # Complete prerequisite steps
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch taint", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch quiesce -y", environment=["DB_UPDATE_ONLY=true"])

    # Test each state transition
    for current_state, expected_state in test_cases:
        # Set batch status to current state
        set_batch_status(mock_cluster_fixture, 1, current_state.value)

        # Attempt migration
        result = mock_cluster_fixture.exec_cscfg(
            "cluster upgrade batch migrate -y",
            environment=["DB_UPDATE_ONLY=true", "ONE_STEP_ONLY=true"]
        )
        assert result.returncode == 0

        # Verify expected state
        result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1", environment=["DB_UPDATE_ONLY=true"])
        assert expected_state.value in result.stdout

    # Verify final state includes all devices
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1 --devices", environment=["DB_UPDATE_ONLY=true"])
    assert "server1" in result.stdout
    assert "system1" in result.stdout
    assert "COMPLETED" in result.stdout

    # Verify devices moved to destination cluster
    result = mock_cluster_fixture.exec_cscfg("cluster device show --cluster green-cluster")
    assert "server1" in result.stdout
    assert "system1" in result.stdout

def test_cluster_upgrade_batch_migrate_from_failed_state(mock_cluster_fixture):
    """Test recovery from UPDATE_DEVICE_ASSOCIATION_FAILED state."""
    
    # Setup: Create clusters and upgrade process
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")

    # add control plane devices
    mock_cluster_fixture.exec_cscfg(
        f"device add blue-cluster-control-plane-1 SR MG "
        f"-p kubernetes.controlplane=true -p management_credentials.password=root"
    )
    mock_cluster_fixture.exec_cscfg(
        f"device add green-cluster-control-plane-1 SR MG "
        f"-p kubernetes.controlplane=true -p management_credentials.password=root"
    )

    # add cluster devices
    mock_cluster_fixture.exec_cscfg(
        f"cluster device edit --cluster blue-cluster -f "
        f"name=blue-cluster-control-plane-1 --controlplane=true -y"
    )
    mock_cluster_fixture.exec_cscfg(
        f"cluster device edit --cluster green-cluster -f "
        f"name=green-cluster-control-plane-1 --controlplane=true -y"
    )

    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )

    # Add test devices
    mock_cluster_fixture.exec_cscfg("device add server1 SR MG")
    mock_cluster_fixture.exec_cscfg("device add system1 SY CS")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=server1")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=system1")

    # Setup upgrade process and create batch
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list server1,system1", environment=["DB_UPDATE_ONLY=true"])

    # Define test cases: (initial_failed_state, expected_completed_state)
    test_cases = [
        (UpgradeBatchStatus.UPDATE_DEVICE_ASSOCIATION_FAILED, UpgradeBatchStatus.UPDATE_DEVICE_ASSOCIATION_COMPLETED),
        (UpgradeBatchStatus.MIGRATE_OUT_FAILED, UpgradeBatchStatus.MIGRATE_OUT_COMPLETED),
        (UpgradeBatchStatus.SECURITY_PATCH_SERVERS_FAILED, UpgradeBatchStatus.SECURITY_PATCH_SERVERS_COMPLETED),
        (UpgradeBatchStatus.UPGRADE_SYSTEMS_FAILED, UpgradeBatchStatus.UPGRADE_SYSTEMS_COMPLETED),
        (UpgradeBatchStatus.MIGRATE_IN_FAILED, UpgradeBatchStatus.MIGRATE_IN_COMPLETED),
        (UpgradeBatchStatus.COMPLETE_BATCH_MIGRATION_FAILED, UpgradeBatchStatus.MIGRATE_COMPLETED),
    ]

    # Test each state transition
    for failed_state, completed_state in test_cases:
        # Set batch status to failed state
        set_batch_status(mock_cluster_fixture, 1, failed_state.value)

        # Verify initial failed state
        result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1", environment=["DB_UPDATE_ONLY=true"])
        assert failed_state.value in result.stdout

        # Attempt recovery
        result = mock_cluster_fixture.exec_cscfg(
            "cluster upgrade batch migrate -y",
            environment=["DB_UPDATE_ONLY=true", "ONE_STEP_ONLY=true"]
        )
        assert result.returncode == 0

        # Verify final state
        result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1", environment=["DB_UPDATE_ONLY=true"])
        assert completed_state.value in result.stdout

def test_cluster_upgrade_batch_migrate_force_completed(mock_cluster_fixture):
    """Test that force flag with MIGRATE_COMPLETED state skips migration."""
    
    # Setup: Create clusters and upgrade process
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")

    # add control plane devices
    mock_cluster_fixture.exec_cscfg(
        f"device add blue-cluster-control-plane-1 SR MG "
        f"-p kubernetes.controlplane=true -p management_credentials.password=root"
    )
    mock_cluster_fixture.exec_cscfg(
        f"device add green-cluster-control-plane-1 SR MG "
        f"-p kubernetes.controlplane=true -p management_credentials.password=root"
    )

    # add cluster devices
    mock_cluster_fixture.exec_cscfg(
        f"cluster device edit --cluster blue-cluster -f "
        f"name=blue-cluster-control-plane-1 --controlplane=true -y"
    )
    mock_cluster_fixture.exec_cscfg(
        f"cluster device edit --cluster green-cluster -f "
        f"name=green-cluster-control-plane-1 --controlplane=true -y"
    )

    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )

    # Add test devices
    mock_cluster_fixture.exec_cscfg("device add server1 SR MG")
    mock_cluster_fixture.exec_cscfg("device add system1 SY CS")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=server1")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=system1")

    # Setup upgrade process and create batch
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list server1,system1", environment=["DB_UPDATE_ONLY=true"])

    # Set batch to MIGRATE_COMPLETED state
    set_batch_status(mock_cluster_fixture, 1, UpgradeBatchStatus.MIGRATE_COMPLETED.value)

    # Verify initial state
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1", environment=["DB_UPDATE_ONLY=true"])
    assert UpgradeBatchStatus.MIGRATE_COMPLETED.value in result.stdout

    # Attempt migration with force flag - should skip
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade batch migrate --batch-id 1 -y --force",
        environment=["DB_UPDATE_ONLY=true"]
    )
    assert result.returncode == 0
    assert "Batch 1 migrated successfully" in result.stdout

def test_cluster_upgrade_batch_migrate_force_not_started(mock_cluster_fixture):
    """Test that force flag with NOT_STARTED state transitions to QUIESCE_COMPLETED."""
    
    # Setup: Create clusters and upgrade process
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")

    # add control plane devices
    mock_cluster_fixture.exec_cscfg(
        f"device add blue-cluster-control-plane-1 SR MG "
        f"-p kubernetes.controlplane=true -p management_credentials.password=root"
    )
    mock_cluster_fixture.exec_cscfg(
        f"device add green-cluster-control-plane-1 SR MG "
        f"-p kubernetes.controlplane=true -p management_credentials.password=root"
    )

    # add cluster devices
    mock_cluster_fixture.exec_cscfg(
        f"cluster device edit --cluster blue-cluster -f "
        f"name=blue-cluster-control-plane-1 --controlplane=true -y"
    )
    mock_cluster_fixture.exec_cscfg(
        f"cluster device edit --cluster green-cluster -f "
        f"name=green-cluster-control-plane-1 --controlplane=true -y"
    )

    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"]
    )

    # Add test devices
    mock_cluster_fixture.exec_cscfg("device add server1 SR MG")
    mock_cluster_fixture.exec_cscfg("device add system1 SY CS")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=server1")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=system1")

    # Setup upgrade process and create batch
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"])
    mock_cluster_fixture.exec_cscfg("cluster upgrade batch create --device-list server1,system1", environment=["DB_UPDATE_ONLY=true"])

    # Set batch to NOT_STARTED state
    set_batch_status(mock_cluster_fixture, 1, UpgradeBatchStatus.NOT_STARTED.value)

    # Verify initial state
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1", environment=["DB_UPDATE_ONLY=true"])
    assert UpgradeBatchStatus.NOT_STARTED.value in result.stdout

    # Attempt migration with force flag - should use MIGRATE_STARTED handler to trigger batch migration
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade batch migrate --batch-id 1 -y --force",
        environment=["DB_UPDATE_ONLY=true"]
    )
    print(f"result stdout: {result.stdout}")
    print(f"result stderr: {result.stderr}")
    assert result.returncode == 0
    assert "Setting batch 1 status to MIGRATE_STARTED" in result.stdout

    # Verify state was changed to MIGRATE_COMPLETED
    result = mock_cluster_fixture.exec_cscfg("cluster upgrade batch show --batch-id 1", environment=["DB_UPDATE_ONLY=true"])
    assert UpgradeBatchStatus.MIGRATE_COMPLETED.value in result.stdout

def test_upgrade_cluster_control_plane_nodes_security_patch_servers_type_validation(mock_cluster_fixture, monkeypatch):
    """Test that security_patch_servers parameter must be a list of Device objects."""

    # Use monkeypatch to set environment variable just for this test
    monkeypatch.setenv("DB_ONLY", "true")

    # Create a list of strings (invalid type)
    invalid_servers = ["server1", "server2"]

    # Attempt to call the function directly with invalid type
    with pytest.raises(AttributeError) as exc_info:
        upgrade_cluster_control_plane_nodes(
            profile="default",
            cluster_name="blue-cluster",
            cluster_pkg_path="/fake/path",
            security_patch_path="/fake/security/path",
            security_patch_servers=invalid_servers
        )
    print(f"exc_info: {exc_info.value}")
    assert "has no attribute 'name'" in str(exc_info.value)

def test_upgrade_cluster_control_plane_nodes_security_patch_systems_type_validation(mock_cluster_fixture):
    """Test that security_patch_servers parameter must be a list of Device objects."""

    # Create a list of strings (invalid type)
    invalid_systems = ["system1", "system2"]

    # Attempt to call the function directly with invalid type
    with pytest.raises(AttributeError) as exc_info:
        upgrade_systems(invalid_systems, "/fake/path")
    assert "has no attribute 'device'" in str(exc_info.value)

def test_cluster_upgrade_batch_show_devices_implicit_after_cancel(mock_cluster_fixture):
    """Scenario: One upgrade with two batches is cancelled, then a new upgrade with one batch is created.
    Verify that `cluster upgrade batch show --devices -ojson` implicitly selects the only active batch
    of the only active upgrade and lists only its devices.
    """
    # Create clusters
    mock_cluster_fixture.exec_cscfg("cluster add blue-cluster -d blue.example.com -m blue.example.com")
    mock_cluster_fixture.exec_cscfg("cluster add green-cluster -d green.example.com -m green.example.com")

    # --- Create 1st upgrade with 2 batches ---
    # Add devices u1d1, u1d2 and assign to blue-cluster
    mock_cluster_fixture.exec_cscfg("device add u1d1 SR IX")
    mock_cluster_fixture.exec_cscfg("device add u1d2 SR IX")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=u1d1")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=u1d2")

    # Create and progress upgrade 1
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"],
    )
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"]) 
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"]) 

    # Create two batches in upgrade 1
    r = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade batch create --device-list u1d1",
        environment=["DB_UPDATE_ONLY=true"],
    )
    assert r.returncode == 0
    r = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade batch create --device-list u1d2",
        environment=["DB_UPDATE_ONLY=true"],
    )
    assert r.returncode == 0

    # Cancel upgrade 1
    r = mock_cluster_fixture.exec_cscfg("cluster upgrade cancel --upgrade-id 1 --force", environment=["DB_UPDATE_ONLY=true"]) 
    assert r.returncode == 0

    # --- Create 2nd upgrade with 1 batch ---
    mock_cluster_fixture.exec_cscfg("device add u2d1 SR IX")
    mock_cluster_fixture.exec_cscfg("cluster device edit --cluster blue-cluster -y -f name=u2d1")
    mock_cluster_fixture.exec_cscfg(
        "cluster upgrade create --source blue-cluster --dest green-cluster --upgrade-pkg-path /fake/path",
        environment=["DB_UPDATE_ONLY=true"],
    )
    mock_cluster_fixture.exec_cscfg("cluster upgrade upgrade_dest_cluster", environment=["DB_UPDATE_ONLY=true"]) 
    mock_cluster_fixture.exec_cscfg("cluster upgrade prepare_data", environment=["DB_UPDATE_ONLY=true"]) 
    r = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade batch create --device-list u2d1",
        environment=["DB_UPDATE_ONLY=true"],
    )
    assert r.returncode == 0

    # Implicitly select the only active batch of the only active upgrade
    result = mock_cluster_fixture.exec_cscfg(
        "cluster upgrade batch show --devices -ojson",
        environment=["DB_UPDATE_ONLY=true"],
    )
    assert result.returncode == 0
    # Ensure only the device from the active batch/upgrade is listed
    assert '"device_name": "u2d1"' in result.stdout
    assert '"device_name": "u1d1"' not in result.stdout
    assert '"device_name": "u1d2"' not in result.stdout
