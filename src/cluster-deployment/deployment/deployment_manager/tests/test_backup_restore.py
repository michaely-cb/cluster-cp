import json
from deployment_manager.tests.conftest import MockCluster


def test_backup_restore():
    # Test backup and restore functionality
    with MockCluster("backup_restore") as cluster:
        cluster_deployment_base = "/opt/cerebras/cluster-deployment"
        cluster.must_exec_root("mkdir -p /new")
        cluster.must_exec_root("mkdir -p /n0")
        backup_file_dir = "/new"
        # Install cscfg tooling
        cluster.must_exec_root("/project/install.sh")
        cluster.must_exec_root("cscfg profile create mocktest --config_input /host/config/vendor_ingestion-input.yml")
        cluster.must_exec_root("cscfg device import_inventory /host/config/vendor_ingestion-inventory.csv -y")
        # Create a backup
        cluster.must_exec_root(f"cscfg backup create --output_dir {backup_file_dir}")
        
        # Ensure backing up in cluster deployment base fails
        try:
            cluster.must_exec_root(f"cscfg backup create --output_dir {cluster_deployment_base}")
            assert False, "The command 'invalid_command_that_should_fail' should have failed but it succeeded."
        except Exception as e:
            print(f"Expected failure occurred: {e}")

        # Verify the backup file exists
        backup_dir_files = cluster.must_exec_root(f"ls {backup_file_dir}")
        print(f"Backup directory files: {backup_dir_files}")
        backup_files = [file for file in backup_dir_files.splitlines() if file.startswith("cluster-deployment-backup_") and file.endswith(".tar.gz")]
        print(f"Backup files found: {backup_files}")
        assert len(backup_files) == 1, "There should be exactly one backup file starting with 'cluster-deployment-backup_' and ending with '.tar.gz'."

        cluster.must_exec_root("cscfg backup create")
        cluster_deployment_dir = "/n0/cluster-deployment-backups"
        cluster_dir_files = cluster.must_exec_root(f"ls {cluster_deployment_dir}")
        cluster_dir_files = [file for file in cluster_dir_files.splitlines() if file.startswith("cluster-deployment-backup_") and file.endswith(".tar.gz")]
        print(f"Cluster deployment directory files: {cluster_dir_files}")
        assert len(cluster_dir_files) == 1, "Backup file should exist in the default cluster deployment directory."


        # Perform some operations that change the state
        # Add a device
        cluster.must_exec_root("cscfg device add wse002-cs-sy02-test SY CS")

        # Remove a device
        cluster.must_exec_root("cscfg device remove wsx-wse002-pdu01")

        # Restore from the backup
        cluster.must_exec_root(f"cscfg backup restore {backup_file_dir}/{backup_files[0]} -y")

        # Verify the devices were restored
        added_device = cluster.must_exec_root("cscfg device show -f name=wse002-cs-sy02-test -ojson")
        print(f"Added device JSON: {added_device}")
        added_device_json = json.loads(added_device.strip())
        assert not added_device_json, "Device 'wse002-cs-sy02-test' should not exist after restore operation."

        removed_device = cluster.must_exec_root("cscfg device show -f name=wsx-wse002-pdu01 -ojson")
        print(f"Removed device JSON: {removed_device}")
        removed_device_json = json.loads(removed_device.strip())
        assert removed_device_json and removed_device_json[0].get("name") == "wsx-wse002-pdu01", "Device 'wsx-wse002-pdu01' should be restored after restore operation."
        