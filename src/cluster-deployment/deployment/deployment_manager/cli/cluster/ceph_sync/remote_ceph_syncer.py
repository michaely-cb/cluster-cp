import json
import logging
import os
import tempfile
from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node
from deployment_manager.db import device_props as props

logger = logging.getLogger(__name__)


class RemoteCephSyncer:
    """
    CephSyncer that runs sync operations on the management nodes via SSH.
    
    This avoids network connectivity issues by running the sync directly on nodes
    that have access to both source and destination CephFS clusters.
    """
    
    def __init__(
        self,
        profile,
        src_cluster,
        dst_cluster,
        cluster_pkg_path: str,
        src_conf_content: str,
        src_keyring_content: str,
        dst_conf_content: str,
        dst_keyring_content: str,
        volumes: list[tuple[str, str]],
        cleanup_mounts: bool = True,
        src_mnt: str = "/mnt/src_ceph",
        dst_mnt: str = "/mnt/dst_ceph",
    ):
        self.profile = profile
        self.src_cluster = src_cluster
        self.dst_cluster = dst_cluster
        self.cluster_pkg_path = cluster_pkg_path
        self.src_conf_content = src_conf_content
        self.src_keyring_content = src_keyring_content
        self.dst_conf_content = dst_conf_content
        self.dst_keyring_content = dst_keyring_content
        self.volumes = volumes
        self.cleanup_mounts = cleanup_mounts
        self.src_mnt = src_mnt
        self.dst_mnt = dst_mnt
        
        # Get management nodes
        self.src_mgmt_node = get_k8s_lead_mgmt_node(self.profile, self.src_cluster.name)
        self.dst_mgmt_node = get_k8s_lead_mgmt_node(self.profile, self.dst_cluster.name)
        
        # We'll run the sync on the source management node since both
        # source and destination CephFS are accessible from there
        self.sync_node = self.src_mgmt_node
        self.sync_user = self.sync_node.get_prop(props.prop_management_credentials_user)
        self.sync_password = self.sync_node.get_prop(props.prop_management_credentials_password)
    
    def _upload_scripts(self, kctl: KubernetesCtl, remote_dir: str):
        """Upload sync scripts to the remote management node."""
        logger.info(f"Uploading sync scripts to {self.sync_node.name}:{remote_dir}")
        
        # Create the remote directory
        kctl.run(f"mkdir -p {remote_dir}")
        
        # Get the local script directory
        local_script_dir = os.path.dirname(__file__)
        
        # List of scripts to upload
        scripts = ["run_sync.sh", "mount_ceph.sh", "sync_volume.sh", "unmount_ceph.sh"]
        
        for script_name in scripts:
            script_path = os.path.join(local_script_dir, script_name)
            if os.path.exists(script_path):
                with open(script_path, 'r') as f:
                    script_content = f.read()
                kctl.run(f"cat > {remote_dir}/{script_name} << 'EOF'\n{script_content}\nEOF")
                kctl.run(f"chmod +x {remote_dir}/{script_name}")
                logger.info(f"Uploaded {script_name}")
            else:
                logger.warning(f"Script not found: {script_path}")
    
    def _upload_ceph_configs(self, kctl: KubernetesCtl, remote_dir: str):
        """Upload Ceph configuration files to the remote management node."""
        logger.info(f"Uploading Ceph configs to {self.sync_node.name}:{remote_dir}")
        
        # Create config directories
        kctl.run(f"mkdir -p {remote_dir}/src {remote_dir}/dst")
        
        # Upload source cluster config
        kctl.run(f"cat > {remote_dir}/src/ceph.conf << 'EOF'\n{self.src_conf_content}\nEOF")
        kctl.run(f"cat > {remote_dir}/src/keyring << 'EOF'\n{self.src_keyring_content}\nEOF")
        
        # Upload destination cluster config
        kctl.run(f"cat > {remote_dir}/dst/ceph.conf << 'EOF'\n{self.dst_conf_content}\nEOF")
        kctl.run(f"cat > {remote_dir}/dst/keyring << 'EOF'\n{self.dst_keyring_content}\nEOF")
        
        # Set appropriate permissions
        kctl.run(f"chmod 600 {remote_dir}/src/keyring {remote_dir}/dst/keyring")
        kctl.run(f"chmod 644 {remote_dir}/src/ceph.conf {remote_dir}/dst/ceph.conf")
    
    def _prepare_sync_environment(self, kctl: KubernetesCtl, remote_dir: str):
        """Prepare the environment for running the shared sync script."""
        logger.info(f"Preparing sync environment on {self.sync_node.name}")
        
        # Create volumes JSON for the script
        volumes_json = json.dumps([{"group": group, "subvol": subvol} for group, subvol in self.volumes])
        
        # Create environment script that sets up variables for run_sync.sh
        env_script_content = f'''#!/bin/bash
# Environment setup for CephFS sync
export VOLUMES_TO_SYNC='{volumes_json}'
export SRC_MNT="{self.src_mnt}"
export DST_MNT="{self.dst_mnt}"
export CLEANUP_MOUNTS="{str(self.cleanup_mounts).lower()}"
export SRC_CEPH_CONF="{remote_dir}/configs/src/ceph.conf"
export SRC_KEYRING="{remote_dir}/configs/src/keyring"
export DST_CEPH_CONF="{remote_dir}/configs/dst/ceph.conf"
export DST_KEYRING="{remote_dir}/configs/dst/keyring"

# Change to the scripts directory so run_sync.sh can find the other scripts
cd "{remote_dir}/scripts"

# Execute the shared orchestration script
exec ./run_sync.sh
'''
        
        kctl.run(f"cat > {remote_dir}/execute_sync.sh << 'EOF'\n{env_script_content}\nEOF")
        kctl.run(f"chmod +x {remote_dir}/execute_sync.sh")
    
    def _verify_credentials(self) -> None:
        """Verify that Ceph credentials contain required fields."""
        logger.info("Verifying Ceph credentials...")
        
        # Verify source credentials
        if "mon_host" not in self.src_conf_content:
            raise RuntimeError("Source ceph.conf missing mon_host")
        if "[client.admin]" not in self.src_keyring_content:
            raise RuntimeError("Source keyring missing [client.admin] section")
        
        # Verify destination credentials  
        if "mon_host" not in self.dst_conf_content:
            raise RuntimeError("Destination ceph.conf missing mon_host")
        if "[client.admin]" not in self.dst_keyring_content:
            raise RuntimeError("Destination keyring missing [client.admin] section")
            
        # Extract and log monitor IPs for debugging
        import re
        src_mons = re.search(r'mon_host\s*=\s*(.+)', self.src_conf_content)
        dst_mons = re.search(r'mon_host\s*=\s*(.+)', self.dst_conf_content)
        
        if src_mons:
            logger.info(f"Source cluster monitors: {src_mons.group(1).strip()}")
        if dst_mons:
            logger.info(f"Destination cluster monitors: {dst_mons.group(1).strip()}")
            
        logger.info("✓ Credentials verification passed")


    def run(self) -> None:
        """
        Run the CephFS sync operation on the remote management node.
        
        1. Verify credentials and Rook pods
        2. Connect to the destination management node
        3. Upload scripts and configurations
        4. Execute the sync operation
        5. Clean up temporary files
        """
        logger.info(f"Starting remote CephFS sync on {self.sync_node.name}")
        
        # Verify credentials
        self._verify_credentials()
        
        # Create a unique temporary directory for this sync operation
        import time
        sync_id = f"ceph-sync-{int(time.time())}"
        remote_dir = f"/tmp/{sync_id}"
        
        try:
            with KubernetesCtl(self.profile, self.sync_node.name, self.sync_user, self.sync_password) as kctl:
                # Create ceph macvlan link if it doesn't exist
                kctl.run(f"{self.cluster_pkg_path}/csadm/add_ceph_macvlan_link.sh")

                # Create remote directory structure
                kctl.run(f"mkdir -p {remote_dir}/scripts {remote_dir}/configs")
                
                # Upload scripts and configurations
                self._upload_scripts(kctl, f"{remote_dir}/scripts")
                self._upload_ceph_configs(kctl, f"{remote_dir}/configs")
                self._prepare_sync_environment(kctl, remote_dir)
                
                # Execute the sync operation with retry logic
                logger.info("Executing CephFS sync operation...")
                max_retries = 2
                retry_delay = 30  # seconds
                
                for attempt in range(max_retries + 1):
                    try:
                        if attempt > 0:
                            logger.info(f"Retry attempt {attempt}/{max_retries} after {retry_delay}s delay...")
                            import time
                            time.sleep(retry_delay)
                        
                        _, stdout, stderr = kctl.run(f"{remote_dir}/execute_sync.sh")
                        logger.info("Sync operation completed successfully")
                        if stdout:
                            logger.info(f"Sync output: {stdout}")
                        if stderr:
                            logger.warning(f"Sync stderr: {stderr}")
                        break  # Success, exit retry loop
                        
                    except Exception as e:
                        logger.error(f"Sync operation failed on attempt {attempt + 1}: {e}")
                        
                        # Check if this is a retryable error
                        error_str = str(e).lower()
                        retryable_errors = [
                            "connection timed out",
                            "connection refused", 
                            "network is unreachable",
                            "temporary failure",
                            "no route to host"
                        ]
                        
                        is_retryable = any(err in error_str for err in retryable_errors)
                        
                        if attempt == max_retries or not is_retryable:
                            logger.error(f"Sync failed after {attempt + 1} attempts. Error: {e}")
                            if not is_retryable:
                                logger.error("Error is not retryable, aborting.")
                            raise
                        else:
                            logger.warning(f"Retryable error detected, will retry in {retry_delay}s...")
                
        except Exception as sync_error:
            # Don't clean up on failure for debugging
            logger.error(f"Sync failed, preserving directory for debugging: {remote_dir}")
            logger.info(f"To examine debug output: ssh {self.sync_node.name} 'cd {remote_dir} && ./run_sync.sh'")
            logger.info(f"To clean up manually: ssh {self.sync_node.name} 'rm -rf {remote_dir}'")
            raise sync_error
        finally:
            # Only clean up on success
            cleanup_on_failure = os.environ.get("CLEANUP_ON_CEPH_FAILURE", "false").lower() == "true"
            if cleanup_on_failure:
                try:
                    with KubernetesCtl(self.profile, self.sync_node.name, self.sync_user, self.sync_password) as kctl:
                        kctl.run(f"rm -rf {remote_dir}")
                        logger.info(f"Cleaned up temporary directory: {remote_dir}")
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary directory {remote_dir}: {e}")
            else:
                logger.info(f"Directory preserved for success case: {remote_dir}")
        
        logger.info("Remote CephFS sync operation completed")