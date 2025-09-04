"""
SinglePodCephSyncer - CephFS sync using a single pod on the source cluster.

Creates a pod on the source cluster that mounts both CephFS filesystems
and performs rsync between them for better network isolation.
"""

import base64
import json
import logging
import os
import time
from typing import List, Tuple

from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node
from deployment_manager.db import device_props as props

logger = logging.getLogger(__name__)


class SinglePodCephSyncer:
    """CephFS syncer that runs a single pod on the source cluster."""
    
    def __init__(
        self,
        profile: str,
        src_cluster,
        dst_cluster,
        src_conf_content: str,
        src_keyring_content: str,
        dst_conf_content: str,
        dst_keyring_content: str,
        volumes: List[Tuple[str, str]],
        cleanup_mounts: bool = True,
        src_mnt: str = "/mnt/src_ceph",
        dst_mnt: str = "/mnt/dst_ceph",
    ):
        self.profile = profile
        self.src_cluster = src_cluster
        self.dst_cluster = dst_cluster
        self.src_conf_content = src_conf_content
        self.src_keyring_content = src_keyring_content
        self.dst_conf_content = dst_conf_content
        self.dst_keyring_content = dst_keyring_content
        self.volumes = volumes
        self.cleanup_mounts = cleanup_mounts
        self.src_mnt = src_mnt
        self.dst_mnt = dst_mnt
        
        # Get source management node (where pod will be deployed)
        self.src_mgmt_node = get_k8s_lead_mgmt_node(self.profile, self.src_cluster.name)
        self.src_user = self.src_mgmt_node.get_prop(props.prop_management_credentials_user)
        self.src_password = self.src_mgmt_node.get_prop(props.prop_management_credentials_password)
        
        # Generate unique names for this sync operation
        sync_id = f"{int(time.time())}"
        self.job_id = sync_id
        self.job_name = f"ceph-sync-{sync_id}"
        
        # Kubernetes client for source cluster
        self.kctl = None
    
    def _load_external_yaml_template(self) -> str:
        """Load the external YAML template file."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        yaml_file = os.path.join(script_dir, "ceph_sync_job.yaml")
        
        if not os.path.exists(yaml_file):
            raise FileNotFoundError(f"YAML template not found: {yaml_file}")
        
        with open(yaml_file, 'r') as f:
            return f.read()
    
    def _load_shell_script(self, script_name: str) -> str:
        """Load a shell script file."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        script_file = os.path.join(script_dir, script_name)
        
        if not os.path.exists(script_file):
            raise FileNotFoundError(f"Script not found: {script_file}")
        
        with open(script_file, 'r') as f:
            return f.read()
    
    def _create_templated_yaml(self) -> str:
        """Create the final YAML by templating the external file."""
        
        # Load the external YAML template
        yaml_template = self._load_external_yaml_template()
        
        # Load shell scripts and indent them for YAML literal blocks
        def indent_script(content: str) -> str:
            """Indent each line of the script by 4 spaces for YAML literal block."""
            return '\n'.join('    ' + line for line in content.splitlines())
        
        run_sync_script = indent_script(self._load_shell_script("run_sync.sh"))
        mount_ceph_script = indent_script(self._load_shell_script("mount_ceph.sh"))
        sync_volume_script = indent_script(self._load_shell_script("sync_volume.sh"))
        unmount_ceph_script = indent_script(self._load_shell_script("unmount_ceph.sh"))
        
        # Create volumes JSON for the script
        volumes_json = json.dumps([{"group": group, "subvol": subvol} for group, subvol in self.volumes])
        
        # Base64 encode the credentials
        src_conf_b64 = base64.b64encode(self.src_conf_content.encode()).decode()
        src_keyring_b64 = base64.b64encode(self.src_keyring_content.encode()).decode()
        dst_conf_b64 = base64.b64encode(self.dst_conf_content.encode()).decode()
        dst_keyring_b64 = base64.b64encode(self.dst_keyring_content.encode()).decode()
        
        # Template substitutions
        substitutions = {
            "job_id": self.job_id,
            "sync_image": "ubuntu:20.04",
            "volumes_json": volumes_json,
            "cleanup_mounts": str(self.cleanup_mounts).lower(),
            "run_sync_script": run_sync_script,
            "mount_ceph_script": mount_ceph_script,
            "sync_volume_script": sync_volume_script,
            "unmount_ceph_script": unmount_ceph_script,
            "src_conf_b64": src_conf_b64,
            "src_keyring_b64": src_keyring_b64,
            "dst_conf_b64": dst_conf_b64,
            "dst_keyring_b64": dst_keyring_b64,
        }
        
        # Apply substitutions
        templated_yaml = yaml_template
        for key, value in substitutions.items():
            templated_yaml = templated_yaml.replace(f"{{{key}}}", value)
        
        return templated_yaml
    
    def _deploy_pod(self) -> str:
        """Deploy the CephFS sync pod and return the pod name."""
        logger.info(f"Deploying CephFS sync pod: {self.job_name}")
        
        # Clean up any existing jobs with the same name first
        try:
            self.kctl._conn.exec(f"kubectl delete job {self.job_name} -n kube-system --ignore-not-found")
            self.kctl._conn.exec(f"kubectl delete secret ceph-src-config-{self.job_id} -n kube-system --ignore-not-found")
            self.kctl._conn.exec(f"kubectl delete secret ceph-dst-config-{self.job_id} -n kube-system --ignore-not-found")
            self.kctl._conn.exec(f"kubectl delete configmap ceph-sync-scripts-{self.job_id} -n kube-system --ignore-not-found")
            logger.info(f"Cleaned up any existing resources for job: {self.job_name}")
        except Exception as e:
            logger.warning(f"Failed to cleanup existing resources: {e}")
        
        # Generate complete YAML with all resources
        yaml_content = self._create_templated_yaml()
        
        # Debug: Write YAML to file for inspection
        debug_yaml_path = f"/tmp/ceph-sync-debug-{self.job_id}.yaml"
        with open(debug_yaml_path, 'w') as f:
            f.write(yaml_content)
        logger.info(f"Generated YAML written to: {debug_yaml_path}")
        
        # Apply the YAML using a temporary file instead of HERE document
        remote_yaml_path = f"/tmp/ceph-sync-{self.job_id}.yaml"
        
        # Write YAML to remote file
        ret, _, err = self.kctl._conn.exec(f"cat > {remote_yaml_path} << 'EOFYAML'\n{yaml_content}\nEOFYAML")
        if ret != 0:
            raise RuntimeError(f"Failed to write YAML file: {err}")
        
        # Apply from the file
        ret, stdout, err = self.kctl._conn.exec(f"kubectl apply -f {remote_yaml_path}")
        logger.info(f"kubectl apply - return code: {ret}")
        logger.info(f"kubectl apply - stdout: '{stdout}'")
        logger.info(f"kubectl apply - stderr: '{err}'")
        
        if ret != 0:
            raise RuntimeError(f"Failed to create job {self.job_name}: {err}")
        
        # Clean up the temporary file
        self.kctl._conn.exec(f"rm -f {remote_yaml_path}")
        
        logger.info(f"Created job: {self.job_name}")
        return self.job_name
    
    def _monitor_pod(self, job_name: str) -> bool:
        """Monitor the pod until completion and return success status."""
        logger.info(f"Monitoring pod for job: {job_name}")
        
        timeout = 28800  # 8 hours - matches the activeDeadlineSeconds in YAML
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Check job status
            ret, job_status, _ = self.kctl._conn.exec(
                f"kubectl get job {job_name} -n kube-system -o jsonpath='{{.status.conditions[0].type}}'"
            )
            
            if ret == 0:
                status = job_status.strip()
                if status == "Complete":
                    logger.info(f"✓ Job {job_name} completed successfully")
                    return True
                elif status == "Failed":
                    logger.error(f"✗ Job {job_name} failed")
                    return False
            
            # Check pod logs periodically
            if int(time.time()) % 30 == 0:  # Every 30 seconds
                ret, logs, _ = self.kctl._conn.exec(
                    f"kubectl logs -n kube-system -l job-name={job_name} --tail=10"
                )
                if ret == 0 and logs.strip():
                    logger.info(f"Pod progress: {logs.strip()}")
            
            time.sleep(5)
        
        logger.error(f"Job {job_name} timed out after {timeout} seconds")
        return False
    
    def _cleanup(self, job_name: str = None):
        """Clean up Kubernetes resources."""
        if not self.cleanup_mounts:
            logger.info("Cleanup disabled, preserving resources for debugging")
            return
        
        logger.info("Cleaning up Kubernetes resources")
        
        try:
            if job_name:
                self.kctl._conn.exec(f"kubectl delete job {job_name} -n kube-system --ignore-not-found")
                self.kctl._conn.exec(f"kubectl delete secret ceph-src-config-{self.job_id} -n kube-system --ignore-not-found")
                self.kctl._conn.exec(f"kubectl delete secret ceph-dst-config-{self.job_id} -n kube-system --ignore-not-found")
                self.kctl._conn.exec(f"kubectl delete configmap ceph-sync-scripts-{self.job_id} -n kube-system --ignore-not-found")
                logger.info(f"Cleaned up resources for job: {job_name}")
        except Exception as e:
            logger.warning(f"Failed to cleanup resources for job {job_name}: {e}")
    
    def run(self) -> None:
        """
        Run the single-pod CephFS sync operation.
        
        1. Deploy sync pod with embedded credentials and scripts
        2. Monitor pod until completion
        3. Clean up resources
        """
        logger.info("Starting single-pod CephFS sync")
        logger.info(f"Source cluster: {self.src_cluster.name}")
        logger.info(f"Destination cluster: {self.dst_cluster.name}")
        logger.info(f"Pod will run on: {self.src_mgmt_node.name}")
        
        job_name = None
        
        try:
            # Connect to source cluster
            with KubernetesCtl(self.profile, self.src_mgmt_node.name, self.src_user, self.src_password) as kctl:
                self.kctl = kctl
                
                # Deploy pod
                job_name = self._deploy_pod()
                
                # Monitor pod until completion
                success = self._monitor_pod(job_name)
                
                if success:
                    logger.info("Single-pod CephFS sync completed successfully")
                else:
                    # Get final logs for debugging
                    ret, logs, _ = self.kctl._conn.exec(
                        f"kubectl logs -n kube-system -l job-name={job_name} --tail=50"
                    )
                    if ret == 0:
                        logger.error(f"Pod logs:\\n{logs}")
                    
                    raise RuntimeError("CephFS sync failed")
                
        except Exception as e:
            logger.error(f"Single-pod CephFS sync failed: {e}")
            raise
        finally:
            # Clean up resources
            if self.kctl:
                self._cleanup(job_name)