import logging
from typing import List, Optional, Set

import yaml

from deployment_manager.network_config.schema import NetworkConfigSchema
from deployment_manager.tools.ssh import ExecMixin, SSHConn

logger = logging.getLogger(__name__)

_K8_NET_JSON_PATH = "/opt/cerebras/cluster/network_config.json"


class KubernetesCtl:
    """
    Class to interact with k8s cluster
    """

    def __init__(self, profile, k8s_deploy_host, k8s_deploy_user, k8s_deploy_password):
        self._profile = profile
        self._deploy_host = k8s_deploy_host
        self._deploy_user = k8s_deploy_user
        self._deploy_password = k8s_deploy_password
        self._conn: Optional[ExecMixin] = None

    def __enter__(self) -> 'KubernetesCtl':
        self._conn = SSHConn(self._deploy_host, self._deploy_user, self._deploy_password)
        self._conn.__enter__()
        return self

    def __exit__(self, *args):
        if hasattr(self._conn, '__exit__'):
            self._conn.__exit__(*args)

    def run(self, cmd: str, timeout=None, save_output=None, raise_exc=True):
        """
        Execute the command on the k8s deploy node with enhanced logging.

        Args:
            cmd: Command to execute
            timeout: Execution timeout
            save_output: If True, saves output to /tmp file and logs the filename.
                        If None, auto-detects based on command (csadm.sh commands get saved)
            raise_exc: raise exception if error
        """
        import time

        # Auto-detect if we should save output for csadm.sh commands
        if save_output is None:
            save_output = "csadm.sh" in cmd

        # Log command execution start
        logger.info(f"[{self._deploy_host}] Executing: {cmd}")
        start_time = time.time()

        try:
            ret, stdout, stderr = self._conn.exec(cmd, timeout=timeout)
            duration = time.time() - start_time

            # Save output to file if requested
            output_file = None
            if save_output:
                output_file = self._save_command_output(cmd, stdout, stderr, ret, duration)
                logger.info(f"[{self._deploy_host}] Command output saved to: {output_file}")

            # Log execution summary
            if ret == 0:
                logger.info(f"[{self._deploy_host}] Command completed successfully (exit_code={ret}, duration={duration:.2f}s)")
            else:
                logger.error(f"[{self._deploy_host}] Command failed (exit_code={ret}, duration={duration:.2f}s)")
                if not save_output and stderr:
                    logger.error(f"[{self._deploy_host}] STDERR: {stderr[:500]}{'...' if len(stderr) > 500 else ''}")

            if ret != 0:
                error_msg = f"Command failed with exit code {ret}"
                if output_file:
                    error_msg += f" (full output in {output_file})"
                else:
                    error_msg += f": {stderr}"
                if raise_exc:
                    raise Exception(error_msg)

            return ret, stdout, stderr
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[{self._deploy_host}] Command execution failed after {duration:.2f}s: {str(e)}")
            raise Exception(f"Exception running {cmd}: {e}")

    def _save_command_output(self, cmd: str, stdout: str, stderr: str, exit_code: int, duration: float):
        """Save command output to a temporary file for debugging."""
        import tempfile
        import time

        # Create a unique filename
        timestamp = int(time.time())
        cmd_name = cmd.split()[0].split('/')[-1] if cmd else "unknown"
        temp_file = tempfile.mktemp(
            prefix=f"k8s_cmd_{cmd_name}_{timestamp}_",
            suffix=".log",
            dir="/tmp"
        )

        try:
            with open(temp_file, 'w') as f:
                f.write(f"=== Command Execution Log ===\n")
                f.write(f"Node: {self._deploy_host}\n")
                f.write(f"Command: {cmd}\n")
                f.write(f"Exit Code: {exit_code}\n")
                f.write(f"Duration: {duration:.2f} seconds\n")
                f.write(f"Timestamp: {time.ctime()}\n")
                f.write(f"\n=== STDOUT ===\n")
                f.write(stdout if stdout else "(no stdout)\n")
                f.write(f"\n=== STDERR ===\n")
                f.write(stderr if stderr else "(no stderr)\n")

            return temp_file
        except Exception as e:
            logger.warning(f"Failed to save command output to {temp_file}: {e}")
            return None

    def csctl_get_nodes(self) -> str:
        """
        Returns the list of nodes in the cluster.
        """
        try:
            ret, out, _ = self._conn.exec("csctl get cluster --node-only --output json")
            if ret != 0:
                raise Exception("Failed to get nodes from kubernetes cluster")
            return out.strip()
        except Exception as e:
            raise Exception(f"Exception getting nodes: {e}")


    def csctl_get_node_names(self) -> int:
        """
        Returns the names for all nodes in the cluster. separated by commas.
        """
        ret, out, err = self._conn.exec(
            "set -o pipefail; csctl get cluster --node-only --output json |"
            " jq -r '[.items[]?.meta.name // empty] | join(\",\")'"
        )
        if ret != 0:
            raise Exception(f"Failed to get the names of nodes from kubernetes cluster: {err}")
        return out.strip()


    def csctl_get_system_names(self) -> int:
        """
        Returns the names of systems in the cluster, separated by commas.
        """
        ret, out, stderr = self._conn.exec(
            "set -o pipefail; csctl get cluster --system-only --output json |"
            " jq -r '[.items[]?.meta.name // empty] | join(\",\")'"
        )
        if ret != 0:
            raise Exception(f"Failed to get the number of systems from kubernetes cluster: {stderr}")
        return out.strip()


    def csctl_cancel_job(self, job_id: str, namespace: str) -> str:
        """
        Cancel a job in the kubernetes cluster.
        :param job_id: The ID of the job to cancel.
        :param namespace: The namespace of the job.
        :return: The output of the cancel command.
        """
        try:
            ret, out, err = self._conn.exec(f"csctl cancel job {job_id} -n {namespace}")
            if ret != 0:
                raise Exception(f"Failed to cancel job {job_id} in namespace {namespace}: {err}")
            return out.strip()
        except Exception as e:
            raise Exception(f"Exception cancelling job {job_id}: {e}")

    def get_cluster_device_names(self) -> Set[str]:
        """
        Reads the cluster config map and returns set of switch, system, node names
        """
        names = set()
        try:
            ret, out, _ = self._conn.exec(
                "kubectl get cm cluster -n job-operator -ojsonpath='{.data.clusterConfiguration\.yaml}'"
            )
            if ret != 0:
                # xxx: deployment manager should track the state of the k8s cluster deployment
                logger.warning(
                    "failed to get cluster config map. This may be because this is the first time "
                    "the cluster is being deployed"
                )
                return set()
            else:
                cluster_cfg = yaml.safe_load(out)
                if cluster_cfg is not None:
                    names.update(set(n['name'] for n in cluster_cfg.get('nodes', [])))
                    names.update(set(s['name'] for s in cluster_cfg.get('systems', [])))
                    names.update(set(g['name'] for g in cluster_cfg.get("v2Groups", [])))  # switches
        except:
            logger.exception("Failed to retrieve K8S devices")
            raise Exception("Failed to retrieve K8S devices")
        return names

    def get_network_config(self) -> Optional[str]:
        """ Attempt to read the network config from the k8s deploy node, returning None if it fails """
        try:
            rv, out, _ = self._conn.exec(f"cat {_K8_NET_JSON_PATH}")
            return out
        except Exception as e:
            logger.warning("Failed to read network config from k8s deploy node: %s", e)
            return None

    def upload_network_config(self, network_config_file):
        """
        Copy network_config_file to /opt/cerebras/cluster/network_config.json on the k8s deploy node.
        """
        try:
            NetworkConfigSchema().load_json(network_config_file)
        except Exception as e:
            raise Exception(f"Invalid network config: {network_config_file}: {e}")
        try:
            self._conn.scp_file(network_config_file, "/tmp/network_config.json")
            self._conn.exec(f"mv -f /tmp/network_config.json {_K8_NET_JSON_PATH}")
        except:
            raise Exception("Failed to update k8s network config")

    def copy_file(self, source_file_path, dest_file_path):
        """
        Copy a file specified at source_file_path to dest_file_path on the k8s deploy node.
        """
        self._conn.scp_file(source_file_path, dest_file_path)

    def create_prometheus_system_credential_secret(self, secret_name: str, filename_with_secret_value: str) -> int:
        """
        Create (or replace an existing) secret using a management node
        """
        try:
            ret, out, err = self._conn.exec("mktemp")
            if ret:
                logger.warning(
                    f"Failed to create k8s secret {secret_name}: {err}"
                )
                return 1
            tmpname = out.strip()
            self._conn.scp_file(filename_with_secret_value, tmpname)
            ret, _, err = self._conn.exec(
                "set -o pipefail; "
                "kubectl create secret generic system-credential -nprometheus --from-literal=user=admin "
                f"--from-file=password={tmpname} --dry-run=client -o yaml | "
                "kubectl apply --server-side --force-conflicts=true -f -;"
            )
            self._conn.exec(f"rm -f {tmpname}")  # always execute even if previous command failed
            if ret:
                logger.error(
                    f"Failed to create k8s secret {secret_name}: {err}"
                )
                return 1
            else:
                logger.info(f"Created k8s secret {secret_name}")
                return 0
        except:
            logger.exception(f"Failed to connect to {self._deploy_host}")
            return 1

    def create_aws_ecr_credential_secret(
        self, secret_name: str, filename_for_key: str, filename_for_secret: str, account_id: str, region: str
    ) -> int:
        """
        Create a (or replace an existing) aws ecr credential secret using a management node
        """
        try:
            ret, out, err = self._conn.exec("mktemp")
            if ret:
                logger.warning(
                    f"Failed to create k8s secret {secret_name}: {err}"
                )
                return 1
            tmpname_for_key = out.strip()
            self._conn.scp_file(filename_for_key, tmpname_for_key)

            ret, out, err = self._conn.exec("mktemp")
            if ret:
                logger.warning(
                    f"Failed to create k8s secret {secret_name}: {err}"
                )
                return 1
            tmpname_for_secret = out.strip()
            self._conn.scp_file(filename_for_secret, tmpname_for_secret)

            ret, _, err = self._conn.exec(
                "set -o pipefail; "
                f"kubectl create secret generic {secret_name} -njob-operator "
                f"--from-file=key={tmpname_for_key} --from-file=secret={tmpname_for_secret} "
                f"--from-literal=account_id={account_id} --from-literal=region={region} --dry-run=client -o yaml | "
                "kubectl apply --server-side --force-conflicts=true -f -;"
            )
            # always execute rm even if previous command failed
            self._conn.exec(f"rm -f {tmpname_for_key}")
            self._conn.exec(f"rm -f {tmpname_for_secret}")
            if ret:
                logger.error(
                    f"Failed to create k8s secret {secret_name}: {err}"
                )
                return 1
            else:
                logger.info(f"Created k8s secret {secret_name}")
                return 0
        except:
            logger.exception(f"Failed to connect to {self._deploy_host}")
            return 1
