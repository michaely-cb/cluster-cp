import json
import logging
from abc import ABC, abstractmethod
from typing import List, Tuple

from deployment_manager.tools.kubernetes.interface import KubernetesCtl

logger = logging.getLogger(__name__)


class HealthCheck(ABC):
    """Abstract base class for a single health check."""

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    @abstractmethod
    def execute(self, kctl: KubernetesCtl) -> Tuple[bool, str]:
        """
        Executes the health check.

        Args:
            kctl: An initialized KubernetesCtl instance.

        Returns:
            A tuple containing:
            - bool: True if the check passed, False otherwise.
            - str: A message describing the result.
        """
        pass


class ApiServerCheck(HealthCheck):
    """Checks if Kubernetes API server pods are healthy."""

    def __init__(self):
        super().__init__("API Server", "Checks if all Kubernetes API server pods are running and ready.")

    def execute(self, kctl: KubernetesCtl) -> Tuple[bool, str]:
        try:
            _, out, _ = kctl.run("kubectl get pods -n kube-system -l component=kube-apiserver -o json")
            data = json.loads(out)
            if not data["items"]:
                return False, "No API server pods found."
            for pod in data["items"]:
                if pod["status"]["phase"] != "Running":
                    return False, f"Pod {pod['metadata']['name']} is not running (status: {pod['status']['phase']})."
            return True, "All API server pods are running."
        except Exception as e:
            return False, f"Failed to check API server pods: {e}"


class KubeVipCheck(HealthCheck):
    """Checks if kube-vip pods are healthy."""

    def __init__(self):
        super().__init__("Kube-VIP", "Checks if all kube-vip pods are running and ready.")

    def execute(self, kctl: KubernetesCtl) -> Tuple[bool, str]:
        try:
            _, out, _ = kctl.run("kubectl get pods -n kube-system -l app.kubernetes.io/name=kube-vip -o json")
            data = json.loads(out)
            if not data["items"]:
                return False, "No kube-vip pods found."
            for pod in data["items"]:
                if pod["status"]["phase"] != "Running":
                    return False, f"Pod {pod['metadata']['name']} is not running (status: {pod['status']['phase']})."
            return True, "All kube-vip pods are running."
        except Exception as e:
            return False, f"Failed to check kube-vip pods: {e}"


class CephHealthCheck(HealthCheck):
    """Checks if the Ceph cluster is healthy."""

    def __init__(self):
        super().__init__("Ceph Cluster", "Checks the health of the Ceph storage cluster.")

    def execute(self, kctl: KubernetesCtl) -> Tuple[bool, str]:
        try:
            _, out, _ = kctl.run("kubectl -n rook-ceph exec -it deploy/rook-ceph-tools -- ceph status --format json")
            data = json.loads(out)
            health_status = data.get("health", {}).get("status")
            if health_status == "HEALTH_OK":
                return True, "Ceph cluster health is OK."
            else:
                return False, f"Ceph cluster is not healthy. Status: {health_status}."
        except Exception as e:
            return False, f"Failed to check Ceph status: {e}"


class RegistryCheck(HealthCheck):
    """Checks if container registry pods are healthy."""

    def __init__(self):
        super().__init__("Container Registry", "Checks if container registry pods are running and ready.")

    def execute(self, kctl: KubernetesCtl) -> Tuple[bool, str]:
        try:
            _, out, _ = kctl.run("kubectl get pods -n kube-system -l app=private-registry -o json")
            data = json.loads(out)
            if not data["items"]:
                return False, "No registry pods found in namespace 'kube-system' with label 'app=private-registry'."
            for pod in data["items"]:
                if pod["status"]["phase"] != "Running":
                    return False, f"Pod {pod['metadata']['name']} is not running (status: {pod['status']['phase']})."
            return True, "All registry pods are running."
        except Exception as e:
            return False, f"Failed to check registry pods: {e}"


class NodeDiskSpaceCheck(HealthCheck):
    """Checks if each node has sufficient free disk space."""

    def __init__(self, required_gb=220):
        self.required_gb = required_gb
        super().__init__("Node Disk Space", f"Checks if each node has at least {required_gb}GB of free disk space.")

    def execute(self, kctl: KubernetesCtl) -> Tuple[bool, str]:
        try:
            _, out, _ = kctl.run("kubectl get nodes -o json")
            nodes = json.loads(out)["items"]
            failed_nodes = []
            for node in nodes:
                node_name = node["metadata"]["name"]
                try:
                    # This command checks the available space on the root partition in gigabytes.
                    _, df_out, _ = kctl.run(f"ssh {node_name} df -B1 / | tail -n +2")

                    parts = df_out.strip().split()
                    if len(parts) < 4:
                        failed_nodes.append(f"{node_name} (failed to parse API response)")
                        continue

                    available_bytes = int(parts[3]) 
                    available_gb = available_bytes / (1024 ** 3)

                    if available_gb < self.required_gb:
                        failed_nodes.append(f"{node_name} ({available_gb:.2f}GB free)")

                except Exception as e:
                    failed_nodes.append(f"{node_name} (error during check: {e})")

            if failed_nodes:
                return False, f"The following nodes have less than {self.required_gb}GB free: {', '.join(failed_nodes)}."
            return True, f"All nodes have at least {self.required_gb}GB of free space."
        except Exception as e:
            return False, f"Failed to check node disk space: {e}"


def get_all_health_checks() -> List[HealthCheck]:
    """Returns a list of all health check instances."""
    return [
        ApiServerCheck(),
        KubeVipCheck(),
        CephHealthCheck(),
        RegistryCheck(),
        NodeDiskSpaceCheck(),
    ]