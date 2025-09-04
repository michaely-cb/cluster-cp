import json
import unittest
from unittest.mock import MagicMock

from deployment_manager.lib.health_check import (
    ApiServerCheck,
    CephHealthCheck,
    KubeVipCheck,
    NodeDiskSpaceCheck,
    RegistryCheck,
)


class TestHealthChecks(unittest.TestCase):
    def setUp(self):
        """Set up a mock KubernetesCtl for each test."""
        self.mock_kctl = MagicMock()

    def test_api_server_check_success(self):
        """Test ApiServerCheck when pods are running."""
        pod_list = {"items": [{"status": {"phase": "Running"}}]}
        self.mock_kctl.run.return_value = (0, json.dumps(pod_list), "")
        check = ApiServerCheck()
        passed, message = check.execute(self.mock_kctl)
        self.assertTrue(passed)
        self.assertEqual(message, "All API server pods are running.")

    def test_api_server_check_failure(self):
        """Test ApiServerCheck when a pod is not running."""
        pod_list = {"items": [{"metadata": {"name": "api-pod-1"}, "status": {"phase": "Pending"}}]}
        self.mock_kctl.run.return_value = (0, json.dumps(pod_list), "")
        check = ApiServerCheck()
        passed, message = check.execute(self.mock_kctl)
        self.assertFalse(passed)
        self.assertIn("is not running (status: Pending)", message)

    def test_kube_vip_check_success(self):
        """Test KubeVipCheck when pods are running."""
        pod_list = {"items": [{"status": {"phase": "Running"}}]}
        self.mock_kctl.run.return_value = (0, json.dumps(pod_list), "")
        check = KubeVipCheck()
        passed, message = check.execute(self.mock_kctl)
        self.assertTrue(passed)
        self.assertEqual(message, "All kube-vip pods are running.")

    def test_ceph_check_success(self):
        """Test CephHealthCheck when health is OK."""
        ceph_status = {"health": {"status": "HEALTH_OK"}}
        self.mock_kctl.run.return_value = (0, json.dumps(ceph_status), "")
        check = CephHealthCheck()
        passed, message = check.execute(self.mock_kctl)
        self.assertTrue(passed)
        self.assertEqual(message, "Ceph cluster health is OK.")

    def test_ceph_check_failure(self):
        """Test CephHealthCheck when health is not OK."""
        ceph_status = {"health": {"status": "HEALTH_WARN"}}
        self.mock_kctl.run.return_value = (0, json.dumps(ceph_status), "")
        check = CephHealthCheck()
        passed, message = check.execute(self.mock_kctl)
        self.assertFalse(passed)
        self.assertIn("Status: HEALTH_WARN", message)

    def test_registry_check_success(self):
        """Test RegistryCheck when pods are running."""
        pod_list = {"items": [{"status": {"phase": "Running"}}]}
        self.mock_kctl.run.return_value = (0, json.dumps(pod_list), "")
        check = RegistryCheck()
        passed, message = check.execute(self.mock_kctl)
        self.assertTrue(passed)
        self.assertEqual(message, "All registry pods are running.")


    def test_node_disk_space_check_success(self):
        """Test NodeDiskSpaceCheck when all nodes have enough space."""
        nodes = {"items": [{"metadata": {"name": "node-1"}}]}
        # Mock df output *after* 'tail' has removed the header.
        df_output_success = "/dev/mapper/root_vg-root 999999999999 999999999999 322122547200 76% /"

        self.mock_kctl.run.side_effect = [
            (0, json.dumps(nodes), ""),  # First call: kubectl get nodes
            (0, df_output_success, ""),  # Second call: result of ssh ... | tail
        ]
        check = NodeDiskSpaceCheck(required_gb=220)
        passed, message = check.execute(self.mock_kctl)

        self.assertTrue(passed)
        self.assertIn("All nodes have at least 220GB", message)

    def test_node_disk_space_check_failure(self):
        """Test NodeDiskSpaceCheck when a node has insufficient space."""
        nodes = {"items": [{"metadata": {"name": "node-1"}}, {"metadata": {"name": "node-2"}}]}
        df_output_success = "/dev/mapper/root_vg-root 999999999999 999999999999 322122547200 76% /"
        # Mock df output showing 100GB available (100 * 1024**3)
        df_output_fail = "/dev/mapper/root_vg-root 999999999999 999999999999 107374182400 85% /"

        self.mock_kctl.run.side_effect = [
            (0, json.dumps(nodes), ""),      # Call for get nodes
            (0, df_output_success, ""),      # Call for node-1
            (0, df_output_fail, ""),        # Call for node-2
        ]
        check = NodeDiskSpaceCheck(required_gb=220)
        passed, message = check.execute(self.mock_kctl)

        self.assertFalse(passed)
        self.assertIn("node-2 (100.00GB free)", message)

    def test_node_disk_space_check_ssh_failure(self):
        """Test NodeDiskSpaceCheck when the ssh command fails for a node."""
        nodes = {"items": [{"metadata": {"name": "node-1"}}]}
        self.mock_kctl.run.side_effect = [
            (0, json.dumps(nodes), ""),
            # Simulate the entire 'ssh ... | tail' command failing
            (1, "", "ssh: connect to host node-1 port 22: Connection refused"),
        ]
        check = NodeDiskSpaceCheck(required_gb=220)
        passed, message = check.execute(self.mock_kctl)

        self.assertFalse(passed)
        self.assertIn("node-1 (failed to parse API response", message)


if __name__ == "__main__":
    unittest.main()
