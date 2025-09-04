"""
Tests for database migrations, specifically for cluster initialization and device association
"""
# Import the migration functions by importing the module dynamically
import importlib.util
import logging
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from django.test.testcases import TestCase

from deployment_manager.db.models import (
    Cluster, ClusterDevice, DeploymentDeviceRoles, DeploymentDeviceTypes, DeploymentProfile, Device, DeviceProperty,
)


def import_migration_module():
    """Dynamically import the 0007 migration module"""
    migration_file = Path(__file__).parent.parent / 'db' / 'migrations' / '0007_init_cluster_data.py'
    spec = importlib.util.spec_from_file_location("migration_0007", str(migration_file))
    migration_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration_module)
    return migration_module


migration_0007 = import_migration_module()


@pytest.mark.skip
class ClusterDbMigrationTest(TestCase):
    """
    Tests for the 0007_init_cluster_data migration functions.
    Django models tests. Should not be run through pytest as they require additional setup steps
    which are launched through django's manage script. Run make -C $GITTOP/src/cluster_deployment/deployment unittest
    to execute
    """

    def setUp(self):
        """Set up test data for migration tests"""
        # Log the current test method name
        test_method = self._testMethodName
        logger = logging.getLogger(__name__)
        logger.info(f"Setting up test: {self.__class__.__name__}.{test_method}")
        
        # Create a test profile
        self.profile = DeploymentProfile.create("test-profile", "test-cluster", True)

        # Create a primary cluster for the profile
        self.cluster = Cluster.objects.create(
            name="test-cluster",
            profile=self.profile,
            is_primary=True
        )

    def tearDown(self):
        """Clean up all test data after each test"""
        # Clean up in reverse order of dependencies
        ClusterDevice.objects.all().delete()
        DeviceProperty.objects.all().delete()
        Device.objects.all().delete()
        Cluster.objects.all().delete()
        DeploymentProfile.objects.all().delete()

    def get_django_apps(self):
        """Get Django apps for migration context"""
        from django.apps import apps
        return apps

    def test_single_mg_node_marked_as_controlplane(self):
        """Test that with a single MG node, it gets marked as controlplane"""
        # Create a single MG device
        mg_device = Device.add("mg-node-1", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.MANAGEMENT, self.profile)

        # Cleanup auto created association by "sync_cluster_device" to allow migration
        ClusterDevice.objects.all().delete()

        # Run the migration function
        migration_0007.associate_all_devices_with_cluster(self.get_django_apps(), None)

        # Check that the device was marked as controlplane
        cluster_device = ClusterDevice.objects.get(device=mg_device)
        self.assertTrue(cluster_device.controlplane)
        self.assertEqual(cluster_device.cluster, self.cluster)

    def test_root_server_gets_controlplane(self):
        """Test that with multiple MG nodes, only the root server gets marked as controlplane"""
        # Import the migration module to patch ConfGen in its namespace
        with patch.object(migration_0007, "ConfGen") as mock_confgen:
            # Create multiple MG devices
            mg_device1 = Device.add("mg-node-1", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.MANAGEMENT, self.profile)
            mg_device2 = Device.add("mg-node-2", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.MANAGEMENT, self.profile)

            # Cleanup auto created association by "sync_cluster_device" to allow migration
            ClusterDevice.objects.all().delete()

            # Mock ConfGen to return mg-node-1 as root server
            mock_cg_instance = Mock()
            mock_cg_instance.get_root_server.return_value = "mg-node-1"
            mock_confgen.return_value = mock_cg_instance

            # Run the migration function
            migration_0007.associate_all_devices_with_cluster(self.get_django_apps(), None)

            # Check that only mg-node-1 is marked as controlplane
            mg1_cluster_device = ClusterDevice.objects.get(device=mg_device1)
            self.assertTrue(mg1_cluster_device.controlplane)

            mg2_cluster_device = ClusterDevice.objects.get(device=mg_device2)
            self.assertFalse(mg2_cluster_device.controlplane)

    def test_controlplane_property_mgmt_node(self):
        """Test that explicit controlplane property takes precedence over heuristics"""
        # Import the migration module to patch ConfGen in its namespace
        with patch.object(migration_0007, "ConfGen") as mock_confgen:
            # Create multiple MG devices
            mg_device1 = Device.add("mg-node-1", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.MANAGEMENT, self.profile)
            mg_device2 = Device.add("mg-node-2", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.MANAGEMENT, self.profile)

            # Set explicit controlplane property on mg-node-2
            mg_device2.update_property("kubernetes", "controlplane", "True")

            # Cleanup auto created association by "sync_cluster_device" to allow migration
            ClusterDevice.objects.all().delete()

            # Don't set any root server - explicit property should take precedence
            mock_cg_instance = Mock()
            mock_cg_instance.get_root_server.return_value = None
            mock_confgen.return_value = mock_cg_instance

            # Run the migration function
            migration_0007.associate_all_devices_with_cluster(self.get_django_apps(), None)

            # Check that mg-node-2 is marked as controlplane due to explicit property
            mg1_cluster_device = ClusterDevice.objects.get(device=mg_device1)
            self.assertFalse(mg1_cluster_device.controlplane)

            mg2_cluster_device = ClusterDevice.objects.get(device=mg_device2)
            self.assertTrue(mg2_cluster_device.controlplane)

    def test_migration_is_idempotent(self):
        """Test that running the migration multiple times doesn't create duplicates"""
        # Import the migration module to patch ConfGen in its namespace
        with patch.object(migration_0007, "ConfGen") as mock_confgen:
            # Create devices
            mg_device = Device.add("mg-node-1", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.MANAGEMENT, self.profile)
            worker_device = Device.add("worker-1", DeploymentDeviceTypes.SERVER, DeploymentDeviceRoles.WORKER, self.profile)

            # Cleanup auto created association by "sync_cluster_device" to allow migration
            ClusterDevice.objects.all().delete()

            # Mock ConfGen
            mock_cg_instance = Mock()
            mock_cg_instance.get_root_server.return_value = "mg-node-1"
            mock_confgen.return_value = mock_cg_instance

            # Run migration first time
            migration_0007.associate_all_devices_with_cluster(self.get_django_apps(), None)
            first_count = ClusterDevice.objects.count()

            # Run migration second time
            migration_0007.associate_all_devices_with_cluster(self.get_django_apps(), None)
            second_count = ClusterDevice.objects.count()

            # Count should be the same (no duplicates)
            self.assertEqual(first_count, second_count)

            # Verify the devices are still properly associated
            mg_cluster_device = ClusterDevice.objects.get(device=mg_device)
            self.assertTrue(mg_cluster_device.controlplane)

            worker_cluster_device = ClusterDevice.objects.get(device=worker_device)
            self.assertFalse(worker_cluster_device.controlplane)
