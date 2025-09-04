import json
import logging
import os
import shutil
import tempfile

from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from django.utils import timezone

from deployment_manager.cli.cluster.batch import ClusterUpgradeBatchCmd
from deployment_manager.cli.cluster.ceph_sync.ceph_syncer import CephSyncer
from deployment_manager.cli.cluster.ceph_sync.remote_ceph_syncer import RemoteCephSyncer
from deployment_manager.cli.cluster.ceph_sync.single_pod_ceph_syncer import SinglePodCephSyncer
from deployment_manager.cli.cluster.event_logger import UpgradeEventLogger
from deployment_manager.cli.cluster.helpers import (
    clear_cluster_node_and_system_count,
    fetch_ceph_credentials_to_memory,
    get_upgrade_process,
    upgrade_cluster_control_plane_nodes,
    ClusterUpgradeWrapper,
    get_ongoing_upgrade_ids,
    save_cluster_node_and_system_count,
    remove_thanos_config,
    _security_patch_servers,
)
from deployment_manager.lib.health_check import get_all_health_checks
from deployment_manager.cli.cluster.repr import ClusterUpgradeRepr, ClusterUpgradeBatchDeviceRepr
from deployment_manager.cli.migrate_config_and_secret import (label_and_collect_resources, migrate_with_labels)
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.models import (
    Cluster, ClusterUpgrade, DataPreparationStatus, DeploymentDeviceTypes, UpgradeProcessStatus, list_cluster_nodes,
    ClusterUpgradeBatchDevice, UpgradeDeviceStatus, get_device_count_by_cluster
)
from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node

logger = logging.getLogger(__name__)
event_logger = UpgradeEventLogger()

default_thanos_node_port = 30033
cluster_dir = "/opt/cerebras/cluster"
properties_path = f"{cluster_dir}/pkg-properties.yaml"

default_deploy_meta_path = f"/opt/cerebras/cluster-deployment/meta/"
default_network_config_path = f"{cluster_dir}/network_config.json"
default_dm_db_path = "/opt/cerebras/cluster-deployment/dm.db"
default_snapshot_dir = "/tmp/snapshots"

"""Static step names"""
PREPARE_DATA_STEP = "prepare_data"
UPGRADE_SOURCE_CLUSTER_STEP = "upgrade_src_cluster"
UPGRADE_DEST_CLUSTER_STEP = "upgrade_dest_cluster"
END_UPGRADE_STEP = "end_upgrade"

# --- Upgrade Commands (cscfg cluster upgrade ...) ---
class ClusterUpgradeSnapshotCmd(SubCommandABC):
    """Take a snapshot of dm.db and network_config.json for disaster recovery."""
    """This is only meant to be run against a testing cluster like perfdrop, not a production one."""

    name = "snapshot"

    def construct(self):
        self.parser.add_argument(
            "--upgrade-id",
            required=False,
            type=int,
            help="ID of the upgrade process (used to name the snapshot folder)."
        )
        self.parser.add_argument(
            "--db-path",
            required=False,
            type=str,
            default=default_dm_db_path,
            help="Path to dm.db"
        )
        self.parser.add_argument(
            "--meta-path",
            required=False,
            type=str,
            default=default_deploy_meta_path,
            help="Path to deployment meta directory"
        )
        self.parser.add_argument(
            "--config-path",
            required=False,
            type=str,
            default=default_network_config_path,
            help="Path to network_config.json"
        )
        self.parser.add_argument(
            "--snapshot-dir",
            required=False,
            type=str,
            default=default_snapshot_dir,
            help="Directory to store snapshots"
        )

    @staticmethod
    def take_snapshot(profile, upgrade_id, db_path, meta_path, config_path, snapshot_dir, logger=logger):
        logger.info(f"Creating cluster upgrade snapshot (upgrade_id={upgrade_id})")

        upgrade_wrapper, error_code = get_upgrade_process(upgrade_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        # Validate current status - should be NOT_STARTED
        if upgrade_process.status != UpgradeProcessStatus.NOT_STARTED.value:
            logger.error(f"Upgrade process with ID {upgrade_process.id} is in status '{upgrade_process.status}', expected NOT_STARTED.")
            return 1

        # Prepare destination directory
        upgrade_id_val = upgrade_process.id
        snapshot_dir_path = os.path.join(snapshot_dir, str(upgrade_id_val))

        # If snapshot directory exists, prompt before overwriting
        if os.path.exists(snapshot_dir_path) and os.listdir(snapshot_dir_path):
            print(f"Snapshot already exists at {snapshot_dir_path}.")
            confirm = input("Overwrite existing snapshot? [y/N]: ").strip().lower()
            if confirm != "y":
                print("Aborted. Snapshot was not overwritten.")
                logger.info("User declined to overwrite existing snapshot.")
                return 0
            else:
                logger.info("User chose to overwrite existing snapshot.")
                try:
                    shutil.rmtree(snapshot_dir_path)
                except Exception as e:
                    logger.error(f"Failed to remove existing snapshot dir: {e}")
                    return 1

        os.makedirs(snapshot_dir_path, exist_ok=True)
        db_dst = os.path.join(snapshot_dir_path, "dm.db")
        meta_dst = os.path.join(snapshot_dir_path, "meta")

        shutil.copy(db_path, db_dst)
        logger.info(f"dm.db backed up to {db_dst}")
        shutil.copytree(meta_path, meta_dst, dirs_exist_ok=True)
        logger.info(f"meta backed up to {meta_dst}")

        try:
            source_cluster = upgrade_process.source_cluster
            source_node = get_k8s_lead_mgmt_node(profile, source_cluster.name)
            source_user = source_node.get_prop(props.prop_management_credentials_user)
            source_password = source_node.get_prop(props.prop_management_credentials_password)

            with KubernetesCtl(profile, source_node.name, source_user, source_password) as source_kctl:
                remote_snapshot_dir = os.path.join(snapshot_dir, str(upgrade_id_val))
                source_kctl.run(f"mkdir -p {remote_snapshot_dir}")
                yaml_dst = os.path.join(remote_snapshot_dir, "cluster-job-operator-cm.yaml")

                remote_config_dst = os.path.join(remote_snapshot_dir, "network_config.json")
                source_kctl.run(f"cp {config_path} {remote_config_dst}")

                source_kctl.run(f"kubectl get -n job-operator cm cluster -o yaml > {yaml_dst}")
                logger.info(f"ConfigMap 'cluster' YAML saved remotely to {source_node.name}:{yaml_dst}")

        except Exception as e:
            logger.error(f"Encountered an error while taking a snapshot: {e}")
            raise e

    def run(self, args):
        self.take_snapshot(self.profile, args.upgrade_id, args.db_path, args.meta_path, args.config_path, args.snapshot_dir)


class ClusterUpgradeCreateCmd(SubCommandABC):
    """Start a new cluster upgrade process."""
    name = "create"

    def construct(self):
        self.parser.add_argument(
            "--source",
            required=True,
            type=str,
            help="Name of the source (current) cluster."
        )
        self.parser.add_argument(
            "--dest",
            required=True,
            type=str,
            help="Name of the destination (target) cluster for the upgrade."
        )
        self.parser.add_argument(
            "--upgrade-pkg-path",
            required=True,
            type=str,
            help="Path to the upgrade package."
        )
        self.parser.add_argument(
            "--skip-thanos-update",
            default=False,
            action='store_true',
            help="Skip thanos update if you don't want to allow src cluster to access dest cluster's prometheus db."
        )
        self.parser.add_argument(
            "--skip-pkg-copy",
            default=False,
            action='store_true',
            help="Skip copying cluster packages to both source and destination clusters."
        )
        self.add_arg_output_format()

    def copy_cluster_package_to_one_cluster(self, cluster, upgrade_wrapper: ClusterUpgradeWrapper):
        """
        Copy the cluster package to a given cluster.
        """
        node = get_k8s_lead_mgmt_node(self.profile, cluster.name)
        user = node.get_prop(props.prop_management_credentials_user)
        password = node.get_prop(props.prop_management_credentials_password)
        upgrade_process = upgrade_wrapper.upgrade_process

        with KubernetesCtl(self.profile, node.name, user, password) as kctl:
            logger.info(f"Copying cluster package to {node.name}:{upgrade_wrapper.cluster_pkg_tarball}")
            kctl.run(f"mkdir -p {upgrade_process.upgrade_pkg_path}")
            # We first check whether there are sufficient disk space on the root partition for the
            # cluster. If the root partition has less than 80GB free space and SKIP_DISK_SPACE_CHECK
            # is not set, we raise an error.
            ret, stdout, stderr = kctl.run(
                f"df -h / | awk 'NR==2 {{print $4}}' | sed 's/G//g'", raise_exc=False
            )
            if ret != 0:
                raise RuntimeError(f"Can't retrieve free space on {node.name}: {stderr}")
            free_space = int(stdout.strip())

            skip_disk_space_check = os.environ.get("SKIP_DISK_SPACE_CHECK", False)

            if free_space < 80:
                if not skip_disk_space_check:
                    raise RuntimeError(
                        f"Insufficient disk space on {node.name}. "
                        f"At least 80GB free space is required, but only {free_space}GB is available."
                    )
                else:
                    logger.warning(
                        f"Skipping disk space check on {node.name}. "
                        f"Proceeding with {free_space}GB free space."
                    )

            # We always copy over the package for now. We can optimize this later if needed.
            kctl.copy_file(upgrade_wrapper.cluster_pkg_tarball, upgrade_wrapper.cluster_pkg_tarball)
            logger.info(f"Copied cluster package to {node.name}:{upgrade_wrapper.cluster_pkg_tarball}")
            kctl.run(f"tar xfz {upgrade_wrapper.cluster_pkg_tarball} -C {upgrade_process.upgrade_pkg_path}", raise_exc=True)
            logger.info(f"Untar'ed cluster package to {node.name}:{upgrade_wrapper.cluster_pkg_path}")
            # remove the tarball after extraction to save space
            kctl.run(f"rm -f {upgrade_wrapper.cluster_pkg_tarball}", raise_exc=True)

    def copy_cluster_package(self, upgrade_wrapper: ClusterUpgradeWrapper):
        """
        Copy the cluster package to both source and destination clusters.
        This is a prerequisite for the upgrade process.
        """
        skip_src_cluster = os.environ.get("SKIP_SRC_CLUSTER", False)
        skip_dest_cluster = os.environ.get("SKIP_DEST_CLUSTER", False)
        upgrade_process = upgrade_wrapper.upgrade_process

        try:
            if not skip_src_cluster:
                source_cluster = upgrade_process.source_cluster
                logger.info(f"Copying cluster package to source cluster: {source_cluster.name}")
                self.copy_cluster_package_to_one_cluster(source_cluster, upgrade_wrapper)

            if not skip_dest_cluster:
                dest_cluster = upgrade_process.dest_cluster
                logger.info(f"Copying cluster package to destination cluster: {dest_cluster.name}")
                self.copy_cluster_package_to_one_cluster(dest_cluster, upgrade_wrapper)
        except Exception as e:
            logger.error(f"Encountered an error while copying cluster package: {e}")
            raise e

    def add_additional_ingress(self, upgrade_process):
        """
        Add additional ingress property to both source and destination cluster's pkg-properties.yaml.

        Args:
            upgrade_process: The ClusterUpgrade model instance
        """
        skip_src_cluster = os.environ.get("SKIP_SRC_CLUSTER", False)
        skip_dest_cluster = os.environ.get("SKIP_DEST_CLUSTER", False)

        try:
            if not skip_src_cluster:
                source_cluster = upgrade_process.source_cluster
                source_node = get_k8s_lead_mgmt_node(self.profile, source_cluster.name)
                source_user = source_node.get_prop(props.prop_management_credentials_user)
                source_password = source_node.get_prop(props.prop_management_credentials_password)
                with KubernetesCtl(self.profile, source_node.name, source_user, source_password) as kctl:
                    kctl.run(
                        f"yq -i '.properties.clusterMgmt.additionalIngress |= (. // "
                        f"\"{upgrade_process.source_cluster.name}-blue\")' {properties_path}"
                    )
                    logger.info(
                        f"Added additional ingress property "
                        f"({upgrade_process.source_cluster.name}-blue) to {properties_path} in source cluster {source_cluster.name}"
                    )

            if not skip_dest_cluster:
                dest_cluster = upgrade_process.dest_cluster
                dest_node = get_k8s_lead_mgmt_node(self.profile, dest_cluster.name)
                dest_user = dest_node.get_prop(props.prop_management_credentials_user)
                dest_password = dest_node.get_prop(props.prop_management_credentials_password)

                with KubernetesCtl(self.profile, dest_node.name, dest_user, dest_password) as kctl:
                    kctl.run(
                        f"yq -i '.properties.clusterMgmt.additionalIngress |= (. // "
                        f"\"{upgrade_process.source_cluster.name}\")' {properties_path}"
                    )
                    logger.info(
                        f"Added additional ingress property "
                        f"({upgrade_process.source_cluster.name}) to {properties_path} in destination cluster {dest_cluster.name}"
                    )
        except Exception as e:
            logger.error(f"Encountered an error while adding additional ingress: {e}")
            raise e

    def update_thanos_config(self, upgrade_process):
        """
        Deploy a Thanos query GRPC service to the source cluster + update props in dest cluster.

        Args:
            upgrade_process: The ClusterUpgrade model instance
        """
        if self.skip_thanos_update:
            return
        try:
            yaml_content = f"""apiVersion: v1
kind: Namespace
metadata:
  name: prometheus
---
apiVersion: v1
kind: Service
metadata:
  name: thanos-query-grpc-external
  namespace: prometheus
spec:
  ports:
  - name: grpc
    nodePort: {default_thanos_node_port}
    port: 10901
    protocol: TCP
    targetPort: grpc
  selector:
    app.kubernetes.io/component: query
    app.kubernetes.io/instance: thanos
    app.kubernetes.io/name: thanos
  type: NodePort"""

            skip_src_cluster = os.environ.get("SKIP_SRC_CLUSTER", False)
            skip_dest_cluster = os.environ.get("SKIP_DEST_CLUSTER", False)
            source_ip = ""
            if not skip_src_cluster:
                source_cluster = upgrade_process.source_cluster
                source_node = get_k8s_lead_mgmt_node(self.profile, source_cluster.name)
                source_user = source_node.get_prop(props.prop_management_credentials_user)
                source_password = source_node.get_prop(props.prop_management_credentials_password)
                with KubernetesCtl(self.profile, source_node.name, source_user, source_password) as kctl:
                    # Create a temporary file with the YAML content
                    temp_file = tempfile.mktemp()
                    kctl.run(f"cat > {temp_file} << 'EOF'\n{yaml_content}\nEOF")
                    # Apply the YAML to create or update the service
                    kctl.run(f"kubectl apply -f {temp_file}")
                    logger.info(f"Deployed Thanos query GRPC service to {source_cluster.name}")
                    # Clean up the temporary file
                    kctl.run(f"rm -f {temp_file}")
                    _, source_ip, _ = kctl.run("hostname -I | awk '{print $1}'")
                    source_ip = source_ip.strip()
                logger.info(f"Successfully deployed Thanos query GRPC service to source cluster: {source_cluster.name}")

            # Update pkg-properties.yaml with Thanos extraStore property
            if not skip_dest_cluster:
                dest_cluster = upgrade_process.dest_cluster
                dest_node = get_k8s_lead_mgmt_node(self.profile, dest_cluster.name)
                dest_user = dest_node.get_prop(props.prop_management_credentials_user)
                dest_password = dest_node.get_prop(props.prop_management_credentials_password)
                with KubernetesCtl(self.profile, dest_node.name, dest_user, dest_password) as kctl:
                    kctl.run(
                        f"mkdir -p {cluster_dir}; touch {properties_path}"
                    )
                    kctl.run(
                        f"yq -i '.properties.prometheus.thanos.extraStore = "
                        f"\"{source_ip}:{default_thanos_node_port}\"' {properties_path}"
                    )
                logger.info(
                    f"Added Thanos extraStore property "
                    f"({source_ip}:{default_thanos_node_port}) to {properties_path}"
                )
        except Exception as e:
            logger.error(f"Encountered an error during Thanos query GRPC service deployment: {e}")
            raise e

    def run(self, args):
        self.skip_thanos_update = args.skip_thanos_update
        if self.skip_thanos_update:
            logger.info(f"skip_thanos_update")
        db_update_only = os.environ.get("DB_UPDATE_ONLY", False)
        if db_update_only:
            logger.info(f"DB_UPDATE_ONLY")

        logger.info(f"Executing ClusterUpgradeCreateCmd with ")
        logger.info(f"  Source Cluster: {args.source}")
        logger.info(f"  Destination/Target: {args.dest}")
        logger.info(f"  Package Path: {args.upgrade_pkg_path}")

        try:
            # Validate source cluster from DB
            try:
                source_cluster = Cluster.objects.get(profile__name=self.profile, name=args.source)
            except Cluster.DoesNotExist:
                logger.error(f"Source cluster '{args.source}' not found in profile '{self.profile}'.")
                return 1

            # Validate destination cluster from DB
            try:
                dest_cluster = Cluster.objects.get(profile__name=self.profile, name=args.dest)
            except Cluster.DoesNotExist:
                logger.error(f"Destination cluster '{args.dest}' not found in profile '{self.profile}'.")
                return 1

            if not args.upgrade_pkg_path:
                logger.error("Upgrade package path is required.")
                return 1

            # Check if there are any ongoing upgrade processes
            ongoing_upgrades = get_ongoing_upgrade_ids()
            if ongoing_upgrades:
                print("The following upgrade process IDs are still active:")
                for upgrade_id in ongoing_upgrades:
                    print(f" - Upgrade ID: {upgrade_id}")
                print("Please `cscfg cluster upgrade end/cancel --upgrade-id <id>` for these upgrades .")
                return 1

            # We don't invalidate existing upgrade pairs
            # because there is legitimate cause for starting/stopping/re-starting an upgrade.

            # Create the new record
            new_upgrade = ClusterUpgrade(
                source_cluster=source_cluster,
                dest_cluster=dest_cluster,
                upgrade_pkg_path=args.upgrade_pkg_path,
                status=UpgradeProcessStatus.NOT_STARTED.value,
                created_at=timezone.now(),
            )
            new_upgrade.save()
            logger.info(f"Successfully created new upgrade {new_upgrade.id}")
            print(ClusterUpgradeRepr.format_reprs([ClusterUpgradeRepr.from_orm(new_upgrade)], 'table'))

            if not db_update_only:
                logger.info(f"Copying the cluster package to both source and destination clusters")
                if not args.skip_pkg_copy:
                    upgrade_wrapper = ClusterUpgradeWrapper(new_upgrade)
                    self.copy_cluster_package(upgrade_wrapper)

                self.update_thanos_config(new_upgrade)
                self.add_additional_ingress(new_upgrade)

            event_logger.set_upgrade_id(db_update_only, new_upgrade.id)
            event_logger.upgrade_start(db_update_only)

            return 0
        except Exception as e:
            logger.error(f"Encountered an error during upgrade creation: {e}")
            logger.exception("Failed during upgrade creation.")
            return 1


class ClusterUpgradeCancelCmd(SubCommandABC):
    """Cancel a cluster upgrade process."""
    name = "cancel"

    def construct(self):
        self.parser.add_argument(
            "--upgrade-id",
            required=True,
            type=int,
            help="ID of the upgrade process to cancel."
        )
        self.parser.add_argument(
            "--skip-thanos-update",
            default=False,
            action='store_true',
            help="Skip thanos update if you don't want to remove dest cluster's access to src cluster's prometheus db."
        )
        self.parser.add_argument(
            "--force", action="store_true", required=False,
            help="force flag to forcefully mark the upgrade as cancelled"
        )

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeCancelCmd with upgrade-id={args.upgrade_id}")
        upgrade_wrapper, error_code = get_upgrade_process(args.upgrade_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process
        self.db_update_only = os.environ.get("DB_UPDATE_ONLY", False)

        if not args.force:
            if upgrade_process.status in (
                    UpgradeProcessStatus.COMPLETED.value,
                    UpgradeProcessStatus.CANCELLED.value):
                logger.info(
                    f"Upgrade process with ID {upgrade_process.id} is already in terminal status: {upgrade_process.status}"
                )
                return 0
            elif upgrade_process.status == UpgradeProcessStatus.NOT_STARTED.value:
                logger.info(
                    f"Cancelling upgrade process with ID {upgrade_process.id} with status: {upgrade_process.status}"
                )
            else:
                print(f"Upgrade process {upgrade_process.id} is currently in status '{upgrade_process.status}'.")
                print(f"Please consider cleaning up the ongoing upgrade before cancellation as cancellation only updates the metadata.")
                confirm = input("Are you sure you want to cancel this upgrade? [y/N]: ").strip().lower()
                if confirm != "y":
                    print("Aborted. Upgrade was not cancelled.")
                    logger.info("User declined to cancel upgrade.")
                    return 0
                logger.info(f"User confirmed cancellation of upgrade process wiht id {upgrade_process.id}.")

        try:
            if not self.db_update_only and not args.skip_thanos_update:
                # Remove thanos config on cancel as well
                skip_src_cluster = os.environ.get("SKIP_SRC_CLUSTER", False)
                skip_dest_cluster = os.environ.get("SKIP_DEST_CLUSTER", False)
                remove_thanos_config(
                    profile=self.profile,
                    upgrade_process=upgrade_process,
                    properties_path=properties_path,
                    logger=logger,
                    skip_src_cluster=skip_src_cluster,
                    skip_dest_cluster=skip_dest_cluster,
                )

            upgrade_process.status = UpgradeProcessStatus.CANCELLED.value
            upgrade_process.save(update_fields=["status"])
            logger.info(f"Upgrade process {upgrade_process.id} has been successfully cancelled.")
            return 0
        except Exception as e:
            logger.error(f"Failed to cancel upgrade process: {e}")
            logger.exception("Failed during upgrade cancellation.")
            return 1


class UpgradeSecurityPatchUsernodes(SubCommandABC):
    """Security patch a user-provided list of server nodes for the active upgrade."""
    name = "security_patch_usernodes"

    def construct(self):
        self.parser.add_argument(
            "--upgrade-id",
            required=False,
            type=int,
            help="ID of the upgrade process (if omitted, uses the active upgrade).",
        )
        self.parser.add_argument(
            "--servers",
            required=True,
            nargs='+',
            help="Space-separated list of server node names to security patch.",
        )

    def run(self, args):
        logger.info(f"Executing UpgradeSecuirtyPatchUsernodes with args: {args}")

        upgrade_wrapper, error_code = get_upgrade_process(args.upgrade_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        node_names = args.servers
        patch_dir = upgrade_process.upgrade_pkg_path

        try:
            successful_nodes, failed_nodes, unreachable_nodes, unknown_nodes = _security_patch_servers(
                profile=self.profile,
                dnode=None,
                node_names=node_names,
                patch_dir=patch_dir,
            )
        except Exception as e:
            logger.error(f"Security patching invocation failed: {e}")
            return 1

        failed_device_names = failed_nodes + unreachable_nodes + unknown_nodes
        if failed_device_names:
            logger.error(
                "Security patching failed for %d nodes: %s",
                len(failed_device_names),
                ", ".join(failed_device_names),
            )
            return 1

        logger.info(
            "Security patching succeeded for all %d nodes: %s",
            len(successful_nodes),
            ", ".join(successful_nodes) if successful_nodes else "",
        )
        return 0

class ClusterUpgradeDestClusterCmd(SubCommandABC):
    """Upgrade the control plane of the destination cluster."""
    name = "upgrade_dest_cluster"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Upgrade the destination cluster."
        self.db_update_only = os.environ.get("DB_UPDATE_ONLY", False)
        if self.db_update_only:
            logger.info("db_update_only set")

    def construct(self):
        self.parser.add_argument(
            "--upgrade-id",
            required=False,
            type=int,
            help="ID of the upgrade process."
        )
        self.parser.add_argument(
            "--skip-security-patch",
            action="store_true",
            help="Skip security patching of the control plane nodes destination cluster."
        )
        self.parser.add_argument(
            "--force-new-install-version",
            required=False,
            type=str,
            help = "Provide a specific Kubernetes install version to force (e.g., 1.32.6). "
                "Requires that the network JSON and cluster YAML files are available."
        )
        self.parser.add_argument(
            "--force", action="store_true", required=False,
            help="force flag to skip phase check)"
        )

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeDestClusterCmd with args: {args}")
        upgrade_wrapper, error_code = get_upgrade_process(args.upgrade_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        try:
            if not args.force:
                # Validate the current status of the upgrade process
                if upgrade_process.status != UpgradeProcessStatus.NOT_STARTED.value:
                    logger.error(
                        f"Upgrade process with ID '{upgrade_process.id}' is not in expected status. "
                        f"Expected: '{UpgradeProcessStatus.NOT_STARTED.value}' Current: '{upgrade_process.status}'"
                    )
                    return 1

            event_logger.set_upgrade_id(self.db_update_only, upgrade_process.id)
            event_logger.step_start(self.db_update_only, UPGRADE_DEST_CLUSTER_STEP)

            if not self.db_update_only:
                # Get the number of nodes in the source cluster
                device_count = get_device_count_by_cluster(
                    profile=self.profile,
                    cluster_name=upgrade_process.source_cluster.name,
                    device_type=DeploymentDeviceTypes.SERVER.value)

                system_count = get_device_count_by_cluster(
                    profile=self.profile,
                    cluster_name=upgrade_process.source_cluster.name,
                    device_type=DeploymentDeviceTypes.SYSTEM.value)

                # Update pkg-properties on destination cluster with number of nodes in source cluster
                success, error_msg = save_cluster_node_and_system_count(
                    profile=self.profile,
                    cluster_name=upgrade_process.dest_cluster.name,
                    node_count=device_count,
                    system_count=system_count)
                if not success:
                    # Log the error and continue, the cluster upgrade can still proceed
                    logger.error(error_msg)

                cluster_nodes = list_cluster_nodes(profile=self.profile, cluster_name=upgrade_process.dest_cluster.name)
                if len(cluster_nodes) == 0:
                    logger.error(
                        f"No c management nodes found in destination cluster '{upgrade_process.dest_cluster.name}'. "
                        "Cannot proceed with upgrade."
                    )
                    return 1

                success, err = upgrade_cluster_control_plane_nodes(
                    profile=self.profile,
                    cluster_name=upgrade_process.dest_cluster.name,
                    cluster_pkg_path=upgrade_wrapper.cluster_pkg_path,
                    security_patch_path=upgrade_process.upgrade_pkg_path,
                    security_patch_servers=cluster_nodes if not args.skip_security_patch else [],
                    force_new_install_version=args.force_new_install_version
                )
                if not success:
                    logger.error(err)
                    event_logger.step_end(self.db_update_only, UPGRADE_DEST_CLUSTER_STEP, False, err)
                    return 1

            # Update ClusterUpgrade.status to DEST_CLUSTER_UPGRADED
            upgrade_process.status = UpgradeProcessStatus.DEST_CLUSTER_UPGRADED.value
            upgrade_process.save()
            event_logger.step_end(self.db_update_only, UPGRADE_DEST_CLUSTER_STEP, True)
            logger.info(f"Successfully completed destination cluster upgrade for upgrade process {upgrade_process.id}")
            return 0

        except Exception as e:
            logger.error(f"Encountered an error during upgrade destination cluster: {e}")
            logger.exception("Failed during upgrade destination cluster")
            event_logger.step_end(self.db_update_only, UPGRADE_DEST_CLUSTER_STEP, False, str(e))
            upgrade_process.status = UpgradeProcessStatus.FAILED.value
            upgrade_process.save()
            return 1


class ClusterUpgradePrepareDataCmd(SubCommandABC):
    """Prepare and sync data to the destination cluster."""
    name = "prepare_data"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Prepare and sync data to the destination cluster."
        self.db_update_only = os.environ.get("DB_UPDATE_ONLY", False)
        if self.db_update_only:
            logger.info("db_update_only set")

    def construct(self):
        self.parser.add_argument(
            "--upgrade-id",
            required=False,
            type=int,
            help="ID of the upgrade process."
        )
        self.parser.add_argument(
            "--session",
            required=False,
            type=str,
            help="session of the data."
        )
        self.parser.add_argument(
            "--ceph-src-config",
            required=False,
            type=str,
            help="Directory containing ceph conf and keyring for source cluster."
        )
        self.parser.add_argument(
            "--ceph-dst-config",
            required=False,
            type=str,
            help="Directory containing ceph conf and keyring for destination cluster."
        )

    def run(self, args):
        logger.info(f"Executing ClusterUpgradePrepareDataCmd with args: {args}")

        k8s_metadata_namespace = args.session or "prometheus"

        upgrade_wrapper, error_code = get_upgrade_process(args.upgrade_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        # Note: we want to allow this command to be run ad-hoc during the upgrade process
        # so we do not validate the current status

        try:
            # Take snapshot before data preparation, unless db_update_only
            if not self.db_update_only:
                try:
                    logger.info(f"Taking snapshot before data preparation for upgrade_id={args.upgrade_id}")
                    ClusterUpgradeSnapshotCmd.take_snapshot(
                        self.profile,
                        args.upgrade_id,
                        default_dm_db_path,
                        default_deploy_meta_path,
                        default_network_config_path,
                        default_snapshot_dir
                    )
                    logger.info("Snapshot taken successfully before data preparation.")
                except Exception as e:
                    logger.error(f"Failed to take snapshot before data preparation: {e}")

            # Update status to IN_PROGRESS
            upgrade_process.data_preparation_status = DataPreparationStatus.IN_PROGRESS.value
            upgrade_process.save()
            logger.info(f"Data preparation started for upgrade process {upgrade_process.id}")

            event_logger.upgrade_start(self.db_update_only)
            event_logger.set_upgrade_id(self.db_update_only, upgrade_process.id)
            event_logger.step_start(self.db_update_only, PREPARE_DATA_STEP)

            if not self.db_update_only:
                self.migrate_k8s_metadata(upgrade_process, k8s_metadata_namespace)

                # Locate the helper scripts relative to this file:
                base_dir = os.path.dirname(__file__)  # .../cli/cluster
                ceph_sync_dir = os.path.join(base_dir, "ceph_sync")

                mount_script = os.path.join(ceph_sync_dir, "mount_ceph.sh")
                sync_script = os.path.join(ceph_sync_dir, "sync_volume.sh")
                unmount_script = os.path.join(ceph_sync_dir, "unmount_ceph.sh")

                # 1. Basic sanity checks:
                if not os.path.isfile(mount_script) or not os.access(mount_script, os.X_OK):
                    logger.error(f"Cannot find or execute: {mount_script}")
                    raise RuntimeError("Missing mount_ceph.sh")
                if not os.path.isfile(sync_script) or not os.access(sync_script, os.X_OK):
                    logger.error(f"Cannot find or execute: {sync_script}")
                    raise RuntimeError("Missing sync_volume.sh")
                if not os.path.isfile(unmount_script) or not os.access(unmount_script, os.X_OK):
                    logger.warning(f"Cannot find or execute: {unmount_script} - will use fallback unmount")
                    unmount_script = None  # Will use fallback in CephSyncer

                # 2. Hard-coded list of (group, subvolume)
                volumes_to_sync = [
                    ("cached-compile-group", "cached-compile-subvolume"),
                    ("debug-artifact-group", "debug-artifact-subvolume"),
                    # ("log-export-group", "log-export-subvolume"),
                    # TODO(agam): Re-enable the above, or add other volumes to this list, if needed
                ]

                # 3. Get credentials
                if args.ceph_src_config and args.ceph_dst_config:
                    # Use user-provided credential directories
                    src_conf = os.path.join(args.ceph_src_config, "ceph.conf")
                    src_keyring = os.path.join(args.ceph_src_config, "keyring")
                    dst_conf = os.path.join(args.ceph_dst_config, "ceph.conf")
                    dst_keyring = os.path.join(args.ceph_dst_config, "keyring")

                    logger.info("Using src/dst config+keyring from supplied args")

                    # Run syncer with user-provided paths (no special cleanup needed)
                    cleanup_mounts = not os.environ.get("KEEP_CEPH_MOUNTS", False)
                    if not cleanup_mounts:
                        logger.info("KEEP_CEPH_MOUNTS set - will not unmount filesystems after sync")

                    syncer = CephSyncer(
                        mount_script=mount_script,
                        sync_script=sync_script,
                        src_conf=src_conf,
                        src_keyring=src_keyring,
                        dst_conf=dst_conf,
                        dst_keyring=dst_keyring,
                        volumes=volumes_to_sync,
                        unmount_script=unmount_script,
                        cleanup_mounts=cleanup_mounts,
                    )

                    logger.info("Starting CephFS synchronization...")
                    syncer.run()
                    logger.info("CephFS synchronization completed successfully.")
                else:
                    # Auto-fetch credentials to memory and use temporary files
                    source_cluster_name = upgrade_process.source_cluster.name
                    dest_cluster_name = upgrade_process.dest_cluster.name

                    logger.info("Auto-fetching src/dst config+keyring to memory")

                    # Fetch credentials to memory
                    src_conf_content, src_keyring_content = fetch_ceph_credentials_to_memory(self.profile, source_cluster_name)
                    dst_conf_content, dst_keyring_content = fetch_ceph_credentials_to_memory(self.profile, dest_cluster_name)

                    logger.info("Fetched credentials to memory, selecting syncer approach")

                    cleanup_mounts = not os.environ.get("KEEP_CEPH_MOUNTS", False)
                    if not cleanup_mounts:
                        logger.info("KEEP_CEPH_MOUNTS set - will not unmount filesystems after sync")

                    # Choose syncer based on environment variable
                    # Default: RemoteCephSyncer (node-based, simpler)
                    # Fallback: Set USE_POD_CEPH_SYNCER=true for SinglePodCephSyncer (pod-based, more reliable)
                    use_pod_syncer = os.environ.get("USE_POD_CEPH_SYNCER", "").lower() in ("true", "1", "yes")

                    if use_pod_syncer:
                        # Use SinglePodCephSyncer - runs sync from a pod on the source cluster
                        logger.info("Using SinglePodCephSyncer (USE_POD_CEPH_SYNCER=true) - will run a pod on source cluster to mount both CephFS filesystems")
                        syncer = SinglePodCephSyncer(
                            profile=self.profile,
                            src_cluster=upgrade_process.source_cluster,
                            dst_cluster=upgrade_process.dest_cluster,
                            src_conf_content=src_conf_content,
                            src_keyring_content=src_keyring_content,
                            dst_conf_content=dst_conf_content,
                            dst_keyring_content=dst_keyring_content,
                            volumes=volumes_to_sync,
                            cleanup_mounts=cleanup_mounts,
                        )
                        logger.info("Starting single pod CephFS synchronization on source cluster...")
                        syncer.run()
                        logger.info("Single pod CephFS synchronization completed successfully.")
                    else:
                        # Use RemoteCephSyncer - runs sync from source management node (default)
                        logger.info("Using RemoteCephSyncer (default) - will run sync from source management node")
                        syncer = RemoteCephSyncer(
                            profile=self.profile,
                            src_cluster=upgrade_process.source_cluster,
                            dst_cluster=upgrade_process.dest_cluster,
                            cluster_pkg_path=upgrade_wrapper.cluster_pkg_path,
                            src_conf_content=src_conf_content,
                            src_keyring_content=src_keyring_content,
                            dst_conf_content=dst_conf_content,
                            dst_keyring_content=dst_keyring_content,
                            volumes=volumes_to_sync,
                            cleanup_mounts=cleanup_mounts,
                        )
                        logger.info("Starting remote CephFS synchronization from source management node...")
                        syncer.run()
                        logger.info("Remote CephFS synchronization completed successfully.")

                # Prepare/sync registry images.
                logger.info("Simulating preparation/sync of registry images...")

                # Prepare/sync K8s state (etcd snapshots, Velero backups, etc.)
                logger.info("Simulating preparation/sync of K8s state...")
            else:
                logger.info("DB_UPDATE_ONLY mode: Skipping actual data preparation operations")

            # Mark as completed
            upgrade_process.data_preparation_status = DataPreparationStatus.COMPLETED.value
            upgrade_process.save()
            logger.info(f"Successfully completed data preparation for upgrade process {upgrade_process.id}")
            event_logger.step_end(self.db_update_only, PREPARE_DATA_STEP, True)
            return 0

        except Exception as e:
            logger.error(f"Encountered an error during data preparation: {e}")
            event_logger.step_end(self.db_update_only, PREPARE_DATA_STEP, False, error_message=str(e))
            logger.exception("Failed during data preparation.")
            # Mark data preparation as failed
            upgrade_process.data_preparation_status = DataPreparationStatus.FAILED.value
            upgrade_process.save()
            return 1

    def migrate_k8s_metadata(self, upgrade_process, namespace):
        try:
            source_cluster = upgrade_process.source_cluster
            source_node = get_k8s_lead_mgmt_node(self.profile, source_cluster.name)
            source_user = source_node.get_prop(props.prop_management_credentials_user)
            source_password = source_node.get_prop(props.prop_management_credentials_password)

            dest_cluster = upgrade_process.dest_cluster
            dest_node = get_k8s_lead_mgmt_node(self.profile, dest_cluster.name)
            dest_user = dest_node.get_prop(props.prop_management_credentials_user)
            dest_password = dest_node.get_prop(props.prop_management_credentials_password)

            config_to_migrate = ['alert-classification']
            secret_to_migrate = ['cluster-operator-contacts']
            label_key = 'bg_migration'

            with KubernetesCtl(self.profile, source_node.name, source_user, source_password) as source_kctl, \
                KubernetesCtl(self.profile, dest_node.name, dest_user, dest_password) as dest_kctl:
                logger.info(f"Applying label {label_key}:{upgrade_process.id} to config and secret on {source_cluster.name}.")
                label_and_collect_resources(source_kctl, config_to_migrate, secret_to_migrate, namespace, label_key, upgrade_process.id, logger)
                logger.info(f"Migrating config and secret w/label {label_key}:{upgrade_process.id} to {dest_cluster.name}.")
                migrate_with_labels(source_kctl, dest_kctl, label_key, upgrade_process.id, namespace, logger)

        except Exception as e:
            logger.error(f"Encountered an error during k8s data migration: {e}")
            raise e


class ClusterUpgradeShowCmd(SubCommandABC):
    """Show current and past cluster upgrade processes."""
    name = "show"

    def construct(self):
        self.add_arg_output_format()
        self.parser.add_argument(
            "--upgrade-id",
            required=False,
            type=int,
            help="ID of a specific upgrade process to show. Shows all if omitted. If not provided, --devices/--error-only will use the active upgrade."
        )
        self.parser.add_argument(
            "--devices",
            action="store_true",
            help="Show all devices across all batches for the upgrade process (uses active upgrade if --upgrade-id not provided)"
        )
        self.parser.add_argument(
            "-e", "--error-only",
            action="store_true",
            help="Show only failed devices (use with --devices)"
        )

    def run(self, args):
        logger.debug(f"Executing ClusterUpgradeShowCmd with args: {args}")

        # Show upgrades table first
        return_code = self._show_upgrades(args)
        if return_code != 0:
            logger.error("Failed to show upgrade processes.")
            return return_code

        # Handle device display if requested
        if args.devices:
            # Get the upgrade process (by id or implicit)
            upgrade_wrapper, error_code = get_upgrade_process(args.upgrade_id, select_for_update=False)
            if error_code:
                return error_code
            upgrade_process = upgrade_wrapper.upgrade_process
            print("\n=== Device Upgrade Status Table ===\n")
            self._show_devices(args, upgrade_process)
        elif args.error_only:
            logger.error("--error-only must be used with --devices.")
            return 1
        return return_code

    def _show_devices(self, args, upgrade_process):

        # Get all batch devices for this upgrade process
        batch_devices_qs = ClusterUpgradeBatchDevice.objects.filter(
            upgrade_batch__upgrade_process=upgrade_process
        ).select_related('device')

        if args.error_only:
            batch_devices_qs = batch_devices_qs.filter(status=UpgradeDeviceStatus.FAILED.value)
        batch_devices = list(batch_devices_qs)

        if not batch_devices:
            if args.error_only:
                logger.info(f"No failed devices found for upgrade process {upgrade_process.id}")
            else:
                logger.info(f"No devices found for upgrade process {upgrade_process.id}")
            return 0

        # Use ClusterUpgradeBatchDeviceRepr for output
        reprs = [ClusterUpgradeBatchDeviceRepr.from_orm(bd) for bd in batch_devices]
        print(ClusterUpgradeBatchDeviceRepr.format_reprs(reprs, args.output))
        return 0

    def _show_upgrades(self, args):
        """Show upgrade processes."""
        reprs = []
        query = ClusterUpgrade.objects.select_related('source_cluster', 'dest_cluster').order_by('-created_at')

        if args.upgrade_id:
            try:
                upgrade = query.get(id=args.upgrade_id)
                reprs.append(ClusterUpgradeRepr.from_orm(upgrade))
            except ObjectDoesNotExist:
                logger.error(f"Upgrade process with ID {args.upgrade_id} not found in profile '{self.profile}'.")
                return 1
        else:
            upgrades = query.all()
            if not upgrades:
                logger.info(f"No upgrade processes found for profile '{self.profile}'.")
                return 0
            for upgrade in upgrades:
                reprs.append(ClusterUpgradeRepr.from_orm(upgrade))

        if reprs:
            print(ClusterUpgradeRepr.format_reprs(reprs, args.output))
        return 0

class ClusterUpgradeSrcClusterCmd(SubCommandABC):
    """Upgrade the control plane of the source cluster."""
    name = "upgrade_src_cluster"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Upgrade the source cluster."
        self.db_update_only = os.environ.get("DB_UPDATE_ONLY", False)
        if self.db_update_only:
            logger.info("db_update_only set")

    def construct(self):
        self.parser.add_argument(
            "--upgrade-id",
            required=False,
            type=int,
            help="ID of the upgrade process."
        )
        self.parser.add_argument(
            "--skip-security-patch",
            action="store_true",
            help="Skip security patching of the control plane nodes in source cluster."
        )
        self.parser.add_argument(
            "--force", action="store_true", required=False,
            help="force flag to skip phase check)"
        )

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeSrcClusterCmd with args: {args}")
        upgrade_wrapper, error_code = get_upgrade_process(args.upgrade_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        event_logger.set_upgrade_id(self.db_update_only, upgrade_process.id)
        event_logger.step_start(self.db_update_only, UPGRADE_SOURCE_CLUSTER_STEP)
        try:
            if not args.force:
                # Validate the current status of the upgrade process
                if upgrade_process.status != UpgradeProcessStatus.MIGRATING_BATCHES.value:
                    logger.error(
                        f"Upgrade process with ID '{upgrade_process.id}' is not in expected status. "
                        f"Expected: '{UpgradeProcessStatus.MIGRATING_BATCHES.value}' Current: '{upgrade_process.status}'"
                    )
                    return 1

            event_logger.step_start(self.db_update_only, UPGRADE_SOURCE_CLUSTER_STEP)

            if not self.db_update_only:
                cluster_nodes = list_cluster_nodes(profile=self.profile, cluster_name=upgrade_process.source_cluster.name)
                if len(cluster_nodes) == 0:
                    logger.error(
                        f"No c management nodes found in source cluster '{upgrade_process.source_cluster.name}'. "
                        "Cannot proceed with upgrade."
                    )
                    return 1
                success, err = upgrade_cluster_control_plane_nodes(
                    profile=self.profile,
                    cluster_name=upgrade_process.source_cluster.name,
                    cluster_pkg_path=upgrade_wrapper.cluster_pkg_path,
                    security_patch_path=upgrade_process.upgrade_pkg_path,
                    security_patch_servers=cluster_nodes if not args.skip_security_patch else [],
                )
                if not success:
                    logger.error(err)
                    event_logger.step_end(self.db_update_only, UPGRADE_SOURCE_CLUSTER_STEP, False, err)
                    return 1

            # Update the upgrade process status to SOURCE_CLUSTER_UPGRADED
            upgrade_process.status = UpgradeProcessStatus.SOURCE_CLUSTER_UPGRADED.value
            upgrade_process.save()
            event_logger.step_end(self.db_update_only, UPGRADE_SOURCE_CLUSTER_STEP, True)
            logger.info("Successfully completed source cluster upgrade")
            return 0

        except Exception as e:
            logger.error(f"Encountered an error during upgrade source cluster: {e}")
            logger.exception("Failed during upgrade source cluster")
            upgrade_process.status = UpgradeProcessStatus.FAILED.value
            upgrade_process.save()
            event_logger.step_end(self.db_update_only, UPGRADE_SOURCE_CLUSTER_STEP, False, error_message=str(e))

            return 1


class ClusterUpgradeEndCmd(SubCommandABC):
    """Finalize the upgrade process and mark it as completed."""
    name = "end_upgrade"

    def construct(self):
        self.parser.add_argument(
            "--upgrade-id", required=False, type=int,
            help="ID of the upgrade process (uses active upgrade by default)."
        )
        self.parser.add_argument(
            "--skip-thanos-update",
            default=False,
            action='store_true',
            help="Skip thanos update if you don't want to remove dest cluster's access to src cluster's prometheus db."
        )
        self.parser.add_argument(
            "--force", action="store_true", required=False,
            help="force flag to skip phase check)"
        )

    def update_thanos_config(self, upgrade_process):
        """
        Remove a Thanos query GRPC service in the source cluster + remove extra store in dest cluster.
        """
        if self.skip_thanos_update:
            return
        skip_src_cluster = os.environ.get("SKIP_SRC_CLUSTER", False)
        skip_dest_cluster = os.environ.get("SKIP_DEST_CLUSTER", False)
        remove_thanos_config(
            profile=self.profile,
            upgrade_process=upgrade_process,
            properties_path=properties_path,
            logger=logger,
            skip_src_cluster=skip_src_cluster,
            skip_dest_cluster=skip_dest_cluster,
        )

    def run(self, args):
        self.skip_thanos_update = args.skip_thanos_update
        if self.skip_thanos_update:
            logger.info(f"skip_thanos_update")
        db_update_only = os.environ.get("DB_UPDATE_ONLY", False)
        if db_update_only:
            logger.info(f"db_update_only")
        logger.info(f"Executing ClusterUpgradeEndUpgradeCmd with args: {args}")
        upgrade_wrapper, error_code = get_upgrade_process(args.upgrade_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        if not args.force:
            if upgrade_process.status != UpgradeProcessStatus.SOURCE_CLUSTER_UPGRADED.value:
                logger.error(
                    f"Upgrade process with ID {upgrade_process.id} is not in the expected status "
                    f"{UpgradeProcessStatus.SOURCE_CLUSTER_UPGRADED.value} (current: {upgrade_process.status})."
                )
                return 1

        # Display data preparation status and show warning if needed
        logger.info(f"Data preparation status for upgrade process {upgrade_process.id}: {upgrade_process.data_preparation_status}")

        if upgrade_process.data_preparation_status != DataPreparationStatus.COMPLETED.value:
            logger.warning("WARNING: Data preparation has never been completed successfully for this upgrade!")
            logger.warning("This means no data synchronization (CephFS volumes, registry images, K8s state) has been performed.")
            logger.warning("Consider running 'cluster upgrade prepare_data' before completing the upgrade.")

        event_logger.set_upgrade_id(db_update_only, upgrade_process.id)
        event_logger.step_start(db_update_only, END_UPGRADE_STEP)
        if not db_update_only:
            self.update_thanos_config(upgrade_process)

            # Clear the node counter overrides
            clear_cluster_node_and_system_count(
                profile=self.profile,
                cluster_name=upgrade_process.dest_cluster.name
            )

        try:
            with transaction.atomic():
                source_cluster = upgrade_process.source_cluster
                dest_cluster = upgrade_process.dest_cluster

                # 1. Make source_cluster non-primary
                if source_cluster.is_primary:
                    source_cluster.is_primary = False
                    source_cluster.save(update_fields=['is_primary'])

                # 2. Make all other clusters in dest_cluster's profile non-primary.
                Cluster.objects.filter(
                    profile=dest_cluster.profile,
                    is_primary=True
                ).exclude(pk=dest_cluster.pk).update(is_primary=False)

                # 3. Make dest_cluster primary
                if not dest_cluster.is_primary:
                    dest_cluster.is_primary = True
                    dest_cluster.save(update_fields=['is_primary'])

                # Update the upgrade process status to COMPLETED
                upgrade_process.status = UpgradeProcessStatus.COMPLETED.value
                upgrade_process.save(update_fields=['status'])
        except Exception as e:  # Catch potential exceptions during the transaction
            logger.error(f"Error during cluster primary switch or upgrade completion: {e}")
            event_logger.step_end(db_update_only, END_UPGRADE_STEP, False, error_message=str(e))
            return 1  # Indicate failure

        logger.info(f"Upgrade process with ID {upgrade_process.id} completed successfully.")
        event_logger.step_end(db_update_only, END_UPGRADE_STEP, True)
        event_logger.upgrade_end(db_update_only)
        return 0

class ClusterUpgradeHealthCheckCmd(SubCommandABC):
    """Perform a health check on a cluster."""
    name = "health_check"

    def construct(self):
        self.parser.add_argument(
            "--cluster-name",
            required=True,
            type=str,
            help="Name of the cluster to perform health checks on."
        )

    def run(self, args):
        logger.info(f"Starting health check for cluster '{args.cluster_name}'...")
        all_checks_passed = True

        try:
            cluster = Cluster.objects.get(profile__name=self.profile, name=args.cluster_name)
            logger.info(f"Found cluster '{cluster.name}' in profile '{self.profile}'.")
            mgmt_node = get_k8s_lead_mgmt_node(self.profile, cluster.name)
            logger.info(f"Management node for cluster '{args.cluster_name}': {mgmt_node.name}")
            user = mgmt_node.get_prop(props.prop_management_credentials_user)
            password = mgmt_node.get_prop(props.prop_management_credentials_password)
        except ObjectDoesNotExist:
            logger.error(f"Cluster '{args.cluster_name}' not found in profile '{self.profile}'.")
            return 1
        except Exception as e:
            logger.error(f"Failed to get management node for cluster '{args.cluster_name}': {e}")
            return 1

        with KubernetesCtl(self.profile, mgmt_node.name, user, password) as kctl:
            health_checks = get_all_health_checks()
            for check in health_checks:
                print(f"[*] Running check: {check.name} - {check.description}")
                passed, message = check.execute(kctl)
                if passed:
                    print(f"  [PASS] {message}\n")
                else:
                    print(f"  [FAIL] {message}\n")
                    all_checks_passed = False

        if all_checks_passed:
            logger.info("All health checks passed successfully.")
            return 0
        else:
            logger.error("One or more health checks failed.")
            return 1

# --- Main ClusterUpgradeCmd (ensure this is the only definition) ---
class ClusterUpgradeCmd(SubCommandABC):
    """Manage the Blue/Green cluster upgrade workflow."""
    name = "upgrade"
    # TODO(agam): Add an arc to upgrade failed (rename to 'abort' if needed)
    COMMANDS = [
        ClusterUpgradeSnapshotCmd,
        ClusterUpgradeCreateCmd,
        ClusterUpgradeDestClusterCmd,
        ClusterUpgradePrepareDataCmd,
        ClusterUpgradeShowCmd,
        ClusterUpgradeBatchCmd,
        ClusterUpgradeSrcClusterCmd,
        ClusterUpgradeEndCmd,
        ClusterUpgradeHealthCheckCmd,
        ClusterUpgradeCancelCmd,
        UpgradeSecurityPatchUsernodes,
    ]

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = \
            ("Manage the Blue/Green cluster upgrade workflow, "
             "including creating upgrade processes and managing batches of devices.")

    def construct(self):
        subparsers = self.parser.add_subparsers(
            dest="upgrade_action",
            required=True,
            title="Upgrade Actions",
            help="Specific upgrade actions or sub-groups for managing the upgrade workflow."
        )
        for cmd_class in self.COMMANDS:
            m = cmd_class(subparsers, profile=self.profile, cli_instance=self.cli_instance)
            m.build()

    def run(self, args):
        if hasattr(args, "func"):
            return args.func(args)
        else:
            self.parser.print_help()
            return 1