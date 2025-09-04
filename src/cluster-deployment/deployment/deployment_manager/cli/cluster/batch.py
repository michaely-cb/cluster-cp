import json
import logging
import os
import tempfile
from typing import List, Tuple, Optional, Any

from django.db import transaction
from django.utils import timezone

from deployment_manager.cli.cluster.event_logger import UpgradeEventLogger
from deployment_manager.cli.cluster.helpers import (
    get_ceph_nodes,
    _security_patch_servers,
    confirm_quiesce_completed,
    get_batch_and_upgrade_process,
    get_upgrade_process,
    UpgradeCreds,
    update_security_patch_status,
    upgrade_pb3,
    upgrade_systems,
)
from deployment_manager.cli.cluster.repr import ClusterUpgradeBatchDeviceRepr, ClusterUpgradeBatchRepr
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceTypes
from deployment_manager.db.models import (
    ClusterDevice, ClusterUpgradeBatch, ClusterUpgradeBatchDevice,
    DataPreparationStatus, UpgradeBatchStatus, UpgradeDeviceStatus, UpgradeProcessStatus,
)
from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node
from deployment_manager.tools.network_config_tool import NetworkConfigTool

logger = logging.getLogger(__name__)
event_logger = UpgradeEventLogger()

default_thanos_node_port = 30033
properties_path = "/opt/cerebras/cluster/pkg-properties.yaml"

# --- Cluster Upgrade Batch Steps ---
BATCH_TAINT_STEP = "batch_taint"
BATCH_QUIESCE_STEP = "batch_quiesce"
BATCH_MIGRATE_STEP = "batch_migrate"
BATCH_UPDATE_DEVICE_ASSOCIATION_STEP = "batch_update_device_association"
BATCH_MIGRATE_OUT_STEP = "batch_migrate_out"
BATCH_SECURITY_PATCH_NODE_STEP = "batch_security_patch_node"
BATCH_SECURITY_PATCH_SYSTEM_STEP = "batch_security_patch_system"
BATCH_MIGRATE_IN_STEP = "batch_migrate_in"
BATCH_UPDATE_BATCH_DEVICE_STATUS_STEP = "batch_update_batch_device_status"

# --- Batch Upgrade Commands (cscfg cluster upgrade batch ...) ---

class ClusterUpgradeBatchCreateCmd(SubCommandABC):
    """Identify and create a new batch of devices for migration."""
    name = "create"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Create a new batch of devices/systems to be migrated as part of an upgrade process."
        self.db_update_only = os.environ.get("DB_UPDATE_ONLY", False)
        if self.db_update_only:
            logger.info("db_update_only set in ClusterUpgradeBatchCreateCmd")
        self.skip_src_cluster = os.environ.get("SKIP_SRC_CLUSTER", False)
        if self.skip_src_cluster:
            logger.info("skip_src_cluster set in ClusterUpgradeBatchCreateCmd")

    def construct(self):
        self.parser.add_argument(
            "--upgrade-id", required=False, type=int,
            help="ID of the parent upgrade process (uses active upgrade by default)"
        )
        self.parser.add_argument(
            "--force", action="store_true", required=False,
            help="force flag to skip phase check)"
        )
        group = self.parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--device-list", type=str,
            help="Comma-separated list of device names for the batch."
        )
        group.add_argument(
            "--device-list-file", type=str,
            help="Path to file containing list of device names (one per line)."
        )
        group.add_argument(
            "--session", type=str,
            help="Name of the session whose devices should form the batch."
        )
        self.parser.add_argument(
            "--job-id", type=str,
            help="Job ID whose associated devices should form the batch."
        )
        self.add_arg_output_format()

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeBatchUpgradeCreateCmd with args: {args}")

        upgrade_wrapper, error_code = get_upgrade_process(args.upgrade_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        # We expect the upgrade process to be either just done with data preparation, or having begun with other batches
        # If this is the first batch, move the upgrade to `MIGRATING_BATCHES`
        if not args.force:
            # Check if upgrade process is in a valid state for batch creation
            if upgrade_process.status not in [UpgradeProcessStatus.DEST_CLUSTER_UPGRADED.value,
                                              UpgradeProcessStatus.MIGRATING_BATCHES.value]:
                logger.error(
                    f"Upgrade process with ID {upgrade_process.id} is in status '{upgrade_process.status}', "
                    f"expected DEST_CLUSTER_UPGRADED or MIGRATING_BATCHES."
                )
                return 1

            # Optional check: warn if data preparation hasn't been completed yet
            if upgrade_process.data_preparation_status != DataPreparationStatus.COMPLETED.value:
                logger.warning(
                    f"Data preparation has not been completed yet for upgrade process {upgrade_process.id}. "
                    f"Consider running 'prepare_data' first."
                )

        upgrade_process.status = UpgradeProcessStatus.MIGRATING_BATCHES.value
        upgrade_process.save()
        try:
            device_names = []
            ceph_nodes = []
            if not self.db_update_only and args.session:
                dnode = get_k8s_lead_mgmt_node(self.profile, upgrade_process.source_cluster.name)
                user = dnode.get_prop(props.prop_management_credentials_user)
                password = dnode.get_prop(props.prop_management_credentials_password)
                with KubernetesCtl(self.profile, dnode.name, user, password) as kctl:
                    if args.job_id:
                        _, out, _ = kctl.run(f"csctl get job {args.job_id} -n{args.session} -d1 -ojson")
                        data = json.loads(out)
                        device_names = [
                                           instance["node"]
                                           for replica in data.get("status", {}).get("replicas", [])
                                           for instance in replica.get("instances", [])
                                       ] + data.get("status", {}).get("systems", [])
                        # deduplicate device names
                        device_names = list(set(device_names))
                    else:
                        pick_exception = None
                        try:
                            # select-upgrade-nodes requires target_version as first arg, session as second
                            # For batch migration, we use a non-existent version to select all nodes in session
                            # TODO: This is a hack - select-upgrade-nodes should support a --all-nodes-in-session flag
                            # select nodes with ignore-running-jobs flag to allow batch creation
                            # jobs will be quiesced later in the batch workflow
                            logger.info(f"Selecting nodes for batch migration from session: {args.session}")
                            kctl.run(
                                f"cd {upgrade_wrapper.cluster_pkg_path} && "
                                f"bash csadm.sh select-upgrade-nodes "
                                f"ignore-version {args.session} false ignore-running-jobs ignore-sx-pick-error"
                            )
                        except Exception as e:
                            logger.error(f"pick devices failed in session {args.session}: {e}")
                            raise
                        _, out, _ = kctl.run(f"cat /tmp/upgrade-nodes-{args.session}")
                        device_names = out.splitlines()

                        # Find all systems in the given session, including systems that are in error state
                        try:
                            _, out, _ = kctl.run(f"csctl get cluster --system-only -n {args.session} -ojson")
                            data = json.loads(out)
                            systems = [
                                item["meta"]["name"]
                                for item in data.get("items", [])
                            ]
                        except Exception as e:
                            logger.error(f"Failed to get systems from session {args.session}: {e}")
                            raise
                        device_names.extend(systems)
            elif args.device_list_file:
                with open(args.device_list_file, "r") as f:
                    device_names = [line.strip() for line in f]
            elif args.device_list:
                device_names = [name.strip() for name in args.device_list.split(',') if name.strip()]

            if not self.db_update_only and not self.skip_src_cluster:
                ceph_nodes = get_ceph_nodes(self.profile, upgrade_process.source_cluster.name)

            # Validate the selected devices
            devices_to_add = []
            for device_name in device_names:
                try:
                    # Look up device by name in the source cluster
                    # Note: device profile field is a must have to avoid duplicate records
                    cluster_device = ClusterDevice.objects.get(
                        device__name=device_name,
                        cluster=upgrade_process.source_cluster,
                        device__profile=upgrade_process.source_cluster.profile
                    )
                    devices_to_add.append(cluster_device.device)
                except ClusterDevice.DoesNotExist:
                    logger.error(
                        f"Device with name {device_name} not a part of "
                        f"'{upgrade_process.source_cluster.name}'."
                    )
                    return 1

            with transaction.atomic():
                # Create a new ClusterUpgradeBatch record linked to the ClusterUpgrade, status NOT_STARTED.
                new_batch = ClusterUpgradeBatch(
                    upgrade_process=upgrade_process,
                    status=UpgradeBatchStatus.NOT_STARTED.value,
                    created_at=timezone.now(),
                )
                new_batch.save()
                for device in devices_to_add:
                    # Filter out ceph nodes
                    if device.name in ceph_nodes:
                        logger.info(f"Skipping ceph node {device.name} from batch for batch {new_batch.id}")
                        continue

                    logger.info(f"Adding device {device.name} to batch {new_batch.id}")
                    ClusterUpgradeBatchDevice(
                        upgrade_batch=new_batch,
                        device=device,
                        status=UpgradeDeviceStatus.PENDING.value,
                        created_at=timezone.now(),
                    ).save()

            logger.info(f"Successfully created new batch {new_batch.id}")
            return 0
        except Exception as e:
            logger.error(f"Encountered an error during batch creation: {e}")
            return 1


class ClusterUpgradeBatchAddDeviceCmd(SubCommandABC):
    """Add devices to an existing batch."""
    name = "add_device"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.db_update_only = os.environ.get("DB_UPDATE_ONLY", False)
        if self.db_update_only:
            logger.info("db_update_only set in ClusterUpgradeBatchAddDeviceCmd")
        self.description = "Add devices to an existing batch."

    def construct(self):
        self.parser.add_argument(
            "--batch-id", required=False, type=int,
            help="ID of the batch to which the devices will be added (uses active batch by default)."
        )
        self.parser.add_argument(
            "--device-list", required=True, type=str,
            help="Comma-separated list of device names to add to the batch."
        )
        self.parser.add_argument(
            "--force", action="store_true", required=False,
            help="force flag to skip phase check)"
        )

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeBatchAddDeviceCmd with args: {args}")

        _, batch, error_code = get_batch_and_upgrade_process(args.batch_id, select_for_update=False)
        if error_code:
            return error_code

        if not args.force:
            # Check the batch hasn't started processing
            if batch.status != UpgradeBatchStatus.NOT_STARTED.value:
                logger.error(
                    f"Batch {batch.id} is in status '{batch.status}'. Devices can only be added when batch status is NOT_STARTED."
                )
                return 1

        # Parse device list
        device_names = [name.strip() for name in args.device_list.split(',') if name.strip()]
        if not device_names:
            logger.error("No valid device names provided in device list.")
            return 1


        ceph_nodes = []
        added_devices = []
        failed_devices = []

        try:
            with transaction.atomic():
                # Lock the batch for the update
                batch_for_update = ClusterUpgradeBatch.objects.select_for_update().get(id=batch.id)
                if not self.db_update_only:
                    ceph_nodes = get_ceph_nodes(self.profile, batch_for_update.upgrade_process.source_cluster.name)

                for device_name in device_names:
                    try:
                        cluster_device = ClusterDevice.objects.get(
                            device__name=device_name, 
                            cluster=batch_for_update.upgrade_process.source_cluster
                        )
                        device_to_add = cluster_device.device

                        if device_to_add is None:
                            logger.error(f"Device with name '{device_name}' not found.")
                            failed_devices.append(device_name)
                            continue

                        # Check if the device is already part of another batch for this upgrade process
                        other_batch_device = ClusterUpgradeBatchDevice.objects.filter(
                            upgrade_batch__upgrade_process=batch_for_update.upgrade_process,
                            device=device_to_add
                        ).exclude(upgrade_batch=batch_for_update).first()

                        if other_batch_device:
                            logger.error(
                                f"Device '{device_name}' is already part of another batch "
                                f"(ID: {other_batch_device.upgrade_batch.id}) for this upgrade."
                            )
                            failed_devices.append(device_name)
                            continue

                        # Check if the device is already in the batch
                        if ClusterUpgradeBatchDevice.objects.filter(upgrade_batch=batch_for_update, device=device_to_add).exists():
                            logger.warning(f"Device '{device_name}' is already in batch {batch.id}. Skipping.")
                            continue

                        if device_to_add.name in ceph_nodes:
                            logger.info(f"Skipping ceph node {device_to_add.name} from batch {batch.id}")
                            continue

                        # Add the device
                        ClusterUpgradeBatchDevice.objects.create(
                            upgrade_batch=batch_for_update,
                            device=device_to_add,
                            status=UpgradeDeviceStatus.PENDING.value,
                            current_step="batch_device_added",
                        )
                        added_devices.append(device_name)
                        logger.info(f"Successfully added device '{device_name}' to batch {batch.id}.")

                    except ClusterDevice.DoesNotExist:
                        logger.error(f"Device with name '{device_name}' not found in the source cluster of the upgrade process.")
                        failed_devices.append(device_name)
                        continue

                # Report results
                if added_devices:
                    logger.info(f"Successfully added {len(added_devices)} devices to batch {batch.id}: {', '.join(added_devices)}")
                if failed_devices:
                    logger.error(f"Failed to add {len(failed_devices)} devices to batch {batch.id}: {', '.join(failed_devices)}")

                # Return error code if any devices failed to be added
                return 1 if failed_devices else 0

        except Exception as e:
            logger.error(f"An error occurred while adding devices to batch: {e}")
            return 1


class ClusterUpgradeBatchRemoveDeviceCmd(SubCommandABC):
    """Remove a device from an existing batch."""
    name = "remove_device"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Remove a device from an existing batch."

    def construct(self):
        self.parser.add_argument(
            "--batch-id", required=False, type=int,
            help="ID of the batch from which the devices will be removed (uses active batch by default)."
        )
        self.parser.add_argument(
            "--device-list", required=True, type=str,
            help="Comma-separated list of device names to remove from the batch."
        )
        self.add_arg_noconfirm(help="Skip confirmation before removing the devices from the batch.")

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeBatchRemoveDeviceCmd with args: {args}")

        _, batch, error_code = get_batch_and_upgrade_process(args.batch_id, select_for_update=False)
        if error_code:
            return error_code

        # Parse device list
        device_names = [name.strip() for name in args.device_list.split(',') if name.strip()]
        if not device_names:
            logger.error("No valid device names provided in device list.")
            return 1

        if not args.noconfirm:
            device_list_str = ', '.join(device_names)
            confirmed = input(f"Are you sure you want to remove devices [{device_list_str}] from batch {batch.id}? [y/N]: ").lower().strip() == 'y'
            if not confirmed:
                logger.info("Device removal cancelled by user.")
                return 0

        removed_devices = []
        failed_devices = []

        try:
            with transaction.atomic():
                # Lock the batch for the update
                batch_for_update = ClusterUpgradeBatch.objects.select_for_update().get(id=batch.id)

                for device_name in device_names:
                    try:
                        cluster_device = ClusterDevice.objects.get(
                            device__name=device_name, 
                            cluster=batch_for_update.upgrade_process.source_cluster
                        )
                        device_to_remove = cluster_device.device

                        if device_to_remove is None:
                            logger.error(f"Device with name '{device_name}' not found.")
                            failed_devices.append(device_name)
                            continue

                        # Find and delete the batch device record
                        batch_device_entry = ClusterUpgradeBatchDevice.objects.get(upgrade_batch=batch_for_update, device=device_to_remove)
                        batch_device_entry.delete()
                        removed_devices.append(device_name)
                        logger.info(f"Successfully removed device '{device_name}' from batch {batch.id}.")

                    except ClusterDevice.DoesNotExist:
                        logger.error(f"Device with name '{device_name}' not found in the source cluster of the upgrade process.")
                        failed_devices.append(device_name)
                        continue
                    except ClusterUpgradeBatchDevice.DoesNotExist:
                        logger.error(f"Device '{device_name}' is not in batch {batch.id}.")
                        failed_devices.append(device_name)
                        continue

                # Report results
                if removed_devices:
                    logger.info(f"Successfully removed {len(removed_devices)} devices from batch {batch.id}: {', '.join(removed_devices)}")
                if failed_devices:
                    logger.error(f"Failed to remove {len(failed_devices)} devices from batch {batch.id}: {', '.join(failed_devices)}")

                # Return error code if any devices failed to be removed
                return 1 if failed_devices else 0

        except Exception as e:
            logger.error(f"An error occurred while removing devices from batch: {e}")
            return 1


class ClusterUpgradeBatchShowCmd(SubCommandABC):
    """Show batches for cluster upgrade processes."""
    name = "show"

    def construct(self):
        self.add_arg_output_format()
        self.parser.add_argument("--upgrade-id", required=False, type=int, help="ID of the upgrade process.")
        self.parser.add_argument("--batch-id", required=False, type=int, help="ID of a specific batch to show. Shows all batches if omitted.")
        self.parser.add_argument(
            "--devices",
            action="store_true",
            help="Show all devices in the batch (uses active batch if --batch-id not provided)"
        )
        self.parser.add_argument(
            "-e", "--error-only",
            action="store_true",
            help="Show only failed devices (use with --devices)"
        )

    def run(self, args):
        logger.debug(f"Executing ClusterUpgradeBatchShowCmd with args: {args}")

        # Show batch table first
        return_code = self._show_batches(args)
        if return_code != 0:
            logger.error("Failed to show batches.")
            return return_code

        # Handle device display if requested
        if args.devices:
            # Get the batch (by id or implicit)
            _, batch, error_code = get_batch_and_upgrade_process(args.batch_id)
            if error_code:
                return error_code
            print("\n=== Batch Device Status Table ===\n")
            self._show_batch_devices(args, batch)
        elif args.error_only:
            logger.error("--error-only must be used with --devices.")
            return 1
        return return_code

    def _show_batch_devices(self, args, batch):
        # Get all batch devices for this batch
        query = ClusterUpgradeBatchDevice.objects.filter(upgrade_batch=batch).select_related('device')
        if args.error_only:
            query = query.filter(status=UpgradeDeviceStatus.FAILED.value)
        batch_devices = list(query)
        if not batch_devices:
            if args.error_only:
                logger.info(f"No failed devices found in batch {batch.id}")
            else:
                logger.info(f"No devices found in batch {batch.id}")
            return 0
        reprs = [ClusterUpgradeBatchDeviceRepr.from_orm(bd) for bd in batch_devices]
        print(ClusterUpgradeBatchDeviceRepr.format_reprs(reprs, args.output))
        return 0

    def _show_batches(self, args):
        """Show batch information."""
        upgrade_wrapper, error_code = get_upgrade_process(args.upgrade_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        # Query batches for this upgrade process
        query = ClusterUpgradeBatch.objects.filter(upgrade_process=upgrade_process).order_by('-created_at')

        batches = []
        if args.batch_id:
            try:
                batch = query.get(id=args.batch_id)
                batches = [batch]
            except ClusterUpgradeBatch.DoesNotExist:
                logger.error(f"Batch with ID {args.batch_id} not found in upgrade process {args.upgrade_id}.")
                return 1
        else:
            batches = list(query.all())
            if not batches:
                logger.info(f"No batches found for upgrade process {args.upgrade_id}.")
                return 0

        # Create repr objects for batches
        reprs = []
        for batch in batches:
            reprs.append(ClusterUpgradeBatchRepr.from_orm(batch))

        print(ClusterUpgradeBatchRepr.format_reprs(reprs, args.output))
        return 0


class ClusterUpgradeBatchRunTaintCmd(SubCommandABC):
    """Taint devices in a specified batch in the source cluster."""
    name = "taint"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.db_update_only = os.environ.get("DB_UPDATE_ONLY", False)
        if self.db_update_only:
            logger.info("db_update_only set in ClusterUpgradeBatchRunTaintCmd")
        self.description = "Taint devices in a batch within the source cluster to prevent new workloads."

    def construct(self):
        self.parser.add_argument(
            "--batch-id", required=False, type=int,
            help="ID of the batch whose devices to taint (uses active batch by default).")
        self.parser.add_argument(
            "--force", action="store_true", required=False,
            help="force flag to skip phase check)"
        )

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeBatchUpgradeRunTaintCmd with args: {args}")

        upgrade_wrapper, batch, error_code = get_batch_and_upgrade_process(args.batch_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        if not args.force:
            # Validate batch status - should be NOT_STARTED
            if batch.status != UpgradeBatchStatus.NOT_STARTED.value:
                logger.error(f"Batch {batch.id} is in status '{batch.status}', expected NOT_STARTED.")
                return 1

        try:
            source_cluster = upgrade_process.source_cluster.name
            upgrade_pkg_path = upgrade_process.upgrade_pkg_path
            batch_devices = ClusterUpgradeBatchDevice.objects.filter(upgrade_batch=batch)
            logger.info(f"Tainting {len(batch_devices)} devices in batch {batch.id}")

            sys_nodes = [node for node in batch_devices if
                         node.device.device_type == DeploymentDeviceTypes.SYSTEM.value]
            server_nodes = [node for node in batch_devices if
                            node.device.device_type == DeploymentDeviceTypes.SERVER.value]

            # If not DB update only, prepare for actual tainting
            if not self.db_update_only:
                dnode = get_k8s_lead_mgmt_node(self.profile, source_cluster)
                user = dnode.get_prop(props.prop_management_credentials_user)
                password = dnode.get_prop(props.prop_management_credentials_password)

                # Prepare the list of server nodes for batch tainting
                # TODO: remove file once the overall BG logic matures as keeping the file provides visibility into it.
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                    for node in server_nodes:
                        logger.info(f"Adding server node {node.device.name} to the batch to taint.")
                        temp_file.write(node.device.name + '\n')
                    temp_file_path = temp_file.name

                logger.info("Start batch tainting server nodes")

                # TODO: hardcode the state and reason for now
                state = 'error'
                reason = 'cluster-bg-upgrade-in-progress'
                dest_file_path = '/opt/cerebras/cluster/taintBatchNodes'
                should_throw = False
                with KubernetesCtl(self.profile, dnode.name, user, password) as kctl:
                    logger.info(f"Start tainting of server nodes: state={state}, reason={reason}")
                    try:
                        kctl.copy_file(temp_file_path, dest_file_path)
                        kctl.run(f"cd {upgrade_pkg_path} && bash csadm.sh batch-update-nodes {dest_file_path} {state} {reason}")
                        logger.info(f"Server nodes tainted successfully: {len(server_nodes)} nodes")
                    except Exception as e:
                        should_throw = True
                        logger.error(f"Failed to taint server nodes in a batch: {e}")

                    logger.info("Start tainting of system nodes")
                    # System nodes: taint one by one
                    for node in sys_nodes:
                        node_name = node.device.name
                        try:
                            logger.info(f"Tainting system node: {node_name}")
                            kctl.run(f"cd {upgrade_pkg_path} && bash csadm.sh update-system --name={node_name} --state={state} --note={reason}")
                            logger.info(f"System node {node_name} tainted successfully")
                        except Exception as e:
                            should_throw = True
                            logger.error(f"Failed to taint system node {node_name}: {e}")
                if should_throw:
                    raise RuntimeError("Something went wrong during taint, check logs for more details!")
            else:
                # DB update only: just log what would be done
                logger.info("Server nodes to be tainted (DB update only):")
                for node in server_nodes:
                    logger.info(f"Adding server node {node.device.name} to the batch to taint.")

                logger.info("System nodes to be tainted (DB update only):")
                for node in sys_nodes:
                    node_name = node.device.name
                    logger.info(f"System node {node_name} to be tainted (DB update only):")

            with transaction.atomic():
                for batch_device in batch_devices:
                    # Update ClusterUpgradeBatchDevice.status and current_step
                    batch_device.status = UpgradeDeviceStatus.IN_PROGRESS.value
                    batch_device.current_step = "tainted"
                    batch_device.save()

                # Update ClusterUpgradeBatch.status to TAINT_COMPLETED
                batch.status = UpgradeBatchStatus.TAINT_COMPLETED.value
                batch.save()

            logger.info(f"Successfully completed tainting for batch {batch.id}")
            return 0

        except Exception as e:
            logger.error(f"Encountered an error during tainting: {e}")
            logger.exception("Failed during tainting.")
            # Revert status on error
            batch.status = UpgradeBatchStatus.FAILED.value
            batch.save()
            return 1


class ClusterUpgradeBatchRunQuiesceCmd(SubCommandABC):
    """Quiesce workloads on devices in a specified batch."""
    name = "quiesce"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.db_update_only = os.environ.get("DB_UPDATE_ONLY", "False")
        self.description = "Cancel/drain jobs from devices in a batch within the source cluster."

    def construct(self):
        self.parser.add_argument(
            "--batch-id", required=False, type=int,
            help="ID of the batch whose devices to quiesce (uses active batch by default)."
        )
        self.parser.add_argument(
            "--force", action="store_true", required=False,
            help="force flag to skip phase check)"
        )
        self.add_arg_noconfirm(help="Skip confirmation before quiescing the batch.")

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeBatchRunQuiesceCmd with args: {args}")

        upgrade_wrapper, batch, error_code = get_batch_and_upgrade_process(args.batch_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        if not args.force:
            # Validate batch status - should be TAINT_COMPLETED
            if batch.status != UpgradeBatchStatus.TAINT_COMPLETED.value:
                logger.error(f"Batch {batch.id} is in status '{batch.status}', expected TAINT_COMPLETED.")
                return 1

        event_logger.set_upgrade_id(self.db_update_only, upgrade_process.id)
        event_logger.step_start(self.db_update_only, BATCH_QUIESCE_STEP)

        batch_devices = ClusterUpgradeBatchDevice.objects.filter(upgrade_batch=batch)
        logger.info(f"Quiescing workloads on {len(batch_devices)} devices in batch {batch.id}")

        try:
            if self.db_update_only == "False":
                dnode = get_k8s_lead_mgmt_node(self.profile, upgrade_process.source_cluster.name)
                if dnode is None:
                    logger.error("Could not get Kubernetes deployment node")
                    event_logger.step_end(self.db_update_only, BATCH_QUIESCE_STEP, False, "Kubernetes deployment node not found")
                    return 1

                user = dnode.get_prop(props.prop_management_credentials_user)
                password = dnode.get_prop(props.prop_management_credentials_password)

                with KubernetesCtl(self.profile, dnode.name, user, password) as kctl:
                    nodes_output = json.loads(kctl.csctl_get_nodes())
                    jobs_map = {}
                    for node_item in nodes_output.get("items", []):
                        meta = node_item.get("meta", {})
                        node_name = meta.get("name")
                        if node_name:
                            jobs_map[node_name] = node_item.get("jobIds", [])

                    # Collect all devices with jobs first
                    devices_with_jobs = []
                    total_job_count = 0
                    for batch_device in batch_devices:
                        device_name = batch_device.device.name
                        job_ids = jobs_map.get(device_name, [])
                        if job_ids:
                            # There are duplicate job ids in the list, so we deduplicate it first.
                            job_ids = list(set(job_ids))
                            devices_with_jobs.append((batch_device, device_name, job_ids))
                            total_job_count += len(job_ids)

                    if not devices_with_jobs:
                        logger.info("No jobs found on any devices in the batch, continuing")
                    else:
                        # Ask for confirmation once for all jobs
                        if not args.noconfirm:
                            logger.info(f"About to cancel {total_job_count} jobs across {len(devices_with_jobs)} devices:")
                            for _, device_name, job_ids in devices_with_jobs:
                                logger.info(f"  - Device '{device_name}': {len(job_ids)} jobs {job_ids}")
                            confirmed = input("Are you sure you want to proceed? [y/N]: ").lower().strip() == 'y'
                            if not confirmed:
                                logger.info("Operation cancelled by user.")
                                event_logger.step_end(self.db_update_only, BATCH_QUIESCE_STEP, True, "Operation cancelled by user.")
                                return 0

                        # Cancel all jobs after confirmation
                        cancelled_jobs = set()
                        for batch_device, device_name, job_ids in devices_with_jobs:
                            for job_id in job_ids:
                                if job_id in cancelled_jobs:
                                    logger.info(f"Job {job_id} on device {device_name} already cancelled, skipping...")
                                    continue

                                namespace, actual_job_id = job_id.split("/", 1)
                                cancel_output = kctl.csctl_cancel_job(actual_job_id, namespace)
                                cancelled_jobs.add(job_id)

                                if "does not exist" in cancel_output or "Job already ended" in cancel_output:
                                    logger.info(f"Job {job_id} did not exist on device {device_name}, continuing...")
                                    continue

                                logger.info(f"Cancelled job {job_id} on device {device_name}")

                    # Update all batch devices to quiesced state
                    for batch_device in batch_devices:
                        batch_device.current_step = "quiesced"
                        batch_device.save()

            else:
                # Handle the DB_UPDATE_ONLY case
                for batch_device in batch_devices:
                    batch_device.current_step = "quiesced"
                    batch_device.save()

            # Update ClusterUpgradeBatch.status to QUIESCE_COMPLETED
            batch.status = UpgradeBatchStatus.QUIESCE_COMPLETED.value
            batch.save()
            event_logger.step_end(self.db_update_only, BATCH_QUIESCE_STEP, True)
            logger.info(f"Successfully completed quiescing for batch {batch.id}")
            return 0

        except Exception as e:
            logger.error(f"Encountered an error during quiescing: {e}")
            logger.exception("Failed during quiescing.")
            event_logger.step_end(self.db_update_only, BATCH_QUIESCE_STEP, False, str(e))
            # Revert status on error
            batch.status = UpgradeBatchStatus.FAILED.value
            batch.save()
            return 1


class ClusterUpgradeBatchRunSecurityPatchNodesCmd(SubCommandABC):
    """Apply security patches to the nodes (servers) in a specified batch."""
    name = "security_patch_nodes"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = ("Apply security patches to the server nodes (e.g., SR type) in a batch. "
                            "Failed nodes are dropped from the batch; command succeeds if any nodes pass.")

    def construct(self):
        self.parser.add_argument(
            "--batch-id", required=False, type=int,
            help="ID of the batch whose server nodes to patch (uses active batch by default)."
        )
        self.parser.add_argument(
            "--force", action="store_true",
            help="Skip prerequisite checks (for testing)."
        )
        # Add any specific options for patching if needed, e.g., --patch-level, --package-list

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeBatchRunSecurityPatchNodesCmd with args: {args}")

        upgrade_wrapper, batch, error_code = get_batch_and_upgrade_process(args.batch_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        # Validate batch status - should be QUIESCE_COMPLETED (unless forced)
        if not confirm_quiesce_completed(batch, args.force):
            return 1

        event_logger.set_upgrade_id(self.db_update_only, upgrade_process.id)
        event_logger.step_start(self.db_update_only, BATCH_SECURITY_PATCH_NODE_STEP)

        try:
            # Get list of devices for this batch from ClusterUpgradeBatchDevice.
            batch_devices = ClusterUpgradeBatchDevice.objects.filter(upgrade_batch=batch)

            # Filter for devices that are "nodes" (e.g., type 'SR').
            nodes = [bd for bd in batch_devices if bd.device.device_type == 'SR']
            logger.info(f"Applying security patches to {len(nodes)} server nodes in batch {batch.id}")

            if len(nodes) == 0:
                logger.warning(f"No server nodes (type 'SR') found in batch {batch.id}. Nothing to patch.")
                event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_NODE_STEP, True)
                return 0

            node_names = [n.device.name for n in nodes]
            successful_nodes, failed_nodes, unreachable_nodes, unknown_nodes = \
                _security_patch_servers(self.profile, None, node_names, upgrade_process.upgrade_pkg_path)
            update_security_patch_status(nodes, successful_nodes, failed_nodes, unreachable_nodes, unknown_nodes)

            # Remove failed devices from the batch by deleting their ClusterUpgradeBatchDevice records
            failed_device_names = failed_nodes + unreachable_nodes + unknown_nodes
            if failed_device_names:
                logger.warning(f"Dropping {len(failed_device_names)} failed nodes from batch {batch.id}: {', '.join(failed_device_names)}")
                ClusterUpgradeBatchDevice.objects.filter(
                    upgrade_batch=batch,
                    device__name__in=failed_device_names
                ).delete()

            # Command succeeds as long as SOME nodes passed
            if successful_nodes:
                logger.info(f"Security patching completed successfully for {len(successful_nodes)} nodes in batch {batch.id}")
                if failed_device_names:
                    logger.warning(f"Note: {len(failed_device_names)} nodes were dropped from the batch due to failures")
                logger.info(f"Batch {batch.id} will continue with {len(successful_nodes)} remaining nodes")
                event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_NODE_STEP, True)
                return 0
            else:
                logger.error(f"All {len(nodes)} nodes failed security patching in batch {batch.id}")
                logger.error(f"Batch {batch.id} cannot continue - no nodes remaining")
                # Mark batch as failed only if ALL nodes failed
                batch.status = UpgradeBatchStatus.FAILED.value
                batch.save()
                event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_NODE_STEP, False, "All nodes failed security patching")
                return 1

        except Exception as e:
            logger.error(f"Encountered an error during node security patching: {e}")
            logger.exception("Failed during node security patching.")
            event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_NODE_STEP, False, str(e))
            return 1


class ClusterUpgradeBatchRunUpgradeSystemsCmd(SubCommandABC):
    """Apply upgrades to the CS systems in a specified batch."""
    name = "upgrade_systems"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Apply upgrades to the CS systems (SY type) in a batch. Failed systems are dropped from the batch; command succeeds if any systems pass."

    def construct(self):
        self.parser.add_argument("--batch-id", required=False, type=int, help="ID of the batch whose CS systems to patch/upgrade (uses active batch by default).")
        self.parser.add_argument("--force", action="store_true", help="Skip prerequisite checks (for testing).")

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeBatchRunUpgradeSystemsCmd with args: {args}")

        upgrade_wrapper, batch, error_code = get_batch_and_upgrade_process(args.batch_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        # Validate batch status - should be QUIESCE_COMPLETED (unless forced)
        if not confirm_quiesce_completed(batch, args.force):
            return 1

        # Validate system image path exists
        if not os.path.isfile(upgrade_wrapper.system_img_tarball):
            logger.error(f"System image path '{upgrade_wrapper.system_img_tarball}' does not exist or is not a file.")
            return 1

        event_logger.set_upgrade_id(self.db_update_only, upgrade_process.id)
        event_logger.step_start(self.db_update_only, BATCH_SECURITY_PATCH_SYSTEM_STEP)

        try:
            # Get list of devices for this batch from ClusterUpgradeBatchDevice.
            batch_devices = ClusterUpgradeBatchDevice.objects.filter(upgrade_batch=batch)

            # Filter for devices that are "systems" (e.g., type 'SY').
            systems = [bd for bd in batch_devices if bd.device.device_type == 'SY']
            logger.info(f"Applying security patches/upgrades to {len(systems)} CS systems in batch {batch.id}")

            if len(systems) == 0:
                logger.warning(f"No CS systems (type 'SY') found in batch {batch.id}. Nothing to patch.")
                event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_SYSTEM_STEP, True)
                return 0

            # Use the new helper function
            success, failed_device_names = upgrade_systems(systems, upgrade_wrapper.system_img_tarball)

            if failed_device_names:
                logger.warning(f"Dropping {len(failed_device_names)} failed systems from batch {batch.id}: {', '.join(failed_device_names)}")
                ClusterUpgradeBatchDevice.objects.filter(
                    upgrade_batch=batch,
                    device__name__in=failed_device_names
                ).delete()

            # Command succeeds as long as SOME systems passed
            if success:
                logger.info(f"System patching completed successfully for systems in batch {batch.id}")
                if failed_device_names:
                    logger.warning(f"Note: {len(failed_device_names)} systems were dropped from the batch due to failures")
                logger.info(f"Batch {batch.id} will continue with remaining systems")
                event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_SYSTEM_STEP, True)
                return 0
            else:
                logger.error(f"All systems failed patching in batch {batch.id}")
                logger.error(f"Batch {batch.id} cannot continue - no systems remaining")
                # Mark batch as failed only if ALL systems failed
                event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_SYSTEM_STEP, False, "All systems failed security patching")
                batch.status = UpgradeBatchStatus.FAILED.value
                batch.save()
                return 1

        except Exception as e:
            logger.error(f"Encountered an error during system security patching: {e}")
            logger.exception("Failed during system security patching.")
            event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_SYSTEM_STEP, False, str(e))
            return 1


class ClusterUpgradeBatchRunMigrateCmd(SubCommandABC):
    """Migrate devices in a specified batch from source to destination cluster."""
    name = "migrate"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Migrate devices in a batch from the source cluster to the destination cluster."
        self.db_update_only = os.environ.get("DB_UPDATE_ONLY", False)
        if self.db_update_only:
            logger.info("db_update_only set in ClusterUpgradeBatchRunMigrateCmd")
        self.skip_dest_cluster = os.environ.get("SKIP_DEST_CLUSTER", False)
        if self.skip_dest_cluster:
            logger.info("skip_dest_cluster set in ClusterUpgradeBatchRunMigrateCmd")
        self.skip_src_cluster = os.environ.get("SKIP_SRC_CLUSTER", False)
        if self.skip_src_cluster:
            logger.info("skip_src_cluster set in ClusterUpgradeBatchRunMigrateCmd")
        self.one_step_only = os.environ.get("ONE_STEP_ONLY", False)
        if self.one_step_only:
            logger.info("one_step_only set")
        
        # Initialize state machine handlers mapping
        self.handlers = {
            UpgradeBatchStatus.MIGRATE_STARTED.value: self._handle_migrate_started,
            UpgradeBatchStatus.UPDATE_DEVICE_ASSOCIATION_FAILED.value: self._handle_migrate_started,
            UpgradeBatchStatus.UPDATE_DEVICE_ASSOCIATION_COMPLETED.value: self._handle_migrate_out,
            UpgradeBatchStatus.MIGRATE_OUT_FAILED.value: self._handle_migrate_out,
            UpgradeBatchStatus.MIGRATE_OUT_COMPLETED.value: self._handle_security_patch_servers,
            UpgradeBatchStatus.SECURITY_PATCH_SERVERS_FAILED.value: self._handle_security_patch_servers,
            UpgradeBatchStatus.SECURITY_PATCH_SERVERS_COMPLETED.value: self._handle_upgrade_systems,
            UpgradeBatchStatus.UPGRADE_SYSTEMS_FAILED.value: self._handle_upgrade_systems,
            UpgradeBatchStatus.UPGRADE_SYSTEMS_COMPLETED.value: self._handle_migrate_in,
            UpgradeBatchStatus.MIGRATE_IN_FAILED.value: self._handle_migrate_in,
            UpgradeBatchStatus.MIGRATE_IN_COMPLETED.value: self._handle_complete_batch_migration,
            UpgradeBatchStatus.COMPLETE_BATCH_MIGRATION_FAILED.value: self._handle_complete_batch_migration,
        }

    def construct(self):
        self.parser.add_argument(
            "--batch-id", required=False, type=int,
            help="ID of the batch to migrate (uses active batch by default)."
        )
        self.parser.add_argument(
            "--skip-security-patch",
            action="store_true",
            help="Skip security patching on the destination cluster."
        )
        self.parser.add_argument(
            "--skip-upgrade-system",
            action="store_true",
            help="Skip system upgrade on the destination cluster."
        )
        self.parser.add_argument(
            "--force",
            action="store_true",
            help="Force the migration to start even if the batch is not in the correct status."
        )
        self.add_arg_noconfirm(help="Skip confirmation before migrating the batch.")
    
    def k8s_cluster_update(
        self,
        profile: str,
        cluster_name: str,
        cluster_mgmt_node: str,
        user: str,
        password: str,
        operation: str,
        cluster_pkg_path: str,
        force_new_install_version: str = None
    ) -> None:
        """Helper function to update K8S with network configuration.
        
        Args:
            profile: The deployment profile
            cluster_name: The cluster name
            cluster_mgmt_node: The k8s management node name
            user: The username
            password: The password
            operation: The operation to perform "add" or "remove"
            cluster_pkg_path: The path to the cluster package
            force_new_install_version: The version to install
        Raises:
            RuntimeError: If the update fails
        """
        nwt = NetworkConfigTool.for_profile(self.profile)
        k8s_cfg = nwt.generate_nw_config_for_k8s(cluster_name=cluster_name)
        logger.info(f"Network config generated at {k8s_cfg}")
        with KubernetesCtl(profile, cluster_mgmt_node, user, password) as kctl:
            kctl.upload_network_config(k8s_cfg)
            logger.info(f"Network config uploaded to K8S deploy node on {cluster_name}")

        upgrade_pb3(profile, cluster_mgmt_node, user, password, operation, cluster_pkg_path, force_new_install_version)
    
    def _update_device_association(self, batch_devices, source_cluster, dest_cluster):
        with transaction.atomic():
            # Update ClusterDevice records separately
            for batch_device in batch_devices:
                device = batch_device.device
                device_name = device.name
                # Find and update the ClusterDevice record
                try:
                    cluster_device = ClusterDevice.objects.get(cluster=source_cluster, device=device)
                    cluster_device.cluster = dest_cluster
                    cluster_device.save()
                    logger.info(
                        f"Updated ClusterDevice for {device_name} from '{source_cluster.name}' to '{dest_cluster.name}'"
                    )
                except ClusterDevice.DoesNotExist:
                    logger.warning(
                        f"ClusterDevice not found for device {device_name} in source cluster '{source_cluster.name}'. Check destination cluster."
                    )
                    # The device might have been moved to the destination cluster already. If it is not found in the destination cluster,
                    # raise an error.
                    try:
                        cluster_device = ClusterDevice.objects.get(cluster=dest_cluster, device=device)
                    except ClusterDevice.DoesNotExist:
                        logger.error(
                            f"ClusterDevice not found for device {device_name} in destination cluster '{dest_cluster.name}'"
                        )
                        raise
                # Update the batch device status
                batch_device.current_step = "cluster_association_updated"
                batch_device.save()

    def _fetch_devices(self, batch: ClusterUpgradeBatch) -> Tuple[List[ClusterUpgradeBatchDevice], List[ClusterUpgradeBatchDevice], List[ClusterUpgradeBatchDevice]]:
        """Helper to get current batch devices grouped by type"""
        bds = ClusterUpgradeBatchDevice.objects.filter(upgrade_batch=batch)
        srvs = [bd for bd in bds if bd.device.device_type == "SR"]
        syss = [bd for bd in bds if bd.device.device_type == "SY"]
        return bds, srvs, syss

    def _handle_migrate_started(self, batch, upgrade_wrapper, creds, args):
        """Handle initial migration state"""
        try:
            batch.status = UpgradeBatchStatus.UPDATE_DEVICE_ASSOCIATION_IN_PROGRESS.value
            batch.save()
            
            event_logger.step_start(self.db_update_only, BATCH_UPDATE_DEVICE_ASSOCIATION_STEP)
            
            upgrade_process = upgrade_wrapper.upgrade_process
            source_cluster = upgrade_process.source_cluster
            dest_cluster = upgrade_process.dest_cluster
            
            batch_devices, _, _ = self._fetch_devices(batch)
            self._update_device_association(batch_devices, source_cluster, dest_cluster)
            
            event_logger.step_end(self.db_update_only, BATCH_UPDATE_DEVICE_ASSOCIATION_STEP, True)
            batch.status = UpgradeBatchStatus.UPDATE_DEVICE_ASSOCIATION_COMPLETED.value
            batch.save()
        except (KeyboardInterrupt, Exception) as e:
            batch.status = UpgradeBatchStatus.UPDATE_DEVICE_ASSOCIATION_FAILED.value
            batch.save()
            event_logger.step_end(self.db_update_only, BATCH_UPDATE_DEVICE_ASSOCIATION_STEP, False, str(e))
            raise RuntimeError(f"Failed to update device association: {e}")

    def _handle_migrate_out(self, batch, upgrade_wrapper, creds, args):
        """Handle migration out of source cluster"""
        try:
            if self.db_update_only or self.skip_src_cluster:
                batch.status = UpgradeBatchStatus.MIGRATE_OUT_COMPLETED.value
                batch.save()
                return

            batch.status = UpgradeBatchStatus.MIGRATE_OUT_IN_PROGRESS.value
            batch.save()

            event_logger.step_start(self.db_update_only, BATCH_MIGRATE_OUT_STEP)

            upgrade_process = upgrade_wrapper.upgrade_process
            
            self.k8s_cluster_update(
                profile=self.profile,
                cluster_name=upgrade_process.source_cluster.name,
                cluster_mgmt_node=creds.dnode_source_cluster.name,
                user=creds.user_source_cluster,
                password=creds.password_source_cluster,
                operation="remove",
                cluster_pkg_path=upgrade_wrapper.cluster_pkg_path,
            )
            if not self.validate_device_list(upgrade_process.source_cluster):
                raise RuntimeError(f"Device count validation failed for source cluster {upgrade_process.source_cluster.name}")
            
            batch.status = UpgradeBatchStatus.MIGRATE_OUT_COMPLETED.value
            batch.save()

            event_logger.step_end(self.db_update_only, BATCH_MIGRATE_OUT_STEP, True)
        
        except (KeyboardInterrupt, Exception) as e:
            batch.status = UpgradeBatchStatus.MIGRATE_OUT_FAILED.value
            batch.save()
            event_logger.step_end(self.db_update_only, BATCH_MIGRATE_OUT_STEP, False, str(e))
            raise RuntimeError(f"Failed to handle migrate out: {e}")

    def _handle_security_patch_servers(self, batch, upgrade_wrapper, creds, args):
        """Handle server security patching"""
        try:
            _, servers, _ = self._fetch_devices(batch)
            if len(servers) == 0 or args.skip_security_patch or self.db_update_only:
                batch.status = UpgradeBatchStatus.SECURITY_PATCH_SERVERS_COMPLETED.value
                batch.save()
                return
            
            batch.status = UpgradeBatchStatus.SECURITY_PATCH_SERVERS_IN_PROGRESS.value
            batch.save()

            event_logger.step_start(self.db_update_only, BATCH_SECURITY_PATCH_NODE_STEP)
            
            upgrade_process = upgrade_wrapper.upgrade_process
            node_names = [n.device.name for n in servers]
            successful_nodes, failed_nodes, unreachable_nodes, unknown_nodes = _security_patch_servers(self.profile, None, node_names, upgrade_process.upgrade_pkg_path)
            update_security_patch_status(servers, successful_nodes, failed_nodes, unreachable_nodes, unknown_nodes)
            
            failed_device_names = failed_nodes + unreachable_nodes + unknown_nodes
            if len(failed_device_names) > 0:
                raise RuntimeError(f"Security patching failed for {len(failed_device_names)} servers")

            batch.status = UpgradeBatchStatus.SECURITY_PATCH_SERVERS_COMPLETED.value
            batch.save()

            event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_NODE_STEP, True)

        except (KeyboardInterrupt, Exception) as e:
            batch.status = UpgradeBatchStatus.SECURITY_PATCH_SERVERS_FAILED.value
            batch.save()
            event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_NODE_STEP, False, str(e))
            raise RuntimeError(f"Failed to security patch servers: {e}")

    def _handle_upgrade_systems(self, batch, upgrade_wrapper, creds, args):
        """Handle system security patching"""
        try:
            _, _, systems = self._fetch_devices(batch)
            if len(systems) == 0 or args.skip_upgrade_system or self.db_update_only:
                batch.status = UpgradeBatchStatus.UPGRADE_SYSTEMS_COMPLETED.value
                batch.save()
                return
            
            batch.status = UpgradeBatchStatus.UPGRADE_SYSTEMS_IN_PROGRESS.value
            batch.save()

            event_logger.step_start(self.db_update_only, BATCH_SECURITY_PATCH_SYSTEM_STEP)
            
            _, failed_names = upgrade_systems(systems, upgrade_wrapper.system_img_tarball)
            if len(failed_names) > 0:
                raise RuntimeError(f"Security patching failed for {len(failed_names)} systems")

            batch.status = UpgradeBatchStatus.UPGRADE_SYSTEMS_COMPLETED.value
            batch.save()

            event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_SYSTEM_STEP, True)

        except (KeyboardInterrupt, Exception) as e:
            batch.status = UpgradeBatchStatus.UPGRADE_SYSTEMS_FAILED.value
            batch.save()
            event_logger.step_end(self.db_update_only, BATCH_SECURITY_PATCH_SYSTEM_STEP, False, str(e))
            raise RuntimeError(f"Failed to upgrade systems: {e}")

    def _handle_migrate_in(self, batch, upgrade_wrapper, creds, args):
        """Handle migration into destination cluster"""
        try:
            if self.db_update_only or self.skip_dest_cluster:
                batch.status = UpgradeBatchStatus.MIGRATE_IN_COMPLETED.value
                batch.save()
                return

            batch.status = UpgradeBatchStatus.MIGRATE_IN_IN_PROGRESS.value
            batch.save()

            event_logger.step_start(self.db_update_only, BATCH_MIGRATE_IN_STEP)

            upgrade_process = upgrade_wrapper.upgrade_process
            self.k8s_cluster_update(
                profile=self.profile,
                cluster_name=upgrade_process.dest_cluster.name,
                cluster_mgmt_node=creds.dnode_dest_cluster.name,
                user=creds.user_dest_cluster,
                password=creds.password_dest_cluster,
                operation="add",
                cluster_pkg_path=upgrade_wrapper.cluster_pkg_path,
            )
            if not self.validate_device_list(upgrade_process.dest_cluster):
                raise RuntimeError(f"Device count validation failed for destination cluster {upgrade_process.dest_cluster.name}")

            batch.status = UpgradeBatchStatus.MIGRATE_IN_COMPLETED.value
            batch.save()

            event_logger.step_end(self.db_update_only, BATCH_MIGRATE_IN_STEP, True)

        except (KeyboardInterrupt, Exception) as e:
            batch.status = UpgradeBatchStatus.MIGRATE_IN_FAILED.value
            batch.save()
            event_logger.step_end(self.db_update_only, BATCH_MIGRATE_IN_STEP, False, str(e))
            raise RuntimeError(f"Failed to migrate into destination cluster: {e}")
    
    def _handle_complete_batch_migration(self, batch, upgrade_wrapper, creds, args):
        """Handle completion of migrated batch/device status update"""
        try:
            event_logger.step_start(self.db_update_only, BATCH_UPDATE_BATCH_DEVICE_STATUS_STEP)
            devices, _, _ = self._fetch_devices(batch)
            for bd in devices:
                bd.status = UpgradeDeviceStatus.COMPLETED.value
                bd.current_step = "completed"
                bd.save()
            batch.status = UpgradeBatchStatus.MIGRATE_COMPLETED.value
            batch.save()
            event_logger.step_end(self.db_update_only, BATCH_UPDATE_BATCH_DEVICE_STATUS_STEP, True)
        except (KeyboardInterrupt, Exception) as e:
            batch.status = UpgradeBatchStatus.COMPLETE_BATCH_MIGRATION_FAILED.value
            batch.save()
            event_logger.step_end(self.db_update_only, BATCH_UPDATE_BATCH_DEVICE_STATUS_STEP, False, str(e))
            raise RuntimeError(f"Failed to update batch devices: {e}")

    def validate_device_list(self, cluster):
        """
        Validate the lists of servers and systems in the cluster are the same as the entries in the
        database.
        """
        dnode = get_k8s_lead_mgmt_node(self.profile, cluster.name)
        user = dnode.get_prop(props.prop_management_credentials_user)
        password = dnode.get_prop(props.prop_management_credentials_password)
        with KubernetesCtl(self.profile, dnode.name, user, password) as kctl:
            # Get the names of nodes and systems in the cluster
            server_names_str = kctl.csctl_get_node_names()
            system_names_str = kctl.csctl_get_system_names()
            server_names = server_names_str.split(",") if server_names_str else []
            system_names = system_names_str.split(",") if system_names_str else []

        # Get the expected names from the database
        expected_servers = ClusterDevice.objects.filter(
            cluster=cluster,
            device__device_type=DeploymentDeviceTypes.SERVER.value,
            device__profile__name=self.profile
        )
        expected_systems = ClusterDevice.objects.filter(
            cluster=cluster,
            device__device_type=DeploymentDeviceTypes.SYSTEM.value,
            device__profile__name=self.profile
        )
        expected_server_names = [server.device.name for server in expected_servers]
        expected_system_names = [system.device.name for system in expected_systems]

        servers_only_in_cluster = [name for name in server_names if name not in expected_server_names]
        systems_only_in_cluster = [name for name in system_names if name not in expected_system_names]

        servers_only_in_db = [name for name in expected_server_names if name not in server_names]
        systems_only_in_db = [name for name in expected_system_names if name not in system_names]

        if len(servers_only_in_cluster) > 0 or len(servers_only_in_db) > 0:
            logger.error(f"Server names mismatch in cluster {cluster.name}: only in cluster: {servers_only_in_cluster}, only in DB: {servers_only_in_db}")
            return False

        if len(systems_only_in_cluster) > 0 or len(systems_only_in_db) > 0:
            logger.error(f"System names mismatch in cluster {cluster.name}: only in cluster: {systems_only_in_cluster}, only in DB: {systems_only_in_db}")
            return False

        logger.info(f"Cluster {cluster.name} has expected {len(server_names)} servers and {len(system_names)} systems")
        return True

    def run(self, args):
        logger.info(f"Executing ClusterUpgradeBatchUpgradeRunMigrateCmd with args: {args}")
        upgrade_wrapper, batch, error_code = get_batch_and_upgrade_process(args.batch_id)
        if error_code:
            return error_code
        upgrade_process = upgrade_wrapper.upgrade_process

        source_cluster = upgrade_process.source_cluster
        dest_cluster = upgrade_process.dest_cluster
        creds = UpgradeCreds(self.profile, upgrade_process, skip_src_cluster=self.skip_src_cluster, skip_dest_cluster=self.skip_dest_cluster)

        if not args.noconfirm:
            logger.info(
                "About to migrate %d devices from cluster '%s' to '%s'.",
                ClusterUpgradeBatchDevice.objects.filter(upgrade_batch=batch).count(),
                source_cluster.name,
                dest_cluster.name,
            )
            if input("Are you sure you want to proceed? [y/N]: ").lower().strip() != "y":
                logger.info("Migration cancelled by user.")
                return 0

        # Initialize batch migration by setting status to MIGRATE_STARTED if either:
        # 1. The batch is in QUIESCE_COMPLETED state (normal flow) or
        # 2. Force flag is set AND batch is not yet completed AND no handler exists for current status
        #    (allows recovery from unknown states when using --force)
        if batch.status == UpgradeBatchStatus.MIGRATE_COMPLETED.value:
            pass
        elif batch.status == UpgradeBatchStatus.QUIESCE_COMPLETED.value or (args.force and self.handlers.get(batch.status) is None):
            logger.info(f"Setting batch {batch.id} status to MIGRATE_STARTED")
            batch.status = UpgradeBatchStatus.MIGRATE_STARTED.value
            batch.save()

        event_logger.set_upgrade_id(self.db_update_only, upgrade_process.id)
        event_logger.step_start(self.db_update_only, BATCH_MIGRATE_STEP)

        # Main state machine loop
        try:
            while batch.status != UpgradeBatchStatus.MIGRATE_COMPLETED.value:
                handler = self.handlers.get(batch.status)
                if handler is None:
                    raise RuntimeError(f"No handler for state {batch.status.value}")

                # Call handler with standardized parameters
                handler(batch, upgrade_wrapper, creds, args)

                # If ONE_STEP_ONLY is set, return after one state transition
                if self.one_step_only:
                    logger.info("ONE_STEP_ONLY set, stopping after one state transition")
                    return 0
            
            logger.info(f"Batch {batch.id} migrated successfully.")
            event_logger.step_end(self.db_update_only, BATCH_MIGRATE_STEP, True)

        except (KeyboardInterrupt, Exception) as e:
            event_logger.step_end(self.db_update_only, BATCH_MIGRATE_STEP, False, str(e))
            logger.error("Batch %d migration failed in state '%s': %s", batch.id, batch.status, e)
            return 1


class ClusterUpgradeBatchCmd(SubCommandABC):
    """Manage batches of devices for an upgrade process (create, taint, etc.)."""
    name = "batch"
    COMMANDS = [
        ClusterUpgradeBatchCreateCmd,
        ClusterUpgradeBatchAddDeviceCmd,
        ClusterUpgradeBatchRemoveDeviceCmd,
        ClusterUpgradeBatchShowCmd,
        ClusterUpgradeBatchRunTaintCmd,
        ClusterUpgradeBatchRunQuiesceCmd,
        ClusterUpgradeBatchRunSecurityPatchNodesCmd,
        ClusterUpgradeBatchRunUpgradeSystemsCmd,
        ClusterUpgradeBatchRunMigrateCmd
    ]

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = ("Commands to create and manage high-level batch operations "
                            "(create, taint, quiesce, migrate).")

    def construct(self):
        subparsers = self.parser.add_subparsers(dest="batch_action", required=True, title="Batch Upgrade Actions")
        for cmd_class in self.COMMANDS:
            m = cmd_class(subparsers, profile=self.profile, cli_instance=self.cli_instance)
            m.build()

    def run(self, args):
        # Stepwise logging for the batch action (if any)
        upgrade_id = getattr(args, "upgrade_id", None)
        batch_action = getattr(args, "batch_action", None)

        if upgrade_id is None:
            upgrade_process, batch, error_code = get_batch_and_upgrade_process(args.batch_id)
            if error_code:
                return error_code
            upgrade_id = upgrade_process.id

        event_logger.set_upgrade_id(upgrade_id)

        # Check if we have a function to execute
        if not hasattr(args, "func"):
            self.parser.print_help()
            return 1

        # Define steps that don't use standard logging as these steps use a custom failure message
        CUSTOM_LOGGING_STEPS = {
            BATCH_QUIESCE_STEP,
            BATCH_SECURITY_PATCH_NODE_STEP,
            BATCH_SECURITY_PATCH_SYSTEM_STEP,
            BATCH_MIGRATE_STEP
        }

        step_name_in_event_logger = f"batch_{batch_action}"
        uses_standard_logging = step_name_in_event_logger not in CUSTOM_LOGGING_STEPS and not self.db_update_only

        if uses_standard_logging:
            event_logger.step_start(batch_action)

        try:
            result = args.func(args)

            # End logging on success (logs regardless of upgrade_id/batch_action presence)
            if uses_standard_logging:
                event_logger.step_end(step_name_in_event_logger, result == 0)

            return result

        except Exception as e:
            # End logging on failure (logs regardless of upgrade_id/batch_action presence)
            if uses_standard_logging:
                event_logger.step_end(step_name_in_event_logger, False, error_message=str(e))
            raise
        batch_name = f"batch_{batch_action}" if batch_action else "batch"
        # Try to fetch upgrade_id for logging, if possible
        # Only log step if there is an upgrade_id and an action
        if upgrade_id is not None and batch_action:
            event_logger = UpgradeEventLogger()
            event_logger.set_upgrade_id(upgrade_id)
            event_logger.step_start(batch_name)
            try:
                result = args.func(args)
                event_logger.step_end(batch_name, result="success" if result == 0 else "error")
                return result
            except Exception as e:
                event_logger.step_end(batch_name, result="error", error_message=str(e))
                raise
        elif hasattr(args, "func"):
            return args.func(args)
        else:
            self.parser.print_help()
            return 1
