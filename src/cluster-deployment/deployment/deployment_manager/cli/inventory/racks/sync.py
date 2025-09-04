from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.models import RackEntity
from deployment_manager.cli.inventory.filter import Filter
import logging
import deployment_manager.cli.inventory.service as service
from deployment_manager.cli.device.utils import DeviceRepr
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)

class Sync(SubCommandABC):
    """Sync the generated devices from the given elevations/racks"""
    name = "sync"

    def construct(self):
        self.add_arg_output_format()
        self.add_arg_filter()
        self.parser.add_argument(
            "--noconfirm",
            "-y",
            default=False,
            action="store_true",
            help="Remove conflicting devices and regenerate them (they will become stubs)",
        )
        self.parser.add_argument("-m", "--mode",
            help="Choose to whether to ignore existing devices that conflict, or to regenerate them as stubs.",
            choices=["skip", "overwrite"],
            default=None,
        )

    def run(self, args):
        profile = service.get_profile(self.profile)
        fmt = args.output

        query_set = service.get_query_set(profile, RackEntity)
        rack_entities = Filter.filter_devices(RackEntity, args, query_set=query_set)
        racks = [service.rack_entity_to_repr(r) for r in rack_entities]

        all_sync_devices, all_existing_devices, all_errors = [], [], []
        for rack in racks:
            sync, existing, errors = service.get_devices_to_sync(rack, profile)
            all_sync_devices.extend(sync)
            all_existing_devices.extend(existing)
            all_errors.extend(errors)

        if all_errors:
            for error in all_errors:
                logger.error(error)
            return 1

        devices_to_add = []

        if all_existing_devices:
            logger.info("These devices already exist in the inventory and conflict with the devices to be synced:")
            all_existing_devices.sort(key=lambda d: (d.type, d.role, d.name,))
            for existing_device in all_existing_devices:
                print(existing_device.name)

            if args.mode not in ["skip", "overwrite"]:
                logger.error("Must specify -m or --mode flag with argument either 'skip' or 'overwrite' to proceed")
                return 1

            if args.mode == "skip":
                existing_names = {d.name for d in all_existing_devices}
                devices_to_add = [d for d in all_sync_devices if d.name not in existing_names]
            elif args.mode == "overwrite":
                devices_to_add = all_sync_devices + all_existing_devices
            
        else:
            devices_to_add = all_sync_devices

        if not devices_to_add:
            logger.info("No devices to be added")
            return 0
            
        logger.info("These devices will be added to the database")
        devices_to_add.sort(key=lambda d: (d.type, d.role, d.name,))
        for device in devices_to_add:
            print(device.name)

        if not args.noconfirm and not prompt_confirm("Continue?"):
            print("Aborted")
            return 0

        service.add_devices(devices_to_add, profile)
        print(DeviceRepr.format_reprs(devices_to_add, fmt))
        return 0