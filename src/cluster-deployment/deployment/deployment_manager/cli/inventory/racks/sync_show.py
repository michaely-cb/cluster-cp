from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.models import RackEntity
from deployment_manager.cli.inventory.filter import Filter
from deployment_manager.cli.device.utils import DeviceRepr
import logging
import deployment_manager.cli.inventory.service as service


logger = logging.getLogger(__name__)

class SyncShow(SubCommandABC):
    """Show the devices associated with a given rack"""
    name = "sync-show"

    def construct(self):
        self.add_arg_output_format()
        self.add_arg_filter()

    def run(self, args):
        profile = service.get_profile(self.profile)
        fmt = args.output

        query_set = service.get_query_set(profile, RackEntity)
        
        rack_entities = Filter.filter_devices(
            RackEntity, args, query_set=query_set
        )

        racks = [service.rack_entity_to_repr(rack_entity) for rack_entity in rack_entities]

        all_devices = []
        all_errors = []
        for rack in racks:
            devices, errors = service.get_sync_devices(rack, profile)
            all_devices.extend(devices)
            all_errors.extend(errors)
        
        if all_errors:
            for error in all_errors:
                logger.error(error)
            return 1

        all_devices.sort(key=lambda d: (d.type, d.role, d.name,))
        print(DeviceRepr.format_reprs(all_devices, fmt))
        return 0