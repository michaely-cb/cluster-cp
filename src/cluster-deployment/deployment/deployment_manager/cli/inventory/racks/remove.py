from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.device.utils import DeviceRepr
from deployment_manager.common import logger as cl
import logging
import deployment_manager.cli.inventory.service as service

logger = logging.getLogger(__name__)

class Remove(SubCommandABC):
    """Remove an existing rack"""
    name = "remove"

    def construct(self):
        self.parser.add_argument(
            "RACK_NAME",
            help="The rack name"
        )

    def run(self, args):
        name = args.RACK_NAME
        profile = service.get_profile(self.profile)

        if not service.rack_already_exists(name, profile):
            logger.error(f"Rack with name '{name}' does not exist")
            return 1 
        
        devices = service.get_devices_in_rack(profile, name)

        if devices:
            logger.error(f"There are currently {len(devices)} devices associated with this rack '{name}'")
            logger.error(f"The devices below must be removed before deleting the rack '{name}'")
            print(DeviceRepr.format_reprs(devices))
            return 1

        service.remove_rack(name, profile)
        cl.force_log(logger, logging.INFO, f"Rack {name} removed successfully.")
        return 0