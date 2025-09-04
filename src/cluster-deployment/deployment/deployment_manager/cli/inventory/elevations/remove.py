from deployment_manager.cli.subcommand import SubCommandABC
import logging
from deployment_manager.common import logger as cl
import deployment_manager.cli.inventory.service as service


logger = logging.getLogger(__name__)

class Remove(SubCommandABC):
    """Remove an existing elevation"""
    name = "remove"

    def construct(self):
        self.parser.add_argument(
            "ELEVATION_NAME",
            help="The elevation name"
        )

    def run(self, args):
        name = args.ELEVATION_NAME
        profile = service.get_profile(self.profile)

        if not service.elevation_already_exists(name, profile):
            logger.error(f"Elevation '{name}' not found.")
            return 1
        
        racks = service.get_racks_for_elevation(name, profile)

        if racks.exists():
            print(f"To delete this elevation '{name}' the following racks must be removed first")
            for rack in racks:
                print(f"  - Rack Name: {rack.name}")
            return 1

        service.remove_elevation(name, profile)
        cl.force_log(logger, logging.INFO, f"Elevation {name} removed successfully.")

        return 0
