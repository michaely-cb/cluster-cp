from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.inventory.repr import Elevation
from deployment_manager.db.models import ElevationEntity
from deployment_manager.cli.inventory.filter import Filter
import logging
import deployment_manager.cli.inventory.service as service


logger = logging.getLogger(__name__)

class Show(SubCommandABC):
    """Show existing elevations"""
    name = "show"

    def construct(self):
        self.add_arg_output_format()
        self.add_arg_filter()

    def run(self, args):
        fmt = args.output
        profile = service.get_profile(self.profile)

        query_set = service.get_query_set(profile, ElevationEntity)
        
        elevation_entities = Filter.filter_devices(
            ElevationEntity, args, query_set=query_set
        )
        
        elevations = []
        for elevation_entity in elevation_entities:
            elevation = service.elevation_entity_to_repr(elevation_entity)
            elevations.append(elevation)
        
        print(Elevation.format_reprs(elevations, fmt=fmt))
        return 0