from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.inventory.repr import Rack
from deployment_manager.cli.inventory.filter import Filter
# from deployment_manager.db.models import DeploymentProfile
from deployment_manager.db.models import RackEntity
# import logging
# from typing import List, Dict
import deployment_manager.cli.inventory.service as service

class Show(SubCommandABC):
    """Show existing racks"""
    name = "show"
    
    def construct(self):
        self.add_arg_output_format()
        self.add_arg_filter()

    def run(self, args):
        fmt = args.output
        profile = service.get_profile(self.profile)

        # rack_entities_query_set = (
        #     RackEntity.objects
        #     .filter(profile=DeploymentProfile.get_profile(self.profile))
        # )

        query_set = service.get_query_set(profile, RackEntity)
        
        rack_entities = Filter.filter_devices(
            RackEntity, args, query_set=query_set
        )

        racks = []
        for rack_entity in rack_entities:
            rack = service.rack_entity_to_repr(rack_entity)
            racks.append(rack)
        
        print(Rack.format_reprs(racks, fmt=fmt))
        return 0