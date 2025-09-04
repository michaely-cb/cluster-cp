from deployment_manager.common.representation import parse_reprs_file
from deployment_manager.cli.inventory.repr import Elevation
from deployment_manager.cli.subcommand import SubCommandABC
import logging
from django.db import transaction
import deployment_manager.cli.inventory.service as service


logger = logging.getLogger(__name__)

class Add(SubCommandABC):
    """Add new elevations"""
    name = "add"
    description = """
    An elevation defines the device composition of a given datacenter rack.
    It has information about which devices are in the rack, as well as device-specific properties.
    This command allows for cscfg to add these elevation models to it's db.
    
    Here is an example elevation:
    
    - name: test_elevation
      device_specs:
      - type: SR
        role: SX
        count: 8
      - type: SR
        role: AX
        name_format: "{location.rack}-axxl-sr{index}"
        properties:
          vendor:
            name: DL
        count: 4
    
    An elevation consists of it's name and a list of device_specs.
    Each device spec must contain the device type and role.
    Optionally it can also contain name_format which is used to name the device in a custom manner.
    'index' is a recogonized keyword here which takes on values in the range from 1 to 'count'
    In addition, you can add 'vendor' device properties to the 'properties' attribute in the device spec.
    
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, description=self.description, **kwargs)
    
    def construct(self):
        self.parser.add_argument(
            "FILE_NAME",
            help="The input filename for the elevations"
        )
        self.parser.add_argument(
            "-i", "--input-format",
            default=None,
            choices=["yaml", "json", "csv"],
            help="Input format; if omitted, inferred from file extension"
        )
        self.add_arg_output_format()

    def run(self, args):
        filename = args.FILE_NAME
        profile = service.get_profile(self.profile)

        if args.input_format or '.' in filename:
            fmt = args.input_format if args.input_format else filename.split('.')[-1]
        else:
            logger.error(
                "File extension not determined. Either pass in via -i/--input-format or rename file to include its extension"
            )
            return 1

        output_fmt = args.output
        elevations, parse_errors = parse_reprs_file(Elevation, filename, fmt=fmt)
        if parse_errors:
            for error in parse_errors:
                logger.error(error)
            return 1
        
        validate_errors = []
        for elevation in elevations:
            validate_errors.extend(service.validate_elevation(elevation))
        if validate_errors:
            for error in validate_errors:
                logger.error(error)
            return 1

        added_elevations = []
        try:
            with transaction.atomic():
                added_elevations = []
                for elevation in elevations:
                    service.add_elevation(elevation, profile)
                    added_elevations.append(elevation)
        except Exception as e:
            logger.error(str(e))
            return 1
        
        print(Elevation.format_reprs(added_elevations, fmt=output_fmt))
        return 0