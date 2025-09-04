from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.common.representation import parse_reprs_file
from deployment_manager.cli.inventory.repr import Rack
from django.db import transaction
import logging
import textwrap
import deployment_manager.cli.inventory.service as service


logger = logging.getLogger(__name__)

class Add(SubCommandABC):
    """Add a new rack"""
    name = "add"
    description = """
    An rack is the physical implementation of an elevation.
    This command allows for cscfg to add these rack models to it's db.
    
    Here is an example rack:
    
    - name: test_rack
      elevation_name: test_elevation
      part_number: 000-00-0000
      properties:
        location:
          position: '01'
          stamp: cg6
    
    An elevation consists of it's name and a list.
    It must have an elevation_name which references a valid elevation in the db.
    It also has a part number, which corresponds to the actual rack part number.
    In addition, it requires the position and stamp attributes of properties.location to exist (although they can be null).
    
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, description=self.description, **kwargs)

    def construct(self):
        self.parser.add_argument(
            "FILE_NAME",
            help=textwrap.dedent("The input filename").strip()
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
        output_fmt = args.output

        if args.input_format or '.' in filename:
            fmt = args.input_format if args.input_format else filename.split('.')[-1]
        else:
            logger.error("File extension not determined. Either pass in via -i or --input-format or rename file to include it's extension")
            return 1

        racks, parse_errors = parse_reprs_file(Rack, filename, fmt=fmt)
        if parse_errors:
            for error in parse_errors:
                logger.error(error)
            return 1

        validate_errors = []
        for rack in racks:
            validate_errors.extend(service.validate_rack(rack, profile))
        if validate_errors:
            for error in validate_errors:
                logger.error(error)
            return 1
        
        added_racks = []
        try:
            with transaction.atomic():
                for rack in racks:
                    service.add_rack(rack, profile)
                    added_racks.append(rack)

        except Exception as e:
            logger.error(f"Error adding racks: {str(e)}")
            return 1
        
        print(Rack.format_reprs(added_racks, fmt=output_fmt))
        return 0