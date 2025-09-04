import logging
import pathlib
import tempfile
import yaml

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.tools.pb1.ansible_task import (
    generate_full_inventory,
)


logger = logging.getLogger(__name__)

class GenerateInventory(SubCommandABC):
    """
    Generate full ansible inventory
    """

    name = "generate_inventory"

    def construct(self):
        self.parser.add_argument(
            "--output_dir",
            default=None,
            help="Location to write ansible inventory file",
        )

    def run(self, args):

        user_provided_dir = args.output_dir is not None
        if user_provided_dir:
            output_path = pathlib.Path(args.output_dir)
            if not output_path.is_dir():
                raise ValueError(
                    f"output path {output_path} is not a directory"
                )
        else:
            output_path = pathlib.Path(
                tempfile.mkdtemp(prefix="ansible_inventory")
            )

        outfile = output_path / "hosts.yml"
        outfile.write_text(
            yaml.dump(
                generate_full_inventory(self.profile),
                indent=4,
                default_flow_style=False,
            )
        )

        print(f"files written to {output_path}/")


CMDS = [
    GenerateInventory,
]
