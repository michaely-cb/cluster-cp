import logging
from tabulate import tabulate

from deployment_manager.cli.deploy.utils import DeviceDeployRepr
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.models import (
    DeploymentCtx,
    DeploymentProfile,
    Device,
)

logger = logging.getLogger(__name__)


class ShowDeployment(SubCommandABC):
    """ Show deployment details """
    name = "show"

    def construct(self):
        self.parser.add_argument("--profile",
                                 help="Name of a saved cluster profile")
        self.add_arg_filter(False)
        self.add_arg_output_format()

    def run(self, args):
        profilename = self.profile
        if args.profile:
            profilename = args.profile

        p = DeploymentProfile.get_profile(profilename)
        if p is None:
            print(f"Profile {profilename} does not exist")
            print("Use 'profile create' to create one")
            self.cli_instance.returncode = 1
            return

        devices = self.filter_devices(args, query_set=Device.get_all(self.profile))
        devices = devices.order_by("device_type", "device_role", "name")

        if args.output == "table":
            ctx = DeploymentCtx.get_ctx(profilename)
            print(f"Status: {ctx.status_str}\n")
            if ctx.current_stage is not None:
                headers = ["Stage", "Section", "Status"]
                rows = [[
                    ctx.current_stage.name,
                    ctx.current_stage.section_str,
                    ctx.current_stage.status_str
                ]]
                print(tabulate(rows, headers))
                print("\n")

        out = DeviceDeployRepr.format_reprs([DeviceDeployRepr.from_device(d) for d in devices], args.output)
        print(out)
