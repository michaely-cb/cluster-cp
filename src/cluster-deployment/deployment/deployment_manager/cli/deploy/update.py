import logging

from deployment_manager.cli.deploy.utils import DeviceDeployRepr
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import (DeploymentStageStatus, DeviceDeploymentStages)
from deployment_manager.db.models import Device
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)


class UpdateDeployStatus(SubCommandABC):
    """ Update devices' deployment status """
    name = "update"

    def construct(self):
        self.parser.add_argument(
            "--name",
            type=str,
            nargs="+",
            help="Device names. If not set, all devices are updated"
        )
        self.parser.add_argument(
            "--stage",
            required=True,
            type=str,
            choices=[d.name for d in DeviceDeploymentStages]
        )
        self.parser.add_argument(
            "--status",
            required=True,
            type=str,
            choices=[d.name for d in DeploymentStageStatus]
        )
        self.add_arg_output_format()
        self.add_arg_filter(required=False)
        self.add_arg_noconfirm(help="Update status without confirmation prompt")

    def run(self, args):
        if args.name and args.filter:
            logger.error("Both '--name' and '--filter' cannot be set at the same time")
            return 1
        devices = self.filter_devices(args, name=args.name, query_set=Device.get_all(self.profile))
        if args.name and len(devices) != len(args.name):
            actual = set([d.name for d in devices])
            expected = set(args.name)
            expected.difference_update(actual)
            logger.error(f"devices {', '.join(expected)} do not exist")
            return 1

        if not args.noconfirm:
            out = DeviceDeployRepr.format_reprs([DeviceDeployRepr.from_device(d) for d in devices], args.output)
            print(out)
            print(f"Deployment status for these devices will be set to {args.stage} {args.status}")
            if not prompt_confirm("Continue?"):
                return 0

        reprs = []
        for device in devices:
            device.update_stage(
                DeviceDeploymentStages[args.stage].value,
                DeploymentStageStatus[args.status].value,
                force=True
            )
            reprs.append(DeviceDeployRepr.from_device(device))
        out = DeviceDeployRepr.format_reprs([DeviceDeployRepr.from_device(d) for d in devices], args.output)
        print(out)
