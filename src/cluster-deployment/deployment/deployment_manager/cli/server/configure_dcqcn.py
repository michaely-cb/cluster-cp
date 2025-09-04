import logging

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import DeploymentDeviceRoles, DeploymentDeviceTypes
from deployment_manager.db.models import Device
from deployment_manager.tools.pb1.ansible_task import ConfigureDcqcn as dcqcn_task
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)


class ConfigureDcqcn(SubCommandABC):
    """
    Configure Mellanox NICs on servers in the cluster to enable/disable DCQCN
    """
    name = "configure_dcqcn"

    def construct(self):
        self.parser.add_argument(
            "--action", required=True,
            type=str,
            choices=("enable", "disable"),
            help="Choose whether command should enable/disable DCQCN"
        )
        self.add_arg_noconfirm()
        self.add_arg_filter(required=True)

    def run(self, args):
        roles = [
            DeploymentDeviceRoles.ACTIVATION.value,
            DeploymentDeviceRoles.MEMORYX.value,
            DeploymentDeviceRoles.SWARMX.value,
        ]
        targets = self.filter_devices(
            args,
            device_type=DeploymentDeviceTypes.SERVER.value,
            query_set=Device.get_all(self.profile).filter(device_role__in=roles),
        )
        if not targets:
            logger.info("No targets matching filter")
            return 0

        names = [t.name for t in targets]

        print(f"DCQCN will be {args.action}d on")
        print("\n".join(names))

        if not args.noconfirm and not prompt_confirm("Continue?"):
            return 0

        dcqcn_task(self.profile, args.action).run(names)
