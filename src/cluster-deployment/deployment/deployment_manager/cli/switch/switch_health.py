from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.health import (
    HealthShow,
    HealthCheck,
    HealthForceUpdate,
    AddDeviceType,
)
from deployment_manager.db.const import DeploymentDeviceTypes


@AddDeviceType(DeploymentDeviceTypes.SWITCH.value)
class SwitchHealthShow(HealthShow):
    """
    Show switch health
    """
    pass


@AddDeviceType(DeploymentDeviceTypes.SWITCH.value)
class SwitchHealthCheck(HealthCheck):
    """
    Run health check on switch
    """
    pass


@AddDeviceType(DeploymentDeviceTypes.SWITCH.value)
class SwitchHealthForceUpdate(HealthForceUpdate):
    """
    Force update switch health
    """
    pass


class SwitchHealth(SubCommandABC):
    """
    Switch health related actions
    """

    SUB_CMDS = [SwitchHealthShow, SwitchHealthCheck, SwitchHealthForceUpdate]

    name = "health"

    def construct(self):
        subparsers = self.parser.add_subparsers(dest="action", required=True)
        for ssc in self.SUB_CMDS:
            m = ssc(
                subparsers, profile=self.profile, cli_instance=self.cli_instance
            )
            m.build()

    def run(self, args):
        if hasattr(args, "func"):
            args.func(args)

