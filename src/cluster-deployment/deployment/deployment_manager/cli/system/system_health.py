from deployment_manager.cli.health import (
    HealthShow,
    HealthCheck,
    HealthForceUpdate,
    AddDeviceType,
)
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import DeploymentDeviceTypes


@AddDeviceType(DeploymentDeviceTypes.SYSTEM.value)
class SystemHealthShow(HealthShow):
    """
    Show system health
    """
    pass


@AddDeviceType(DeploymentDeviceTypes.SYSTEM.value)
class SystemHealthCheck(HealthCheck):
    """
    Run health check on systems
    """
    pass


@AddDeviceType(DeploymentDeviceTypes.SYSTEM.value)
class SystemHealthForceUpdate(HealthForceUpdate):
    """
    Force update system health
    """
    pass


class SystemHealth(SubCommandABC):
    """
    System health related actions
    """

    SUB_CMDS = [SystemHealthShow, SystemHealthCheck, SystemHealthForceUpdate]

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
