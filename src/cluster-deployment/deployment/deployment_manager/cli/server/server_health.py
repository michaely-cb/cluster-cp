from deployment_manager.cli.health import (
    HealthShow,
    HealthCheck,
    HealthForceUpdate,
    AddDeviceType,
)
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import DeploymentDeviceTypes


@AddDeviceType(DeploymentDeviceTypes.SERVER.value)
class ServerHealthShow(HealthShow):
    """
    Show server health
    """
    pass


@AddDeviceType(DeploymentDeviceTypes.SERVER.value)
class ServerHealthCheck(HealthCheck):
    """
    Run health check on servers
    """
    pass


@AddDeviceType(DeploymentDeviceTypes.SERVER.value)
class ServerHealthForceUpdate(HealthForceUpdate):
    """
    Force update server health
    """
    pass


class ServerHealth(SubCommandABC):
    """
    Server health related actions
    """

    SUB_CMDS = [ServerHealthShow, ServerHealthCheck, ServerHealthForceUpdate]

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
