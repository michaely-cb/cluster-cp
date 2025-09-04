from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.switch.link.link import LinkShow, LinkImport, LinkAdd, LinkRemove, LinkStatus, LinkDetectMacs
from deployment_manager.cli.switch.link.lldp import LinkLldpStatus
from deployment_manager.cli.switch.link.sync import LinkSync

class LinkCmd(SubCommandABC):
    """ Switch link related actions """

    SUB_CMDS = [LinkShow, LinkImport, LinkAdd, LinkRemove, LinkSync, LinkLldpStatus, LinkStatus, LinkDetectMacs]

    name = "link"

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