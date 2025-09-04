from deployment_manager.cli.subcommand import SubCommandABC


class ElevationsCmd(SubCommandABC):
    """ Elevations commands """

    from deployment_manager.cli.inventory.elevations.add import Add
    from deployment_manager.cli.inventory.elevations.edit import Edit
    from deployment_manager.cli.inventory.elevations.show import Show
    from deployment_manager.cli.inventory.elevations.remove import Remove

    COMMANDS = [Add, Edit, Show, Remove]

    name = "elevations"

    def construct(self):
        subparsers = self.parser.add_subparsers(dest="action", required=True)
        for cls in self.COMMANDS:
            m = cls(
                subparsers, profile=self.profile, cli_instance=self.cli_instance
            )
            m.build()

    def run(self, args):
        if hasattr(args, "func"):
            return args.func(args)
        else:
            self.parser.print_help()
            return 1


class RacksCmd(SubCommandABC):
    """ Racks commands """

    from deployment_manager.cli.inventory.racks.add import Add
    from deployment_manager.cli.inventory.racks.edit import Edit
    from deployment_manager.cli.inventory.racks.show import Show
    from deployment_manager.cli.inventory.racks.remove import Remove
    from deployment_manager.cli.inventory.racks.sync_show import SyncShow
    from deployment_manager.cli.inventory.racks.sync import Sync

    COMMANDS = [Add, Edit, Show, Remove, SyncShow, Sync]

    name = "racks"

    def construct(self):
        subparsers = self.parser.add_subparsers(dest="action", required=True)
        for cls in self.COMMANDS:
            m = cls(
                subparsers, profile=self.profile, cli_instance=self.cli_instance
            )
            m.build()

    def run(self, args):
        if hasattr(args, "func"):
            return args.func(args)
        else:
            self.parser.print_help()
            return 1