import logging
import os
from pathlib import Path

import yaml

from deployment_manager.cli.profile import create_profile, import_profile, profiles_table
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.common import models as models
from deployment_manager.db.models import DeploymentProfile
from deployment_manager.tools.config import ClusterProfile, ConfGen
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)


class Change(SubCommandABC):
    """ Change the current cluster profile """

    name = "change"

    def construct(self):
        self.parser.add_argument("profile",
                                 choices=[p.name for p in DeploymentProfile.list()],
                                 help="name of the profile")

    def run(self, args):
        self.cli_instance.set_profile(args.profile)


class Create(SubCommandABC):
    """ Create a new cluster profile if not exists """
    name = "create"

    def construct(self):
        self.parser.add_argument("profile", help="name of the new profile")
        self.parser.add_argument(
            '--config_input',
            help='Required location of the cluster config input.',
            type=str,
            required=True,
        )
        self.parser.add_argument(
            '--inventory_file',
            help='Optional location of the cluster inventory file. '
                 'You can import devices later with `device import_inventory`',
            type=str
        )

    def run(self, args):
        err = create_profile(args.profile, args.config_input, args.inventory_file, self.cli_instance)
        if err:
            logger.error(err)
            return 1
        return 0


class Import(SubCommandABC):
    """ Import profile from k8s cluster definition. Note: no deployment commands can be run on imported profiles """
    name = "import"

    def construct(self):
        self.parser.add_argument('profile', help='Name to be given to the imported profile')
        self.parser.add_argument(
            '--network_config',
            help='Specify the location of the network configuration JSON file',
            type=str
        )

    def run(self, args):
        err = import_profile(args.profile, args.network_config, self.cli_instance)
        if err:
            logger.error(err)
            return 1
        return 0


class Delete(SubCommandABC):
    """ Delete a cluster profile """
    name = "delete"

    def construct(self):
        self.parser.add_argument('profile', help='Name of profile to delete')

    def run(self, args):
        if ClusterProfile.get(args.profile):
            ClusterProfile.delete(args.profile)
        p = DeploymentProfile.get_profile(args.profile)
        if p is None:
            logger.info(f"Profile {args.profile} not found")
            return 0

        p.delete()
        # If we are deleting the current profile, need to replace it with another
        if self.cli_instance.current_profile == args.profile:
            other_profile = DeploymentProfile.objects.first()
            if other_profile:
                self.cli_instance.set_profile(other_profile.name)
                logger.warning(f"set current profile to {other_profile.name}")
            else:
                logger.warning("only profile deleted. You'll need to create a new profile with 'cscfg profile create'")


class Show(SubCommandABC):
    """ Show all profiles """

    name = "show"

    def construct(self):
        pass

    def run(self, args):
        profiles = profiles_table(self.profile)
        if not profiles:
            print(
                "No profiles created. Use 'profile create' to create one."
            )
            self.cli_instance.returncode = 1
            return
        print(profiles)


class Config(SubCommandABC):
    """ Show/edit/create config for the current profile. This is the 'input.yml' file """

    name = "config"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cg = ConfGen(self.profile)

    def construct(self):
        subparsers = self.parser.add_subparsers(title='actions', dest='action')
        subparsers.add_parser('edit', help='Edit config')
        subparsers.add_parser('show', help='Show config')
        subparsers.add_parser('path', help='Show config location')
        parser_create = subparsers.add_parser('create', help='Create config template.')
        parser_create.add_argument("--output", "-o",
                                   help="Output path. '-' redirects to stdout",
                                   default="-")

    def _edit_cfg(self):
        print("This will force network configuration to be reinitialized.")
        if not prompt_confirm("Continue?"):
            return 0
        # TODO: forcing network reconfiguration seems too heavy-handed
        if ClusterProfile.edit_config(self.profile):
            if not self._cg.write_master_config():
                logger.info("Failed to create master-config")
                return 1
        return 0

    def run(self, args):
        if args.action == "show":
            ClusterProfile.show_config(self.profile)
        elif args.action == "path":
            _show_file_path(self._cg.input_file)
        elif args.action == "edit":
            return self._edit_cfg()
        elif args.action == "create":
            default_conf = models.ClusterDeploymentConfig()
            default_conf.basic.name = self.profile
            d = default_conf.model_dump()
            input_contents = yaml.safe_dump(d)
            if args.output == "-":
                print(input_contents)
            else:
                with open(args.output, "w") as f:
                    f.write(input_contents)
        else:
            raise NotImplementedError(f"command not implemented for {args.ACTION} {args.FILE}")


class NetworkConfig(SubCommandABC):
    """ Show the current profile's network config """

    name = "network_config"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cg = ConfGen(self.profile)

    def construct(self):
        subparsers = self.parser.add_subparsers(title='actions', dest='action')
        subparsers.add_parser('show', help='Show config')
        subparsers.add_parser('path', help='Show config location')

    def run(self, args):
        if args.action == "show":
            ClusterProfile.show_network_config(self.profile)
        elif args.action == "path":
            _show_file_path(self._cg.network_config_file)
        else:
            raise NotImplementedError(f"command not implemented for {args.ACTION} {args.FILE}")


class Update(SubCommandABC):
    """ Update properties of the current cluster profile """
    name = "update"

    def construct(self):
        self.parser.add_argument('--deployable', required=True,
                                help='Set whether profile is deployable',
                                choices=('yes', 'no'))

    def run(self, args):
        # get confgen
        # if input.yaml exists and deployable is yes, mark deployable
        p = DeploymentProfile.get_profile(self.profile)
        if p is None:
            logger.error(f"Profile {self.profile} not found")
            return 1
        if args.deployable == "yes":
            cg = ConfGen(self.profile)
            if os.path.isfile(cg.input_file):
                p.mark_deployable()
            else:
                logger.error(
                    f"{self.profile} cannot be marked deployable since corresponding input file does not exist"
                )
                return 1
        else:
            p.mark_not_deployable()
        logger.info(f"{self.profile} updated successfully")

def _show_file_path(file: str):
    logger.info(Path(file).absolute())


CMDS = [
    Create,
    Change,
    Delete,
    Import,
    Show,
    Config,
    NetworkConfig,
    Update,
]
