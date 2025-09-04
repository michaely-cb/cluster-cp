import argparse
import cmd
import datetime
import json
import logging
import os
import readline
import shlex
import textwrap
from typing import Optional

import deployment_manager.cli.cluster as cmd_cluster
import deployment_manager.cli.cmd_ansible as cmd_ansible
import deployment_manager.cli.cmd_cbinfra as cmd_cbinfra
import deployment_manager.cli.cmd_cluster_mgmt as cmd_cluster_mgmt
import deployment_manager.cli.cmd_cluster_platform as cmd_cluster_platform
import deployment_manager.cli.cmd_dev as cmd_dev
import deployment_manager.cli.cmd_profile as cmd_profile
import deployment_manager.cli.cmd_validate as cmd_validate
import deployment_manager.cli.deploy as cmd_deploy
import deployment_manager.cli.device as cmd_device
import deployment_manager.cli.cmd_peripherals as cmd_peripherals
import deployment_manager.cli.dhcp as cmd_dhcp
import deployment_manager.cli.inventory as cmd_inventory
import deployment_manager.cli.k8s as cmd_k8s
import deployment_manager.cli.network as cmd_network
import deployment_manager.cli.server as cmd_server
import deployment_manager.cli.switch as cmd_switch
import deployment_manager.cli.system as cmd_system
import deployment_manager.flow.flow as cmd_flows
import deployment_manager.cli.cmd_bom as cmd_bom
import deployment_manager.cli.cmd_backup as cmd_backup
from deployment_manager.cli.profile import ProfileUserPrompts, ProfileManager
from deployment_manager.db.models import (
    DeploymentProfile
)
from deployment_manager.tools.config import (
    ConfGen,
    init_profile_deployability
)
from deployment_manager.tools.const import (
    CSCFG_CONFIG
)
from deployment_manager.tools.utils import (
    get_version
)

logger = logging.getLogger(__name__)

class CSConfigCLI(cmd.Cmd, ProfileManager):
    base_prompt = "CSCFG"
    with_exec = False

    # cscfg config
    config = {}

    def __init__(self, profile: Optional[str] = None) -> None:
        super().__init__()
        # The exit code of the last command executed
        # This is mainly used for the non-interactive mode
        self.returncode = 0

        if profile is not None:
            self.current_profile = profile
        else:
            self.current_profile = self.get_current_profile()

    @property
    def prompt(self):
        now = datetime.datetime.now()
        ts = now.strftime("%m/%d %H:%M:%S")
        prompt = f"{ts} {self.base_prompt} {self.current_profile}> "

        logger.info(prompt)
        return prompt

    def read_config(self):
        try:
            with open(CSCFG_CONFIG, "r") as fp:
                out = fp.readlines()
        except FileNotFoundError:
            return

        try:
            outstr = "\n".join(out).strip()
            if outstr:
                self.config = json.loads(outstr)
        except:
            logger.info("Error reading cscfg config file")
            exit(1)

    def get_current_profile(self) -> Optional[str]:
        if not self.config:
            self.read_config()

        profile = self.config.get("profile")
        if not profile:
            return None

        # Check if the configured profile exists
        if profile in [p.name for p in DeploymentProfile.list()]:
            return profile

        return None

    def set_profile(self, profile: str):
        """
        Set the current profile and update the config file
        """
        self.current_profile = profile
        self.update_config("profile", profile)

    def update_config(self, field, value):
        self.config[field] = value
        with open(CSCFG_CONFIG, "w") as fp:
            fp.write(json.dumps(self.config, indent=4))

    def onecmd(self, line: str):
        self.readline_complete = readline.get_completer()
        rtn = None
        try:
            # write the command only to the log file, not to the console
            root = logging.getLogger()
            file_handler = root.handlers[1]
            record = logger.makeRecord(logger.name, logging.INFO, file_handler.name, 0, line, None, None)
            file_handler.emit(record)

            rtn = super().onecmd(line)
        except Exception as e:
            logger.exception(e)
            self.returncode = 1
        except SystemExit:
            # Specifically added to handle argparse exit on help behavior
            self.returncode = 1
        readline.parse_and_bind("tab: complete")
        readline.set_completer(self.readline_complete)
        return rtn

    def emptyline(self):
        # To ensure that the previous command is not repeated
        # on an empty input
        pass

    def precmd(self, line):
        sl = line.split()
        if len(sl) == 0:
            return line

        # If shell command, just passthru
        if line.startswith("shell") or line[0] == "!":
            return line

        # sub '?' with help
        if line == "?":
            return line
        if line[-1] == "?":
            line = line[:-1] + "--help"

        return line

    def do_exit(self, arg):
        """ Exit the interpreter """
        return True

    def do_EOF(self, arg):
        return True  # exit - do not document this function otherwise "EOF" shows up in helptext

    def do_shell(self, arg):
        try:
            os.system(arg)  # run shell cmd (! built-in for Cmd class) - do not document else it shows up in helptext
        except KeyboardInterrupt:
            pass

    def do_version(self, arg):
        """ Print the version of cscfg """
        print(get_version())

    def do_deploy(self, arg):
        """ Deployment commands """
        self._do_cmds(arg, "deploy", self.do_deploy.__doc__.strip(), cmd_deploy)
    
    def do_bom(self, arg):
        """ Software & Hardware Bill of Materials commands """
        self._do_cmds(arg, "bom", self.do_bom.__doc__.strip(), cmd_bom)

    def _do_cmds(self, arg, prog: str, descr: str, cmd_module, parser=None):
        if hasattr(cmd_module, "HAS_MODULE") and not cmd_module.HAS_MODULE:
            descr += f"\nNOTE: {prog} is disabled because it could not load required modules"

        if parser is None:
            parser = argparse.ArgumentParser(
                prog=prog,
                description=descr,
                formatter_class=argparse.RawTextHelpFormatter
            )

        subparsers = parser.add_subparsers()
        for sc in cmd_module.CMDS:
            m = sc(subparsers, profile=self.current_profile, cli_instance=self)
            m.build()

        try:
            args = parser.parse_args(shlex.split(arg))
        except SystemExit:
            self.returncode = 1
            return
        except Exception as exc:
            logger.error(exc, stack_info=True)
            self.returncode = 1
            return

        if hasattr(args, "func"):
            try:
                self.returncode = args.func(args)
            except SystemExit:
                self.returncode = 1
                return
            except Exception as exc:
                self.returncode = 1
                raise exc

    def do_profile(self, arg):
        """
        Manage profiles
        """
        self._do_cmds(arg,
                      "profile",
                      textwrap.dedent(self.do_profile.__doc__).strip(),
                      cmd_profile)

    def do_import_profile(self, arg):
        self.onecmd(f"profile import {arg}") # alias for backward compat

    def do_delete_profile(self, arg):
        self.onecmd(f"profile delete {arg}") # alias for backward compat

    def do_cluster(self, arg):
        """
        Manage clusters
        """
        self._do_cmds(arg,
                      "cluster",
                      textwrap.dedent(self.do_cluster.__doc__).strip(),
                      cmd_cluster)

    def do_device(self, arg):
        """
        Manage devices and their properties
        """
        self._do_cmds(arg,
                      "device",
                      textwrap.dedent(self.do_device.__doc__).strip(),
                      cmd_device)
        
    def do_inventory(self, arg):
        """
        Manage elevations and racks as well as their properties
        """
        self._do_cmds(arg,
                      "inventory",
                      textwrap.dedent(self.do_inventory.__doc__).strip(),
                      cmd_inventory)

    def do_switch(self, arg):
        """
        Manage switches
        """
        self._do_cmds(arg,
                      "switch",
                      textwrap.dedent(self.do_switch.__doc__).strip(),
                      cmd_switch)

    def do_system(self, arg):
        """
        Manage systems
        """
        self._do_cmds(arg,
                      "system",
                      textwrap.dedent(self.do_system.__doc__).strip(),
                      cmd_system)

    def do_server(self, arg):
        """
            Manage servers and related actions
        """
        self._do_cmds(arg,
                      "server",
                      textwrap.dedent(self.do_server.__doc__).strip(),
                      cmd_server)

    def do_peripherals(self, arg):
        """
            Manage peripherals (e.g. flow controllers)
        """
        self._do_cmds(arg,
                      "peripherals",
                      textwrap.dedent(self.do_peripherals.__doc__).strip(),
                      cmd_peripherals)

    def do_cbinfra(self, arg):
        """ Update configuration cerebras internal infra """
        self._do_cmds(arg, "cbinfra", self.do_cbinfra.__doc__.strip(), cmd_cbinfra)

    def do_validate(self, arg):
        """
            Validation commands
        """
        self._do_cmds(arg,
                      "validate",
                      textwrap.dedent(self.do_validate.__doc__).strip(),
                      cmd_validate)

    def do_network(self, arg):
        """
        Run 100g network commands
        """
        self._do_cmds(arg,
                      "network",
                      textwrap.dedent(self.do_switch.__doc__).strip(),
                      cmd_network)

    def do_run_flow(self, arg):
        """
            Run predefined flows
        """
        self._do_cmds(arg,
                      "run_flow",
                      textwrap.dedent(self.do_run_flow.__doc__).strip(),
                      cmd_flows)

    def do_cluster_platform(self, arg):
        """
            Run cluster_platform commands
        """
        self._do_cmds(arg,
                      "cluster_platform",
                      textwrap.dedent(self.do_cluster_platform.__doc__).strip(),
                      cmd_cluster_platform)

    def do_cluster_mgmt(self, arg):
        """
        Run cluster_mgmt update/upgrade commands
        """
        self._do_cmds(arg,
                      "cluster_mgmt",
                      textwrap.dedent(self.do_cluster_mgmt.__doc__).strip(),
                      cmd_cluster_mgmt)

    def do_dhcp(self, arg):
        """
        Run dhcp related commands
        """
        self._do_cmds(arg,
                      "dhcp",
                      textwrap.dedent(self.do_dhcp.__doc__).strip(),
                      cmd_dhcp)

    def do_dnsmasq(self, arg):
        self._do_cmds(arg,
                      "dnsmasq",
                      textwrap.dedent(self.do_dhcp.__doc__).strip(),
                      cmd_dhcp)
        # ^ leave dnsmasq command without comment for backwards compatibility

    def do_ansible(self, arg):
        """
        Run ansible related commands
        """
        self._do_cmds(arg,
                      "ansible",
                      textwrap.dedent(self.do_ansible.__doc__).strip(),
                      cmd_ansible)

    def do_k8s(self, arg):
        """
        Run k8s related commands
        """
        self._do_cmds(arg,
                      "k8s",
                      textwrap.dedent(self.do_k8s.__doc__).strip(),
                      cmd_k8s)

    def do_dev(self, arg):
        """ Developer commands """
        self._do_cmds(arg, "dev", self.do_dev.__doc__.strip(), cmd_dev)
    
    def do_backup(self, arg):
        """ Manage backup operations """
        self._do_cmds(arg,
                      "backup",
                      textwrap.dedent(self.do_backup.__doc__).strip(),
                      cmd_backup)

    def do_help(self, arg):
        # overwrite default do_help function to hide functions without docs and fmt like argparse

        # construct an argparse for its help-formatting capabilities
        parser = cscfg_argparser()
        subparsers = parser.add_subparsers(title='commands', dest='command')

        names = self.get_names()
        cmds_doc = []

        prevname = ''
        for name in sorted(names):
            if name[:3] == 'do_':
                if name == prevname:
                    continue
                prevname = name
                cmd = name[3:]
                doc = getattr(self, name).__doc__
                if doc:
                    cmds_doc.append((cmd, doc))

        # show the subparser's help
        if arg and arg in {c[0] for c in cmds_doc}:
            getattr(self, "do_" + arg)("--help")
            return

        for cmd, doc in cmds_doc:
            subparser = subparsers.add_parser(cmd, help=doc)
            subparser.description = doc
        parser.print_help()


class LazyHelpAction(argparse.Action):
    """ hack to print the CLI parser's help message in the entrypoint's argparse --help handler """
    def __init__(self, option_strings, dest, **kwargs):
        super().__init__(option_strings, dest, nargs=0, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        cli = CSConfigCLI()
        cli.do_help("")
        parser.exit()


def cscfg_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cscfg",
        description="Deployment manager CLI",
        add_help=False,
    )
    parser.add_argument("-h", "--help", action=LazyHelpAction)
    parser.add_argument("-p", "--profile",
                        help="Name of the profile to handle"
                        )
    parser.add_argument("--version",
                        action="store_true",
                        help="Show the version of cscfg"
                        )
    parser.add_argument('-v', '--verbosity',
                type=int,
                choices=[0, 1, 2, 3],
                help="Verbosity level",
                )
    return parser


def run_shell(args):
    # Cleanup profiles, both database entry and directory must exists
    # If the database entry exists, but directory does not, delete the database
    # entry
    if args.version:
        print(f"Cerebras cluster deployment configurator {get_version()}")
        exit(0)

    profiles = DeploymentProfile.list()
    for p in profiles:
        cg = ConfGen(p.name)
        if not cg.is_exists():
            p.delete()

    # Initialize profiles' deployable flag to appropriate value
    init_profile_deployability()

    if args is not None and (args.exec or args.exec_cmd):
        if args.exec:
            cmd_args = " ".join([a.rstrip("\\") for a in args.exec_cmd])  # handle backwards compatibility
        else:
            cmd_args = " ".join(args.exec_cmd)
        cli = CSConfigCLI(profile=args.profile)
        cli.with_exec = True
        cli.onecmd(cmd_args)
        exit(cli.returncode)
    else:
        cli = CSConfigCLI(profile=args.profile)
        ProfileUserPrompts(cli).on_cscfg_start()
        intro = (
            f"Welcome to the Cerebras cluster deployment configurator {get_version()}\n"
            "Type help or ? to list commands\n"
        )
        while True:
            try:
                cli.cmdloop(intro=intro)
                break
            except KeyboardInterrupt:
                logger.info("^C")
                cli.intro = ""
