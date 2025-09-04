import argparse
import code
import concurrent
import logging
import os
import pathlib
import re
import shutil
import sys
import tempfile
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device
from deployment_manager.deploy.runner import DeploymentRunner
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.const import (CLUSTER_DEPLOYMENT_BASE, DEPLOYMENT_LOGS)
from deployment_manager.tools.master_config import generate_pb2_master_config
from deployment_manager.tools.ssh import SSHConn, make_prompt_detector
from deployment_manager.tools.switch.switchctl import get_vendor_impl
from deployment_manager.tools.utils import prompt_confirm, diff_files

logger = logging.getLogger(__name__)


class Python(SubCommandABC):
    """ Starts a python interactive shell with the current execution context """

    name = "python"

    def construct(self):
        pass

    def run(self, args):
        # Give access to both local and global vars
        vars = globals().copy()
        vars.update(locals())

        shell = code.InteractiveConsole(vars)
        try:
            shell.interact()
        except SystemExit:
            # Catch SystemExit to prevent the outer interpreter from crashing
            logger.info("Exiting the inner interpreter.")
        except Exception as exc:
            logger.error(exc)
            return


class Shell(SubCommandABC):
    """ Executes a command in the shell """

    name = "shell"

    def construct(self):
        self.parser.add_argument('exec_cmd', nargs=argparse.REMAINDER,
                            help='Command to execute. If none given, will enter interpreter mode')

    def run(self, args):
        cmd = args.exec_cmd or "bash"
        try:
            os.system(cmd)
        except KeyboardInterrupt:
            pass


class GenerateMasterConfig(SubCommandABC):
    """
    Generate master config file, this is used as input of PB2
    """

    name = "generate_master_config"

    def construct(self):
        self.parser.add_argument(
            "--output",
            "-o",
            help="Location to write master config file. The default is /tmp/<tmp>/master_config.yml",
        )
        self.add_arg_filter(required=False)
        self.add_arg_noconfirm()

    def run(self, args):

        cg = ConfGen(self.profile)

        if args.output == "-":
            output_path = sys.stdout
        elif args.output is not None:
            output_path = pathlib.Path(args.output)
        else:
            output_path = pathlib.Path(tempfile.mkdtemp()) / "master_config.yml"

        new_master_config = output_path

        devices = SubCommandABC.filter_devices(args, query_set=Device.get_all(self.profile))

        generate_pb2_master_config(
            self.profile, new_master_config, devices=devices
        )

        if args.output == "-":
            return 0

        cur_master_config = pathlib.Path(cg.master_file)
        diffs = diff_files(cur_master_config, new_master_config)
        if not diffs.strip():
            logger.info("No changes detected")
            return 0
        logger.info(diffs)

        # short-circuiting boolean operator
        if not args.noconfirm and not prompt_confirm(
            "Overwrite master_config.yml?"
        ):
            # not overwrite master_config
            logger.info(
                f"master_config.yml written to {str(new_master_config.absolute())}"
            )
            return 0

        shutil.copyfile(
            str(new_master_config.absolute()),
            str(cur_master_config.absolute()),
        )
        logger.info(
            f"master_config.yml overwritten to {str(cur_master_config.absolute())}"
        )
        return 0


class ShowLogs(SubCommandABC):
    """
        View logs
    """
    name = "logs"

    loglist = ["current", "cscfg", "readonly", "ansible"] + DeploymentRunner.stage_names()

    def construct(self):
        self.parser.add_argument("--logname",
                                 choices=self.loglist,
                                 default="current",
                                 help="The name of the log to view")
        self.parser.add_argument("--profile",
                                 help=textwrap.dedent("""
                                        Name of a saved cluster profile.
                                        This is only needed if you want to view deploy stage from other profiles.
                                    """).strip())
        self.parser.add_argument("--head",
                                 action="store_true",
                                 help="View the beginning of the log")
        self.parser.add_argument("--tail",
                                 action="store_true",
                                 help="View the end of the log")
        self.parser.add_argument("-n", "--num",
                                 type=int,
                                 default=-1,
                                 help="The number of lines to display for --head or --tail")

    def run(self, args):
        if args.logname == "current":
            logfile = os.environ["CSCFG_LOGFILE"]
        elif args.logname == "cscfg":
            logfile = f"{DEPLOYMENT_LOGS}/cscfg.log"
        elif args.logname == "ansible":
            logfile = f"{CLUSTER_DEPLOYMENT_BASE}/os-provision/ansible.log"
        else:
            profilename = self.profile
            if args.profile:
                profilename = args.profile
            logfile = f"{DEPLOYMENT_LOGS}/{profilename}/{args.logname}.log"

        if not os.path.exists(logfile):
            logger.info(f"Cannot find the log: {logfile}")
            self.cli_instance.returncode = 1
            return

        num_opt = ""
        if args.num > 0:
            num_opt = f"-n {args.num}"

        logger.info(f"\n##### {logfile}")
        if args.head:
            os.system(f"head {num_opt} {logfile} | sed -e 's/^/    /'")
        elif args.tail:
            os.system(f"tail {num_opt} {logfile} | sed -e 's/^/    /'")
        else:
            os.system(f"less {logfile}")
        logger.info("\n###########\n")


class SplitArgs(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        namespace.NAME = []
        namespace.exec_cmd = []
        delimiter_found = False
        for value in values:
            if delimiter_found:
                namespace.exec_cmd.append(value)
            elif value == '--':
                delimiter_found = True
            else:
                namespace.NAME.append(value)


class Exec(SubCommandABC):
    """
    Exec commands on a host. Login details will be looked up from the DB unless provided

    Examples
        # execute a command directly on 2 devices
        cscfg dev exec net001-mg0 net001-mg1 -- uptime

        # enter interactive shell
        cscfg dev exec net001-mg0

        # execute a script in batch mode on all server's IPMIs
        cscfg dev exec --ipmi --filter type=SR --file my-script.sh
    """
    name = "exec"

    def construct(self):
        cgroup = self.parser.add_mutually_exclusive_group(required=False)
        cgroup.add_argument("--mgmt",
                            action="store_true",
                            help="Connect to the device's management address. This is the default behavior")
        cgroup.add_argument("--ipmi",
                            action="store_true",
                            required=False,
                            help="When connecting to servers, connect to the IPMI not the management address")
        cgroup.add_argument("--console",
                            action="store_true",
                            help="Connect to the server console. Must set properties "
                                 "console.[ip,username,password] on device for this to work")

        self.parser.add_argument("--output_dir",
                                 required=False,
                                 help="Write output to specified directory in a file for each hostname")
        self.parser.add_argument("--file",
                                 required=False,
                                 help="Execute a script file line by line")
        self.parser.add_argument("--username", "-u",
                                 required=False,
                                 help="User to connect as. If not set, looks up name from inventory")
        self.parser.add_argument("--password", "-p",
                                 required=False,
                                 help="Password to connect with. If not set, looks up password from inventory")
        self.parser.add_argument("--timeout", "-t",
                                 default=0,
                                 type=int,
                                 help="Timeout. Applies to individual commands executes not batches of commands. "
                                      "0 disables timeouts")
        self.add_arg_noconfirm(help="When running over --filter devices, don't ask for confirmation first")
        self.add_arg_filter(False)
        self.parser.add_argument('args', nargs=argparse.REMAINDER, action=SplitArgs,
                                 help="Names of devices to connect to followed by '--' and the command to execute.")

    def run(self, args):
        output_dir = None
        if args.output_dir:
            output_dir = pathlib.Path(args.output_dir)
            if output_dir.exists() and not output_dir.is_dir():
                logger.error(f"invalid args: --output_dir={output_dir} is not a directory")
                return 1
            output_dir.mkdir(parents=True, exist_ok=True)

        if args.filter and args.NAME:
            logger.error("invalid args: set names or --filter but not both")
            return 1

        connect_mode = "mgmt"
        if args.console:
            connect_mode = "console"
        elif args.ipmi:
            connect_mode = "ipmi"

        query_set = self.filter_devices(args=args, device_type=None, name=args.NAME,
                                        query_set=Device.objects.filter(profile__name=self.profile))
        if len(query_set) == 0:
            logger.error(f"invalid args: no devices match given filters or names")
            return 1
        elif args.NAME and len(args.NAME) != len(query_set):
            # If user specifies NAMEs, then check that all names exist
            names = set(args.NAME)
            names.difference_update(set([d.name for d in query_set]))
            logger.error(f"failed to find {', '.join(names)}")
            return 1

        conn_info, errors = [], []
        for device in query_set:
            name, addr, user, password, prompt = device.name, "", args.username, args.password, make_prompt_detector()
            if connect_mode == "ipmi":
                addr = device.get_prop(props.prop_ipmi_info_ip)
                user = user if user else device.get_prop(props.prop_ipmi_credentials_user)
                password = password if password else device.get_prop(props.prop_ipmi_credentials_password)
            elif connect_mode == "mgmt":
                addr = device.get_prop(props.prop_management_info_ip) or device.name
                user = args.username or device.get_prop(props.prop_management_credentials_user)
                password = args.password or device.get_prop(props.prop_management_credentials_password)
                if device.device_type == "SW":
                    impl = get_vendor_impl(device.get_prop(props.prop_vendor_name))
                    if impl:
                        prompt = impl[1]().prompt_detector()  # switch vendor specific prompt detector
            elif connect_mode == "console":
                addr = device.get_prop(props.prop_console_ip)
                user = args.username or device.get_prop(props.prop_console_username)
                password = args.password or device.get_prop(props.prop_console_password)
            else:
                raise AssertionError(f"unknown connection mode: {connect_mode}")
            if not addr or not user:
                errors.append(f"inventory did not contain {connect_mode} ip/username for {device.name}")
            conn_info.append((name, str(addr), user, password, prompt,))

        if errors:
            logger.error("\n".join(errors))
            return 1

        timeout = None if args.timeout < 1 else args.timeout
        cmd = args.exec_cmd
        script = args.file
        if cmd and script:
            logger.error(f"invalid args: cannot specify both command ('{cmd}') and --file={script}")
            return 1

        if args.filter and not args.noconfirm:
            names = [d.name for d in query_set]
            print("Going to run on:\n" + "\n".join(names))
            if not prompt_confirm(f"exec on {len(names)} devices?"):
                return 0

        def make_printer(name: str, stderr=False) -> Callable:
            if output_dir:
                def print_(o="\n", **kwargs):
                    with open(output_dir / name, 'a') as file:
                        file.write(o)
                return print_

            def print_(o: str = "\n", **kwargs):
                prefix = f"{name}/std{'err' if stderr else 'out'}: "
                o = prefix + o
                o = re.sub(r"\n", "\n" + prefix, o)
                outfile = sys.stderr if stderr else sys.stdout
                print(o, file=outfile, **kwargs)

            return print_

        if cmd:
            cmd = " ".join(cmd)
            if not cmd:
                logger.error("no command specified. Use -- to delimit names from commands, "
                                    "e.g. cscfg dev exec worker1 worker2 -- uptime")
                return 1

            def exec_one(name, host, user, password, prompt):
                print_stdout = make_printer(name)
                print_stderr = make_printer(name, stderr=True)
                with SSHConn(host, user, password) as conn:
                    rv, so, se = conn.exec(cmd, timeout=timeout)
                    if len(so.strip()) > 0:
                        print_stdout(so)
                    if len(se.strip()) > 0:
                        print_stderr(se)
                    return rv

            with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
                statuses = []
                futures = {
                    executor.submit(exec_one, *info): info[0]
                    for info in conn_info
                }
                done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
                for future in done:
                    try:
                        statuses.append(future.result())
                    except Exception as e:
                        logger.error(f"{futures[future]} failed: {e}")
                        statuses.append(1)
                return 1 if sum(statuses) > 0 else 0

        elif script:
            script = pathlib.Path(script)
            if not script.is_file():
                logger.error(f"invalid args: --file {script} is not a file")
                return 1

            def exec_one(name, host, user, password, prompt):
                print_ = make_printer(name)
                with SSHConn(host, user, password) as conn:
                    with conn.shell_session() as sh:
                        sh.prompt_detect = prompt
                        time.sleep(0.2)  # let session init
                        for cmd in script.read_text().splitlines():
                            cmd = cmd.strip()
                            if cmd:
                                out = sh.exec(cmd, timeout=timeout, capture_init=True)
                                print_(out)
                        print_()
                return 0

            with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
                statuses = []
                futures = {
                    executor.submit(exec_one, *info): info[0]
                    for info in conn_info
                }
                done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
                for future in done:
                    try:
                        statuses.append(future.result())
                    except Exception as e:
                        logger.error(f"{futures[future]} failed: {e}")
                        statuses.append(1)
                return 1 if sum(statuses) > 0 else 0

        else:

            if len(conn_info) > 1:
                logger.error("cannot open interactive terminal for multiple devices, please select one device")
                return 1
            _, addr, user, password, _ = conn_info[0]
            with SSHConn(addr, user, password) as conn:
                shell = conn.shell_session()
                shell.interactive()


CMDS = [
    Exec,
    Python,
    Shell,
    ShowLogs,
    GenerateMasterConfig,
]
