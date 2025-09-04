import logging
import pathlib
import subprocess
import textwrap
import time
import traceback
from typing import Iterable, List

import deployment_manager.flow.system_swap as system_swap
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device
from deployment_manager.flow import system_dcqcn
from deployment_manager.flow.task import FlowContext, FlowDeviceState, FlowStatus, ParallelTask
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.flags import add_image_flag
from deployment_manager.tools.secrets import create_secrets_provider
from deployment_manager.tools.system import SystemCtl
from deployment_manager.tools.utils import flat_map, prompt_confirm

logger = logging.getLogger(__name__)

class SwapSystemFlow(SubCommandABC):
    """
    Command sequence to replace one system: swaps system in network configuration,
    updates root server DNS, regenerates and pushes system network configuration,
    and updates Kubernetes configuration. Additionally, the flow validates that
    the cables from switch to system have not changed order before/after swapping.

    Updates the new systems' image to the given --image if specified.

    Example:
        # swap s0 for s2 and s1 for s8
        swap_system s0,s2 s1,s8
    """

    name = "swap_system"

    def __init__(self, *args, **kwargs):
        tasks_help = "Tasks:\n"
        for task in [system_swap.PreflightCheck,
                     system_swap.UpdateDMCfgsTask,
                     system_swap.UpdateLegacyDnsmasqConf,
                     system_swap.DiscoverMgmtNetworkTask,
                     system_swap.UpdateDnsmasqTask,
                     system_swap.SystemStandbyTask,
                     system_swap.ConfigureSystemTask,
                     system_swap.UpdateSystemImageTask,
                     system_swap.SystemActivateTask,
                     system_swap.ValidateSystemIPsTask,
                     system_swap.ValidateSystemLinksTask,
                     system_swap.UpdateK8Task,
                     system_swap.CompleteTask,
                     system_swap.UpdateCBInfra]:
            tasks_help += "      " + task.human_name() + "\n"
            tasks_help += textwrap.indent(textwrap.dedent(task.__doc__).strip("\n"), "        ")
            tasks_help += "\n"
        desc = self.__class__.__doc__ + tasks_help
        super().__init__(*args, description=desc, **kwargs)

    def construct(self):
        self.parser.add_argument("system_swaps", nargs="+", help="System pairs to swap")
        self.parser.add_argument("--skip-tasks", default="", help="Comma-separated list of tasks to skip")
        self.parser.add_argument("--skip-until", default="", help="Skip all tasks up to this task")
        add_image_flag(self.parser, required=False)
        self.add_arg_noconfirm()

    def _parse_swap_reqs(self, arg: Iterable[str]) -> List[system_swap.SwapRequest]:
        pairs = [p.lower().split(",") for p in arg]
        reqs = []
        for p in pairs:
            if len(p) != 2 or len(p[0]) == 0 or len(p[1]) == 0:
                logger.error(f"invalid argument: systems must be pairs like 's1,s2' but got: {p}")
                return []
            reqs.append(system_swap.SwapRequest(self.profile, p[0], p[1]))
        if len(set(flat_map(pairs))) != len(flat_map(pairs)):
            logger.error(f"invalid argument: systems names must be unique")
            return []
        for pair in reqs:
            replacement = Device.get_device(pair.old, self.profile)
            if not replacement:
                replacement = Device.get_device(pair.new, self.profile)
            if not replacement:
                logger.error(f"invalid state: both old and new system in {pair} were not in the database, "
                                    "unable to swap")
                return []
            if replacement.get_prop(props.prop_management_info_name):
                pair.alias_mode = True

            # final sanity check related to alias mode
            if pair.alias_mode:
                # ensure that the new system doesn't appear as a non-aliased name
                duplicate = Device.get_device(pair.new, self.profile)
                if duplicate is not None:
                    logger.error(f"invalid state: {pair.new} already exists as a non-aliased device record. You "
                                        "must remove it first before attempting to swap")
                    return []

        return reqs

    def run(self, args):
        reqs = self._parse_swap_reqs(args.system_swaps)
        if not reqs:
            return 1

        skip_tasks = args.skip_tasks.split(",")

        cg = ConfGen(self.profile)
        config = cg.parse_profile()
        secrets_provider = create_secrets_provider(config)

        # construct the task list
        tasks = [
            system_swap.PreflightCheck(config, reqs),
            *[system_swap.UpdateDMCfgsTask(r) for r in reqs],
            system_swap.UpdateLegacyDnsmasqConf(self.profile, reqs),
            ParallelTask([system_swap.UpdatePasswordTask(r, secrets_provider) for r in reqs], name="UpdatePassword"),
            *[system_swap.DiscoverMgmtNetworkTask(r) for r in reqs],
            system_swap.UpdateDnsmasqTask(self.profile, config),
            ParallelTask([system_swap.SystemStandbyTask(r) for r in reqs], name="SystemStandby"),
            *[
                system_swap.ConfigureSystemTask(cg, config, req)
                for i, req in enumerate(reqs)
            ],
        ]

        if args.image:
            if not pathlib.Path(args.image).is_file():
                raise ValueError(f"image {args.image} is not a file")
            tasks.append(ParallelTask(
                [system_swap.UpdateSystemImageTask(r, args.image) for r in reqs],
                name="UpdateSystemImage"
            ))

        tasks += [
            ParallelTask(
                [system_swap.SystemActivateTask(r) for r in reqs],
                name="SystemActivate"
            ),
            system_swap.ValidateSystemIPsTask(cg, reqs),
            system_swap.ValidateSystemLinksTask(cg, config, reqs),
            system_swap.UpdateK8Task(self.profile, reqs),
            *[system_swap.CompleteTask(self.profile, r) for r in reqs],
        ]
        if system_swap.UpdateCBInfra.test():
            tasks.append(system_swap.UpdateCBInfra(self.profile, reqs))

        tasks = [t for t in tasks if t.name not in skip_tasks]
        if args.skip_until:
            skip_idx = [t.name for t in tasks].index(args.skip_until)
            if skip_idx == -1:
                logger.info(f"skip-until task {args.skip_until} was not present in task list!")
                return 1
            tasks = tasks[skip_idx:]

        explain = []
        for task in tasks:
            explain.append(str(task))
        explain = "  " + '\n  '.join(explain)
        logger.info(f"execute flow tasks:\n{explain}")

        if not args.noconfirm and not prompt_confirm(msg="Continue? y/n: "):
            return

        for task in tasks:
            start_time = time.time()
            success = False
            try:
                logger.info(f"start task {task}")
                rv = task.run()
                success = rv is not False
                if not success:
                    return 1
            except subprocess.CalledProcessError as e:
                logger.error(f"task {task} cmd {e.cmd} failed with {e.returncode}:\n{e.stdout}\n{e.stderr}")
                logger.debug(traceback.format_exc())
                return 1
            except Exception as e:
                logger.error(f"task {task} raised exception {e}")
                logger.debug(traceback.format_exc())
                return 1
            finally:
                logger.info(f"end task {task}: {'success' if success else 'fail'} ({time.time() - start_time:0.2f}s)")


class SetSystemDcqcnFlow(SubCommandABC):
    """
    Enable or disable DCQCN. Note: enabling/disabling will standby/activate system causing disruption.
    Note: this function is temporary. Sometime after 2.4, all systems in V2 clusters will have DCQCN enabled.
    """

    name = "set_dcqcn"

    def __init__(self, *args, **kwargs):
        tasks_help = "Tasks:\n"
        for task in [system_dcqcn.Preflight, system_dcqcn.ConfigureSystemsTask]:
            tasks_help += "      " + task.human_name() + "\n"
            tasks_help += textwrap.indent(textwrap.dedent(task.__doc__).strip("\n"), "        ")
            tasks_help += "\n"
        desc = self.__class__.__doc__ + tasks_help
        super().__init__(*args, description=desc, **kwargs)

    def construct(self):
        self.parser.add_argument("ACTION", choices=["enable", "disable"])
        self.add_arg_filter()
        self.parser.add_argument("--skip-tasks", default="", help="Comma-separated list of tasks to skip")
        self.parser.add_argument("--skip-until", default="", help="Skip all tasks up to this task")
        self.add_arg_noconfirm()

    @staticmethod
    def exec(ctx: FlowContext, should_enable: bool, sys_controllers: List[SystemCtl],
             noconfirm=False, skip_tasks: List[str] = (), skip_until: str = "") -> bool:
        """ organize method this way for testing """
        tasks = [
            ParallelTask([system_dcqcn.Preflight(ctx, c, should_enable) for c in sys_controllers], name="Preflight"),
            system_dcqcn.ConfigureSystemsTask(ctx, sys_controllers, should_enable),
        ]

        tasks = [t for t in tasks if t.name not in skip_tasks]
        if skip_until:
            skip_idx = [t.name for t in tasks].index(skip_until)
            if skip_idx == -1:
                logger.info(f"skip-until task {skip_until} was not present in task list!")
                return False
            tasks = tasks[skip_idx:]

        explain = []
        for task in tasks:
            explain.append(str(task))
        explain = "  " + '\n  '.join(explain)
        logger.info(f"execute flow tasks:\n{explain}")

        if not noconfirm and not prompt_confirm(msg="Continue? y/n: "):
            return False

        for task in tasks:
            start_time = time.time()
            success = False
            try:
                logger.info(f"start task {task}")
                task.run()
                success = True
            except Exception:
                logger.debug(f"task {task} raised exception\n{traceback.format_exc()}")
                raise
            finally:
                logger.info(f"end task {task}: {'success' if success else 'fail'} ({time.time() - start_time:0.2f}s)")
            print(FlowDeviceState.format_reprs(list(ctx.device_state.values())))
        for device in ctx.get_progressing_devices():
            ctx.update_state(device, ctx.device_state[device].step, FlowStatus.Success)
        return True

    def run(self, args):
        should_enable = args.ACTION == "enable"
        qs = Device.get_systems(self.profile)
        qs = self.filter_devices(args, qs)

        sys_controllers = [
            SystemCtl(
                d.name,
                d.get_prop(props.prop_management_credentials_user) or "admin",
                d.get_prop(props.prop_management_credentials_password) or "admin",
            ) for d in qs
        ]

        ctx = FlowContext(self.profile)
        ok = self.exec(ctx, should_enable, sys_controllers, args.noconfirm, args.skip_tasks, args.skip_until)
        if not ok:
            return 1
        print(FlowDeviceState.format_reprs(list(ctx.device_state.values())))
        return 0 if all(v.status == FlowStatus.Success for v in ctx.device_state.values()) else 1


CMDS = [
    SwapSystemFlow,
]
