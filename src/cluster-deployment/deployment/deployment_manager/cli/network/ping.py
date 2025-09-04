from collections import defaultdict

import dataclasses
import functools
import logging
from typing import List, Dict

from deployment_manager.cli.filter import process_filter
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.models import Device
from deployment_manager.network_config.common.context import NetworkCfgCtx
from deployment_manager.network_config.common.task import generate_tasks_for_names, generate_tasks_for_entity_type, \
    run_tasks, summarize_task_result_errors
from deployment_manager.network_config.tasks.node import NodeInterfaceCheckNetworkTask, NodePingTestNetworkTask
from deployment_manager.network_config.tasks.node_ping import PingTestKind, NodePingTestGenerator
from deployment_manager.network_config.tasks.switch import SwitchPingGenerator
from deployment_manager.network_config.tasks.system import SystemInterfaceCheckTask
from deployment_manager.network_config.tasks.utils import PingTest
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools import middleware
from deployment_manager.tools.network_config_tool import NetworkConfigTool
from deployment_manager.tools.switch.utils import interface_sort_key
from deployment_manager.tools.utils import ReprFmtMixin

logger = logging.getLogger(__name__)

def fmt(*args):
    return ":".join(filter(lambda a: bool(a), args))


class PingTestRepr(ReprFmtMixin, PingTest):

    @classmethod
    def from_parent(cls, parent_instance):
        data = dataclasses.asdict(parent_instance)
        return cls(**data)

    @classmethod
    def table_header(cls) -> list:
        return ["kind", "src", "dst", "ok", "details"]

    def to_table_row(self) -> list:
        return [self.kind,
                fmt(self.src_name, self.src_if, self.src_ip),
                fmt(self.dst_name, self.dst_if, self.dst_ip),
                self.ok, self.details]

    @functools.cached_property
    def sort_key(self):
        return self.kind, self.src_name, interface_sort_key(self.src_if), self.dst_name, interface_sort_key(self.dst_if)


class PingSwitchCmd(SubCommandABC):
    """ Run switch ping checks """

    name = "switch"

    def construct(self):
        self.add_arg_filter()
        self.add_arg_output_format()
        self.parser.add_argument("--destination_types", "-d",
                                 choices=[SwitchPingGenerator.DEST_XCONN, SwitchPingGenerator.DEST_UPLINKS,
                                          SwitchPingGenerator.DEST_SYSTEMS, SwitchPingGenerator.DEST_NODES],
                                 default=(),
                                 required=False)
        self.add_error_only_flag()

    def run(self, args):
        nw = NetworkConfigTool.for_profile(self.profile)

        qs = Device.objects.filter(device_type="SW", device_role__in=["SP", "LF", "MX", "SW"])
        if args.filter:
            qs = self.filter_devices(args, query_set=qs)
        if not qs:
            return 0

        switch_result: Dict[str, List[PingTest]] = nw.switch_ping_check(qs, (args.destination_types,))

        reprs = []
        for switch, results in switch_result.items():
            for result in results:
                if result.is_fatal():
                    logger.error(f"{switch} had a fatal error: {result.details}")
                    continue
                if not result.ok or not args.error_only:
                    reprs.append(PingTestRepr.from_parent(result))
        print(PingTestRepr.format_reprs(reprs, args.output))


class PingNodeCmd(SubCommandABC):
    """ Run node ping checks """

    name = "node"

    def construct(self):
        self.parser.add_argument(
            "-s", "--src_filter",
            nargs="+",
            help="Filter for source nodes",
        )
        self.parser.add_argument(
            "-d", "--dst_filter",
            nargs="+",
            help="Filter for destination nodes/systems",
        )
        self.parser.add_argument('--skip_down',
                                 help=str("Scan source nodes for down or misconfigured interfaces prior to ping test "
                                          "and skip using these source interfaces. Doesn't apply to dsts"),
                                 required=False, action="store_true")
        self.add_arg_output_format()
        self.parser.add_argument("--types", "-t",
                                 choices=[kind.value for kind in PingTestKind],
                                 nargs="+",
                                 default=[],
                                 help="Types of tests to run. Default of no value means run all the test types",
                                 required=False)
        self.add_error_only_flag()

    def run(self, args):
        if args.types:
            test_kinds = [PingTestKind(dt) for dt in args.types]
        else:
            test_kinds = [kind for kind in PingTestKind]

        src_names = None
        if args.src_filter:
            qs = Device.get_servers(self.profile)
            for f in args.src_filter:
                qs = process_filter(f, qs)
            src_names = [d.name for d in qs]
            if not src_names:
                print(PingTestRepr.format_reprs([], args.output))
                return 0

        dst_names = None
        if args.dst_filter:
            qs = Device.objects.filter(profile__name=self.profile, device_type__in=("SY", "SR",))
            for f in args.dst_filter:
                qs = process_filter(f, qs)
            dst_names = [d.name for d in qs]
            # if there's no dsts, it's possible some tests like the GATEWAY test will still produce ping results
            # so don't short-circuit

        cg = ConfGen(self.profile)
        with NetworkCfgCtx(cg.network_config_file, middleware.AppCtxImpl(self.profile), ) as ctx:
            if args.skip_down:
                tasks = generate_tasks_for_entity_type(ctx, SystemInterfaceCheckTask)
                tasks.extend(generate_tasks_for_entity_type(ctx, NodeInterfaceCheckNetworkTask))
                results = run_tasks(tasks)
                errs = summarize_task_result_errors(results)
                if errs:
                    logger.debug(f"failed to check some device interface statuses: {errs}")

            gen = NodePingTestGenerator(
                ctx.network_config,
                include_src=src_names,
                include_dst=dst_names,
                tests_per_dst_node_if=1,
                tests_per_dst_system_if=1,
                exclude_down_src_interfaces=args.skip_down,
            )
            if src_names:
                tasks = generate_tasks_for_names(ctx, NodePingTestNetworkTask, src_names, gen, test_kinds)
            else:
                tasks = generate_tasks_for_entity_type(ctx, NodePingTestNetworkTask, gen, test_kinds)
            results = run_tasks(tasks)
        reprs = []
        for node, result in results.items():
            if not result.ok:
                logger.error(f"failed to execute ping test on {node}: {result.message}")
            else:
                reprs.extend([PingTestRepr.from_parent(r) for r in result.data if not args.error_only or not r.ok])

        print(PingTestRepr.format_reprs(reprs, args.output))


class PingCmd(SubCommandABC):
    """ Ping related actions """

    SUB_CMDS = [PingSwitchCmd, PingNodeCmd]

    name = "ping"

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
