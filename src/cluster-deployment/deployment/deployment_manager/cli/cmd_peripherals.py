import dataclasses
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List, Dict, Union, Tuple

from deployment_manager.common.constants import MAX_WORKERS
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.client.flow_controller import BelimoFlowController, GetFlowRateRepr
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles, DeploymentDeviceTypes
from deployment_manager.db.models import Device
from deployment_manager.tools.utils import prompt_confirm, ReprFmtMixin
from deployment_manager.tools.flow_controller_util import (
    create_client,
    get_fc_config_updates,
    put_fc_config_updates,
    check_fc_flow_rate,)
from deployment_manager.cli.health import (
    HealthShow,
    HealthCheck,
    HealthForceUpdate,
    AddDeviceType,
)

@dataclasses.dataclass
class FlowControllerUpdateRepr(ReprFmtMixin):
    name: str
    ok: bool
    comment: str

    @classmethod
    def table_header(cls) -> list:
        return ["name", "ok", "comment"]

    def to_table_row(self) -> list:
        return [self.name, str(self.ok), self.comment]

    @property
    def sort_key(self):
        return (self.name,)


class UpdateFlowController(SubCommandABC):
    """Update flow controller configuration to default values."""

    name = "update_flow_controller"

    def construct(self):
        self.add_arg_filter()
        self.add_arg_noconfirm()
        self.add_arg_output_format()
        self.add_arg_dryrun()
        self.add_error_only_flag(help="Only display unreachable devices. Implies --dryrun")

    def run(self, args):
        dryrun = args.dryrun or args.error_only

        query_set = Device.get_all(self.profile).filter(
            device_type="PR", device_role=DeploymentDeviceRoles.VALVECTRL.value
        )
        query_set = self.filter_devices(args, query_set=query_set)
        clients = [create_client(d) for d in query_set]
        updates = get_fc_config_updates(clients)
        rv = 0
        reprs = []
        candidates = {}
        for name, update in updates.items():
            if isinstance(update, str):
                reprs.append(FlowControllerUpdateRepr(name, False, update))
                rv = 1
            else:
                msg = ",".join([path.split("/")[-1] + "=" + str(value) for path, value in update])
                reprs.append(FlowControllerUpdateRepr(name, True, f"{len(update)} fields to update: {msg}"))
                if len(update) > 0:
                    candidates[name] = update

        if not args.noconfirm or dryrun:
            def _should_display(r: FlowControllerUpdateRepr) -> bool:
                if args.error_only:
                    return not r.ok
                if dryrun:
                    return True
                return r.name in candidates or not r.ok # has updates or is error

            display = [r for r in reprs if _should_display(r)]
            print(FlowControllerUpdateRepr.format_reprs(display, fmt=args.output))
            if args.dryrun:
                return rv
            if len(candidates) == 0:
                print("No updates")
                return rv
            if not prompt_confirm("Apply updates to flow controllers?"):
                print("Update cancelled.")
                return 0

        results = put_fc_config_updates(clients, candidates)
        reprs = []
        for name, result in results.items():
            if result:
                reprs.append(FlowControllerUpdateRepr(name, False, result.message))
                rv = 1
            else:
                reprs.append(FlowControllerUpdateRepr(name, True, "updated"))
        print(FlowControllerUpdateRepr.format_reprs(reprs, fmt=args.output))
        return rv


class CheckFlowControllerFlow(SubCommandABC):
    """Check flow controller current flow rate against configured min/max values."""
    name = "check_flow_controller_flow"

    def construct(self):
        self.add_arg_filter()
        self.add_arg_output_format()

    def run(self, args):
        query_set = Device.get_all(self.profile).filter(
            device_type="PR", device_role=DeploymentDeviceRoles.VALVECTRL.value
        )
        query_set = self.filter_devices(args, query_set=query_set)
        clients = [create_client(d) for d in query_set]
        results = check_fc_flow_rate(clients)
        print(GetFlowRateRepr.format_reprs(list(results.values()), fmt=args.output))
        return 1 if any(bool(r.error) for r in results.values()) else 0

@AddDeviceType(DeploymentDeviceTypes.PERIPHERAL.value)
class PeripheralHealthShow(HealthShow):
    """Show the health of peripheral devices."""
    pass

@AddDeviceType(DeploymentDeviceTypes.PERIPHERAL.value)
class PeripheralHealthCheck(HealthCheck):
    """Check the health of peripheral devices."""
    pass

@AddDeviceType(DeploymentDeviceTypes.PERIPHERAL.value)
class PeripheralHealthForceUpdate(HealthForceUpdate):
    """Force update health of peripheral devices."""
    pass

class PeripheralHealth(SubCommandABC):
    """
    System health related actions
    """
    SUB_CMDS = [PeripheralHealthShow, PeripheralHealthCheck, PeripheralHealthForceUpdate]

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

CMDS = [
    PeripheralHealth,
    CheckFlowControllerFlow,
    UpdateFlowController,
]
