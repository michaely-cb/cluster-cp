import dataclasses
import inspect
import json
import logging
from typing import Dict, List

import yaml
from tabulate import tabulate

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import DeploymentDeviceTypes
from deployment_manager.db.models import Device, HealthState
from deployment_manager.tools.pb1.health_check import (
    filter_critical_devices,
    get_health_attrs_for_device_type,
    filter_degraded_devices,
    update_device_health,
    get_device_health_all,
    SERVER_CHECK_CLASSES,
    SWITCH_CHECK_CLASSES,
    SYSTEM_CHECK_CLASSES,
    PERIPHERAL_CHECK_CLASSES,
)

logger = logging.getLogger(__name__)


def print_health_table(targets, device_type):
    rows = []
    for target in targets:
        health = get_device_health_all(target)
        row = HealthRepr.from_dict(target.name, health).to_table_row(
            device_type
        )
        rows.append(row)
    header = ["device", "status"]
    header.extend(
        [attr for attr in get_health_attrs_for_device_type(device_type)]
    )
    header.append("message")
    print(tabulate(rows, header))


@dataclasses.dataclass
class HealthRepr:
    name: str
    overall: HealthState
    attrs: Dict[str, dict]

    def to_table_row(self, device_type: str) -> List[str]:
        r = [self.name, self.overall.value]
        msgs = []
        health_attrs = get_health_attrs_for_device_type(device_type)
        for attr in health_attrs:
            r.append(self.attrs[attr]["status"])
            if "message" in self.attrs[attr]:
                if self.attrs[attr]['message'] != "":
                    msgs.append(self.attrs[attr]['message'])
        msg = "; ".join(msgs)
        msg = msg[:57] + "..." if len(msg) > 60 else msg
        r.append(msg)
        return r

    def to_dict_obj(self) -> Dict:
        health_statuses = []
        for attr, item in self.attrs.items():
            data = item
            data.update({"name": attr})
            health_statuses.append(data)

        r = dict(
            name=self.name,
            health=self.overall.value,
            healthStatuses=health_statuses,
        )
        return r

    @classmethod
    def from_dict(cls, name, health):
        overall = None
        for _, status in health.items():
            cur_status = HealthState(status["status"])
            if not overall:
                overall = cur_status
            if overall < cur_status:
                overall = cur_status

        assert overall, "health item cannot be empty"
        return HealthRepr(name, overall, health)


class HealthShow(SubCommandABC):
    """
    Show server health
    """

    name = "show"
    device_type = None

    def construct(self):
        target_group = self.parser.add_mutually_exclusive_group(required=False)
        self.add_arg_filter(required=False)
        target_group.add_argument(
            "--degraded",
            help="filter for degraded (WARNING, CRITICAL) devices only",
            action="store_true",
            default=False,
        )
        target_group.add_argument(
            "--critical",
            help="filter for CRITICAL devices only",
            action="store_true",
            default=False,
        )
        self.parser.add_argument(
            "-o",
            "--output",
            help="specify output formats",
            choices=["table", "yaml", "json"],
            default="table",
            required=False,
        )

    def run(self, args):
        assert (
            self.device_type
        ), "device type should be specified before using this class"

        targets = self.filter_devices(
            args,
            device_type=self.device_type,
            query_set=Device.get_all(self.profile),
        )

        if args.degraded:
            targets = filter_degraded_devices(targets, self.device_type)
        elif args.critical:
            targets = filter_critical_devices(targets, self.device_type)

        # sorting based on name
        targets = targets.order_by("name")
        health_list_obj = []
        for target in targets:
            health = get_device_health_all(target)
            health_list_obj.append(
                HealthRepr.from_dict(target.name, health).to_dict_obj(),
            )

        if args.output == "yaml":
            print(
                yaml.dump(health_list_obj, default_flow_style=False, indent=2)
            )
        elif args.output == "json":
            print(json.dumps(health_list_obj, indent=2))
        elif args.output == "table":
            print_health_table(targets, self.device_type)


class HealthCheck(SubCommandABC):
    """
    Run health check on devices
    """

    name = "check"
    device_type = None

    def construct(self):
        self.add_arg_filter(required=False)
        self.parser.add_argument(
            "--degraded",
            help="filter for degraded (WARNING, CRITICAL) devices only",
            action="store_true",
            default=False,
        )

    def run(self, args):
        profilename = self.profile
        assert (
            self.device_type
        ), "device type should be specified before using this class"

        targets = self.filter_devices(
            args,
            device_type=self.device_type,
            query_set=Device.get_all(self.profile),
        )
        if args.degraded:
            targets = filter_degraded_devices(targets, self.device_type)

        targets_str = [tgt.name for tgt in targets]

        if self.device_type == DeploymentDeviceTypes.SERVER.value:
            check_classes = SERVER_CHECK_CLASSES
        elif self.device_type == DeploymentDeviceTypes.SWITCH.value:
            check_classes = SWITCH_CHECK_CLASSES
        elif self.device_type == DeploymentDeviceTypes.SYSTEM.value:
            check_classes = SYSTEM_CHECK_CLASSES
        elif self.device_type == DeploymentDeviceTypes.PERIPHERAL.value:
            check_classes = PERIPHERAL_CHECK_CLASSES
        else:
            check_classes = []

        for count, check_item in enumerate(check_classes):
            logger.info(
                f"checking {count + 1} out of {len(check_classes)}: "
                f"{check_item.command_name()}..."
            )
            check = check_item(profilename)
            check.run_check_update(targets_str)
        print_health_table(targets, self.device_type)


class HealthForceUpdate(SubCommandABC):
    """
    Force update server health
    """

    name = "force-update"
    device_type = None

    def construct(self):
        health_attrs = get_health_attrs_for_device_type(self.device_type)

        self.add_arg_filter(required=False)
        self.parser.add_argument(
            "--degraded",
            help="filter for degraded (WARNING, CRITICAL) devices only",
            action="store_true",
            default=False,
        )

        self.parser.add_argument(
            "--attr",
            help="specify health check attribute",
            choices=health_attrs,
            type=str,
            required=True,
        )

        self.parser.add_argument(
            "--status",
            help="specify health check attribute",
            choices=[e.value for e in HealthState],
            type=str,
            required=True,
        )

        self.parser.add_argument(
            "--message",
            help="add comment",
            type=str,
            default="manual user override",
            required=False,
        )

    def run(self, args):
        profilename = self.profile
        assert (
            self.device_type
        ), "device type should be specified before using this class"

        targets = self.filter_devices(
            args,
            device_type=self.device_type,
            query_set=Device.get_all(self.profile),
        )
        if args.degraded:
            targets = filter_degraded_devices(targets, self.device_type)

        for device in targets:
            update_device_health(
                profilename,
                device.name,
                args.attr,
                HealthState(args.status),
                args.message,
            )


class AddDeviceType:
    """
    Decorator to add device type
    """

    def __init__(self, device_type: str):
        self._device_type = device_type

    def __call__(self, cls):
        if not inspect.isclass(cls):
            raise ValueError(f'Non-class object provided: {type(cls).__name__}')

        setattr(cls, 'device_type', self._device_type)
        return cls
