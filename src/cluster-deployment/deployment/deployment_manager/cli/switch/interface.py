import dataclasses
import logging
import pathlib
from typing import List, Tuple, Type

import yaml

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.tools.middleware import must_lookup_switch_info, lookup_switch_info
from deployment_manager.db.models import Device, Link
from deployment_manager.tools.switch.interface import LldpNeighbor, SwitchInterface, InterfaceErrStats, MacAddrTableEntry
from deployment_manager.tools.switch.switchctl import SwitchInfo, perform_group_action, create_client
from deployment_manager.tools.utils import prompt_confirm, ReprFmtMixin, to_duration
from deployment_manager.db.const import DeploymentDeviceTypes

logger = logging.getLogger(__name__)

def _gather_switches(profile: str, devices: List[Device]) -> List[SwitchInfo]:
    rv: List[SwitchInfo] = []
    try:
        for d in devices:
            rv.append(must_lookup_switch_info(profile, d.name))
    except ValueError as e:
        logger.error(str(e))
        return []
    return rv

class Breakout(SubCommandABC):
    """
    Breakout ports according to link information and current port state of switch.

    This does not take into account the current breakout state of interfaces since this can be hard to determine
    without parsing the running-configuration of the switch. Therefore, this should only be run at switch initialization
    or partially manually applied.
    """
    name = "breakout"

    def construct(self):
        self.add_arg_filter(False)
        self.add_arg_noconfirm()
        self.add_arg_dryrun(help="Do not execute. Only print or write commands to --output_dir")
        self.parser.add_argument("--force", action="store_true", default=False,
                                 help="Force running breakout commands even if target ports appear to be in the "
                                      "target breakout mode")
        self.parser.add_argument("--output_dir", default="", help="Location to write output, else print to stdout")

    def run(self, args):
        switches = Device.get_all(self.profile).filter(device_type="SW", device_role__in=("SP", "SX", "MX", "LF"))
        switches = self.filter_devices(args, query_set=switches)

        infos = _gather_switches(self.profile, switches)
        if not infos:
            logger.info("nothing to configure")
            return 1

        inf_speeds = {}
        for device in switches:
            speeds = {}
            src_links = Link.objects.filter(src_device__device_id=device.device_id)
            for l in src_links:
                speeds[l.src_if] = l.speed
            dst_links = Link.objects.filter(dst_name=device.name, src_device__profile__name=self.profile)
            for l in dst_links:
                speeds[l.dst_if] = l.speed
            inf_speeds[device.name] = speeds

        # TODO: parallelize
        sw_cmds = {}
        for switch_info in infos:
            with create_client(switch_info) as client:
                sw_cmds[switch_info.name] = client.get_breakout_commands(inf_speeds.get(switch_info.name), force=args.force)

        modified = []
        status = ""
        if args.output_dir:
            output_path = pathlib.Path(args.output_dir)
            output_path.mkdir(exist_ok=True)
            for name, cmds_warnings in sw_cmds.items():
                for warning in cmds_warnings[1]:
                    logger.warning(f"{name} {warning}")
                if cmds_warnings[0]:
                    (output_path / name).write_text("\n".join(cmds_warnings[0]))
                    modified.append(name)
        else:
            doc = {"warnings": {}, "commands": {}}
            for name, cmds_warnings in sw_cmds.items():
                cmds, warnings = cmds_warnings[0], cmds_warnings[1]
                if warnings:
                    doc["warnings"][name] = warnings
                if cmds:
                    doc["commands"][name] = cmds if len(cmds) < 10 else [*cmds[0:9], "...output truncated..."]
                    modified.append(name)
            if not doc["warnings"]:
                del doc["warnings"]
            status = yaml.safe_dump(doc, default_flow_style=False)

        if args.dryrun:
            return 0
        if not modified:
            logger.info("no new breakouts detected. Use --force to force breakouts if needed")
            return 0
        elif status:
            logger.info(status)

        print(f"modifying {len(modified)} switches:")
        print("  " + "\n  ".join(modified))
        if not args.noconfirm and not prompt_confirm("continue?"):
            return 0

        rv = 0
        args = [((sw_cmds[i.name][0],),) for i in infos]
        result = perform_group_action(infos, "execute_commands", switch_args=args)
        for sw, res in result.items():
            if isinstance(res, str):
                logger.error(f"{sw} failed: {res}")
                rv = 1

        return rv


class _GenericSwitchCmd(SubCommandABC):

    def class_params(self) -> Tuple[Type[ReprFmtMixin], str]:
        raise NotImplementedError(f"{self.__class__} error")

    def post_process(self, results):
        return results

    def construct(self):
        self.add_arg_filter(False)
        self.add_arg_output_format()

    def run(self, args):
        repr_type, method_name = self.class_params()

        switches = Device.get_all(self.profile).filter(device_type="SW")
        switches = self.filter_devices(args, query_set=switches)
        devices = _gather_switches(self.profile, switches)
        if not devices:
            logger.error("no devices found to query")
            return 1

        result = perform_group_action(devices, method_name)
        ok, err = [], []
        for k, v in result.items():
            if isinstance(v, str):
                err.append(f"{k}: {v}")
            else:
                ok.extend(v)

        if err:
            logger.error(f"Failed to {method_name} on {len(err)} switches:")
            for e in err:
                logger.error(e)

        ok = self.post_process(ok)
        print(repr_type.format_reprs(ok, args.output))

        if err:
            return 1
        return 0


class Status(_GenericSwitchCmd):
    """ Show interface status normalized across vendors """
    name = "status"

    def class_params(self) -> Tuple[Type[ReprFmtMixin], str]:
        return SwitchInterface, "list_interfaces"

    def post_process(self, results):
        return results


class Lldp(_GenericSwitchCmd):
    """ Show interface lldp normalized across vendors """
    name = "lldp"

    def class_params(self) -> Tuple[Type[ReprFmtMixin], str]:
        return LldpNeighbor, "list_lldp_neighbors"

    def post_process(self, results):
        return [r for r in results if r.dst_name]


@dataclasses.dataclass
class InterfaceDebugStats(InterfaceErrStats):
    """ Stats requested by field team for debugging links. Not all switch vendors will support all stats
    """
    lldp_neighbor: str
    lldp_neighbor_if: str

    @classmethod
    def from_stats(cls, stats: InterfaceErrStats):
        return cls(switch=stats.switch,
                   name=stats.name,
                   up=stats.up,
                   state_duration=stats.state_duration,
                   min_rx_power=stats.min_rx_power,
                   min_tx_power=stats.min_tx_power,
                   tail_phys_errs=stats.tail_phys_errs,
                   raw=stats.raw,
                   lldp_neighbor="",
                   lldp_neighbor_if="")

    def to_table_row(self) -> List[str]:
        def fmt_power(f):
            return "" if not f else f"{f:.1f}"
        return [self.switch, self.name,
                self.up, to_duration(self.state_duration),
                fmt_power(self.min_rx_power),
                fmt_power(self.min_tx_power),
                self.tail_phys_errs,
                self.lldp_neighbor,
                self.lldp_neighbor_if,
        ]

    def to_csv_row(self) -> List[str]:
        return [self.switch, self.name, self.up, self.state_duration, self.min_rx_power, self.min_tx_power,
                self.tail_phys_errs, self.lldp_neighbor, self.lldp_neighbor_if]

    @classmethod
    def table_header(cls) -> List[str]:
        return ["switch", "name", "up", "state_duration", "min_rx_power", "min_tx_power",
                "tail_phys_errs", "neighbor", "neighbor_if"]

    def to_dict(self) -> dict:
        return self.__dict__


class ErrStatus(SubCommandABC):
    """ Show interface error statuses normalized across vendors """
    name = "err_status"

    def construct(self):
        self.add_arg_filter(False)
        self.add_arg_output_format()

    def run(self, args):
        switches = Device.get_all(self.profile).filter(device_type="SW", device_role__in=("SX", "MX", "LF", "SP", "MC"))
        switches = self.filter_devices(args, query_set=switches)
        devices = _gather_switches(self.profile, switches)
        if not devices:
            logger.error("no devices found to query")
            return 1

        result = perform_group_action(devices, 'list_interfaces_err_stats')
        ok_status, err_status, remove = {}, [], []
        for k, v in result.items():
            if isinstance(v, str):
                err_status.append(f"{k}: {v}")
                remove.append(k)
            else:
                for status_val in v:
                    ok_status[(status_val.switch, status_val.name)] = InterfaceDebugStats.from_stats(status_val)
        if err_status:
            logger.error(f"Failed to 'list_interfaces_err_stats' on {len(err_status)} switches:")
            for e in err_status:
                logger.error(e)

        devices = [d for d in devices if d.name not in remove]
        result = perform_group_action(devices, 'list_lldp_neighbors')
        ok_lldp, err_lldp = {}, []
        for k, v in result.items():
            if isinstance(v, str):
                err_lldp.append(f"{k}: {v}")
            else:
                for lldp_val in v:
                    if not lldp_val.dst_name:
                        continue
                    rv = ok_status.get((lldp_val.src, lldp_val.src_if))
                    if rv:
                        rv.lldp_neighbor = lldp_val.dst_name
                        rv.lldp_neighbor_if = lldp_val.dst_if
                        rv.raw["lldp"] = lldp_val
        if err_status:
            logger.error(f"Failed to 'list_lldp_neighbors' on {len(err_status)} switches:")
            for e in err_status:
                logger.error(e)

        print(InterfaceDebugStats.format_reprs(ok_status.values(), args.output))

        if err_status:
            return 1
        return 0

class MacTable(SubCommandABC):
    """
    Show the MAC address table of a switch
    """

    name = "mac_table"

    def construct(self):
        self.add_arg_filter(required=False)
        self.add_arg_output_format()

    def run(self, args):
        targets = self.filter_devices(
            args,
            device_type=None,
            query_set=Device.get_all(self.profile).filter(device_type="SW").exclude(name__icontains='virtual'),
        )
        switch_infos = _gather_switches(self.profile, targets)
        sw_mac_tables = perform_group_action(list(switch_infos), "get_mac_addr_table")
        entries = []
        for table_entries in sw_mac_tables.values():
            if isinstance(table_entries, str):
                logger.error(f"failed to gather MAC address table: {table_entries}")
            else:
                entries.extend(table_entries)
        print(MacAddrTableEntry.format_reprs([entry for entry in entries], args.output))
        return 0

class InterfaceCmd(SubCommandABC):
    """ Switch interface related actions """

    SUB_CMDS = [Breakout, Status, Lldp, ErrStatus, MacTable]

    name = "interface"

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
