import dataclasses
import logging
from django.db.models import Q
from typing import Dict, List, Optional

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.switch.interface import _gather_switches
from deployment_manager.cli.switch.link.reprs import LINK_STATE_MATCH, LINK_STATE_MISMATCH, LINK_STATE_MISSING, \
    LINK_STATE_UNRECOGNIZED, \
    LinkNeighStatusRepr
from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device, DeviceProperty, Link
from deployment_manager.tools.switch.interface import LldpNeighbor
from deployment_manager.tools.switch.switchctl import perform_group_action

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class _LinkSearch:
    dst: str
    dst_if: str
    found: bool = False
    origin: str = ""
    dst_alias: str = ""


def _generate_reverse_lookup_keys(lldp_data: LldpNeighbor, alias_names: Dict[str,str]) -> List[str]:
    keys = []

    if lldp_data.dst_name.endswith("-cm"):
        # likely a system name, usually end in "-cm"
        sys_name = lldp_data.dst_name[:-3]
        keys.append(f"{sys_name}:{lldp_data.dst_if}")
        if sys_name in alias_names:
            keys.append(f"{alias_names[sys_name]}:{lldp_data.dst_if}")

    if "." in lldp_data.dst_name:
        # likely FQDN
        short_name = lldp_data.dst_name.split(".")[0]
        keys.append(f"{short_name}:{lldp_data.dst_if}")
        if short_name in alias_names:
            keys.append(f"{alias_names[short_name]}:{lldp_data.dst_if}")

    # default case
    keys.append(f"{lldp_data.dst_name}:{lldp_data.dst_if}")

    return keys


def _generate_forwards_lookup_keys(name: str, inf: str, alias_name: Optional[str]) -> List[str]:
    keys = [
        f"{name}:{inf}",
    ]
    if alias_name:
        keys.append(f"{alias_name}:{inf}")

    # try looking up system names, e.g. -cm
    keys.append(f"{name}-cm:{inf}")
    if alias_name:
        keys.append(f"{alias_name}-cm:{inf}")

    return keys


def perform_matching(
        sw_lldp_data: Dict[str, List[LldpNeighbor]],
        links: List[Link],
        alias_names: Dict[str, str]
) -> List[LinkNeighStatusRepr]:

    # switch -> interface -> LLDP info
    actual_neighbors: Dict[str, Dict[str, LldpNeighbor]] = {}
    for sw, result in sw_lldp_data.items():
        result: List[LldpNeighbor] = result
        for r in result:
            r.dst_name = r.dst_name.split(".")[0]  # strip FQDN
        actual_neighbors[sw] = {
            r.src_if: r
            for r in result if r.dst_name
        }

    # switch -> inf -> dst info
    sw_names = list(sw_lldp_data.keys())
    expected_neighbors: Dict[str, Dict[str, _LinkSearch]] = {n: {} for n in sw_names}
    for l in links:
        if l.src_device.name in sw_names:
            expected_neighbors[l.src_device.name][l.src_if] = _LinkSearch(
                l.dst_name, l.dst_if, False, l.origin, alias_names.get(l.dst_name, "")
            )
        if l.dst_name in sw_names:
            expected_neighbors[l.dst_name][l.dst_if] = _LinkSearch(
                l.src_device.name, l.src_if, False, l.origin, alias_names.get(l.src_device.name, "")
            )

    statuses: List[LinkNeighStatusRepr] = []
    # loop through actual lldp neighbors, lookup expected neighbors
    actual_reverse_lookup = {
        f"{lldp.dst_name}:{lldp.dst_if}": f"{sw}:{inf}"
        for sw, lldps in actual_neighbors.items()
        for inf, lldp in lldps.items()
    }
    expect_reverse_lookup = {
        f"{inf_info.dst}:{inf_info.dst_if}": f"{sw}:{inf}"
        for sw, inf_infos in expected_neighbors.items()
        for inf, inf_info in inf_infos.items()
    }

    for sw, if_info in actual_neighbors.items():
        expect = expected_neighbors.get(sw, {})
        for inf, lldp in if_info.items():
            expect_lldp = expect.get(inf)
            if not expect_lldp:  # not expecting a neighbor but found one
                comment, state = "", LINK_STATE_UNRECOGNIZED
                for key in _generate_reverse_lookup_keys(lldp, alias_names):
                    if key in expect_reverse_lookup:
                        comment = f"move to {expect_reverse_lookup[key]}"
                        state = LINK_STATE_MISMATCH
                        break
                statuses.append(LinkNeighStatusRepr(sw, inf, "", "",
                                                    "LLDP",
                                                   lldp.dst_name, lldp.dst_if,
                                                   state, "", "", "", comment))
                continue

            expect_lldp.found = True
            if (not lldp.dst_name.startswith(expect_lldp.dst) and (not expect_lldp.dst_alias or not lldp.dst_name.startswith(expect_lldp.dst_alias))) or expect_lldp.dst_if != lldp.dst_if:
                comment = ""
                for key in _generate_forwards_lookup_keys(expect_lldp.dst, expect_lldp.dst_if, expect_lldp.dst_alias):
                    if key in actual_reverse_lookup:
                        comment = f"move from {actual_reverse_lookup[key]}"
                        break
                statuses.append(LinkNeighStatusRepr(sw, inf,
                                                   expect_lldp.dst, expect_lldp.dst_if,
                                                   "LLDP",
                                                   lldp.dst_name, lldp.dst_if,
                                                   LINK_STATE_MISMATCH,
                                                   expect_lldp.origin,
                                                   "", "",
                                                   comment))
            else:
                statuses.append(LinkNeighStatusRepr(sw, inf,
                                                   expect_lldp.dst, expect_lldp.dst_if,
                                                   "LLDP",
                                                   lldp.dst_name, lldp.dst_if,
                                                   LINK_STATE_MATCH,
                                                   expect_lldp.origin))
        # for any links not marked, add as not found
        for inf, expect_lldp in expect.items():
            if expect_lldp.found:
                continue
            comment, state = "", LINK_STATE_MISSING
            for key in _generate_forwards_lookup_keys(expect_lldp.dst, expect_lldp.dst_if, expect_lldp.dst_alias):
                if key in actual_reverse_lookup:
                    comment = f"move from {actual_reverse_lookup[key]}"
                    state = LINK_STATE_MISMATCH
                    break
            statuses.append(LinkNeighStatusRepr(
                sw, inf,
                expect_lldp.dst, expect_lldp.dst_if,
                "LLDP",
                "", "",
                state,
                expect_lldp.origin,
                "", "",
                comment))
    return statuses

class LinkLldpStatus(SubCommandABC):
    """ Compare switch LLDP output against link data """
    name = "lldp"

    def construct(self):
        self.add_arg_filter()
        self.add_arg_output_format()
        self.add_error_only_flag()

    def run(self, args):
        # find target switches, and links associated with those switches
        switches = Device.get_all(self.profile).filter(
            device_type="SW", device_role__in=("SX", "MX", "LF", "SP", "MC")
        )
        switches = self.filter_devices(args, query_set=switches)
        switch_infos = _gather_switches(self.profile, switches)
        if not switch_infos:
            logger.error("no switches matched filter")
            return 1

        # gather switches -> List[LldpNeighbor]|error
        rv = 0
        sw_lldp_data = perform_group_action(list(switch_infos), "list_lldp_neighbors")
        ok_switches = set()
        for sw, result in sw_lldp_data.items():
            if isinstance(result, str):
                logger.error(f"failed to gather lldp results from {sw}: {result}")
                rv = 1
            else:
                ok_switches.add(sw)

        # gather links for succeeded switches
        links: List[Link] = Link.objects.filter(
            Q(src_device__name__in=list(ok_switches)) | Q(dst_device__name__in=list(ok_switches))
        )
        alias_names: Dict[str, str] = dict()
        # actual_neighbors is the list of neighbors as reported by the switch.
        # The switch reports the hostname as reported by the device. In the case of systems
        # the hostname is stored in management_info.name so that needs to be taken into account
        prop = props.prop_management_info_name
        for p in DeviceProperty.get_all(self.profile, prop).filter(device__name__in=[l.dst_name for l in links]):
            alias_names[p.device.name] = prop.cast_value(p.property_value)

        statuses = perform_matching(sw_lldp_data, links, alias_names)

        if args.error_only:
            statuses = [s for s in statuses if s.state != LINK_STATE_MATCH]

        print(LinkNeighStatusRepr.format_reprs(statuses, args.output))
        return rv
