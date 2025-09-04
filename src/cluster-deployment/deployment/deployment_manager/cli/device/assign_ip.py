import ipaddress
import logging
import yaml
from typing import List, Dict

from deployment_manager.cli.device.utils import DeviceUpdateRepr, commit_device_updates
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles as DRoles
from deployment_manager.db.const import DeploymentDeviceTypes as DTypes
from deployment_manager.db.device_props import asn
from deployment_manager.db.models import Device, get_props_values, get_devices_with_prop
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.ip_allocator import IPAllocator, NetworkExtent, PREFIX_SETTINGS
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)

def load_allocator_from_db(profile: str, cfg: dict) -> IPAllocator:
    nw_cfg = cfg["mgmt_network_int_config"]
    alloc_cfg = nw_cfg.get("ip_allocation", None)
    if alloc_cfg is None:
        raise ValueError(
            "config field 'mgmt_network_int_config.ip_allocation' must be set in order to allocate IPs")
    strategy = alloc_cfg.get("allocation_strategy", "")
    if strategy.lower() != "v2":
        raise ValueError(f"unsupported 'mgmt_network_int_config.ip_allocation.allocation_strategy', '{strategy}'")

    subnet_prefixlen = nw_cfg.get("mgmt_netmask_per_rack", 24)
    if isinstance(subnet_prefixlen, str):
        subnet_prefixlen = int(subnet_prefixlen.replace("/", ""))

    asn_start = asn(alloc_cfg["asn_start"])
    asn_count = int(alloc_cfg["asn_count"])
    asn_end = asn(asn_start + asn_count)

    used_ips = [
        *get_props_values(profile, props.prop_ipmi_info_ip),
        *get_props_values(profile, props.prop_management_info_ip),
        *get_props_values(profile, props.prop_switch_info_loopback_ip),
        *get_props_values(profile, props.prop_subnet_info_gateway),
    ]
    # ensure vip is reserved if present
    vip = nw_cfg.get("mgmt_vip", {}).get("vip")
    if vip:
        used_ips.append(ipaddress.ip_address(vip))

    used_asn = get_props_values(profile, props.prop_subnet_info_asn)
    if "asn_agg_0" in nw_cfg:
        used_asn.append(asn(nw_cfg["asn_agg_0"]))
    if "asn_agg_1" in nw_cfg:
        used_asn.append(asn(nw_cfg["asn_agg_1"]))

    used_subnets = get_props_values(profile, props.prop_subnet_info_subnet)

    network_extents: Dict[str, NetworkExtent] = {}
    for extent in alloc_cfg.get("network_extents", []):
        subnets = [ipaddress.IPv4Network(sn) for sn in extent["subnets"]]
        network_extents[extent["name"]] = NetworkExtent(subnets)
        if "deploy_mgmt" == extent["name"]:
            used_subnets.extend(subnets)

    return IPAllocator(
        ipaddress.ip_network(alloc_cfg["parentnet"]),
        subnet_prefixlen,
        network_extents,
        asn_start=asn_start,
        asn_end=asn_end,
        used_subnet=used_subnets,
        used_asn=used_asn,
        used_ip=used_ips,
    )


def gather_allocations(profile: str, ip_allocator: IPAllocator) -> List[DeviceUpdateRepr]:
    device_updates = []

    mg_switches = Device.get_all(
        profile,
        device_type=DTypes.SWITCH.value,
        device_role=DRoles.MANAGEMENT.value,
    )
    mg_switches = sorted(mg_switches, key=lambda d: d.name)

    # perform mg_switch initial allocations: subnet, asn, and mg IP
    rack_sn = {}
    for sw in mg_switches:
        rack = sw.get_prop(props.prop_location_rack)
        if not rack:
            logger.warning(f"switch {sw.name} did not have a rack assigned, skipping allocation")
            continue

        updates = {}

        # assign subnet
        sn = sw.get_prop(props.prop_subnet_info_subnet)
        if not sn:
            sn = ip_allocator.next_subnet(prefixlen=sw.get_prop(props.prop_subnet_info_prefixlen))
            updates[props.prop_subnet_info_subnet] = sn
        rack_sn[rack] = sn
        additional_racks = sw.get_prop(props.prop_switch_info_connected_racks)
        if additional_racks:
            for rack in additional_racks.split(","):
                rack_sn[rack] = sn

        # assign asn
        asn = sw.get_prop(props.prop_subnet_info_asn)
        if not asn:
            asn = ip_allocator.next_asn()
            updates[props.prop_subnet_info_asn] = asn

        # assign mgmt IP
        mg_ip = sw.get_prop(props.prop_management_info_ip)
        if not mg_ip:
            deploy_sn = sw.get_prop(props.prop_switch_info_deploy_subnet, include_default=False)
            if deploy_sn:
                ip = ip_allocator.next_ip(deploy_sn)
            else:
                try:
                    ip = ip_allocator.next_extent_ip("deploy_mgmt")
                except ValueError:
                    settings = PREFIX_SETTINGS.get(sn.prefixlen)
                    offset = 0 if not settings else settings.dynamic_start_offset
                    ip = ip_allocator.next_ip(sn, preferred_offset=offset)
            updates[props.prop_management_info_ip] = ip

        # assign gw
        mg_gw = sw.get_prop(props.prop_subnet_info_gateway)
        if not mg_gw:
            gw_ip = ip_allocator.next_ip(sn, preferred_offset=-1)
            updates[props.prop_subnet_info_gateway] = gw_ip

        # assign loopback
        loopback = sw.get_prop(props.prop_switch_info_loopback_ip)
        if not loopback:
            try:
                ip = ip_allocator.next_extent_ip("loopback")
                updates[props.prop_switch_info_loopback_ip] = ip
            except ValueError:
                pass

        if updates:
            device_updates.append(DeviceUpdateRepr(sw.name, sw.device_type, sw.device_role, updates))

    # assign devices by rack
    for rack, sn in rack_sn.items():
        devices = sorted(
            get_devices_with_prop(profile, props.prop_location_rack, rack),
            key=lambda d: (d.device_type, d.device_role, d.name)
        )
        for d in devices:
            updates = {}
            if d.device_type == DTypes.SERVER.value:
                # assign mgmt IP
                mg_ip = d.get_prop(props.prop_management_info_ip)
                if not mg_ip:
                    updates[props.prop_management_info_ip] = ip_allocator.next_ip(sn)

                # assign ipmi IP
                ipmi_ip = d.get_prop(props.prop_ipmi_info_ip)
                if not ipmi_ip:
                    updates[props.prop_ipmi_info_ip] = ip_allocator.next_ip(sn)
            elif ((d.device_type == DTypes.SWITCH.value and d.device_role != DRoles.MANAGEMENT.value)
                  or d.device_type in (DTypes.CONSOLE.value,
                                       DTypes.PERIPHERAL.value,
                                       # DTypes.SYSTEM.value XXX: systems are generally found on the network based on DHCP leases
                                       )):
                # assign mgmt IP
                mg_ip = d.get_prop(props.prop_management_info_ip)
                if not mg_ip:
                    settings = PREFIX_SETTINGS.get(sn.prefixlen)
                    offset = 0 if not settings else settings.dynamic_start_offset
                    updates[props.prop_management_info_ip] = ip_allocator.next_ip(
                        sn, preferred_offset=offset
                    )
            if updates:
                device_updates.append(DeviceUpdateRepr(d.name, d.device_type, d.device_role, updates))
    return device_updates


class AssignIPs(SubCommandABC):
    """
    Assign management IPs to devices without them. Displays a list of changes prior to committing
    them. Does not alter existing IP or subnet assignments.

    Uses parameters defined in input.yaml::mgmt_network_int_config.ip_allocation to assign subnets
    to management switches, and IPs to interfaces. See input.yaml schema for more details.
    """
    name = "assign_mgmt_ips"

    def construct(self):
        self.parser.add_argument(
            "--noconfirm", "-y",
            help="Do not ask for confirmation before syncing to the DB",
            action="store_true",
            default=False,
        )

    def run(self, args):
        cg = ConfGen(self.profile)
        ip_allocator = load_allocator_from_db(self.profile, cg.parse_profile())
        d_updates = gather_allocations(self.profile, ip_allocator)
        if not d_updates:
            logger.info("no devices require allocation")
            return 0

        # separate the mg switch updates (which update more fields than just IP) from servers/data_switches which
        # only update mgmt IPs and IPMI IPs
        mg_switch_updates = sorted([
            d.dict() for d in d_updates
            if d.type == DTypes.SWITCH.value and d.role == DRoles.MANAGEMENT.value
        ], key=lambda d: d["name"])
        mg_switch_names = set([s["name"] for s in mg_switch_updates])
        device_updates = sorted([
            {"name": d.name, "ips": [str(ip) for ip in d.prop_updates.values()]}
            for d in d_updates
            if d.name not in mg_switch_names
        ], key=lambda d: d["name"])
        msg = yaml.dump([*mg_switch_updates, *device_updates], default_flow_style=False)
        print(f"{len(d_updates)} devices require allocation:\n{msg}")
        if not args.noconfirm and not prompt_confirm("Proceed with allocation?"):
            print("aborted")
            return 0

        commit_device_updates(self.profile, d_updates)
