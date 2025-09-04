import dataclasses
import logging
from typing import List

from deployment_manager.cli.device.utils import DeviceUpdateRepr, commit_device_updates
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles
from deployment_manager.db.models import Device, QuerySet
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.dhcp import interface as dhcp
from deployment_manager.tools.utils import prompt_confirm, ReprFmtMixin

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class DeviceDiscoverIPMacUpdateRepr(ReprFmtMixin):
    name: str
    default_hostname: str
    ok: bool  # if the lease was found or the system already has a MAC/IP
    status: str  # LEASE_NOT_FOUND, NO_CHANGE, ADD, CHANGE
    current_ip: str = ""
    current_mac: str = ""
    dhcp_ip: str = ""
    dhcp_mac: str = ""

    @property
    def sort_key(self):
        return (self.name,)

    @classmethod
    def table_header(cls) -> list:
        return ["name", "status", "current_ip", "current_mac", "dhcp_ip", "dhcp_mac",]

    def to_table_row(self) -> list:
        return [self.name, self.status, self.current_ip, self.current_mac, self.dhcp_ip, self.dhcp_mac,]


def get_system_ip_mac_updates(profile: str, systems: QuerySet[Device]) -> List[DeviceDiscoverIPMacUpdateRepr]:
    """ Compare the DHCP lease entries with the mac/ip values of the systems and provide the diff as output. """
    if not systems:
        return []

    cg = ConfGen(profile)
    dhcp_provider = dhcp.get_provider(profile, cg.parse_profile())
    host_lease = {}
    for lease in dhcp_provider.get_leases():
        host_lease[lease.host.split(".")[0]] = lease

    # get systems without IP and/or MAC property
    results = []
    default_hostnames = {}  # if systems are aliased - wse0xx-cs-sy0x -> xs0xxxx
    for s in systems:
        alt_name = s.get_prop(props.prop_management_info_name)
        default_hostnames[s.name] = alt_name or s.name

    for s in systems:
        current_mac = s.get_prop(props.prop_management_info_mac)
        current_mac = current_mac if current_mac else ""
        current_ip = s.get_prop(props.prop_management_info_ip)
        current_ip = str(current_ip) if current_ip else ""
        ok = current_ip and current_mac

        repr = DeviceDiscoverIPMacUpdateRepr(
            name=s.name,
            default_hostname=default_hostnames[s.name],
            ok=ok,
            status="NOT_SET",
            current_ip=current_ip,
            current_mac=current_mac,
        )

        if s.name in host_lease:
            lease = host_lease[s.name]
        elif default_hostnames[s.name] in host_lease:
            lease = host_lease[default_hostnames[s.name]]
        else:
            repr.status = "LEASE_NOT_FOUND"
            results.append(repr)
            continue

        if not current_ip or not current_mac:
            repr.status = "ADD"
        elif not (current_ip == str(lease.ip) and current_mac == lease.mac):
            repr.status = "CHANGE"
        else:
            repr.status = "NO_CHANGE"
        repr.dhcp_mac = lease.mac
        repr.dhcp_ip = lease.ip
        repr.ok = True
        results.append(repr)

    return results


class AssignIPs(SubCommandABC):
    """
    Assign management IPs to systems. In v2 management IP allocation, systems are assumed to be added
    to racks without knowledge of their subnet association ahead of time. Once they receive a dynamic
    IP allocation, it's preferable to assign the IP permanently to avoid the possibility of the IP
    changing and breaking downstream services that depend on a stable IP address.
    """
    name = "assign_mgmt_ips"

    def construct(self):
        self.add_arg_noconfirm(help="Do not ask for confirmation before syncing to the DB")
        self.add_arg_filter()
        self.add_arg_output_format()
        self.add_arg_dryrun()
        self.add_error_only_flag()

    def run(self, args):
        selected_systems = self.filter_devices(args, query_set=Device.get_all(self.profile, device_type="SY"))

        updates = get_system_ip_mac_updates(self.profile, selected_systems)

        if args.dryrun or args.error_only:
            if args.error_only:
                updates = [u for u in updates if not u.ok]
            print(DeviceDiscoverIPMacUpdateRepr.format_reprs(updates, args.output))
            return 0

        if not args.noconfirm or not updates:
            warnings = [r for r in updates if not r.ok]
            if warnings:
                print(
                    "The following devices could not be discovered:\n" +
                    DeviceDiscoverIPMacUpdateRepr.format_reprs(warnings, args.output)
                )
            require_update = [r for r in updates if r.status in ("ADD", "CHANGE",) and r.ok]
            if not require_update:
                print("No updates found")
                return 0
            print(
                "The following devices require an update:\n" +
                DeviceDiscoverIPMacUpdateRepr.format_reprs(require_update, args.output)
            )
            if not prompt_confirm("Perform update?"):
                print("No update performed")
                return 0

        commit = [DeviceUpdateRepr(
            name=r.name, type="SY", role=DeploymentDeviceRoles.SYSTEM.value,
            prop_updates={
                props.prop_management_info_ip: r.dhcp_ip,
                props.prop_management_info_mac: r.dhcp_mac,
            },
        ) for r in updates if r.status in ("ADD", "CHANGE",)]
        commit_device_updates(self.profile, commit)
        return 0
