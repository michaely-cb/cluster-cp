import dataclasses
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List

from tabulate import tabulate

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceTypes
from deployment_manager.db.models import Device
from deployment_manager.tools.pb1.health_check import (
    filter_devices_with_health_state,
)
from deployment_manager.tools.pb1.ipmi import with_ipmi_client

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class MacUpdateRepr:
    name: str
    type: str
    updated: bool
    mac: str
    error: str


def update_device_mac(
    server: Device, update_ipmi_mac: bool = True, force: bool = False
) -> List[MacUpdateRepr]:
    """ Returns updated or error macs """
    rv = []

    mgmt_mac = server.get_prop(props.prop_management_info_mac)
    ipmi_mac = server.get_prop(props.prop_ipmi_info_mac)
    if not force and mgmt_mac and (ipmi_mac or not update_ipmi_mac):
        return []

    with with_ipmi_client(*server.get_ipmi_client_args()) as ipmi:
        # update mgmt mac address
        if not mgmt_mac or force:
            try:
                actual_mgmt_mac = ipmi.find_mgmt_mac_address()
                if actual_mgmt_mac != mgmt_mac:
                    server.set_prop(props.prop_management_info_mac, actual_mgmt_mac)
                    rv.append(MacUpdateRepr(server.name, "mgmt", True, actual_mgmt_mac, ""))
            except Exception as e:
                rv.append(MacUpdateRepr(server.name, "mgmt", False, "", str(e)))

        if not update_ipmi_mac:
            return rv

        # optionally update ipmi mac address
        ipmi_mac = server.get_prop(props.prop_ipmi_info_mac)
        if not ipmi_mac or force:
            try:
                actual_ipmi_mac = ipmi.find_ipmi_mac_address()
                if actual_ipmi_mac != ipmi_mac:
                    server.set_prop(props.prop_ipmi_info_mac, actual_ipmi_mac)
                    rv.append(MacUpdateRepr(server.name, "ipmi", True, actual_ipmi_mac, ""))
            except Exception as e:
                rv.append(MacUpdateRepr(server.name, "ipmi", False, "", str(e)))
    return rv


class UpdateMacAddress(SubCommandABC):
    """
    Update mac address for mgmt port and ipmi port of all servers. Queries the
    IPMI redfish API to get mac address info.
    """

    name = "update_mac_address"

    def construct(self):
        target_group = self.parser.add_mutually_exclusive_group(required=False)
        self.add_arg_filter(required=False, group=target_group)
        target_group.add_argument(
            "--healthy-only",
            help="filter devices whose mgmt link is healthy",
            action="store_true",
            default=False,
        )
        self.parser.add_argument(
            "--force",
            help="force devices which already have a MAC property set to update their mac address anyways",
            action="store_true",
            default=False,
        )
        self.parser.add_argument(
            "--skip-ipmi-mac",
            help="skip updating mac info for ipmi port. "
            "Use this option if you want to use the IPMI hostname for DHCP IP assignment",
            action="store_true",
            default=False,
        )

    def run(self, args):
        if args.filter:
            targets = self.filter_devices(
                args,
                device_type=DeploymentDeviceTypes.SERVER.value,
                query_set=Device.get_all(self.profile),
            )
        elif args.healthy_only:
            targets = filter_devices_with_health_state(
                self.profile, "SR", "mgmt_link", "OK"
            )
        else:
            targets = Device.get_all(self.profile, device_type="SR")

        if targets.count() == 0:
            logger.info(
                "No device found to update its mac address, "
                "check if there are healthy devices"
            )
            return 0

        results = []
        with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
            future_to_device = {
                executor.submit(
                    update_device_mac, tgt, not args.skip_ipmi_mac, args.force
                ): tgt
                for tgt in targets
            }
            for f, device in future_to_device.items():
                try:
                    results.extend(f.result())
                except Exception as e:
                    results.append(MacUpdateRepr(device.name, "*", False, "", str(e)))

        error_count = len(set([r.name for r in results if r.error != ""]))
        updated_count = len(set([r.name for r in results if r.updated]))

        if results:
            rows = [dataclasses.asdict(r) for r in results]
            logger.info("\n" + tabulate(rows, headers="keys"))
        logger.info(f"Updated {updated_count} of {targets.count()} servers, {error_count} errors.")
        return error_count
