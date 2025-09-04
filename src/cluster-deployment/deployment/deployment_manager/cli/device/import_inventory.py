import collections
import logging
import pathlib
import yaml
from django.db import transaction
from typing import List

from deployment_manager.cli.device.utils import DeviceRepr
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.device.config_sanity import check_property_format, check_property_duplicate
from deployment_manager.db.const import DeploymentDeviceTypes
from deployment_manager.db.models import Device, DeploymentDeviceRoles
from deployment_manager.db.device_props import ENFORCE_UNIQUE_PROPS
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.utils import prompt_confirm
from deployment_manager.tools.vendor import process_vendor_file

logger = logging.getLogger(__name__)


@transaction.atomic()
def sync_devices(profile: str, devices: List[DeviceRepr]):
    errors = []
    for d in devices:
        dev_entity = Device.get_device(d.name, profile_name=profile)
        if dev_entity:
            dev_entity.device_type = d.type
            dev_entity.device_role = d.role
            dev_entity.save()
        else:
            dev_entity = Device.add(d.name, d.type, d.role, profile)
        dev_entity.batch_update_properties(d.properties)
        device_queryset = Device.objects.filter(name=d.name, profile__name=profile)
        errors.extend(check_property_format(profile, device_queryset))
    for key, duplicate_prop_group in ENFORCE_UNIQUE_PROPS.items():
        errors.extend(check_property_duplicate(profile, duplicate_prop_group))
    
    if errors:
        raise ValueError("Errors found during device sync:\n" +
                         "\n".join([f"{e.device}: {e.message}" for e in errors]))

    
        

class ImportInventory(SubCommandABC):
    """
    Import an inventory CSV: a CSV file containing minimal information about devices sufficient for
    deployment in the racks.

    This is a minimal implementation and its update capabilities have the limitation:
    When updating the DB, it does not scan devices for devices matching identifying fields such as MAC addresses.
    It searches for a device in the profile matching the generated name for the device and creates/updates it
    """

    name = "import_inventory"
    description = f"""
    The vendor import must contain the following columns:

    location.rack               String describing the rack, e.g. wse001
    type                        One of {','.join(sorted([t.value for t in DeploymentDeviceTypes]))} (console, peripheral, server, switch, system, ...)
    role                        One of {','.join(sorted([role.value for role in DeploymentDeviceRoles]))}
    vendor.name                 One of AR,CS,DL,HP,SM,EC,JU (arista, cerebras, dell, HPE, supermicro, edgecore, juniper)
    name                        Required for SY type. System name. Generated for SR, SW types if not provided
    management_info.mac         Required for SW type. Mgmt port MAC address
    ipmi_info.name              Required for SR type. IPMI DNS name
    ipmi_credentials.user       Required for SR type. IPMI username
    ipmi_credentials.password   Required for SR type. IPMI password

    Additionally, it may include other device properties
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, description=ImportInventory.description, **kwargs)
        self.confgen = ConfGen(self.profile)

    def construct(self):
        self.parser.add_argument(
            "file_path",
            help="Vendor CSV file path",
            type=str,
        )
        self.parser.add_argument(
            "--noconfirm", "-y",
            help="Do not ask for confirmation before syncing to the DB",
            action="store_true",
            default=False,
        )

    def run(self, args) -> int:
        fp = pathlib.Path(args.file_path)
        if not fp.exists():
            raise ValueError(f"file {fp} does not exist")

        cfg = self.confgen.parse_profile()
        name_prefix = cfg.get("basic", {}).get("hostname_prefix", "")
        devices = process_vendor_file(fp, name_prefix)
        if devices is None:
            return 1

        device_reprs = [DeviceRepr.from_flat_dict(p) for p in devices]

        print(
            "Preparing to create/update the following devices:\n" +
            yaml.dump([d.to_dict(include_none=False) for d in device_reprs], default_flow_style=False),
        )
        counter = collections.Counter([(d.type, d.role) for d in device_reprs])
        print(f"Summary of {len(device_reprs)} devices:")
        for item, count in counter.most_common():
            print(f"  {item[0]}/{item[1]}: {count}")
        if not args.noconfirm and not prompt_confirm("Accept these device updates?"):
            return 1

        sync_devices(self.profile, device_reprs)
        return 0
