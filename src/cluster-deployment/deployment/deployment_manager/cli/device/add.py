import argparse
import datetime
import logging
import os
import textwrap

import yaml
from django.db import transaction
from deployment_manager.cli.device.utils import (
    validate_prop_attr_options,
)
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import (
    DeploymentDeviceRoles,
    DeploymentDeviceTypes
)
from deployment_manager.db.device_props import (
    INVENTORY_DEVICE_PROPS
)
from deployment_manager.db.models import Device
from deployment_manager.tools.utils import edit_file
from deployment_manager.db.device_props import ENFORCE_UNIQUE_PROPS
from deployment_manager.cli.device.config_sanity import (
    check_property_format,
    check_property_duplicate
)

logger = logging.getLogger(__name__)


class DeviceAdd(SubCommandABC):
    """
        Add a new device
    """
    name = "add"

    def __init__(self, *args, **kwargs):
        desc = textwrap.dedent("""
            Add a new device

            Valid device properties with defaults:
        """).strip()
        desc += "\n" + "    " + "\n    ".join([f"{pname}: {d.default_value}" for pname, d in INVENTORY_DEVICE_PROPS.items()])
        super().__init__(*args, **kwargs, description=desc)

    def construct(self):
        self.parser.add_argument("device_name",
            help="The name of the new device"
        )
        self.parser.add_argument("device_type",
            help="The device type",
            choices=[t.value for t in DeploymentDeviceTypes]
        )
        self.parser.add_argument("device_role",
            help="The device role",
            choices=[r.value for r in DeploymentDeviceRoles]
        )
        self.parser.add_argument("-p", "--properties",
            help=textwrap.dedent(f"""
                The device properties.

                Space separated Key=Value pairs.
                Keys are in the form Prop.Attr.
                i.e. vendor.name=HP

                Valid Prop.Attr with defaults:
            """).strip(),
            nargs="*",
            default=argparse.SUPPRESS,
            type=validate_prop_attr_options
        )
        self.parser.add_argument("-i", "--interactive",
            help="Interactively add the properties.attributes values.  This will ignore any --properties arguments.",
            action="store_true"
        )
        self.parser.add_argument("-f", "--force",
            help="If device already exist, delete it first",
            action="store_true",
        )

    def run(self, args):
        # Check if device type and role are valid
        if not DeploymentDeviceTypes(args.device_type).is_valid_role(args.device_role):
            logger.error(f"Device role '{args.device_role}' is invalid for type '{args.device_type}'")
            self.returncode = 1
            return

        dev = Device.get_device(args.device_name, profile_name=self.profile)
        if dev:
            if args.force:
                logger.info(f"Device {args.device_name} exist and will be deleted")
                dev.delete()
            else:
                logger.error(f"Device {args.device_name} already exists")
                self.returncode = 1
                return
        
        with transaction.atomic():
            savepoint = transaction.savepoint()
            new_dev: Device = Device.add(
                name=args.device_name,
                device_role=args.device_role,
                device_type=args.device_type,
                profile=self.profile
            )

            new_prop = {}
            if args.interactive:
                prop_dict = new_dev.get_device_inventory_dict()
                tempfile = f"/tmp/temp_{datetime.datetime.now().timestamp()}.yml"
                with open(tempfile, "w") as fp:
                    yaml.safe_dump(prop_dict, fp)
                edit_file(tempfile)
                with open(tempfile, "r") as f:
                    new_prop = yaml.safe_load(f)
                os.unlink(tempfile)
            elif hasattr(args, "properties"):
                for p in args.properties:
                    k, v = p.split("=", maxsplit=1)
                    prop, attr = k.split(".", maxsplit=1)
                    if prop not in new_prop:
                        new_prop[prop] = {}
                    new_prop[prop][attr] = v

            # Validate property formats
            errors = []
            new_dev.batch_update_properties(new_prop)

            device_queryset = Device.objects.filter(device_id=new_dev.device_id)
            errors.extend(check_property_format(self.profile, device_queryset))
            # Check for duplicate properties
            for key, duplicate_prop_group in ENFORCE_UNIQUE_PROPS.items():
                errors.extend(check_property_duplicate(self.profile, duplicate_prop_group, {new_dev.device_id}))
            
            if errors:
                logger.error("Errors found in the provided properties:")
                for err in errors:
                    logger.error(f"{err.device}: {err.message}")
                transaction.savepoint_rollback(savepoint)
            else:
                logger.info("New device added successfully")
