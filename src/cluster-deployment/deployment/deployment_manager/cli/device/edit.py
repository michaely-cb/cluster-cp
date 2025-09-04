import argparse
import logging
import tempfile
import textwrap
import yaml
from typing import List, Optional, Tuple

from deployment_manager.cli.device.update import do_update
from deployment_manager.cli.device.utils import DeviceRepr, validate_prop_attr_options
from deployment_manager.common.yamlhandler import CustomYamlLoader, CustomYamlDumper
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.const import (
    DeploymentDeviceRoles,
    DeploymentDeviceTypes
)
from deployment_manager.db.models import Device, MissingPropVals
from deployment_manager.tools.utils import edit_file, prompt_confirm
from deployment_manager.cli.device.config_sanity import ConstraintValidationError

logger = logging.getLogger(__name__)


def _parse_prop_updates(properties: List[str], skip_validation: bool = False) -> Tuple[Optional[props.PropAttrDict], str]:
    prop_updates = {}
    for prop in properties:
        k, v = prop.split("=", 1)
        name, attr = k.split(".", 2)

        if name not in props.ALL_DEVICE_PROP_DICT or attr not in props.ALL_DEVICE_PROP_DICT[name]:
            return None, f"unknown device property '{k}'"

        dprop = props.ALL_DEVICE_PROPS[k]
        if dprop.value_type is not None and v != "" and not skip_validation:
            if dprop.cast_value(v) is None:
                return None, f"invalid property value for '{prop}'. Prop must be format {dprop.value_type}"
        elif v == "":
            v = None

        attrs = prop_updates.get(name, {})
        attrs[attr] = v
        prop_updates[name] = attrs

    return prop_updates, ""


def do_editor_update(devices: List[Device], profile: str, skip_validation: bool = False) -> Tuple[List[Device], List[ConstraintValidationError]]:
    names_before = set([d.name for d in devices])
    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        temp_file.write(yaml.dump(
            [DeviceRepr.from_device(d, MissingPropVals.NULLS).to_dict() for d in devices],
            default_flow_style=False,
            Dumper=CustomYamlDumper
        ).encode())
        temp_file.flush()

        update = []
        errors = []
        allow_reedit = True
        error_count = float('inf')
        while True:
            try:
                edit_file(temp_file.name)
                temp_file.seek(0)
                doc = yaml.load(temp_file, Loader=CustomYamlLoader)

            # prevent attempted name changes
                names_after = set([d.get('name', '') for d in doc])
                names_after.difference_update(names_before)
                if names_after:
                    raise ValueError("changing device names is not permitted")
                
                update, errors = do_update(doc, profile, skip_validation)
                if errors:
                    # If user is working on fixing errors, allow re-editing
                    if len(errors) < error_count:
                        error_count = len(errors)
                        allow_reedit = True

                    error_messages = "\n".join(f"{e.device}: {e.message}" for e in errors)
                    raise ValueError(f"Errors encountered during update: {error_messages}")
                
                return update, errors
            except (ValueError, yaml.YAMLError) as e:
                # Add error as YAML comment
                if not allow_reedit:
                    logger.error(f"{e}")
                    return update, errors 
                
                allow_reedit = False
                
                with open(temp_file.name, 'r') as f:
                    current_content = f.read()
                
                # Clean up previous errors
                lines = current_content.split('\n')
                cleaned_lines = []
                found_non_comment = False
                for line in lines:
                    if not found_non_comment and line.strip().startswith('#'):
                        # Skip comment lines at the top
                        continue
                    else:
                        found_non_comment = True
                        cleaned_lines.append(line)
                cleaned_content = '\n'.join(cleaned_lines)
                
                error_lines = str(e).strip().split('\n')
                error_comment = "# ERROR:\n"
                for line in error_lines:
                    error_comment += f"# {line}\n"
                error_comment += "# Please fix the error and save again.\n\n"
    
                initial_content = error_comment + cleaned_content                            
                with open(temp_file.name, 'w') as f:
                    f.write(initial_content)
                
                continue



class DeviceEdit(SubCommandABC):
    """
    Edit a device
    """
    name = "edit"

    def __init__(self, *args, **kwargs):
        desc = textwrap.dedent("""
            Performs a PATCH style edit - properties not mentioned in the update are ignored.
            Properties in the update set to a null/empty value are set to default. Otherwise, specified properties are 
            updated.

            example:
                # set roce enabled and clear the management IP
                device edit server01 --properties network_config.roce=enabled management_info.ip=    

            Valid Device Properties with defaults:
        """).strip()

        all_props = sorted([f"{k}={v.default_value}" for k,v in props.ALL_DEVICE_PROPS.items()])
        all_props_desc = "  " + "\n  ".join(all_props)
        desc += f"\n{all_props_desc}"

        super().__init__(*args, **kwargs, description=desc)

    def construct(self):
        self.parser.add_argument("NAME", nargs="*", help="Device names")
        self.add_arg_filter(required=False)
        self.add_arg_noconfirm()
        self.parser.add_argument("-t", "--device-type",
            help="The device type to update",
            choices=[t.value for t in DeploymentDeviceTypes]
        )
        self.parser.add_argument("-r", "--device-role",
            help="The device role to update",
            choices=[r.value for r in DeploymentDeviceRoles]
        )
        self.parser.add_argument(
            "--properties", "-p",
            help="Device properties, specified like 'prop_name.prop_attr=prop_value'. "
                 "Empty value clears the property record",
            nargs="+",
            default=argparse.SUPPRESS,
            type=validate_prop_attr_options
        )
        self.parser.add_argument(
            "-o", "--output",
            default="wide",
            choices=["table", "wide", "yaml", "json"],
            help="Output format"
        )
        self.parser.add_argument(
            "--skip-validation",
            action="store_true",
            help=argparse.SUPPRESS 
        )

    def run(self, args) -> int:
        errors = []
        if args.filter and args.NAME:
            logger.error("Must not specify both --filter and NAME")
            return 1
        if not args.filter and not args.NAME:
            logger.error("Must specify either --filter or NAME")
            return 1

        devices = self.filter_devices(args, name=args.NAME, query_set=Device.get_all(self.profile))
        if args.NAME and len(devices) != len(args.NAME):
            names = set(args.NAME)
            names.difference_update(set([d.name for d in devices]))
            logger.error(f"device {', '.join(names)} not found")
            return 1

        if not devices:
            logger.error("no devices selected")
            return 0

        if args.device_type or args.device_role or hasattr(args, "properties"):
            base_update = {"properties": {}}
            if args.device_type:
                base_update["type"] = args.device_type
            if args.device_role:
                base_update["role"] = args.device_role
            if hasattr(args, "properties"):
                prop_updates, err = _parse_prop_updates(args.properties, args.skip_validation)
                if err and not args.skip_validation:
                    logger.error(err)
                    return 1
                base_update["properties"] = prop_updates

            if not args.noconfirm and not args.NAME:
                # when user uses --filter, prompt so they can verify the targets they filtered
                print("Devices selected:\n" + "\n".join([d.name for d in devices]))
                if not prompt_confirm(f"Apply updates to {len(devices)} devices?"):
                    return 0

            update_doc = []
            for d in devices:
                update_doc.append({"name": d.name, "type": d.device_type, "role": d.device_role, **base_update})

            updated, errors = do_update(update_doc, self.profile, args.skip_validation)
        else:
            updated, errors = do_editor_update(devices, self.profile, args.skip_validation)
            
        if errors:
            logger.error("Errors encountered during update:")
            for error in errors:
                logger.error(f"{error.device}: {error.message}")
            return 1

        if not updated:
            if args.output == "yaml":
                pass
            elif args.output == "json":
                print("[]")
            else:
                print("No changes")
            return 0

        
        reprs = [DeviceRepr.from_device(d) for d in updated]
        print(DeviceRepr.format_reprs(reprs, args.output))
        return 0
