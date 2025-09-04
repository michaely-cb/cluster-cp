import csv
import datetime
import django.db.transaction
import logging
import os
import pathlib
import sys
import textwrap
import yaml
from typing import List, Tuple

from deployment_manager.cli.device.utils import DeviceRepr
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import (
    DeploymentDeviceRoles,
    DeploymentDeviceTypes
)

from deployment_manager.db.models import Device, MissingPropVals
from deployment_manager.db.device_props import ENFORCE_UNIQUE_PROPS
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.utils import edit_file, try_parse_mac
from deployment_manager.cli.device.config_sanity import check_property_format, check_property_duplicate, ConstraintValidationError
from django.db import transaction
logger = logging.getLogger(__name__)

@django.db.transaction.atomic
def do_update(doc: List[dict], profile: str, skip_validation: bool = False) -> Tuple[List[Device], List[ConstraintValidationError]]:
    updated = []
    errors = []
    updated_device_ids = set()
    with transaction.atomic():
        savepoint = transaction.savepoint()
        for d in doc:
            is_updated = False
            repr = DeviceRepr.from_dict(d)
            repr.properties = preprocess_properties(repr.properties)
            device_entity = Device.get_device(repr.name, profile)

            # Check if device type and role are valid
            dtype = None
            try:
                dtype = DeploymentDeviceTypes(repr.type)
            except Exception as exc:
                errors.append(
                    ConstraintValidationError(
                        device=repr.name,
                        validation_type="invalid_type",
                        property_key="device_type",
                        property_attr="",
                        property_value=f"{repr.type}",
                        message=str(exc)
                    )
                )


            try:
                DeploymentDeviceRoles(repr.role)
            except Exception as exc:
                errors.append(
                    ConstraintValidationError(
                        device=repr.name,
                        validation_type="invalid_role",
                        property_key="device_role",
                        property_attr="",
                        property_value=f"{repr.role}",
                        message=str(exc)
                    )
                )


            if dtype and not dtype.is_valid_role(repr.role):
                errors.append(
                    ConstraintValidationError(
                        device=repr.name,
                        validation_type="invalid_role",
                        property_key="device_role",
                        property_attr="",
                        property_value=repr.role,
                        message=f"Device role '{repr.role}' is not valid for type '{repr.type}'"
                    )
                )

            if not device_entity:
                device_entity = Device.add(repr.name, repr.type, repr.role, profile)
                device_entity.batch_update_properties(repr.properties)
                device_queryset = Device.objects.filter(device_id=device_entity.device_id)
                errors.extend(check_property_format(profile, device_queryset))
                is_updated = True
                updated_device_ids.add(device_entity.device_id)
            else:
                updated_device_ids.add(device_entity.device_id)
                if repr.type != device_entity.device_type:
                    device_entity.device_type = repr.type
                    is_updated = True
                if repr.role != device_entity.device_role:
                    device_entity.device_role = repr.role
                    is_updated = True
                if is_updated:
                    device_entity.save()
                props = device_entity.get_prop_attr_dict(include_mode=MissingPropVals.EXCLUDE)
                updates = {}
                for name, attrs in repr.properties.items():
                    if name not in props and any([a is not None for a in attrs.values()]):
                        updates[name] = attrs
                    else:
                        existing = props.get(name, {})
                        for attr, v in attrs.items():
                            if attr in existing and existing[attr] == v:
                                continue
                            if attr not in existing and v is None:
                                continue
                            u = updates.get(name, {})
                            u[attr] = v
                            updates[name] = u
                if updates:
                    device_entity.batch_update_properties(updates)
                    if not skip_validation:    
                        device_queryset = Device.objects.filter(device_id=device_entity.device_id)
                        errors.extend(check_property_format(profile, device_queryset))
                    is_updated = True
            if is_updated:
                updated.append(device_entity)
        
        if not skip_validation:     
            for key, duplicate_prop_group in ENFORCE_UNIQUE_PROPS.items():
                errors.extend(check_property_duplicate(profile, duplicate_prop_group, updated_device_ids)) 

        if errors:
            transaction.savepoint_rollback(savepoint)

        return updated, errors

def preprocess_properties(properties: dict) -> dict:
    """
    Preprocess property values before updating device properties.
    Apply the same transformations as in Device.update_property.
    """
    processed = {}
    for prop_name, attrs in properties.items():
        processed_attrs = {}
        for attr_name, value in attrs.items():
            # Apply preprocessing
            if value is not None:
                if attr_name == "mac":
                    if try_parse_mac(value):
                        # Apply consistent formatting
                        value = try_parse_mac(value)
                    elif not value.strip():
                        # Empty strings parsed as NULL
                        value = None
                elif prop_name == "network_config" and attr_name == "roce" and isinstance(value, bool):
                    value = "enabled" if value else "disabled"
            
            processed_attrs[attr_name] = value
        processed[prop_name] = processed_attrs
    return processed


def update_devices_from_file(profile: str, file: str, fmt: str) -> Tuple[List[Device], List[ConstraintValidationError]]:
    """
    Perform an additive update - devices which are in the file but not in the db are added. Devices which exist
    will have their properties updated to the specified value where None values delete the existing property.

    Returns:
        a list of updated or new devices
    """
    if file == "-":
        text = sys.stdin.read()
    else:
        f = pathlib.Path(file)
        if not f.is_file():
            raise ValueError(f"file {file} does not exist")
        text = f.read_text()

    if fmt == "yaml":
        try:
            doc = yaml.safe_load(text)
            if isinstance(doc, dict):
                doc = [doc]
        except Exception as e:
            raise ValueError(f"unable to parse file, must be a valid json or yaml file from device show output: {e}")
    elif fmt == "csv":
        reader = csv.DictReader(text.splitlines())
        doc = [
            DeviceRepr.from_flat_dict(props).to_dict(include_none=False) for props in reader
        ]
    else:
        raise AssertionError(f"unrecognized format: {fmt}")


    return do_update(doc, profile)


class DeviceUpdate(SubCommandABC):
    """
        Update the devices records, or from a file (same format as device show -o json/yaml)
    """
    name = "update"

    def __init__(self, *args, **kwargs):
        desc = textwrap.dedent("""
                Performs a PATCH style update - properties not mentioned in the update are ignored.
                Properties in the update set to a null/empty value are deleted. Otherwise, specified properties are updated.
                If device does not exists, it will be added

                example:
                    #### Update devices from command line with a file
                    # Generate a file and edit
                    cscfg device show -o yaml >devices.yaml
                    vim devices.yaml # do edits
                    # do the update via input redirection
                    cscfg device update - <devices.yaml
                    
                    # alternatively, can give the name of the file in argument
                    cscfg device update devices.yaml
        """).strip()

        super().__init__(*args, **kwargs, description=desc)

    def construct(self):
        self.parser.add_argument(
            "FILE_NAME",
            nargs="?",
            help=textwrap.dedent("""
                The input filename
                If filename is '-' the file content is taken from stdin
                If filename is not given, interactive update of the full inventory
            """).strip()
        )
        self.parser.add_argument(
            "-i", "--input-format",
            default="-",
            choices=["yaml", "csv"],
            help=str("The input format for the interactive update. Tries to infer the format from file name if not "
                     "given but defaults to yaml")
        )
        self.parser.add_argument(
            "-o", "--output",
            default="wide",
            choices=["table", "wide", "yaml", "json"],
            help="Output format"
        )

    def run(self, args) -> int:
        cg = ConfGen(self.profile)
        errors = []
        try:
            if args.FILE_NAME:
                fmt, fn = args.input_format, args.FILE_NAME
                if not fmt or fmt == "-":
                    fmt = fn.split(".")[-1] if fn.endswith(".csv") or fn.endswith(".yaml") else "yaml"
                if not fmt:
                    logger.warning("defaulted to '--input-format yaml'")
                    fmt = "yaml"
                updated, errors = update_devices_from_file(self.profile, fn, fmt)
            else:
                if args.input_format == "yaml":
                    devices_repr = [
                        DeviceRepr(d.name, d.device_type, d.device_role, d.get_prop_attr_dict(include_mode=MissingPropVals.EXCLUDE))
                        for d in Device.get_all(profile_name=self.profile)
                    ]
                    devices_repr = sorted(devices_repr, key=lambda d: (d.name, d.type, d.role))
                    dump_dict = [d.__dict__ for d in devices_repr]
                    tempfile = f"/tmp/temp_update_device_{datetime.datetime.now().timestamp()}.yml"
                    with open(tempfile, "w") as fp:
                        yaml.safe_dump(dump_dict, fp)
                    edit_file(tempfile)
                    updated, errors = update_devices_from_file(self.profile, tempfile)
                    os.unlink(tempfile)
                else:
                    # create inventory from db
                    cg.generate_inventory_csv()
                    cg.edit_inventory()
                    inventory = cg.inventory_file_to_list()
                    doc = []
                    for inv in inventory:
                        doc.append({
                            "name": inv["name"],
                            "type": inv["device_type"],
                            "role": inv["device_role"],
                            "properties": {k: v for k, v in inv.items() if k not in ['name', 'device_type', 'device_role']}
                        })
                    updated = do_update(doc, self.profile)
        except ValueError as e:
            logger.error(f"{e}")
            return 1

        if errors:
            for error in errors:
                logger.error(f"{error.device}: {error.message}")
            return 1

        if not updated:
            if args.output == "yaml" or args.output == "json":
                print("[]")
            else:
                print("No changes")
            return 0

        reprs = [
            DeviceRepr(d.name, d.device_type, d.device_role, d.get_prop_attr_dict(include_mode=MissingPropVals.EXCLUDE))
            for d in updated
        ]

        print(DeviceRepr.format_reprs(reprs, args.output))
        return 0
