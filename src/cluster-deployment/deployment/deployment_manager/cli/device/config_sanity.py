import logging
import os
import tarfile
import sqlite3
import tempfile
import json
import functools
import time
import re
from typing import List, Set
from datetime import datetime
from dataclasses import dataclass
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.device.utils import DeviceRepr, ReprFmtMixin
from deployment_manager.db.models import Device, MissingPropVals, DeviceProperty
from deployment_manager.db.const import DeploymentDeviceRoles, DeploymentDeviceTypes
from deployment_manager.db.device_props import ALL_DEVICE_PROP_DICT, ALL_DEVICE_PROPS, DeviceProp, ENFORCE_UNIQUE_PROPS, get_props_for_device_type
from django.db.models import Count, Q, QuerySet

logger = logging.getLogger(__name__)

@dataclass
class ConstraintValidationError(ReprFmtMixin):
    device: str
    validation_type: str
    property_key: str
    property_attr: str
    property_value: str
    message: str

    @classmethod
    def table_header(cls) -> list:
        return ["device", "validation_type", "property", "value", "message"]

    def to_table_row(self) -> list:
        prop_full = f"{self.property_key}.{self.property_attr}" if self.property_attr else self.property_key
        return [self.device, self.validation_type, prop_full, self.property_value, self.message]

    @property
    def sort_key(self):
        return (self.validation_type, self.device, self.property_key, self.property_attr)


class ConfigSanity(SubCommandABC):
    """
    Check current inventory and configuration for format and consistency.
    """
    name = "config_sanity"
    
    def construct(self):
        self.add_arg_filter(required=False)
        self.add_arg_output_format()
    
    def run(self, args):
        devices = Device.get_all(self.profile)
        targets = self.filter_devices(
            args, device_type=None, query_set=devices
        )
        target_device_ids = set(targets.values_list('device_id', flat=True))
        errors = []
        errors.extend(check_property_format(self.profile, targets))
        for key, duplicate_prop_group in ENFORCE_UNIQUE_PROPS.items():
            errors.extend(check_property_duplicate(self.profile, duplicate_prop_group, target_device_ids))

        errors.extend(check_deployment_required_fields(self.profile, targets))
        errors.extend(deployment_validation(self.profile, targets))
        if errors:
            print(ConstraintValidationError.format_reprs(errors, args.output))
            return 1

        if args.output == "table":
            print("All properties have correct format and no duplicates found.")
        else:
            print(ConstraintValidationError.format_reprs(errors, args.output))
        
        return 0

def check_property_format(profile: str, devices: QuerySet[Device]) -> List[ConstraintValidationError]:
    """
    Check if all properties of devices have correct format.
    """
    errors = []
    # Process devices in batches to avoid memory issues
    batch_size = 999
    total_devices = devices.count()
    
    for offset in range(0, total_devices, batch_size):
        batch_devices = devices[offset:offset + batch_size].prefetch_related('properties')
        for device in batch_devices:
            for prop_model in device.properties.all():
                pattr = get_props_for_device_type(device.device_type)
                device_prop = pattr.get(f"{prop_model.property_name}.{prop_model.property_attribute}", None)
                if device_prop:
                    ok, err = device_prop.validate_value(prop_model.property_value)
                    if not ok:
                        error = ConstraintValidationError(
                            device=device.name,
                            validation_type="invalid_format",
                            property_key=prop_model.property_name,
                            property_attr=prop_model.property_attribute,
                            property_value=prop_model.property_value,
                            message=err
                        )
                        errors.append(error)

    return errors

def is_duplicate_exception(duplicates: list[DeviceProperty]) -> bool: 
    """
    Check if the property is allowed to have duplicates within the same device.
    """
    def properties_match_pair(prop_one: DeviceProperty, prop_two: DeviceProperty, 
                             expected_prop_one: tuple, expected_prop_two: tuple) -> bool:
        """
        Check if two properties match an expected pair in either order.
        """
        prop_one_tuple = (prop_one.property_name, prop_one.property_attribute)
        prop_two_tuple = (prop_two.property_name, prop_two.property_attribute)
        
        return ((prop_one_tuple == expected_prop_one and prop_two_tuple == expected_prop_two) or
                (prop_one_tuple == expected_prop_two and prop_two_tuple == expected_prop_one))

    device_names = [instance.device.name for instance in duplicates]
    if len(set(device_names)) == 1 and len(duplicates) == 2:
        instance_one = duplicates[0]
        instance_two = duplicates[1]
        if instance_one.device.device_type == DeploymentDeviceTypes.SWITCH.value and instance_one.device.device_role == DeploymentDeviceRoles.MANAGEMENT.value and properties_match_pair(instance_one, instance_two, ('management_info', 'ip'), ('subnet_info', 'gateway')):
            # Management IP and Subnet_info.gateway can be duplicated for SW MG devices
            return True
    
    return False

def check_property_duplicate(profile: str, device_props: List[DeviceProp], device_ids: Set[int]=None) -> List[ConstraintValidationError]:
    """
    Check for duplicate values across all properties of the same type (e.g., all IP properties).
    """    
    errors = []
    
    def _include(did: int) -> bool:
        return device_ids is None or did in device_ids


    q_objects = Q()
    for prop in device_props:
        q_objects |= Q(property_name=prop.name, property_attribute=prop.attr)
    
    # Get all property values for all properties of this type
    all_duplicates = (DeviceProperty.objects.filter(
        q_objects,
        device__profile__name=profile
    )
    .values('property_value')
    .annotate(count=Count('property_value'))
    .filter(count__gt=1))
    
    # Group by property value to find duplicates across different properties
    for dup in all_duplicates:
        duplicate_instances = DeviceProperty.objects.filter(
            q_objects,
            property_value=dup['property_value'],
            device__profile__name=profile
        ).select_related('device')
        
        instances_list = list(duplicate_instances)
        involved_target_devices = [inst for inst in instances_list if _include(inst.device_id)]
        
        if involved_target_devices:
            device_list = []
            for instance in involved_target_devices:
                device_list.append(f'{instance.property_name}.{instance.property_attribute} in device {instance.device.name}')
            
            error_message = f"Duplicate value '{dup['property_value']}' found for property: "
            error_message += ', '.join(device_list)

            for instance in involved_target_devices:
                if instance.property_value and not is_duplicate_exception(duplicate_instances):
                    errors.append(
                        ConstraintValidationError(
                            device=instance.device.name,
                            validation_type="duplicate",
                            property_key=instance.property_name,
                            property_attr=instance.property_attribute,
                            property_value=str(dup['property_value']),
                            message=error_message
                        )
                    )
    
    return errors

def check_deployment_required_fields(profile: str, devices: QuerySet[Device]) -> List[ConstraintValidationError]:
    """
    Check if devices have all required fields based on deployment rules.
    
    Required fields:
    - vendor.name: required for deployment
    - mgmt_info {mac, ip, username, password}: required for deployment  
    - ipmi_info {mac, ip, username, password}: required for SR deployment
    - mgmt_info.name: required for SY deployment in case of alias-named systems
    """
    errors = []
    batch_size = 999
    total_devices = devices.count()

    for offset in range(0, total_devices, batch_size):
        devices_with_props = devices[offset:offset + batch_size].prefetch_related('properties')
        for device in devices_with_props:
            # Get all properties for this device
            props = {}
            for prop_model in device.properties.all():
                prop_key = f"{prop_model.property_name}.{prop_model.property_attribute}"
                props[prop_key] = prop_model.property_value
            
            # Check vendor.name (required for all deployments)
            if not props.get('vendor.name'):
                errors.append(
                    ConstraintValidationError(
                        device=device.name,
                        validation_type="deployment_field",
                        property_key="vendor",
                        property_attr="name",
                        property_value="",
                        message="vendor.name is required for deployment"
                    )
                )
            
            # Check mgmt_info fields (required for all deployments)
            mgmt_fields = ['mac', 'ip']
            mgmt_credentials = ['user', 'password']
            for field in mgmt_fields:
                if not props.get(f'management_info.{field}'):
                    errors.append(
                        ConstraintValidationError(
                            device=device.name,
                            validation_type="deployment_field",
                            property_key="management_info",
                            property_attr=field,
                            property_value="",
                            message=f"management_info.{field} is required for deployment"
                        )
                    )

            for field in mgmt_credentials:
                if not props.get(f'management_credentials.{field}'):
                    errors.append(
                        ConstraintValidationError(
                            device=device.name,
                            validation_type="deployment_field",
                            property_key="management_credentials",
                            property_attr=field,
                            property_value="",
                            message=f"management_credentials.{field} is required for deployment"
                        )
                    )
                
            
            # Check ipmi_info fields (required for SR deployment)
            if device.device_type == 'SR':  # Assuming SR is the role value
                ipmi_fields = ['mac', 'ip']
                ipmi_credentials = ['user', 'password']
                for field in ipmi_fields:
                    if not props.get(f'ipmi_info.{field}'):
                        errors.append(
                            ConstraintValidationError(
                                device=device.name,
                                validation_type="deployment_field",
                                property_key="ipmi_info",
                                property_attr=field,
                                property_value="",
                                message=f"ipmi_info.{field} is required for SR deployment"
                            )
                        )
                
                for field in ipmi_credentials:
                    if not props.get(f'ipmi_credentials.{field}'):
                        errors.append(
                            ConstraintValidationError(
                                device=device.name,
                                validation_type="deployment_field",
                                property_key="ipmi_credentials",
                                property_attr=field,
                                property_value="",
                                message=f"ipmi_credentials.{field} is required for SR deployment"
                            )
                        )
            
            # Check mgmt_info.name (required for SY deployment)
            if device.device_type == 'SY' and re.match(r'^.*wse[0-9]{3}-cs-sy[0-9]{2}$', device.name):
                if not props.get('management_info.name'):
                    errors.append(
                        ConstraintValidationError(
                            device=device.name,
                            validation_type="deployment_field",
                            property_key="management_info",
                            property_attr="name",
                            property_value="",
                            message="management_info.name is required for SY deployment"
                        )
                    )
        
    return errors


def deployment_validation(profile: str, devices: QuerySet[Device]) -> List[ConstraintValidationError]:
    """
    Validate required fields for a single device.
    """
    errors = []
    
    # Get device IDs and names for comparison
    device_map = {device.device_id: device.name for device in devices}
    
    # Fetch only management_info.name properties for devices in our profile
    mgmt_name_props = DeviceProperty.objects.filter(
        property_name='management_info',
        property_attribute='name',
        device__profile__name=profile
    ).select_related('device')
    
    
    # Filter in memory to only include devices from our queryset
    for prop in mgmt_name_props:
        if prop.device_id in device_map:
            device_name = device_map[prop.device_id]
            # Validation rule: mgmt_info.name should not be the same as device name
            if prop.property_value == device_name:
                errors.append(
                    ConstraintValidationError(
                        device=device_name,
                        validation_type="deployment_field",
                        property_key="management_info",
                        property_attr="name",
                        property_value=prop.property_value,
                        message="management_info.name should not be the same as the device alias name"
                    )
                )
    
    return errors