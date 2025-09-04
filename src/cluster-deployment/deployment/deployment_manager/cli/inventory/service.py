import deployment_manager.cli.inventory.repr as repr
import deployment_manager.db.models as db
from typing import List, Tuple
from deployment_manager.db.device_props import PropAttrDict
import re
from deployment_manager.cli.device.utils import DeviceRepr
from django.db import transaction
from deployment_manager.db.device_props import ALL_DEVICE_PROP_DICT

def get_profile(profile: str):
    return db.DeploymentProfile.get_profile(profile)


def get_query_set(profile: str, cls):
    query_set = (
        cls.objects
        .filter(profile=profile)
    )
    return query_set

######_____Device Spec_____######

def device_spec_to_entity(device_spec: repr.DeviceSpec, elevation_entity: db.ElevationEntity) -> db.DeviceSpecEntity:
    device_spec_entity = db.DeviceSpecEntity(
        elevation_entity=elevation_entity,
        type=device_spec.type,
        role=device_spec.role,
        count=device_spec.count,
        name_format=device_spec.name_format,
        properties=device_spec.properties,
    )
    return device_spec_entity



######_____Elevation_____######

ELEVATION_PROP_NAMES = ["vendor"]

def elevation_to_entity(elevation: repr.Elevation, profile: str) -> db.ElevationEntity:
    return db.ElevationEntity(name=elevation.name, profile=profile)


def elevation_entity_to_repr(elevation_entity: db.ElevationEntity) -> repr.Elevation:
    device_spec_entities = elevation_entity.device_specs.all()
    device_specs = [
        repr.DeviceSpec(
            type=spec.type,
            role=spec.role,
            count=spec.count,
            name_format=spec.name_format,
            properties=spec.properties
        )
        for spec in device_spec_entities
    ]
    return repr.Elevation(
        name=elevation_entity.name,
        device_specs=device_specs
    )


def elevation_already_exists(name: str, profile: str) -> bool:
    return db.ElevationEntity.objects.filter(name=name, profile=profile).exists()


def add_elevation(elevation: repr.Elevation, profile: str) -> None:
    if elevation_already_exists(elevation.name, profile):
        raise ValueError(f"Elevation with name '{elevation.name}' already exists or is attempting to be committed")

    elevation_entity = elevation_to_entity(elevation, profile)
    elevation_entity.save()

    for device_spec in elevation.device_specs:
        device_spec_entity = device_spec_to_entity(device_spec, elevation_entity)
        device_spec_entity.save()


def remove_elevation(elevation_name: str, profile: str) -> None:
    if not elevation_already_exists(elevation_name, profile):
        raise ValueError(f"Elevation with name '{elevation_name}' does not exist")
    
    elevation_entity = db.ElevationEntity.objects.filter(name=elevation_name, profile=profile).first()
    elevation_entity.delete()


def update_elevation(elevation: repr.Elevation, profile: str) -> None:
    if not elevation_already_exists(elevation.name, profile):
        raise ValueError(f"Elevation with name '{elevation.name}' does not exist")
    
    elevation_entity = db.ElevationEntity.objects.filter(name=elevation.name, profile=profile).first()
    
    for device_spec in elevation_entity.device_specs.all():
        device_spec.delete()

    for device_spec in elevation.device_specs:
        device_spec_entity = device_spec_to_entity(device_spec, elevation_entity)
        device_spec_entity.save()


def validate_elevation(elevation: repr.Elevation) -> List[str]:
        errors = []
        elevation_name = getattr(elevation, "name", "<unknown>")

        for device_spec in elevation.device_specs:
            device_type = getattr(device_spec, "type", "<unknown>")
            device_role = getattr(device_spec, "role", "<unknown>")
            prefix = f"Elevation '{elevation_name}' | Type: {device_type} | Role: {device_role} → "

            if not db.DeploymentDeviceTypes(device_spec.type).is_valid_role(device_spec.role):
                errors.append(prefix + f"Invalid device role '{device_spec.role}' for type '{device_spec.type}'")

            valid_props = ALL_DEVICE_PROP_DICT
            if device_spec.properties:
                for prop_name, attributes in device_spec.properties.items():
                    if prop_name not in ELEVATION_PROP_NAMES:
                        errors.append(prefix + f"Invalid property name '{prop_name}', device_spec properties only allow 'vendor'")
                        continue
                    for attr_name, attr_value in attributes.items():
                        if attr_name not in valid_props[prop_name]:
                            errors.append(prefix + f"Invalid attribute '{prop_name}.{attr_name}'")
        return errors


######_____Rack_____######

def rack_to_entity(rack: repr.Rack, profile) -> db.RackEntity:
    profile=profile
    elevation_entity = db.ElevationEntity.objects.get(name=rack.elevation_name, profile=profile)
    return db.RackEntity(name=rack.name, elevation_entity=elevation_entity, profile=profile, part_number=rack.part_number, properties=rack.properties.model_dump())


def rack_entity_to_repr(rack_entity: db.RackEntity) -> repr.Rack:
    return repr.Rack(
        name=rack_entity.name,
        elevation_name=rack_entity.elevation_entity.name,
        part_number=rack_entity.part_number,
        properties=rack_entity.properties,
    )


def rack_already_exists(name: str, profile: str) -> bool:
    return db.RackEntity.objects.filter(name=name, profile=profile).exists()


def add_rack(rack: repr.Rack, profile):
    if rack_already_exists(rack.name, profile):
        raise ValueError(f"Rack with name '{rack.name}' already exists or is attempting to be committed")

    rack_entity = rack_to_entity(rack, profile)
    rack_entity.save()


def remove_rack(rack_name: str, profile: str):
    if not rack_already_exists(rack_name, profile):
        raise ValueError(f"Rack with name '{rack_name}' does not exist")
    
    rack_entity = db.RackEntity.objects.filter(name=rack_name, profile=profile).first()
    rack_entity.delete()


def update_rack(rack: repr.Rack, profile: str):
    if not rack_already_exists(rack.name, profile):
        raise ValueError(f"Rack with name '{rack.name}' does not exist")

    rack_entity = db.RackEntity.objects.filter(name=rack.name, profile=profile).first()
    elevation_entity = db.ElevationEntity.objects.filter(name=rack.elevation_name, profile=profile).first()
    rack_entity.elevation_entity = elevation_entity
    rack_entity.part_number = rack.part_number
    rack_entity.properties = rack.properties.model_dump()
    rack_entity.save()


def get_racks_for_elevation(elevation_name: str, profile: str):
    elevation_entity = db.ElevationEntity.objects.get(name=elevation_name, profile=profile)
    racks = db.RackEntity.objects.filter(elevation_entity=elevation_entity, profile=profile)
    return racks


def get_devices_in_rack(profile: str, rack_name: str) -> List[DeviceRepr]:
    device_entities = db.Device.objects.filter(profile=profile)
    devices = []
    for device_entity in device_entities:
        if device_entity.get_property("location", "rack") == rack_name:
            device = DeviceRepr(
                name=device_entity.name,
                type=device_entity.device_type,
                role=device_entity.device_role,
                properties=device_entity.get_prop_attr_dict(include_mode=0),
            )
            devices.append(device)
    return devices

def get_sync_devices(rack: repr.Rack, profile: str) -> Tuple[List[DeviceRepr], List[str]]:
    def render_name(name_format: str, spec_properties: dict, index: int) -> str:
        def resolve(path: str):
            if path == "index":
                return f"{index:02}"
            cur = spec_properties
            for part in path.split("."):
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    return "{" + path + "}"
            return str(cur)
        return re.sub(r"\{([^{}]+)\}", lambda m: resolve(m.group(1)), name_format)
    
    elevation_entity = db.ElevationEntity.objects.filter(name=rack.elevation_name, profile=profile).first()
    rack_entity = db.RackEntity.objects.filter(name=rack.name, profile=profile).first()
    
    errors = []
    device_names_set = set()
    devices = []

    for device_spec in elevation_entity.device_specs.all():
        for i in range(1, device_spec.count+1):
            properties_list = [
                properties_dict for properties_dict in [device_spec.properties, rack_entity.properties] 
                if properties_dict is not None
            ]
            
            properties: PropAttrDict = {}
            for properties_dict in properties_list:
                for p1, v in properties_dict.items():
                    assert isinstance(v, dict)
                    if p1 not in properties:
                        properties[p1] = {}
                    for p2, p3 in v.items():
                        properties[p1][p2] = p3
            
            if "location" not in properties: properties["location"] = {}
            properties["location"]["rack"] = rack_entity.name

            device_name = (f"{rack_entity.name}-{device_spec.role}-{device_spec.type}{i:02}").lower()
            if device_spec.name_format:
                device_name = render_name(device_spec.name_format, properties, i)

            if device_name in device_names_set:
                errors.append(f"The derived device name '{device_name}' is duplicated, fix the device specs in the elevation")

            device_names_set.add(device_name)

            sync_device = DeviceRepr(
                device_name,
                device_spec.type,
                device_spec.role,
                properties,
            )

            devices.append(sync_device)
    
    return (devices, errors)


def get_devices_to_sync(rack: repr.Rack, profile) -> Tuple[List[DeviceRepr], List[DeviceRepr], List[str]]:

    def normalize_dict(d: dict) -> dict:
        """Recursively convert all non-None values to str. Drop keys with None values."""
        if not isinstance(d, dict):
            return str(d) if d is not None else None
        return {
            k: normalize_dict(v)
            for k, v in d.items()
            if v is not None
        }
    
    sync_devices = []
    existing_devices = []
    errors = []

    devices, errors = get_sync_devices(rack, profile)
    if errors: return ([], [], errors)

    errors = []
    
    for sync_device in devices:
        existing_device_entity = db.Device.objects.filter(name=sync_device.name, profile=profile).first()
        if existing_device_entity:
            existing_device = DeviceRepr(
                sync_device.name,
                existing_device_entity.device_type,
                existing_device_entity.device_role,
                existing_device_entity.get_prop_attr_dict(include_mode=db.MissingPropVals.EXCLUDE),
            )
            
            if existing_device.type != sync_device.type:
                errors.append(f"Device with the name {sync_device.name} already exists in the db, but has type '{existing_device.type}' instead of '{sync_device.type}' as expected")
            if existing_device.role != sync_device.role:
                errors.append(f"Device with the name {sync_device.name} already exists in the db, but has role '{existing_device.role}' instead of '{sync_device.role}' as expected")

            if not (sync_device.type == existing_device.type 
                    and sync_device.role == existing_device.role 
                    and normalize_dict(sync_device.properties) == normalize_dict(existing_device.properties)):
                existing_devices.append(sync_device)
        else:
            sync_devices.append(sync_device)

    return (sync_devices, existing_devices, errors)


def add_devices(devices: List[DeviceRepr], profile: str) -> List[str]:    
    all_errors = []
    with transaction.atomic():
        savepoint = transaction.savepoint()
        for device in devices:
            existing_device = db.Device.objects.filter(name=device.name, profile=profile).first()
            if existing_device:
                existing_device.batch_update_properties(device.properties)
                continue

            new_device: db.Device = db.Device.add(
                name=device.name,
                device_type=device.type,
                device_role=device.role,
                profile=profile
            )

            new_device.batch_update_properties(device.properties)
            
        if all_errors:
            transaction.savepoint_rollback(savepoint)
            return all_errors
        

def validate_rack(rack: repr.Rack, profile: str) -> List[str]:
        errors: List[str] = []
        rack_name = getattr(rack, "name", "<unknown>")
        prefix = f"Rack '{rack_name}' → "

        if not elevation_already_exists(rack.elevation_name, profile):
            errors.append(
                f"{prefix}Referenced elevation '{rack.elevation_name}' does not exist"
            )
        return errors