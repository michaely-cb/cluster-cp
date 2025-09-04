import argparse
import dataclasses
import logging
import typing
from django.db.transaction import atomic
from typing import Dict, List

from deployment_manager.db import device_props as props
from deployment_manager.db.device_props import (
    PropAttrDict,
    ALL_DEVICE_PROPS
)
from deployment_manager.db.models import Device, MissingPropVals
from deployment_manager.tools.utils import ReprFmtMixin

logger = logging.getLogger(__name__)

_TABLE_PROPS = [
    ("rack", props.prop_location_rack),
    ("vendor", props.prop_vendor_name),
    ("ip", props.prop_management_info_ip),
    ("ipmi_ip", props.prop_ipmi_info_ip),
    ("ipmi_name", props.prop_ipmi_info_name),
    ("ipmi_username", props.prop_ipmi_credentials_user),
    ("ipmi_password", props.prop_ipmi_credentials_password),
]

_CSV_PROPS_NAMES = sorted(list(props.ALL_DEVICE_PROPS.keys()))


def remove_none_values(d):
    return {k: remove_none_values(v) if isinstance(v, dict) else v
            for k, v in d.items() if v is not None}


@dataclasses.dataclass
class DeviceUpdateRepr:
    name: str
    type: str
    role: str
    prop_updates: Dict[props.DeviceProp, typing.Any]

    def dict(self):
        return {"name": self.name, "prop_updates": {str(p): str(v) for p, v in self.prop_updates.items()}}


@atomic
def commit_device_updates(profile: str, allocs: typing.List[DeviceUpdateRepr]):
    for d_update in allocs:
        d = Device.get_device(d_update.name, profile)
        d.batch_update_properties(props.expand_prop_dict(d_update.prop_updates))


@dataclasses.dataclass
class DeviceRepr(ReprFmtMixin):
    name: str
    type: str
    role: str
    properties: PropAttrDict

    @classmethod
    def table_header(cls) -> list:
        return ["name", "type", "role"] + [name for name, _ in _TABLE_PROPS]

    @classmethod
    def csv_header(cls) -> list:
        return ["name", "type", "role"] + _CSV_PROPS_NAMES

    @classmethod
    def from_flat_dict(cls, props: dict) -> 'DeviceRepr':
        """
        Convert property format {name.attribute: value} -> {name: {attribute: value}}
        """
        device_props, name, device_type, device_role = {}, props['name'], props['type'], props['role']
        for k, v in props.items():
            if "." in k:
                n, a = k.split(".", 2)
                attrs = device_props.get(n, {})
                attrs[a] = v
                device_props[n] = attrs
        return cls(name, device_type, device_role, device_props)

    @classmethod
    def from_dict(cls, d: dict) -> 'DeviceRepr':
        """
        Convert a dict form of the DeviceRepr to a DeviceRepr
        """
        try:
            name, device_type, device_role, props = d['name'], d['type'], d['role'], d["properties"]
        except KeyError as e:
            raise ValueError(f"malformed device {d.get('name')}, missing required field {e}")

        for k, v in props.items():
            if not isinstance(v, dict):
                raise ValueError(f"malformed device {name}, properties.{k} was not a dict")

        return cls(name, device_type, device_role, props)

    @classmethod
    def from_device(cls, device: Device, prop_include: MissingPropVals = MissingPropVals.EXCLUDE) -> 'DeviceRepr':
        return cls(device.name, device.device_type, device.device_role, device.get_prop_attr_dict(prop_include))

    def to_table_row(self) -> List[str]:
        r = [self.name, self.type, self.role]
        for _, prop in _TABLE_PROPS:
            r.append(self.properties.get(prop.name, {}).get(prop.attr, ""))
        return r

    def to_csv_row(self) -> list:
        r = [self.name, self.type, self.role]
        for prop in _CSV_PROPS_NAMES:
            k, v = prop.split(".", 1)
            r.append(self.properties.get(k, {}).get(v, ""))
        return r

    def to_dict(self, include_none: bool = True) -> dict:
        if include_none:
            return self.__dict__
        return DeviceRepr(self.name, self.type, self.role, remove_none_values(self.properties)).__dict__


def validate_prop_attr_options(value: str):
    """
    Validate the prop_attr_options
    
    Raise argpare.ArgumentTypeError if the Prop.Attr is invalid
    """
    valid_props = ALL_DEVICE_PROPS
    k, _ = value.split("=", maxsplit=1)
    if k not in valid_props:
        raise argparse.ArgumentTypeError(f"invalid Prop.Attr: '{k}'")
    return value


