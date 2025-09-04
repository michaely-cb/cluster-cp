import collections
import csv
import logging
import pathlib
import re
from typing import Dict, List, Optional, Tuple, Union

from deployment_manager.db.const import DeploymentDeviceRoles, DeploymentDeviceTypes
from deployment_manager.db.device_props import INVENTORY_DEVICE_PROPS
from deployment_manager.tools.utils import parse_mac

logger = logging.getLogger(__name__)

vendor_patterns = {
    "HP": r"hp.?|hewlett.*",
    "SM": r"sm.?|super-?micro.*",
    "DL": r"dl|dell.*",
    "AR": r"ar|arista.*",
    "EC": r"ec|edgecore.*",
    "JU": r"ju|juniper.*",
    "CS": r"cs|cerebras.*",
    "CR": r"cr|credo.*",
    "OG": r"og|open[-_]?gear.*",
    "EN": r"en|enlogic",
    "VE": r"ve|vertiv",
    "ST": r"st|server[-_]?tech.*",
    "AP": r"ap|apc",
    "LN": r"ln|lenovo.*",
}

required_properties_all = [
    "type",
    "role",
    "location.rack",
    "vendor.name",
]

# list: require ALL _PropSpec
# tuple: require ANY _PropSpec
# string: required prop
_PropSpec = Union[str, Tuple['_PropSpec'], List['_PropSpec']]
require_properties_type: Dict[str, _PropSpec] = {
    "SW": [("management_info.mac", ["subnet_info.subnet", "management_info.ip"])],
    "SR": [
        ("ipmi_info.name", "ipmi_info.mac",),
        "ipmi_credentials.user",
        "ipmi_credentials.password",
    ],
    "SY": ["name"],
}

LINENUM = "__linenum"


def _normalize_device_type(t: str) -> Tuple[str, str]:
    v = t.lower().strip()
    if v in ["sr", "server"]:
        return "SR", ""
    if v in ["sw", "switch"]:
        return "SW", ""
    if v in ["cn", "console"]:
        return "CN", ""
    if v in ["pu", "psu", "pdu", "powerdistributionunit"]:
        return "PU", ""
    if v in ["pr", "peripheral"]:
        return "PR", ""
    if re.match(r"cs.?|sy(stem)?", v):
        return "SY", ""
    allowed = ", ".join([DeploymentDeviceTypes(v).name for v in DeploymentDeviceTypes.values()])
    return t, f"'{v}' should be one of {allowed}"


def _format_prop_spec(spec: _PropSpec) -> str:
    if isinstance(spec, str):
        return spec
    elif isinstance(spec, tuple):
        return f"at least one of ({' or '.join([_format_prop_spec(s) for s in spec])})"
    elif isinstance(spec, list):
        return f"all of ({' and '.join([_format_prop_spec(s) for s in spec])})"
    else:
        raise AssertionError("invalid propspec")


def _check_prop_spec(d: dict, spec: _PropSpec) -> str:
    if isinstance(spec, str):
        if not d.get(spec, ""):
            return _format_prop_spec(spec)
    elif isinstance(spec, tuple):  # OR
        for s in spec:
            if _check_prop_spec(d, s) == "":
                break
        else:
            return _format_prop_spec(spec)
    elif isinstance(spec, list):  # AND
        for s in spec:
            if _check_prop_spec(d, s) != "":
                return _format_prop_spec(s)
    else:
        raise AssertionError("invalid propspec")
    return ""


def read_vendor_csv(reader: csv.DictReader) -> Tuple[List[dict], List[str]]:
    """
    Parse vendor CSV file. Allows some flexibility in the naming of columns mapping the old 'inventory.csv' to the new
    properties (name.attribute) format if provided in the old way.
    Returns
        Tuple:  List of parsed devices where keys are properties "name.attribute" or just "name" for top level fields.
                List of errors encountered when parsing the vendor file
    """
    inventory_prop_alias = {v.title.lower().replace(" ", "_"): k for k, v in
                            INVENTORY_DEVICE_PROPS.items()}
    prop_mapping = {}

    for k in reader.fieldnames:
        kn = k.strip().lower().replace(" ", "_")
        if kn in inventory_prop_alias:
            prop_mapping[k] = inventory_prop_alias[kn]
        elif 0 <= kn.count(".") < 2:
            prop_mapping[k] = kn
        else:
            logger.warning(f"dropping unknown column '{k}'")

    missing_fields = set(set(required_properties_all)).difference(prop_mapping.values())
    if missing_fields:
        return [], [f"missing required top level fields: {', '.join(missing_fields)}"]

    rv, errors = [], []
    for row in reader:
        if not row:
            continue
        device = {}
        rv.append(device)
        for k, v in row.items():
            v = str(v).strip()
            if prop_mapping[k] is None:
                continue
            if v in ["", "None", "null"]:
                continue
            device[prop_mapping[k]] = v

        if not device.get("type"):
            errors.append(f"line {reader.line_num:04}: missing required field 'type'")
            continue
        
        device["type"], error = _normalize_device_type(device["type"])
        if error:
            errors.append(f"line {reader.line_num:04}: unrecognized device type, {error}")
            continue

        for prop_spec in require_properties_type.get(device["type"], []):
            err = _check_prop_spec(device, prop_spec)
            if err:
                errors.append(f"line {reader.line_num:04}: missing required properties - {err}")
        device[LINENUM] = reader.line_num  # helper for error message generation
    return rv, errors


def _ensure_unit_int_set(devices: List[dict]):
    """ Set location.unit for (rack,type,role) tuples which have not set them to an integer """

    def group_key(d):
        return d['location.rack'], d['type'], d['role']

    check_dr = {group_key(d): [] for d in devices}

    for d in devices:
        check_dr[group_key(d)].append(d)

    for ds in check_dr.values():
        unit_seen = {d.get("location.unit", "") for d in ds}
        unit = 1
        for d in ds:
            if "location.unit" in d and re.match(r"\d+", d.get("location.unit", "")):
                continue
            while str(unit) in unit_seen:
                unit += 1
            d["location.unit"] = str(unit)
            unit_seen.add(str(unit))


def _fmt_str_args(d: dict):
    return {k.replace(".", "_"): int(v) if k == "location.unit" else v for k, v in d.items()}


def _set_server_names(devices: List[dict], pattern: str):
    for d in devices:
        if d.get("type") != "SR":
            continue
        if not bool(d.get("name", "")):
            d["name"] = pattern.format(**_fmt_str_args(d)).lower()
        if "ipmi_info.name" not in d:
            # default the ipmi name to the server name + "-ipmi"
            d["ipmi_info.name"] = d["name"] + "-ipmi"


def _set_mg_switch_names(devices: List[dict], pattern: str):
    for d in devices:
        if d.get("type") != "SW" or d.get("role") != "MG" or bool(d.get("name", "")):
            continue
        d["name"] = pattern.format(**_fmt_str_args(d)).lower()


def _set_data_switch_names(devices: List[dict], pattern: str):
    for d in devices:
        if d.get("type") != "SW" or d.get("role") == "MG" or bool(d.get("name", "")):
            continue
        d["name"] = pattern.format(**_fmt_str_args(d)).lower()


def _set_console_names(devices: List[dict], pattern: str):
    for d in devices:
        if d.get("type") != "CN" or bool(d.get("name", "")):
            continue
        d["name"] = pattern.format(**_fmt_str_args(d)).lower()


def _set_pdu_names(devices: List[dict], pattern: str):
    for d in devices:
        if (d.get("type") !=  "PR" and d.get("role") != "PU") or bool(d.get("name", "")):
            continue
        d["name"] = pattern.format(**_fmt_str_args(d)).lower()


def _set_system_names(devices: List[dict], pattern: str):
    for d in devices:
        if d.get("type") != "SY" or bool(d.get("name", "")):
            continue
        d["name"] = pattern.format(**_fmt_str_args(d)).lower()


def _set_peripheral_names(devices: List[dict], pattern: str):
    for d in devices:
        if d.get("type") != "PR" or bool(d.get("name", "")):
            continue
        d["name"] = pattern.format(**_fmt_str_args(d)).lower()


def _normalize_vendor(devices) -> List[str]:
    errors = []
    for d in devices:
        v: str = d.get("vendor.name", "").lower().strip()
        for vendor, pattern in vendor_patterns.items():
            if re.match(pattern, v):
                d["vendor.name"] = vendor
                break
        else:
            errors.append(f"line {d[LINENUM]:04}: unknown vendor {v}. Options: {vendor_patterns.keys()}")
    return errors


def _normalize_mac(devices: List[dict]) -> List[str]:
    errors = []
    for d in devices:
        for k, v in d.items():
            if k.endswith(".mac"):
                try:
                    d[k] = parse_mac(v)
                except ValueError:
                    errors.append(f"line {d[LINENUM]:04}: device had invalid mac property {k}={v}")
    return errors


def _normalize_names(devices: List[dict]) -> List[str]:
    for d in devices:
        if "name" in d:
            d["name"] = d["name"].lower()
    return []


def _normalize_roles(devices: List[dict]) -> List[str]:
    errors = []
    for d in devices:
        if not d.get("role"):
            errors.append(f"line {d[LINENUM]:04}: device role is required")
            continue
        d["role"] = d["role"].upper()
        try:
            dr = DeploymentDeviceRoles(d["role"])
        except ValueError as e:
            errors.append(f"line {d[LINENUM]:04}: invalid device role '{d['role']}'. {e}")
            continue
        if not dr.is_valid_type(d["type"]):
            errors.append(f"line {d[LINENUM]:04}: invalid device role '{d['role']}' for device type {d['type']}")
    return errors


def process_vendor_file(fp: pathlib.Path, name_prefix: str = "") -> Optional[List[Dict[str, str]]]:
    with open(fp, 'r') as f:
        devices, errors = read_vendor_csv(csv.DictReader(f))

    if errors:
        # TODO: it might make sense to have an interactive edit flow so user can fix errors in-band
        logger.warning(f"invalid vendor file\n  " + "\n  ".join(sorted(errors)))
        return None

    # normalize some field values
    errors = [
        *_normalize_roles(devices),
        *_normalize_names(devices),
        *_normalize_vendor(devices),
        *_normalize_mac(devices),
    ]
    if errors:
        logger.warning(f"invalid vendor file\n  " + "\n  ".join(sorted(errors)))
        return None

    # auto-fill some fields if not set: unit, name
    _ensure_unit_int_set(devices)
    _set_server_names(devices, pattern=name_prefix + "{location_rack}-{role}-sr{location_unit:02}")
    _set_mg_switch_names(devices, pattern=name_prefix + "{location_rack}-{role}-sw{location_unit:02}")
    _set_data_switch_names(devices, pattern=name_prefix + "{location_rack}-{role}-sw{location_unit:02}")
    _set_console_names(devices, pattern=name_prefix + "{location_rack}-cn{location_unit:02}")
    _set_pdu_names(devices, pattern=name_prefix + "{location_rack}-pdu{location_unit:02}")
    _set_system_names(devices, pattern=name_prefix + "{location_rack}-cs-sy{location_unit:02}")
    _set_peripheral_names(devices, pattern=name_prefix + "{location_rack}-peripheral{location_unit:02}")

    # ensure names unique
    names = collections.Counter([d['name'] for d in devices])
    errors = []
    for name, count in names.items():
        if count > 1:
            errors.append(f"device name '{name}' appears more than once ({count})")
    if errors:
        logger.warning(f"invalid vendor file\n  " + "\n  ".join(errors))
        return None

    for d in devices:
        del d[LINENUM]
    return devices
