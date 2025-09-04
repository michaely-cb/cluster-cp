import json
import logging
import yaml

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Type, TypeVar, Union, get_origin

logger = logging.getLogger(__name__)


# Node/Switch/System info, help scoping the manifest, the attributes can be
# empty, to indicate a "don't care" scoping.
@dataclass
class NodeInfo:
    vendor: str = ""
    model: str = ""
    role: str = ""  # (MG/SX/MX/AX/US)


@dataclass
class SwitchInfo:
    vendor: str = ""  # Long name, like arista
    model: str = ""
    role: Optional[str] = None  # (MG/SX/MX/SPINE/LEAF)


@dataclass
class SystemInfo:
    model: str = ""  # CS2/CS3/CS3+


@dataclass
class NodeExpectedValue:
    value: str = ""
    scope: Optional[NodeInfo] = None


@dataclass
class NodeManifest:
    name: str = ""
    expected: List[NodeExpectedValue] = field(
        default_factory=list
    )  # empty means just collect the value
    cmd: Optional[str] = None  # Command to run on the node to collect the feature


@dataclass
class SwitchExpectedValue:
    value: str = ""
    scope: Optional[NodeInfo] = None
#    scope: SwitchInfo = field(default_factory=SwitchInfo) # TODO: Optional doesn't work


@dataclass
class SwitchManifest:
    name: str = ""
    expected: List[SwitchExpectedValue] = field(
        default_factory=list
    )  # empty means just collect the value


@dataclass
class SystemManifest:
    name: str = ""
    expected: Optional[str] = None  # NULL means just collect the value
    # No scope needed for now


@dataclass
class ClusterManifest:
    release: str = ""
    node_manifest: List[NodeManifest] = field(default_factory=list)
    switch_manifest: List[SwitchManifest] = field(default_factory=list)
    system_manifest: List[SystemManifest] = field(default_factory=list)


def from_dict(data_class, data):
    """Recursively convert a dictionary to a dataclass."""
    # print(f"{data_class}\n{data}")
    if isinstance(data, list):
        return [from_dict(data_class.__args__[0], item) for item in data]
    if isinstance(data, dict):
        if get_origin(data_class) is Union:
            # Assume the Union is due to the Optional attribute in the data class.
            # Extract the non-None type inside the Union
            for t in data_class.__args__:
                if t is not None:
                    data_class = t
                    break
        fieldtypes = {f.name: f.type for f in data_class.__dataclass_fields__.values()}
        return data_class(**{f: from_dict(fieldtypes[f], data[f]) for f in data})
    return data


# Functions to read/write YAML
def write_to_yaml(data: ClusterManifest, file_path: str):
    with open(file_path, "w") as file:
        yaml.dump(asdict(data), file)


def read_from_yaml(file_path: str) -> ClusterManifest:
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
        return from_dict(ClusterManifest, data)


# Functions to read/write json
def write_to_json(data: ClusterManifest, file_path: str):
    with open(file_path, "w") as file:
        json.dump(asdict(data), file, indent=4)


def read_from_json(file_path: str) -> ClusterManifest:
    with open(file_path, "r") as file:
        data = json.load(file)
        return from_dict(ClusterManifest, data)


# Utility Functions
def get_expected(manifest: SwitchManifest, info: SwitchInfo) -> str:
    if len(manifest.expected) == 0:
        return "N/A"

    # Return the first one that the scope covers
    for e in manifest.expected:
        if not hasattr(e, 'scope'):
            return e.value

        s = e.scope
        if s.vendor != "" and s.vendor != info.vendor:
            continue
        if s.model != "" and s.model != info.model:
            continue
        if s.role is not None and s.role != "" and s.role != info.role:
            continue
        # info matches scope
        return e.value

    # scope not matched
    return "Not specified"


def get_expected(manifest: NodeManifest, info: NodeInfo) -> str:
    if len(manifest.expected) == 0:
        return "N/A"

    # Return the first one that the scope covers
    for e in manifest.expected:
        if e.scope is None:
            return e.value

        s = e.scope
        if s.vendor != "" and s.vendor != info.vendor:
            continue
        if s.model != "" and s.model != info.model:
            continue
        if s.role is not None and s.role != "" and s.role != info.role:
            continue
        # info matches scope
        return e.value

    # scope not matched
    return "Not specified"
