import json
import yaml

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import List, Optional

from deployment_manager.platform.manifests.cluster_manifest import (
    NodeInfo,
    SwitchInfo,
    SystemInfo,
    from_dict,
)

# Dataclasses for showing cluster status


@dataclass
class ClusterNodeSummary:
    num_nodes: int = 0
    num_discrepant_nodes: int = 0


@dataclass
class ClusterSwitchSummary:
    num_switches: int = 0
    num_discrepant_switches: int = 0


@dataclass
class ClusterSystemSummary:
    num_systems: int = 0
    num_discrepant_systems: int = 0


@dataclass
class ClusterSummary:
    nodes: ClusterNodeSummary = field(default_factory=ClusterNodeSummary)
    switches: ClusterSwitchSummary = field(default_factory=ClusterSwitchSummary)
    systems: ClusterSystemSummary = field(default_factory=ClusterSystemSummary)


@dataclass
class Feature:
    """A feature to report"""

    name: str = ""
    value: str = (
        ""  # TODO: could be a list of values, e.g. system status can be "OK" or "DEGRADED"
    )
    expected: Optional[str] = None  # NULL means just to show the value


@dataclass
class NicInfo:
    vendor: str = ""
    model: str = ""


@dataclass
class NicFeatures:
    name: str = ""
    info: NicInfo = field(default_factory=NicInfo)
    features: List[Feature] = field(default_factory=list)


@dataclass
class ClusterNodeFeatures:
    name: str = ""
    info: NodeInfo = field(default_factory=NodeInfo)
    features: List[Feature] = field(default_factory=list)
    nics: List[NicFeatures] = field(default_factory=list)


@dataclass
class ClusterSwitchFeatures:
    name: str = ""
    info: SwitchInfo = field(default_factory=SwitchInfo)
    features: List[Feature] = field(default_factory=list)


@dataclass
class ClusterSystemFeatures:
    name: str = ""
    info: SystemInfo = field(default_factory=SystemInfo)
    features: List[Feature] = field(default_factory=list)


@dataclass
class ClusterFeatures:
    nodes: List[ClusterNodeFeatures] = field(default_factory=list)
    switches: List[ClusterSwitchFeatures] = field(default_factory=list)
    systems: List[ClusterSystemFeatures] = field(default_factory=list)


@dataclass
class ClusterStatus:
    summary: ClusterSummary = field(default_factory=ClusterSummary)
    discrepancies: ClusterFeatures = field(default_factory=ClusterFeatures)
    details: ClusterFeatures = field(default_factory=ClusterFeatures)


# Functions to read/write YAML
def write_to_yaml(data: ClusterStatus, file_path: str):
    with open(file_path, "w") as file:
        yaml.dump(asdict(data), file)


def read_from_yaml(file_path: str) -> ClusterStatus:
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
        return from_dict(ClusterStatus, data)


# Functions to read/write json
def write_to_json(data: ClusterStatus, file_path: str):
    with open(file_path, "w") as file:
        json.dump(asdict(data), file)


def read_from_json(file_path: str) -> ClusterStatus:
    with open(file_path, "r") as file:
        data = json.load(file)
        return from_dict(ClusterStatus, data)
