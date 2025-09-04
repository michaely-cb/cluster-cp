""" Modules needed by L3 push-button functionality
"""
from .cluster import ClusterSyncTask, ClusterAddTask
from .config import NetworkConfig
from .config_builder import config_builders, discovery_classes
from .config_uploader import (
    SwitchEnableRoce,
    SwitchGetRoceStatus,
    switch_config_uploaders,
)
from .environment import NetworkEnvironment
from .nodes import Nodes
from .precheck import (
    NodeInterfaceStatusCheck,
    SwitchVersionAndModelCheck
)
from .root_server import RootServer
from .switches import (
    ExteriorConnections, ExteriorSwitches,
    AWSwitches,
    BRSwitches,
    LeafSwitches,
    SpineSwitches
)
from .systems import Systems, SystemConnections
from .tiers import NetworkTier

init_classes = [
    NetworkConfig,
    NetworkEnvironment,
    NetworkTier,
    RootServer,
    ClusterAddTask,
]

# transform master config to network config
transformation_classes = [
    ExteriorSwitches,
    AWSwitches,
    BRSwitches,
    LeafSwitches,
    SpineSwitches,
    Systems,
    SystemConnections,
    Nodes,
    ExteriorConnections,
    ClusterSyncTask,
]
generation_classes = transformation_classes + discovery_classes + config_builders

switch_deployment_classes = switch_config_uploaders

