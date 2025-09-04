"""
Commands to populate interface and address information
"""
import logging

from deployment_manager.network_config.cli import (
    BuildConfigs,
    RunPlacer,
    SanityCheckConfig,
    SwitchTasks,
    UpdateOverlayPrefixSize
)
from .base import Deploy, get_obj_name
from .command import ToolCommand
from .schema_migration import MigrateNetworkSchemaTask
from ..tasks.node import NodeInterfaceSpeedNetworkTask, NodeLLDPNetworkTask
from ..tasks.misc import OverlayPrefixSizeGeneratorTask, ConfigGeneratorTask, NetworkPlacerTask
from ..tasks.switch import SwitchModelTask, SwitchLLDPTask
from ..tasks.system import SystemModelTask, SystemPortDiscoverTask, SystemIfaceBasedFromTierTask, SystemVersionTask


logger = logging.getLogger(__name__)



class NetworkPlacer(Deploy):
    """ Assign addresses based on subnet information
    """
    name = "Provision addresses based on subnet information"

    def get_command(self):
        return ToolCommand(
            RunPlacer,
            dict(
                no_switches=False,
                no_vips=False,
                no_xconnect=False,
                no_interfaces=False,
                config=self.cfg.network_config_filename
            )
        )




class SanityChecker(Deploy):
    """ Check if generated network_config conforms with expectations
    """
    name = "Run sanity check on network_config JSON"

    def get_command(self):
        return ToolCommand(
            SanityCheckConfig,
            dict(verbose=False, config=self.cfg.network_config_filename)
        )




discovery_classes = [
    SystemModelTask,
    SystemVersionTask,
    SystemPortDiscoverTask,

    NodeLLDPNetworkTask,
    NodeInterfaceSpeedNetworkTask,

    SwitchModelTask,
    SwitchLLDPTask,

    SystemIfaceBasedFromTierTask,
]


config_builders = [
    MigrateNetworkSchemaTask,
    NetworkPlacerTask,
    OverlayPrefixSizeGeneratorTask,
    ConfigGeneratorTask
]
