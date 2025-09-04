#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Network configuration CLI
"""

from .base import \
    add_network_config_arg, \
    UsesNetworkConfig, \
    SubCommandBase, \
    CLIBase

from .misc import \
    NewConfig, \
    AddRootServer, \
    RunPlacer, \
    RemoveMgtAddrs, \
    UpdateOverlayPrefixSize, \
    BuildConfigs, \
    BuildSwitchSystemBgpConfigs, \
    BuildSwitchSystemStaticConfigs, \
    RemoveNode, \
    AddNode, \
    SwapNode, \
    RemoveSystem, \
    AddSystem, \
    SwapSystem, \
    RemoveSwitch, \
    AddSwitch, \
    AddExteriorSwitch, \
    AddExteriorConnections, \
    ExteriorSwitchConfig, \
    AllHosts100G, \
    ImportMgmtHosts, \
    ConnectionList, \
    ImportSwitchLLDP, \
    BatchImportNodeLLDP, \
    BuildSwitchDcqcnConfigs

from .cerebras import \
    IngestMultiboxConfig

from .system import SetSystemConnections

from .allocator import \
    BuildTierAllocations

from .sanity import \
    SanityCheckConfig

from .network_resource_mapper import \
    NetworkResourceMapper

from .task_runner import \
    NodeTasks, \
    SystemTasks, \
    SwitchTasks

COMMAND_CLASSES = tuple(
    [ cls
      for cls in globals().values()
      if getattr(cls, 'sub_command', '') ])

class CLI(CLIBase):
    """ Run network configuration CLI
    """
    command_classes = COMMAND_CLASSES


__all__ = [
    'add_network_config_arg',
    'UsesNetworkConfig',
    'SubCommandBase',
    'CLIBase',

    'NewConfig',
    'AddRootServer'
    'RunPlacer',
    'RemoveMgtAddrs',
    'UpdateOverlayPrefixSize',
    'BuildConfigs',
    'BuildSwitchSystemBgpConfigs',
    'BuildSwitchSystemStaticConfigs',
    'RemoveNode',
    'AddNode',
    'SwapNode',
    'RemoveSystem',
    'AddSystem',
    'SetSystemConnections',
    'SwapSystem',
    'AddSwitch',
    'AddExteriorSwitch',
    'AddExteriorConnections',
    'ExteriorSwitchConfig',
    'AllHosts100G',
    'ImportMgmtHosts',
    'ConnectionList',
    'ImportSwitchLLDP',
    'BatchImportNodeLLDP',
    'BuildSwitchDcqcnConfigs',

    'IngestMultiboxConfig',

    'BuildTierAllocations',

    'SanityCheckConfig',

    'NodeTasks',
    'SystemTasks',
    'SwitchTasks',

    'CLI',
    'NetworkResourceMapper',
]
