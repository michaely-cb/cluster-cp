#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Configuration file builders
"""

from .switch import (
    SwitchConfigWriter,
    AllSwitchesConfigWriter,
    EnableSystemBgpConfigWriter,
    DisableSystemBgpConfigWriter,
    DcqcnConfigWriter
)
from .node import (
    NodeConfigWriter,
    AllNodesConfigWriter,
    NODE_INTERFACE_INDEXES
)
from .system import SystemNetworkConfigWriter, AllSystemsConfigWriter
from .configs import AllConfigsWriter

__all__ = [
    'SwitchConfigWriter',
    'AllSwitchesConfigWriter',
    'EnableSystemBgpConfigWriter',
    'DisableSystemBgpConfigWriter',
    'DcqcnConfigWriter',
    'NodeConfigWriter',
    'AllNodesConfigWriter',
    'SystemNetworkConfigWriter',
    'AllSystemsConfigWriter',
    'AllConfigsWriter',
    'NODE_INTERFACE_INDEXES'
]
