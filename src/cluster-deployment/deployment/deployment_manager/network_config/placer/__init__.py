#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""Configure network resources throughout the cluster

This placer requires that the delegated prefix has been carved up
appropriately for each tier and use case.

Placement is cumulative. If resources are added to an existing list,
then only the new resources will have addresses and existing ones will
not be changed.
"""

from .node import (
    InterfacePlacer,
    SystemVLANInterfacePlacer,
    SwarmxVLANInterfacePlacer
)
from .switch import \
    SwitchVirtualAddrPlacer, \
    SwitchASNPlacer, \
    SwitchVLANPlacer

__all__ = [
    'SwitchVirtualAddrPlacer',
    'SwitchASNPlacer',
    'SwitchVLANPlacer',
    'InterfacePlacer',
    'SystemVLANInterfacePlacer',
    'SwarmxVLANInterfacePlacer',
]
