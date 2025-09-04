#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Juniper switch templates
"""

from .templates import (
    L3Base,
    L3BaseLeaf,
    L3BaseSpine,
    L3PeerIface,
    L3PeerBgp,
    L3ClearStaticRoute,
    L3StaticRoute,
    L2Iface,
    L2SystemIface,
    L2SystemVlan,
    L2SwarmxVlan,
    L3UplinkIface,
    L3MultiMgmtBgp,
    L3SystemBgpBase,
    Ntp,
    Snmp
)

__all__ = [
    'L3Base',
    'L3BaseLeaf',
    'L3BaseSpine',
    'L3PeerIface',
    'L3PeerBgp',
    'L3ClearStaticRoute',
    'L3StaticRoute',
    'L2Iface',
    'L2SystemIface',
    'L2SystemVlan',
    'L2SwarmxVlan',
    'L3UplinkIface',
    'L3MultiMgmtBgp',
    'Ntp',
    'Snmp'
]
