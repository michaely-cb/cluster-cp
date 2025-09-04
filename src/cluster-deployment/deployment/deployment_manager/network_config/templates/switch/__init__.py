#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Switch templates
"""

import inspect

from ..base import _TemplateBase

from . import arista, dell, edgecore, hpe, juniper

def _get_switch_module(switch_type):
    """ Return the module containing a switch of a specific type
    """
    mod_map = {
        'arista': arista,
        'hpe': hpe,
        'dell': dell,
        'edgecore': edgecore,
        'juniper': juniper
    }
    mod = mod_map.get(str(switch_type).lower())
    if mod is not None:
        return mod
    else:
        raise AttributeError(f'Unknown/invalid switch type: {switch_type}')

class _SwitchClassOverride(_TemplateBase):
    DIRNAME = ""
    FILENAME = ""

    def __new__(cls, switch_type, *args, **kwargs):
        module = _get_switch_module(switch_type)
        for cls_name, cls_ent in inspect.getmembers(module):
            if cls_name == cls.__name__:
                new_obj = cls_ent.__new__(cls_ent, *args, **kwargs)
                new_obj.__init__(*args, **kwargs)
                return new_obj

        raise AttributeError(f'Class missing: {cls.__name__}')

    def substitute(self, mapping=None, **kwargs):
        """ Run template substitutions
        """


class L3Base(_SwitchClassOverride):
    """ Return the L3 base template for a switch
    """


class L3SystemBgpBase(_SwitchClassOverride):
    """ Return the L3 system BGP base template for a switch
    """


class L3BaseLeaf(_SwitchClassOverride):
    """ Return the L3 base template for a leaf switch
    """


class L3BaseSpine(_SwitchClassOverride):
    """ Return the L3 base template for a spine switch
    """


class L3PeerIface(_SwitchClassOverride):
    """ Return the L3 peer template for a switch
    """

class L3PeerBgp(_SwitchClassOverride):
    """ Return the L3 peer template for a switch
    """

class L3ClearStaticRoute(_SwitchClassOverride):
    """ Return the clear L3 static route template for a switch
    """


class L3StaticRoute(_SwitchClassOverride):
    """ Return the L3 static route template for a switch
    """


class L2Iface(_SwitchClassOverride):
    """ Return the L2 interface template for a switch
    """


class L2SystemIface(_SwitchClassOverride):
    """ Return the L2 system interface template for a switch
    """


class L2SystemVlan(_SwitchClassOverride):
    """ Return the L2 system vlan template for a switch
    """


class L2SwarmxVlan(_SwitchClassOverride):
    """ Return the L2 swarmx vlan template for a switch
    """


class L3UplinkIface(_SwitchClassOverride):
    """ Return the L3 peer template for a switch
    """


class L3MultiMgmtBgp(_SwitchClassOverride):
    """ Return the L3 multi mgmt bgp template for a switch
    """

class Ntp(_SwitchClassOverride):
    """ Return the Ntp template for a switch
    """

class Snmp(_SwitchClassOverride):
    """ Return the Snmp template for a switch
    """


__all__ = [
    'L3Base',
    'L3SystemBgpBase',
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
    'L3MultiMgmtBgp',
    'Ntp',
    'Snmp'
]
