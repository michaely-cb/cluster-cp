#!/usr/bin/env python

# Copyright 2022 Cerebras Systems, Inc. All rights reserved.

"""
Dell templates
"""

from abc import ABCMeta, abstractmethod

from ...base import _TemplateBase

class _SwitchDellBase(_TemplateBase, metaclass=ABCMeta):

    DIRNAME = './switch/dell/data/'

    @property
    @abstractmethod
    def FILENAME(self):
        """
        Return the filename to load
        """

class L3Base(_SwitchDellBase):
    """ Manage the L3 base template
    """
    FILENAME = 'l3_base_template.txt'


class L3SystemBgpBase(_SwitchDellBase):
    """ Manage the L3 system BGP base template
    """
    FILENAME = 'l3_system_bgp_base_template.txt'


class L3BaseLeaf(_SwitchDellBase):
    """ Manage the L3 base template for leaf switches
    """
    FILENAME = 'l3_base_template_leaf.txt'


class L3BaseSpine(_SwitchDellBase):
    """ Manage the L3 base template for spine switches
    """
    FILENAME = 'l3_base_template_spine.txt'


class L3PeerIface(_SwitchDellBase):
    """ Manage the L3 peer interface template
    """
    FILENAME = 'l3_peer_iface_template.txt'


class L3PeerBgp(_SwitchDellBase):
    """ Manage the L3 peer BGP template
    """
    FILENAME = 'l3_peer_bgp_template.txt'


class L3ClearStaticRoute(_SwitchDellBase):
    """ Manage the clear L3 static route template
    """
    FILENAME = 'l3_clear_static_route_template.txt'


class L3StaticRoute(_SwitchDellBase):
    """ Manage the L3 static route template
    """
    FILENAME = 'l3_static_route_template.txt'


class L2Iface(_SwitchDellBase):
    """ Manage the L2 interface template
    """
    FILENAME = 'l2_iface_template.txt'


class L2SystemIface(_SwitchDellBase):
    """ Manage the L2 system interface template
    """
    FILENAME = 'l2_system_iface_template.txt'


class L2SystemVlan(_SwitchDellBase):
    """ Manage the L2 system vlan interface template
    """
    FILENAME = 'l2_system_vlan_template.txt'


class L2SwarmxVlan(_SwitchDellBase):
    """ Manage the L2 swarmx vlan interface template
    """
    FILENAME = 'l2_leaf_swarmx_vlan_template.txt'


class L3UplinkIface(_SwitchDellBase):
    """ Manage the L3 uplink interface template
    """
    FILENAME = 'l3_uplink_iface_template.txt'


class L3MultiMgmtBgp(_SwitchDellBase):
    """ Manage the L3 multi mgmt BGP template
    """
    FILENAME = 'l3_multi_mgmt_bgp_template.txt'

class Ntp(_SwitchDellBase):
    """ Manage the NTP template
    """
    FILENAME = 'ntp_template.txt'

class Snmp(_SwitchDellBase):
    """ Manage the SNMP template
    """
    FILENAME = 'snmp_template.txt'
