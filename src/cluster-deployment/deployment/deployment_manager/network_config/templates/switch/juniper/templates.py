#!/usr/bin/env python

# Copyright 2022 Cerebras Systems, Inc. All rights reserved.

"""
Juniper templates
"""

from abc import ABCMeta, abstractmethod

from ...base import _TemplateBase

class _SwitchJuniperBase(_TemplateBase, metaclass=ABCMeta):

    DIRNAME = './switch/juniper/data/'

    @property
    @abstractmethod
    def FILENAME(self):
        """
        Return the filename to load
        """

class L3Base(_SwitchJuniperBase):
    """ Manage the L3 base template
    """
    FILENAME = 'l3_base_template.txt'


class L3SystemBgpBase(_SwitchJuniperBase):
    """ Manage the L3 system BGP base template
    """
    FILENAME = 'l3_system_bgp_base_template.txt'


class L3BaseLeaf(_SwitchJuniperBase):
    """ Manage the L3 base template for leaf switches
    """
    FILENAME = 'l3_base_template_leaf.txt'


class L3BaseSpine(_SwitchJuniperBase):
    """ Manage the L3 base template for spine switches
    """
    FILENAME = 'l3_base_template_spine.txt'


class L3PeerIface(_SwitchJuniperBase):
    """ Manage the L3 peer interface template
    """
    FILENAME = 'l3_peer_iface_template.txt'


class L3PeerBgp(_SwitchJuniperBase):
    """ Manage the L3 peer BGP template
    """
    FILENAME = 'l3_peer_bgp_template.txt'


class L3ClearStaticRoute(_SwitchJuniperBase):
    """ Manage the clear L3 static route template
    """
    FILENAME = 'l3_clear_static_route_template.txt'


class L3StaticRoute(_SwitchJuniperBase):
    """ Manage the L3 static route template
    """
    FILENAME = 'l3_static_route_template.txt'


class L2Iface(_SwitchJuniperBase):
    """ Manage the L2 interface template
    """
    FILENAME = 'l2_iface_template.txt'


class L2SystemIface(_SwitchJuniperBase):
    """ Manage the L2 system interface template
    """
    FILENAME = 'l2_system_iface_template.txt'


class L2SystemVlan(_SwitchJuniperBase):
    """ Manage the L2 system vlan template
    """
    FILENAME = 'l2_system_vlan_template.txt'


class L2SwarmxVlan(_SwitchJuniperBase):
    """ Manage the L2 swarmx vlan template
    """
    FILENAME = 'l2_leaf_swarmx_vlan_template.txt'


class L3UplinkIface(_SwitchJuniperBase):
    """ Manage the L3 uplink interface template
    """
    FILENAME = 'l3_uplink_iface_template.txt'


class L3MultiMgmtBgp(_SwitchJuniperBase):
    """ Manage the L3 multi mgmt BGP template
    """
    FILENAME = 'l3_multi_mgmt_bgp_template.txt'

class Ntp(_SwitchJuniperBase):
    """ Manage the NTP template
    """
    FILENAME = 'ntp_template.txt'

class Snmp(_SwitchJuniperBase):
    """ Manage the SNMP template
    """
    FILENAME = 'snmp_template.txt'
