#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Node templates
"""

from abc import ABCMeta, abstractmethod

from ..base import _TemplateBase

class _NodeBase(_TemplateBase, metaclass=ABCMeta):
    """ Base class for node templates
    """

    DIRNAME = './node/data/'

    @property
    @abstractmethod
    def FILENAME(self):
        """
        Return the filename to load
        """

class ArpFluxConf(_NodeBase):
    """ Manage template for arp flux configuration
    """
    FILENAME = '01-arp-flux.conf'


class ConntrackLiberalConf(_NodeBase):
    """ Manage template for conntrack liberal setting
    """
    FILENAME = '02-liberal-conntrack.conf'


class BasicRoutes(_NodeBase):
    """ Manage template for basic routes configuration
    """
    FILENAME = 'basic_routes_template.txt'


class Dispatch(_NodeBase):
    """ Manage template for NetworkManager dispatch script
    """
    FILENAME = 'dispatch_template.txt'


class ExteriorRoute(_NodeBase):
    """ Manage template for exterior route configurtation
    """
    FILENAME = 'exterior_route_template.txt'


class ExteriorNonPolicyRoute(_NodeBase):
    """ Manage template for exterior route configurtation without policy routes
    """
    FILENAME = 'exterior_nonpolicy_route_template.txt'


class IfCfg(_NodeBase):
    """ Manage template for interface configuration
    """
    FILENAME = 'ifcfg_template.txt'


class InteriorRoute(_NodeBase):
    """ Manage template for interior cluster route
    """
    FILENAME = 'interior_route_template.txt'


class Rule(_NodeBase):
    """ Manage template for policy route configuration
    """
    FILENAME = 'rule_template.txt'


class HostsHeader(_NodeBase):
    """ Manage template for the header of the hosts file
    """
    FILENAME = 'hosts_header.txt'


class HostsEntry(_NodeBase):
    """ Manage template for an entry in the hosts file
    """
    FILENAME = 'hosts_entry.txt'


class Hostname(_NodeBase):
    """ Manage template for the hostname file
    """
    FILENAME = 'hostname.txt'
