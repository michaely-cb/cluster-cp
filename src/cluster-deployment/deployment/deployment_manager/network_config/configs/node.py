#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Configuration builders for nodes
"""

from abc import ABCMeta, abstractmethod

from .base import (
    _ConfigFileWriterBase,
    _ConfigAggregateBase,
    MTU_DEFAULT,
    MTU_MIN,
    MTU_MAX_WITH_OVERHEAD
)
from ..schema import \
    JSIPInterface, \
    JSIPNetwork, \
    NetworkConfigSchema
from ..templates.node import \
    ArpFluxConf, \
    ConntrackLiberalConf, \
    BasicRoutes, \
    Dispatch, \
    ExteriorRoute, \
    ExteriorNonPolicyRoute, \
    IfCfg, \
    InteriorRoute, \
    Rule, \
    HostsHeader, \
    HostsEntry, \
    Hostname
from ..utils import items_name_iter

# Duplicated from idxtables in ansible/roles/weightstream/defaults/main.yml
NODE_INTERFACE_INDEXES = (
    ("enp1s0f0", 501),
    ("enp1s0f1", 502),
    ("enp67s0", 503),
    ("enp67s1", 504),
    ("enp197s0f1", 505),
    ("enp197s0f0", 506),
#    ("enp129s0f1", 507),    # Duplicate
#    ("enp129s0f0", 508),    # Duplicate
    ("enp129s0", 509),
    ("enp197s0", 510),
    ("enp193s0f0", 511),
    ("enp193s0f1", 512),
    ("enp129s0f0", 513),
    ("enp129s0f1", 514),
    ("enp161s0f0", 515),
    ("enp161s0f1", 516),
    ("ens3f0", 517),
    ("ens3f1", 518),
    ("ens3", 519),
    ("ens1f0np0", 520),
    ("ens1f1np1", 521),
    ("ens2f0np0", 522),
    ("ens2f1np1", 523),
    ("ens8f0np0", 524),
    ("ens8f1np1", 525),
    ("ens2f0", 526),
    ("ens2f1", 527),
    ("ens1f0", 528),
    ("ens1f1", 529),
    ("enp94s0", 530),
    ("ens8f0", 531),
    ("ens8f1", 532),
    ("p3p1", 533),
    ("enp129s0np0", 534),
    ("enp197s0f0np0", 535),
    ("enp197s0f1np1", 536),
    ("enp67s0np0", 537),
    ("enp67s1np1", 538),
    ("eth100g0", 539),
    ("eth100g1", 540),
    ("eth100g2", 541),
    ("eth100g3", 542),
    ("eth100g4", 543),
    ("eth100g5", 544),
    ("eth400g0", 545),
    ("eth400g1", 546),
    ("eth400g2", 547),
    ("eth400g3", 548),
    ("eth400g4", 549),
    ("eth400g5", 550),
    ("eth4", 551),
)


class _NodeConfigFileBase(_ConfigFileWriterBase, metaclass=ABCMeta):
    """ Base class for node-wide config files
    """
    @property
    def config_class_name(self):
        return self.node_section

    @property
    def config_item_name(self):
        return self.name

    def __init__(self, network_config, node_section, node_obj):
        self.network_config = network_config
        self.node_section = node_section
        self.name = node_obj["name"]

    @property
    @abstractmethod
    def config_file_mode(self):
        """ Unix permissions (mode) of the output file
        """

    @property
    @abstractmethod
    def config_file_name(self):
        """ Name of the file being written
        """

    @abstractmethod
    def output_config(self):
        """ Return a buffer with the configuration contents
        """


class NodeConfigArpFluxConf(_NodeConfigFileBase):
    """ ARP Flux sysconfig
    """
    config_file_name = "/etc/sysctl.d/01-arp-flux.conf"
    config_file_mode = 0o644

    def output_config(self):
        return ArpFluxConf().substitute()


class NodeConfigConntrackLiberalConf(_NodeConfigFileBase):
    """ ARP Flux sysconfig
    """
    config_file_name = "/etc/sysctl.d/02-liberal-conntrack.conf"
    config_file_mode = 0o644

    def output_config(self):
        return ConntrackLiberalConf().substitute()


class NodeConfig100GHosts(_NodeConfigFileBase):
    """ Node configuration hosts file for 100G
    """
    config_file_name = "/etc/hosts_100G"
    config_file_mode = 0o644

    def output_config(self):
        outbuf = str()
        outbuf += HostsHeader().substitute()

        iface_ids = dict()
        for ifname, index in NODE_INTERFACE_INDEXES:
            iface_ids[ifname] = index

        for section in NetworkConfigSchema.node_sections:
            for node_obj in self.network_config.get(section, list()):
                node_ifaces = node_obj.get("interfaces", list())

                sorted_ifaces = sorted(node_ifaces,
                                       key=lambda x: iface_ids[x["name"]])

                for iface_obj in sorted_ifaces:
                    address = JSIPInterface(iface_obj["address"]).ip
                    outbuf += HostsEntry().substitute(
                        name=str(node_obj["name"]),
                        address=str(address))
                    # At most 1 entry per node
                    break

        return outbuf


class NodeConfig100GHostname(_NodeConfigFileBase):
    """ Node configuration hostname file for 100G
    """
    config_file_name = "/etc/hostname_100G"
    config_file_mode = 0o644

    def output_config(self):
        return Hostname().substitute(name=self.name)


# pylint: disable=too-many-instance-attributes
class _NodeConfigInterfaceBase(_NodeConfigFileBase, metaclass=ABCMeta):
    """ Base class for node interface config files
    """
    #pylint: disable=too-many-arguments
    def __init__(self, network_config, node_section,
                 node_obj, iface_obj, switch_obj,
                 cluster_prefix, exterior_prefixes,
                 exterior_nonpolicy_prefixes):
        super().__init__(network_config, node_section, node_obj)

        self.iface_name = str(iface_obj["name"])
        self.switch_name = str(iface_obj["switch_name"])
        self.address = JSIPInterface(iface_obj["address"])
        self.gateway = JSIPInterface(switch_obj["address"])

        self.mtu = int(switch_obj.get("mtu", MTU_DEFAULT))
        if (self.mtu < MTU_MIN) or (self.mtu > MTU_MAX_WITH_OVERHEAD):
            raise ValueError(f'Invalid MTU: {self.mtu}')
        # Cap node MTU at 9000
        self.mtu = min(self.mtu, MTU_DEFAULT)

        self.network = JSIPNetwork(switch_obj["prefix"])

        self.cluster_prefix = JSIPNetwork(cluster_prefix)

        if node_section == "swarmx_nodes":
            if switch_obj.get("swarmx_vlan_address"):
                self.gateway = JSIPInterface(switch_obj["swarmx_vlan_address"])
            if switch_obj.get("swarmx_prefix"):
                self.network = JSIPNetwork(switch_obj["swarmx_prefix"])

        self.exterior_prefixes = [
            JSIPNetwork(prefix)
            for prefix in exterior_prefixes
        ]

        self.exterior_nonpolicy_prefixes = [
            JSIPNetwork(prefix)
            for prefix in exterior_nonpolicy_prefixes
        ]

        for ifname, index in NODE_INTERFACE_INDEXES:
            if ifname == self.iface_name:
                self.index = index
                break
        else:
            raise ValueError(
                f'Interface not in index table: {self.iface_name}')


    @property
    @abstractmethod
    def config_file_mode(self):
        """ Unix permissions (mode) of the output file
        """

    @property
    @abstractmethod
    def config_file_name(self):
        """ Name of the file being written
        """

    @abstractmethod
    def output_config(self):
        """ Return a buffer with the configuration contents
        """


class NodeConfigDispatch(_NodeConfigInterfaceBase):
    """ Network Manager interface dispatch configuration
    """
    config_file_mode = 0o755

    @property
    def config_file_name(self):
        return f'/etc/NetworkManager/dispatcher.d/route_fix-{self.iface_name}.sh'

    def output_config(self):
        return Dispatch().substitute(
            name=str(self.iface_name),
            prefix=str(self.network))


class NodeConfigIfCfg(_NodeConfigInterfaceBase):
    """ Interface configuration
    """
    config_file_mode = 0o644

    @property
    def config_file_name(self):
        return f'/etc/sysconfig/network-scripts/ifcfg-{self.iface_name}'

    def output_config(self):
        return IfCfg().substitute(
            name=str(self.iface_name),
            address=str(self.address.ip),
            prefixlen=str(self.network.prefixlen),
            table_id=str(self.index))


class NodeConfigRule(_NodeConfigInterfaceBase):
    """ Interface routing rule configuration
    """
    config_file_mode = 0o644

    @property
    def config_file_name(self):
        return f'/etc/sysconfig/network-scripts/rule-{self.iface_name}'

    def output_config(self):
        return Rule().substitute(
            table_id=str(self.index),
            address=str(self.address.ip))


class NodeConfigRoute(_NodeConfigInterfaceBase):
    """ Interface route configuration
    """
    config_file_mode = 0o644

    @property
    def config_file_name(self):
        return f'/etc/sysconfig/network-scripts/route-{self.iface_name}'

    def output_config(self):
        outbuf = str()

        outbuf += BasicRoutes().substitute(
            name=str(self.iface_name),
            table_id=str(self.index),
            address=str(self.address.ip),
            prefix=str(self.network))

        outbuf += InteriorRoute().substitute(
            name=str(self.iface_name),
            table_id=str(self.index),
            address=str(self.address.ip),
            prefix=str(self.cluster_prefix),
            gw=str(self.gateway.ip))

        for prefix in self.exterior_prefixes:
            outbuf += ExteriorRoute().substitute(
                name=str(self.iface_name),
                table_id=str(self.index),
                address=str(self.address.ip),
                prefix=str(prefix),
                gw=str(self.gateway.ip))

        for prefix in self.exterior_nonpolicy_prefixes:
            outbuf += ExteriorNonPolicyRoute().substitute(
                name=str(self.iface_name),
                table_id=str(self.index),
                address=str(self.address.ip),
                prefix=str(prefix),
                gw=str(self.gateway.ip))

        return outbuf


class NodeConfigWriter(_ConfigAggregateBase):
    """ Write host configuration files
    """
    config_host_classes = (
        NodeConfigArpFluxConf,
        NodeConfigConntrackLiberalConf,
        NodeConfig100GHostname,
        NodeConfig100GHosts
    )

    config_iface_classes = (
        NodeConfigDispatch,
        NodeConfigIfCfg,
        NodeConfigRule,
        NodeConfigRoute,
    )

    def __init__(self, network_config, node_section, node_name):
        for node_obj in network_config[node_section]:
            if node_obj["name"] == node_name:
                break
        else:
            raise ValueError(f'Node does not exist in type: {node_section}, {node_name}')

        switches = dict(items_name_iter(network_config["switches"]))

        cluster_prefix = JSIPNetwork(
            network_config.get("environment", {}).get("cluster_prefix"))

        exterior_prefixes = list(
            network_config.get("environment", {}).get("exterior_prefixes", []))

        exterior_nonpolicy_prefixes = list(
            network_config.get("environment", {}).get(
                "exterior_nonpolicy_prefixes", []))

        self._config_instances = list()

        for cls in self.config_host_classes:
            instance = cls(network_config, node_section, node_obj)
            self._config_instances.append(instance)

        for cls in self.config_iface_classes:
            for iface_obj in node_obj.get("interfaces", list()):
                if iface_obj["switch_name"] in switches:
                    switch_obj = switches[iface_obj["switch_name"]]
                    instance = cls(network_config, node_section,
                                   node_obj, iface_obj, switch_obj,
                                   cluster_prefix, exterior_prefixes,
                                   exterior_nonpolicy_prefixes)
                    self._config_instances.append(instance)

    def config_instances(self):
        return self._config_instances


class AllNodesConfigWriter(_ConfigAggregateBase):
    """ Write configuration files for all nodes
    """
    def __init__(self, network_config):
        self._config_instances = list()

        # TODO: User nodes need different configuration style
        node_sections = list(NetworkConfigSchema.node_sections)
        ## node_sections.remove("user_nodes")

        for node_section in node_sections:
            for node_obj in network_config.get(node_section, list()):
                self._config_instances.append(
                    NodeConfigWriter(network_config, node_section,
                                     node_obj["name"]))

    def	config_instances(self):
        return self._config_instances
