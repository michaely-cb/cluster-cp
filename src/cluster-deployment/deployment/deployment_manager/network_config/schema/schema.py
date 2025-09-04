#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Multibox cluster network configuration classes
"""

import json
import os
import tempfile
from abc import ABCMeta, abstractmethod

import jsonschema
from pkg_resources import resource_string

from .jstypes import JSIPAddress, JSIPNetwork, JSIPInterface, ASN4
from ..utils import add_or_update_item, update_item_name


def _build_checker(ref):
    def _checker(value):
        try:
            ref(value)
            return True
        except Exception: # pylint: disable=broad-except
            pass

        return False

    return _checker


class _ManagedJsonSchemaBase(metaclass=ABCMeta):
    """ ABC for managed JSON files with associated schema

    Subclasses must call this classes __init__
    """

    # Be generous with backup files
    backup_file_count = 20

    @property
    @abstractmethod
    def schema_dirname(self) -> str:
        """ Return the directory name to load with trailing /
        """

    @property
    @abstractmethod
    def schema_filename(self) -> str:
        """ Return the schema filename to load
        """

    def __init__(self):
        schema_file_path = str(self.schema_dirname) + str(self.schema_filename)
        schema_buf = resource_string(__name__, schema_file_path)
        self.schema = json.loads(schema_buf)

        self.format_checker = jsonschema.FormatChecker()
        self.format_checker.checks("ip_address")(_build_checker(JSIPAddress))
        self.format_checker.checks("ip_network")(_build_checker(JSIPNetwork))
        self.format_checker.checks("ip_interface")(_build_checker(JSIPInterface))
        self.format_checker.checks("asn4")(_build_checker(ASN4))

        self.validator = jsonschema.validators.Draft4Validator(
            schema=self.schema, format_checker=self.format_checker)

    def validate(self, data_obj):
        """ Validate the provided object against this schema
        """
        return self.validator.validate(data_obj)

    def load_json(self, data_filename):
        """ Load the json object and validate against this schema
        """
        with open(data_filename) as input_data:
            data_obj = json.load(input_data)

        self.validate(data_obj)

        return data_obj

    def save_json(self, data_obj, data_filename):
        """ Validate the JSON object against this schema and save
        """
        self.validate(data_obj)

        dest_filename = str(data_filename)
        dest_prefix = os.path.abspath(dest_filename) + "."
        dest_backup = dest_filename + ".old"
        dest_backup_chain = [ dest_filename, dest_backup ] + \
            [ dest_backup + '.' + str(idx)
              for idx in range(1, self.backup_file_count + 1) ]

        with tempfile.NamedTemporaryFile(mode='w+', prefix=dest_prefix) as output:
            json.dump(data_obj, output, sort_keys=True, indent=4,
                      separators=(',', ': '))
            output.flush()

            backup_last = None
            for backup_this in reversed(dest_backup_chain):
                if backup_last is None:
                    if os.path.exists(backup_this):
                        os.unlink(backup_this)

                else: # if backup_last ...
                    if os.path.exists(backup_last):
                        os.unlink(backup_last)

                    if os.path.exists(backup_this):
                        os.rename(backup_this, backup_last)

                backup_last = backup_this

            os.link(output.name, dest_filename)


class NetworkConfigSchema(_ManagedJsonSchemaBase):
    """ Schema for the network configuration file.
    """
    schema_dirname = "./data/"
    schema_filename = "cerebras_cluster_network_config_schema.json"

    node_sections = ("memoryx_nodes", "swarmx_nodes", "worker_nodes",
                     "user_nodes", "management_nodes", "activation_nodes",
                     "inferencedriver_nodes",
                     )

    system_sections = ("systems",)

    switch_sections = ("switches",)

    host_sections = node_sections + system_sections + switch_sections

    def new_config(self, name, description=None, notes=None):
        """ Create a new network configuration
        """
        new_obj = dict(
            name=str(name)
        )

        if description is not None:
            new_obj["description"] = str(description)

        if notes is not None:
            new_obj["notes"] = list(notes)

        self.validate(new_obj)
        return new_obj

    def update_root_server(self, network_config, name, ip_address):
        """ Update the root_server information
        """
        network_config["root_server"] = {
            "name": name,
            "ip_address": ip_address
        }
        return network_config

    def add_switch(self, network_config, name, mgt_addr, vendor, model, tier,
                   tier_pos, username=None, password=None, rack=None, rack_unit=None,
                   stamp=None, prefix=None, system_prefix=None, swarmx_prefix=None,
                   validate=True):
        """ Add or update a switch

        Note, by_default this validates after the network_config object has been modified.
        """
        new_obj = dict(
            name=str(name),
            vendor=str(vendor),
            tier=str(tier),
            tier_pos=int(tier_pos),
        )

        if model:
            new_obj["model"] = str(model)

        if mgt_addr is not None and mgt_addr:
            new_obj["management_address"] = str(mgt_addr)
            new_obj["router_id"] = str(mgt_addr)

        if username:
            new_obj["username"] = username
        if password:
            new_obj["password"] = password
        if rack:
            new_obj["rack"] = rack
        if rack_unit:
            new_obj["rack_unit"] = int(rack_unit)
        if stamp:
            new_obj["stamp"] = stamp

        if prefix:
            new_obj["provided_prefix"] = prefix
        if system_prefix:
            new_obj["provided_system_prefix"] = system_prefix
        if swarmx_prefix:
            new_obj["provided_swarmx_prefix"] = swarmx_prefix

        add_or_update_item(network_config, "switches", new_obj)
        if validate:
            self.validate(network_config)
        return network_config

    def add_system(self, network_config, name, mgt_addr, nodegroup,
                   rack, rack_unit, stamp, system_vlan, default_hostname="",
                   validate=True):
        """ Add or update a system

        Note, by default this validates after the network_config object has been modified.
        """
        new_obj = dict(
            name=str(name)
        )

        if mgt_addr is not None and mgt_addr:
            new_obj["management_address"] = str(mgt_addr)
        if nodegroup:
            new_obj["nodegroup"] = int(nodegroup)
        if rack:
            new_obj["rack"] = rack
        if rack_unit:
            new_obj["rack_unit"] = int(rack_unit)
        if stamp:
            new_obj["stamp"] = stamp
        if system_vlan:
            new_obj["system_vlan"] = "true"
        if default_hostname:
            new_obj["default_hostname"] = default_hostname

        add_or_update_item(network_config, "systems", new_obj)
        if validate:
            self.validate(network_config)
        return network_config

    def update_system_name(self, network_config, old_name, new_name):
        """ Update the name of a system
        """
        if old_name == new_name:
            return network_config
        update_item_name(network_config, "systems", old_name, new_name)
        return network_config

    def add_node(self, network_config, node_section, name, mgt_addr,
                 nodegroup, username, password, rack, rack_unit, stamp,
                 validate=True):
        """ Add or update a node
        Note, by default this validates after the network_config object has been modified.
        """
        if node_section not in self.node_sections:
            raise ValueError(f'invalid node section: {node_section}')

        new_obj = dict(
            name=str(name)
        )

        if mgt_addr is not None and mgt_addr:
            new_obj["management_address"] = str(mgt_addr)

        if nodegroup:
            new_obj["nodegroup"] = int(nodegroup)
        if username:
            new_obj["username"] = username
        if password:
            new_obj["password"] = password
        if rack:
            new_obj["rack"] = rack
        if rack_unit:
            new_obj["rack_unit"] = int(rack_unit)
        if stamp:
            new_obj["stamp"] = stamp

        add_or_update_item(network_config, node_section, new_obj)
        if validate:
            self.validate(network_config)
        return network_config

    def update_node_name(self, network_config, node_section,
                            old_name, new_name):
        """ Update the name of a node
        """
        if node_section not in self.node_sections:
            raise ValueError(f'invalid node section: {node_section}')
        update_item_name(network_config, node_section, old_name, new_name)
        return network_config


SCHEMA = NetworkConfigSchema()

class ObjContact:
    """ How to contact a switch/system/node object
    """
    def __init__(self, obj):
        if "management_address" in obj:
            self._address = obj["management_address"]

        else:
            self._address = obj["name"]

    def __str__(self):
        return str(self._address)
