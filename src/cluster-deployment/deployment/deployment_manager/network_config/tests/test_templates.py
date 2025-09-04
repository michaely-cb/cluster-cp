#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

""" Tests for the templates modules
"""
import importlib
import inspect
import unittest
from types import ModuleType
from typing import List, Type, Union

import deployment_manager.network_config as network_config
from deployment_manager.network_config.templates.switch import _SwitchClassOverride


def find_subclasses(module: Union[ModuleType, str], parent_class: Type) -> List[Type]:
    """
    Find all classes in a module that inherit from a specific parent class.
    """
    subclasses = []

    if isinstance(module, str):
        module = importlib.import_module(module)

    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj) and issubclass(obj, parent_class) and obj is not parent_class:
            subclasses.append(obj)
    return subclasses


class TestTemplates(unittest.TestCase):
    """ Test cases for template classes
    """

    @staticmethod
    def test_load_node_templates():
        """Validate that the template module can load. This loads the
        underlying template file.

        """
        network_config.templates.node.ArpFluxConf()
        network_config.templates.node.ConntrackLiberalConf()
        network_config.templates.node.BasicRoutes()
        network_config.templates.node.Dispatch()
        network_config.templates.node.ExteriorRoute()
        network_config.templates.node.ExteriorNonPolicyRoute()
        network_config.templates.node.IfCfg()
        network_config.templates.node.InteriorRoute()
        network_config.templates.node.Rule()
        network_config.templates.node.HostsHeader()
        network_config.templates.node.HostsEntry()
        network_config.templates.node.Hostname()

    def test_load_switch_templates(self):
        """Validate that the template module can load. This implicitly
            verifies the underlying template can load.
        """
        switch_types = {
            "arista": [None, "7060DX5", "7060X6", "7020", "7808"],
            "hpe": [None, "12908", "3700", "4600"],
            "dell": [None],
            "edgecore": [None],
            "juniper": [None, "ptx10002-36qdd"]
        }
        errors = []
        for cls in find_subclasses("deployment_manager.network_config.templates.switch", _SwitchClassOverride):
            for vendor, models in switch_types.items():
                for model in models:
                    try:
                        cls(vendor, model_name=model)
                    except AttributeError as e:
                        errors.append(f"failed to lookup template {vendor}/{model} for {cls}: {e}")
        self.assertEqual([], errors)
