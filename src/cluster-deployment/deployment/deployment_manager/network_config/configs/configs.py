#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Configuration builder driver for all configs
"""

from .base import _ConfigAggregateBase

from .system import AllSystemsConfigWriter
from .node import AllNodesConfigWriter
from .switch import AllSwitchesConfigWriter


class AllConfigsWriter(_ConfigAggregateBase):
    """ Write all configuration files
    """
    def __init__(self, network_config):
        self._config_instances = [
            AllSystemsConfigWriter(network_config),
            AllNodesConfigWriter(network_config),
            AllSwitchesConfigWriter(network_config)
        ]

    def config_instances(self):
        return self._config_instances
