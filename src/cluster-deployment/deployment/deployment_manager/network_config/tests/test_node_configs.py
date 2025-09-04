#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.


""" Tests for the network planner node config module
"""
from pathlib import Path

import deployment_manager.network_config as network_config
from deployment_manager.network_config.tests.network_builder import LeafSpineNetworkBuilder


def test_all_pairs_unique():
    """ Validate that all pairs of interface name and number are unique
    """
    name_to_id = dict()
    id_to_name = dict()

    for entry in network_config.configs.node.NODE_INTERFACE_INDEXES:
        assert entry[0] not in name_to_id
        assert entry[1] not in id_to_name
        name_to_id[entry[0]] = entry[1]
        id_to_name[entry[1]] = entry[0]


def test_write_config_files_exist(tmp_path):
    """ ensure that no generated config files were cleaned up during config file generation """
    builder = LeafSpineNetworkBuilder(0)
    builder.add_mleaf(mg=1)
    builder.call_allocate_tiers(tmp_path)
    builder.call_placer(tmp_path)
    files = builder.call_generate_cfg(tmp_path)
    for file in files:
        assert Path(file).is_file
