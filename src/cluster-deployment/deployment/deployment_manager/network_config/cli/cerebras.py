#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Cerebras internal environment specific CLI classes and methods
"""

import json

from .base import \
    SubCommandBase, \
    UsesNetworkConfig

@UsesNetworkConfig(save=True)
class IngestMultiboxConfig(SubCommandBase):
    """ Ingest multibox configuration into the inventory

    Note: This is specific to the Cerebras internal environment
    """
    sub_command = 'ingest_multibox'

    HOST_TO_NODE_TYPES = (
        ('management_hosts', 'management_nodes'),
        ('memoryx_hosts', 'memoryx_nodes'),
        ('activation_hosts', 'activation_nodes'),
        ('swarmx_hosts', 'swarmx_nodes'),
        ('user_hosts', 'user_nodes'),
        ('worker_hosts', 'worker_nodes'),
    )

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-m', '--multibox_config', required=True,
                            help="json file from the multibox database")

    def __call__(self, args):
        with open(args.multibox_config) as multibox_fd:
            multibox_obj = json.load(multibox_fd)

        for name in multibox_obj.get("systems", list()):
            args.network_schema.add_system(args.network_config, name)

        for host_type, node_type in self.HOST_TO_NODE_TYPES:
            for name in multibox_obj.get(host_type, list()):
                args.network_schema.add_node(
                    args.network_config, node_type, name)
