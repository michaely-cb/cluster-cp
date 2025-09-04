#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved

"""
System based configuration
"""
import json
from pathlib import Path
from typing import Dict, List
import logging
from ..common.context import NetworkCfgDoc
from .base import \
    SubCommandBase, \
    UsesNetworkConfig

logger = logging.getLogger(__name__)

# We should really be getting this from the database
SYSTEM_INTERFACE_COUNT = 12


def safe_load_system_connections(fp: str, allow_names: set[str]) -> List[dict[str, str]]:
    """ Load system_connections from file, validating their format and filtering on allow_names """
    fp = Path(fp)
    try:
        conns = json.loads(fp.read_text())
        assert isinstance(conns, list) and all(isinstance(d, dict) for d in conns), \
            f"expected a json list of objects, got {type(conns)}"
    except Exception as e:
        raise ValueError(f"failed to read system_connections {fp}: {e}")

    rv = []
    for i, conn in enumerate(conns):
        assert isinstance(conn, dict), f"{fp}: object at index {i} was not a dict: {conn}"
        o = {}
        for k in ("system_name", "system_port", "switch_name", "switch_port"):
            if not conn.get(k):
                raise ValueError(f"{fp}: object at index {i} missing required field '{k}': {conn}")
            o[k] = conn[k]
        if o["system_name"] not in allow_names or o["switch_name"] not in allow_names:
            logger.debug(f"filtering system connection, its switch or system not in network_config.json: {o}")
            continue
        rv.append(o)
    return rv


@UsesNetworkConfig(save=True)
class SetSystemConnections(SubCommandBase):
    """
    Set system_connections to network config from a file rather than LLDP output.
    """
    sub_command = 'set_system_connections'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('--filename', required=True,
                            help="File containing system connections")

    def __call__(self, args):
        names = set(s['name'] for s in args.network_config.get("systems", [])).union(
            s['name'] for s in args.network_config.get("switches", [])
        )
        args.network_config["system_connections"] = safe_load_system_connections(args.filename, names)

