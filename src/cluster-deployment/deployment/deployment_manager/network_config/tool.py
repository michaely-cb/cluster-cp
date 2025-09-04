#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved

"""
Network configuration tool

Drive L3 cluster network configuration and deployment tasks
"""

import argparse
import logging
import sys

# pylint: disable=import-error
from . import cli


def _main(argv):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(logging.StreamHandler(sys.stdout))

    running_cli = cli.CLI()

    parser = argparse.ArgumentParser(description=__doc__)

    running_cli.build_parser(parser)

    args = parser.parse_args(argv)

    rv = running_cli(args)
    if rv is None:
        return 0
    elif isinstance(rv, int):
        return rv
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
