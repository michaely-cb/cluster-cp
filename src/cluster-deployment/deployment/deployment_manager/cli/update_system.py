#!/usr/bin/env python
"""
Script for updating a CS system's image
owner : Steven Li
"""

import argparse
import logging
import sys

import urllib3

from deployment_manager.tools.flags import add_image_flag, add_system_user_pw_flags
from deployment_manager.tools.system import SystemCtl

# Disable warning about insecure connections.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def parse_args():
    """
    parse command-line args
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    add_image_flag(parser)
    add_system_user_pw_flags(parser)
    parser.add_argument(
        "--system", help="CS system name", type=str, required=True
    )
    parser.add_argument(
        "--force",
        help="Force update",
        action='store_true',
        dest='force',
        default=False,
    )
    parser.add_argument(
        "--skip_activate",
        help="Skip system activation after OS update",
        action='store_true',
        dest='skip_activate',
        default=False,
    )
    return parser.parse_args()


def main():
    """ Main function for updating CS image """
    # The whole process can take up to one hour from standby, update, to
    # activate.
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    system = SystemCtl(args.system, args.username, args.password)
    system.update_service(args.image, args.force, args.skip_activate)

    return 0


if __name__ == "__main__":
    sys.exit(main())
