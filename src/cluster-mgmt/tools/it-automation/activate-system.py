#!/usr/bin/env python3

"""
Uses monolith's sm_helper to activate the system

Usage: activate-system.py SYSTEM_NAME
"""

import time
import sys

import devinfra.common.helpers.sm_helpers as smh


def main(system: str):
    start_time = time.time()
    sm = smh.Sm(system)

    if sm.is_active:
        print(f"{system} already active, skipping")
        exit(0)

    print(f"begin system activate, system {system} health={sm.health}")
    rv, msg = sm.activate()
    print(f"\nsystem activate returned: code={rv} msg={msg}")
    print(f"system {system} activate took={int(time.time() - start_time)}s, health={sm.health}")
    exit(rv)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: activate-system.py SYSTEM_NAME")
        exit(1)
    main(sys.argv[1])