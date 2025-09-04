#!/usr/bin/env python3

"""
Uses monolith's sm_helper to do a standby/activate cycle on the system

Usage: reset-system.py SYSTEM_NAME
"""

import time
import sys

import cerebras.common.sm_helpers as smh


def main(system: str):
    start_time = time.time()
    sm = smh.Sm(system)
    print(f"begin system reset, system {system} health={sm.health}")
    rv, msg = sm.reset()
    print(f"\nsystem reset returned: code={rv} msg={msg}")
    print(f"system {system} reset took={int(time.time() - start_time)}s, health={sm.health}")
    exit(rv)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: reset-system.py SYSTEM_NAME")
        exit(1)
    main(sys.argv[1])