import logging
from typing import Dict, List, Tuple


logger = logging.getLogger(__name__)


def interface_sort_key(inf: str) -> str:
    """
    Normalize the port numbering for better sort ordering, e.g.
    sort(Ethernet1/1, Ethernet11/1, Ethernet2/1) -> Ethernet1/1, Ethernet2/1, Ethernet11/1
    """
    spans, span = [], -1
    for c in inf:
        i = ord(c) - ord("0")
        if 0 <= i < 10:
            span = i if span == -1 else span * 10 + i
        elif span > -1:
            spans.append(span)
            span = -1
    if span > -1:
        spans.append(span)
    return ":".join([f"{d:05}" for d in spans])


def parse_table_lines(cols: List[Tuple[str, int, int]], lines: List[str]) -> List[dict]:
    rv = []
    for line in lines:
        if not line.strip():
            break
        entry = {}
        for col in cols:
            entry[col[0]] = line[col[1]:col[2]].strip()
        rv.append(entry)
    return rv


def parse_sonic_structured_output(output: str) -> dict:
    """ Parse a yaml-ish like doc that sonic CLI outputs. It will drop value output if the key has nested keys
    e.g.
    Ethernet220: SFP EEPROM detected
        Application Advertisement:
                1: 400GAUI-8
        Connector: MPOx12
    Ethernet224: No Connection
    ->
    {"Ethernet220": {
       "Application Advertisement": {
            1: "400GAUI-8"
       },
       "Connector": "MP0x12"
     },
     "Ethernet224": "No Connection"}
    """
    lines = output.strip().split("\n")
    obj = {}
    entry_stack = [obj]
    last_key = ""
    indent_width = -1
    level = 0

    for line in lines:
        if not line.strip() or ":" not in line:
            continue
        leading_spaces = len(line) - len(line.lstrip(" "))
        if indent_width == -1 and line.startswith(" "):
            indent_width = len(line) - len(line.lstrip(" "))

        if indent_width != -1:
            next_level = int(leading_spaces / indent_width)
        else:
            next_level = level

        key_value = line.strip().split(":", 1)
        key = key_value[0].strip()
        value = key_value[1].strip() if len(key_value) == 2 else ""

        if next_level < level or next_level == 0:
            while len(entry_stack) > next_level + 1:
                entry_stack.pop()
        elif next_level == level + 1:
            entry_stack[-1][last_key] = {}
            entry_stack.append(entry_stack[-1][last_key])

        entry_stack[-1][key] = value
        last_key = key
        level = next_level

    return obj
