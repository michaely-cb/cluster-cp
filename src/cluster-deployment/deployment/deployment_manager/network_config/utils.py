#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Various common utilities
"""
import ipaddress
from typing import Iterator, Tuple, List


def align_segment(start_addr: ipaddress.IPv4Address, current_offset: int, segment_size: int) -> int:
    """ Return an offset from start_addr which is aligned to a network address of a prefix with segment_size addrs """
    offset = int(start_addr) + current_offset
    if offset % segment_size == 0:
        return current_offset
    return ((offset // segment_size) + 1) * segment_size - int(start_addr)

def filter_items_iter(items, search_function=None, morph_function=None):
    """ Iterate through a list of items filtered by a search function
    and modified by an optional morph function.
    """
    if search_function is None:
        search_function = lambda x: True

    if morph_function is None:
        morph_function = lambda x: x

    for item_obj in items:
        if search_function(item_obj):
            yield morph_function(item_obj)


def items_name_iter(items: List[dict]) -> Iterator[Tuple[str, dict]]:
    """ Iterate through pairs of name, obj
    """
    def search_function(_):
        return True

    def morph_function(obj: dict) -> Tuple[str, dict]:
        return obj["name"], obj

    for item_name, item_obj in filter_items_iter(items, search_function, morph_function):
        yield item_name, item_obj


def find_item_named(items, item_find_name):
    """ Find the first item with a specific name
    """
    def search_function(item_obj):
        return item_obj["name"] == item_find_name

    def morph_function(item_obj):
        return item_obj

    for item_obj in filter_items_iter(items, search_function, morph_function):
        return item_obj

    raise KeyError(f'Item not found: {item_find_name}')

def add_or_update_item(base_obj, item_section, new_item):
    """ Add or update an existing item in a collection inside of the
    base object.

    If the section doesn't exist, it will be created.
    """
    if item_section not in base_obj:
        base_obj[item_section] = list()

    for old_item in base_obj[item_section]:
        if old_item["name"] == new_item["name"]:
            old_item.update(new_item)
            break

    else: # for old_item ...
        base_obj[item_section].append(new_item)

def update_item_name(base_obj, item_section, old_name, new_name):
    """ Update the name of an existing item
    """
    for item in base_obj.get(item_section, []):
        if item["name"] == old_name:
            item["name"] = new_name
            break

__all__ = [
    'filter_items_iter',
    'items_name_iter',
    'find_item_named',
    'add_or_update_item',
    'update_item_name'
]
