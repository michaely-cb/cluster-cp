#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Multibox cluster network configuration JS object types
"""

# Note: This was a work-in-progress implementation of directly
# converting the JSON schema into a class tree.

# That implementation is around half complete but is being withheld
# for now because it was taking too much time.

# For now, users of the schema should just use the nested dict objects
# resulting from JSON conversion and should exercise caution with
# assumptions about what values are provided.

from abc import ABCMeta, abstractmethod
from functools import total_ordering
import collections
import ipaddress


class _BaseResourceExtent(collections.abc.MutableSequence, metaclass=ABCMeta):
    """ Base class for an extent of resources that can be consumed.
    """
    def __init__(self, starting_value=None, count=None):
        if starting_value is None:  # empty extent object
            self._items = []
            return

        if int(count) < 1:
            raise ValueError(f'Count must be >= 1: {count}')

        self._items = [ self.construct_item(starting_value) ]

        for idx in range(count - 1):
            self._items.append(self.next_item(self._items[idx]))

    def __str__(self):
        return f"{len(self._items)} items from {str(self._items[0])}"

    def __getitem__(self, index):
        return self._items[index]

    def __setitem__(self, index, value):
        self._items[index] = self.construct_item(value)

    def __delitem__(self, index):
        del self._items[index]

    def __len__(self):
        return len(self._items)

    def __add__(self, extent):
        self._items += extent._items

    def insert(self, index, value):
        self._items.insert(index, self.construct_item(value))

    @abstractmethod
    def construct_item(self, value):
        """ Construct an item based on a value
        """

    @abstractmethod
    def next_item(self, prev_value):
        """ Construct the next item based on the previous value
        """

@total_ordering
class ASN4:
    """ Store a 4 byte ASN object
    """
    def __init__(self, val):
        asval = None

        # ASN4 object, integer or integer string
        try:
            asval = int(val)
        except ValueError:
            pass

        # Asdot4 notation
        if asval is None:
            try:
                parts = val.split('.')
                if len(parts) != 2:
                    raise ValueError("Malformed ASN+4 string")

                as_parts = []
                for part in parts:
                    as_part = int(part)
                    if as_part > 0xFFFF:
                        raise ValueError("ASN+4 component overrun")
                    as_parts.append(as_part)

                asval = (as_parts[0] << 16) | as_parts[1]

            except (ValueError, AttributeError):
                pass

        if asval is None:
            raise ValueError("Invalid ASN+4 format or type")

        if asval > 0xFFFFFFFF:
            raise ValueError("ASN+4 value overrun")

        if asval < 0:
            raise ValueError("ASN+4 value underrun")

        self._val = asval


    def __int__(self):
        return self._val

    def __str__(self):
        if self._val <= 0xFFFF:
            return str(self._val)

        parts = (self._val >> 16, self._val & 0xFFFF)
        str_parts = [str(part) for part in parts]
        return '.'.join(str_parts)

    def __eq__(self, other):
        return int(self) == int(other)

    def __lt__(self, other):
        return int(self) < int(other)

    def __add__(self, other):
        return self.__class__(int(self) + int(other))

    def __sub__(self, other):
        return self.__class__(int(self) - int(other))


class ASN4Extent(_BaseResourceExtent):
    """ An extent of ASN4s
    """
    def construct_item(self, value):
        return ASN4(value)

    def next_item(self, prev_value):
        return ASN4(prev_value) + 1


class JSIPAddress(ipaddress.IPv4Address):
    """ Manage an IP Address object
    """


class JSIPAddressExtent(_BaseResourceExtent):
    """ An extent of JSIPAddress objects
    """
    def construct_item(self, value):
        return JSIPAddress(value)

    def next_item(self, prev_value):
        prev_item = JSIPAddress(prev_value)
        return prev_item + 1


class JSIPInterface(ipaddress.IPv4Interface):
    """ Manage an IP Interface object
    """

    @property
    def as_netmask(self):
        return " ".join(self.with_netmask.split('/'))


class JSIPInterfaceExtent(_BaseResourceExtent):
    """ An extent of JSIPInterface objects
    """
    def construct_item(self, value):
        return JSIPInterface(value)

    def next_item(self, prev_value):
        prev_item = JSIPInterface(prev_value)
        ip = prev_item.ip
        ip += 1
        prefixlen = prev_item.network.prefixlen
        return JSIPInterface(str(ip) + "/" + str(prefixlen))


class JSIPNetwork(ipaddress.IPv4Network):
    """ Manage an IP Network object
    """

    def interfaces(self):
        """ Produce an extent of interfaces from this network
        """
        interfaces_extent = JSIPInterfaceExtent()
        for item in iter(self):
            interfaces_extent.append(str(item) + "/" +
                                     str(self.prefixlen))

        return interfaces_extent


    def addresses(self):
        """ Produce an extent of addresses from this network
        """
        addresses_extent = JSIPAddressExtent()
        for item in iter(self):
            addresses_extent.append(item)

        return addresses_extent


class JSIPNetworkExtent(_BaseResourceExtent):
    """ An extent of JSIPNetwork objects
    """
    def construct_item(self, value):
        return JSIPNetwork(value)

    def next_item(self, prev_value):
        prev_item = JSIPNetwork(prev_value)
        ip = prev_item.network_address
        ip += prev_item.num_addresses
        return JSIPNetwork(str(ip) + "/" + str(prev_item.prefixlen))
