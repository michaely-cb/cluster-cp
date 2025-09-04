#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Placer base classes
"""
from abc import ABCMeta, abstractmethod


class _BasePlacer(metaclass=ABCMeta):
    def __call__(self):
        # pass 1, manage previous allocations
        for item_obj in self.items_iterator():
            item_resource = self.get_item_resource(item_obj)
            if item_resource is not None:
                try:
                    self.resource_extent(item_obj).remove(item_resource)
                except ValueError:
                    # the allocated resource is no longer valid
                    self.del_item_resource(item_obj)

        # pass 2, allocate any missing resource
        for item_obj in self.items_iterator():
            item_resource = self.get_item_resource(item_obj)
            resource_extent = self.resource_extent(item_obj)
            if item_resource is None:
                if resource_extent:
                    self.set_item_resource(
                        item_obj, self.resource_extent(item_obj).pop(0))
                else:
                    self.set_item_resource(
                        item_obj, None)

    @abstractmethod
    def resource_extent(self, item_obj):
        """Provide the resource extent allocation for a specific item

        Note, this should provide the same extent on repeated calls
        based on the item_obj properties in order to accommodate usage
        tracking.
        """

    @abstractmethod
    def items_iterator(self):
        """ Return a new iterator for items being examined/modified
        """

    @abstractmethod
    def get_item_resource(self, item_obj):
        """ Return the value of the item or None if it is not set in the obj
        """

    @abstractmethod
    def set_item_resource(self, item_obj, resource):
        """ Set the item value in the object
        """

    @abstractmethod
    def del_item_resource(self, item_obj):
        """ Remove the value from the object
        """


class _BaseItemKeyPlacer(_BasePlacer, metaclass=ABCMeta):
    """ Base class for placers that use a simple key
    """

    @abstractmethod
    def resource_extent(self, item_obj):
        """Provide the resource extent allocation for a specific item

        Note, this should provide the same extent on repeated calls
        based on the item_obj properties in order to accommodate usage
        tracking.
        """

    @abstractmethod
    def items_iterator(self):
        """ Return a new iterator for items being examined/modified
        """

    @abstractmethod
    def resource_object(self, value):
        """ Build the resource object for a value
        """

    @property
    @abstractmethod
    def item_key(self):
        """ Return the item key for the object
        """

    def get_item_resource(self, item_obj):
        if self.item_key in item_obj:
            item_value = item_obj[self.item_key]
            if item_value:
                return self.resource_object(item_value)

        return None

    def set_item_resource(self, item_obj, resource):
        item_obj[self.item_key] = str(self.resource_object(resource))

    def del_item_resource(self, item_obj):
        del item_obj[self.item_key]
