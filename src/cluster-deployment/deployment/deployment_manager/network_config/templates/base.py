#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Methods to manage template files
"""

import os
import string
from abc import ABCMeta, abstractmethod
from pkg_resources import resource_string


class _TemplateBase(metaclass=ABCMeta):

    @property
    @abstractmethod
    def DIRNAME(self): # pylint: disable=invalid-name
        """
        Return the directory name to load with trailing /
        """

    @property
    @abstractmethod
    def FILENAME(self): # pylint: disable=invalid-name
        """
        Return the filename to load
        """

    def __init__(self, model_name=None):
        mn = ""
        if model_name:
            mn = f"{model_name}/"
        file_path = f"{self.DIRNAME}{mn}{self.FILENAME}"
        # Not all models will need special config. In such
        # cases, we will use files in the vendor dir
        if not os.path.exists(
            os.path.join(os.path.dirname(__file__), file_path)
        ):
            file_path = f"{self.DIRNAME}{self.FILENAME}"
        file_buf = resource_string(__name__, file_path)
        self.template = string.Template(file_buf.decode())

    def substitute(self, mapping=None, **kwargs):
        """ Run template substitutions
        """
        if mapping is None:
            mapping = {}
        sub = self.template.substitute(mapping, **kwargs).rstrip()
        return sub + "\n"
