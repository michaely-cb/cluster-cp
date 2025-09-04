#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Configuration file base classes
"""
import os
import shutil
from abc import ABCMeta, abstractmethod
from typing import List

MTU_MAX = 9000
MTU_MAX_WITH_OVERHEAD = 9 * 1024
MTU_MIN = 1500
MTU_DEFAULT = MTU_MAX


class _ConfigFileWriterBase(metaclass=ABCMeta):
    """ Config file writer base class
    """

    def cfgdir(self, outdir):
        """ Output the configuration file base dir and full filepath """
        # Can't use os.path.join here
        entity_basedir = os.path.normpath('/'.join((outdir, self.config_class_name, self.config_item_name,)))
        cfgfile = os.path.normpath('/'.join((entity_basedir, self.config_file_name,)))
        return entity_basedir, cfgfile

    def write_config(self, outdir) -> List[str]:
        """ Output the configuration file, return the name of the config file """
        _, cfgfile = self.cfgdir(outdir)
        cfgfile_dir = os.path.dirname(cfgfile)
        if not os.path.exists(cfgfile_dir):
            os.makedirs(cfgfile_dir)

        with open(cfgfile, 'w') as output_fd:
            output_fd.write(self.output_config())
            os.fchmod(output_fd.fileno(), self.config_file_mode)
        return [cfgfile]

    @property
    @abstractmethod
    def config_class_name(self):
        """ Class of config object (node, switch, system, etc...)
        """

    @property
    @abstractmethod
    def config_item_name(self):
        """ Name of the thing being configured (ex: host name, switch name)
        """

    @property
    @abstractmethod
    def config_file_name(self):
        """ Name of the file being written
        """

    @property
    @abstractmethod
    def config_file_mode(self):
        """ Unix permissions (mode) of the output file
        """

    @abstractmethod
    def output_config(self):
        """ Return a buffer with the configuration contents
        """


class _ConfigAggregateBase(metaclass=ABCMeta):
    """ Aggregate generation of several config files
    """

    def write_config(self, outdir) -> List[str]:
        """ Output the configuration files, return list of written config files """

        seen_dir, output_files = set(), []
        for config_instance in self.config_instances():
            if isinstance(config_instance, _ConfigFileWriterBase):
                entity_basedir, _ = config_instance.cfgdir(outdir)
                if entity_basedir not in seen_dir:
                    # cleanup stale dir first
                    shutil.rmtree(entity_basedir, ignore_errors=True)
                    seen_dir.add(entity_basedir)
            output_files.extend(config_instance.write_config(outdir))
        return output_files

    @abstractmethod
    def config_instances(self):
        """ Return a list of configuration files instances to write
        """
