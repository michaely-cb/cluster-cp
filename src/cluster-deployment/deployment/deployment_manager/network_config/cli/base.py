#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

""" Base CLI classes and methods
"""

import argparse
import copy
import functools
import inspect
import types
from abc import ABCMeta, abstractmethod

from .. import schema


def add_network_config_arg(parser):
    """ Add network config json common argument
    """
    parser.add_argument('-c', '--config', required=True,
                        help='Network configuration file')

class UsesNetworkConfig:
    """ Decorate a class to provide the network schema and configuration
    """
    def __init__(self, save=False):
        self.save = save

    def __call__(self, wrapped):
        @functools.wraps(getattr(wrapped, '__call__'))
        def new_call(instance, cmd_args, *args, **kwargs):
            cmd_dict = vars(cmd_args)

            if 'config' not in cmd_dict:
                raise AttributeError(
                    f'config not in args for {instance.__class__.__name__}')

            network_schema = schema.NetworkConfigSchema()
            network_config = network_schema.load_json(cmd_dict['config'])
            backup = copy.deepcopy(network_config)

            cmd_dict['network_schema'] = network_schema
            cmd_dict['network_config'] = network_config

            ret = super(instance.__class__, instance).__call__(
                argparse.Namespace(**cmd_dict), *args, **kwargs)

            # Note, an exception in the caller must skip over the save
            if network_config != backup:
                if not self.save:
                    raise RuntimeError('Network config modified by subcommand')

                network_schema.save_json(network_config, cmd_dict["config"])

            return ret


        @functools.wraps(getattr(wrapped, 'build_parser'))
        def new_build_parser(instance, parser):
            add_network_config_arg(parser)
            super(instance.__class__, instance).build_parser(parser)

        cls_update = dict(__call__=new_call,
                          build_parser=new_build_parser)


        new_cls = types.new_class(wrapped.__name__,
                                  bases=inspect.getmro(wrapped),
                                  exec_body=lambda x: x.update(cls_update))

        functools.update_wrapper(new_cls, wrapped, updated=())

        return new_cls


class SubCommandBase(metaclass=ABCMeta):
    """ Base class for sub commands
    """
    sub_command = ''

    @abstractmethod
    def build_parser(self, parser):
        """ Add arguments to the parser for this subcommand
        """

    @abstractmethod
    def __call__(self, args):
        """ Run the subcommand with provided arguments
        """

class CLIBase(SubCommandBase):
    """ Base class for command CLIs
    """
    command_classes = ()

    def __init__(self):
        self.sub_commands = dict()

        for cls in self.command_classes:
            sub_command = getattr(cls, 'sub_command', '')
            if sub_command:
                self.sub_commands[sub_command] = cls()

    def build_parser(self, parser):
        sub_parsers = parser.add_subparsers(help='sub commands', required=True,
                                            dest='sub_command')

        for sub_command in sorted(self.sub_commands):
            obj = self.sub_commands[sub_command]
            cmd_parser = sub_parsers.add_parser(sub_command,
                                                help=obj.__doc__)
            if hasattr(obj, 'build_parser'):
                obj.build_parser(cmd_parser)

    def __call__(self, args):
        ret = self.sub_commands[args.sub_command](args)
        return ret[0] if isinstance(ret, tuple) else ret
