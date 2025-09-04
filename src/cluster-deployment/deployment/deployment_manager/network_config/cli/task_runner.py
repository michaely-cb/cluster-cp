#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved

"""
CLI frontend for network configuration tasks
"""

from .base import \
    SubCommandBase, \
    UsesNetworkConfig
from .. import schema, tasks


class _TasksBase(SubCommandBase):
    """ Base class for deployment tasks
    """
    username = ''
    sections = ()

    def build_parser(self, parser):
        parser.add_argument('-u', '--username', default=self.username,
                            help='Username to log in with')

        parser.add_argument('-p', '--password',
                            help='Password to log in with')

        parser.add_argument('-n', '--names', action='append',
                            help='Only run on named objects')

        parser.add_argument('-e', '--exclude', action='append',
                            help='Do not run on named objects')

        parser.add_argument('-b', '--base_dir',
                            help="Base directory for generated template files")

        parser.add_argument('-v', '--verbose', action="store_true",
                            help="Print commands being run")

        sub_parsers = parser.add_subparsers(help='tasks', required=True, dest='task')

        for cls in tasks.TASK_CLASSES:
            for section in getattr(cls, 'compat_sections', ()):
                if section in self.sections:
                    sub_parsers.add_parser(cls.task_alias, help=cls.__doc__)
                    break

    def __call__(self, args):
        if getattr(args, 'sections', None):
            filter_sections = args.sections
        else:
            filter_sections = self.sections

        def filter_fn(section, obj):
            if section not in filter_sections:
                return False

            if getattr(args, 'names', None) and args.names and (obj["name"] not in args.names):
                return False

            if getattr(args, 'exclude', None) and args.exclude and (obj["name"] in args.exclude):
                return False

            return True

        kwargs = dict(network_config=args.network_config,
                      task_alias=args.task,
                      username=getattr(args, 'username', self.username),
                      filter_fn=filter_fn,
                      task_kwargs=dict())

        if getattr(args, 'password', None) and args.password:
            kwargs['password'] = args.password
        if getattr(args, 'base_dir', None) and args.base_dir:
            kwargs['task_kwargs']['base_dir'] = str(args.base_dir)
        if getattr(args, 'verbose', None) and args.verbose:
            kwargs['task_kwargs']['verbose'] = args.verbose

        if getattr(args, "detail", None):
            kwargs['task_kwargs']['detail'] = args.detail
        if getattr(args, "destination_types", None):
            kwargs['task_kwargs']['destination_types'] = args.destination_types
        if getattr(args, "destination_names", None):
            kwargs['task_kwargs']['destination_names'] = args.destination_names
        if getattr(args, "mtu", None):
            kwargs['task_kwargs']['mtu'] = args.mtu

        # switch specific
        if getattr(args, "config_section", None):
            kwargs['task_kwargs']['config_section'] = args.config_section
        if getattr(args, "ports_for", None):
            kwargs['task_kwargs']['ports_for'] = args.ports_for
        if getattr(args, "ignore_interfaces", None):
            kwargs['task_kwargs']['ignore_interfaces'] = args.ignore_interfaces

        generator = tasks.TaskGenerator(**kwargs)

        return tasks.TaskRunner(generator)()


@UsesNetworkConfig(save=True)
class NodeTasks(_TasksBase):
    """ Run node deployment tasks
    """
    sub_command = 'node_tasks'

    username = 'root'
    sections = schema.NetworkConfigSchema.node_sections

    def build_parser(self, parser):
        super().build_parser(parser)
        parser.add_argument('-s', '--sections', action='append',
                            choices=self.sections,
                            help='Only run on these node types')
        parser.add_argument('--destination_types', action="append",
                            help="Specify type(s) of destinations to ping from nodes",
                            choices=["nodes", "systems", "vlans", "gateways"])
        parser.add_argument('--destination_names', action="append",
                            help="Specify name(s) of destinations to ping from nodes")
        parser.add_argument('--detail', action="store_true",
                            help="Run elaborate version of task")
        parser.add_argument('--mtu', default=None,
                            help="MTU value for ping check. Default to None")


@UsesNetworkConfig(save=True)
class SystemTasks(_TasksBase):
    """ Run system deployment tasks
    """
    sub_command = 'system_tasks'

    username = 'admin'
    sections = schema.NetworkConfigSchema.system_sections


@UsesNetworkConfig(save=True)
class SwitchTasks(_TasksBase):
    """ Run node deployment tasks
    """
    sub_command = 'switch_tasks'

    username = 'admin'
    sections = schema.NetworkConfigSchema.switch_sections

    def build_parser(self, parser):
        super().build_parser(parser)
        parser.add_argument('--config_section',
                            help="Choose section of switch config to upload")
        parser.add_argument('--ports_for', action="append",
                            help="Names of nodes/systems/links whose associated switch ports are to be configured")
        parser.add_argument('--destination_types', action="append",
                            help="Specify type(s) of destinations to ping from switches",
                            choices=["nodes", "systems", "xconnects"])
        parser.add_argument('--ignore_interfaces', action="append",
                            help="Specify interface(s) to avoid pinging from switches (device_name:if_name)")
