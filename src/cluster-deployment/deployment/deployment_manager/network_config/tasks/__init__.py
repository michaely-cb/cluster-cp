#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""Task infrastructure

A task is a thing to do (ex: "feeding Vaal", or "ssh into node X and
execute command Y") represented by a set of callable objects.

Tasks are submitted in parallel to a thread pool. Any returned results
or exceptions are handled by a post or handle call bundled with the task.

A run of tasks do not need to be homogeneous, but the typical use case
is to launch a task across a collection of nodes.

"""

from .node import \
    NodePrintingTask
from .runner import TaskRunner
from .ssh_tasks import \
    SSHLoginConnector, \
    SSHPrintPost, \
    SSHPrintHandler, \
    SSHCommandBase, \
    SSHCommand, \
    SSHBatchShellBase, \
    SSHBatchShellCommands, \
    SSHBatchShellFile, \
    SSHUploadBase, \
    SSHCommandTaskBase, \
    MakePrintingSSHTask, \
    SSHTaskGeneratorBase
from .switch import \
    SwitchSessionBase, \
    SwitchSession, \
    SwitchPrintingTask, \
    SwitchVersionTask, \
    SwitchReloadTask, \
    SwitchShowUptimeTask, \
    SwitchShowRunningConfigTask, \
    SwitchLLDPTask, \
    SwitchModelTask, \
    SwitchUploadConfigTask, \
    SwitchPingCheckTask, \
    SwitchRoceStatusCommand, \
    SwitchRoceStatusPost, \
    SwitchRoceStatusTask, \
    SwitchASNCommand, \
    SwitchASNPost, \
    SwitchASNTask, \
    SwitchEnableRoceCommand, \
    SwitchEnableRocePost, \
    SwitchEnableRoceTask, \
    SwitchFirmwareVersionTask
from .task import Task

TASK_CLASSES = tuple(
    [ cls for cls in globals().values() if getattr(cls, 'task_alias', '') ])

# The task generator needs access to all classes
class TaskGenerator(SSHTaskGeneratorBase):
    """ Task generator for tasks in this package
    """
    task_classes = TASK_CLASSES

__all__ = [
    'Task',

    'TaskRunner',

    'SSHLoginConnector',
    'SSHPrintPost',
    'SSHPrintHandler',
    'SSHCommandBase',
    'SSHCommand',
    'SSHBatchShellBase',
    'SSHBatchShellCommands',
    'SSHBatchShellFile',
    'SSHUploadBase',
    'SSHCommandTaskBase',
    'MakePrintingSSHTask',
    'SSHTaskGeneratorBase',

    'NodePrintingTask',

    'SwitchSessionBase',
    'SwitchSession',
    'SwitchPrintingTask',
    'SwitchVersionTask',
    'SwitchReloadTask',
    'SwitchShowUptimeTask',
    'SwitchShowRunningConfigTask',
    'SwitchLLDPTask',
    'SwitchModelTask',
    'SwitchUploadConfigTask',
    'SwitchPingCheckTask',
    'SwitchRoceStatusCommand',
    'SwitchRoceStatusPost',
    'SwitchRoceStatusTask',
    'SwitchFirmwareVersionTask',
    'SwitchASNCommand',
    'SwitchASNPost',
    'SwitchASNTask',
    'SwitchEnableRoceCommand',
    'SwitchEnableRocePost',
    'SwitchEnableRoceTask',

    'TASK_CLASSES',
    'TaskGenerator'
]
