#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

""" Basic task templates
"""

from abc import ABCMeta, abstractmethod

class _TaskBase(metaclass=ABCMeta):
    """ Abstract base class for task objects
    """

    @abstractmethod
    def __call__(self):
        """ Call to execute the task
        """

    @abstractmethod
    def post(self, result):
        """ Called with the result of a task
        """

    @abstractmethod
    def handler(self, exception):
        """Called with any exception raised by the task

        Only exceptions from the call are handled here, not exceptions
        from the post call.
        """


class Task(_TaskBase):
    """ Simple task class
    """
    def __init__(self, task_call, post, handler):
        for callback in (task_call, post, handler):
            if not callable(callback):
                raise ValueError(f'Not callable: {type(callback).__name__}')

        self._task_call = task_call
        self._post = post
        self._handler = handler

    def __call__(self, *args, **kwargs):
        return self._task_call(*args, **kwargs)

    def post(self, result):
        return self._post(result)

    def handler(self, exception):
        return self._handler(exception)
