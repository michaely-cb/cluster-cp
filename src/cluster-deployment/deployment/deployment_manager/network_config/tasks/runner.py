#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

""" Task runner
"""
import abc
import concurrent.futures
from typing import Dict, Any
from deployment_manager.common.constants import MAX_WORKERS

class APITask:
    @abc.abstractmethod
    def object_name(self) -> str:
        pass

    @abc.abstractmethod
    def __call__(self, *args, **kwargs) -> Any:
        pass

    @abc.abstractmethod
    def post(self, call_result: Any) -> Any:
        """ Called with what __call__ returns """
        pass

    @abc.abstractmethod
    def handler(self, exception: Exception) -> Any:
        pass


class TaskRunner:
    """Execute tasks in parallel and post process their results in the
    main thread.
    """

    def __init__(self, task_iter):
        self.task_iter = iter(task_iter)

    def call_api_tasks(self, *args, **kwargs) -> Dict[str, Any]:
        r = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_objs = {}
            for task_obj in self.task_iter:
                assert isinstance(task_obj, APITask), "programmer error: must pass APITasks to call_api_tasks"
                future_obj = executor.submit(task_obj, *args, **kwargs)
                future_objs[future_obj] = task_obj

            for future_obj in concurrent.futures.as_completed(future_objs):
                task_obj = future_objs[future_obj]
                try:
                    r[task_obj.object_name()] = task_obj.post(future_obj.result())
                except concurrent.futures.CancelledError:
                    pass
                except Exception as e:
                    r[task_obj.object_name()] = task_obj.handler(e)
            return r

    def __call__(self, *args, **kwargs):
        future_objs = dict()
        return_vals = list()

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=MAX_WORKERS) as executor:

            for task_obj in self.task_iter:
                future_obj = executor.submit(task_obj, *args, **kwargs)
                future_objs[future_obj] = task_obj

            exit_code = 0
            for future_obj in concurrent.futures.as_completed(future_objs):
                task_obj = future_objs[future_obj]

                try:
                    exception = future_obj.exception()
                    if exception is not None:
                        v = task_obj.handler(exception)
                        if isinstance(v, tuple):
                            if v[0] == False:
                                exit_code = 1
                            return_vals.append(v[1])
                        else:
                            if not v:
                                exit_code = 1
                    else:
                        result = future_obj.result()
                        rtn = task_obj.post(result)

                        # If post return False, it means there are failures
                        if isinstance(rtn, bool) and not rtn:
                            exit_code = 1
                        elif isinstance(rtn, tuple):
                            if rtn[0] == False:
                                exit_code = 1
                            return_vals.append(rtn[1])

                except concurrent.futures.CancelledError:
                    pass

            return exit_code, return_vals
