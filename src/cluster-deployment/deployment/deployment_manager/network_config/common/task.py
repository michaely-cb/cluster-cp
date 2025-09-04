import abc
import concurrent.futures
import logging
import typing
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from deployment_manager.network_config.common.context import OBJ_TYPE_SR, OBJ_TYPE_SW, OBJ_TYPE_SY, NetworkCfgCtx

logger = logging.getLogger(__name__)

MAX_WORKERS=128

@dataclass
class NetworkTaskResult:
    ok: bool
    data: Any = None
    message: str = ""

    @classmethod
    def ok(cls, data=None) -> 'NetworkTaskResult':
        return cls(True, data=data)

    @classmethod
    def error(cls, message: str):
        return cls(False, data=None, message=message)


class TargetNodes:
    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_SR

class TargetSwitches:
    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_SW

class TargetSystems:
    @classmethod
    def get_obj_type(cls):
        return OBJ_TYPE_SY


class NetworkTask:
    def __init__(self, ctx: NetworkCfgCtx, name: str, *args, **kwargs):
        self._ctx = ctx
        self.name = name

    @classmethod
    @abc.abstractmethod
    def get_obj_type(cls) -> str:
        """ Return the object type this task is for: SR|SW|SY """
        pass

    @abc.abstractmethod
    def run(self, *args, **kwargs) -> NetworkTaskResult:
        """ Execute some remote task. Should not modify shared state. """
        pass

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        """ Called with what run() returns in the OK case. The NetworkTaskExecutor should run these serially """
        return result

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        """ Optional recover method to handle when run() returns an error """
        return result


class ConfigTask(ABC):
    """
    A Task that is run serially, i.e. the run() method is called on the main thread.
    This is useful for tasks that need to update shared state like the network config.

    Subclasses generally should not override process() but MAY override recover() to handle errors, though it seems
    unlikely that a serial task would need to do so.
    """
    def __init__(self, ctx: NetworkCfgCtx, *args, **kwargs):
        self._ctx = ctx

    @abc.abstractmethod
    def run(self, *args, **kwargs) -> NetworkTaskResult:
        """ Modify the network config in some way. """
        pass


def generate_tasks_for_names(ctx: NetworkCfgCtx, cls: typing.Type[NetworkTask], names: typing.List[str], *args, **kwargs) -> typing.List[NetworkTask]:
    """ Generate tasks for the given task type filtered by the list of given names and enqueue them. """
    targets = set(ctx.network_config.get_names_by_type(cls.get_obj_type())).intersection(set(names))
    return [cls(ctx, name, *args, **kwargs) for name in targets]

def generate_tasks_for_entity_type(ctx: NetworkCfgCtx, cls: typing.Type[NetworkTask], *args, **kwargs) -> typing.List[NetworkTask]:
    """ Generate tasks for the given task's target object type and enqueue them """
    return [cls(ctx, name, *args, **kwargs) for name in ctx.network_config.get_names_by_type(cls.get_obj_type())]

def run_tasks(tasks: typing.List[NetworkTask], threads=MAX_WORKERS) -> typing.Dict[str, NetworkTaskResult]:
    """
    Run tasks in parallel using a threadpool. If a task throws an exception, the task's recover() method is called,
    otherwise the task's process() method is called. Note that process() and recover() are run on the main thread
    to update shared state like the network config.

    Returns:
        dict of device name to NetworkTaskResult
    """
    futures = {}
    with ThreadPoolExecutor(threads) as tp:
        for task in tasks:
            futures[tp.submit(task.run)] = task

    results = {}
    for f in concurrent.futures.as_completed(futures.keys()):
        task = futures[f]

        def _recover(run_result) -> NetworkTaskResult:
            try:
                return task.recover(run_result)
            except Exception as e:
                final_result = NetworkTaskResult.error(f"{task.__class__.__name__}:recover() raised exception: {e}")
                final_result.data = e
                return final_result

        try:
            run_result = f.result()
            if not run_result.ok:
                result = _recover(run_result)
            else:
                try:
                    result = task.process(run_result)
                except Exception as e:
                    result = NetworkTaskResult.error("unhandled exception in process()")
                    result.data = e
        except Exception as e:
            run_result = NetworkTaskResult.error(f"{task.__class__.__name__}:run() raised exception: {e}")
            run_result.data = e
            result = _recover(run_result)

        results[task.name] = result
        if not result.ok and isinstance(result.data, Exception):
            logger.error(f"task {task.name}", exc_info=result.data)
    return results

def run_task(task: NetworkTask) -> NetworkTaskResult:
    results = run_tasks([task])
    return results[task.name]

def summarize_task_result_errors(results: typing.Dict[str, NetworkTaskResult]) -> str:
    """
    Summarize the errors from a task result dict. Returns a string of errors or an empty string if there are no errors.
    """
    failed = [f"{k}: {v.message}" for k ,v in results.items() if not v.ok]
    if not failed:
        return ""
    return ", ".join(failed)


class ConfigUpdateTask:
    def __init__(self, ctx: NetworkCfgCtx, *args, **kwargs):
        self._ctx = ctx

    @abc.abstractmethod
    def run(self, *args, **kwargs) -> NetworkTaskResult:
        """ Execute some remote task. Should not modify shared state. """
        pass