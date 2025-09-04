import abc
import dataclasses
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import List, Dict, Optional

from deployment_manager.tools.utils import ReprFmtMixin

logger = logging.getLogger(__name__)


class FlowStatus(Enum):
    Progressing = "progressing"
    Fail = "fail"
    Success = "success"


@dataclasses.dataclass
class FlowDeviceState(ReprFmtMixin):
    name: str
    step: str
    status: FlowStatus
    message: str

    @classmethod
    def table_header(cls) -> list:
        return ["name", "last_step", "status", "message"]

    def to_table_row(self) -> list:
        return [self.name, self.step, self.status, self.message]


class FlowContext:
    def __init__(self, profile: str):
        self.profile = profile
        self.device_state: Dict[str, FlowDeviceState] = {}

    def is_progressing(self, name: str) -> bool:
        if name not in self.device_state:
            return False
        return self.device_state[name].status == FlowStatus.Progressing

    def get_progressing_devices(self) -> List[str]:
        return [d.name for d in self.device_state.values() if d.status == FlowStatus.Progressing]

    def update_state(self, device_name: str, task_name: str, status: FlowStatus, msg: str = ""):
        old_state = self.device_state.get(device_name).step if device_name in self.device_state else "init"
        self.device_state[device_name] = FlowDeviceState(device_name, task_name, status, msg)
        logger.debug(f"{device_name} {old_state} -> {task_name}[{status}] {msg}")


class Task(abc.ABC):
    """ Tasks are idempotent, user-friendly chunks of work """

    @classmethod
    def human_name(cls):
        cls_name = cls.__name__
        if cls_name.endswith("Task"):
            return cls_name[:-4]
        return cls_name

    @property
    def name(self):
        return self.human_name()

    @abc.abstractmethod
    def run(self) -> Optional[bool]:
        pass

    def __str__(self):
        return self.name


class ParallelTask(Task):
    def __init__(self, tasks: List[Task], name=None, parallelism=100):
        self._tasks = tasks
        self._name = "ParallelTask" if not name else name
        self._parallelism = parallelism

    @property
    def name(self):
        return self._name

    def run(self):
        failed_tasks = {}
        with ThreadPoolExecutor(max_workers=self._parallelism) as executor:
            futures = [executor.submit(t.run) for t in self._tasks]
            for i, f in enumerate(futures):
                try:
                    f.result()
                except Exception as e:
                    logger.error(f"parallel task failed {self._tasks[i]}:")
                    traceback.print_exc()
                    failed_tasks[i] = e
        if failed_tasks:
            raise next(iter(failed_tasks.values()))

    def __str__(self):
        return f"{self.name}[{','.join([str(t) for t in self._tasks])}]"
