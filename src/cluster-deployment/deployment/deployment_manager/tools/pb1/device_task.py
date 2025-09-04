import abc
import dataclasses
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple

from tabulate import tabulate

from deployment_manager.tools.utils import countdown
from .device_status import DeviceStatus, Status

logger = logging.getLogger(__name__)



@dataclasses.dataclass
class DeviceInfo:
    ipmi_ip: str
    ipmi_dns: str
    ipmi_user: str
    ipmi_password: str
    device_role: str


class DeviceTask(abc.ABC):
    """
    Task that requires device info.
    """
    def __init__(self, profile: str):
        self._profile = profile

    @classmethod
    def name(cls):
        return cls.__name__

    @abc.abstractmethod
    def run(self, targets: List[str]) -> Dict[str, DeviceStatus]:
        raise NotImplementedError

    def __str__(self) -> str:
        return self.name()


class WaitTask(DeviceTask):
    def __init__(self, profile: str, wait_time: int):
        super().__init__(profile)
        self._wait_time = wait_time

    def run(self, targets: List[str]) -> Dict:
        if len(targets) == 0:
            return {}
        countdown(self._wait_time)
        result = {target: DeviceStatus(Status.OK, "") for target in targets}
        return result

    def __str__(self) -> str:
        return f"{super().__str__()} wait_time:{self._wait_time}"


class SerialTask(DeviceTask):
    """
    Run task sequentially
    """

    def __init__(self, profile: str, tasks: List[DeviceTask]):
        super().__init__(profile)
        self._tasks = tasks

    def run(self, targets: List[str]) -> Dict[str, DeviceStatus]:
        final_result = {}

        current_targets = targets
        for task in self._tasks:
            target_names = ','.join(current_targets)
            target_names = target_names if len(target_names) < 30 else target_names[:27] + "..."
            logger.info(
                f"Running {task} with {len(current_targets)} targets ({target_names})"
            )
            current_result = task.run(current_targets)

            # rebuild targets for next task
            current_targets = []
            rows = []
            for device, status in current_result.items():
                if status.status == Status.FAILED:
                    final_result[device] = status
                    rows.append([device, str(status.status), status.message])
                else:
                    final_result[device] = status
                    current_targets.append(device)
            header = ["device", "status", "message"]
            logger.info("\n" + tabulate(rows, header))

            # Check if all targets have failed
            if targets and not current_targets:
                logger.error("All targets have failed, not proceeding with further tasks.")
                break

        return final_result


class ParallelDeviceTask(DeviceTask):
    """
    Parallel device tasks
    """

    def __init__(self, profile: str, parallelism: int = 1):
        super().__init__(profile)
        self._parallelism = parallelism

    def run(self, targets: List[str]) -> Dict[str, DeviceStatus]:
        """
        Args:
            targets (List[str]): run task over targets
        """
        result = {}

        with ThreadPoolExecutor(max_workers=self._parallelism) as executor:
            future_to_device = {
                executor.submit(self._run, tgt): tgt for tgt in targets
            }
            for f, device in future_to_device.items():
                try:
                    status, msg = f.result()
                    dev_status = DeviceStatus(status, msg)
                    result[device] = dev_status
                except Exception as e:
                    result[device] = DeviceStatus(Status.FAILED, f"subtask {self.name()} failed: {e}")
        return result

    @abc.abstractmethod
    def _run(self, target) -> Tuple[Status, str]:
        """Single thread run"""
        pass
