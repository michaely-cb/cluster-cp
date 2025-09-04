from typing import Callable, Tuple, List

from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device
from deployment_manager.tools.pb1.device_status import Status
from deployment_manager.tools.pb1.device_task import ParallelDeviceTask
from deployment_manager.tools.system import SystemCtl, change_system_password

_SYSTEM_PARALLELISM = 32


class _ParallelSysCtlTask(ParallelDeviceTask):
    def __init__(self, profile: str, ctl_generator: Callable[[str], SystemCtl], *sysctl_args):
        super().__init__(profile, parallelism=_SYSTEM_PARALLELISM)
        self._ctl_generator = ctl_generator
        self._args = sysctl_args

    def _get_ctl_method(self, ctl: SystemCtl) -> Callable:
        """Subclasses must override this to return the appropriate method from ctl."""
        raise NotImplementedError("Subclasses must implement _get_ctl_method")

    def _run(self, target: str) -> Tuple[Status, str]:
        ctl = self._ctl_generator(target)
        self._get_ctl_method(ctl)(*self._args)
        return Status.OK, ""


class SystemConfigUpdateTask(_ParallelSysCtlTask):
    """ Set global system configs. Arg is dict of system configs """

    def _get_ctl_method(self, ctl: SystemCtl):
        return ctl.update_syscfg


class SystemImageUpdateTask(_ParallelSysCtlTask):
    """ Update system image. Arg is string path to system image """

    def _get_ctl_method(self, ctl: SystemCtl):
        return ctl.update_image


class SystemActivateTask(_ParallelSysCtlTask):
    """ Activate the system. No args """

    def _get_ctl_method(self, ctl: SystemCtl):
        return ctl.activate


class SystemPasswordTask(ParallelDeviceTask):
    """ Use the root user to change some user on the system's password """

    def __init__(
            self,
            profile: str,
            try_root_pw: List[str],
            target_user: str,
            target_password: str,
    ):
        super().__init__(profile, _SYSTEM_PARALLELISM)
        self._u, self._p = target_user, target_password
        self._args = [try_root_pw, target_user, target_password]

    def _run(self, target) -> Tuple[Status, str]:
        rv = change_system_password(target, "root", *self._args)

        if rv.status == "error":
            return Status.FAILED, rv.messsage

        d = Device.get_device(target, self._profile)
        username = d.get_prop(props.prop_management_credentials_user)
        if username == self._u:
            d.set_prop(props.prop_management_credentials_password, self._p)
        return Status.OK, rv.messsage

    def __str__(self):
        return f"{self.__class__.__name__}[{self._u}]"