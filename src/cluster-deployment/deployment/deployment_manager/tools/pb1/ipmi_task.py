from typing import Tuple

from deployment_manager.db import device_props as props
from .device_status import Status
from .device_task import ParallelDeviceTask
from .ipmi import create_ipmi_client, with_ipmi_client
from ...db.models import Device


class ResetIpmi(ParallelDeviceTask):
    """
    Send reset IPMI commands to each server
    """

    def __init__(self, profile: str, parallelism: int = 20):
        super().__init__(profile, parallelism)

    def _run(self, target) -> Tuple:
        device = Device.get_device(target, self._profile)

        try:
            with create_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                ipmi.reset_ipmi()
            return Status.OK, ""
        except Exception as e:
            return Status.FAILED, f"{e}"


class DisableHostCheck(ParallelDeviceTask):
    """ Disable DELL idrac host check """

    def __init__(self, profile: str, parallelism: int = 20):
        super().__init__(profile, parallelism)

    def _run(self, target) -> Tuple:
        device = Device.get_device(target, self._profile)
        if device.get_prop(props.prop_vendor_name) != "DL":
            return Status.OK, ""

        try:
            with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                ipmi.set_host_header_check(False)
                return Status.OK, ""
        except Exception as e:
            return Status.FAILED, f"{e}"


class CreateMetricsUser(ParallelDeviceTask):
    """ Create read-only user for metrics scraping """

    def __init__(self, profile: str, metrics_user: str, metrics_password: str, parallelism: int = 20):
        super().__init__(profile, parallelism)
        self._username = metrics_user
        self._password = metrics_password

    def _run(self, target) -> Tuple:
        device = Device.get_device(target, self._profile)
        try:
            with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                ipmi.create_or_update_user(self._username, self._password, role="ReadOnly")
                return Status.OK, ""
        except Exception as e:
            return Status.FAILED, f"{e}"


class InitializePxeBoot(ParallelDeviceTask):
    """
    Initialize PXE boot
    """

    def __init__(self, profile: str, parallelism: int = 20):
        super().__init__(profile, parallelism)

    def _run(self, target) -> Tuple:
        device = Device.get_device(target, self._profile)

        try:
            with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                ipmi.onetimeboot_from_pxe()
            return Status.OK, ""
        except Exception as e:
            return Status.FAILED, f"{e}"
