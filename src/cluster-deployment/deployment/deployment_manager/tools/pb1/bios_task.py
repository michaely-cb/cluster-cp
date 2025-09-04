import logging
import time
from typing import Tuple

from .bios_settings import get_bios_settings
from .device_status import Status
from .device_task import ParallelDeviceTask
from .ipmi import with_ipmi_client
from ...db.models import Device
from ...db.models import props

logger = logging.getLogger(__name__)



class UpdateBiosSettings(ParallelDeviceTask):
    """
    Update BIOS for each server
    """

    def __init__(self, profile: str, parallelism: int = 20, retry: int = 3):
        super().__init__(profile, parallelism)
        self._retry = retry

    def _run(self, target) -> Tuple[Status, str]:
        device = Device.get_device(target, self._profile)
        role = device.device_role.lower()

        with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
            err = ""
            for i in range(0, self._retry):
                time.sleep(10)
                try:
                    bios_settings = get_bios_settings(role, ipmi)
                    time.sleep(1)
                    ipmi.update_bios_config_privilege(device.get_prop(props.prop_ipmi_credentials_user))
                    time.sleep(1)
                    ipmi.update_bios_settings(bios_settings)
                    time.sleep(1)
                    return Status.OK, f"{i} retries"
                except Exception as e:
                    err += f"{e}"
                    continue
            return Status.FAILED, f"failed after {i} retries. Error: {err}"


class WaitForBiosSettingsUpdate(ParallelDeviceTask):
    """
    Wait for BIOS settings update completion.

    Seen this take as long as 13 minutes on Dell. Set default wait to 15 for some additional grace
    """

    def __init__(self, profile: str, parallelism: int = 20, timeout: int = 900):
        super().__init__(profile, parallelism)
        self._timeout = timeout

    def _run(self, target) -> Tuple:
        device = Device.get_device(target, self._profile)

        try:
            with with_ipmi_client(*device.get_ipmi_client_args()) as ipmi:
                try:
                    ipmi.wait_for_bios_settings_update(timeout=self._timeout)
                    return Status.OK, ""
                except Exception as e:
                    msg = f"failed to wait for BIOS update: {e.__class__.__name__}:{e}"
                    logger.exception(msg)
                    return Status.FAILED, msg
        except Exception as e:
            return Status.FAILED, f"failed to create IPMI client: {e}"
