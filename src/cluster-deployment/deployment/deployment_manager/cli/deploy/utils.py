import dataclasses
import datetime
import logging
import pytz
import time
from typing import List, Optional

from deployment_manager.db.models import (
    Device,
)
from deployment_manager.tools.utils import ReprFmtMixin, to_duration

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DeviceDeployRepr(ReprFmtMixin):
    name: str
    type: str
    role: str
    stage: str
    status: str
    update_time: Optional[datetime.datetime]
    details: str

    def to_table_row(self) -> List[str]:
        details = self.details if len(self.details) <= 60 else self.details[:57] + "..."
        last_update = "Unknown" if not self.update_time else to_duration(time.time() - self.update_time.timestamp())
        return [self.name, self.type, self.role, self.stage, self.status, last_update, details]

    @classmethod
    def table_header(cls) -> List[str]:
        return ["Device", "Type", "Role", "Stage", "Status", "LastUpdate", "Comment"]

    @classmethod
    def csv_header(cls) -> list:
        return cls.table_header()

    def to_csv_row(self) -> list:
        return [self.name, self.type, self.role, self.stage, self.status, self.update_time, self.details]

    def to_dict(self) -> dict:
        d = dict(self.__dict__)
        if "sort_key" in d:
            del d["sort_key"]
        if d.get("update_time"):
            d["update_time"] = d["update_time"].astimezone(pytz.UTC).isoformat()
        return d

    @staticmethod
    def from_device(d: Device) -> 'DeviceDeployRepr':
        return DeviceDeployRepr(
            d.name,
            d.device_type,
            d.device_role,
            d.deployment_stage_str,
            d.deployment_status_str,
            d.deployment_update_time,
            d.deployment_msg
        )
