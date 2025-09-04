from dataclasses import dataclass
from enum import Enum


class Status(Enum):
    OK = "OK"
    FAILED = "FAILED"


@dataclass()
class DeviceStatus:
    """ Class for tracking device status and extra info"""
    status: Status
    message: str
