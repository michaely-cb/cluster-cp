from enum import Enum

class UpdateStatus(Enum):
    """ Enums for the update status of a server or system """
    OK = 0
    FAILED = 1
    UNREACHABLE = 2
