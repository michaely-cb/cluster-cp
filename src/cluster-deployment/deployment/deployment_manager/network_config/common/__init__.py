from collections import defaultdict
from enum import Enum

class SystemModels(Enum):
    CS3 = "cs3"
    CS4 = "cs4"

# We should really be getting this from the database
SYSTEM_INTERFACE_COUNT = 12
SY_MAX_INTERFACE_COUNT = 24
MAX_CLUSTERS = 2
NUM_EXPECTED_CS_PORTS = defaultdict(lambda: SYSTEM_INTERFACE_COUNT)
NUM_EXPECTED_CS_PORTS[SystemModels.CS4.value] = SY_MAX_INTERFACE_COUNT

