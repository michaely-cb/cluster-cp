FUNCTION_TYPES_PAIR = [
    ("AX", "act"),
    ("CN", "console"),
    ("CS", "cs"),
    ("IN", "infra"),
    ("IX", "inferencedriver"),
    ("LF", "leaf"),
    ("MA", "aggr"),
    ("MC", "mediaconverter"),
    ("MG", "mgmt"),
    ("MX", "memx"),
    ("SP", "spine"),
    ("SX", "swarmx"),
    ("US", "user"),
    ("WK", "worker"),
]

FUNCTION_TYPES = {item[0] for item in FUNCTION_TYPES_PAIR}
FUNCTION_TYPES_LONG = {item[1] for item in FUNCTION_TYPES_PAIR}
FUNCTION_TYPES_LONG2SHORT = dict(
    [(item[1], item[0]) for item in FUNCTION_TYPES_PAIR]
)

DEVICE_TYPES_PAIR = [("system", "SY"), ("switch", "SW"), ("server", "SR")]

DEVICE_TYPES = {item[0] for item in DEVICE_TYPES_PAIR}
DEVICE_TYPES_SHORT = {item[1] for item in DEVICE_TYPES_PAIR}
TYPE_TO_SHORT = dict(DEVICE_TYPES_PAIR)
TYPE_TO_LONG = dict([(item[1], item[0]) for item in DEVICE_TYPES_PAIR])


LABEL_PREFIX = "CS"
