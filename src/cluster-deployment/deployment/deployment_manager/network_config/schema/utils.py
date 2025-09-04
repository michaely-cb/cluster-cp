"""
Schema related utility functions
"""

NODE_ROLE_SCHEMA_MAPPING = {
    "MG": "management_nodes",
    "US": "user_nodes",
    "WK": "worker_nodes",
    "MX": "memoryx_nodes",
    "SX": "swarmx_nodes",
    "AX": "activation_nodes",
    "IX": "inferencedriver_nodes",
    "CS": "systems",
}

DEFAULT_NODE_ROLE_IF_COUNT = {
    "MX": 2,
    "AX": 2,
    "IX": 2,
    "SX": 6,
    "MG": 1,
    "US": 1,
    "WK": 1,
    "CS": 24,
}

def is_spine_switch(switch_obj):
    return switch_obj.get("tier") == "SP"
