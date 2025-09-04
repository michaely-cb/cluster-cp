import os

CLUSTER_DEPLOYMENT_PYTHON = os.getenv("PYTHON", "python3")
CLUSTER_DEPLOYMENT_BASE = os.getenv("CLUSTER_DEPLOYMENT_BASE", ".")
CONFIG_BASEPATH = os.getenv("CONFIG_BASEPATH", f"{CLUSTER_DEPLOYMENT_BASE}/meta")
DEPLOYMENT_LOGS = os.getenv("DEPLOYMENT_LOGS", f"{CLUSTER_DEPLOYMENT_BASE}/logs")
DEPLOYMENT_PACKAGES = os.getenv("DEPLOYMENT_PACKAGES", f"{CLUSTER_DEPLOYMENT_BASE}/packages")

# The config file for cscfg 
CSCFG_CONFIG = f"{CLUSTER_DEPLOYMENT_BASE}/cscfg.json"

# Other file locations
CLUSTER_YAML = "/opt/cerebras/cluster/cluster.yaml"
