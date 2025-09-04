import os
import sys

#####################################################
# Global variables
#####################################################

# Get the current directory (tests directory), and then go up a level to include the "lib" directory
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
lib_dir = os.path.join(project_dir, 'lib')

sys.path.append(lib_dir)
from cluster_info import ClusterInfo

config_file = None
if 'MASTER_CONFIG_YAML' in os.environ:
    if os.path.exists(os.environ['MASTER_CONFIG_YAML']):
        config_file = os.environ['MASTER_CONFIG_YAML']
        print(f"Using master_config.yaml as {config_file}")
        
if config_file:
    ClusterInfoObj = ClusterInfo(config_file=config_file)
else:
    print("[ERROR]: MASTER_CONFIG_YAML is either not defined or the file does not exist.\nPlease check the file path and permissions.")
    sys.exit(1)
