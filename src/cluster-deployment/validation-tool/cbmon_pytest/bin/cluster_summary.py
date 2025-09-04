import os, sys

# Get the current directory (tests directory), and then go up a level to include the "lib" directory
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
lib_dir = os.path.join(project_dir, 'lib')

sys.path.append(lib_dir)
from cluster_info import ClusterInfo

# TODO: Remove config_file hardcoding
config_file = '/net/rushins-dev/srv/nfs/rushins-data/ws/cluster-deployment/validation-tool/cbmon_test/tmp/master_config.yaml'
if 'MASTER_CONFIG_YAML' in os.environ:
    if os.path.exists(os.environ['MASTER_CONFIG_YAML']):
        config_file = os.environ['MASTER_CONFIG_YAML']
        print(f"Using master_config.yaml as {config_file}")

ClusterInfoObj = ClusterInfo(config_file=config_file)

print(f"Top-Level Details for Cluster: {ClusterInfoObj.get_system_name()} ")
print("Details of types/count of hosts:")
print(ClusterInfoObj.get_cluster_info_summary_as_table())
print("\nDetails of each host:")
print(ClusterInfoObj.get_cluster_info_detailed_as_table())