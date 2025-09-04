from deployment_manager.cli.network.ping import PingCmd
from deployment_manager.cli.network.repair_xconnects import RepairXconnects
from deployment_manager.cli.network.update_network_json import UpdateNetworkConfigJson

CMDS = [RepairXconnects, PingCmd, UpdateNetworkConfigJson]
