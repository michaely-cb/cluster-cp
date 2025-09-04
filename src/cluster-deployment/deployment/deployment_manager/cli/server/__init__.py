from deployment_manager.cli.server.configure_dcqcn import ConfigureDcqcn
from deployment_manager.cli.server.server_health import ServerHealth
from deployment_manager.cli.server.update_mac_address import UpdateMacAddress
from deployment_manager.cli.server.ipmi import IpmiAction


CMDS = [
    ConfigureDcqcn,
    ServerHealth,
    UpdateMacAddress,
    IpmiAction
]
