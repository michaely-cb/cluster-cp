from deployment_manager.cli.system.assign_ip import AssignIPs
from deployment_manager.cli.system.password import Password
from deployment_manager.cli.system.status import Status
from deployment_manager.cli.system.system_health import SystemHealth
from deployment_manager.flow.flow import SetSystemDcqcnFlow

CMDS = [AssignIPs, Status, SystemHealth, SetSystemDcqcnFlow, Password]
