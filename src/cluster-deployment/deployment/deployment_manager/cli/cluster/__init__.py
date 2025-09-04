from deployment_manager.cli.cluster.commands import ClusterDeviceCmd, ClusterShow, ClusterAdd, ClusterRemove, ClusterEdit
from deployment_manager.cli.cluster.upgrade import ClusterUpgradeCmd

CMDS = [ClusterShow, ClusterAdd, ClusterRemove, ClusterEdit, ClusterDeviceCmd, ClusterUpgradeCmd]
