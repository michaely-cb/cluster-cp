"""
Module dealing with network_config.json creation
"""
from deployment_manager.network_config.cli import NewConfig

from .base import Deploy
from .command import ToolCommand


class NetworkConfig(Deploy):
    """ Command to generate initial network_config.json
    """
    name = "Create network_config.json"

    @staticmethod
    def get_tool_cmd(cfg_filename, cluster_name):
        args = dict(
            name=cluster_name,
            description=None,
            config=cfg_filename
        )
        return ToolCommand(NewConfig, args)

    def get_command(self):
        return self.get_tool_cmd(
            self.cfg.network_config_filename,
            self.cfg.cluster_name
        )
