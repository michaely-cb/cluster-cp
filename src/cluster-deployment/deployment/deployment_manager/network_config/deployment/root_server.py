"""
Root server of the cluster
"""
import logging

from deployment_manager.network_config.cli import AddRootServer
from .base import Deploy
from .command import ToolCommand

logger = logging.getLogger(__name__)

class RootServer(Deploy):
    """ Add information about the root server
    """
    name = "Add root server information"

    def get_command(self):
        if self.cfg.network is None:
            logger.info("No network configuration found.")
            return None

        args = dict(
            name=self.cfg.root_server,
            ip_address=self.cfg.root_server_ip,
            config=self.cfg.network_config_filename
        )

        return ToolCommand(AddRootServer, args)
