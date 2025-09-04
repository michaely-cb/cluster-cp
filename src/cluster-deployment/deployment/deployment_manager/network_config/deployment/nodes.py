"""
Commands to add nodes to network_config.json
"""
import logging

from .base import (
    Deploy,
    DeploymentConfig,
    get_obj_name,
    get_obj_credentials,
    get_obj_nodegroup,
    get_obj_rack,
    get_obj_stamp,
)
from .command import ToolDeviceAdder
from ..schema.utils import NODE_ROLE_SCHEMA_MAPPING

logger = logging.getLogger(__name__)

class Nodes(Deploy):
    """ Class to add nodes
    """
    name = "Add nodes to network config"

    def __init__(self, deployment_config: DeploymentConfig):
        super().__init__(deployment_config)
        self.nodes = self.cfg.nodes

    @staticmethod
    def get_node_section(node):
        return NODE_ROLE_SCHEMA_MAPPING.get(node['role'])

    def get_command(self):
        fname = self.cfg.network_config_filename
        if not self.nodes:
            logger.info(f"No nodes to be deployed")
            return None
        device_args = list()
        for n in self.nodes:
            node_name = get_obj_name(n)
            creds = get_obj_credentials(n)
            rack, rack_unit = get_obj_rack(n)
            stamp = get_obj_stamp(n)
            node_section = self.get_node_section(n)
            if node_section is None:
                logger.error(f"Invalid node role {n['role']} for {node_name}")
                continue
            device_args.append({
                'node_section': node_section,
                'name': node_name,
                'mgt_addr': None,
                'nodegroup': get_obj_nodegroup(n),
                'username': creds['username'],
                'password': creds['password'],
                'rack': str(rack) if rack else None,
                'rack_unit': rack_unit,
                'stamp': stamp
            })
        return ToolDeviceAdder(fname, 'add_node', device_args)
