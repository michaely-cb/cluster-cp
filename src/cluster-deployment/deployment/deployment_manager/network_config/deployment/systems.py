"""
Module to add systems
"""
import logging

from deployment_manager.network_config.cli import AddSystem, SetSystemConnections

from .base import (
    DeployIterator,
    Deploy,
    get_obj_name,
    get_obj_nodegroup,
    get_obj_rack,
    get_obj_stamp,
    NetworkConfig,
)
from .command import ToolCommand, ToolDeviceAdder

logger = logging.getLogger(__name__)


class Systems(Deploy):
    """ Command to add system to network_config.json
    """
    name = "Add systems to network config"

    def get_command(self):
        systems = self.cfg.systems
        if not systems:
            logger.info("No systems specified.")
            return None

        fname = self.cfg.network_config_filename
        device_args = list()
        for s in systems:
            system_name = get_obj_name(s)
            rack, rack_unit = get_obj_rack(s)
            stamp = get_obj_stamp(s)
            system_vlan = s.get("systemVlan", False)
            default_hostname = s.get("defaultHostname")
            device_args.append({
                'name': system_name,
                'mgt_addr': self.cfg.get_system_mgmt_addr(s),
                'nodegroup': get_obj_nodegroup(s),
                'rack': str(rack) if rack else None,
                'rack_unit': rack_unit,
                'stamp': stamp,
                'default_hostname': default_hostname,
                'system_vlan': system_vlan
            })

        return ToolDeviceAdder(fname, 'add_system', device_args)


class SystemConnections(Deploy):
    name = "Optionally set system connections from file"

    def get_command(self):
        fname = self.cfg.network_config_filename
        system_conns_file = self.cfg.system_connections_file
        if system_conns_file:
            return ToolCommand(
                        SetSystemConnections,
                        dict(
                            filename=system_conns_file,
                            config=fname
                        )
                    )
        return None
