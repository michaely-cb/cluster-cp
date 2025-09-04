"""
Add switches to network_config.json
"""
import logging

from deployment_manager.network_config.cli import AddExteriorSwitch, AddSwitch, AddExteriorConnections

from .base import (
    Deploy, DeploymentConfig,
    DeployIterator,
    get_obj_name,
    get_obj_credentials,
    get_obj_rack,
    get_obj_stamp
)
from .command import ToolCommand, ToolDeviceAdder

logger = logging.getLogger(__name__)


class ExteriorSwitches(DeployIterator):
    """ Add exterior switch information
    """
    name = "Add exterior switch information"

    def get_commands(self):
        ext_switches = self.cfg.exterior_switches
        cmds = []
        if ext_switches is None:
            logger.info("No exterior switches specified.")
            return cmds

        network_cfg = self.network_config
        for es in ext_switches:
            es_name = get_obj_name(es)
            cmds.append(ToolCommand(
                AddExteriorSwitch,
                dict(
                    name=es_name,
                    vendor=DeploymentConfig.get_switch_vendor(es),
                    mtu=es['mtu'],
                    asn=es.get('asn'),
                    config=self.cfg.network_config_filename
                )
            ))
        return cmds


class AWSwitches(Deploy):
    """ Add memx switch information
    """
    name = "Add memx switch information"

    @staticmethod
    def get_device_args(
        cfg_filename, switch_name, vendor,
        model=None, username=None, password=None,
        rack=None, rack_unit=None, stamp=None,
        prefix=None, system_prefix=None):
        return {
            'name': switch_name,
            'mgt_addr': None,
            'vendor': vendor,
            'model': model,
            'tier': 'AW',
            'tier_pos': 0,
            'username': username,
            'password': password,
            'rack': str(rack) if rack else None,
            'rack_unit': rack_unit,
            'stamp': stamp,
            'prefix': prefix,
            'system_prefix': system_prefix
        }

    def get_command(self):
        aw_switches = self.cfg.aw_switches
        if not aw_switches:
            logger.info("No AW switches specified.")
            return None

        device_args = list()
        for s in aw_switches:
            switch_name = get_obj_name(s)
            creds = get_obj_credentials(s)
            rack, rack_unit = get_obj_rack(s)
            stamp = get_obj_stamp(s)
            prefix = self.cfg.get_switch_ip_block(rack)
            system_prefix = self.cfg.get_system_switch_ip_block(rack)
            device_args.append(
                self.get_device_args(
                    self.cfg.network_config_filename, switch_name,
                    DeploymentConfig.get_switch_vendor(s),
                    DeploymentConfig.get_switch_model(s),
                    creds['username'], creds['password'],
                    rack, rack_unit, stamp, prefix, system_prefix
                )
            )
        return ToolDeviceAdder(self.cfg.network_config_filename, 'add_switch', device_args)


class BRSwitches(Deploy):
    """ Add swarmx switch information
    """
    name = "Add swarmx switch information"

    def get_command(self):
        br_switches = self.cfg.br_switches
        if not br_switches:
            logger.info("No BR switches specified.")
            return None

        network_cfg = self.network_config
        device_args = list()
        for s in br_switches:
            switch_name = get_obj_name(s)
            creds = get_obj_credentials(s)
            rack, rack_unit = get_obj_rack(s)
            stamp = get_obj_stamp(s)
            prefix = self.cfg.get_switch_ip_block(rack)
            system_prefix = self.cfg.get_system_switch_ip_block(rack)
            device_args.append({
                'name': switch_name,
                'mgt_addr': None,
                'vendor': DeploymentConfig.get_switch_vendor(s),
                'model': DeploymentConfig.get_switch_model(s),
                'tier': 'BR',
                'tier_pos': s.get('system_port_group', 0),
                'username': creds['username'],
                'password': creds['password'],
                'rack': str(rack) if rack else None,
                'rack_unit': rack_unit,
                'stamp': stamp,
                'prefix': prefix,
                'system_prefix': system_prefix,
                'swarmx_prefix': None
            })
        return ToolDeviceAdder(self.cfg.network_config_filename, 'add_switch', device_args)


class LeafSwitches(Deploy):
    """ Add leaf switch information
    """
    name = "Add leaf switch information"

    def get_command(self):
        leaf_switches = self.cfg.leaf_switches
        if not leaf_switches:
            logger.info("No leaf switches specified.")
            return None

        network_cfg = self.network_config
        device_args = list()
        for s in leaf_switches:
            switch_name = get_obj_name(s)
            creds = get_obj_credentials(s)
            rack, rack_unit = get_obj_rack(s)
            stamp = get_obj_stamp(s)
            prefix = self.cfg.get_switch_ip_block(rack)
            system_prefix = self.cfg.get_system_switch_ip_block(rack)
            swarmx_prefix = self.cfg.get_swarmx_switch_ip_block(rack)
            device_args.append({
                'name': switch_name,
                'mgt_addr': None,
                'vendor': DeploymentConfig.get_switch_vendor(s),
                'model': DeploymentConfig.get_switch_model(s),
                'tier': 'LF',
                'tier_pos': 0,
                'username': creds['username'],
                'password': creds['password'],
                'rack': str(rack) if rack else None,
                'rack_unit': rack_unit,
                'stamp': stamp,
                'prefix': prefix,
                'system_prefix': system_prefix,
                'swarmx_prefix': swarmx_prefix
            })
        return ToolDeviceAdder(self.cfg.network_config_filename, 'add_switch', device_args)


class SpineSwitches(Deploy):
    """ Add spine switch information
    """
    name = "Add spine switch information"

    def get_command(self):
        spine_switches = self.cfg.spine_switches
        if not spine_switches:
            logger.info("No spine switches specified.")
            return None

        network_cfg = self.network_config
        device_args = list()
        for s in spine_switches:
            switch_name = get_obj_name(s)

            mgmt_addr = DeploymentConfig.get_switch_management_addr(s)
            creds = get_obj_credentials(s)
            rack, rack_unit = get_obj_rack(s)
            stamp = get_obj_stamp(s)
            device_args.append({
                'name': switch_name,
                'mgt_addr': mgmt_addr,
                'vendor': DeploymentConfig.get_switch_vendor(s),
                'model': DeploymentConfig.get_switch_model(s),
                'tier': 'SP',
                'tier_pos': 0,
                'username': creds['username'],
                'password': creds['password'],
                'rack': str(rack) if rack else None,
                'rack_unit': rack_unit,
                'stamp': stamp
            })
        return ToolDeviceAdder(self.cfg.network_config_filename, 'add_switch', device_args)


class ExteriorConnections(Deploy):
    """ Command to specify uplinks/downlinks
    """
    name = "Specify uplink/downlink information"

    def get_command(self):
        ext_conn_fname = self.cfg.exterior_connection_filename

        # build the exterior conn file
        if not self.cfg.uplinks:
            logger.info("No uplinks specified.")
            return None
        lines = []
        for u in self.cfg.uplinks:
            dl = u['downlink']

            l = f"{u['aw_switch']} {u['aw_switch_port']} {u['uplink_ip']}"
            l = f"{l} {dl['switch_name']} {dl['switch_port']} {dl['downlink_ip']}"
            lines.append(l)
        with open(ext_conn_fname, "w") as f:
            f.write("\n".join(lines))

        args = dict(
            input_file=ext_conn_fname,
            config=self.cfg.network_config_filename
        )

        return ToolCommand(AddExteriorConnections, args)
