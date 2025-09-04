"""
Config upload tasks
"""
import logging

from deployment_manager.network_config.cli import SwitchTasks
from .base import Deploy
from .command import ToolCommand

logger = logging.getLogger(__name__)


class SwitchGetRoceStatus(Deploy):
    """
    This command updates network.json with the current RoCE status
    """
    name = "Read RoCE status on switches"

    def __init__(self, cfg, names=None):
        super().__init__(cfg)
        self.names = names

    def get_command(self):
        args = dict(
            task='roce_status',
            config=self.cfg.network_config_filename
        )
        if self.cfg.switch_password:
            args['password'] = self.cfg.switch_password
        return ToolCommand(SwitchTasks, args)


class SwitchEnableRoce(Deploy):
    """
    Dell and Edgecore switches need a reboot for RoCE to be enabled
    """
    name = "Enable RoCE on switches"

    def __init__(self, cfg, names=None):
        super().__init__(cfg)
        self.names = names

    def get_command(self):
        args = dict(
            task='enable_roce',
            names=self.names,
            config=self.cfg.network_config_filename
        )
        if self.cfg.switch_password:
            args['password'] = self.cfg.switch_password
        return ToolCommand(SwitchTasks, args)


class SwitchUpload(Deploy):
    """ Upload config to switches
    """
    name = "Upload config to switches"

    def __init__(self, cfg, names=None, config_section=None,
                ports_for=None):
        super().__init__(cfg)
        self.names = names
        self.config_section = config_section
        self.ports_for = ports_for

    @staticmethod
    def get_tool_cmd(cfg_filename, base_dir, names=None, config_section=None, ports_for=None, password=None):
        args = dict(
            task='upload',
            names=names,
            config_section=config_section,
            ports_for=ports_for,
            base_dir=base_dir,
            config=cfg_filename
        )
        if password:
            args['password'] = password
        return ToolCommand(SwitchTasks, args)

    def get_command(self):
        return self.get_tool_cmd(
            self.cfg.network_config_filename, self.cfg.output_directory,
            self.names, self.config_section, self.ports_for, self.cfg.switch_password
        )


switch_config_uploaders = [
    SwitchUpload
]
