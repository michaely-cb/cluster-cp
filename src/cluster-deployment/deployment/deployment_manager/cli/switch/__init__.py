from deployment_manager.cli.switch.configure_dcqcn import ConfigureDcqcn
from deployment_manager.cli.switch.generate_mgmt_config import GenerateMgmtConfig
from deployment_manager.cli.switch.interface import InterfaceCmd
from deployment_manager.cli.switch.sflow import SFlowCmd
from deployment_manager.cli.switch.link import LinkCmd
from deployment_manager.cli.switch.mediaconverter import MediaConverterCmd
from deployment_manager.cli.switch.switch_health import SwitchHealth
from deployment_manager.cli.switch.update_mac_address import UpdateMacAddress

CMDS = [
    ConfigureDcqcn,
    InterfaceCmd,
    SFlowCmd,
    GenerateMgmtConfig,
    SwitchHealth,
    UpdateMacAddress,
    LinkCmd,
    MediaConverterCmd,
]
