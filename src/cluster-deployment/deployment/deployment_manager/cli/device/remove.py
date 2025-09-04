import logging

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.models import Device
from deployment_manager.tools.network_config_tool import NetworkConfigTool

logger = logging.getLogger(__name__)


class DeviceRemove(SubCommandABC):
    """
        Remove a device
    """
    name = "remove"

    def construct(self):
        self.parser.add_argument("device_name",
            help="The name of the device to remove"
        )

    def run(self, args):
        dev = Device.get_device(args.device_name, profile_name=self.profile)
        if not dev:
            logger.warning(f"Device {args.device_name} does not exists")
            return
        nct = NetworkConfigTool.for_profile(self.profile)
        nct.network_device_removal(remove_devices=[dev])
        logger.info(f"Device removed")
