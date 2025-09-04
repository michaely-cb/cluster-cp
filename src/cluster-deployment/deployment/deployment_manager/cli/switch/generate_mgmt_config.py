import logging
import pathlib

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles
from deployment_manager.db.models import Device
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.switch.ztp_config import setup_ztp

logger = logging.getLogger(__name__)

HTTP_DIR = pathlib.Path("/var/www/html/")
TFTP_DIR = pathlib.Path("/var/ftpd/")


def _get_output_dir(device: Device) -> pathlib.Path:
    """ different vendors / models will either use TFTP or HTTP """
    vendor = device.get_prop(props.prop_vendor_name)
    is_mgmt = device.device_role == "MG"
    if vendor == "DL":
        return HTTP_DIR
    if vendor == "JU" and is_mgmt:
        # juniper 100G switches didn't obey the HTTP-PORT suboption in option 43 but 1G mgmt switches did
        return HTTP_DIR
    return TFTP_DIR


class GenerateMgmtConfig(SubCommandABC):
    """
    Generate ZTP and management configuration for switches in the cluster and updates devices with DHCP option
    properties pointing to these files.
    IP and subnet information will be retrieved from the database and input.yml.
    'cscfg device assign_mgmt_ips' needs to have been run for the db to contain correct information.
    """
    name = "generate_mgmt_config"

    def construct(self):
        self.parser.add_argument(
            "--output_dir", default="",
            type=str,
            help="Optional directory where the generated config files are written instead of the default http or tftp "
                 "directories. Note: setting this option is effectively a dryrun - the DHCP options properties will "
                 "not be set on the devices and effective options are cleared"
        )
        self.parser.add_argument(
            "NAME", nargs="*",
            type=str,
            help="Names of switches to generate config for"
        )
        self.add_arg_filter(required=False)

    def run(self, args):
        query_set = self.filter_devices(args, name=args.NAME, device_type="SW", query_set=Device.get_all(self.profile))
        query_set = query_set.filter(device_role__in=(  # avoid mediaconverters, frontend, management-agg, ... for now
            DeploymentDeviceRoles.MANAGEMENT.value,
            DeploymentDeviceRoles.LEAF.value,
            DeploymentDeviceRoles.SPINE.value,
            DeploymentDeviceRoles.MEMORYX.value,
            DeploymentDeviceRoles.SWARMX.value,
        ))

        retval = setup_ztp(self.profile, query_set, args.output_dir)
        failures = [s for s in retval if retval[s]]
        logger.info(f"Generated ZTP config successfully for {query_set.count() - len(failures)}"
                    f" out of {query_set.count()} switches")
        return 1 if len(failures) != 0 else 0
