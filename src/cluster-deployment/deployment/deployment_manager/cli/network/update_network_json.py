import logging
import pathlib
import shutil
import tempfile

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import DeploymentStageStatus, DeviceDeploymentStages
from deployment_manager.db.models import Device
from deployment_manager.deploy.network import DeployNetworkGenerate
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.utils import diff_files, prompt_confirm
from django.db.models import Q

logger = logging.getLogger(__name__)

class UpdateNetworkConfigJson(SubCommandABC):
    """
    Update the network_config.json file with any new location/stamp assignments
    """

    name = "update_network_json"

    def construct(self):
        self.parser.add_argument(
            "--output",
            "-o",
            help="Location to write updated network_config.json file. The default is /tmp/<tmp>/network_config.json",
        )
        self.add_arg_noconfirm()

    def run(self, args):
        if args.output == "-":
            logger.error("Output to stdout not supported.")
            return 1
        elif args.output is not None:
            output_path = pathlib.Path(args.output)
        else:
            output_path = pathlib.Path(tempfile.mkdtemp()) / "network_config.json"

        # Get the default paths
        cg = ConfGen(self.profile)
        new_network_config = output_path
        cur_network_config = pathlib.Path(cg.network_config_file)

        # Need to parse the old file before generating a new one, so copy it
        shutil.copyfile(
            str(cur_network_config.absolute()),
            str(new_network_config.absolute()),
        )

        # Only use devices that have already been included in a master config/net json -
        # those that have already completed at least the NETWORK_GENERATE stage.
        # Don't move something through the generated stage for the first time here.
        all_devs = Device.get_all(self.profile)
        generated_devices = all_devs.filter(
            Q(
                deployment_stage=DeviceDeploymentStages.NETWORK_GENERATE.value,
                deployment_status=DeploymentStageStatus.COMPLETED.value,
            )
            | Q(deployment_stage__gt=DeviceDeploymentStages.NETWORK_GENERATE.value)
        )

        generator = DeployNetworkGenerate(self.profile, dev_command_network_config_file=output_path)
        if generator.execute_get_targets(generated_devices):
            if not generator.execute(args):
                logger.error("Failed to generate the updated network_config.json")

        diffs = diff_files(cur_network_config, new_network_config)
        if not diffs.strip():
            logger.info("No changes detected")
            return 0
        logger.info(diffs)

        if not args.noconfirm and not prompt_confirm("Overwrite network_config.json?"):
            # not overwrite network_config
            logger.info(f"network_config.json  written to {str(new_network_config.absolute())}")
            return 0

        shutil.copyfile(
            str(new_network_config.absolute()),
            str(cur_network_config.absolute()),
        )
        logger.info(f"network_config.json overwritten to {str(cur_network_config.absolute())}")
        return 0
