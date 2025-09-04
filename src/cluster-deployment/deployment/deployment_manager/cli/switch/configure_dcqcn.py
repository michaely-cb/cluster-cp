import logging

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.tools.network_config_tool import (
    NetworkConfigTool
)

logger = logging.getLogger(__name__)


class ConfigureDcqcn(SubCommandABC):
    """
    Enable/disable DCQCN on switches in the cluster
    """
    name = "configure_dcqcn"

    def construct(self):
        self.parser.add_argument(
            "--action", required=True,
            type=str,
            choices=("enable", "disable"),
            help="Choose whether command should enable/disable DCQCN"
        )
        self.parser.add_argument(
            "--switch_category",
            choices=("leaf", "spine"),
            help=(
                "Category of switches to which configuration"
                " is to be applied [leaf|spine]"
            )
        )
        self.parser.add_argument(
            "--ecn_min_threshold",
            type=int,
            help="Minimum ECN threshold in kbytes (does not apply to Arista J3AI spine)"
        )
        self.parser.add_argument(
            "--ecn_max_threshold",
            type=int,
            help="Maximum ECN threshold in kbytes (does not apply to Arista J3AI spine)"
        )
        self.parser.add_argument(
            "--ecn_probability",
            type=int,
            help="ECN probability [1-100] (does not apply to Arista J3AI spine)"
        )
        self.parser.add_argument(
            "--ecn-delay",
            type=int,
            help="ECN delay in milliseconds (only applicable to Arista J3AI spine)"
        )
        self.parser.add_argument(
            "--interface_type",
            type=str,
            choices=("100g", "400g", "800g"),
            help=(
                "Interfaces to which the config is to be applied. "
                "Applies to all interfaces by default."
            )
        )

    def run(self, args):
        nwt = NetworkConfigTool.for_profile(self.profile)

        if args.switch_category is None:
            self.cli_instance.returncode = 1
            logger.error(
                "Switch category [leaf|spine] must be chosen to apply DCQCN config"
            )
            return
        if args.action == "enable":
            ret = nwt.enable_switch_dcqcn(
                    args.switch_category,
                    args.ecn_min_threshold, args.ecn_max_threshold,
                    args.ecn_probability, args.ecn_delay, args.interface_type
            )
        else:
            ret = nwt.disable_switch_dcqcn(args.switch_category, args.interface_type)

        if ret:
            self.cli_instance.returncode = 1
            logger.error(f"Failed to {args.action} DCQCN config")
        else:
            logger.info(f"DCQCN config {args.action}d successfully")
