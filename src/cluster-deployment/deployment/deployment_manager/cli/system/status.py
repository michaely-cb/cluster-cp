import logging

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device
from deployment_manager.tools.system import SystemCtl, SystemStatus, perform_group_action

logger = logging.getLogger(__name__)

class Status(SubCommandABC):
    """ Show systems current status """
    name = "status"

    def construct(self):
        self.parser.add_argument("--system", help="CS system name. All if none selected", type=str, nargs='+')
        self.add_arg_filter()
        self.add_arg_output_format()

    def run(self, args):
        username, password = "admin", "admin"
        systems = []
        query_set = self.filter_devices(args, query_set=Device.get_all(self.profile, device_type="SY"))
        for d in query_set:
            u = d.get_prop(props.prop_management_credentials_user, include_default=False) or username
            p = d.get_prop(props.prop_management_credentials_password, include_default=False) or password
            if not args.system or d.name in args.system:
                systems.append(SystemCtl(d.name, u, p))

        results = perform_group_action(systems, SystemCtl.get_system_status.__name__)
        reprs = []
        for system, result in results.items():
            if isinstance(result, str):
                logger.error(f"{system}: unexpected error occurred '{result}'")
            else:
                reprs.append(result)

        print(SystemStatus.format_reprs(reprs, fmt=args.output))
        if args.output == "table":
            logger.info(f"Total {len(results)} systems")
