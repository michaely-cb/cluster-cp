import logging

from deployment_manager.cli.device.utils import DeviceRepr
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.models import Device, MissingPropVals

logger = logging.getLogger(__name__)


class DeviceShow(SubCommandABC):
    """
    Show devices in the profile
    """
    name = "show"

    def construct(self):
        self.add_arg_output_format()
        self.parser.add_argument(
            "name",
            nargs="*",
            help="Names of devices to show. If not set, shows all devices",
        )
        self.add_arg_filter(required=False)

    def run(self, args):
        targets = self.filter_devices(
            args, device_type=None, name=args.name, query_set=Device.get_all(self.profile)
        )

        devices_repr = sorted([
            DeviceRepr(
                d.name,
                d.device_type,
                d.device_role,
                d.get_prop_attr_dict(include_mode=MissingPropVals.EXCLUDE),
            )
            for d in targets
        ], key=lambda r: (r.type, r.role, r.name,))

        print(DeviceRepr.format_reprs(devices_repr, args.output))
