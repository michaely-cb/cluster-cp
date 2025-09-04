import logging

from deployment_manager.cli.cluster.repr import ClusterDeviceRepr, ClusterRepr
from deployment_manager.cli.subcommand import SubCommandABC, bool_type
from deployment_manager.db.models import (
    Cluster, ClusterDevice, DeploymentProfile, Device, list_cluster_controlplanes, update_clusterdevices,
)
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)


class ClusterShow(SubCommandABC):
    """ Show clusters """

    name = "show"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Show cluster information"

    def construct(self):
        self.add_arg_output_format()

    def run(self, args):
        reprs = []
        for c in Cluster.objects.filter(profile__name=self.profile).order_by('is_primary', 'name'):
            repr = ClusterRepr.from_cluster(c)
            repr.status.controlplane = sorted([d.name for d in list_cluster_controlplanes(self.profile, c.name)])
            reprs.append(repr)
        print(ClusterRepr.format_reprs(reprs, args.output))


class ClusterAdd(SubCommandABC):
    """ Add a cluster """

    name = "add"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Add a new cluster instance. Add management nodes separately with 'cluster device edit"

    def construct(self):
        self.add_arg_output_format()
        self.parser.add_argument("NAME", help="cluster name")
        self.parser.add_argument(
            "--dns-name",
            "-d",
            required=True,
            help="DNS name of the cluster"
        )
        self.parser.add_argument(
            "--management-vip",
            "-m",
            required=True,
            help="Management VIP of the cluster"
        )
        self.parser.add_argument(
            "--properties", "-p", nargs="+", default=[],
            help="properties, e.g. -p dev=true"
        )

    def run(self, args):
        cluster = Cluster.objects.filter(profile__name=self.profile, name=args.NAME).first()
        if cluster:
            raise ValueError(f"cluster with name {args.NAME} already exists")
        repr = ClusterRepr(name=args.NAME, dns_name=args.dns_name, management_vip=args.management_vip, )
        repr.update_attrs([*args.properties, f"name={args.NAME}", ])

        profile = DeploymentProfile.get_profile(self.profile)

        logger.info(
            f"Adding a cluster {args.NAME} with DNS {args.dns_name}, Management-Vip {args.management_vip}"
        )
        logger.info(f"Using profile {profile}")

        cluster = Cluster(
            profile=profile,
            name=repr.name,
            dns_name=repr.dns_name,
            is_primary=False,  # Never allow creating a primary cluster. Should be controlled by the migration process
            management_vip=repr.management_vip,
        )
        cluster.save()
        repr = ClusterRepr.from_cluster(cluster)
        print(ClusterRepr.format_reprs([repr], args.output))


class ClusterRemove(SubCommandABC):
    """ Remove a cluster """

    name = "remove"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Remove an existing cluster instance. Must not be the primary cluster"

    def construct(self):
        self.add_arg_noconfirm()
        self.add_arg_output_format()
        self.parser.add_argument("NAME", help="cluster name")

    def run(self, args):
        cluster_name = args.NAME
        cluster = Cluster.objects.filter(name=cluster_name, profile__name=self.profile).first()
        if not cluster:
            raise ValueError(f"cluster '{cluster_name}' was not found")
        if cluster.is_primary:
            raise ValueError(f"invalid cluster: '{cluster_name}' is the primary cluster and cannot be deleted")

        # Get all devices associated with the cluster to be removed
        cluster_devices = ClusterDevice.objects.filter(cluster=cluster)
        devices_to_reassign = [cd.device for cd in cluster_devices]
        device_count = len(devices_to_reassign)
        if device_count > 0:
            # Find the primary cluster
            primary_cluster = Cluster.objects.filter(profile__name=self.profile, is_primary=True).first()
            if not primary_cluster:
                raise ValueError("No primary cluster found to reassign devices")
            if not args.noconfirm and not prompt_confirm(
                f"Delete {cluster_name} + reassign {device_count} devices to primary cluster '{primary_cluster.name}'?"
            ):
                return 0
            update_clusterdevices(primary_cluster, devices_to_reassign)
            logger.info(
                f"Reassigned {device_count} devices "
                f"from deleted cluster '{cluster_name}' to primary cluster '{primary_cluster.name}'"
            )
        else:
            if not args.noconfirm and not prompt_confirm(f"Delete {cluster_name}?"):
                return 0
        repr = ClusterRepr.from_cluster(cluster)
        cluster.delete()
        logger.info(f"deleted cluster {cluster_name}")
        print(ClusterRepr.format_reprs([repr], args.output))
        return 0


class ClusterEdit(SubCommandABC):
    """ Edit a cluster """

    name = "edit"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Edit a cluster instance"

    def construct(self):
        self.add_arg_output_format()
        self.parser.add_argument("NAME", help="cluster name")
        self.parser.add_argument(
            "--properties", "-p", nargs="+", default=[],
            help="properties, e.g. -p dev=true"
        )

    def run(self, args):
        cluster = Cluster.objects.filter(profile__name=self.profile, name=args.NAME).first()
        if not cluster:
            raise ValueError(f"cluster '{args.NAME}' does not exist")
        repr = ClusterRepr.from_cluster(cluster).update_attrs([*args.properties, f"name={args.NAME}", ])
        cluster.dns_name = repr.dns_name
        cluster.management_vip = repr.management_vip
        cluster.save()

        repr = ClusterRepr.from_cluster(cluster)
        repr.status.controlplane = sorted([d.name for d in list_cluster_controlplanes(self.profile, cluster.name)])
        logging.info(f"updated cluster {cluster}: {args.properties}")
        print(ClusterRepr.format_reprs([repr], args.output))


class ClusterDeviceShowCmd(SubCommandABC):
    """ Show cluster devices """

    name = "show"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = "Show cluster device information"

    def construct(self):
        self.add_arg_output_format()
        self.parser.add_argument(
            "--cluster",
            default=None,
            help="Selected cluster or primary cluster if not set, set to '*' to show all clusters"
        )

    def run(self, args):
        cluster_name = args.cluster
        if cluster_name == "*":
            cluster_name = None
        elif cluster_name is None:
            # Use primary cluster if available, otherwise don't filter
            primary_cluster = Cluster.objects.filter(profile__name=self.profile, is_primary=True).first()
            if primary_cluster:
                cluster_name = primary_cluster.name
        query = ClusterDevice.objects.all()
        if cluster_name is not None:
            query = query.filter(cluster__name=cluster_name, device__profile__name=self.profile)
        rv = [
            ClusterDeviceRepr.from_clusterdevice(cd)
            for cd in query
        ]
        print(ClusterDeviceRepr.format_reprs(rv, args.output))


class ClusterDeviceEditCmd(SubCommandABC):
    """ Edit cluster devices """

    name = "edit"

    def __init__(self, subparsers, cli_instance, profile):
        super().__init__(subparsers, cli_instance, profile)
        self.description = str(
            "Edit cluster device membership. This command should only be used when initializing a new cluster or "
            "if you really know what you're doing."
        )

    def construct(self):
        self.add_arg_output_format()
        self.add_arg_noconfirm()
        self.add_arg_filter(required=True)
        self.parser.add_argument(
            "--cluster",
            default="*",
            help="Target cluster association"
        )
        self.parser.add_argument(
            "--controlplane",
            default=None,
            nargs='?',
            const=True,
            type=bool_type,
            help="Set to True/False to mark controlplanes. Omit to leave unchanged"
        )

    def run(self, args):
        cluster = Cluster.must_get(self.profile, args.cluster)

        qs = Device.objects.filter(
            profile__name=self.profile, device_type__in=("SY", "SR")
        ).exclude(
            device_role__in=('US', 'IN')
        )

        if args.controlplane is not None:
            qs = qs.filter(device_type="SR", device_role="MG")  # only allow setting controlplane on MG servers

        def do_update(save=True) -> int:
            updates = update_clusterdevices(cluster, qs, controlplane=args.controlplane, save=save)
            reprs = [
                ClusterDeviceRepr(
                    cluster.name, r.device.name, r.device.device_type, r.device.device_role, r.controlplane
                ) for r in updates
            ]
            if not save and not reprs:
                return 0

            print(
                ClusterDeviceRepr.format_reprs(reprs, args.output),
            )
            return len(reprs)

        qs = self.filter_devices(args, qs)
        if not args.noconfirm:
            # ideally we'd hold the txn open until the end of the command
            count = do_update(save=False)
            if not count:
                print("No changes to be made")
                return 0
            if not prompt_confirm(f"Commit {count} changes?"):
                return 1

        do_update()
        return 0


class ClusterDeviceCmd(SubCommandABC):
    """ Cluster device commands """

    COMMANDS = [ClusterDeviceShowCmd, ClusterDeviceEditCmd]

    name = "device"

    def construct(self):
        subparsers = self.parser.add_subparsers(dest="action", required=True)
        for cls in self.COMMANDS:
            m = cls(
                subparsers, profile=self.profile, cli_instance=self.cli_instance
            )
            m.build()

    def run(self, args):
        if hasattr(args, "func"):
            return args.func(args)
        else:
            self.parser.print_help()
            return 1
