import logging
import textwrap

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.models import Device, DeploymentProfile
from deployment_manager.tools import cbscheduler, systemdb

logger = logging.getLogger(__name__)


class UpdateSystemDB(SubCommandABC):
    """
        Update the systemdb
    """
    name = "systemdb"

    def construct(self):
        self.parser.add_argument("--location",
                                 default="Colovore",
                                 help="The location of the cluster")
        self.parser.add_argument("--purpose",
                                 default="",
                                 help="The purpose of the cluster")
        self.parser.add_argument("--replace",
                                 action="store_true",
                                 help=textwrap.dedent("""
                                        Replace the entry if it already exists.  
                                        This is same as executing delete/add operations on systemdb
                                    """).strip())
        self.parser.add_argument("--clear-user-hosts",
                                 action="store_true",
                                 help=textwrap.dedent("""
                                    Clear the user_hosts list if no user_hosts defined in the inventory.
                                    By default, systemdb's user_hosts is kept even when --replace option is given, 
                                        unless user_hosts are defined in the inventory.
                                    This option overrides the default and will clear the user_hosts list if not defined 
                                        in the inventory.
                                    """).strip()
                                 )

    def run(self, args):
        mbdata = systemdb.generate_systemdb_multibox_entry(
            self.profile,
            location=args.location,
            purpose=args.purpose,
        )
        if args.clear_user_hosts and "user_hosts" in mbdata:
            del mbdata["user_hosts"]
        status, err_msg = systemdb.update_systemdb(
            update_body=mbdata,
            merge_update=False,
            replace=args.replace,
        )
        if status == 200:
            logger.info(f"{mbdata['name']} inserted into systemdb successful")
        else:
            logger.error(f"{mbdata['name']}: {err_msg}")
            self.cli_instance.returncode = 1


class UpdateCbscheduler(SubCommandABC):
    """
        Add the cbscheduler resource with the cluster name
    """
    name = "cbscheduler"

    def construct(self):
        pass

    def run(self, args):
        profile = DeploymentProfile.get_profile(self.profile)
        cluster_name = profile.cluster_name
        if not cluster_name:
            return

        status, resObj, reason = cbscheduler.get_resource()
        if status != 200:
            logger.error(f"Could not retrieve cbscheduler's resource list.\nError {status}: {reason}")
            self.cli_instance.returncode = 1
            return

        for d in Device.get_systems(profile_name=self.profile):
            if d.name in resObj:
                continue
            status, msg, reason = cbscheduler.add_resource(resource_name=d.name, data={"sys_type": "cs"})
            if status == 200:
                if msg:
                    logger.info(f"{d.name}: {msg}")
                else:
                    logger.info(f"{d.name} added to cbscheduler successful")
            else:
                logger.error(f"{d.name}: {status}:{reason}")
                self.cli_instance.returncode = 1

        if cluster_name in resObj:
            logger.info(f"{cluster_name} is already in cbscheduler")
            return

        status, msg, reason = cbscheduler.add_resource(resource_name=cluster_name, data={"sys_type": "multibox"})
        if status == 200:
            if msg:
                logger.info(f"{cluster_name}: {msg}")
            else:
                logger.info(f"{cluster_name} added to cbscheduler successful")
        else:
            logger.error(f"{cluster_name}: {status}:{reason}")
            self.cli_instance.returncode = 1


CMDS = [
    UpdateSystemDB,
    UpdateCbscheduler,
]

