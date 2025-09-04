import shutil

import abc
import logging
from typing import Optional, List

from tabulate import tabulate

from deployment_manager.db.const import DeploymentDeviceTypes, DeploymentDeviceRoles
from deployment_manager.db.models import DeploymentProfile
from deployment_manager.tools.config import ClusterProfile, ConfGen
from deployment_manager.tools.network_config_tool import NetworkConfigTool
from deployment_manager.tools.utils import (
    prompt
)

logger = logging.getLogger(__name__)

class ProfileManager:
    @abc.abstractmethod
    def get_current_profile(self) -> Optional[str]:
        pass

    @abc.abstractmethod
    def set_profile(self, profile: str):
        pass


def create_profile(name: str, cfg: str, inv_file: Optional[str], profile_mgr: ProfileManager) -> Optional[str]:
    """ Returns error string if an error occurred """
    if DeploymentProfile.get_profile(name) is not None:
        return "Profile already exists"

    ret = ClusterProfile.create(name, cfg, inv_file)
    if ret:
        return f"Failed to create profile {name}"
    profile_mgr.set_profile(name)
    logger.info(f"Profile {name} created successfully. Current profile set to {name}")
    return None


def import_profile(name: str, network_config: str, profile_mgr: ProfileManager) -> Optional[str]:
    """ Returns error string if an error occurred """
    if DeploymentProfile.get_profile(name) is not None:
        return "Profile already exists"

    def _import_network(profile: str, devices: List[dict]):
        nwt = NetworkConfigTool.for_profile(profile)
        ret = nwt.init_network_config(profile)
        if ret:
            return ret
        for d in devices:
            if d['device_type'] == DeploymentDeviceTypes.SERVER.value:
                if d['device_role'] == DeploymentDeviceRoles.ACTIVATION.value:
                    nwt.add_activation_node(d['name'])
                elif d['device_role'] == DeploymentDeviceRoles.MANAGEMENT.value:
                    nwt.add_management_node(d['name'])
                elif d['device_role'] == DeploymentDeviceRoles.MEMORYX.value:
                    nwt.add_memoryx_node(d['name'])
                elif d['device_role'] == DeploymentDeviceRoles.WORKER.value:
                    nwt.add_worker_node(d['name'])
                elif d['device_role'] == DeploymentDeviceRoles.SWARMX.value:
                    nwt.add_swarmx_node(d['name'])
            elif d['device_type'] == DeploymentDeviceTypes.SYSTEM.value:
                nwt.add_system(d['name'])

        nwt.discover_switches()
        nwt.read_switch_models()
        nwt.discover_connections()

    def _import_profile(profile, network_config_file: Optional[str] = None):
        cg = ConfGen(profile)
        logger.info("Parsing kubernetes cluster config")
        ret, k8s_config = cg.parse_k8s_config()
        if ret:
            logger.error("Failed to parse Kubernetes cluster definition")
        else:
            cg.create_deployment_dirs()
            devices = k8s_config.devices
            if network_config_file is not None:
                shutil.copy(network_config_file, cg.network_config_file)
                logger.info("Parsing network configuration")
                nw_config = cg.parse_network_config(network_config_file)
                devices += nw_config.switches
            else:
                logger.info("Generating network configuration")
                ret = _import_network(profile, devices)
                if ret:
                    return ret
                logger.info("Parsing network configuration")
                nw_config = cg.parse_network_config(cg.network_config_file)
                devices += nw_config.switches
            ClusterProfile.initialize_profile_database(
                profile, k8s_config.cluster_name, devices,
                deployable=False
            )
        return ret

    ret = _import_profile(name, network_config)
    if ret:
        return "Failed to import profile"
    else:
        logger.info("Profile imported successfully")
        profile_mgr.set_profile(name)


def profiles_table(current_profile: str) -> str:
    profiles = DeploymentProfile.list()
    if not profiles:
        return ""
    headers = ["Profile Name", "Cluster Name", "# Servers", "# Systems", "# Switches", "Deployable"]
    rows = [
        [p.name if p.name != current_profile else f"*{p.name}",
         p.cluster_name,
         len(p.servers()),
         len(p.systems()),
         len(p.switches()),
         "Yes" if p.deployable else "No"
         ]
        for p in sorted(profiles, key=lambda p: p.name)
    ]
    return tabulate(rows, headers)


class ProfileUserPrompts:
    """ User-interactive prompts for selecting profiles """

    def __init__(self, profile: ProfileManager):
        self._profile = profile

    def on_cscfg_start(self):
        # If more than one profile is marked deployable, force the user
        # to choose the one that's currently deployed on the cluster
        self.choose_deployable_profile()

        if self._profile is None:
            self.ask_for_profile()

    def choose_deployable_profile(self):
        dps = [p for p in DeploymentProfile.list() if p.deployable]
        dp = None
        if len(dps) > 1:
            print(
                "More than one profile is deployable. "
                "Please choose the one currently deployed on this cluster."
            )
            print(profiles_table(""))
            while dp is None:
                selected = prompt("Enter name of the deployed profile: ",
                                  values=[p.name for p in dps]
                                  )
                dp = DeploymentProfile.get_profile(selected)
                if dp is None:
                    logger.error("Invalid profile name entered")
                    continue
            for p in dps:
                p.mark_not_deployable()
            dp.mark_deployable()

    def ask_for_profile(self):
        if self._profile.get_current_profile() is not None:
            return

        pinfos = DeploymentProfile.list()
        profile_list = [p.name for p in pinfos]
        if profile_list:
            logger.info("")
            logger.info(profiles_table(""))
            logger.info("")
            selected = prompt(msg="Enter a profile: ", values=profile_list)
            self._profile.set_profile(selected)
        else:
            logger.info("\nThere is no existing profile.\nCreate a new profile.")
            self._profile.set_profile(self.prompt_new_profile())

    def prompt_new_profile(self):
        while True:
            profile = prompt(msg="\nEnter Profile Name: ", response_type="str")
            print(
                "\nDo you want to\n"
                "1. Create new profile\n"
                "2. Import from k8s\n"
            )
            action = prompt(
                msg="Choose: ",
                values=["1", "2"]
            )
            if action == "1":
                config_input = prompt(msg="Enter Config Input file: ", response_type="path")
                inventory = prompt(msg="Enter Inventory file: ", response_type="path", allow_empty=True)
                create_profile(profile, config_input, inventory, self._profile)
            elif action == "2":
                logger.info("\nOptional values, press <enter> to skip")
                network_config = prompt(msg="Enter network config file: ",
                                        response_type="path", allow_empty=True
                                        )
                import_profile(profile, network_config, self._profile)

            # Check if successfully created, if not exit
            pnames = [p.name for p in DeploymentProfile.list()]
            assert profile in pnames, "failed to persist new profile"
            return profile
