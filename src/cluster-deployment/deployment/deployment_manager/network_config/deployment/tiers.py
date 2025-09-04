"""
Tier provisioning commands
"""
import logging

from deployment_manager.network_config.cli import BuildTierAllocations
from .base import Deploy
from .command import ToolCommand

logger = logging.getLogger(__name__)

class NetworkTier(Deploy):
    """ Command to add memx and swarmx switch and prefix information to the cfg
    """
    name = "Add switch and subnet information to network_config JSON"

    def get_command(self):
        if self.cfg.network is None:
            logger.info("No network configuration found.")
            return None
        tiers = self.cfg.tiers
        if tiers is None:
            logger.info("No tier allocation extents specified.")
            return None

        cp = self.cfg.cluster_prefix
        if cp is None:
            logger.info("No cluster prefix configured.")
            return None

        is_leaf_spine = self.cfg.network_topology == "leaf_spine"

        args = dict(
            cluster_prefix=self.cfg.cluster_prefix,
            topology=self.cfg.network_topology,
            aw_size=tiers.get("aw_size"),
            aw_count=self.cfg.num_aw_switches if not is_leaf_spine else None,
            leaf_count=self.cfg.num_leaf_switches if is_leaf_spine else None,
            aw_physical=tiers.get("aw_physical", 0),
            br_size=tiers.get("br_size"),
            br_count=self.cfg.num_br_switches if not is_leaf_spine else None,
            br_physical=tiers.get("br_physical", 0),
            xconn_count=tiers.get("xconn_count", 0),
            system_prefix_size=tiers.get("system_size"),
            vip_prefix=self.cfg.vip_prefix,
            aw_prefix=self.cfg.aw_prefix,
            br_prefix=self.cfg.br_prefix,
            system_prefix=self.cfg.system_prefix,
            xconn_prefix=self.cfg.xconn_prefix,
            vip_count=tiers.get("vip_count", 0),
            config=self.cfg.network_config_filename
        )

        return ToolCommand(BuildTierAllocations, args)

