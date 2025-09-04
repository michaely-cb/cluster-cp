"""
Network environment of the cluster
"""
import logging
from typing import Tuple

from .base import DeployCmd
from ..cli.misc import set_network_config_environment
from ..common.context import NetworkCfgCtx

logger = logging.getLogger(__name__)


class NetworkEnvironment(DeployCmd):
    """ Add information about the network environment
    """
    name = "Add network environment information"

    def run(self, dry_run=False) -> Tuple[int, None, str]:
        if self.cfg.network is None:
            logger.info("No network configuration found.")
            return 0, None, "no action: network configuration found"

        with NetworkCfgCtx(self.cfg.network_config_filename, persist_config_updates=not dry_run) as ctx:
            set_network_config_environment(
                network_config=ctx.network_config.raw(),
                cluster_prefix=self.cfg.cluster_prefix,
                exterior_prefixes=self.cfg.exterior_prefixes,
                exterior_nonpolicy_prefixes=None,
                asn_extents=[f"{a['start']},{a['count']}" for a in self.cfg.cluster_asns],
                overlay_prefixes=self.cfg.overlay_prefixes,
                service_prefix=self.cfg.service_prefix,
                subnet_pools=self.cfg.network.get("clusterNetwork", {}).get("ipConfig", {}).get("subnetPools", []),
                system_enable_dcqcn=self.cfg.network.get("clusterNetwork", {}).get("systemEnableDcqcn", False),
            )
        return 0, None, "network environment information added"
