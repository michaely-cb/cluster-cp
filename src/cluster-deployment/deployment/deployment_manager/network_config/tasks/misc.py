import math
import logging
from typing import Dict, List, Tuple, Optional
from .. import schema, configs, placer
from ..schema import (
    NetworkConfigSchema,
    JSIPInterface,
    JSIPNetwork,
    JSIPAddress,
    ASN4
)
from deployment_manager.network_config.common.task import NetworkTask, NetworkTaskResult, ConfigTask
from ..placer import switch as switch_placer
from ..placer import vip as vip_placer
from ..common.context import SX_VLAN, SYS_VLAN, DEFAULT_VLAN
from ..placer.switch import place_xconnect_prefixes

logger = logging.getLogger(__name__)
class OverlayPrefixSizeGeneratorTask(ConfigTask):
    """
    Calculate overlay prefix size based on provided overlay
    prefixes and nodes in the configuration
    """
    def run(self, *args, **kwargs) -> NetworkTaskResult:
        nw_doc = self._ctx.network_config
        orig_size = nw_doc.raw().get("environment").get("overlay_prefix_mask_size")
        if orig_size is not None:
            return NetworkTaskResult.ok() # cluster-mgmt can't handle this variable changing after cluster has been deployed

        overlay_prefix_size = 0
        for op in nw_doc.raw().get("environment").get("overlay_prefixes", []):
            overlay_prefix_size += JSIPNetwork(op).num_addresses
        if not overlay_prefix_size:
            return NetworkTaskResult.ok()

        num_nodes = 0
        for section in NetworkConfigSchema.node_sections:
            if section == "user_nodes":
                continue
            
            num_nodes += len(nw_doc.raw().get(section) if nw_doc.raw().get(section) else [])
        if not num_nodes:
            return NetworkTaskResult.ok()

        mask_size = 32 - math.floor(math.log((overlay_prefix_size / num_nodes), 2))
        if mask_size > 27:
            raise ValueError(
                f"calculated environment.overlay_prefix_mask_size was /{mask_size} "
                f"but require at most /{27} (32 addresses)"
            )
        # shouldn't need more than 128 pods per node
        mask_size = max(mask_size, 25)
        nw_doc.raw()["environment"]["overlay_prefix_mask_size"] = mask_size


        return NetworkTaskResult.ok()


class ConfigGeneratorTask(ConfigTask):
    def run(self, *args, **kwargs) -> NetworkTaskResult:
        writer_cls = configs.AllConfigsWriter
        writer_cls(self._ctx.network_config.raw()).write_config(self._ctx._config_base_dir)

        return NetworkTaskResult.ok()



class NetworkPlacerTask(ConfigTask):
    def run(self, *args, **kwargs) -> NetworkTaskResult:
        placer.SwitchASNPlacer(self._ctx.network_config)()

        switch_placer.place_switch_prefixes(self._ctx.network_config, DEFAULT_VLAN)
        switch_placer.place_switch_prefixes(self._ctx.network_config, SYS_VLAN)
        switch_placer.place_switch_prefixes(self._ctx.network_config, SX_VLAN)
        placer.SwitchVirtualAddrPlacer(self._ctx.network_config)()
        placer.SwitchVLANPlacer(self._ctx.network_config)()
        
        # Then place VIPs and cross connects
        vip_placer.place_cluster_vips(self._ctx.network_config)
        vip_placer.place_system_vips(self._ctx.network_config)

        place_xconnect_prefixes(self._ctx.network_config)

        # Finally place all interfaces
        placer.InterfacePlacer(self._ctx.network_config)()
        placer.SystemVLANInterfacePlacer(self._ctx.network_config)()
        placer.SwarmxVLANInterfacePlacer(self._ctx.network_config)()

        return NetworkTaskResult.ok()