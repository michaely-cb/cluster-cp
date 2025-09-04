import logging
from typing import List, Optional

from deployment_manager.network_config.common import MAX_CLUSTERS
from deployment_manager.network_config.common.context import NetworkCfgDoc
from deployment_manager.network_config.common.task import ConfigTask, NetworkTaskResult
from deployment_manager.common.models import MgmtNetworkConfig, Cluster, MgmtVip

logger = logging.getLogger(__name__)


def _validate_mgmt_vip(d: dict) -> bool:
    for p in ("vip", "router_asn", "node_asn"):
        if not d.get(p):
            return False
    return True


def _sync_cluster_mgmt_vip(doc: NetworkCfgDoc, mg_nw_cfg: MgmtNetworkConfig):
    """ Sync the cluster management VIP from the management network config """
    # TODO: this field could be removed if everyone uses the k8s.update_network_config function since
    # the individual cluster[].mgmt_network.vip field is used instead.
    mgmt_vip = mg_nw_cfg.mgmt_vip
    if mgmt_vip:
        v = {
            "vip": mg_nw_cfg.mgmt_vip.vip,
            "router_asn": mg_nw_cfg.mgmt_vip.router_asn,
            "node_asn": mg_nw_cfg.mgmt_vip.node_asn,
        }
        if _validate_mgmt_vip(v):
            doc.raw()["cluster_mgmt_vip"] = v
    else:
        # clear the VIP fields if not set
        existing_vip = doc.raw().get("cluster_mgmt_vip", None)
        if existing_vip:
            logger.warning(f"clearing cluster_mgmt_vip since it is not in the app config. prev={existing_vip}")
            del doc.raw()["cluster_mgmt_vip"]


def _migrate_clusters_entry(doc: NetworkCfgDoc):
    """
    Perform initial schema migration if this is an incremental generate call and clusters
    field is not present
    """
    raw_doc = doc.raw()
    if "clusters" in raw_doc:
        return

    cluster = {
        "name": raw_doc["name"],
        "data_network": {
            # VIP is always allocated even if the cluster is single mgmt-node however if the placer has not
            # been run, this field will not be present
            "vip": raw_doc.get("cluster_data_vip", {}).get("vip", ""),
            "virtual_addr_index": 0,
        },
    }
    # field is optional - only for multi-mgmt node clusters
    if "cluster_mgmt_vip" in raw_doc:
        cluster["mgmt_network"] = {
            "vip": raw_doc["cluster_mgmt_vip"]["vip"],
            "node_asn": raw_doc["cluster_mgmt_vip"]["node_asn"],
            "router_asn": raw_doc["cluster_mgmt_vip"]["router_asn"],
        }

    raw_doc["clusters"] = [cluster]


def _sync_cluster_config(
        doc: NetworkCfgDoc,
        clusters: List[Cluster],
        default_mg_vip: Optional[MgmtVip],
        is_cluster_init=False,
) -> NetworkTaskResult:
    """
    Add or update clusters from the app config. Guard against removing clusters.
    """
    app_clusters = {c.name: c for c in clusters}
    if len(app_clusters) > MAX_CLUSTERS:
        return NetworkTaskResult.error(
            f"k8 cluster count should not exceed 2, got: {len(app_clusters)}. Please remove an unused cluster"
        )
    if not app_clusters:
        logger.error("no clusters found in the app config, continuing without modifying network config")
        return NetworkTaskResult.ok()

    raw_doc = doc.raw()

    doc_clusters = {c["name"]: c for c in raw_doc.get("clusters", [])}

    # check for removals
    for name in list(doc_clusters.keys()):
        if name in app_clusters:
            continue
        return NetworkTaskResult.error(
            f"cluster '{name}' is in the network config but not in the app config. "
            "Please fully teardown the associated k8s cluster before manually removing it from the network config."
        )

    # add or update clusters from the app config
    mgmt_network_base = {}
    if default_mg_vip:
        mgmt_network_base = {
            "node_asn": default_mg_vip.node_asn,
            "router_asn": default_mg_vip.router_asn,
            "vip": default_mg_vip.vip,
        }

    virtual_addr_index = len(doc_clusters)
    for name, cluster in app_clusters.items():
        if name not in doc_clusters:
            if not is_cluster_init:
                return NetworkTaskResult.error(
                    f"cluster '{name}' is in the app config but not in the network config but adding clusters "
                    "is not permitted. Please only add it manually if you're certain you'll not create IP conflicts."
                    # user should manually shrink the old cluster virtual address range to allow adding a new cluster
                )
            logger.info(f"adding cluster {name} to network config")
            doc_clusters[name] = {
                "name": name,
                "data_network": {
                    # we're technically doing placing here but since we only allow adding clusters during init,
                    # we don't need a full placing function for this purpose.
                    "virtual_addr_index": virtual_addr_index,
                },
            }
            c = doc_clusters[name]
            virtual_addr_index += 1
        else:
            logger.info(f"updating cluster {name} in network config")
            c = doc_clusters[name]
        
        mgmt_vip = {
            **mgmt_network_base,
            "vip": cluster.mgmt_vip,
        }
        if _validate_mgmt_vip(mgmt_vip):
            c["mgmt_network"] = mgmt_vip
    raw_doc["clusters"] = list(doc_clusters.values())
    return NetworkTaskResult.ok()


class ClusterAddTask(ConfigTask):

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        """ Set the cluster mgmt VIP and clusters fields. Allow adding clusters. """
        app_ctx = self._ctx.get_app_ctx()
        _sync_cluster_mgmt_vip(self._ctx.network_config, app_ctx.get_mgmt_network_config())
        return _sync_cluster_config(
            self._ctx.network_config,
            app_ctx.get_clusters(),
            app_ctx.get_mgmt_network_config().mgmt_vip,
            is_cluster_init=True,
        )


class ClusterSyncTask(ConfigTask):

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        """
        Set the cluster mgmt VIP and clusters fields. Don't allow adding clusters since
        the cluster might have already been deployed and we can't risk changing the virtual range index.
        """
        app_ctx = self._ctx.get_app_ctx()
        _sync_cluster_mgmt_vip(self._ctx.network_config, app_ctx.get_mgmt_network_config())
        _migrate_clusters_entry(self._ctx.network_config)
        return _sync_cluster_config(
            self._ctx.network_config,
            app_ctx.get_clusters(),
            app_ctx.get_mgmt_network_config().mgmt_vip,
            is_cluster_init=False,
        )
