import logging
from typing import List, Optional

from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles
from deployment_manager.db.models import (Cluster, Device, list_cluster_controlplanes, update_clusterdevices)
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.ssh import SSHConn

logger = logging.getLogger(__name__)


def _try_query_controlplane_nodes(profile) -> List[Device]:
    """
    Returns
        list of k8s controlplane nodes or an empty list if no controlplane nodes could be found
    """
    mgmt_nodes = Device.get_servers(
        profile, device_role=DeploymentDeviceRoles.MANAGEMENT.value
    ).order_by('name')

    for node in mgmt_nodes:
        user = node.get_prop(props.prop_management_credentials_user)
        password = node.get_prop(props.prop_management_credentials_password)
        try:
            with SSHConn(node.name, user, password) as conn:
                ret, out, stderr = conn.exec(
                    "kubectl get nodes --selector 'node-role.kubernetes.io/control-plane' -o custom-columns=NAME:.metadata.name --no-headers"
                )
            if ret:
                logger.debug(f"failed to 'kubectl get nodes' on {node.name}, trying next: {stderr}")
                continue
            rv = []
            for node_name in out.strip().splitlines():
                d = Device.get_device(node_name, profile)
                if d:
                    rv.append(d)
            return sorted(rv, key=lambda d: d.name)
        except:
            logger.exception(f"unable to detect k8s controlplane on {node.name}: failed to connect")
    return []


def _discover_controlplane_nodes(profile: str) -> List[Device]:
    # if there's more than one cluster for this profile, immediately give up since it's dangerous to assume we know
    # which is correct cluster
    cluster_count = Cluster.objects.filter(profile__name=profile).count()
    if cluster_count > 1:
        raise ValueError(f"failed to discover k8s deployment node for profile {profile}: profile has multiple clusters")

    # if the root server is a management node, that's a sure sign that it's a deploy node
    root_server = Device.get_device(ConfGen(profile).get_root_server(), profile)
    if root_server and root_server.device_role == DeploymentDeviceRoles.MANAGEMENT.value:
        return [root_server]

    # iterate through the management nodes and attempt to query k8
    return _try_query_controlplane_nodes(profile)


def get_k8s_lead_mgmt_node(profile: str, cluster_name: Optional[str] = None) -> Device:
    """
    Get k8s deployment node from the DB or attempt to discover it by either an assumption that if the root server is
    a management node then it must be a k8s node or else by querying k8s.

    Returns
        The first k8s controlplane node sorted alphabetically by name
    """
    # TODO: this is a weak assumption that the first alphabetically sorted controlplane name is the primary
    primary_controlplane = list_cluster_controlplanes(profile, cluster_name).first()
    if primary_controlplane:
        return primary_controlplane

    cluster = Cluster.must_get(profile, cluster_name)
    cp_devices = _discover_controlplane_nodes(profile)
    if not cp_devices:
        raise ValueError(
            "unable to auto-discover k8 controlplane nodes. "
            "You must manually mark them with "
            "'cscfg cluster device edit -f --name=<node-name> --controlplane=true -y'"
        )

    update_clusterdevices(cluster, cp_devices, controlplane=True)
    return cp_devices[0]
