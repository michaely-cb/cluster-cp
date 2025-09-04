#!/usr/bin/env bash
##
## Globals
##
export PKG_COMMON_FULL_PATH=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
export PKG_GEN_PATH="${PKG_COMMON_FULL_PATH}/gen"
mkdir -p "${PKG_GEN_PATH}"

if [ "$(id -u)" != "0" ]; then
  export SUDO="sudo -E"
else
  export SUDO=""
fi

export KUBECTL="$SUDO kubectl"
export MKDIR="$SUDO mkdir"
export CHMOD="$SUDO chmod"
export CHOWN="$SUDO chown"
export CP="$SUDO cp"
export LN="$SUDO ln"
export RM="$SUDO rm"
export MV="$SUDO mv"
export HELM="$SUDO $(which helm)"
export NERDCTL="$SUDO $(which nerdctl)"
export TOUCH="$SUDO touch"
export YQ="$SUDO $(which yq)"
export SUDO_BASH="$SUDO bash -c"

source "$PKG_COMMON_FULL_PATH/pkg-functions/cluster.sh"
source "$PKG_COMMON_FULL_PATH/pkg-functions/nodes.sh"
source "$PKG_COMMON_FULL_PATH/pkg-functions/systems.sh"
source "$PKG_COMMON_FULL_PATH/pkg-functions/images.sh"
source "$PKG_COMMON_FULL_PATH/pkg-functions/multus.sh"
source "$PKG_COMMON_FULL_PATH/pkg-functions/ceph.sh"
source "$PKG_COMMON_FULL_PATH/pkg-functions/cluster-server.sh"

if [ -n "${dry_run}" ]; then
  return
fi

if [ -z "${CEREBRAS_INTERNAL_DEPLOY}" ]; then
  if netapp_is_available || [ -d /kind ]; then
    echo "Cerebras internal deployment detected" >&2
    export CEREBRAS_INTERNAL_DEPLOY="true"
  else
    export CEREBRAS_INTERNAL_DEPLOY="false"
  fi
fi

function is_incremental_deploy() {
  [ -n "$INCREMENTAL_DIR" ]
}

if is_incremental_deploy && [ -n "$SKIP_IF_INCREMENTAL" ]; then
  exit 0
fi

if [ -z "$SKIP_CHECK" ]; then
  # If we have source pkg-common.sh before, don't need to run the following commands again.
  if [ "$CLUSTER_MGMT_PKG_COMMON_SOURCED" = "True" ]; then
    echo "pkg-common.sh sourced"
    return
  fi
  export CLUSTER_MGMT_PKG_COMMON_SOURCED=True

  _check_control_plane_node
  
  if get_cluster_config; then
    echo "Successfully get cluster config"
  else
    echo "Error looking for cluster config"
    exit 1
  fi
fi

set_pssh_max_threads
if ! init_cluster_properties; then
  return 1
fi
if ! _validate_cilium_ranges; then
  return 1
fi
# the get_*_node functions create node lists - should not be called if incremental deploy
# (because the incremental deploy manages these lists instead)
if ! is_incremental_deploy; then
  get_node_ips
  get_mgmt_node_ips
  get_crd_node_ips
  get_worker_node_ips
  get_not_br_nodes
fi

export cluster_name=$(yq -r '.name' ${CLUSTER_CONFIG})
export short_cluster_name=${cluster_name/multibox/mb}
export service_domain=$(yq -r '.properties.serviceDomain // .serviceDomain // "cerebrassc.local"' ${CLUSTER_CONFIG})
# Use fixed url for registry, since the registry is local to the cluster.
export registry_url="registry.local"
export cluster_service_domain="${cluster_name}.${service_domain}"

# MGMT_NODE_IP will be on 1g link
export MGMT_NODE_IP=$(get_node_mgmt_ip)
export MGMT_NODE_IPS=$(cat ${MGMT_NODE_LIST} | paste -sd, -)

# MGMT_NODE_DATA_IP will be on 100g link
export MGMT_NODE_DATA_IPS=$(get_mgmt_node_data_ips)
MGMT_NODE_DATA_IP=$(echo $MGMT_NODE_DATA_IPS | cut -d ',' -f 1)
# for backwards compatible or kind test
if [ -z "$MGMT_NODE_DATA_IP" ]; then
  echo "warning: 100g Data IP not found, fall back to mgmt node ip $MGMT_NODE_IP"
  MGMT_NODE_DATA_IP=$MGMT_NODE_IP
fi
export MGMT_NODE_DATA_IP

CEPH_NODES=$(yq -r '.nodes[] | select(.properties.storage-type == "ceph") | .name // ""' ${CLUSTER_CONFIG} | xargs)
export CEPH_NODES=$CEPH_NODES

KAFKA_NODES=$(yq -r '.nodes[] | select(.properties.kafka-node == "") | .name // ""' ${CLUSTER_CONFIG} | xargs)
export KAFKA_NODES=$KAFKA_NODES
