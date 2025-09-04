#!/usr/bin/env bash

run() {
  if [ -z "$DRY_RUN" ]; then
    $@
  else
    echo "skipping dry run: $@"
  fi
}

get_nad_by_nic_index() {
  index=$1
  if [ "$index" == 0 ]; then
    echo "$DEFAULT_NET_ATTACH"
  else
    echo "$DEFAULT_NET_ATTACH-$index"
  fi
}

create_net_attach() {
  cat <<EOF | kubectl apply -f-
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata:
  name: $1
  namespace: $SYSTEM_NAMESPACE
spec:
  config: ""
EOF
}

create_net_attaches() {
  local staging_root="/tmp/cluster/multus"
  local dest_dir="/etc/cni/multus/net.d"
  local legacy_dest_dir="/etc/cni/net.d"
  mkdir -p ./gen

  if [ -n "$DRY_RUN" ]; then
    dest_dir="${staging_root}/dest_dir"
  fi

  # if no new nodes and the standard container is available
  if ! is_incremental_deploy && check_image_in_registry alpine-containerd latest; then
    # Upgrade an already-running cluster with no new nodes.
    # Every existing node has some version of nerdctl. Use the installer to check for updates.
    rm -rf ${staging_root}
    mkdir -p ${staging_root}
    cp ${CLUSTER_CONFIG} ${staging_root}/cluster.yaml
    cp -r node ${staging_root}

    bash ${staging_root}/node/node-net-attach-def.sh $dest_dir $INCREMENTAL_DIR

    # In rel-2.3, we have moved the data net configs from $dest_dir to $legacy_dest_dir
    # TODO: Remove the stale config removal step in rel-2.5
    rm -f $legacy_dest_dir/*-data-net*

    installer_rollout_wait_or_warn installers net-attach-def-installer ./net-attach-def-installer.sh 180 unreachable_ok INCREMENTAL_DIR=$INCREMENTAL_DIR DEST_DIR=$dest_dir
  else
    # Incremental deploy to some new nodes, or the cluster hasn't passed the common-images step.
    # In the incremental case, the PSSH call will only connect to new nodes.
    PSSH "rm -rf \"${staging_root}\"; mkdir -p ${staging_root}"
    PSCP "${CLUSTER_CONFIG}" "${staging_root}/cluster.yaml"
    PSCP -r "node" "${staging_root}"
    if is_incremental_deploy; then
      PSCP -r "$INCREMENTAL_DIR" "/tmp"
    fi
    PSSH "bash ${staging_root}/node/node-net-attach-def.sh $dest_dir $INCREMENTAL_DIR"
    PSSH "rm -f $legacy_dest_dir/*-data-net*"
  fi


  if is_incremental_deploy; then
    return 0
  fi

  run create_net_attach "$(get_nad_by_nic_index 0)"
  run create_net_attach "$(get_nad_by_nic_index 1)"
}

# Below can be removed after all clusters' ceph are going with hostnetwork.
# Create a net-attach-def object for ceph.
# The is needed because Ceph operator rejects an empty config in net-attach-def
# it's only a template to bypass the validation and the real NAD will be replaced by the default data-net in webhook
# https://github.com/Cerebras/monolith/blob/9998a67eefe6458fcd87c7692f5717ae922f8bd1/src/cluster_mgmt/src/kube-webhook/cmd/cmd.go#L101
create_ceph_net_attach() {
  if [ -n "$DRY_RUN" ] || has_multiple_mgmt_nodes; then
    mkdir -p gen

    local group=$(yq -r "[.nodes[] | select(.role == \"management\")][0] | .properties.v2Group // .properties.group // \"\"" "${CLUSTER_CONFIG}")
    if [ -z "$group" ]; then
      local group=$(yq -r "[.nodes[] | select(.role == \"management\")][0] | .networkInterfaces[0].v2Group" "${CLUSTER_CONFIG}")
    fi
    local nic=$(yq -r "[.nodes[] | select(.role == \"management\")][0] | .networkInterfaces[0].name" "${CLUSTER_CONFIG}")
    create_net_attach_config "rook-public-net" "$group" "" "$nic" "${CLUSTER_CONFIG}" \
      "gen/rook-public-net.conf" "net-attach-config-ceph.jsontemplate"

    jq -n \
      --arg config "$(cat gen/rook-public-net.conf)" \
      --arg name "rook-public-net" \
      --arg namespace "rook-ceph" \
      "$(cat net-attach-def.jsontemplate)" | yq -P >"gen/rook-public-net-nad.yaml"

    # use rook-ceph namespace for this net-attach-def
    run kubectl create namespace rook-ceph &>/dev/null || true
    run kubectl apply -f "gen/rook-public-net-nad.yaml"
  fi
}

main() {
  if is_incremental_deploy; then
    CLUSTER_CONFIG="${INCREMENTAL_DIR}/incremental-cluster.yaml"
  fi
  if ! should_install_multus; then
    echo "Cluster is not configured for multiple networks, not installing multus and whereabouts"
    exit 0
  fi

  rm -rf ./gen

  echo "creating net-attach-defs..."
  create_net_attaches
  if ! is_incremental_deploy; then
    create_ceph_net_attach
  fi
}

set -e

cd "$(dirname "$0")"
source "./node/common.sh"

if [ "$1" == "ceph" ]; then
  # Test creating ceph net-attach-def
  DEFAULT_NET_ATTACH=${DEFAULT_NET_ATTACH:-"multus-data-net"} \
    CLUSTER_CONFIG=${CLUSTER_CONFIG:-"/opt/cerebras/cluster/cluster.yaml"} \
    SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE:-"job-operator"} \
    DRY_RUN=${DRY_RUN:-"true"} \
    create_ceph_net_attach
else
  source "../pkg-common.sh"
  main
fi
