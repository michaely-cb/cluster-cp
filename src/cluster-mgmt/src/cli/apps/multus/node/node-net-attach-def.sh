#!/usr/bin/env bash
#
# Script to create net-attach-def config files for the node.
#
# note: https://backreference.org/2014/03/20/some-notes-on-macvlanmacvtap/
# Irrespective of the mode used for the macvlan, there's no connectivity from whatever uses the macvlan (eg a container) to the lower device.
# This is by design, and is due to the the way macvlan interfaces "hook into" their physical interface.
# If communication with the host is needed, the solution is kind of easy:
# just create another macvlan on the host on the same lower device, and use this to communicate with the guest.
USAGE=$(
  cat <<EOM
Usage: $0 [create_net_attaches | <dest_dir>]

Generate the net-attach-def config files for the current node. Copy the config
files to the given dest_dir.

If create_net_attaches is provided, it is for testing only. It will generate
net-attach-def config files for all nodes, and these config files will be
copied to a temporary location.
EOM
)

set -e

get_nad_by_nic_index() {
  index=$1
  if [ "$index" == 0 ]; then
    echo "$DEFAULT_NET_ATTACH"
  else
    echo "$DEFAULT_NET_ATTACH-$index"
  fi
}

create_node_net_attach_def() {
  local node="$1"
  local dest_dir="$2"

  # If we're running in the installer DS, then hostname is the name of the pod, not the host
  # (if we're not in the installer DS, MY_NODE_NAME isn't defined)
  if [ -n "$MY_NODE_NAME" ]; then
      node=$MY_NODE_NAME
  fi
  local gen_dir="${WORKDIR}/${node}/gen"

  local INCREMENTAL_DIR="$3"

  rm -rf "${gen_dir}"
  mkdir -p "${gen_dir}"
  mkdir -p "${dest_dir}"

  if [ -n "$INCREMENTAL_DIR" ]; then
    CLUSTER_CONFIG="${INCREMENTAL_DIR}/incremental-cluster.yaml"
  fi

  # yq's -r arg is new, although it's been available for longer as --unwrapScalar
  #   - the -r arg is introduced in yq 4.25.3
  # yq version in alpine 3.16, 4.25.1-r5, has --unwrapScalar but not -r
  # yq version in Rocky8 is 4.33.x, has -r
  # Newer versions of alpine have newer yq - alpine 3.17 has yq 4.30.4
  # alpine 3.16 released in 2022, 3.17 in 2023
  role=$(node="$node" yq --unwrapScalar '.nodes[] | select(.name == env(node)) | .role' "${CLUSTER_CONFIG}")
  if [[ "$role" == "broadcastreduce" ]]; then
    return 0  # not all clusters allocated room for BR / BR doesn't use multus today. Skip NAD deployment
  fi
  group=$(node="$node" yq --unwrapScalar '.nodes[] | select(.name == env(node)) | .properties.v2Group // .properties.group // ""' "${CLUSTER_CONFIG}")
  sorted_nics=$(node="$node" yq --unwrapScalar '.nodes[] | select(.name == env(node)) | [.networkInterfaces[].name] | sort | @csv' "${CLUSTER_CONFIG}")
  IFS=', ' read -r -a nics <<<"$sorted_nics"
  nic_id=0
  for nic in "${nics[@]}"; do
    echo "$nic"
    nad=$(get_nad_by_nic_index "$nic_id")
    nic_group=$(node="$node" nic="$nic" yq --unwrapScalar '.nodes[] | select(.name == env(node)) | .networkInterfaces[] | select(.name == env(nic)) | .v2Group // ""' "${CLUSTER_CONFIG}")
    if [[ $nic_group != "" ]]; then
      group="$nic_group"
    fi
    # for v1, BR don't have group setup
    if [[ $group == "" ]]; then
      continue
    fi
    create_net_attach_config "${nad}" "$group" "$role" "$nic" "${CLUSTER_CONFIG}" "${gen_dir}/template.conf"
    jq -r --arg name "${nad}" \
      '.name = $name' "${gen_dir}/template.conf" >"${gen_dir}/${nad}.conf"
    cp "${gen_dir}/${nad}.conf" "${dest_dir}/${nad}.conf"
    nic_id=$((nic_id + 1))
  done
}

create_net_attaches() {
  nodes=$(yq --unwrapScalar "[.nodes[].name] | @csv" "${CLUSTER_CONFIG}")
  IFS=', ' read -r -a nodes <<<"$nodes"
  for no in "${nodes[@]}"; do
    create_node_net_attach_def "$no" "${WORKDIR}/${no}/dest_dir"
  done
}

main() {
  dest_dir="$1"
  node=$(hostname)
  DEFAULT_NET_ATTACH=${DEFAULT_NET_ATTACH:-"multus-data-net"} \
    WORKDIR=${WORKDIR:-"/tmp/cluster/multus"} \
    CLUSTER_CONFIG=${CLUSTER_CONFIG:-"${WORKDIR}/cluster.yaml"} \
    create_node_net_attach_def "$node" "$dest_dir" "$2"
}

cd "$(dirname "$0")"
source "./common.sh"

if [ "$#" != 1 ] && [ "$#" != 2 ]; then
  echo "$USAGE"
  exit 1
elif [ "$1" = "create_net_attaches" ]; then
  # execute stand-alone for tests
  DEFAULT_NET_ATTACH=${DEFAULT_NET_ATTACH:-"multus-data-net"} \
    WORKDIR=${WORKDIR:-"/tmp/cluster/multus"} \
    CLUSTER_CONFIG=${CLUSTER_CONFIG:-"${WORKDIR}/cluster.yaml"} \
    SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE:-"job-operator"} \
    create_net_attaches
else
  main "$1" "$2"
fi
