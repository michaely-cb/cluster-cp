#!/usr/bin/env bash

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"
source "../pkg-common.sh"

set -e

num_kafka_nodes=$(get_num_kafka_nodes)

# Set kafka-node property in cluster.yaml if it is not set, so that the future upgrade will choose
# the same set of kafka nodes. Also, use the current nodes with the label of kafka-node=
# as the kafka nodes, to handle the existing clusters which do not have this property set in
# cluster.yaml.
if [ -z "${KAFKA_NODES}" ]; then
  labeled_nodes=$(kubectl get nodes -lkafka-node= -ojson | jq -r '.items[] | .metadata.name')
  if [ -z "${labeled_nodes}" ]; then
    kafka_no=0
    for node in $(get_all_mgmt_nodes); do
      yq -i ".nodes[$kafka_no].properties.kafka-node = \"\"" ${CLUSTER_CONFIG}
      kafka_no=$((kafka_no + 1))
      if [ "$kafka_no" == "$num_kafka_nodes" ]; then
        break
      fi
    done
  else
    for node in ${labeled_nodes}; do
      yq -i "(.nodes[] | select(.name == \"$node\") | .properties.kafka-node) = \"\"" ${CLUSTER_CONFIG}
    done
  fi

  update_cluster_cm
  KAFKA_NODES=$(yq -r '.nodes[] | select(.properties.kafka-node == "") | .name // ""' ${CLUSTER_CONFIG} | xargs)
  export KAFKA_NODES="${KAFKA_NODES}"
fi

# label management node with 'kafka-node=' so that kafka installation
# will only use these nodes for deployment.
#
# If nodes have `kafka-node` property set, we will use these nodes as kafka nodes.
# Otherwise, we choose the first few management nodes as kafka nodes.
if [ -z "${KAFKA_NODES}" ]; then
  echo "${CLUSTER_CONFIG} should have nodes with properties.kafka-node="
  exit 1
else
  # unlabel the kafka-node to handle removed nodes as well
  for node in $(kubectl get nodes -lkafka-node= -ojson | jq -r '.items[] | .metadata.name'); do
    kubectl label node $node kafka-node-
  done
  for node in ${KAFKA_NODES}; do
    kubectl label node $node kafka-node= --overwrite
  done
fi
