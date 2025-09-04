#!/bin/bash

set -eo pipefail

current_date=$(date -u +"%Y-%m-%d")
exec >>/tmp/nvme-initiator-udev-rename-${current_date}.log 2>&1

echo ""
date -u

CLUSTER_YAML="/opt/cerebras/cluster/cluster.yaml"
INCREMENTAL_CLUSTER_YAML="/opt/cerebras/cluster/incremental-cluster.yaml"

if [[ -f "${CLUSTER_YAML}" && -f "${INCREMENTAL_CLUSTER_YAML}" ]]; then
  # if both files exist, use the one which was most recently last modified
  if [[ "${INCREMENTAL_CLUSTER_YAML}" -nt "${CLUSTER_YAML}" ]]; then
    CLUSTER_CONFIG="${INCREMENTAL_CLUSTER_YAML}"
  else
    CLUSTER_CONFIG="${CLUSTER_YAML}"
  fi
elif [[ -f "${CLUSTER_YAML}" ]]; then
  CLUSTER_CONFIG="${CLUSTER_YAML}"
elif [[ -f "${INCREMENTAL_CLUSTER_YAML}" ]]; then
  CLUSTER_CONFIG="${INCREMENTAL_CLUSTER_YAML}"
else
  echo "ERROR: Not ready for NVMe-oF configuration since neither ${CLUSTER_YAML} nor ${INCREMENTAL_CLUSTER_YAML} exists."
  exit 1
fi

max_coordinator_nodes=$(yq e '.nodes | map(select(
  .role == "management" and
  .properties.storage-type != "ceph")) | length' ${CLUSTER_CONFIG})

echo "DEVNAME: $DEVNAME"
devname="${DEVNAME#/dev/}"
if [ $# -lt 1 ]; then
  echo "Usage (by udev rules): $0 ID of the rule"
  exit 1
fi

nsid=$(cat /sys/class/block/${devname}/nsid)
i=1
while [ $i -le $max_coordinator_nodes ]; do
  arg_nsid="${i}$1"
  echo "arg_nsid: $arg_nsid nsid: $nsid"

  if [[ "$nsid" -eq "$arg_nsid" ]]; then
    echo "MATCH of $nsid for device $DEVNAME"
    ln -sf $DEVNAME /dev/datanvme$arg_nsid
    exit 0
  else
    echo "MISMATCH of $nsid for device $DEVNAME"
    i=$((i + 1))
  fi
done

exit 1
