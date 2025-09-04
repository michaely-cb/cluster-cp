#!/bin/bash

set -eo pipefail

current_date=$(date -u +"%Y-%m-%d")
exec >>/tmp/nvme-target-udev-rename-${current_date}.log 2>&1

echo ""
date -u

CLUSTER_YAML="/opt/cerebras/cluster/cluster.yaml"
if [ -f "${CLUSTER_YAML}.staging" ]; then
  CLUSTER_YAML="${CLUSTER_YAML}.staging"
fi
HOSTNAME=$(hostname -s)

target_id=$(yq eval ".nodes[] | select(.name == \"$HOSTNAME\") | .properties.nvme-of-target-id" "${CLUSTER_YAML}")
if [ "$target_id" == "null" ]; then
  echo "ERROR: '${HOSTNAME}' has not been configured as an NVMe-oF target."
  exit 1
fi

echo "DEVNAME: $DEVNAME"
devname="${DEVNAME#/dev/}"
if [ $# -lt 1 ]; then
  echo "Usage (by udev rules): $0 ID of the local disk"
  exit 1
fi

check_disk_size() {
  local_devname="$1"
  if [ ! -b /dev/${local_devname} ]; then
    echo "Device /dev/${local_devname} not found"
    return 1
  fi
  block_size=$(cat /sys/class/block/${local_devname}/queue/physical_block_size)
  num_blocks=$(cat /sys/class/block/${local_devname}/size)
  disk_size=$((block_size * num_blocks))
  # Note that non-POR clusters like perfdrop may not have such big SSDs
  # Deploying NVMe-oF requires to manually comment out the following check
  if [ $disk_size -lt 1000000000000 ]; then
    return 1
  fi
  return 0
}

# Go over all nvmeXn1 dev nodes and find the ones that have more than 1TB capacity
# then count them and return success only if the count matches the given ID (which is
# given as an arg in udev rule)
arg_id=$1
data_nvme_it=1
for i in {0..10}; do
  nvme_name="nvme${i}n1"
  if check_disk_size $nvme_name; then
    if [ $data_nvme_it -eq $arg_id ] && [ "$devname" = "$nvme_name" ]; then
      chmod 666 $DEVNAME
      ln -sf $DEVNAME /dev/datanvme${target_id}0${arg_id}
      exit 0
    fi
    data_nvme_it=$((data_nvme_it + 1))
  fi
done
exit 1
