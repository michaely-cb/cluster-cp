#!/bin/bash

if [ -z "${FORCE_DISABLE}" ]; then
  current_date=$(date -u +"%Y-%m-%d")
  exec >>"/tmp/nvme-initiator-enable-${current_date}.log" 2>&1
  echo ""
fi
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

target_ids=$(yq eval '.nodes[] | select(.properties.nvme-of-target-id != null) | .properties.nvme-of-target-id' ${CLUSTER_CONFIG})
port=4420

set -e

# We need to load these modules for the following setup to succeed
echo "NVMe modules loaded:"
lsmod_outuput=$(lsmod)
echo "$lsmod_outuput" | awk '{print $1}' | grep -w nvme
echo "$lsmod_outuput" | awk '{print $1}' | grep -w nvmet
echo "$lsmod_outuput" | awk '{print $1}' | grep -w nvmet_rdma
echo "$lsmod_outuput" | awk '{print $1}' | grep -w nvme_fabrics

set +e

# List the IDs/IPs/ports of coordinator nodes this MemX might interact with, new
# IDs can be added without a reboot, just add the new ID, new IP and new port, and
# eventually save the file. No further action is needed.

disconnect_namespace() {
  local ns_suffix=$1
  nvme disconnect -n "nvmeMgmtOffload_${ns_suffix}" 2>/dev/null
}

connect_namespace() {
  local ns_suffix=$1
  local name="nvmeMgmtOffload_${ns_suffix}"
  nvme connect -n $name -t rdma -a $target_address -s $port
  if [ $? -ne 0 ]; then
    echo "Failed to connect to nvme $ns_suffix"
  else
    echo "Successfully connected to $name"
  fi
}

check_all_namespaces_connected() {
  local -n namespaces="$1"
  local all_connected=true

  for ns in "${namespaces[@]}"; do
    if ! nvme list | awk '{print $4}' | grep -w -q "${ns}"; then
      echo "Namespace ${ns} is not properly connected."
      all_connected=false
    fi
  done
  $all_connected && return 0 || return 1
}

while IFS= read -r target_id; do
  mgmt_node=$(yq eval ".nodes[] | select(.properties.nvme-of-target-id == \"$target_id\") | .name" ${CLUSTER_CONFIG})
  target_address=$(yq eval ".nodes[] | select(.properties.nvme-of-target-id == \"$target_id\") | .networkInterfaces[0].address" ${CLUSTER_CONFIG})
  expected_namespaces=("${target_id}01" "${target_id}02" "${target_id}03")
  echo "Trying to connect to mgmt node $mgmt_node (id: $target_id), ip: $target_address, port: $port"

  if [ -n "$FORCE_DISABLE" ]; then
    echo "'FORCE_DISABLE' was set to ${FORCE_DISABLE}. Going to tear down the NVMe-oF initiator setup."
    for ns in "${expected_namespaces[@]}"; do
      disconnect_namespace $ns
    done
    continue
  elif check_all_namespaces_connected expected_namespaces; then
    echo "All expected namespaces for mgmt node $mgmt_node (id: $target_id) are properly connected."
    continue
  fi

  for ns in "${expected_namespaces[@]}"; do
    connect_namespace $ns
  done

  # There can be a few seconds lag before `nvme list` shows the newly connected devices.
  sleep 5
  if ! check_all_namespaces_connected expected_namespaces; then
    echo "Not all namespaces for mgmt node $mgmt_node (id: $target_id) are properly connected. Discard the connected namespaces."
    for ns in "${expected_namespaces[@]}"; do
      disconnect_namespace $ns
    done
  else
    echo "Successfully connected all namespaces for mgmt node $mgmt_node (id: $target_id)."
  fi
done <<< "$target_ids"

# Clean up invalid renamed devices
find /dev -maxdepth 1 -mindepth 1 -name 'datanvme*' | while read -r renamed_device; do
  if [ -L "$renamed_device" ] && [ ! -e "$renamed_device" ]; then
    echo "Warning: $renamed_device is removed since broken."
    echo "This could happen when the nvme-of target went offline or the connections on the initiator were forcefully shut down."
    rm -f $renamed_device
  else
    real_device=$(realpath $renamed_device)
    namespace=$(nvme list --output json | jq -r ".Devices[] | select(.ModelNumber == \"Linux\" and .DevicePath == \"${real_device}\") | .NameSpace")
    if [ "$renamed_device" != "/dev/datanvme$namespace" ]; then
      echo "Warning: $renamed_device points to $real_device, though $real_device belongs to namespace $namespace."
      echo "This could happen to an incomplete and/or stale setup."
      rm -f $renamed_device
    fi
  fi
done

if [ -z "$FORCE_DISABLE" ]; then
  yq eval "(.nodes[] | select(.name == \"${HOSTNAME}\") | .properties.nvme-of-initiator) = \"\"" ${CLUSTER_CONFIG} > ${CLUSTER_CONFIG}.staging
fi
echo "All done!"
