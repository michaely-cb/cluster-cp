#!/bin/bash

set -eo pipefail

if [ -z "$FORCE_DISABLE" ]; then
  current_date=$(date -u +"%Y-%m-%d")
  exec >>"/tmp/nvme-target-enable-${current_date}.log" 2>&1
  echo ""
fi

date -u

CLUSTER_DIR="/opt/cerebras/cluster"
CLUSTER_YAML="${CLUSTER_DIR}/cluster.yaml"
INCREMENTAL_CLUSTER_YAML="${CLUSTER_DIR}/incremental-cluster.yaml"
# NVMe targets cache is agnostic to incremental deploy
TARGETS_YAML="${CLUSTER_DIR}/nvme-targets.yaml"

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

NVMET_CONFIG_DIR="/sys/kernel/config/nvmet"
HOSTNAME=$(hostname -s)
remaining_coordinator_nodes=$(
  yq eval '
    .nodes |
    map(select(
      .role == "management" and
      .properties.storage-type != "ceph" and
      .properties.nvme-of-target-id == null
    )).[] |
    .name // ""' "${CLUSTER_CONFIG}" | sort -h
)

data_net_ip=$(yq eval ".nodes[] | select(.name == \"${HOSTNAME}\") | .networkInterfaces[0].address" ${CLUSTER_CONFIG})
selected_ip_port="4420"
subsystem_port_id="15"
subsystem_ns_base="nvmeMgmtOffload"
expected_local_nvmes=("nvme0n1" "nvme1n1" "nvme2n1")

if echo "$remaining_coordinator_nodes" | grep -wq "$HOSTNAME"; then
  echo "Going to configure '${HOSTNAME}' as a NVMe-oF target."
else
  target_id=$(yq eval ".nodes[] | select(.name == \"$HOSTNAME\") | .properties.nvme-of-target-id" "${CLUSTER_CONFIG}")
  if [ "$target_id" != "null" ]; then
    if [ -n "$FORCE_DISABLE" ]; then
      echo "'FORCE_DISABLE' was set to ${FORCE_DISABLE}. Going to tear down the NVMe-oF target setup."
    elif [ $(find ${NVMET_CONFIG_DIR}/subsystems -mindepth 1 -maxdepth 1 -type d | wc -l) -ne 3 ]; then
      echo "'${HOSTNAME}' was once configured as an NVMe-oF target and the target ID is $target_id."
      echo "The NVMe configuration was however missing, possibly due to a Mellanox driver restart."
      echo "Going to configure '${HOSTNAME}' as a NVMe-oF target again."
    else
      echo "'${HOSTNAME}' has already been configured as an NVMe-oF target and the target ID is $target_id."
      exit
    fi
  else
    # This branch should not happen since the Ansible script would guard against this, though it's an unharmful sanity check.
    echo "ERROR: '${HOSTNAME}' cannot be configured as an NVMe-oF target. NVMe-oF targets need to be non-ceph management nodes."
    exit 1
  fi
fi

# Function to return the target ID
assign_target_id() {
  local assigned_target_id=$(yq eval ".[\"${HOSTNAME}\"]" ${TARGETS_YAML})
  if [ "$assigned_target_id" != "null" ]; then
    echo "$assigned_target_id"
  else
    local assigned_target_ids=$(yq eval '. | to_entries | .[].value' "${TARGETS_YAML}" | sort -h)

    # Initialize a counter for the nvme-of-target-id

    local nvme_of_target_id=1
    # Iterate over the remaining coordinator nodes, find the current node and assign a target id
    for node in $remaining_coordinator_nodes; do
      # Find the next available nvme-of-target-id
      while echo "$assigned_target_ids" | grep -wq "$nvme_of_target_id"; do
        ((nvme_of_target_id++))
      done
      if [[ "${node}" != "${HOSTNAME}" ]]; then
        ((nvme_of_target_id++))
        continue
      fi
      break
    done
    echo "${nvme_of_target_id}"
  fi
}

create_subsystem() {
  local subsystem_path="${NVMET_CONFIG_DIR}/subsystems/$1"
  if [ ! -d "$subsystem_path" ]; then
    mkdir "$subsystem_path"
    echo 1 > "$subsystem_path/attr_allow_any_host"
    echo 1 > "$subsystem_path/attr_offload"
  fi
}

configure_namespace() {
  local namespace_path="$1/namespaces/$2"
  if [ ! -d "$namespace_path" ]; then
    mkdir "$namespace_path"
    echo -n $3 > "$namespace_path/device_path"
    echo 1 > "$namespace_path/enable"
  fi
}

setup_port() {
  local port_path="${NVMET_CONFIG_DIR}/ports/$1"
  if [ ! -d "$port_path" ]; then
    mkdir "$port_path"
    echo $2 > "$port_path/addr_trsvcid"
    echo $3 > "$port_path/addr_traddr"
    echo "rdma" > "$port_path/addr_trtype"
    echo "ipv4" > "$port_path/addr_adrfam"
  fi
}

teardown_namespace() {
  local namespace_path="$1/namespaces/$2"
  if [ -d "$namespace_path" ]; then
    echo 0 > "$namespace_path/enable"
  fi

  local port_link="${NVMET_CONFIG_DIR}/ports/$subsystem_port_id/subsystems/${subsystem_ns_base}_${2}"
  if [ -L "$port_link" ]; then
    rm "$port_link"
  fi

  if [ -d "$namespace_path" ]; then
    rmdir "$namespace_path"
  fi
}

teardown_subsystem() {
  local subsystem_path="${NVMET_CONFIG_DIR}/subsystems/$1"
  if [ -d "$subsystem_path" ]; then
    rmdir "$subsystem_path"
  fi
}

teardown_port() {
  local port_path="${NVMET_CONFIG_DIR}/ports/$1"
  if [ -d "$port_path" ]; then
    rmdir "$port_path"
  fi
}

configure_system() {
  local device_path="$1"
  local namespace_id="$2"
  local namespace_path="${subsystem_ns_base}_${namespace_id}"
  create_subsystem $namespace_path
  configure_namespace "${NVMET_CONFIG_DIR}/subsystems/$namespace_path" "$namespace_id" "$device_path"
  local subsystem_link_path="${NVMET_CONFIG_DIR}/ports/$subsystem_port_id/subsystems/$namespace_path"
  if [ ! -L "$subsystem_link_path" ]; then
    ln -s "${NVMET_CONFIG_DIR}/subsystems/$namespace_path" "$subsystem_link_path"
  fi
}

target_id=$(assign_target_id)
expected_namespaces=("${target_id}01" "${target_id}02" "${target_id}03")
echo "Assigned target ID: $target_id"

# Cleanup existing configuration
for ns in "${expected_namespaces[@]}"; do
  echo "Tearing down namespace $ns"
  teardown_namespace "${NVMET_CONFIG_DIR}/subsystems/${subsystem_ns_base}_${ns}" $ns
  echo "Tearing down subsystem ${subsystem_ns_base}_${ns}"
  teardown_subsystem "${subsystem_ns_base}_${ns}"
done

echo "Tearing down port $subsystem_port_id"
teardown_port "$subsystem_port_id"

echo "NVMe-oF target setup has been torn down."

if [ -n "$FORCE_DISABLE" ]; then
  exit 0
fi

# We need to load these modules for the following setup to succeed
echo "NVMe modules loaded:"
lsmod_outuput=$(lsmod)
echo "$lsmod_outuput" | awk '{print $1}' | grep -w nvme
echo "$lsmod_outuput" | awk '{print $1}' | grep -w nvmet
echo "$lsmod_outuput" | awk '{print $1}' | grep -w nvmet_rdma
echo "$lsmod_outuput" | awk '{print $1}' | grep -w nvme_fabrics

echo "Creating port on ${data_net_ip}:${selected_ip_port}"
setup_port "$subsystem_port_id" "$selected_ip_port" "$data_net_ip"

for i in "${!expected_namespaces[@]}"; do
  ns=${expected_namespaces[i]}
  nvme_name=${expected_local_nvmes[i]}
  echo "Creating subsystem/ns ${subsystem_ns_base}_${ns}"
  configure_system "/dev/${nvme_name}" $ns
done

# Write the target id to a staging cluster config
# This is needed so Ansible can later get newly deployed target ID and formally update the cluster config.
yq eval "(.nodes[] | select(.name == \"${HOSTNAME}\") | .properties.nvme-of-target-id) = \"$target_id\"" ${CLUSTER_CONFIG} > ${CLUSTER_CONFIG}.staging
echo "All done!"

