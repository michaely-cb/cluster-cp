#!/bin/bash

set -e

errors='[]'

add_error() {
  local type="$1"
  local message="$2"

  echo "error: $type $msg"
  errors=$(echo "$errors" | jq --arg type "$type" \
    --arg msg "$message" \
    '. += [{
            "error_type": $type,
            "message": $msg
          }]')
}

# run cmd + logging option(default no logging)
run_cmd() {
  local cmd="$1"
  local log="$2"
  local option="&>/dev/null"
  if [ "$log" == "out" ]; then
    option="2>/dev/null"
  elif [ "$log" == "err" ]; then
    option=">/dev/null"
  elif [ "$log" == "all" ]; then
    option=""
  fi
  echo "running cmd: ${cmd}"
  if eval "${cmd}" "${option}"; then
    return 0
  else
    return 1
  fi
}

interface_speed_check() {
  local error_type="NicSpeedDowngraded"
  is_nic_speed_downgraded() {
    local interface=$1
    local default_cmd="ethtool -i $interface | grep bus-info | awk '{print \$2}' | xargs -I {} lspci -s {} -vv 2>/dev/null | grep -A 30 LnkSta: | grep downgraded"
    local cmd="${TEST_INTERFACE_CMD:-$default_cmd}"
    run_cmd "$cmd" "out"
  }

  if [ -z "$interfaces" ]; then
    interfaces=$(yq -r ".nodes[] | select(.name == \"$NODE_NAME\") | .networkInterfaces[].name" /cluster-config/clusterConfiguration.yaml)
  fi
  for interface in $interfaces; do
    if is_nic_speed_downgraded "$interface"; then
      msg="speed downgraded for $interface from lspci, nic needs replacement"
      add_error "$error_type" "$msg"
    else
      echo "$interface speed healthy"
    fi
    # only check first nic for worker/mgmt
    if [ "$(yq -r ".nodes[] | select(.name == \"$NODE_NAME\") | .role" /cluster-config/clusterConfiguration.yaml)" == "worker" ]; then
      break
    fi
    if [ "$(yq -r ".nodes[] | select(.name == \"$NODE_NAME\") | .role" /cluster-config/clusterConfiguration.yaml)" == "management" ]; then
      break
    fi
  done
}

check_host_mounts() {
  local error_type="HostMountHang"
  is_mount_healthy() {
    local mount_path="$1"
    local default_cmd="cd /host && timeout 10 df -h ${mount_path}"
    local cmd="${TEST_HANG_CMD:-$default_cmd}"
    run_cmd "$cmd"
  }

  # check entire filesystem first
  if ! is_mount_healthy; then
    proc_mount_file="${PROC_MOUNT_PATH:-/host/proc/mounts}"
    while IFS= read -r line; do
      IFS=' ' read -r _ mount_path fs_type _ <<<"$line"
      # Check if the filesystem type is one of the specified types
      if [[ "$fs_type" == "nfs" || "$fs_type" == "nfs4" || "$fs_type" == "ceph" ]]; then
        # Check if the mount is not healthy
        if ! is_mount_healthy "$mount_path"; then
          # Adjust mount path
          real_mount_path="${mount_path/\/host/}"
          msg="host mount hang for fs_type $fs_type, mount_path $real_mount_path"
          add_error "$error_type" "$msg"
          break # skip checking to avoid hanging too long
        fi
      fi
    done <"$proc_mount_file"
  else
    echo "all host mounts healthy"
  fi
}

ceph_kernel_log_check() {
  local error_type="CephConnectionError"
  has_ceph_errors() {
    # https://cerebras.atlassian.net/wiki/spaces/runtime/pages/3166175329/Ceph+Runbook
    # https://docs.ceph.com/en/quincy/cephfs/troubleshooting/
    local default_cmd="journalctl -k -S -1m | grep libceph | grep -E 'reset|closed|error'"
    local cmd="${TEST_CEPH_CONNECTION_CMD:-$default_cmd}"
    run_cmd "$cmd" "out"
  }

  if has_ceph_errors; then
    msg="kernel logs showing ceph connection issues"
    add_error "$error_type" "$msg"
  else
    echo "ceph_kernel_log_check passed"
  fi
}

error_file="${ERROR_OUTPUT_FILE:-/errors.json}"
if [ -f "${error_file}" ]; then
  current_time=$(date +%s)
  file_mod_time=$(stat -c %Y "${error_file}")
  time_diff=$((current_time - file_mod_time))
  # Check if the file was modified within the last minute (60 seconds)
  if [ "$time_diff" -lt 60 ]; then
    echo "already checked within last minute, skip"
    exit
  fi
  mv "${error_file}" "${error_file}.old"
  echo "[]" >"${error_file}"
fi

interface_speed_check
ceph_kernel_log_check
check_host_mounts
echo "${errors}" >"${error_file}"
