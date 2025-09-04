#!/bin/bash

set -eo pipefail

echo "NAMESPACE: ${NAMESPACE}"
echo "WSJOB_ID: ${WSJOB_ID}"

# New common directories should follow permissions of the mount dir.
# For example, if a user creates a job in a new namespace, we need to
# make sure the new namespace directory is still writable by other users.
# Similar idea applies to relative compile dirs.
function mkdir_following_permission() {
  mount_dir=$1
  relative_path=$2

  if [ -z "${relative_path}" ]; then
    return
  fi

  timeout 5 mkdir -p "${mount_dir}/${relative_path}" || true

  # Traverse through each directory in the relative path
  current="${mount_dir}"
  IFS='/' read -ra DIRS <<< "${relative_path}"

  for dir in "${DIRS[@]}"; do
    current="${current}/${dir}"
    parent=$(dirname "$current")

    # Get parent directory's permissions
    if [ -d "$parent" ]; then
      PERMISSIONS=$(stat -c "%a" "$parent" 2>/dev/null)
      [ -n "$PERMISSIONS" ] && chmod "$PERMISSIONS" "$current" || true
    fi
  done
}

if [ -n "${WORKDIR_MOUNT_DIR_ROOT}" ]; then
  echo "WORKDIR_MOUNT_DIR_ROOT: ${WORKDIR_MOUNT_DIR_ROOT}"
  if [ ! -d ${WORKDIR_MOUNT_DIR_ROOT}/${NAMESPACE} ]; then
    mkdir_following_permission ${WORKDIR_MOUNT_DIR_ROOT} ${NAMESPACE}
  fi

  echo "$REPLICA_MAP" | jq -r 'to_entries[] | "\(.key) \(.value)"' | while read replica_type num_replicas; do
    i=0
    while [ $i -lt ${num_replicas} ]; do
      workdir="${WORKDIR_MOUNT_DIR_ROOT}/${NAMESPACE}/${WSJOB_ID}/$replica_type-$i"
      echo "Create workdir: ${workdir}"
      timeout 5 mkdir -p ${workdir}
      i=$((i + 1))
    done
  done
fi

if [ -n "${COMPILE_MOUNT_DIR_ROOT}" ] && [ -n "${RELATIVE_COMPILE_DIR}" ]; then
  echo "COMPILE_MOUNT_DIR_ROOT: ${COMPILE_MOUNT_DIR_ROOT}"
  echo "RELATIVE_COMPILE_DIR: ${RELATIVE_COMPILE_DIR}"

  compile_dir="${COMPILE_MOUNT_DIR_ROOT}/${NAMESPACE}/${RELATIVE_COMPILE_DIR}"
  if [ ! -d ${compile_dir} ]; then
    echo "Create compile dir: ${compile_dir}"
    mkdir_following_permission ${COMPILE_MOUNT_DIR_ROOT} ${NAMESPACE}/${RELATIVE_COMPILE_DIR}
  fi
fi
