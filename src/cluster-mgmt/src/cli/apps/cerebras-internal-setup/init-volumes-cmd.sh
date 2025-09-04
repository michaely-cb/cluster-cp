#!/usr/bin/env bash

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"
source "../pkg-common.sh"

USAGE=$(cat <<EOF
$0 [namespace]
Create configmap for volumes in a namespace. If no namespace is provided,
$SYSTEM_NAMESPACE namespace is used.
EOF
)

set -e

if [ $# -ne 0 ] && [ $# -ne 1 ]; then
  echo "$USAGE"
  exit 1
fi

if [ $# -eq 1 ]; then
  namespace=$1
else
  namespace=$SYSTEM_NAMESPACE
fi

if ! kubectl get ns $namespace > /dev/null 2>&1; then
  kubectl create ns $namespace
fi

# Allow the caller to create those volumes as non-cerebras-internal volumes
# by override this option. The default is to create internal volumes.
INTERNAL_VOLUME_OPTION="--cerebras-internal"
if [ "$DISABLE_INTERNAL_VOLUME_OPTION" == "true" ]; then
  INTERNAL_VOLUME_OPTION=""
fi

function get_server_path() {
  local container_path=$1
  local server_path="$(df -TP $container_path | grep nfs | cut -d: -f2 | awk '{print $1}')"

  # If the server_path for the given container path is the same as the server path for
  # "/cb", the server_path needs to be amended to include the subdirectories under "/cb".
  local cb_server_path="$(df -TP /cb | grep nfs | cut -d: -f2 | awk '{print $1}')"
  if [ "$server_path" == "$cb_server_path" ]; then
     server_path="$server_path${container_path##*/cb}"
  fi
  echo "$server_path"
}

function add_volume() {
  local volume_name=$1
  local container_path=$2

  if df -T $container_path | grep -q nfs; then
    local nfs_server=$(df -TP $container_path | grep nfs | cut -d: -f1 | tr -d '\n')
    local server_path=$(get_server_path $container_path)
    NAMESPACE=$namespace $CV put "$volume_name" --server "$nfs_server" --container-path "$container_path" \
      --server-path "$server_path" $INTERNAL_VOLUME_OPTION
  fi
}

CV="/opt/cerebras/tools/cluster-volumes.sh"

if [ "true" = "$CEREBRAS_INTERNAL_DEPLOY" ] ; then
  nfs_server=$(df -TP /cb/tests | grep nfs | cut -d: -f1 | tr -d '\n')
  if [ -z "$nfs_server" ]; then
    echo "Error: NFS server not found" >&2
    exit 1
  fi

  if ! cblocal_is_available; then
    container_path_prefix="/cb/tests"
  else
    add_volume cblocal-volume /cblocal
    nfs_server="$(df -TP /cblocal | grep nfs | cut -d: -f1 | tr -d '\n')"
    container_path_prefix="/cblocal"
  fi
  server_path_prefix=$(get_server_path $container_path_prefix)

  add_volume tests-volume /cb/tests
  add_volume home-volume /cb/home
  add_volume training-data-volume /cb/ml
  add_volume customers-volume /cb/customers

  compile_root="/cluster-mgmt/compile_root"
  NAMESPACE=$namespace $CV put cached-compile-volume \
    --server "${nfs_server}" --server-path "${server_path_prefix}${compile_root}" --container-path "${container_path_prefix}${compile_root}" \
    --create-dir-if-not-exists --cached-compile --permission 777 $INTERNAL_VOLUME_OPTION

  workdir_root="/cluster-mgmt/${short_cluster_name}/workdir"
  NAMESPACE=$namespace $CV put workdir-logs-volume \
    --server "${nfs_server}" --server-path "${server_path_prefix}${workdir_root}" --container-path "${container_path_prefix}${workdir_root}" \
    --create-dir-if-not-exists --workdir-logs --permission 777 $INTERNAL_VOLUME_OPTION

  user_venv_root="/cluster-mgmt/${short_cluster_name}/user-venv"
  NAMESPACE=$namespace $CV put user-venv-volume \
    --server "${nfs_server}" --server-path "${server_path_prefix}${user_venv_root}" --container-path "${container_path_prefix}${user_venv_root}" \
    --create-dir-if-not-exists --allow-venv --permission 777 $INTERNAL_VOLUME_OPTION
else
  # best effort delete any internal volumes
  NAMESPACE=$namespace $CV get --cerebras-internal | grep -v NAME | cut -d' ' -f1 | xargs -r -n1 $CV delete || true
fi
