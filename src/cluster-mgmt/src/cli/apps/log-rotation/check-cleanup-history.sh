#!/usr/bin/env bash

set -eo pipefail

show_all=false
cleanup_dir="/n1/cluster-mgmt/cleanup"
metadata_dir_prefix="metadata"
if [ -n "${in_smoke_test}" ]; then
  cleanup_dir=$(dirname "$0")
  metadata_dir_prefix="smoke-test"
fi

show_help() {
  echo "Usage: $(basename "$0") [--compile-dir <path>] [--cbcore-tag <tag>] [--custom-worker-tag <tag>] [--cluster-server-tag <tag>] [--job-operator-tag <tag>] [--show-all]"
  echo ""
  echo "Options:"
  echo "  --compile-dir <path>  	Specify a compile dir to check against"
  echo "  --cbcore-tag <tag>		Specify a cbcore image tag to check against"
  echo "  --custom-worker-tag <tag>	Specify a custom worker image tag to check against"
  echo "  --cluster-server-tag <tag>	Specify a cluster server image tag to check against"
  echo "  --job-operator-tag <tag>	Specify a job operator image tag to check against"
  echo "  --show-all            	Show all cleanup history related to an artifact or an image"
  echo "  --help                	Show this help message and exit"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --help)
      show_help
      exit 0
      ;;
    --compile-dir|--cbcore-tag|--custom-worker-tag|--cluster-server-tag|--job-operator-tag)
      if [[ -n "$2" ]]; then
        option="$1"
        if [ "$option" == "--compile-dir" ]; then
          compile_dir="$2"
        elif [ "$option" == "--cbcore-tag" ]; then
          cbcore_tag="$2"
        elif [ "$option" == "--custom-worker-tag" ]; then
          custom_worker_tag="$2"
        elif [ "$option" == "--cluster-server-tag" ]; then
          cluster_server_tag="$2"
        elif [ "$option" == "--job-operator-tag" ]; then
          job_operator_tag="$2"
        fi
        shift
      else
        echo "Error: $1 option requires an argument."
        exit 1
      fi
      ;;
    --show-all)
      show_all=true
      ;;
    *)
      echo "Error: Invalid option '$1'"
      show_help
      exit 1
      ;;
  esac
  shift
done

check_history() {
  local option="$1"
  local value="$2"

  local cleanup_history=""
  local last_known_plan=""
  if [ "$option" == "compile" ]; then
    cleanup_history=$(find ${cleanup_dir}/logs -type f -name "*cached_compile*status.json" -path "${cleanup_dir}/logs/${metadata_dir_prefix}*" | sort -r)
  else
    cleanup_history=$(find ${cleanup_dir}/registry -type f -name "registry_cleanup_status.json" -path "${cleanup_dir}/registry/${metadata_dir_prefix}*" | sort -r)
  fi

  local entry=""
  while IFS= read -r cleanup_status; do
    if [ "$option" == "compile" ]; then
      entry=$(jq -r ".logs | map(select(.path | contains(\"${compile_dir}\"))) | .[0] // null" ${cleanup_status})
    else
      entry=$(jq -r ".images[\"$option\"][\"$value\"] // null" ${cleanup_status})
    fi
    if [ "$entry" == "null" ]; then continue; fi

    local plan=$(jq -r '.plan' <<< "$entry")
    local ok=$(jq -r ".status" <<< "$entry")
    if [ "$plan" != "$last_known_plan" ] || [ "$ok" != "done" ]; then
      echo "Cleanup record from ${cleanup_status}:"
      echo "$entry"
      if [[ "$show_all" == false ]]; then
        break
      fi
      last_known_plan="$plan"
      echo ""
    fi
  done <<< "${cleanup_history}"
}

if [ -z "${in_smoke_test}" ]; then
  lead_mgmt_node=$(kubectl get node -lnode-role.kubernetes.io/control-plane -ojsonpath='{.items[0].metadata.name}')
  if [ "$lead_mgmt_node" != $(hostname -s) ]; then
    echo "ERROR: This script is expected to run on the lead mgmt node $lead_mgmt_node."
    echo "Please copy the script to $lead_mgmt_node and retry."
    exit 1
  fi
fi

if [ -n "$compile_dir" ]; then
  check_history "compile" "$compile_dir"
fi

if [ -n "$cbcore_tag" ]; then
  check_history "cbcore" "$cbcore_tag"
fi

if [ -n "$custom_worker_tag" ]; then
  check_history "custom-worker" "$custom_worker_tag"
fi

if [ -n "$cluster_server_tag" ]; then
  check_history "cluster-server" "$cluster_server_tag"
fi

if [ -n "$job_operator_tag" ]; then
  check_history "job-operator" "$job_operator_tag"
fi
