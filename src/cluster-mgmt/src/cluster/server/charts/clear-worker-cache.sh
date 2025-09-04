#!/bin/bash
set -ex

if [ -x /usr/local/bin/pssh-async ]; then
    PSSH_PATH=/usr/local/bin/pssh-async
else
    PSSH_PATH=/usr/bin/pssh
fi

# pssh helper funcs
function retry_pssh {
  attempt=0
  max=2
  while true; do
    "$@" && break
    if [ $? -ne 1 ]; then
      break
    fi
    attempt=$((attempt + 1))
    if [[ $attempt -gt $max ]]; then
      return 1
    fi
    echo "Command failed, retry $attempt/$max"
    sleep 1
  done
}
MAX_PSSH_THREADS=${MAX_PSSH_THREADS:-10}
function PSSH {
  retry_pssh $PSSH_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h /opt/cerebras/cluster/node_list --inline "$@"
}

# Get all the worker nodes in the cluster and save to a file.
mkdir -p /opt/cerebras/cluster

node_selector="k8s.cerebras.com/node-role-worker="

# If namespace node labels are enabled, we should add an additional node selector.
if [ $(kubectl get nodes --selector="k8s.cerebras.com/namespace" --ignore-not-found) != "" ]; then
  node_selector="$node_selector;k8s.cerebras.com/namespace=$NAMESPACE"
fi

kubectl get nodes --selector="$node_selector" -o=json | jq -r '.items[] | (.status.addresses// [])[] | select(.type == "InternalIP") | .address' > /opt/cerebras/cluster/node_list

# Delete cache on all workers.
PSSH "rm -fr /n0/cache/* 2>&1"
