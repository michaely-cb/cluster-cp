#!/usr/bin/env bash

# Note: This script can be used to tear down the Kafka statefulset.
# Developers could also optionally delete the PVs that back the statefulset.

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"
source "../pkg-common.sh"

set -e

ns=kafka
release_name=kafka

if helm -n ${ns} list | grep -q "${release_name}"; then
  echo "cleaning up the helm release"
  helm -n ${ns} uninstall ${release_name} --timeout 1m
  echo "helm release successfully cleaned up"
else
  echo "release '${release_name}' does not exist in the '${ns}' namespace"
fi

delete_pv=${delete_pv:-}
if [ -n "${delete_pv}" ]; then
  echo "cleaning up all jobs in the namespace that may use the pvcs"
  kubectl -n ${ns} delete job --all

  echo "cleaning up all pods in the namespace that may use the pvcs"
  kubectl -n ${ns} delete pod --all

  echo "cleaning up the kafka persistence layer"
  kubectl -n ${ns} delete pvc --all

  pvs=$(kubectl get pv -l app.kubernetes.io/name=kafka --no-headers | awk '{print $1}')
  for pv in ${pvs}; do
    kubectl patch pv ${pv} -p '{"spec":{"claimRef": null}}'

    hostpath=$(kubectl get pv ${pv} -o jsonpath='{.spec.hostPath.path}')
    kubectl delete pv ${pv}
  done

  if [ -n "${hostpath}" ]; then
    kubectl get no -lkafka-node= --no-headers | awk '{print $1}' | xargs -I {} bash -ec "echo -n going to delete ${hostpath} on {}... ; ssh {} rm -rf ${hostpath}; echo done"
  fi
  echo "kakfa persistence layer successfuly cleaned up"
fi
