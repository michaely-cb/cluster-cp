#!/usr/bin/env bash

source "../pkg-common.sh"

set -e

registry_image_full_path="${registry_url}/${registry_image}"

# Remove legacy deploy in order to replace with new daemonset
if kubectl get deploy -n kube-system private-registry &>/dev/null; then
  kubectl delete deploy -n kube-system private-registry
fi

cp -f registry-template.yaml registry.yaml

yq eval -i '.spec.template.spec.containers[0].image = "'${registry_image_full_path}'"' registry.yaml
if has_multiple_mgmt_nodes; then
  yq eval -i '.spec.template.spec.volumes +=
       [{"name": "registry-vol", "persistentVolumeClaim": {"claimName": "registry-pvc"}}]' registry.yaml
else
  yq eval -i '.spec.template.spec.volumes +=
       [{"name": "registry-vol", "hostPath": {"path": "/n0/cluster-mgmt/registry", "type": "DirectoryOrCreate"}}]' registry.yaml
fi

kubectl apply -f registry.yaml
kubectl apply -f registry-service-monitor.yaml
kubectl rollout status -f registry.yaml --timeout=2m

curl -sS --fail --retry-delay 1 --retry 3 --retry-connrefused --connect-timeout 10 "${MGMT_NODE_DATA_IP}":5001/debug/health
echo "private registry container started" >&2
