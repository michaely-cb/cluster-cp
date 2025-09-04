#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"
SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE:-job-operator}

kubectl rollout status deploy -n"$SYSTEM_NAMESPACE" job-operator-controller-manager --timeout 1s
echo "job-operator deployed"
kubectl rollout status deploy -n"$SYSTEM_NAMESPACE" cluster-server --timeout 1s
echo "cluster-server deployed"

if kubectl get no | grep NotReady; then
  echo "Error: detected not ready k8s node"
  exit 1
fi
echo "all k8s nodes healthy"

cluster_server_repo=$(helm get values -n"$SYSTEM_NAMESPACE" cluster-server -ojson | jq .image.repository -r)
cluster_server_tag=$(helm get values -n"$SYSTEM_NAMESPACE" cluster-server -ojson | jq .image.tag -r)
job_operator=$(kubectl get deploy -n"$SYSTEM_NAMESPACE" job-operator-controller-manager -ojson | jq .spec.template.spec.containers[0].image -r)
cilium=$(cilium version | grep running | cut -d'v' -f2)
kubeadm=$(kubeadm version -ojson | jq .clientVersion.gitVersion -r)

echo "cluster-server version: $cluster_server_repo:$cluster_server_tag"
echo "job-operator version: $job_operator"
echo "cilium version: $cilium"
echo "kubeadm version: $kubeadm"
kubeadm config images list --kubernetes-version="$(kubelet --version | awk '{print $2}' | sed 's/^v//')"

if [ "$(echo "$job_operator" | cut -d':' -f2)" != "$cluster_server_tag" ]; then
  echo "ERROR: job-operator version inconsistent with cluster-server"
  exit 1
fi
echo "cluster-server/job-operator versions consistent"

volume_tool="/opt/cerebras/tools/cluster-volumes.sh"
if [ ! -f $volume_tool ]; then
  echo "ERROR: cluster volume tool $volume_tool does not exist. Please redeploy cluster tools."
  exit 1
fi

venv_volumes_configured=$($volume_tool get --allow-venv | sed -n '1!p' | wc -l)

if [ $venv_volumes_configured -eq 0 ]; then
  volumes_configured=$($volume_tool get | sed -n '1!p' | wc -l)
  if [ $volumes_configured -gt 0 ]; then
    log_level="ERROR"
  else
    log_level="WARNING"
  fi

  echo "$log_level: required at least one volume with --allow-venv but none was configured."
  echo "Please create or reconfigure a volume with the '--allow-venv' option."
  echo ""
  echo "Example:"
  echo "  /opt/cerebras/tools/cluster-volumes.sh put nfs-share --server 10.0.0.0 --server-path /wsc --container-path /nfs/wsc --allow-venv"
  echo ""
  echo "    note: --container-path PATH must be accessible on the usernode at PATH as the appliance client"
  echo "    will copy files there during execution for usage by cluster components."
  echo ""
  echo "For more information, consult the Wafer Scale Cluster Admin Guide."
  echo "For customer site deployments, consult the customer and set up a venv-allowed cluster volume to their preference."

  if [ $log_level = "ERROR" ]; then
    exit 1
  fi
fi

echo "Software validation success!"
