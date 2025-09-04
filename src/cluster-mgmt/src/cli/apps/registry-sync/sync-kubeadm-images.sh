#!/usr/bin/env bash
set -e

K8S_VERSION=$(kubeadm version -ojson | jq -r '.clientVersion.gitVersion')
echo "Detected Kubernetes version: $K8S_VERSION"
IMAGES=$(kubeadm config images list -ojson | jq -r '.images[]')
echo "$IMAGES" | while read -r image; do
  local_image="${image/#registry.k8s.io/registry.local}"
  echo "pushing image: $local_image"
  nerdctl -nk8s.io push "$local_image" || true
done
