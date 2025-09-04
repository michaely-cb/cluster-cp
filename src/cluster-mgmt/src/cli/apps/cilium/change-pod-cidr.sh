#!/bin/bash

# This script checks and modifies the pod CIDR and mask size
# To be used to increase the size of the
# Steps:
#   - Checks cilium for the current pod CIDR mask size.
#   - Prompts the user to continue after displaying the updated CIDRs
#   - Updates the pod CIDR and mask size in cilium + pkg-properties
#   - Restarts all pods in the cluster and waits until Pods + cilium is ready
# Before running, ensure that the environment variables at the top of the
# script (NEW_CIDR and NEW_MASK_SIZE) are set to the desired new CIDR and mask size.

set -euo pipefail

DEFAULT_CIDR="192.168.128.0/17"
DEFAULT_MASK="26"

read -p "Enter new CIDR or accept default ($DEFAULT_CIDR) by hitting ENTER: " input
NEW_CIDR=${input:-"$DEFAULT_CIDR"}
read -p "Enter new mask size or accept default ($DEFAULT_MASK) by hitting ENTER: " input
NEW_MASK_SIZE=${input:-"$DEFAULT_MASK"}

if ! [[ "$NEW_CIDR" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}/[0-9]{1,2}$ ]] ; then
  echo "invalid CIDR: $NEW_CIDR"
  exit 1
fi

if ! [[ "$NEW_MASK_SIZE" =~ ^[0-9]{1,2}$ ]] ; then
  echo "invalid netmask: $NEW_MASK_SIZE"
  exit 1
fi

quiet() {
  output=$( { "$@" 2>&1; } )
  exit_status=$?
  if [ $exit_status -ne 0 ]; then
    echo "command failed: $@"
    echo "$output"
  fi
  return $exit_status
}

check_pods_ready() {
  # delete all not ready pods
  kubectl get pods --all-namespaces -o json | \
    jq -r '.items[] | select(.status.phase != "Running" or ([ .status.conditions[] | select(.type == "Ready" and .status == "True") ] | length ) == 0 ) | "\(.metadata.namespace) \(.metadata.name)"' | \
    xargs -r -n2 bash -c 'kubectl delete pod $1 -n $0'
  kubectl wait -A --all --for=condition=Ready --timeout=5s pod
}

configmap=$(kubectl get cm -n kube-system cilium-config -o json)
oldPodCidr=$(echo "$configmap" | jq -r '.data."cluster-pool-ipv4-cidr"')
oldMask=$(echo "$configmap" | jq -r '.data."cluster-pool-ipv4-mask-size"')

if [ "$oldMask" -lt 27 ] ; then
  echo "🚨 Existing mask size ($oldMask) sufficiently large already, is this change really needed?"
fi

notReadyPods=$(kubectl get pods --all-namespaces -o json | \
    jq -r '.items[] | select(.status.phase != "Running" or ([ .status.conditions[] | select(.type == "Ready" and .status == "True") ] | length ) == 0 ) | "\(.metadata.namespace) \(.metadata.name)"' | \
    grep -v wsjob || true)
if [ -n "$notReadyPods" ] ; then
  echo "🚨 Some existing pods are not ready"
  echo "$notReadyPods"
fi

oldServiceCidr=$(kubectl get pods -n kube-system -ojson -lcomponent=kube-apiserver | jq -r '.items[0].spec.containers[0].command[] | select(.[0:20] == "--service-cluster-ip")' | cut -d= -f2)

echo "New pod mask: /$NEW_MASK_SIZE"
echo "New pod CIDR: $NEW_CIDR"
echo "Old pod CIDR: $oldPodCidr"
echo "Old svc CIDR: $oldServiceCidr"
echo "🚨 Ensure these pod/service CIDR ($oldServiceCidr and $NEW_CIDR) would not overlap if updated"
read -p "Do you want to continue updating cilium's pod CIDRs? (y/n): " answer
if [[ "$answer" != "y" ]]
then
  exit 0
fi


echo "⚡ Updating config"
quiet kubectl patch cm -n kube-system cilium-config -p '{"data":{"cluster-pool-ipv4-cidr":"'$NEW_CIDR'","cluster-pool-ipv4-mask-size":"'$NEW_MASK_SIZE'"}}'
quiet yq eval -i '.properties.cilium.clusterPoolIPv4PodCIDRList = ["'$NEW_CIDR'"]' /opt/cerebras/cluster/pkg-properties.yaml
quiet yq eval -i '.properties.cilium.clusterPoolIPv4MaskSize = '$NEW_MASK_SIZE'' /opt/cerebras/cluster/pkg-properties.yaml


echo "⚡ Cleanup Cilium resources"
# spin down cilium pods so that we can safely delete the cilium nodes, not having them instantly re-created by pods using the old config
quiet kubectl patch ds cilium -n kube-system -p='{"spec": {"template": {"spec": {"nodeSelector": {"non-existing-label": "true"}}}}}'
sleep 10
quiet kubectl delete ciliumnode --all
quiet kubectl patch ds cilium -n kube-system -p='{"spec": {"template": {"spec": {"nodeSelector": null}}}}'

echo "⚡ Restarting pods"
quiet kubectl delete pods -A --all --force --grace-period=0
echo "⏰ Awaiting pod restart"
errors=0
retries=0
while [ "$retries" -lt 3 ] ; do
  for i in $(seq 0 30) ; do
    remaining=$(kubectl get pods --all-namespaces -o json | \
      jq -r '.items[] | select(.status.phase != "Running" or ([ .status.conditions[] | select(.type == "Ready" and .status == "True") ] | length ) == 0 ) | "\(.metadata.namespace) \(.metadata.name)"' | \
      wc -l)
    echo -en "NotReady: $remaining\r"
    if [ "$remaining" -eq 0 ] ; then
      break
    fi
    sleep 1
  done
  echo ""
  if check_pods_ready ; then retries='' ; break ; fi
  echo "scanning pods again after after 30 seconds grace..."
done
if [ -n "$retries" ] ; then
  echo "Some pods are not ready:"
  kubectl get pods -A | grep -v 'Running' | grep -v 'Completed'
  errors=$((errors + 1))
fi

echo "⏰ Awaiting cilium ok for max 5m"
if ! cilium status --wait --wait-duration=5m ; then
  echo "error: cilium not ready in time"
  cilium status
fi

if [ "0" = "$errors" ] ; then
  echo "🎉 Done"
fi
exit $errors
