#!/usr/bin/env bash

# script to clean up ceph cluster.

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"
source "../pkg-common.sh"

function remove_resources_with_pvc() {
  local pvc_name="$1"
  local namespace="$2"
  local resource

  # Find all deployments/statefulsets/daemonsets/jobs/cronjobs in the cluster that
  # might use the given pvc and delete them
  echo "Removing resources using PVC: $pvc_name in namespace: $namespace"
  for resource in deployment statefulset daemonset job cronjob; do
    local resource_objects=$(kubectl get $resource -n $namespace -o json |
      jq -r ".items[]
        | select(
            (.spec.template.spec.volumes[]?.persistentVolumeClaim.claimName? == \"$pvc_name\") or
            (.spec.jobTemplate.spec.template.spec.volumes[]?.persistentVolumeClaim.claimName? == \"$pvc_name\")
          )
        | .metadata.name")
    for resource_object in $resource_objects; do
      echo "kubectl -n ${namespace} delete ${resource} ${resource_object}"
      kubectl -n "${namespace}" delete "${resource}" "${resource_object}" --ignore-not-found
    done
  done
}

# https://rook.github.io/docs/rook/latest-release/Getting-Started/ceph-teardown/
# https://github.com/rook/rook/blob/master/Documentation/Storage-Configuration/ceph-teardown.md
# note: if you want to reinstall ceph after cleanup and private registry already installed
# you need to reinstall registry to restore capacity of image pulling
echo "WARNING: this cleanup will erase all existing data in ceph including prometheus/loki/registry/..."
if [ "$CONFIRM" != "yes" ]; then
  echo "         Are you sure you want to continue? (y/n)"
  read -r -n 1 -s
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "aborting"
    exit 1
  fi
fi

helm uninstall prometheus -n prometheus || true
helm uninstall loki -n loki || true
kubectl delete ds private-registry -nkube-system || true

tool_pod=$(kubectl -n rook-ceph get pod -lapp=rook-ceph-tools -o=jsonpath='{.items}')
if [[ $tool_pod != "[]" ]]; then
  echo "echo 'wiping disks'" >./cleanup-disks.sh
  tool_pod=$(kubectl -n rook-ceph get pod -lapp=rook-ceph-tools -o=jsonpath='{.items[0].metadata.name}')
  devices=$(timeout 10 kubectl -n rook-ceph exec -t $tool_pod -- ceph device ls -f json)
  device_locations=$(echo $devices | jq -r '.[] | .location[] | [.host, .dev] | @csv')
  for device in $device_locations; do
    device_host=$(echo $device | cut -d ',' -f 1 | cut -d '"' -f 2)
    device_name=$(echo $device | cut -d ',' -f 2 | cut -d '"' -f 2)
    echo "ssh $device_host \"sfdisk --delete /dev/$device_name; dd if=/dev/zero of=/dev/$device_name bs=1M count=100 oflag=direct,dsync; blkdiscard /dev/$device_name; partprobe /dev/$device_name\" && \\" >>cleanup-disks.sh
  done
  # fall back to default devices
  if ! grep "sfdisk" cleanup-disks.sh; then
    nodes=$(kubectl get nodes -lstorage-type=ceph -ojsonpath='{.items[*].metadata.name}')
    # Iterate through each node
    for node in $nodes; do
      for i in {0..2}; do
        device_host="$node"
        device_name="nvme${i}n1"
        echo "ssh $device_host \"sfdisk --delete /dev/$device_name; dd if=/dev/zero of=/dev/$device_name bs=1M count=100 oflag=direct,dsync; blkdiscard /dev/$device_name; partprobe /dev/$device_name\" && \\" >>cleanup-disks.sh
      done
    done
  fi
  echo "echo 'done'" >>cleanup-disks.sh
fi
echo "Listed all commands to clean osd disks in file 'cleanup-disks.sh'"

# umount existing mount points
unmount_all_ceph_pvcs

# delete all relevant pvcs
# note: if there are pods using these pvcs, they need to be cleaned up explicitly
pvcs=$(kubectl get pvc -Ao jsonpath='{range .items[*]}{@.metadata.name}{"|"}{@.metadata.namespace}{"|"}{@.spec.storageClassName}{"\n"}{end}')
for pvc in $pvcs; do
  name=$(echo $pvc | cut -d "|" -f 1)
  namespace=$(echo $pvc | cut -d "|" -f 2)
  storage_class=$(echo $pvc | cut -d "|" -f 3)
  pv_name=$(kubectl -n "$namespace" get pvc "$name" -ojson | jq -r '.spec.volumeName')

  if [ "$storage_class" == "ceph-filesystem" ]; then
    remove_resources_with_pvc "$name" "$namespace"

    static_volume=$(kubectl get pv "$pv_name" -ojson | jq -r '.spec.csi.volumeAttributes.staticVolume')
    echo "kubectl -n ${namespace} delete pvc ${name}"
    kubectl -n ${namespace} delete pvc ${name}
    if [ "$static_volume" == "true" ]; then
      echo "kubectl delete pv $pv_name"
      kubectl delete pv "$pv_name"
    fi
  fi
done

helm uninstall --namespace rook-ceph rook-ceph-cluster
helm uninstall --namespace rook-ceph rook-ceph

# kubectl -n rook-ceph patch cephcluster rook-ceph --type merge \
#    -p '{"spec":{"cleanupPolicy":{"confirmation":"yes-really-destroy-data"}}}'

# Allow ceph CRDs to be deleted, so that ceph cluster can be deleted.
for CRD in $(kubectl get crd -n rook-ceph | awk '/ceph.rook.io/ {print $1}'); do
  kubectl get -n rook-ceph "$CRD" -o name |
    xargs -I {} kubectl patch -n rook-ceph {} --type merge -p '{"metadata":{"finalizers": []}}'
done

# Allow ceph configmap and secrets to be deleted, so that the ceph namespace can be deleted.
if kubectl -n rook-ceph get configmap rook-ceph-mon-endpoints; then
  kubectl -n rook-ceph patch configmap rook-ceph-mon-endpoints --type merge -p '{"metadata":{"finalizers": []}}'
fi

if kubectl -n rook-ceph get secrets rook-ceph-mon; then
  kubectl -n rook-ceph patch secrets rook-ceph-mon --type merge -p '{"metadata":{"finalizers": []}}'
fi

kubectl -n rook-ceph delete cephcluster rook-ceph --ignore-not-found

PSSH "rm -rf /var/lib/rook"

if [ -f "cleanup-disks.sh" ]; then
  cat cleanup-disks.sh
  bash cleanup-disks.sh
  PSSH "rm -rf /dev/ceph*; rm -rf /dev/mapper/ceph*"
fi

kubectl delete ns rook-ceph
