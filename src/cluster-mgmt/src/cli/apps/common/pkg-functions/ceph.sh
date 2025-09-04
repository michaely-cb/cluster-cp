# Common functions for ceph

KUBECTL=${KUBECTL:-kubectl}
export CEPH_PVC_MNT="/mnt/ceph"
export CEPH_FS_MNT="/mnt/cephfs"

# Create PVC for a given name/size in a given namespace.
function create_ceph_pvc() {
  local name=$1
  local namespace=$2
  local size=$3

  if ! $KUBECTL get pvc -n $namespace $name &>/dev/null; then
    cat <<EOM | $KUBECTL apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $name
  namespace: $namespace
spec:
  storageClassName: ceph-filesystem
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: $size
EOM
  fi
}

# Adjust size of a PVC in a given namespace
function adjust_ceph_pvc_size() {
  local name=$1
  local namespace=$2
  local size=$3  # Example: 10Gi
  local out=$(mktemp)
  cleanup() { rm -f "$out"; }
  trap cleanup EXIT

  # Get current storage request (empty if pvc doesn't exist)
  current_size=$($KUBECTL get pvc "$name" -n "$namespace" -o jsonpath='{.spec.resources.requests.storage}' 2>/dev/null || true)
  if [[ -z "$current_size" ]]; then
    echo "pvc $namespace/$name does not exist"
    return 1
  fi

  # Compare and resize if needed
  desired_size_bytes=$(get_size_bytes "$size")
  current_size_bytes=$(get_size_bytes "$current_size")
  if (( desired_size_bytes > current_size_bytes )); then
    echo "resizing PVC $namespace/$name from $current_size to $size"
    if ! $KUBECTL patch pvc "$name" -n "$namespace" --type merge -p "{\"spec\": {\"resources\": {\"requests\": {\"storage\": \"$size\"}}}}" &>"$out"; then
      echo "failed to resize pvc $namespace/$name:"
      cat "$out"
      return 1
    fi
  else
    echo "no resize needed; desired size ($size) is not greater than current ($current_size)"
  fi
}

# mount ceph fs, supports pvc level mount and defaults to entire fs
# https://docs.ceph.com/en/latest/cephfs/mount-using-kernel-driver.
function mount_ceph_fs() {
  local pvc_name=$1
  local namespace=$2
  local volume_path=${3:-"/"}
  local mount_path=${4:-"$CEPH_FS_MNT"}

  if mountpoint -q "$mount_path"; then
    echo "$mount_path is already mounted"
    return 0
  fi

  ceph_admin_auth=$(ceph_tools_exec ceph auth print-key client.admin)
  if [ -z "$ceph_admin_auth" ]; then
    echo "Can't find admin auth"
    return 1
  fi

  network_provider=$($KUBECTL -n rook-ceph get cephcluster -ojson | jq -r '.items[0].spec.network.provider // empty')
  if [ "$network_provider" == "host" ]; then
    mon_service_ips=$($KUBECTL -n rook-ceph get po -l app=rook-ceph-mon -o=jsonpath='{.items[*].status.podIP}')
  else
    mon_service_ips=$($KUBECTL -n rook-ceph get service -l app=rook-ceph-mon -o=jsonpath='{.items[*].spec.clusterIP}')
  fi
  mon_service_port=6789
  ceph_device=""
  for mon_service_ip in $mon_service_ips; do
    if [ -z "$ceph_device" ]; then
      ceph_device="$mon_service_ip:$mon_service_port"
    else
      ceph_device="$ceph_device,$mon_service_ip:$mon_service_port"
    fi
  done
  ceph_device="$ceph_device:$volume_path"

  # Set the _netdev option to ensure that the file system is mounted after the networking subsystem starts to prevent hanging and networking issues.
  PSSH_MGMT "mkdir -p $mount_path"
  PSSH_MGMT "mount -t ceph $ceph_device $mount_path -o name=admin,secret=$ceph_admin_auth,mds_namespace=ceph-filesystem,_netdev"
  PSSH_MGMT "sed -i \"\| $mount_path |d\" /etc/fstab"
  PSSH_MGMT "echo \"$ceph_device $mount_path ceph name=admin,secret=$ceph_admin_auth,mds_namespace=ceph-filesystem,_netdev 0 0\" >> /etc/fstab"
}

# mount ceph pvc
function mount_ceph_pvc() {
  local name=$1
  local namespace=$2
  local pv_name volume_path mon_service_ips mon_service_port tools_pod ceph_admin_auth ceph_device mount_path

  if mountpoint -q $CEPH_PVC_MNT/$namespace/$name; then
    echo "PVC $name in namespace $namespace is already mounted"
    return 0
  fi

  # find pv for this pvc
  pv_name=$($KUBECTL -n $namespace get pvc $name -o=jsonpath='{.spec.volumeName}')
  if [ -z "$pv_name" ]; then
    echo "Can't find PV for PVC $pvc in namespace $namespace"
    return 1
  fi
  volume_path=$($KUBECTL get pv $pv_name -o=jsonpath={.spec.csi.volumeAttributes.subvolumePath})
  if [ -z "$volume_path" ]; then
    volume_path=$($KUBECTL get pv $pv_name -o=jsonpath={.spec.csi.volumeAttributes.rootPath})
  fi

  if [ -z "$volume_path" ]; then
    echo "Can't find spec.csi.volumeAttributes.subvolumePath or .rootPath for PV $pv_name"
    return 1
  fi

  mount_ceph_fs $name $namespace $volume_path "$CEPH_PVC_MNT/$namespace/$name"
}

# unmount ceph pvc
function umount_ceph_path() {
  local mount_path=$1
  if mountpoint -q $mount_path; then
    PSSH_MGMT "sed -i \"\| $mount_path |d\" /etc/fstab"
    PSSH_MGMT "umount $mount_path && rm -rf $mount_path || true"
  fi
}

# unmount all ceph pvcs
function unmount_all_ceph_pvcs() {
  umount_ceph_path "$CEPH_FS_MNT"
  if ! [ -d "$CEPH_PVC_MNT" ]; then
    return 0
  fi
  mount_paths=$(find $CEPH_PVC_MNT -maxdepth 2 -mindepth 2 -type d)
  for mount_path in $mount_paths; do
    umount_ceph_path $mount_path
  done
}

# find pvc deps
function find_pods_use_pvc() {
  local pvc_name=$1
  local pvc_namespace=$2

  $KUBECTL get pods -n $pvc_namespace -ojson |
    jq --arg pvc "$pvc_name" -r '.items[] | select(.spec.volumes[].persistentVolumeClaim.claimName == $pvc) | .metadata.name'
}

# delete ceph pvc
function delete_ceph_pvc() {
  local name=$1
  local namespace=$2
  umount_ceph_path "$CEPH_PVC_MNT/$namespace/$name"

  # Find all pods that are still using this pvc. If these pods are in Completed state (either
  # Failed or Succeeded), delete these pods before deleting the pvc. Otherwise, deleting pvc
  # will hang. If some of these pods are still running, return 1.
  # note: this is not safe to do, you may need to delete deployments/ds/pods that will recreate them
  local pods_use_pvc=$(find_pods_use_pvc $name $namespace)
  local has_running_pods="False"
  if [ -n "$pods_use_pvc" ]; then
    for pod in $pods_use_pvc; do
      phase=$($KUBECTL get pod $pod -ojson | jq -r '.status.phase')
      if [ "$phase" = "Failed" ] || [ "$phase" = "Succeeded" ]; then
        echo "Deleting pod $pod in namespace $namespace"
        $KUBECTL delete pod $pod -n $namespace --ignore-not-found
      else
        echo "Pod $pod is not in Completed state: $phase"
        has_running_pods="True"
      fi
    done
  fi
  if [ "$has_running_pods" = "True" ]; then
    return 1
  else
    $KUBECTL -n $namespace delete pvc $name --ignore-not-found
    return 0
  fi
}

# Exec a function in the running ceph-tools pod
function ceph_tools_exec() {
  local tools_pod
  tools_pod=$($KUBECTL -n rook-ceph get pods -l app=rook-ceph-tools -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}' | head -n1)
  if [ -z "$tools_pod" ]; then
    echo "Error: cannot find ceph tools pod"
    return 1
  fi
  $KUBECTL exec -n rook-ceph "$tools_pod" -- "$@"
}

# convert a size in Gi, Mi, Ki into the actual bytes.
function get_size_bytes() {
  local size_str=$1
  if [ -z ${size_str##*Gi} ]; then
    size=${size_str%Gi}
    echo $((size * 1024 * 1024 * 1024))
  elif [ -z ${size_str##*Mi} ]; then
    size=${size_str%Mi}
    echo $((size * 1024 * 1024))
  elif [ -z ${size_str##*Ki} ]; then
    size=${size_str%Ki}
    echo $((size * 1024))
  else
    echo $size_str
  fi
}

# Create a subvolumegroup/subvolume for a given name, to be used for static pvc.
# We have 2 use cases currently, one for cached compile, and the other for log-export.
function create_ceph_subvolume() {
  local name=$1
  local size_str=$2
  local out=$(mktemp)
  cleanup() { rm -f "$out"; }
  trap cleanup EXIT

  local subvolumegroup_name="$name-group"
  local subvolume_name="$name-subvolume"

  if ceph_tools_exec ceph fs subvolumegroup info ceph-filesystem "$subvolumegroup_name" &>/dev/null; then
    echo "subvolumegroup '$subvolumegroup_name' already exists"
  else
    if ! ceph_tools_exec ceph fs subvolumegroup create ceph-filesystem "$subvolumegroup_name" &>"$out"; then
      echo "failed to create subvolumegroup '$subvolumegroup_name':"
      cat "$out"
      return 1
    fi
  fi

  if ceph_tools_exec ceph fs subvolume info ceph-filesystem "$subvolume_name" "$subvolumegroup_name" &>/dev/null; then
    echo "subvolume '$subvolume_name' already exists"
  else
    size_bytes=$(get_size_bytes "$size_str")
    if ! ceph_tools_exec ceph fs subvolume create ceph-filesystem "$subvolume_name" "$subvolumegroup_name" --size="$size_bytes" &>"$out"; then
      echo "failed to create subvolume '$subvolume_name':"
      cat "$out"
      return 1
    fi
  fi

  return 0
}

# Adjust size of a subvolume.
function adjust_ceph_subvolume() {
  local name=$1
  local size_str=$2
  local out=$(mktemp)
  cleanup() { rm -f "$out"; }
  trap cleanup EXIT

  local subvolumegroup_name="$name-group"
  local subvolume_name="$name-subvolume"

  # Get current subvolume quota (empty if subvolume doesn't exist)
  local current_size_bytes=$(ceph_tools_exec ceph fs subvolume info ceph-filesystem "$subvolume_name" "$subvolumegroup_name" \
    -f json | jq -r '.bytes_quota' 2>/dev/null || true)
  if [[ -z "$current_size_bytes" ]]; then
    echo "subvolume '$subvolume_name' does not exist in group '$subvolumegroup_name'"
    return 1
  fi

  local desired_size_bytes=$(get_size_bytes "$size_str")

  # Compare and resize if needed
  if (( desired_size_bytes > current_size_bytes )); then
    echo "resizing subvolume '$subvolume_name' from $current_size_bytes to $desired_size_bytes bytes"
    if ! ceph_tools_exec ceph fs subvolume resize ceph-filesystem "$subvolume_name" "$desired_size_bytes" \
      --group_name "$subvolumegroup_name" --no_shrink &>"$out"; then
      echo "failed to resize subvolume '$subvolume_name':"
      cat "$out"
      return 1
    fi
  else
    echo "no resize needed; desired size ($desired_size_bytes) is not greater than current ($current_size_bytes)"
  fi
}

# Create a secret needed for static pvc.
function create_csi_cephfs_secret() {
  if $KUBECTL -n rook-ceph get secret csi-cephfs-secret &>/dev/null; then
    echo "secret csi-cephfs-secret already exists."
    return
  fi

  csi_cephfs_node_key=$(ceph_tools_exec ceph auth print-key client.csi-cephfs-node)
  csi_cephfs_provisioner_key=$(ceph_tools_exec ceph auth print-key client.csi-cephfs-provisioner)
  cat <<EOF | $KUBECTL apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: csi-cephfs-secret
  namespace: rook-ceph
stringData:
  # Required for statically provisioned volumes
  userID: csi-cephfs-node
  userKey: $csi_cephfs_node_key

  # Required for dynamically provisioned volumes
  adminID: csi-cephfs-provisioner
  adminKey: $csi_cephfs_provisioner_key
EOF
}

# create static pvc that uses the same underline subvolume that can be shared
# among namespaces. The created pv/pvc name will be constructed using the given
# name as prefix. For example,
#    create_ceph_static_pvc log-export job-operator
# will create pv `log-export-job-operator-static-pv` and `log-export-static-pvc` in `job-operator
# namespace.`
#
# Note: Resizing static pvc needs to be done through resizing the subvolume, not by changing the size in
# pvc. An example of resizing static pvc is as follows:
#    ceph fs subvolume resize ceph-filesystem log-export-subvolume <new_size> log-export-group --no-shrink
function create_ceph_static_pvc() {
  local name=$1
  local namespace=$2

  if $KUBECTL -n "$namespace" get pvc "$name-static-pvc" &>/dev/null; then
    echo "pvc $name-static-pvc in namespace $namespace already exists"
    return
  fi

  local volume_root_path=$(ceph_tools_exec ceph fs subvolume info ceph-filesystem $name-subvolume $name-group | jq -r '.path')
  local volume_size_bytes=$(ceph_tools_exec ceph fs subvolume info ceph-filesystem $name-subvolume $name-group | jq -r '.bytes_quota')
  if ! $KUBECTL get pvc -n "$namespace" "$name-static-pvc" &>/dev/null; then
    cat <<EOM | $KUBECTL apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: $name-$namespace-pv
  labels:
    cerebras/namespace: $namespace
spec:
  accessModes:
  - ReadWriteMany
  capacity:
    storage: $volume_size_bytes
  csi:
    # controllerExpandSecretRef:
    #   name: rook-csi-cephfs-provisioner
    #   namespace: rook-ceph
    driver: rook-ceph.cephfs.csi.ceph.com
    nodeStageSecretRef:
      # node stage secret name
      name:  csi-cephfs-secret
      # node stage secret namespace where above secret is created
      namespace: rook-ceph
    volumeAttributes:
      # Required options from storageclass parameters need to be added in volumeAttributes
      "clusterID": "rook-ceph"
      "fsName": "ceph-filesystem"
      "staticVolume": "true"
      # rootPath can be found in 'ceph fs subvolume info ceph-filesystem log-export-subvolume log-export-group'
      "rootPath": $volume_root_path
    # volumeHandle can be anything, need not to be same
    # as PV name or volume name. keeping same for brevity
    # volumeHandle needs to be different per namespace.
    volumeHandle: $name-$namespace-pv
  persistentVolumeReclaimPolicy: Retain
  volumeMode: Filesystem
  storageClassName: ceph-filesystem
EOM
    cat <<EOM | $KUBECTL apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $name-static-pvc
  namespace: $namespace
spec:
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: $volume_size_bytes
  volumeMode: Filesystem
  # volumeName should be same as PV name
  volumeName: $name-$namespace-pv
  storageClassName: ceph-filesystem
EOM
  fi
}

# Chmod existing cached compile directories to be world writable.
# Starting from 2.1, we don't use FsGroup to change the file/directory
# permissions on the cached compile static pvc at the time of mount.
# The directories need to be world-readable to accommodate different
# users. This function changes existing cached compile directories
# and their parent directories to be world writable.
#
# TODO: In 2.3, after all existing clusters have this permission setup
# properly, we can simplify this by doing this once during ceph deployment.
function chmod_cached_compile_static_pvc() {
  local namespace=$1
  local image=$2

  local candidate_node=$(hostname)
  local labels="-lk8s.cerebras.com/namespace=$namespace,k8s.cerebras.com/node-role-coordinator"
  local candidate_nodes=$($KUBECTL get nodes $labels -ojson | jq -r ".items[] | .metadata.name")
  if [ -n "$candidate_nodes" ]; then
    candidate_node=$(echo $candidate_nodes | cut -d' ' -f1)
  fi

  # remove any leftover chmod-cached-compile job
  $KUBECTL delete job -n $namespace chmod-cached-compile --ignore-not-found

  # copy over chmod_writable_to_subdirs.sh file to the candidate node.
  ssh $candidate_node "mkdir -p /tmp/$SCRIPT_FULL_PATH"
  scp chmod_writable_to_subdirs.sh $candidate_node:/tmp/$SCRIPT_FULL_PATH/chmod_writable_to_subdirs.sh

  cat <<EOM | $KUBECTL apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: chmod-cached-compile
  namespace: $namespace
spec:
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      nodeName: $candidate_node
      containers:
      - name: main
        image: $image
        command: ["bash", "-c", "/cb/cluster_server/chmod_writable_to_subdirs.sh /cb/compile_root cs_*"]
        volumeMounts:
        - mountPath: /cb/compile_root
          name: cached-compile-path
        - mountPath: /cb/cluster_server
          name: script-path
        workingDir: /cb/compile_root
      volumes:
      - name: cached-compile-path
        persistentVolumeClaim:
          claimName: cached-compile-static-pvc
      - name: script-path
        hostPath:
          path: /tmp/$SCRIPT_FULL_PATH
          type: Directory
      tolerations:
      - key: node-role.kubernetes.io/master
        operator: Exists
        effect: NoSchedule
      - key: node-role.kubernetes.io/control-plane
        operator: Exists
        effect: NoSchedule
EOM

  if ! $KUBECTL -n "$namespace" wait --for=condition=complete --timeout=300s job/chmod-cached-compile; then
    echo "Failed to chmod cached compile"
    echo "$($KUBECTL -n $namespace logs job/chmod-cached-compile)"
    return 1
  else
    echo "Succeeded in chmodding cached compile"
    return 0
  fi
}
