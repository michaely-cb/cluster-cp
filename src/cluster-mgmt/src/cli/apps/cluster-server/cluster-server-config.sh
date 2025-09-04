#!/bin/bash

set -e
set -x

function init_config() {
  set -x
  is_management_node=false
  is_coordinator_node=false
  is_worker_node=false
  node_info=$(kubectl get node $MY_NODE_NAME --show-labels)
  if echo $node_info | grep -q node-role-management; then
    is_management_node=true
  fi
  if echo $node_info | grep -q node-role-coordinator; then
    is_coordinator_node=true
  fi
  if echo $node_info | grep -q node-role-worker; then
    is_worker_node=true
  fi

  echo "is_management_node=$is_management_node, is_coordinator_node=$is_coordinator_node, is_worker_node=$is_worker_node"

  # For local logging post 1.8.0, all job-related logs will be stored under `/n1`, which is part of the root partition.
  # The effect of the above decision would make the hostpath directory as `/n1` across all wsjob pods.
  # However, the management node requires a special consideration, since we have compile root and log export happening
  # there and that has a much larger storage requirement. Therefore, we will keep the logs in `/n0` but create a symlink
  # from `/n1` that points to `/n0`. From the pod spec perspective, the hostpath directory for the coordinator would still
  # be `/n1`.
  host_n1="/host-root/n1"
  host_n0="/host-root/n0"

  # Management/coordinator separation is only an optional setup.
  # We need to create the hostpath on both the management and coordinator nodes.
  if $is_management_node || $is_coordinator_node; then
    if [ -n "$REMOTE_LOG_PATH" ]; then
      mkdir -p /host-root/$REMOTE_LOG_PATH
      chown 65532:65532 /host-root/$REMOTE_LOG_PATH
    fi

    #   Device lookup inside cluster-server-config daemonset pod, 
    #   when loopback device for log storge is present:
    #
    #   lsblk
    #   NAME MAJ:MIN RM   SIZE RO TYPE  MOUNTPOINTS
    #   loop0
    #          7:0    0    20G  0 loop  /host-root/n1/wsjob
    #   sda    8:0    0 223.6G  0 disk
    #   ├─sda1
    #   │      8:1    0     1G  0 part  /host-root/boot
    #   └─sda2
    #          8:2    0 222.6G  0 part
    #    ...
    
    # Check if the loop device is mounted at /n1/wsjob
    if ! lsblk | grep loop | grep -q "/n1/wsjob"; then
      echo "Loopback device not present at /n1/wsjob. Doing additional setup."
      mkdir -p $host_n1 $host_n0/wsjob $host_n0/log-export
      if [ ! -h "$host_n1/wsjob" ] && [ -d "$host_n1/wsjob" ]; then
        cp -a $host_n1/wsjob/. $host_n0/wsjob
        rm -rf $host_n1/wsjob
      fi
      # remove circular wsjob symlinks.
      if [ -h "$host_n0/wsjob/wsjob" ]; then
        rm -f $host_n0/wsjob/wsjob
      fi
      if [ -h "$host_n1/wsjob" ] && [ ! -e "$host_n1/wsjob" ]; then
        rm "$host_n1/wsjob"
      fi
      if [ ! -h "$host_n1/wsjob" ]; then
        ln -sf ../n0/wsjob $host_n1/wsjob
      fi
      echo "wsjob directory is setup: $(ls -l $host_n1)"
    else
      # remove circular wsjob symlinks.
      #
      #   ls -lh
      #   total 0
      #   drwxrwxrwx. 2 root root  6 Dec 14 18:18 compile_root
      #   drwxrwxrwx. 2 root root  6 Dec 14 18:18 workdir
      #   lrwxrwxrwx. 1 root root 11 Dec 14 18:18 wsjob -> ../n0/wsjob
      #   [root@sc-r10ra19-s14 wsjob]# pwd
      #   /n1/wsjob
      echo "Checking if circular wsjob symlink exists"
      if [[ -h $host_n1/wsjob/wsjob ]]; then
        echo "Going to cleanup circular  $host_n1/wsjob/wsjob path"
        rm -f $host_n1/wsjob/wsjob
        echo "DONE cleanup circular  $host_n1/wsjob/wsjob path"
      fi
      echo "Circular wsjob symlink should not exist now"
    fi

    if [ ! -h "$host_n1/log-export" ] && [ -d "$host_n1/log-export" ]; then
      cp -a $host_n1/log-export/. $host_n0/log-export
      rm -rf $host_n1/log-export
    fi
    # remove circular log-export symlinks.
    if [ -h "$host_n0/log-export/log-export" ]; then
      rm -f $host_n0/log-export/log-export
    fi
    if [ -h "$host_n1/log-export" ] && [ ! -e "$host_n1/log-export" ]; then
      rm "$host_n1/log-export"
    fi
    if [ ! -h "$host_n1/log-export" ]; then
      ln -sf ../n0/log-export $host_n1/log-export
    fi
    chmod 777 $host_n0/log-export $host_n1/log-export
    echo "log-export directory is setup: $(ls -l $host_n1)"

    debug_artifact_storage="$host_n1/debug-artifact"
    mkdir -p $host_n0/debug-artifact
    if [ -h "$debug_artifact_storage" ] && [ ! -e "$debug_artifact_storage" ]; then
      rm "$debug_artifact_storage"
    fi
    if [ ! -h "$debug_artifact_storage" ]; then
      ln -sf ../n0/debug-artifact ${debug_artifact_storage}
    fi
    chmod 777 $host_n0/debug-artifact ${debug_artifact_storage}
    echo "debug-artifact directory is setup: $(ls -l $host_n1)"

    # set up tensor storage
    tensor_storage="$host_n1/tensor-storage"
    mkdir -p $host_n0/tensor-storage
    if [ -h "$tensor_storage" ] && [ ! -e "$tensor_storage" ]; then
      rm "$tensor_storage"
    fi
    if [ ! -h "$tensor_storage" ]; then
      ln -sf ../n0/tensor-storage ${tensor_storage}
    fi
    chmod 777 $host_n0/tensor-storage ${tensor_storage}
    echo "tensor-storage directory is setup: $(ls -l $host_n1)"

    # The expected compile dir permission here is world readable/writable for all users.
    mkdir -p $host_n1/wsjob/compile_root && chmod a+w $host_n1/wsjob/compile_root
  fi

  mkdir -p $host_n1/wsjob/workdir && chmod a+w $host_n1/wsjob/workdir

  # Add workload manager directory
  mkdir -p $host_n0/cluster-mgmt/workload-manager && chmod a+w $host_n0/cluster-mgmt/workload-manager

  if $is_worker_node; then
    # Set up worker cache directory
    worker_cache_dir="$host_n0/cache"
    mkdir -p ${worker_cache_dir} && chmod a+w ${worker_cache_dir}
    echo "worker-cache directory is setup: $(ls -l $host_n0)"
  fi
}

function patch_node_condition() {
  local status=$1
  local index=$(kubectl get node "$MY_NODE_NAME" -ojson | jq '.status.conditions | map(.type) |
    index("ClusterDeployNotAppliedClusterServer") // empty')
  if [ -z "$index" ]; then
    index="-1"
  fi
  if [ "$status" = "True" ]; then
    not="not "
  else
    not=""
  fi
  local now="$(date -Is)"
  local json_payload=$(printf '[{"op": "replace", "path": "/status/conditions/%s",
                          "value": {"type": "ClusterDeployNotAppliedClusterServer",
                              "status": "%s",
                              "message": "ClusterMgmt cluster-server config has %sbeen applied",
                              "reason": "ClusterDeployConfigApply",
                              "lastTransitionTime": "%s",
                              "lastHeartbeatTime": "%s"}
                          }]' "${index}" "${status}" "${not}" "${now}" "${now}")
  kubectl patch node "$MY_NODE_NAME" --type='json' --subresource='status' -p="$json_payload"
}

export -f init_config
if timeout 30s bash -c init_config; then
  # Generate a new condition to indicate that the cluster-server config has been applied.
  echo "Patch node $MY_NODE_NAME with ClusterServerConfigNotApplied=False condition"
  patch_node_condition False
  echo "cluster-server-config applied"
  exit 0
else
  # generate a new node condition to indicate that the cluster-server config has not been applied.
  echo "Patch node $MY_NODE_NAME with ClusterServerConfigNotApplied=True condition"
  patch_node_condition True
  echo "cluster-server-config failed to apply"

  # Try again without timeout
  if init_config; then
    echo "Patch node $MY_NODE_NAME with ClusterServerConfigNotApplied=False condition"
    patch_node_condition False
    echo "cluster-server-config applied"
    exit 0
  else
    echo "cluster-server-config failed to apply"
    sleep infinity
  fi
fi