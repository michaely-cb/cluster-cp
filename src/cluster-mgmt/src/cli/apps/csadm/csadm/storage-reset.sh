#!/bin/bash

# storage-reset.sh
set -eo pipefail

dryrun=false
existing_manifest=""
skip_create_manifest=false
cleanup_job_logs=false
cleanup_cached_compile=false
cleanup_user_venvs=false
cleanup_cluster_volumes=false
cleanup_custom_worker_images=false
cleanup_worker_cache=false
cleanup_monitoring_data=false
cleanup_job_history=false

default_namespace="job-operator"
manifest=$(jq -n '{
  logs: {
    local: [],
    ceph: []
  },
  cached_compile: {
    local: [],
    ceph: []
  },
  venv_volumes: [],
  cluster_volumes: {
    should_clean: false,
    namespaces: []
  },
  custom_worker_images: [],
  worker_cache: {
    should_clean: false
  },
  monitoring_data: {},
  job_history: {
    should_clean: false,
    namespaces: []
  }
}')
output_dir="/tmp/storage-reset"
mkdir -p ${output_dir}
manifest_outfile="$output_dir/manifest.json"
releases_outfile="$output_dir/cluster_server_helm_releases.json"
user_venv_volumes_outfile="$output_dir/user_venv_volumes.json"
custom_worker_images_outfile="$output_dir/ustom_worker_images.json"
monitoring_data_outfile="$output_dir/monitoring_data.json"
job_history_outfile="$output_dir/job_history.out"
registry_url="https://127.0.0.1:5000"

function print_help {
  echo "Usage: $0 [OPTIONS]"
  echo "Resets application storage, clearing any potentially sensitive data."
  echo ""
  echo "Options:"
  echo "  --help                 Show this help menu"
  echo "  --job-logs             Delete workdirs logs and log export artifacts"
  echo "  --cached-compile       Delete cached compile artifacts"
  echo "  --user-venvs           Delete user virtual environments"
  echo "  --cluster-volumes      Delete cluster volume configurations"
  echo "  --custom-worker-images Delete custom worker images"
  echo "  --worker-cache         Delete worker cache"
  echo "  --all                  Delete all of the above"
  echo "  --monitoring-data      Delete monitoring data (operate with caution for cleaning up irreproducible data)"
  echo "  --job-history          Delete job history (operate with caution for cleaning up irreproducible data)"
  echo "  --dry-run              Dryrun the storage reset routine"
  echo "  --use-manifest FILE    Use the specified manifest file for cleanup"
  exit 0
}

function _on_nfs() {
  local path="$1"
  if [ ! -d "${path}" ]; then return 1; fi

  local fstype=$(df "${path}" --output=fstype | tail -1)
  if [[ $fstype == "nfs"* ]]; then
    return 0
  fi
  return 1
}

function _has_multi_mgmt_nodes() {
  local mgmt_node_count=$(kubectl get no -l k8s.cerebras.com/node-role-management= --no-headers | wc -l)
  if [ ${mgmt_node_count} -gt 1 ]; then
    return 0
  fi
  return 1
}

function add_logs_to_manifest {
  if [ "$skip_create_manifest" = true ]; then return; fi

  echo "Adding job logs to the cleanup manifest..."
  helm_release_name="cluster-server"
  releases="[]"
  # Scrape cluster server releases to get paths for workdir, compiles and log exports
  namespaces=$(helm list --all-namespaces --filter "^${helm_release_name}$" -o json | jq -r '.[] | .namespace')
  while IFS= read -r ns; do
    values=$(helm get values -n $ns $helm_release_name --output json)
    releases=$(jq --argjson v "$values" '. + [$v]' <<< ${releases})
  done <<< "${namespaces}"
  echo "${releases}" > ${releases_outfile}

  workdir_root_paths="[]"
  while IFS= read -r path; do
    if [ ! -d "$path" ] || _on_nfs $path; then continue; fi
    workdir_root_paths=$(jq '. + ["'${path}'"]' <<< "${workdir_root_paths}")
  done <<< $(jq -r '[.[].wsjob.localWorkdirRoot] | flatten | unique | .[]' ${releases_outfile})

  log_export_paths="[]"
  while IFS= read -r path; do
    if ! _has_multi_mgmt_nodes && [ ! -d "$path" ]; then continue; fi
    log_export_paths=$(jq '. + ["'${path}'"]' <<< "${log_export_paths}")
  done <<< $(jq -r '[.[].logExport.path] | unique | .[]' ${releases_outfile})
  
  # Need to clean on every node
  manifest=$(jq '.logs.local += [{"paths":'"${workdir_root_paths}"', "node_selector": ""}]' <<< "${manifest}")
  
  if _has_multi_mgmt_nodes; then
    # Check if mount paths in Ceph are exactly one
    if [ "$(jq 'length' <<< "${log_export_paths}")" -ne 1 ]; then
      echo "Error: Expected only one mount path for log-export-static-pvc but received ${log_export_paths}."
      exit 1
    fi
  
    # For multi-mgmt nodes envs, compile and log export will be done in Ceph
    manifest=$(jq --arg p $(jq -r '.[0]' <<< "${log_export_paths}") --arg pv "log-export-job-operator-pv" --arg pvc "log-export-static-pvc" \
      '.logs.ceph += [{"pv_name": $pv, "pvc_name": $pvc, "mount_path": $p, "node_selector": "storage-type=ceph"}]' <<< "${manifest}")
  else
    # For single-mgmt node envs, clean up on coordinator nodes
    manifest=$(jq '.logs.local += [{"paths":'"${log_export_paths}"', "node_selector": "k8s.cerebras.com/node-role-coordinator="}]' <<< "${manifest}")
  fi
  
  # Clean additional logs from cluster server, job operator, nginx, registry and workload_manager
  appliance_sw_log_dir="/n0/cluster-mgmt"
  appliance_sw_logs=()
  for path in "cluster-server" "job-operator" "nginx" "registry-logs" "workload_manager"; do
    appliance_sw_logs+=("${appliance_sw_log_dir}/${path}")
  done
  appliance_sw_logs_json=$(echo "${appliance_sw_logs[*]}" | jq -R 'split(" ")')
  manifest=$(jq --argjson paths "$appliance_sw_logs_json" '.logs.local += [{"paths": $paths, "node_selector": "k8s.cerebras.com/node-role-coordinator="}]' <<< "${manifest}")
}

function add_cached_compile_to_manifest {
  if [ "$skip_create_manifest" = true ]; then return; fi

  echo "Adding cached compile to the cleanup manifest..."
  helm_release_name="cluster-server"
  releases="[]"
  # Scrape cluster server releases to get paths for workdir, compiles and log exports
  namespaces=$(helm list --all-namespaces --filter "^${helm_release_name}$" -o json | jq -r '.[] | .namespace')
  while IFS= read -r ns; do
    values=$(helm get values -n $ns $helm_release_name --output json)
    releases=$(jq --argjson v "$values" '. + [$v]' <<< ${releases})
  done <<< "${namespaces}"
  echo "${releases}" > ${releases_outfile}

  compile_root_paths="[]"
  while IFS= read -r path; do
    if ! _has_multi_mgmt_nodes && { [ ! -d "$path" ] || _on_nfs "$path"; }; then continue; fi
    compile_root_paths=$(jq '. + ["'${path}'"]' <<< "${compile_root_paths}")
  done <<< $(jq -r '[.[].wsjob.localCachedCompileRoot] | unique | .[]' ${releases_outfile})

  if _has_multi_mgmt_nodes; then
    # Check if mount paths in Ceph are exactly one
    if [ "$(jq 'length' <<< "${compile_root_paths}")" -ne 1 ]; then
      echo "Error: Expected only one mount path for cache-compile-static-pvc but received ${compile_root_paths}."
      exit 1
    fi

    # For multi-mgmt nodes envs, compile and log export will be done in Ceph
    manifest=$(jq --arg p $(jq -r '.[0]' <<< "${compile_root_paths}") --arg pv "cached-compile-job-operator-pv" --arg pvc "cached-compile-static-pvc" \
      '.cached_compile.ceph += [{"pv_name": $pv, "pvc_name": $pvc, "mount_path": $p, "node_selector": "storage-type=ceph"}]' <<< "${manifest}")
  else
    # For single-mgmt node envs, clean up on coordinator nodes
    manifest=$(jq '.cached_compile.local += [{"paths":'"${compile_root_paths}"', "node_selector": "k8s.cerebras.com/node-role-coordinator="}]' <<< "${manifest}")
  fi
}

function add_user_venvs_to_manifest {
  if [ "$skip_create_manifest" = true ]; then return; fi

  echo "Adding user virtual environments to the cleanup manifest..."

  user_venv_volumes="[]"
  namespaces=$(helm list --all-namespaces --filter "^cluster-server$" -o json | jq -r '.[] | .namespace')
  while IFS= read -r ns; do
    vols=$(csctl --namespace ${ns} get volumes -o json 2>/dev/null || true)
    if [ -n "${vols}" ]; then
      venv_allowed_vols=$(jq -r '[.items[] | select(.meta.labels["allow-venv"] == "true")]' <<< "${vols}")
      user_venv_volumes=$(jq --argjson data "$venv_allowed_vols" '. + $data' <<< "$user_venv_volumes")
    fi
  done <<< "${namespaces}"

  echo "${user_venv_volumes}" | \
    jq 'map({mount_path: .nfs.containerPath, server: .nfs.server, server_path: .nfs.serverPath, node_selector: "k8s.cerebras.com/node-role-worker="})' | \
    jq 'unique_by(.mount_path, .server, .server_path)' > ${user_venv_volumes_outfile}

  venv_volumes=$(jq '.' "${user_venv_volumes_outfile}")
  manifest=$(jq --argjson venv_volumes "$venv_volumes" '.venv_volumes = $venv_volumes' <<< "$manifest")
}

function add_cluster_volumes_to_manifest {
  if [ "$skip_create_manifest" = true ]; then return; fi

  echo "Adding cluster volumes to the cleanup manifest..."
  manifest=$(jq '.cluster_volumes.should_clean = true' <<< "$manifest")
  namespaces=$(helm list --all-namespaces --filter "^cluster-server$" -o json | jq -cr '[.[] | .namespace]')
  manifest=$(jq --argjson namespaces "$namespaces" '.cluster_volumes.namespaces = $namespaces' <<< "$manifest")
}

function add_custom_worker_images_to_manifest {
  if [ "$skip_create_manifest" = true ]; then return; fi

  echo "Adding custom worker images to the cleanup manifest..."

  custom_worker_images="[]"
  image="custom-worker"
  tags=$(curl -ks "${registry_url}/v2/${image}/tags/list" | jq -r '.tags')
  if [ "$tags" = "null" ]; then return 0; fi

  for tag in $(jq -r '.[]' <<< "$tags"); do
    digest_blob=$(curl -ks -I -H "Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json" \
      "${registry_url}/v2/${image}/manifests/${tag}")
    if echo "${digest_blob}" | grep -q docker-content-digest; then
      digest=$(echo "${digest_blob}" | grep docker-content-digest | awk '{print ($2)}' | tr -d '\r')
    else
      digest=""
    fi

    custom_worker_images=$(jq -n --arg tag "$tag" --arg digest "$digest" --argjson images "$custom_worker_images" \
      '$images + [{"tag": $tag, "digest": $digest}]')
  done
  echo "${custom_worker_images}" > ${custom_worker_images_outfile}

  custom_worker_images=$(jq '.' "${custom_worker_images_outfile}")
  manifest=$(jq --argjson custom_worker_images "$custom_worker_images" '.custom_worker_images = $custom_worker_images' <<< "$manifest")
}

function add_worker_cache_to_manifest {
  if [ "$skip_create_manifest" = true ]; then return; fi

  echo "Adding worker cache to the cleanup manifest..."
  manifest=$(jq '.worker_cache.should_clean = true' <<< "$manifest")
}

function add_monitoring_data_to_manifest {
  if [ "$skip_create_manifest" = true ]; then return; fi

  echo "Adding monitoring data to the cleanup manifest..."

  monitoring_data_spec="{}"
  if _has_multi_mgmt_nodes; then
    node_selector="storage-type=ceph"
  else
    node_selector="k8s.cerebras.com/node-role-management="
  fi
  prom_ns="prometheus"
  # Ideally we shoudln't hardcode this, but didn't find a good label selector
  prom_sts="prometheus-prometheus-prometheus"
  prom_data_path="/n0/cluster-mgmt/prom"
  if ! kubectl -n ${prom_ns} get sts ${prom_sts} &>/dev/null; then
    echo "Error: Statefulset ${prom_sts} does not exist in the ${prom_ns} namespace"
    exit 1
  fi
  prom_pvcs=$(kubectl -n ${prom_ns} get pvc -l app.kubernetes.io/name=prometheus --no-headers | awk '{print $1}' | jq -R -s -c 'split("\n")[:-1]')
  prom_persistence="[]"
  for pvc_name in $(echo $prom_pvcs | jq -r '.[]'); do
    pv_name=$(kubectl -n ${prom_ns} get pvc ${pvc_name} -o json | jq -r '.spec.volumeName')
    prom_persistence=$(jq --arg pvc "$pvc_name" --arg pv "$pv_name" --arg mp "$prom_data_path" --arg selector "$node_selector" \
      '. += [{"pv_name": $pv, "pvc_name": $pvc, "mount_path": $mp, "node_selector": $selector}]' <<< "$prom_persistence")
  done
  prom_entry=$(jq -n --arg ns "$prom_ns" --arg sts "$prom_sts" --argjson persist_spec "$prom_persistence" \
    '{namespace: $ns, resource_type: "sts", resource_name: $sts, persistence: $persist_spec}')
  monitoring_data_spec=$(jq --argjson e "$prom_entry" '.prometheus += $e' <<< "$monitoring_data_spec")

  grafana_deploy="prometheus-grafana"
  grafana_data_path="/n0/cluster-mgmt/grafana"
  if ! kubectl -n ${prom_ns} get deploy ${grafana_deploy} &>/dev/null; then
    echo "Error: Deployment ${grafana_deploy} does not exist in the ${prom_ns} namespace"
    exit 1
  fi
  grafana_pvcs=$(kubectl -n ${prom_ns} get pvc -l app.kubernetes.io/name=grafana --no-headers | awk '{print $1}' | jq -R -s -c 'split("\n")[:-1]')
  grafana_persistence="[]"
  for pvc_name in $(echo $grafana_pvcs | jq -r '.[]'); do
    pv_name=$(kubectl -n ${prom_ns} get pvc ${pvc_name} -o json | jq -r '.spec.volumeName')
    grafana_persistence=$(jq --arg pvc "$pvc_name" --arg pv "$pv_name" --arg mp "$grafana_data_path" --arg selector "$node_selector" \
      '. += [{"pv_name": $pv, "pvc_name": $pvc, "mount_path": $mp, "node_selector": $selector}]' <<< "$grafana_persistence")
  done
  grafana_entry=$(jq -n --arg ns "$prom_ns" --arg dpl "$grafana_deploy" --argjson persist_spec "$grafana_persistence" \
    '{namespace: $ns, resource_type: "deploy", resource_name: $dpl, persistence: $persist_spec}')
  monitoring_data_spec=$(jq --argjson e "$grafana_entry" '.grafana += $e' <<< "$monitoring_data_spec")

  loki_ns="loki"
  loki_sts="loki"
  loki_data_path="/n0/cluster-mgmt/loki"
  if ! kubectl -n ${loki_ns} get sts ${loki_sts} &>/dev/null; then
    echo "Error: Statefulset ${loki_sts} does not exist in the ${loki_ns} namespace"
    exit 1
  fi
  loki_pvcs=$(kubectl -n ${loki_ns} get pvc -l app.kubernetes.io/name=loki --no-headers | awk '{print $1}' | jq -R -s -c 'split("\n")[:-1]')
  loki_persistence="[]"
  for pvc_name in $(echo $loki_pvcs | jq -r '.[]'); do
    pv_name=$(kubectl -n ${loki_ns} get pvc ${pvc_name} -o json | jq -r '.spec.volumeName')
    loki_persistence=$(jq --arg pvc "$pvc_name" --arg pv "$pv_name" --arg mp "$loki_data_path" --arg selector "$node_selector" \
      '. += [{"pv_name": $pv, "pvc_name": $pvc, "mount_path": $mp, "node_selector": $selector}]' <<< "$loki_persistence")
  done
  loki_entry=$(jq -n --arg ns "$loki_ns" --arg sts "$loki_sts" --argjson persist_spec "$loki_persistence" \
    '{namespace: $ns, resource_type: "sts", resource_name: $sts, persistence: $persist_spec}')
  monitoring_data_spec=$(jq --argjson e "$loki_entry" '.loki += $e' <<< "$monitoring_data_spec")
  echo "$monitoring_data_spec" > ${monitoring_data_outfile}

  monitoring_data_spec=$(jq '.' "${monitoring_data_outfile}")
  manifest=$(jq --argjson monitoring_data_spec "$monitoring_data_spec" '.monitoring_data = $monitoring_data_spec' <<< "$manifest")
}

function add_job_history_to_manifest {
  if [ "$skip_create_manifest" = true ]; then return; fi

  echo "Adding job history to the cleanup manifest..."
  manifest=$(jq '.job_history.should_clean = true' <<< "$manifest")
  namespaces=$(helm list --all-namespaces --filter "^cluster-server$" -o json | jq -cr '[.[] | .namespace]')
  manifest=$(jq --argjson namespaces "$namespaces" '.job_history.namespaces = $namespaces' <<< "$manifest")

  # Dump all wsjobs
  kubectl get wsjobs -A --sort-by .metadata.creationTimestamp > ${job_history_outfile} 2>/dev/null
}

function cleanup() {
  function format_progress() {
    if [ "$dryrun" = true ]; then
      sed 's/^/  [dryrun] /'
    else
      sed 's/^/  /'
    fi
  }

  function local_cleanup() {
    local entry="$1"
    local node_selector=$(echo "$entry" | jq -r '.node_selector')
    local paths=$(echo "$entry" | jq -r '.paths[]')
    local nodes

    if [ -n "$node_selector" ]; then
      nodes=$(kubectl get nodes -l "$node_selector" -o=jsonpath='{.items[*].metadata.name}')
    else
      nodes=$(kubectl get nodes -o=jsonpath='{.items[*].metadata.name}')
    fi

    for node in $nodes; do
      for path in $paths; do
        echo "Deleting $path/* on node $node..."
        if [ "$dryrun" = true ]; then continue; fi
          ssh $node rm -rf "$path"/* < /dev/null
      done
    done
  }

  function pvc_cleanup() {
    local ns="$1"
    local entry="$2"
    local timeout_in_seconds=300
    local pvc_name=$(echo "$entry" | jq -r '.pvc_name')
    local mount_path=$(echo "$entry" | jq -r '.mount_path')
    local node_selector_name=$(echo "$entry" | jq -r '.node_selector' | cut -d'=' -f1)
    local node_selector_value=$(echo "$entry" | jq -r '.node_selector' | cut -d'=' -f2)
    local job_name="${pvc_name}-pvc-storage-reset"

    dryrun="$dryrun" \
      ns="$ns" \
      job_name="$job_name" \
      pvc_name="$pvc_name" \
      mount_path="$mount_path" \
      node_selector_name="$node_selector_name" \
      node_selector_value="$node_selector_value" \
    envsubst <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: $job_name
  namespace: $ns
spec:
  ttlSecondsAfterFinished: 600
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: main
        image: registry.local/busybox:1.34.1
        command:
        - "sh"
        - "-c"
        - |
          if [ ! -d "$mount_path" ]; then echo "Directory $mount_path does not exist. Exiting."; exit 0; fi;
          find "$mount_path" -maxdepth 1 -mindepth 1 -exec sh -c 'if [ "$dryrun" = "true" ]; then echo "Deleting {} in $pvc_name PVC"; else rm -rf "{}"; fi' \;
          df -h "$mount_path"
        volumeMounts:
        - mountPath: $mount_path
          name: pvc-vol
      volumes:
      - name: pvc-vol
        persistentVolumeClaim:
          claimName: $pvc_name
      tolerations:
      - key: node-role.kubernetes.io/master
        operator: Exists
        effect: NoSchedule
      - key: node-role.kubernetes.io/control-plane
        operator: Exists
        effect: NoSchedule
      nodeSelector:
        $node_selector_name: "$node_selector_value"
EOF

    # Short pause
    sleep 10

    pod_name=$(kubectl -n ${ns} get pods -l job-name=${job_name} --sort-by .metadata.creationTimestamp -oname | tail -1)

    # Run kubectl in the background and capture its PID
    kubectl -n ${ns} logs -f ${pod_name} &
    kubectl_pid=$!

    # Start a loop that checks the pod's status every few seconds
    timeout_reached=0
    for ((i=0; i < ${timeout_in_seconds}; i+=5)); do
      sleep 5
      pod_status=$(kubectl -n ${ns} get "${pod_name}" -o jsonpath='{.status.phase}')
      if [[ "$pod_status" == "Succeeded" ]] || [[ "$pod_status" == "Failed" ]]; then
        break
      fi
      if ((i >= ${timeout_in_seconds})); then
        timeout_reached=1
        kill -SIGINT $kubectl_pid
        break
      fi
    done

    if [[ "$pod_status" == "Succeeded" ]]; then
      kubectl -n ${ns} delete "job/${job_name}"
    else
      echo "Failed to clean ${pvc_name} pvc."
      echo "The $ns/$job_name job is left behind for further investigation."
      echo "Please clean up the job before rerunning this script."
      exit 1
    fi
  }

  function nfs_cleanup() {
    local job_name="$1"
    local entry="$2"
    local ns="$default_namespace"
    local timeout_in_seconds=300
    local server_address=$(echo "$entry" | jq -r '.server')
    local server_path=$(echo "$entry" | jq -r '.server_path')
    local mount_path=$(echo "$entry" | jq -r '.mount_path')
    local node_selector_name=$(echo "$entry" | jq -r '.node_selector' | cut -d'=' -f1)
    local node_selector_value=$(echo "$entry" | jq -r '.node_selector' | cut -d'=' -f2)

    dryrun="$dryrun" \
      ns="$ns" \
      job_name="$job_name" \
      server_address="$server_address" \
      server_path="$server_path" \
      mount_path="$mount_path" \
      node_selector_name="$node_selector_name" \
      node_selector_value="$node_selector_value" \
      envsubst <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: $job_name
  namespace: $ns
spec:
  ttlSecondsAfterFinished: 600
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: main
        image: registry.local/busybox:1.34.1
        command:
        - "sh"
        - "-c"
        - |
          if [ ! -d "$mount_path" ]; then echo "Directory $mount_path does not exist. Exiting."; exit 0; fi;
          find "$mount_path" -maxdepth 1 -mindepth 1 -exec sh -c 'if [ "$dryrun" = "true" ]; then echo "Deleting {} from NFS ($server_address:$server_path)"; else rm -rf "{}"; fi' \;
          # Best-effort approach to understand the usage
          timeout 60s du -sh "$mount_path" 2>/dev/null || true
        volumeMounts:
        - mountPath: $mount_path
          name: nfs-vol
      volumes:
      - name: nfs-vol
        nfs:
          path: $server_path
          server: $server_address
      tolerations:
      - key: node-role.kubernetes.io/master
        operator: Exists
        effect: NoSchedule
      - key: node-role.kubernetes.io/control-plane
        operator: Exists
        effect: NoSchedule
      nodeSelector:
        $node_selector_name: "$node_selector_value"
EOF

    # Short pause
    sleep 10

    pod_name=$(kubectl -n ${ns} get pods -l job-name=${job_name} --sort-by .metadata.creationTimestamp -oname | tail -1)

    # Run kubectl in the background and capture its PID
    kubectl -n ${ns} logs -f ${pod_name} &
    kubectl_pid=$!

    # Start a loop that checks the pod's status every few seconds
    timeout_reached=0
    for ((i=0; i < ${timeout_in_seconds}; i+=5)); do
      sleep 5
      pod_status=$(kubectl -n ${ns} get "${pod_name}" -o jsonpath='{.status.phase}')
      if [[ "$pod_status" == "Succeeded" ]] || [[ "$pod_status" == "Failed" ]]; then
        break
      fi
      if ((i >= ${timeout_in_seconds})); then
        timeout_reached=1
        kill -SIGINT $kubectl_pid
        break
      fi
    done

    if [[ "$pod_status" == "Succeeded" ]]; then
      kubectl -n ${ns} delete "job/${job_name}"
    else
      echo "Failed to clean '$mount_path/*' from NFS ($server_address:$server_path)."
      echo "The $ns/$job_name job is left behind for further investigation."
      echo "Please clean up the job before rerunning this script."
      exit 1
    fi
  }

  function wait_on_replica_count() {
    local ns="$1"
    local resource_type="$2"
    local resource_name="$3"
    local desired_replicas="$4"

    timeout=120
    count=0
    while [[ $(kubectl -n "$ns" get "${resource_type}" "$resource_name" --no-headers | awk '{print $2}') != "$desired_replicas/$desired_replicas" ]]; do
      sleep 1
      count=$((count + 1))
      if [[ $count -ge $timeout ]]; then
        echo "Timed out waiting for replicas to scale to $desired_replicas."
        exit 1
      fi
    done
  }

  # Clean up local job logs
  local_logs=$(jq -c '.logs.local[]' "$manifest_outfile")
  if [ -n "$local_logs" ]; then
    echo "Going to clean up local job logs..."
    (
      echo "$local_logs" | while read -r entry; do local_cleanup "$entry"; done 2>&1
      echo "Completed!"
    ) | format_progress
  fi

  # Clean up job logs in Ceph
  ceph_logs=$(jq -c '.logs.ceph[]' "$manifest_outfile")
  if [ -n "$ceph_logs" ]; then
    echo "Going to clean up job logs in Ceph..."
    (
      echo "$ceph_logs" | while read -r entry; do pvc_cleanup "$default_namespace" "$entry"; done 2>&1
      echo "Completed!"
    ) | format_progress
  fi

  # Clean up local cached compile
  local_cached_compile=$(jq -c '.cached_compile.local[]' "$manifest_outfile")
  if [ -n "$local_cached_compile" ]; then
    echo "Going to clean up local cached compile..."
    (
      echo "$local_cached_compile" | while read -r entry; do local_cleanup "$entry"; done 2>&1
      echo "Completed!"
    ) | format_progress
  fi

  # Clean up cached compile in Ceph
  ceph_cached_compile=$(jq -c '.cached_compile.ceph[]' "$manifest_outfile")
  if [ -n "$ceph_cached_compile" ]; then
    echo "Going to clean up cached compile in Ceph..."
    (
      echo "$ceph_cached_compile" | while read -r entry; do pvc_cleanup "$default_namespace" "$entry"; done 2>&1
      echo "Completed!"
    ) | format_progress
  fi

  # Clean up monitoring data
  monitoring_data=$(jq -c '.monitoring_data' "$manifest_outfile")
  if [ "$monitoring_data" != "{}" ]; then
    echo "Going to clean up monitoring data..."
    (
      for key in $(jq -r '.monitoring_data | keys[]' "$manifest_outfile"); do
        entry=$(jq -c ".monitoring_data.${key}" "$manifest_outfile")
        ns=$(echo "$entry" | jq -r '.namespace')
        resource_type=$(echo "$entry" | jq -r '.resource_type')
        resource_name=$(echo "$entry" | jq -r '.resource_name')
        desired_replicas=$(kubectl -n "$ns" get "$resource_type" "$resource_name" -o jsonpath='{.spec.replicas}')

        if [ "$key" = "prometheus" ]; then
          prom_crd=$(kubectl -n prometheus get prometheus --no-headers | awk '{print $1}')
          function _restore_replicas {
            kubectl -n prometheus patch prometheus "$prom_crd" --type='json' -p='[{"op": "replace", "path": "/spec/replicas", "value":'$desired_replicas'}]'
          }

          trap _restore_replicas ERR

          kubectl -n prometheus patch prometheus "$prom_crd" --type='json' -p='[{"op": "replace", "path": "/spec/replicas", "value":0}]'
          wait_on_replica_count "$ns" "$resource_type" "$resource_name" "0"
          echo "$entry" | jq -c '.persistence[]' | while read -r persistence_entry; do pvc_cleanup "$ns" "$persistence_entry"; done 2>&1
          _restore_replicas
          wait_on_replica_count "$ns" "$resource_type" "$resource_name" "$desired_replicas"

          trap - ERR
        else
          function _restore_replicas {
            kubectl -n "$ns" scale "$resource_type" "$resource_name" --replicas=$desired_replicas
          }

          trap _restore_replicas ERR

          kubectl -n "$ns" scale "$resource_type" "$resource_name" --replicas=0
          wait_on_replica_count "$ns" "$resource_type" "$resource_name" "0"
          echo "$entry" | jq -c '.persistence[]' | while read -r persistence_entry; do pvc_cleanup "$ns" "$persistence_entry"; done 2>&1
          _restore_replicas
          wait_on_replica_count "$ns" "$resource_type" "$resource_name" "$desired_replicas"

          trap - ERR
        fi
      done
      echo "Completed!"
    ) | format_progress
  fi

  # Clean up user venv volumes
  user_venv_volumes=$(jq -c '.venv_volumes[]' "$manifest_outfile")
  if [ -n "$user_venv_volumes" ]; then
    echo "Going to clean up user venv volume..."
    (
      echo "$user_venv_volumes" | while read -r entry; do nfs_cleanup "user-venv-nfs-storage-reset" "$entry"; done 2>&1
      echo "Completed!"
    ) | format_progress
  fi

  # Clean up cluster volume configurations
  if [ "$cleanup_cluster_volumes" = true ]; then
    echo "Going to clean up cluster volume configurations..."
    (
      cm_name="cluster-server-volumes"
      jq -c '.cluster_volumes.namespaces[]' "$manifest_outfile" | while read -r ns; do
        echo "Deleting ConfigMap $cm_name in the $ns namespace"
        if [ "$dryrun" = false ]; then
          kubectl -n $ns delete cm $cm_name --ignore-not-found 2>&1
        fi
      done 2>&1
      echo "Completed!"
    ) | format_progress
  fi

  # Clean up custom worker images
  custom_worker_images=$(jq -c '.custom_worker_images[]' "$manifest_outfile")
  if [ -n "$custom_worker_images" ]; then
    echo "Going to clean up custom worker images..."
    (
      echo "$custom_worker_images" | while read -r entry; do
        image_name="custom-worker"
        tag=$(echo "$entry" | jq -r '.tag')
        digest=$(echo "$entry" | jq -r '.digest')
        echo "Deleting $image_name:$tag ($digest)..."
        if [ "$dryrun" = false ]; then
          curl -sk -X DELETE "${registry_url}/v2/${image_name}/manifests/${digest}"
        fi
      done 2>&1

      echo "Garbage collecting in private registry..."
      if [ "$dryrun" = false ]; then
        registry_pod=$(kubectl get pod -n kube-system -lapp=private-registry --no-headers | grep Running | awk '{print $1}' | head -1)
        kubectl exec "${registry_pod}" -nkube-system -- registry garbage-collect -m /etc/docker/registry/config.yml 2>&1
        kubectl -n kube-system rollout restart daemonset private-registry 2>&1
      fi
      echo "Completed!"
    ) | format_progress
  fi

  # Clean up worker cache
  if [ "$cleanup_worker_cache" = true ]; then
    echo "Going to clean up worker cache..."
    (
      if [ "$dryrun" = false ]; then
        csctl --namespace ${default_namespace} clear-worker-cache 2>&1
      fi
      echo "Completed!"
    ) | format_progress
  fi

  # Clean up job history
  if [ "$cleanup_job_history" = true ]; then
    echo "Going to clean up job history..."
    (
      jq -c '.job_history.namespaces[]' "$manifest_outfile" | while read -r ns; do
        echo "Deleting all jobs in the $ns namespace"
        if [ "$dryrun" = false ]; then
          kubectl -n $ns delete wsjob --all 2>&1
          kubectl -n $ns delete job --all 2>&1
          kubectl -n $ns delete pod -l app.kubernetes.io/name=cluster-server
        fi
      done 2>&1
      echo "Completed!"
    ) | format_progress
  fi
}

# Parse command line options
while [ "$1" != "" ]; do
  case $1 in
    --help ) print_help;;
    --job-logs ) cleanup_job_logs=true;;
    --cached-compile ) cleanup_cached_compile=true;;
    --user-venvs ) cleanup_user_venvs=true;;
    --cluster-volumes ) cleanup_cluster_volumes=true;;
    --custom-worker-images ) cleanup_custom_worker_images=true;;
    --worker-cache ) cleanup_worker_cache=true;;
    --all ) 
      cleanup_job_logs=true
      cleanup_cached_compile=true
      cleanup_user_venvs=true
      cleanup_cluster_volumes=true
      cleanup_custom_worker_images=true
      cleanup_worker_cache=true
      ;;
    --monitoring-data ) cleanup_monitoring_data=true;;
    --job-history ) cleanup_job_history=true;;
    --dry-run ) dryrun=true;;
    --use-manifest ) 
      shift
      existing_manifest="$1"
      skip_create_manifest=true
      ;;
    * ) 
      echo "Invalid option: $1"
      echo "Usage: $0 [--help] [--job-logs] [--cached-compile] [--user-venvs] " \
           "[--cluster-volumes] [--custom-worker-images] [--worker-cache] [--all] " \
           "[--monitoring-data] [--job-history] [--dry-run] [--use-manifest FILE]"
      exit 1
      ;;
  esac
  shift
done

# Check if any options were provided
if [ "$cleanup_job_logs" = false ] && \
   [ "$cleanup_cached_compile" = false ] && \
   [ "$cleanup_user_venvs" = false ] && \
   [ "$cleanup_cluster_volumes" = false ] && \
   [ "$cleanup_custom_worker_images" = false ] && \
   [ "$cleanup_worker_cache" = false ] && \
   [ "$cleanup_monitoring_data" = false ] && \
   [ "$cleanup_job_history" = false ]; then
  echo "Error: You need to provide at least one option."
  print_help
  exit 1
fi

if [ "$skip_create_manifest" = true ]; then
  if [ -z "$existing_manifest" ]; then
    echo "Error: You need to provide a manifest file using the --use-manifest option."
    print_help
    exit 1
  elif [ ! -f "$existing_manifest" ]; then
    echo "Error: The manifest provided '$existing_manifest' is not a file."
    exit 1
  fi

  manifest=$(cat "${existing_manifest}")
fi

# Perform the selected actions
if [ "$cleanup_job_logs" = true ]; then
  add_logs_to_manifest
fi

if [ "$cleanup_cached_compile" = true ]; then
  add_cached_compile_to_manifest
fi

if [ "$cleanup_user_venvs" = true ]; then
  add_user_venvs_to_manifest
fi

if [ "$cleanup_cluster_volumes" = true ]; then
  if [ "$cleanup_user_venvs" = false ]; then
    # Must clean up user venvs before cleaning up the cluster volumes
    add_user_venvs_to_manifest
  fi
  add_cluster_volumes_to_manifest
fi

if [ "$cleanup_custom_worker_images" = true ]; then
  add_custom_worker_images_to_manifest
fi

if [ "$cleanup_worker_cache" = true ]; then
  add_worker_cache_to_manifest
fi

if [ "$cleanup_monitoring_data" = true ]; then
  add_monitoring_data_to_manifest
fi

if [ "$cleanup_job_history" = true ]; then
  add_job_history_to_manifest
fi

# Output the manifest
echo "${manifest}" > ${manifest_outfile}
echo "Going to clean up according to the following manifests:"
cat ${manifest_outfile}

cleanup

echo "Storage reset operation complete."
