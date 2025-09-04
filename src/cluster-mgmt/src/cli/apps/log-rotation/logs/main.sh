#!/usr/bin/env bash

# this is necessary since crontab won't have the env var
export PATH="${PATH}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"

set -eo pipefail

CLUSTER_PROPERTIES="/opt/cerebras/cluster/pkg-properties.yaml"
function get_image_version() {
  image=$1
  if [ -f "${CLUSTER_PROPERTIES}" ]; then
    yq -ojson ${CLUSTER_PROPERTIES} | jq -r ".properties.images.\"${image}\" // \"latest\""
  else
    kubectl get cm job-operator-cluster-env -ojson | jq -r ".metadata.annotations.\"${image}-tag\" // \"latest\""
  fi
}

export script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
now=$(date -u +%Y%m%dT%H%M%SZ)
if [ -n "${dryrun}" ]; then
  # Used during deployments or adhoc validations in clusters
  export metadata_dir="${script_dir}/dryrun-${now}"
elif [ -n "${in_smoke_test}" ]; then
  # Used only during local development and testing
  export metadata_dir="${script_dir}/smoke-test-${now}"
else
  # Used in real cleanup in clusters
  export metadata_dir="${script_dir}/metadata-${now}"
fi

source ${script_dir}/../common.sh
source ${script_dir}/utils.sh

# We dry run the cleanup script during deployment and the NFS part can take a very long time.
# Add a new option to skip NFS cleanup during deployment.
skip_nfs_cleanup=${skip_nfs_cleanup:-}

rm -rf ${metadata_dir}
mkdir -p ${metadata_dir}

if [ -z "$in_smoke_test" ] && [ -z "$dryrun" ]; then
  exec >>${metadata_dir}/log-clean.log 2>&1
fi

if ! bash ${script_dir}/ctr-clean.sh; then
  # fail early to catch errors in dryrun test
  if [ -n "$dryrun" ]; then
    cleanup_metadata "$script_dir"
    exit 1
  fi
  echo "image clean failed, continue to log clean"
fi

if [ "$(is_lead_control_plane)" == "true" ]; then
  # The whitelist file contains the paths of the cached compile artifacts that are currently being used by the pending/running jobs
  # TODO: If time permits, it'd be very valuable to integrate at least part of the logs/artifact rotation with the operator.
  cached_compile_whitelist_file="${metadata_dir}/cached_compile_whitelist.out"

  # Use lock objects to identify pending/running jobs
  active_locks_file="${metadata_dir}/active_locks.out"
  if kubectl get rl &>/dev/null; then
    kubectl get rl -A -o json | jq -r '
  .items[]
  | {namespace: .metadata.namespace, workflow_id: .metadata.labels["cerebras/workflow-id"]}
  | select(.workflow_id != null)
  | "\(.namespace)\t\(.workflow_id)"
' | sort -u >"${active_locks_file}"

    # Check each workflow
    while read -r namespace workflow; do
      # Check if the compile job type already has the annotation
      compile_artifact_dirs=$(kubectl get wsjob -n "${namespace}" --ignore-not-found -l cerebras/workflow-id="${workflow}",k8s.cerebras.com/wsjob-job-type=compile -o json |
        jq -r '.items[]
          | .metadata.annotations["cerebras/compile_artifact_dir"] // empty')

      # If the annotation exists, skip the execute job type check
      if [[ -n "$compile_artifact_dirs" ]]; then
        echo "$compile_artifact_dirs" >>"${cached_compile_whitelist_file}"
        continue
      fi
      # Otherwise, check the execute job type
      kubectl get wsjob -n"${namespace}" --ignore-not-found -l cerebras/workflow-id="${workflow}",k8s.cerebras.com/wsjob-job-type=execute -o json |
        jq -r '.items[]
          | .spec.wsReplicaSpecs.Coordinator.template.spec.containers[0].env
          | map(select(.name == "COMPILE_ARTIFACT_DIR"))
          | .[].value' >>"${cached_compile_whitelist_file}"
    done <"${active_locks_file}"
  fi
fi

clean_local_logs local_logs
if [[ -n "${in_smoke_test}" || "$(is_lead_control_plane)" == "true" ]]; then
  clean_local_logs deployment_package
  # We don't need to clean up debug artifacts from local when this is a multi-mgmt cluster.
  if [ "$(has_multiple_mgmt_nodes)" != "true" ]; then
    clean_local_logs debug_artifact
  fi
fi
echo ""

if [ -z "${in_smoke_test}" ] && [ "$(is_lead_control_plane)" == "false" ]; then
  echo "exit since $(get_hostname) is not a lead control plane"
  cleanup_metadata "$script_dir"
  exit 0
fi

function wait_job() {
  local job_name=$1

  function get_job_status() {
    local timeout="$1"
    local job_status="Running"
    # Wait for the job to complete or fail
    if kubectl -n ${ns} wait --for=condition=complete --timeout="${timeout}" job/${job_name} >/dev/null 2>&1; then
      job_status="Succeeded"
    else
      job_status="Failed"
    fi

    echo "$job_status"
  }

  # Wait for at least one pod to be ready
  if ! kubectl -n ${ns} wait --for=condition=Ready --timeout=2m pod -l job-name=${job_name}; then
    echo "WARNING: pod did not become ready in 2 minutes"
    # It could be possible that the job had already completed successfully
    job_status=$(get_job_status 3s)
    [ "$job_status" = "Succeeded" ] && return 0 || return 1
  fi

  local pod_name=$(kubectl -n ${ns} get pod --sort-by .metadata.creationTimestamp -ljob-name=${job_name} -oname | head -1)

  # Run kubectl in the background and capture its PID
  kubectl -n ${ns} logs -f ${pod_name} &
  kubectl_pid=$!

  job_status=$(get_job_status 55m)

  # Kill the background kubectl process
  kill -SIGINT $kubectl_pid 2>/dev/null || true
  [ "$job_status" = "Succeeded" ] && return 0 || return 1
}

if [ -z "${in_smoke_test}" ] && [ "$(has_multiple_mgmt_nodes)" == "true" ]; then
  export dryrun=${dryrun:-}
  export in_smoke_test=${in_smoke_test:-}
  mapfile -t categories < <(jq -r ".volumes[] | select(.pvc != \"\") | .category" ${config_file})
  mapfile -t pvc_names < <(jq -r ".volumes[] | select(.pvc != \"\") | .pvc" ${config_file})
  mapfile -t ceph_pvc_mount_paths < <(jq -r ".volumes[] | select(.pvc != \"\") | .path" ${config_file})
  export cleanup_dir=$(realpath ${script_dir}/..)
  export tag=$(get_image_version alpine-kubectl)

  for i in "${!pvc_names[@]}"; do
    export category="${categories[$i]}"
    export pvc_name="${pvc_names[$i]}"
    export ceph_pvc_mount_path="${ceph_pvc_mount_paths[$i]}"
    export metadata_prefix=$(echo "ceph-$i-$category" | tr '-' '_')
    if [ -n "${dryrun}" ]; then
      export job_name=$(echo "dryrun-$metadata_prefix-cleanup-job" | tr '_' '-')
    else
      export job_name=$(echo "$metadata_prefix-cleanup-job" | tr '_' '-')
    fi

    kubectl -n ${ns} delete job/${job_name} --ignore-not-found
    cat ${script_dir}/ceph_pvc_cleanup_job.yaml.template | envsubst | kubectl apply -f-

    if wait_job ${job_name}; then
      kubectl -n ${ns} delete "job/${job_name}" --ignore-not-found
    else
      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] WARNING: failed to clean ${pvc_name} pvc."
      echo "The $ns/$job_name job is left behind for further investigation."
    fi
    echo ""
  done
fi

if [ -z "$skip_nfs_cleanup" ]; then
  export dryrun=${dryrun:-}
  export in_smoke_test=${in_smoke_test:-}
  mapfile -t categories < <(jq -r ".volumes[] | select(.on_nfs == \"true\") | .category" ${config_file})
  mapfile -t mount_paths < <(jq -r ".volumes[] | select(.on_nfs == \"true\") | .path" ${config_file})
  mapfile -t server_paths < <(jq -r ".volumes[] | select(.on_nfs == \"true\") | .server_path" ${config_file})
  mapfile -t server_addresses < <(jq -r ".volumes[] | select(.on_nfs == \"true\") | .server_address" ${config_file})
  export cleanup_dir=$(realpath ${script_dir}/..)

  for i in "${!mount_paths[@]}"; do
    export category="${categories[$i]}"
    export mount_path="${mount_paths[$i]}"
    export metadata_prefix=$(echo "nfs-$i-$category" | tr '-' '_')

    if [ -n "${in_smoke_test}" ]; then
      # We don't bring up a job/pod during smoke test.
      clean_nfs_logs ${category} ${mount_path} ${metadata_prefix}
    else
      export tag=$(get_image_version alpine-kubectl)
      export server_path="${server_paths[$i]}"
      export server_address="${server_addresses[$i]}"
      if [ -n "${dryrun}" ]; then
        export job_name=$(echo "dryrun-$metadata_prefix-cleanup-job" | tr '_' '-')
      else
        export job_name=$(echo "$metadata_prefix-cleanup-job" | tr '_' '-')
      fi

      kubectl -n ${ns} delete job/${job_name} --ignore-not-found
      cat ${script_dir}/nfs_cleanup_job.yaml.template | envsubst | kubectl apply -f-

      if wait_job ${job_name}; then
        kubectl -n ${ns} delete "job/${job_name}" --ignore-not-found
      else
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] WARNING: failed to clean '$mount_path/*' from NFS ($server_address:$server_path)."
        echo "The $ns/$job_name job is left behind for further investigation."
      fi
      echo ""
    fi
  done
fi

if [ -n "${dryrun}" ]; then
  echo "dryrun done"
fi

cleanup_metadata "$script_dir"
