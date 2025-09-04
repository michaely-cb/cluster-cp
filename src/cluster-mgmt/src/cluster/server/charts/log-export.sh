#!/bin/bash

set -x

SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE:-job-operator}
NAMESPACE=${NAMESPACE:-job-operator}
WORKDIR_ROOT=${WORKDIR_ROOT:-/n1/wsjob/workdir}
JOB_WORKDIR=${JOB_WORKDIR:-""}
CLUSTER_MGMT_LOG_PATH=${CLUSTER_MGMT_LOG_PATH:-/n0/cluster-mgmt}
NUM_LINES=${NUM_LINES:-0}
TASK_FILTER=${TASK_FILTER:-""}
MAX_PSSH_THREADS=${MAX_PSSH_THREADS:-10}

if [ "${USE_MOUNT_DIR_TARGET}" != "true" ]; then
  mkdir -p /root/.ssh
  cp -r /host_root/.ssh/* /root/.ssh/.
  # There is a chance for known hosts conflict if we do not clean it up.
  rm -f /root/.ssh/known_hosts
fi

SSH_USER="root"
SSH_OPTION="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o UserKnownHostsFile=/dev/null -l ${SSH_USER}"

export SCP="scp -r ${SSH_OPTION}"
export SSH="ssh ${SSH_OPTION}"
export RSYNC="rsync -av --links --rsh=\"$SSH\""

if [ "${USE_DEBUG_VOLUME}" == "true" ]; then
  echo "Debug volume mode on"
  # For debug volume export, the following configuration should always be true
  # There are client-side guardrail checks already. Explicitly setting them here for clarity.
  WITH_BINARY="false"
  WITH_COMPILE_ARTIFACTS="false"
  COPY_NFS_LOGS="true"
  NUM_LINES=0
  SKIP_COMPRESS="true"
  EXPORT_ID="${JOB_ID}"
elif [ "${USE_MOUNT_DIR_TARGET}" == "true" ]; then
  echo "Mount dir target mode on"
fi

EXPORT_LOG_PATH=${EXPORT_LOG_PATH:-/n1/log-export}
EXPORT_DIR="${EXPORT_LOG_PATH}/${EXPORT_ID}"
EXPORT_LOG="${EXPORT_LOG_PATH}/${EXPORT_ID}.logs"
LOG_EXPORT_SPEC_YAML="${EXPORT_LOG_PATH}/${EXPORT_ID}.yaml"
EXPORT_ZIPFILE="${EXPORT_LOG_PATH}/${EXPORT_ID}.zip"
JOB_SPEC_YAML="${EXPORT_DIR}/${JOB_ID}.yaml"
ALL_NODES_FILE="${EXPORT_LOG_PATH}/${EXPORT_ID}-all-nodes.logs"
MGMT_NODE_ROLE="k8s.cerebras.com/node-role-management"
COORD_NODE_ROLE="k8s.cerebras.com/node-role-coordinator"

main() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"

  mkdir -p "${EXPORT_DIR}"
  # Clean up the export directory if not using debug volume or NFS target
  if [ "${USE_DEBUG_VOLUME}" != "true" ] && [ "${USE_MOUNT_DIR_TARGET}" != "true" ]; then
    rm -rf ${EXPORT_ZIPFILE} ${EXPORT_DIR}/* 2>/dev/null
  else
    # Retention marker is for future use where we can track the last successful export
    # and the expected retention period for the artifacts.
    rm -f ${EXPORT_DIR}/.retention-marker.out 2>/dev/null
  fi

  # Persist the log export job yaml for debug purposes
  kubectl -n "${NAMESPACE}" get job "${EXPORT_ID}" --ignore-not-found -oyaml > ${LOG_EXPORT_SPEC_YAML}

  # Respect volume mount if the job spec is still available
  JOB_WORKDIR="${WORKDIR_ROOT}/${NAMESPACE}/${JOB_ID}"
  kubectl -n "${NAMESPACE}" get wsjob "${JOB_ID}" --ignore-not-found -oyaml > ${JOB_SPEC_YAML}
  if [ -s ${JOB_SPEC_YAML} ]; then
    echo "Job spec of ${JOB_ID} is retrieved"
    JOB_WORKDIR=$(yq '.spec.wsReplicaSpecs | .Coordinator // .Worker | .template.spec.containers[0].volumeMounts[] | select(.name == "workdir-volume") | .mountPath' ${JOB_SPEC_YAML})
  else
    echo "Job spec of ${JOB_ID} could not be found in the ${NAMESPACE} namespace"
    rm -f ${JOB_SPEC_YAML}
  fi
  echo "Using ${JOB_WORKDIR} as the workdir for log export"
  echo "Export directory: ${EXPORT_DIR}"

  # If USE_DEBUG_VOLUME is set, we skip the compression step and leave the logs in the target location.
  echo "Use debug volume: ${USE_DEBUG_VOLUME}"

  # If USE_MOUNT_DIR_TARGET is set, we skip the compression step and leave the logs in the target location.
  echo "Use mount dir target: ${USE_MOUNT_DIR_TARGET}"

  # If WITH_BINARY is set, all files including the binaries in the workdir get exported.
  echo "Binary mode: ${WITH_BINARY}"

  # If WITH_COMPILE_ARTIFACTS is set, all files in the cached compile directory get exported.
  echo "Compile artifact mode: ${WITH_COMPILE_ARTIFACTS}"

  # If COPY_NFS_LOGS is set, copy logs instead of creating symlinks for NFS content.
  echo "Copy NFS logs: ${COPY_NFS_LOGS}"

  # If non-zero, return last NUM_LINES
  echo "Num lines: ${NUM_LINES}"

  # If task filter was specified, apply a regex to the `find` command
  echo "Task filter: ${TASK_FILTER}"
  if [ -n "${TASK_FILTER}" ]; then
    # "coordinator-0,chief-0" -> "${JOB_WORKDIR}/coordinator-0 ${JOB_WORKDIR}/chief-0"
    # "worker" -> "${JOB_WORKDIR}/worker-0 ${JOB_WORKDIR}/worker-1 ...""
    IFS=',' read -ra ADDR <<< "$TASK_FILTER"
    for i in "${ADDR[@]}"; do
      if [[ $i == *"-"* ]]; then
        # If the element contains a dash, use it as is
        TASK_FILTER_PATHS+="${JOB_WORKDIR}/$i "
      else
        # If the element does not contain a dash, use find to list all dirs with that prefix
        TASK_FILTER_PATHS+="${JOB_WORKDIR}/$i-* "
      fi
    done
  fi

  get_ip_hostname() {
    # Example output:
    # 172.16.131.1 net001-ax-sr01 k8s.cerebras.com/node-role-activation
    # 172.16.131.3 net001-ax-sr02 k8s.cerebras.com/node-role-activation
    jq -r '
      .items[] |
      {
        ip: (.status.addresses[]? | select(.type == "InternalIP") | .address),
        hostname: (.status.addresses[]? | select(.type == "Hostname") | .address),
        roles: (
          .metadata.labels // {} |
          to_entries |
          map(select(.key | startswith("k8s.cerebras.com/node-role")) | .key) |
          join(",")
        )
      } |
      "\(.ip) \(.hostname) \(.roles)"
    '
  }
  kubectl get no -o=json | get_ip_hostname > ${ALL_NODES_FILE}

  t0="$(date -u +%s)"
  gather_job_logs

  if [ ${NUM_LINES} -eq 0 ]; then
    gather_cluster_mgmt_logs
  fi

  t1="$(date -u +%s)"
  echo "Total of $((t1 - t0)) seconds elapsed for gather logs"

  if [ "${USE_DEBUG_VOLUME}" == "true" ] || [ "${USE_MOUNT_DIR_TARGET}" == "true" ]; then
    echo "Skip log compression since the logs are already in the target location"
    touch ${EXPORT_DIR}/.retention-marker.out
    chmod 666 ${EXPORT_DIR}/.retention-marker.out
    exit 0
  fi

  if [ -z "${SKIP_COMPRESS}" ]; then
    compress_staging_path
  fi
  t2="$(date -u +%s)"
  echo "Total of $((t2 - t1)) seconds elapsed for compress"
  echo "Total of $((t2 - t0)) seconds elapsed overall"

  # Delete the export dir since no longer required
  rm -rf ${EXPORT_DIR}

  # Example: /n1/log-export/<namespace>
  # Since the directory is created by kubelet and has the 755 permission,
  # the following zipfile deletion would not work unless we chmod this directory to 777
  chmod 777 ${EXPORT_LOG_PATH}

  # Change permissions to enable artifact deletion later on
  chown 65532:65532 ${EXPORT_LOG}
  chown 65532:65532 ${EXPORT_ZIPFILE}
}

gather_nlines_job_logs() {
  TAG="tail"
  LOG_FILTER="'out$|err$|log$|txt$|grpc$|br-'"
  # If we don't have a set of specific tasks to look for, we will look under the root workdir of the job.
  if [ -z "${TASK_FILTER_PATHS}" ]; then
    TASK_FILTER_PATHS="${JOB_WORKDIR}"
  fi

  if df -PT "${JOB_WORKDIR}" | grep -q "nfs" 2>/dev/null; then
    # For NFS storage, we can directly access the files locally
    find ${TASK_FILTER_PATHS} 2>/dev/null | grep -E $LOG_FILTER | xargs -I {} bash -c "tail {} -n $NUM_LINES > {}.$TAG"
    find ${TASK_FILTER_PATHS} 2>/dev/null | grep -E .${TAG}$ | xargs -I {} bash -c "dirname {} | sed -e 's#^${JOB_WORKDIR}#${EXPORT_DIR}#' | xargs -I {{}} mkdir -p {{}}"
    find ${TASK_FILTER_PATHS} 2>/dev/null | grep -E .${TAG}$ | xargs -I {} bash -c "echo {} | sed -e 's#^${JOB_WORKDIR}#${EXPORT_DIR}#' | xargs -I {{}} cp {} {{}}"
    find ${TASK_FILTER_PATHS} 2>/dev/null | grep -E .${TAG}$ | xargs -I {} rm -f {}
  else
    cat ${ALL_NODES_FILE} | parallel --will-cite -j ${MAX_PSSH_THREADS} '
      NODE_IP=$(echo {} | awk "{print \$1}")
      NODE_HOSTNAME=$(echo {} | awk "{print \$2}")
      echo "Processing tailed logs on node ${NODE_HOSTNAME} (${NODE_IP})..."

      # Create tail files on remote node by tailing matching log files
      ssh '"${SSH_OPTION}"' ${NODE_IP} "find '"${TASK_FILTER_PATHS}"' 2>/dev/null | grep -E '"${LOG_FILTER}"' | xargs -I {} bash -c \"tail {} -n '"${NUM_LINES}"' > {}.$TAG\" || true"

      # Create rsync filter to include only tail files
      TAIL_INCLUDE_PATTERN="--include=*.$TAG --include=*/ --exclude=*"

      for PATH_ITEM in '"${TASK_FILTER_PATHS}"'; do
        echo "Pulling tail files from path: ${PATH_ITEM} on node ${NODE_HOSTNAME}"
        # Pull the tail files directly, rsync will preserve directory structure
        '"$RSYNC"' -R ${TAIL_INCLUDE_PATTERN} ${NODE_IP}:"${PATH_ITEM}" '"${EXPORT_DIR}"'/ || true
      done

      # Cleanup tail files on remote node
      ssh '"${SSH_OPTION}"' ${NODE_IP} "find '"${TASK_FILTER_PATHS}"' -name \"*.$TAG\" -type f -delete || true"
    '
  fi
  # first create partial files
  echo "find ${TASK_FILTER_PATHS} 2>/dev/null | grep -E '$LOG_FILTER' | xargs -I {} bash -c \"tail {} -n $NUM_LINES > {}.$TAG\""
  # do copy
  echo "find ${TASK_FILTER_PATHS} 2>/dev/null | grep -E '.${TAG}$' | xargs -I {} $SSH ${POD_IP} \"dirname {} | sed -e 's#^${JOB_WORKDIR}#${EXPORT_DIR}#' | xargs -I {{}} mkdir -p {{}}\""
  echo "find ${TASK_FILTER_PATHS} 2>/dev/null | grep -E '.${TAG}$' | xargs -I {} bash -c \"echo {} | sed -e 's#^${JOB_WORKDIR}#${EXPORT_DIR}#' | xargs -I {{}} $SCP {} ${POD_IP}:{{}}\""
  # clean up partial files
  echo "find ${TASK_FILTER_PATHS} 2>/dev/null | grep -E '.${TAG}$' | xargs -I {} rm -f {}"
}

gather_job_logs() {
  DEBUG_ARTIFACT_FILTER="--include='*.mlir' --include='*.json'"
  if [ "${USE_DEBUG_VOLUME}" == "true" ]; then
    DEBUG_ARTIFACT_FILTER="--exclude='*/compiles/cs_*/' --exclude='*/compiles/cs_*/*' --include='artifacts/**' \
                           --include='kernel_tree.json' --include='kernel_annotation.json' --include='dbg_rt_mlir.json' \
                           --include='stream_list.json' --include='global_stall_view.json' --include='viz_dataflow_view.json'"
  fi
  FILE_FILTER="--include='*/' $DEBUG_ARTIFACT_FILTER --include='*.out' --include='*.err' --include='*.log' --include='*.txt' --include='*.yaml' \
               --include='*.grpc' --include='*.pbbin' --include='br-*' --include='tsc_*' --include='gsv_db.sqlite' --include='*stall_data.pb' --include='gsv_summary.pb' --include='dataless_ckpt.pb' \
               --include='debug_compile_artifacts.tar.gz' --include='*.symbols' --include='*.params' --include='*.casm' --include='latest' --exclude='*'"
  if df -PT "${JOB_WORKDIR}" | grep -q "nfs" 2>/dev/null; then
    echo "Logs reside on NFS mount points. Going to copy from NFS."
    if [ $NUM_LINES -gt 0 ]; then
      gather_nlines_job_logs
    elif [ "${COPY_NFS_LOGS}" == "false" ]; then
      find ${JOB_WORKDIR} -maxdepth 1 -mindepth 1 -type d | while read -r dir; do \
        ln -sf $dir ${EXPORT_DIR}/$(basename $dir); \
      done
    elif [ "$WITH_BINARY" == "true" ]; then
      if [ -z "${TASK_FILTER_PATHS}" ]; then
        cp -Pr ${JOB_WORKDIR}/* "${EXPORT_DIR}" >/dev/null
      else
        cp -Pr ${TASK_FILTER_PATHS} "${EXPORT_DIR}" >/dev/null
      fi
    else
      # If we don't have a set of specific tasks to look for, we will look under the root workdir of the job.
      local find_option="-maxdepth 0"
      if [ -z "${TASK_FILTER_PATHS}" ]; then
        TASK_FILTER_PATHS="${JOB_WORKDIR}"
        find_option="-mindepth 1 -maxdepth 1"
      fi
      # file filter only works well when wrapped around with "bash -c"
      # otherwise, everything gets copied
      # TODO: add test coverage for this and understand better why "bash -c" wrapping is needed
      bash -c "find ${TASK_FILTER_PATHS} ${find_option} -type d | while read -r dir; do \
        target_dir=\$(echo \"\$dir\" | sed -e 's#^${JOB_WORKDIR}#${EXPORT_DIR}#' | xargs dirname); \
        $RSYNC ${FILE_FILTER} \"\$dir\" \"\$target_dir\" 2>/dev/null; \
      done"
    fi
  else
    echo "Gathering log from each node for this job..."

    if [ "$WITH_BINARY" == "true" ]; then
      # When exporting binaries, copy all files from each node’s job directory in parallel.
      if [ -z "${TASK_FILTER_PATHS}" ]; then
        cat ${ALL_NODES_FILE} | parallel --will-cite -j ${MAX_PSSH_THREADS} '
          NODE_IP=$(echo {} | awk "{print \$1}")
          NODE_HOSTNAME=$(echo {} | awk "{print \$2}")
          echo "Copying all files from node ${NODE_HOSTNAME} (${NODE_IP})..."
          '"$RSYNC"' ${NODE_IP}:'"${JOB_WORKDIR}"'/* '"${EXPORT_DIR}"'/ 2>/dev/null || true
        '
      else
        for path in ${TASK_FILTER_PATHS}; do
          cat ${ALL_NODES_FILE} | parallel --will-cite -j ${MAX_PSSH_THREADS} '
            NODE_IP=$(echo {} | awk "{print \$1}")
            NODE_HOSTNAME=$(echo {} | awk "{print \$2}")
            echo "Copying '"${path}"' from node ${NODE_HOSTNAME} (${NODE_IP})..."
            '"$RSYNC"' ${NODE_IP}:'"${path}"' '"${EXPORT_DIR}"'/ 2>/dev/null || true
          '
        done
      fi
    elif [ $NUM_LINES -gt 0 ]; then
      gather_nlines_job_logs
    else
      # If we don't have a set of specific tasks to look for, we will look under the root workdir of the job.
      local find_option="-maxdepth 0"
      if [ -z "${TASK_FILTER_PATHS}" ]; then
        TASK_FILTER_PATHS="${JOB_WORKDIR}"
        find_option="-mindepth 1 -maxdepth 1"
      fi

      # Recursively copy job log directories from each node, preserving structure and running in parallel.
      cat ${ALL_NODES_FILE} | parallel --will-cite -j ${MAX_PSSH_THREADS} '
        NODE_IP=$(echo {} | awk "{print \$1}")
        NODE_HOSTNAME=$(echo {} | awk "{print \$2}")
        echo "Copying job logs from node ${NODE_HOSTNAME} (${NODE_IP})..."

        # Stream processing of directories at the specified depth
        # This avoids storing a potentially large list in a variable
        ssh '"${SSH_OPTION}"' ${NODE_IP} "find '"${TASK_FILTER_PATHS}"' '"${find_option}"' -type d 2>/dev/null" | while read -r REMOTE_DIR; do
          if [ -n "${REMOTE_DIR}" ]; then
            # Calculate target directory
            TARGET_DIR=$(echo "${REMOTE_DIR}" | sed -e "s#^'"${JOB_WORKDIR}"'#'"${EXPORT_DIR}"'#" | xargs dirname)
            mkdir -p "${TARGET_DIR}"

            echo "Copying ${REMOTE_DIR} from node ${NODE_HOSTNAME} to ${TARGET_DIR}..."
            '"$RSYNC"' '"${FILE_FILTER}"' ${NODE_IP}:${REMOTE_DIR} "${TARGET_DIR}/" || true
          fi
        done
      '
    fi
  fi

  # If coordinator workdir wasn't collected, we should collect it to get all compile artifact locations.
  # We should also collect the cluster details config map, as the debug tool uses it to reconstruct the task topology for debug purposes.
  if [ ! -d ${EXPORT_DIR}/coordinator-0 ]; then
    rsync_option="--include='*/' --include='.compile_artifact_location.out' --include='cluster-details-config-map.json' --exclude='*'"

    # Process coordinator nodes in parallel
    cat ${ALL_NODES_FILE} | grep -w "${COORD_NODE_ROLE}" | parallel --will-cite -j ${MAX_PSSH_THREADS} '
      NODE_IP=$(echo {} | awk "{print \$1}")
      NODE_HOSTNAME=$(echo {} | awk "{print \$2}")
      echo "Fetching coordinator artifacts from ${NODE_HOSTNAME} (${NODE_IP})..."
      '"$RSYNC"' '"${rsync_option}"' ${NODE_IP}:'"${JOB_WORKDIR}"'/coordinator-0 '"${EXPORT_DIR}"'/ || true
    '
  fi

  SESSION_WORKDIRS=()
  if [ -f "${EXPORT_DIR}/coordinator-0/.compile_artifact_location.out" ]; then
    # compile artifact location from compile jobs
    SESSION_WORKDIRS="${EXPORT_DIR}/coordinator-0"
  else
    # compile artifact location from execute jobs
    SESSION_WORKDIRS=( $(find ${EXPORT_DIR}/coordinator-0/sessions -mindepth 1 -type d -not -name latest) )
  fi
  for session_workdir in "${SESSION_WORKDIRS[@]}"; do
    compile_artifact_location_file="${session_workdir}/.compile_artifact_location.out"
    if [ -f "${compile_artifact_location_file}" ]; then
      cache_compile_cs_dir=$(cat "${compile_artifact_location_file}" | head -1 | tr -d '"')
    else
      echo "WARNING: Could not find the compile artifact dir due to missing location file."
    fi

    if [ -d "$cache_compile_cs_dir" ]; then
      echo "Found the compile artifact dir '${cache_compile_cs_dir}'."
      base_compile_dir="$(basename $cache_compile_cs_dir)"
      if df -PT "${cache_compile_cs_dir}" | grep -q "nfs" && [ "${COPY_NFS_LOGS}" != "true" ]; then
        ln -sf "${cache_compile_cs_dir}" ${EXPORT_DIR}/${base_compile_dir}
      else
        mkdir -p ${EXPORT_DIR}/${base_compile_dir}
        if [ "${USE_DEBUG_VOLUME}" == "true" ]; then
          cp -f "${cache_compile_cs_dir}"/*.mlir "${cache_compile_cs_dir}"/wio_groups.json ${EXPORT_DIR}/${base_compile_dir}/.
        elif [ "$WITH_COMPILE_ARTIFACTS" != "true" ]; then
          cp -f "${cache_compile_cs_dir}"/*.{mlir,txt,json,yaml,csv} ${EXPORT_DIR}/${base_compile_dir}/.
          cp -f "${cache_compile_cs_dir}"/{copy-ws-opt-files,ws-opt-repro,ws-opt-run-perf-model} ${EXPORT_DIR}/${base_compile_dir}/.
        else
          cp -rf "${cache_compile_cs_dir}"/* ${EXPORT_DIR}/${base_compile_dir}/.
        fi
      fi
    elif [ -n "$cache_compile_cs_dir" ]; then
      echo "WARNING: Compile artifact location '${cache_compile_cs_dir}' is not a directory."
    else
      echo "WARNING: Compile artifact location is an empty string."
    fi
  done

  if [ ! -d "${EXPORT_DIR}/coordinator-0" ] ; then
    echo "WARNING: Expected coordinator logs for ${JOB_ID} but nothing was exported"
  fi
}

# TODO: Consolidate logic of copying logs from different apps
gather_cluster_mgmt_logs() {
  # Create the cluster management logs directory centrally
  mkdir -p ${EXPORT_DIR}/cluster-mgmt

  if [ "$SKIP_CLUSTER_LOGS" == "true" ]; then
    echo "Skip collecting cluster management logs"
    return 0
  fi

  get_rsync_filter() {
    local file_prefix="$1"
    echo "--include='*/' --include='${file_prefix}.log' --include='${file_prefix}.log.1' --exclude='*'"
  }

  # Function to handle job-operator logs
  copy_job_operator_logs() {
    local log_dir="${CLUSTER_MGMT_LOG_PATH}/job-operator"

    # We always export the job operator namespace log
    local file_prefixes=("job-operator-controller-manager")
    # In rel 2.0, we introduced a cluster-mode job operator, so job operator doesn't need to be deployed in other namespaces.
    # In case the user namespace job operator was deployed, we'd also export the logs as well.
    if [ "$NAMESPACE" != "$SYSTEM_NAMESPACE" ]; then
      file_prefixes+=("${NAMESPACE}-controller-manager")
    fi

    # Copy job-operator logs from each management node in parallel
    for file_prefix in "${file_prefixes[@]}"; do
      local rsync_filter=$(get_rsync_filter "$file_prefix")
      cat ${ALL_NODES_FILE} | grep -w ${MGMT_NODE_ROLE} | parallel --will-cite -j ${MAX_PSSH_THREADS} '
        NODE_IP=$(echo {} | awk "{print \$1}")
        NODE_HOSTNAME=$(echo {} | awk "{print \$2}")
        # Create the node-specific directory
        mkdir -p "'"${EXPORT_DIR}"'/cluster-mgmt/${NODE_HOSTNAME}"
        '"$RSYNC"' '"${rsync_filter}"' ${NODE_IP}:'"${log_dir}"' "'"${EXPORT_DIR}"'/cluster-mgmt/${NODE_HOSTNAME}/" || true
      '
    done
  }

  # Function to handle cluster-server logs
  copy_cluster_server_logs() {
    local log_dir="${CLUSTER_MGMT_LOG_PATH}/cluster-server"

    # Explicitly try to copy .log and .log.1 files if they exist
    local file_prefix="${NAMESPACE}-cluster-server"
    local rsync_filter=$(get_rsync_filter "$file_prefix")

    # Copy cluster-server logs from each management node in parallel
    cat ${ALL_NODES_FILE} | grep -w ${MGMT_NODE_ROLE} | parallel --will-cite -j ${MAX_PSSH_THREADS} '
      NODE_IP=$(echo {} | awk "{print \$1}")
      NODE_HOSTNAME=$(echo {} | awk "{print \$2}")
      mkdir -p "'"${EXPORT_DIR}"'/cluster-mgmt/${NODE_HOSTNAME}"
      '"$RSYNC"' '"${rsync_filter}"' ${NODE_IP}:'"${log_dir}"' "'"${EXPORT_DIR}"'/cluster-mgmt/${NODE_HOSTNAME}/" || true
    '
  }

  # Function to handle nginx logs
  copy_nginx_logs() {
    local log_dir="${CLUSTER_MGMT_LOG_PATH}/nginx"

    # Explicitly try to copy .log and .log.1 files if they exist
    local file_prefixes=("access" "error")

    # Process each file prefix
    for file_prefix in "${file_prefixes[@]}"; do
      local rsync_filter=$(get_rsync_filter "$file_prefix")

      # Copy nginx logs from each management node in parallel
      cat ${ALL_NODES_FILE} | grep -w ${MGMT_NODE_ROLE} | parallel --will-cite -j ${MAX_PSSH_THREADS} '
        NODE_IP=$(echo {} | awk "{print \$1}")
        NODE_HOSTNAME=$(echo {} | awk "{print \$2}")
        mkdir -p "'"${EXPORT_DIR}"'/cluster-mgmt/${NODE_HOSTNAME}"
        '"$RSYNC"' '"${rsync_filter}"' ${NODE_IP}:'"${log_dir}"' "'"${EXPORT_DIR}"'/cluster-mgmt/${NODE_HOSTNAME}/" || true
      '
    done
  }

  # Function to handle registry-logs
  copy_registry_logs() {
    local log_dir="${CLUSTER_MGMT_LOG_PATH}/registry-logs"

    # Explicitly try to copy .log and .log.1 files if they exist
    local file_prefix="registry"
    local rsync_filter=$(get_rsync_filter "$file_prefix")

    # Copy registry logs from each management node in parallel
    cat ${ALL_NODES_FILE} | grep -w ${MGMT_NODE_ROLE} | parallel --will-cite -j ${MAX_PSSH_THREADS} '
      NODE_IP=$(echo {} | awk "{print \$1}")
      NODE_HOSTNAME=$(echo {} | awk "{print \$2}")
      mkdir -p "'"${EXPORT_DIR}"'/cluster-mgmt/${NODE_HOSTNAME}"
      '"$RSYNC"' '"${rsync_filter}"' ${NODE_IP}:'"${log_dir}"' "'"${EXPORT_DIR}"'/cluster-mgmt/${NODE_HOSTNAME}/" || true
    '
  }

  # We use `hostname` to distinguish logs from different management nodes
  # Loop over each component and call its specific handler function
  for component in job-operator cluster-server nginx registry-logs; do
    case "$component" in
      "job-operator")
        copy_job_operator_logs
        ;;
      "cluster-server")
        copy_cluster_server_logs
        ;;
      "nginx")
        copy_nginx_logs
        ;;
      "registry-logs")
        copy_registry_logs
        ;;
      *)
        echo "Unknown component: $component"
        ;;
    esac
  done
}

compress_staging_path() {
  echo "Compressing logs into archive..."
  cd "${EXPORT_LOG_PATH}" || exit
  outfile="${EXPORT_ID}.zip"
  zip "${outfile}" -y -r -q -1 "${EXPORT_ID}"
  if [[ -f "${outfile}" ]]; then
    echo "Log export done: ${EXPORT_LOG_PATH}/${outfile}"
  else
    echo "Log export failed to generate ${EXPORT_LOG_PATH}/${outfile}"
    exit 1
  fi
}

main > >(tee -a "${EXPORT_LOG}") 2> >(tee -a "${EXPORT_LOG}" >&2)