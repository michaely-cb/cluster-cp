#!/usr/bin/env bash

set -eo pipefail

cd "$(dirname "$0")"
source ../pkg-common.sh

function setup_logrotate_lead() {
  # The files under /etc/logrorate.d must be owned by root.
  cp cluster-mgmt /etc/logrotate.d/cluster-mgmt
  # cp above won't fix the permission if the file already has the wrong permission.
  # explicitly chown the file to be owned by root.
  chown root:root /etc/logrotate.d/cluster-mgmt

  # logrotate runs daily by default but some logs grow quite quickly so we
  # run it hourly instead. NOTE: OK to have daily + hourly logrotate, it will
  # not run twice on 24h mark.
  cp /etc/cron.daily/logrotate /etc/cron.hourly || cp /etc/cron.hourly/logrotate /etc/cron.daily
}

function setup_hourly_cleanup_lead() {
  mkdir -p ${CLEANUP_SCRIPTS_DIR}/etcd
  cp misc/clean-up-etcd.sh ${CLEANUP_SCRIPTS_DIR}/etcd/clean-up-etcd.sh
  echo "bash ${CLEANUP_SCRIPTS_DIR}/etcd/clean-up-etcd.sh" >/etc/cron.hourly/etcd-cleanup
  chmod 755 /etc/cron.hourly/etcd-cleanup

  if has_multiple_mgmt_nodes; then
    chmod 755 misc/clean-up-pod-volumes.sh
    cp misc/clean-up-pod-volumes.sh /etc/cron.hourly/clean-up-pod-volumes.sh
  fi

  zip -r logs.zip logs
  cp logs.zip ${CLEANUP_SCRIPTS_DIR}

  mkdir -p ${CLEANUP_SCRIPTS_DIR}/logs
  cp -a logs/* ${CLEANUP_SCRIPTS_DIR}/logs
  echo "bash ${CLEANUP_SCRIPTS_DIR}/logs/main.sh" >/etc/cron.hourly/log-cleanup
  chmod 755 /etc/cron.hourly/log-cleanup

  mkdir -p ${CLEANUP_SCRIPTS_DIR}/registry
  cp registry/main.sh ${CLEANUP_SCRIPTS_DIR}/registry/main.sh
  echo "bash ${CLEANUP_SCRIPTS_DIR}/registry/main.sh" >/etc/cron.hourly/registry-cleanup
  chmod 755 /etc/cron.hourly/registry-cleanup
}

function setup_daily_cleanup_lead() {
  mkdir -p ${CLEANUP_SCRIPTS_DIR}/misc
  cp misc/clean-up-misc.sh ${CLEANUP_SCRIPTS_DIR}/misc/clean-up-misc.sh
  echo "bash ${CLEANUP_SCRIPTS_DIR}/misc/clean-up-misc.sh" >/etc/cron.daily/clean-up-misc
  chmod 755 /etc/cron.daily/clean-up-misc
}

function setup_daily_wsjob_dump_lead() {
  chmod 755 misc/dump-wsjob.sh
  cp misc/dump-wsjob.sh /etc/cron.daily/dump-wsjob.sh
}

function setup_cleanup_scripts_dir_lead() {
  mkdir -p ${CLEANUP_SCRIPTS_DIR}/configs
  cp common.sh ${CLEANUP_SCRIPTS_DIR}/common.sh
  cp check-cleanup-history.sh ${CLEANUP_SCRIPTS_DIR}/check-cleanup-history.sh

  # Cleanup logs for information purposes only:
  # Total size of unique layers for all 11 tags in 'cbcore': 47.14GB
  # Total size of unique layers for all 10 tags in 'custom-worker': 7.73GB
  # Total size of unique layers for all 20 tags in 'cluster-server': 1.05GB
  # Total size of unique layers for all 7 tags in 'job-operator': 500.89MB

  # Rough baseline disk consumption on 64 systems:
  # 64GB (cbcore) + 12GB (custom-worker) + 2GB (cluster-server) + 1GB (job-operator) = 79GB

  # keep at least 15 images regardless of the cluster size, but scale up linearly with the number of systems when
  # cluster size exceeds 64 systems.
  max_cbcore_images=15
  system_count=$(kubectl -n ${SYSTEM_NAMESPACE} get cm cluster -o jsonpath='{.data.clusterConfiguration\.yaml}' | yq '.systems | length')
  scaled_max_cbcore_images=$(echo "scale=2; $max_cbcore_images * $system_count / 64" | bc | awk '{print int($1)}')
  if [ $scaled_max_cbcore_images -gt $max_cbcore_images ]; then
    max_cbcore_images=$scaled_max_cbcore_images
  fi

  keep_n_cbcore=${max_cbcore_images}
  keep_n_custom_worker=${max_cbcore_images}
  keep_n_cluster_server=$((max_cbcore_images * 2))
  keep_n_job_operator=$((max_cbcore_images * 2))

  all_volumes="[]"
  local_workdir_volume='{"category": "workdir", "path": "/n1/wsjob/workdir", "on_nfs": "false", "pvc": ""}'
  all_volumes=$(echo "$all_volumes" | jq --argjson obj "$local_workdir_volume" '. + [$obj]')

  if has_multiple_mgmt_nodes; then
    ceph_compile_volume='{"category": "cached_compile", "path": "/pvc/compile_root", "on_nfs": "false", "pvc": "cached-compile-static-pvc"}'
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$ceph_compile_volume" '. + [$obj]')

    ceph_log_export_volume='{"category": "log_export", "path": "/pvc/log-export", "on_nfs": "false", "pvc": "log-export-static-pvc"}'
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$ceph_log_export_volume" '. + [$obj]')

    ceph_debug_artifact_volume='{"category": "debug_artifact", "path": "/pvc/debug-artifact", "on_nfs": "false", "pvc": "debug-artifact-static-pvc"}'
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$ceph_debug_artifact_volume" '. + [$obj]')
  else
    local_compile_volume='{"category": "cached_compile", "path": "/n1/wsjob/compile_root", "on_nfs": "false", "pvc": ""}'
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$local_compile_volume" '. + [$obj]')

    local_log_export_volume='{"category": "log_export", "path": "/n1/log-export", "on_nfs": "false", "pvc": ""}'
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$local_log_export_volume" '. + [$obj]')

    local_debug_artifact_volume='{"category": "debug_artifact", "path": "/n1/debug-artifact", "on_nfs": "false", "pvc": ""}'
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$local_debug_artifact_volume" '. + [$obj]')
  fi

  local_deployment_package_volume='{"category": "deployment_package", "path": "/tmp/deployment-packages", "on_nfs": "false", "pvc": ""}'
  all_volumes=$(echo "$all_volumes" | jq --argjson obj "$local_deployment_package_volume" '. + [$obj]')

  # Explicitly create netapp volumes for backward compatibility
  # Starting from rel-2.4, cluster volumes are created for cached compile, workdir logs and user venv
  # TODO: Remove this in rel-2.6
  if netapp_is_available; then
    server=$(df -TP /cb/tests/cluster-mgmt | grep nfs | cut -d: -f1 | tr -d '\n')

    netapp_workdir_volume=$(jq -n --arg cp "/cb/tests/cluster-mgmt/${short_cluster_name}/workdir" \
      --arg s "$server" \
      --arg sp "/tests/cluster-mgmt/${short_cluster_name}/workdir" \
      --arg c "workdir" \
      '{category: $c, path: $cp, server_address: $s, server_path: $sp, on_nfs: "true", pvc: ""}')
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$netapp_workdir_volume" '. + [$obj]')

    netapp_compile_volume=$(jq -n --arg cp "/cb/tests/cluster-mgmt/compile_root" \
      --arg s "$server" \
      --arg sp "/tests/cluster-mgmt/compile_root" \
      --arg c "cached_compile" \
      '{category: $c, path: $cp, server_address: $s, server_path: $sp, on_nfs: "true", pvc: ""}')
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$netapp_compile_volume" '. + [$obj]')

    netapp_user_venv_volume=$(jq -n --arg cp "/cb/tests/cluster-mgmt/${short_cluster_name}/user-venv" \
      --arg s "$server" \
      --arg sp "/tests/cluster-mgmt/${short_cluster_name}/user-venv" \
      --arg c "user_venv" \
      '{category: $c, path: $cp, server_address: $s, server_path: $sp, on_nfs: "true", pvc: ""}')
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$netapp_user_venv_volume" '. + [$obj]')
  fi

  # Explicitly create cblocal volumes for backward compatibility
  # TODO: Remove this in rel-2.6
  if cblocal_is_available; then
    server=$(df -TP /cblocal/tests/cluster-mgmt | grep nfs | cut -d: -f1 | tr -d '\n')

    cblocal_workdir_volume=$(jq -n --arg cp "/cblocal/tests/cluster-mgmt/${short_cluster_name}/workdir" \
      --arg s "$server" \
      --arg sp "/tests/cluster-mgmt/${short_cluster_name}/workdir" \
      --arg c "workdir" \
      '{category: $c, path: $cp, server_address: $s, server_path: $sp, on_nfs: "true", pvc: ""}')
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$cblocal_workdir_volume" '. + [$obj]')

    cblocal_compile_volume=$(jq -n --arg cp "/cblocal/tests/cluster-mgmt/compile_root" \
      --arg s "$server" \
      --arg sp "/tests/cluster-mgmt/compile_root" \
      --arg c "cached_compile" \
      '{category: $c, path: $cp, server_address: $s, server_path: $sp, on_nfs: "true", pvc: ""}')
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$cblocal_compile_volume" '. + [$obj]')

    cblocal_user_venv_volume=$(jq -n --arg cp "/cblocal/tests/cluster-mgmt/${short_cluster_name}/user-venv" \
      --arg s "$server" \
      --arg sp "/tests/cluster-mgmt/${short_cluster_name}/user-venv" \
      --arg c "user_venv" \
      '{category: $c, path: $cp, server_address: $s, server_path: $sp, on_nfs: "true", pvc: ""}')
    all_volumes=$(echo "$all_volumes" | jq --argjson obj "$cblocal_user_venv_volume" '. + [$obj]')
  fi

  add_volume_if_not_exists() {
    volumes="$1"
    category="$2"
    obj="$3"
    new_volume=$(jq -n --arg cp $(jq -r '.containerPath' <<< "$obj") \
      --arg s $(jq -r '.server' <<< "$obj") \
      --arg sp $(jq -r '.serverPath' <<< "$obj") \
      --arg c "$category" \
      '{category: $c, path: $cp, server_address: $s, server_path: $sp, "on_nfs": "true", pvc: ""}')
    volume_exists=$(echo "$volumes" | jq --argjson new_volume "$new_volume" 'any(.[]; . == $new_volume)')
    if [ "$volume_exists" = "false" ]; then
      volumes=$(echo "$volumes" | jq --argjson new_volume "$new_volume" '. + [$new_volume]')
    fi
    echo "$volumes"
  }

  internal_nfs_volumes="[]"
  response=$(kubectl -n ${SYSTEM_NAMESPACE} get cm cluster-server-volumes -o jsonpath='{.data}' --ignore-not-found)
  if [ -n "$response" ]; then
    internal_nfs_volumes=$(echo "$response" | jq -cr 'to_entries[] | (.value | fromjson) | select(.type == "nfs" and .labels["cerebras-internal"] == "true")')
  fi

  while read -r obj; do
    all_volumes=$(add_volume_if_not_exists "$all_volumes" "workdir" "$obj")
  done < <(echo "$internal_nfs_volumes" | jq -c 'select(.labels["workdir-logs"] == "true")')

  while read -r obj; do
    all_volumes=$(add_volume_if_not_exists "$all_volumes" "cached_compile" "$obj")
  done < <(echo "$internal_nfs_volumes" | jq -c 'select(.labels["cached-compile"] == "true")')

  while read -r obj; do
    all_volumes=$(add_volume_if_not_exists "$all_volumes" "user_venv" "$obj")
  done < <(echo "$internal_nfs_volumes" | jq -c 'select(.labels["allow-venv"] == "true")')

  debug_volume_capacity_gb=$((10 * system_count))

  jq -n \
    --argjson keep_n_cbcore "$keep_n_cbcore" \
    --argjson keep_n_custom_worker "$keep_n_custom_worker" \
    --argjson keep_n_cluster_server "$keep_n_cluster_server" \
    --argjson keep_n_job_operator "$keep_n_job_operator" \
    --argjson debug_volume_capacity_gb "$debug_volume_capacity_gb" \
    --argjson all_volumes "$all_volumes" \
    '{
      keep_n_cbcore: $keep_n_cbcore,
      keep_n_custom_worker: $keep_n_custom_worker,
      keep_n_cluster_server: $keep_n_cluster_server,
      keep_n_job_operator: $keep_n_job_operator,
      debug_volume_capacity_gb: $debug_volume_capacity_gb,
      volumes: $all_volumes,
    }' >${CLEANUP_SCRIPTS_DIR}/configs/cleanup-configs.json
}

setup_logrotate_lead
setup_cleanup_scripts_dir_lead
setup_hourly_cleanup_lead
setup_daily_cleanup_lead
setup_daily_wsjob_dump_lead

installer_rollout_wait_or_warn installers log-rotation-installer ./log-rotation-installer.sh 180 notready_ok "CLEANUP_SCRIPTS_DIR=$CLEANUP_SCRIPTS_DIR"

# TODO: Need to add topic cleanup
