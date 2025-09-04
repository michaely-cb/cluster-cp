#!/bin/bash

set -eo pipefail

export script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source ${script_dir}/../common.sh

export deploy_pkg_partition="/tmp/deployment-packages"
export wsjob_partition="/n1/wsjob"
export mocks_dir="${script_dir}/mocks"

if [ -z "${in_smoke_test}" ]; then
  export config_file="${cleanup_scripts_dir}/configs/cleanup-configs.json"
else
  export config_file="${mocks_dir}/cleanup-configs.json"
fi

# Overall cleanup strategy:
# - for nfs cleanup, we use the time-based approach to filter all files that were last modified more than 4 days ago.
# - for local and ceph cleanup, we use `find` and `du` and sort them by the last modified time and ignore entries that
#   do not meet the minimum retention hours criteria.
if [ -z "${in_smoke_test}" ]; then
  # Global to all clusters
  export compile_nfs_retention_days=3
  # Local to individual clusters
  # We can manually tweak this knob on clusters that generate workdirs
  # that use significant amount of NFS storage
  export workdir_nfs_retention_days=3
  # Setting to 30 days as a grace period to account for venvs
  # created without the access marker heartbeats. After release 2.0,
  # we should change this to 1 day.
  export user_venv_retention_days=30
  export empty_dir_retention_days=1
  # The minimum retention period to ensure we don't delete something that's being actively written.
  # For example, if a node already has storage running at a very high level and there's nothing else
  # we could delete other than the only job that's generating job logs, we should avoid deleting
  # the job logs of that job.
  export minimum_nonnfs_retention_hours=1
  export local_storage_watermark=65
  export cached_compile_pvc_storage_watermark=75
  export log_export_pvc_storage_watermark=25
  export max_deploy_pkg_retention_days=7
  
  # debug artifact cleanup variables
  export max_dbg_retention_days=30
  export reg_dbg_retention_days=14
  # watermark level (percentage)
  export dbg_watermark=65
  # maximum storage space for debug artifacts in GB
  export max_dbg_storage_space=$(jq -r '.debug_volume_capacity_gb' ${config_file})
else
  export compile_nfs_retention_days=5
  export workdir_nfs_retention_days=5
  export user_venv_retention_days=30
  export empty_dir_retention_days=1
  export minimum_nonnfs_retention_hours=1
  export local_storage_watermark=68
  export cached_compile_pvc_storage_watermark=75
  export log_export_pvc_storage_watermark=25
  export max_deploy_pkg_retention_days=7

  # debug artifact cleanup variables
  export max_dbg_retention_days=30
  export reg_dbg_retention_days=14
  export dbg_watermark=65
  export max_dbg_storage_space=$(jq -r '.debug_volume_capacity_gb' ${config_file})
fi

if [ -n "${dryrun}" ]; then
  # This is where developers could adjust configurations in an adhoc fashion
  # to validate cleanup strategies without having to impact the real cleanup in clusters
  true
fi

function xcmd() {
  (set -x; "$@")
}

function format_outfile() {
  local path=$1
  if [ ! -f ${path} ]; then
    echo "ERROR: '${path}' does not exist."
    return 1
  fi

  local temp_file=$(mktemp)
  cp -f ${path} ${temp_file}
  cat ${temp_file} | column -t > ${path}
  rm -f ${temp_file}
}

function clean_nfs_logs() {
  local category=$1
  local path=$2
  local metadata_prefix=$3
  local cleanup_spec='{"storage_health": {"nfs": {}}, "logs": []}'

  local nfs_files
  if [ -z "${in_smoke_test}" ]; then
    nfs_files="${metadata_dir}/${metadata_prefix}_files.out"
  else
    nfs_files="${mocks_dir}/${metadata_prefix}_files.out"
  fi
  touch ${nfs_files}

  local df_out
  if [ -z "${in_smoke_test}" ]; then
    if [ ! -d "$path" ]; then continue; fi
    df_out="${metadata_dir}/${metadata_prefix}_df.out"
    df -B 1 -P "$path" > ${df_out}
  else
    df_out="${mocks_dir}/nfs_df.out"
  fi
  local root_nfs_mount_path=$(cat ${df_out} | awk '{print $6}' | tail -1)
  local total_capacity_bytes=$(cat ${df_out} | awk '{print $2}' | tail -1)
  local used_bytes=$(cat ${df_out} | awk '{print $3}' | tail -1)
  local available_bytes=$(cat ${df_out} | awk '{print $4}' | tail -1)
  local total_capacity_bytes_human_readable=$(human_readable_bytes ${total_capacity_bytes})
  local used_bytes_human_readable=$(human_readable_bytes ${used_bytes})
  local available_bytes_human_readable=$(human_readable_bytes ${available_bytes})

  if [ "$(jq -r .storage_health.nfs[\"${root_nfs_mount_path}\"] <<< ${cleanup_spec})" == "null" ]; then
    echo "NFS log retention period: compile - ${compile_nfs_retention_days} day(s), workdir - ${workdir_nfs_retention_days} day(s)"
    echo "User venv retention period: ${user_venv_retention_days} day(s)"
    echo "Total capacity on ${root_nfs_mount_path}: ${total_capacity_bytes_human_readable}, current usage: ${used_bytes_human_readable}"
    echo "Remaining disk space: ${available_bytes_human_readable}"

    local usage=$(jq -n --arg p "${root_nfs_mount_path}" \
      --arg t "$total_capacity_bytes ($total_capacity_bytes_human_readable)" \
      --arg u "$used_bytes ($used_bytes_human_readable)" \
      --arg a "$available_bytes ($available_bytes_human_readable)" \
      --arg cr "${compile_nfs_retention_days}" \
      --arg wr "${workdir_nfs_retention_days}" \
      --arg ur "${user_venv_retention_days}" \
      '{($p): {"total_capacity": $t, "disk_space_used": $u, "remaining_disk_space": $a, "compile_retention_in_days": $cr, "workdir_retention_in_days": $wr, "user_venv_retention_in_days": $ur}}')
    cleanup_spec=$(jq --argjson u "$usage" '.storage_health.nfs += $u' <<< "${cleanup_spec}")
  fi

  if [ -n "${in_smoke_test}" ]; then
    # intentioinally left blank since we do not scrape during smoke tests
    true
  elif [ "$category" == "workdir" ]; then
    # Find all workdirs which reside flatly
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all workdirs under ${path} that were last modified more than ${workdir_nfs_retention_days} day(s) ago..."
    # TODO: Refactor the embedded routine into a function and be used for nfs and non-nfs cleanup
    xcmd find ${path} -mindepth 2 -maxdepth 2 -type d \( -name 'wsjob-*' -o -name 'imgjob-*' \) -exec bash -c '
      for matched do
        most_recent_timestamp=$(stat -c "%Y %n" "${matched}" | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")
        access_markers=$(find "${matched}" -mindepth 2 -maxdepth 2 -name ".access-marker.out" | xargs -r stat -c "%Y %n")
        if [ -n "${access_markers}" ]; then
          most_recent_timestamp=$(echo "${access_markers}" | tail -n 1 | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")
        fi

        # `-u` is required for converting timestamps to UTC, otherwise some timestamps could not be parsed due to daylight saving issues
        timestamp_seconds=$(date -u --date="$(echo "$most_recent_timestamp" | sed "s/T/ /")" +%s)
        current_seconds=$(date -u +%s)
        retention_period=$((60 * 60 * 24 * ${workdir_nfs_retention_days}))

        policy="protected"
        if [ $((current_seconds - timestamp_seconds)) -ge $retention_period ]; then
          policy="not-protected"
        fi
        echo "workdir ${matched} ${most_recent_timestamp}Z ${policy}"
      done
    ' bash {} \; >> ${nfs_files} || true

    # Find disk usage from all workdirs
    # Especially useful to understand outliers that consume most disk space
    local nfs_workdir_du="${metadata_dir}/${metadata_prefix}_du.out"
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Going to check disk usage on all workdirs..."
    xcmd find ${path} -mindepth 2 -maxdepth 2 -type d \( -name 'wsjob-*' -o -name 'imgjob-*' \) | xargs -I {} du -sb {} 2>/dev/null | tee ${nfs_workdir_du} || true
    local temp_file=$(mktemp)
    sort -n -k1 ${nfs_workdir_du} > ${temp_file}
    mv -f ${temp_file} ${nfs_workdir_du}
    rm -f ${temp_file}
    format_outfile ${nfs_workdir_du}
    # Copy the latest du output to NFS so we can easily identify the top offenders
    cp -f ${nfs_workdir_du} ${path}/latest_workdir_du.out
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Done scraping ${category} under ${path}"
  elif [ "$category" == "user_venv" ]; then
    if [ -n "${dryrun}" ] || [ ! -f "${path}/.cleanup" ] || [ -n "$(find "${path}/.cleanup" -mmin +60)" ]; then
      if [ -z "${dryrun}" ]; then echo "$node_name" > ${path}/.cleanup; fi
      # Find all user venvs
      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all user venvs under ${path} that were last modified more than ${user_venv_retention_days} day(s) ago..."
      user_venv_paths="${metadata_dir}/nfs_user_venvs.out"
      touch ${user_venv_paths}
      xcmd find ${path} -mindepth 1 -maxdepth 1 -name 'venv-*' -prune -exec bash -c '
        for matched do
          most_recent_timestamp=$(stat -c "%Y %n" "${matched}" | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")
          access_marker="${matched}/.access-marker.out"
          if [ -f "${access_marker}" ]; then
            most_recent_timestamp=$(stat -c "%Y %n" "${access_marker}" | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")
          fi
          # `-u` is required for converting timestamps to UTC, otherwise some timestamps could not be parsed due to daylight saving issues
          timestamp_seconds=$(date -u --date="$(echo "$most_recent_timestamp" | sed "s/T/ /")" +%s)
          current_seconds=$(date -u +%s)
          retention_period=$((60 * 60 * 24 * ${user_venv_retention_days}))

          policy="protected"
          if [ $((current_seconds - timestamp_seconds)) -ge $retention_period ]; then
            policy="not-protected"
          fi
          echo "user_venv ${matched} ${most_recent_timestamp}Z ${policy}"
        done
      ' bash {} \; >> ${nfs_files} || true
      while read -r line; do
        user_venv_path=$(awk '{print $2}' <<< $line)
        if [ -f "$user_venv_path/.access-marker.out" ]; then
          echo "$line" >> ${nfs_files}
        fi
      done < "$user_venv_paths"
      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Done scraping user venvs under ${path}"
    else
      echo "Skipping cleaning up user venvs under ${path} since already done in the past 1 hours"
    fi
  elif [ "$category" == "cached_compile" ]; then
    # Searching cached compile on the NFS takes a long time
    local scrape_file marker_file
    if [ -z "${dryrun}" ]; then
      scrape_file="${path}/.nfs_cleanables.out"
      marker_file="${path}/.nfs_scrape.marker"
    else
      scrape_file="${metadata_dir}/.nfs_cleanables.out"
      marker_file="${metadata_dir}/.nfs_scrape.marker"
    fi

    function scrape_nfs_root() {
      if [ -n "${dryrun}" ] || [ ! -f "${marker_file}" ] || [ -n "$(find "${marker_file}" -mmin +60)" ]; then
        echo "$node_name" > ${marker_file}
        local temp_file="${metadata_dir}/.nfs_cleanables.wip"
        touch ${temp_file}

        # Usually the scraping only takes about 10-20 minutes, but we use
        # a timeout here regardless to ensure scraping does not overlap
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all cached compiles under ${path} that were last modified more than ${compile_nfs_retention_days} day(s) ago..."
        xcmd timeout 45m find ${path} -type d -name 'cs_*' -prune -exec bash -c '
          for matched do
            _cached_compile_dir=$(echo "${matched}" | sed -E "s/(_user.*|_cleanup)?$//")
            # Calculates the most recent timestamp between cs_123/, cs_123_user1000/ and cs_123.lock
            most_recent_timestamp=$(stat -c "%Y %n" ${_cached_compile_dir}* | sort -nr | head -n 1 | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")

            # `-u` is required for converting timestamps to UTC, otherwise some timestamps could not be parsed due to daylight saving issues
            timestamp_seconds=$(date -u --date="$(echo "$most_recent_timestamp" | sed "s/T/ /")" +%s)
            current_seconds=$(date -u +%s)
            retention_period=$((60 * 60 * 24 * ${compile_nfs_retention_days}))

            policy="protected"
            if [ $((current_seconds - timestamp_seconds)) -ge $retention_period ]; then
              policy="not-protected"
            fi
            echo "cached_compile ${_cached_compile_dir}* ${most_recent_timestamp}Z ${policy}"
          done
        ' bash {} \; >> ${temp_file} || true

        # Find all empty directories
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all empty directories under ${path} that were last modified more than ${empty_dir_retention_days} day(s) ago..."
        xcmd timeout 15m find ${path} -mindepth 1 \( -type d -name 'cs_*' -prune \) -o \( -type d -empty \) -exec bash -c '
          for matched do
            most_recent_timestamp=$(stat -c "%Y %n" "${matched}" | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")
            # `-u` is required for converting timestamps to UTC, otherwise some timestamps could not be parsed due to daylight saving issues
            timestamp_seconds=$(date -u --date="$(echo "$most_recent_timestamp" | sed "s/T/ /")" +%s)
            current_seconds=$(date -u +%s)
            retention_period=$((60 * 60 * 24 * ${empty_dir_retention_days}))

            policy="protected"
            if [ $((current_seconds - timestamp_seconds)) -ge $retention_period ]; then
              policy="not-protected"
            fi
            echo "empty_dir ${matched} ${most_recent_timestamp}Z ${policy}"
          done
        ' bash {} \; >> ${temp_file} || true

        sort -k3,3 ${temp_file} | column -t > ${scrape_file}
        rm -f ${temp_file}
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Done scraping cached compiles under ${path}"
      fi
    }

    # For simplicity, we don't implement locking mechanism to prevent different clusters
    # deleting the same batch in a rare race condition. Can optimize later if need be.
    function get_cleanable_batch() {
      local temp_file=$(mktemp)
      cp -f "${scrape_file}" "${temp_file}"
      local batch_size="$1"
      while IFS= read -r line; do
        local location=$(awk '{print $2}' <<< "$line")
        # Check if the path exists
        if timeout 10 ls ${location} >/dev/null 2>&1; then
          echo "Taking a batch of at most ${batch_size} items from ${scrape_file} starting from ${location}"
          # If it exists, add `batch_size` entries
          echo "${line}" >> ${nfs_files}
          for i in $(seq 1 $((batch_size - 1))); do
            read -r line || break
            echo "${line}" >> ${nfs_files}
          done
          break
        else
          # If it doesn't exist, skip `batch_size` entries
          for i in $(seq 1 $((batch_size - 1))); do
            read -r || break
          done
        fi
      done < ${temp_file}
      rm -f ${temp_file}
    }

    # Searching cached compile on the NFS takes a long time, across the clusters we only scrape once an hour.
    scrape_nfs_root
    # We only clean a batch of 100 from the scrape file each time.
    get_cleanable_batch 100
  fi

  format_outfile ${nfs_files}

  if [ ! -s "${nfs_files}" ]; then
    echo "Scrape file $nfs_files is empty. Nothing to clean."
    return 0
  fi

  echo "Start chunking ${nfs_files} prior to further processing"

  # Split the nfs_files into smaller chunks for processing
  nfs_files_basename=$(basename ${nfs_files})
  if [ -n "$in_smoke_test" ]; then
    split -l 10 ${nfs_files} ${metadata_dir}/${nfs_files_basename}_part_
  else
    split -l 200 ${nfs_files} ${metadata_dir}/${nfs_files_basename}_part_
  fi

  # Iterate over the chunks
  for chunk in ${metadata_dir}/${nfs_files_basename}_part_*; do
    local chunk_index=$(basename $chunk | awk -F'_' '{print $NF}') # Extract the index of the chunk
    local spec_part_file="${metadata_dir}/${metadata_prefix}_${chunk_index}_cleanup_spec.json"

    format_outfile ${chunk}
    chunk_cleanup_spec="$cleanup_spec"

    while IFS= read -r line; do
      local category=$(awk '{print $1}' <<< "$line")
      local path=$(awk '{print $2}' <<< "$line")
      local last_modified_timestamp=$(awk '{print $3}' <<< "$line")
      local policy=$(awk '{print $4}' <<< "$line")
      local plan="delete"
      local reason=
      local _status="pending"
      if [ "$category" == "empty_dir" ]; then
        if [ "$policy" == "protected" ]; then
          plan="keep"
          _status="done"
          reason="Empty directory is protected by the retention period ($empty_dir_retention_days days)"
        else
          reason="Empty directory exceeds the retention period ($empty_dir_retention_days days)"
        fi
      elif [ "$category" == "user_venv" ]; then
        if [ "$policy" == "protected" ]; then
          plan="keep"
          _status="done"
          reason="User venv is protected by the retention period ($user_venv_retention_days days)"
        else
          reason="User venv exceeds the retention period ($user_venv_retention_days days)"
        fi
      elif [ "$category" == "cached_compile" ]; then
        if [ "$policy" == "protected" ]; then
          plan="keep"
          _status="done"
          reason="Cached compile is protected by the retention period ($compile_nfs_retention_days days)"
        else
          reason="Cached compile exceeds the retention period ($compile_nfs_retention_days days)"
        fi
      else
        if [ "$policy" == "protected" ]; then
          plan="keep"
          _status="done"
          reason="NFS log is protected by the retention period ($workdir_nfs_retention_days days)"
        else
          reason="NFS logs exceeds the retention period ($workdir_nfs_retention_days days)"
        fi
      fi
      chunk_cleanup_spec=$(jq --arg path "$path" \
        --arg d "$last_modified_timestamp" \
        --arg r "$reason" \
        --arg plan "$plan" \
        --arg _status "$_status" \
        '.["logs"] += [{"path": $path, "last_modified_timestamp": $d, "reason": $r, "plan": $plan, "status": $_status}]' <<< "${chunk_cleanup_spec}")
    done < <(sort -k3,3 -b ${chunk})

    echo "${chunk_cleanup_spec}" > ${spec_part_file}
    num_candidates=$(jq -r '[.logs[] | select(.plan == "delete" and .status == "pending")] | length' ${spec_part_file})
    echo "In total ${num_candidates} candidate(s) to be cleaned up from chunk ${chunk_index}"

    if [ -n "${in_smoke_test}" ]; then
      echo ""
      cat ${spec_part_file}

      expected_file="${mocks_dir}/expected_$(basename ${spec_part_file})"
      if cmp -s ${spec_part_file} ${expected_file}; then
        echo "The log cleanup spec matched the expected values in the smoke test for chunk ${chunk_index}"
      else
        echo "ERROR: The log cleanup spec did not match the expected values in the smoke test for chunk ${chunk_index}"
        diff ${spec_part_file} ${expected_file}
        return 1
      fi
    fi
  done

  for spec_part_file in ${metadata_dir}/${metadata_prefix}_*_cleanup_spec.json; do
    if [[ ! -f "$spec_part_file" ]]; then continue; fi
    chunk_index=$(basename $spec_part_file | awk -F'_' '{print $(NF-2)}' | sed 's/_cleanup_spec.json//')
    status_part_file="${metadata_dir}/${metadata_prefix}_${chunk_index}_cleanup_status.json"
    _cleanup nfs ${spec_part_file} ${status_part_file}
  done
}

function clean_local_logs() {
  local metadata_prefix="$1"

  function calculate_cleanables() {
    local policy_name="$1"
    local watermark_percentage="$2"
    local bytes_total="$3"
    local bytes_used="$4"
    local bytes_kept="$5"
    local scrape_file="$6"

    local bytes_to_delete=0
    local bytes_to_delete_human_readable="0B"
    if ((bytes_kept > bytes_used)); then
      echo "No need to clean logs"
    else
      bytes_to_delete=$((bytes_used - bytes_kept))
      bytes_to_delete_human_readable=$(human_readable_bytes ${bytes_to_delete})
      echo "Need to delete ${bytes_to_delete_human_readable}"
    fi

    bytes_total_human_readable=$(human_readable_bytes ${bytes_total})
    bytes_used_human_readable=$(human_readable_bytes ${bytes_used})
    bytes_kept_human_readable=$(human_readable_bytes ${bytes_kept})

    echo "Storage watermark percentage predefined: ${watermark_percentage}%"
    echo "Total capacity: ${bytes_total_human_readable}, current usage: ${bytes_used_human_readable}"
    echo "Maximum usage allowed: ${bytes_kept_human_readable}"

    # *************************
    # TODO: For debug artifacts, we should either calculate the bytes_to_delete based on how many files are ready for deletion 
    # or display bytes_to_delete as bytes over water level because for debug artifacts files will still be deleted even if 
    # bytes_to_delete is 0 i.e. under the water level. For the time being no change is needed, will need to revisit this once 
    # all the features are complete. 
    # *************************

    # creating initial cleanup_spec.json file 
    local cleanup_spec='{"storage_health": {}, "logs": []}'
    local usage=$(jq -n --arg wm "$watermark_percentage%" \
      --arg t "$bytes_total ($bytes_total_human_readable)" \
      --arg k "$bytes_kept ($bytes_kept_human_readable)" \
      --arg u "$bytes_used ($bytes_used_human_readable)" \
      --arg b "$bytes_to_delete ($bytes_to_delete_human_readable)" \
      '{"watermark_percentage": $wm, "total_capacity": $t, "disk_usage_upper_limit": $k, 
        "disk_space_used": $u, "bytes_to_delete": $b}')
    cleanup_spec=$(jq --argjson u "$usage" --arg pn "${policy_name}" '.storage_health += {($pn): $u}' <<< "${cleanup_spec}")

    if [ ! -s "$scrape_file" ]; then
      echo "Scrape file $scrape_file is empty. Nothing to clean."
      return 0
    fi

    echo "Start chunking ${scrape_file} prior to further processing"

    # Split the scrape_file into smaller chunks for processing
    scrape_file_basename=$(basename ${scrape_file})
    if [ -n "$in_smoke_test" ]; then
      split -l 25 ${scrape_file} ${metadata_dir}/${scrape_file_basename}_part_
    else
      split -l 200 ${scrape_file} ${metadata_dir}/${scrape_file_basename}_part_
    fi

    for chunk in ${metadata_dir}/${scrape_file_basename}_part_*; do
      local chunk_index=$(basename $chunk | awk -F'_' '{print $NF}') # Extract the index of the chunk
      local spec_part_file="${metadata_dir}/${metadata_prefix}_${chunk_index}_cleanup_spec.json"
      local chunk_cleanup_spec="${cleanup_spec}"

      while IFS=" " read -r category path datetime human_readable_size size_in_bytes policy; do
        local reason="LRU'ed logs exceed the watermark (${watermark_percentage}%) defined in the cleanup policy"
        local plan="delete"
        local _status="pending"

        if ((bytes_to_delete <= 0)); then
          reason="MRU'ed logs respect the watermark (${watermark_percentage}%) defined in the cleanup policy"
          plan="keep"
          _status="done"
        elif [ "$policy" == "protected" ]; then
          reason="Artifact is protected by the retention policy"
          plan="keep"
          _status="done"
        else
          ((bytes_to_delete-=size_in_bytes))
        fi

        # Special cases for debug artifacts
        if [[ "$metadata_prefix" == *debug_artifact ]] && (("$category" == "debug_artifact")); then 
          # if the file is 'not-protected' and we are below the water level, we still need to mark the file for deletion
          if [ "$policy" == "not-protected" ] && ((bytes_to_delete <= 0)); then
            reason="Debug artifact is older than ${reg_dbg_retention_days} days"
            plan="delete"
            _status="pending"
          fi 
        fi
        # write result into the spec file
        chunk_cleanup_spec=$(jq --arg path "$path" \
          --arg d "$datetime" \
          --arg hs "$human_readable_size" \
          --arg b "$size_in_bytes" \
          --arg plan "$plan" \
          --arg r "$reason" \
          --arg s "$_status" \
          '.["logs"] += [{"path": $path, "last_used_timestamp": $d, "human_readable_size": $hs, "size_in_bytes": $b, "plan": $plan, "reason": $r, "status": $s}]' <<< "${chunk_cleanup_spec}")
      done < ${chunk}

      echo "${chunk_cleanup_spec}" > ${spec_part_file}

      num_candidates=$(jq -r '[.logs[] | select(.plan == "delete" and .status == "pending")] | length' ${spec_part_file})
      echo "In total ${num_candidates} candidate(s) to be cleaned up from chunk ${chunk_index}"

      if [ -n "${in_smoke_test}" ]; then
        echo ""
        cat ${spec_part_file}

        expected_file="${mocks_dir}/expected_$(basename ${spec_part_file})"
        if cmp -s ${spec_part_file} ${expected_file}; then
          echo "The log cleanup spec matched the expected values in the smoke test for chunk ${chunk_index}"
        else
          echo "ERROR: The log cleanup spec did not match the expected values in the smoke test for chunk ${chunk_index}"
          diff ${spec_part_file} ${expected_file}
          return 1
        fi
      fi
    done
  }

  local du_out
  local local_files
  if [ -n "${in_smoke_test}" ]; then
    du_out="${mocks_dir}/${metadata_prefix}_du.out"
    local_files="${mocks_dir}/${metadata_prefix}_files.out"
  else
    du_out="${metadata_dir}/${metadata_prefix}_du.out"
    local_files="${metadata_dir}/${metadata_prefix}_files.out"
  fi
  local local_files_with_size="${metadata_dir}/${metadata_prefix}_files_with_size.out"
  touch ${du_out} ${local_files} ${local_files_with_size}

  local pvc_name=${PVC_NAME:-}
  while IFS= read -r obj; do
    if [ -n "${in_smoke_test}" ]; then break; fi
    local category=$(jq -r '.category' <<< "$obj")
    local path=$(jq -r '.path' <<< "$obj")
    if [ ! -d "$path" ]; then continue; fi
    if [[ "$metadata_prefix" != *debug_artifact ]] && [[ "$metadata_prefix" != *deployment_package ]]; then
      if [ "$category" == "workdir" ]; then
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all workdirs under ${path}..."
        # the "-L" option is used because $path can be a directory under a symlink, so "find" needs to follow the symlink in those cases
        xcmd find -L ${path} -mindepth 2 -maxdepth 2 -type d \( -name 'wsjob-*' -o -name 'imgjob-*' \) -exec bash -c '
          for matched do
            most_recent_timestamp=$(stat -c "%Y %n" "${matched}" | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")
            access_markers=$(find -L "${matched}" -mindepth 2 -maxdepth 2 -name ".access-marker.out" | xargs -r stat -c "%Y %n")
            if [ -n "${access_markers}" ]; then
              most_recent_timestamp=$(echo "${access_markers}" | tail -n 1 | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")
            fi
            echo "workdir ${matched} ${most_recent_timestamp}Z"
          done
        ' bash {} \; >> ${local_files} || true
      elif [ "$category" == "log_export" ]; then
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all log exports under ${path}..."
        # `-type d` is left out intentionally. We need to look for both directories and files for log export artifacts cleanup.
        xcmd find -L ${path} -mindepth 2 -maxdepth 2 -exec bash -c 'echo "log_export {} $(date -u -r "{}" "+%Y-%m-%dT%H:%M:%SZ")"' \; >> ${local_files} || true
      elif [ "$category" == "cached_compile" ]; then
        # Find all cached compile dir and lock files
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all cached compiles under ${path}..."
        xcmd find -L ${path} -mindepth 1 -type d -name 'cs_*' -prune -exec bash -c '
          for matched do
            _cached_compile_dir=$(echo "${matched}" | sed -E "s/(_user.*|_cleanup)?$//")
            # Calculates the most recent timestamp between cs_123/, cs_123_user1000/ and cs_123.lock
            most_recent_timestamp=$(stat -c "%Y %n" ${_cached_compile_dir}* | sort -nr | head -n 1 | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")
            echo "cached_compile ${_cached_compile_dir}* ${most_recent_timestamp}Z"
          done
        ' bash {} \; >> ${local_files} || true

        # Find all empty directories
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all empty directories under ${path} that were last modified more than ${empty_dir_retention_days} day(s) ago..."
        xcmd timeout 15m find -L ${path} -mindepth 1 \( -type d -name 'cs_*' -prune \) -o \( -type d -empty -mtime +"${empty_dir_retention_days}" -exec bash -c 'echo "empty_dir {} $(date -u -r "{}" "+%Y-%m-%dT%H:%M:%SZ")"' \; \) >> ${local_files} || true
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Done scraping ${category} under ${path}"
      fi

      # deleting empty directory for other categories
      if [ "$category" == "workdir" ] || [ "$category" == "log_export" ]; then
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all empty directories under ${path} that were last modified more than ${empty_dir_retention_days} day(s) ago..."
        xcmd timeout 15m find -L ${path} -mindepth 1 -maxdepth 1 -type d -empty -mtime +"${empty_dir_retention_days}" -exec bash -c 'echo "empty_dir {} $(date -u -r "{}" "+%Y-%m-%dT%H:%M:%SZ")"' \; >> ${local_files} || true
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Done scraping ${category} under ${path}"
      fi
    elif [[ "$metadata_prefix" == *debug_artifact ]] && [ "$category" == "debug_artifact" ]; then
      # find all directories and log files under the debug artifacts space that begin with 'wsjob-'
      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all debug artifacts under ${path}..."
      xcmd find -L ${path} -mindepth 2 -maxdepth 2 -name 'wsjob-*' -exec bash -c '
        for matched do
          most_recent_timestamp=$(stat -c "%Y %n" "${matched}" | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")
          echo "debug_artifact ${matched} ${most_recent_timestamp}Z"
        done
      ' bash {} \; >> ${local_files} || true
    elif [[ "$metadata_prefix" == *deployment_package ]] && [ "$category" == "deployment_package" ]; then
      # find all directories under the deployment package space
      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finding all deployment packages under ${path}..."
      xcmd find -L ${path} -mindepth 1 -maxdepth 1 -type d -exec bash -c '
        for matched do
          most_recent_timestamp=$(stat -c "%Y %n" "${matched}" | cut -d" " -f 2- | xargs -I {} date -u -r {} "+%Y-%m-%dT%H:%M:%S")
          echo "deployment_package ${matched} ${most_recent_timestamp}Z"
        done
      ' bash {} \; >> ${local_files} || true
    fi
  done < <(jq -c ".volumes[] | select(.on_nfs == \"false\" and .pvc == \"${pvc_name}\")" ${config_file})

  format_outfile ${local_files}

  # Prioritize log export entries before workdirs and cached compiles on the mgmt nodes.
  # As of 2.0, log exports, workdirs and cached compiles all reside on the NVMe.
  # Log export artifacts should only be short-lived. Once successfully exported, we don't need the artifacts any more.
  # If we didn't prioritize cleaning log export, we would clean up the workdirs and cached compiles that were generated prior to the exports.
  # In general, it's more valuable to keep workdirs and cached compiles as they are much less reproducible than log exports.
  temp_file=$(mktemp)
  awk '$1 == "log_export"' ${local_files} | sort -k3,3 -b >> ${temp_file}
  awk '$1 != "log_export"' ${local_files} | sort -k3,3 -b >> ${temp_file}
  cat ${temp_file} | uniq > ${local_files}
  rm -f ${temp_file}
  format_outfile ${local_files}

  # need to keep track of how much space the debug artifacts directory has used up since there is no way for us to query this later 
  local dbg_space_used=0;

  while IFS= read -r line; do
    category=$(awk '{print $1}' <<< "$line")
    path=$(awk '{print $2}' <<< "$line")
    last_modified_timestamp=$(awk '{print $3}' <<< "$line")
    policy="not-protected"

    if [ -n "${in_smoke_test}" ]; then
      if [[ "$(uname -m)" == "arm64" ]]; then
        # Convert the date string to epoch on MacOS
        timestamp_seconds=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "${last_modified_timestamp}" "+%s")
        current_seconds=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "2023-09-13T02:00:00Z" "+%s")
      else
        # Convert the date string to epoch on Linux
        timestamp_seconds=$(date -u --date="$(echo "$last_modified_timestamp" | sed "s/T/ /" | sed "s/Z//")" +%s)
        current_seconds=$(date -u --date="2023-09-13 02:00:00" +%s)
      fi
    else
      timestamp_seconds=$(date -u --date="$(echo "$last_modified_timestamp" | sed "s/T/ /" | sed "s/Z//")" +%s)
      current_seconds=$(date -u +%s)
    fi
    time_difference=$((current_seconds - timestamp_seconds))

    # this is where the deletion policy is determined as well as where the whitelisting policy will be handled 
    if [ "$category" == "debug_artifact" ]; then
      if [ $time_difference -le $((reg_dbg_retention_days * 3600 * 24)) ]; then
        echo "${category} '${path}' is protected since it is within the minimum retention period of ${reg_dbg_retention_days} days for debug artifacts"
        policy="protected"
      fi
    elif [ "$category" == "deployment_package" ]; then
      if [ $time_difference -le $((max_deploy_pkg_retention_days * 3600 * 24)) ]; then
        echo "${category} '${path}' is protected since it is within the minimum retention period of ${max_deploy_pkg_retention_days} days for deployment packages"
        policy="protected"
      fi
    elif [ "$category" == "cached_compile" ]; then
      if [ -n "${in_smoke_test}" ]; then
        whitelist_file="${mocks_dir}/cached_compile_whitelist.out"
      else
        whitelist_file="${metadata_dir}/cached_compile_whitelist.out"
      fi
      if [ -f "$whitelist_file" ] && grep -q "${path}" "${whitelist_file}"; then
        echo "${category} '${path}' is protected since it is in the whitelist file"
        policy="protected"
      fi
    elif [ $time_difference -le $((minimum_nonnfs_retention_hours * 3600)) ]; then
      echo "${category} '${path}' is protected since it is within the minimum retention period"
      policy="protected"
    fi

    local size_in_bytes
    if [ -n "${in_smoke_test}" ]; then
      size_in_bytes=$(grep -w ${path} ${du_out} | tail -1 | awk '{print $1}')
    else
      # "ls" could hang in Ceph due to bad health, adding a timeout to prevent the script from hanging:
      #
      # /pvc/compile_root # timeout 10 ls /pvc/compile_root/cg4-ml-ns-2/cached_compile/cs_16575322137013037375_user1414
      # fabric.json
      #
      # /pvc/compile_root # timeout 5 ls /pvc/compile_root/cg4-ml-ns-2/cached_compile/cs_17617951380819034171_user2045
      # Terminated
      if ! timeout 10 ls ${path} 1>/dev/null 2>&1; then continue; fi
      size_in_bytes=$(du -sb ${path} -c | tail -1 | awk '{print $1}')
      echo "${size_in_bytes} ${path}" >> ${du_out}
    fi

    # track the size of the debug artfiact directory
    if [ "$category" == "debug_artifact" ]; then
      dbg_space_used=$((dbg_space_used + size_in_bytes))
    fi
    human_readable_dir_size=$(human_readable_bytes "${size_in_bytes}")
    echo "${category} ${path} ${last_modified_timestamp} ${human_readable_dir_size} ${size_in_bytes} ${policy}" >> ${local_files_with_size}
  done < ${local_files}

  format_outfile ${du_out}
  format_outfile ${local_files_with_size}

  local policy_name
  local watermark
  if [[ "$metadata_prefix" == *debug_artifact ]]; then 
    policy_name="${metadata_prefix}"
    watermark="${dbg_watermark}"
  elif [ -z "${pvc_name}" ]; then
    policy_name=local
    watermark="${local_storage_watermark}"
  elif [[ "${pvc_name}" == "cached-compile"* ]]; then
    policy_name="${pvc_name}"
    watermark="${cached_compile_pvc_storage_watermark}"
  else
    policy_name="${pvc_name}"
    watermark="${log_export_pvc_storage_watermark}"
  fi

  local df_out
  local total_capacity_bytes
  local used_bytes

  # Want to look for debug artifacts first so that it also works for smoke test 
  if [ "$metadata_prefix" == "debug_artifact" ]; then
    total_capacity_bytes=$(echo "$max_dbg_storage_space * 1024 * 1024 * 1024" | bc)
    # convert back to integer
    total_capacity_bytes=$(echo "$total_capacity_bytes / 1" | bc)
    used_bytes=$dbg_space_used
  elif [ "$metadata_prefix" == "deployment_package" ] && [ -e "$(realpath ${deploy_pkg_partition})" ]; then
    df_out="${metadata_dir}/deployment_package_df.out"
    df -B 1 -P $(realpath ${deploy_pkg_partition}) > ${df_out}
    total_capacity_bytes=$(cat ${df_out} | awk '{print $2}' | tail -1)
    used_bytes=$(cat ${df_out} | awk '{print $3}' | tail -1)
  elif [ -n "${in_smoke_test}" ]; then
    df_out="${mocks_dir}/local_df.out"
    total_capacity_bytes=$(cat ${df_out} | awk '{print $2}' | tail -1)
    used_bytes=$(cat ${df_out} | awk '{print $3}' | tail -1)
  elif [ -e "${PVC_MOUNT_PATH}" ]; then
    df_out="${metadata_dir}/${metadata_prefix}_df.out"
    df -B 1 -P ${PVC_MOUNT_PATH} > ${df_out}
    total_capacity_bytes=$(cat ${df_out} | awk '{print $2}' | tail -1)
    used_bytes=$(cat ${df_out} | awk '{print $3}' | tail -1)
  elif [ -e "$(realpath ${wsjob_partition})" ]; then
    df_out="${metadata_dir}/local_df.out"
    df -B 1 -P $(realpath ${wsjob_partition}) > ${df_out}
    total_capacity_bytes=$(cat ${df_out} | awk '{print $2}' | tail -1)
    used_bytes=$(cat ${df_out} | awk '{print $3}' | tail -1)
  fi
  local bytes_to_keep=$((total_capacity_bytes * watermark / 100))
  #convert back to integer
  bytes_to_keep=$(echo "$bytes_to_keep / 1" | bc)

  calculate_cleanables ${policy_name} ${watermark} ${total_capacity_bytes} ${used_bytes} ${bytes_to_keep} ${local_files_with_size}

  for spec_part_file in ${metadata_dir}/${metadata_prefix}_*_cleanup_spec.json; do
    if [[ ! -f "$spec_part_file" ]]; then continue; fi
    chunk_index=$(basename $spec_part_file | awk -F'_' '{print $(NF-2)}' | sed 's/_cleanup_spec.json//')
    status_part_file="${metadata_dir}/${metadata_prefix}_${chunk_index}_cleanup_status.json"
    _cleanup ${policy_name} ${spec_part_file} ${status_part_file}
  done
}

# TODO: Add summary to how much space was reclaimed
function _cleanup() {
  # Until this point, we had not modified or deleted any logs from local, nfs or ceph.
  local policy="$1"
  local spec_file="$2"
  local status_file="$3"

  cp -f ${spec_file} ${status_file}
  cleanup_status=$(jq -r '.' ${spec_file})
  while IFS= read -r obj; do
    local path=$(jq -r '.path' <<< "$obj")

    if [ "$(jq -r '.status' <<< "$obj")" == "done" ]; then
      continue
    fi

    local _status="skipped"
    if [ -z "${dryrun}" ] && [ -z "${in_smoke_test}" ]; then
      set +e
      # $path can be a pattern like "/cs_123*" and should be expanded before the cleanup
      local resolved_paths=$(eval "find $path -maxdepth 0" 2>/dev/null)
      echo "$resolved_paths" | while read -r resolved_path; do
        rm -rf "$resolved_path" 2>/dev/null
        if [ -e "$resolved_path" ]; then
          # Sometimes the files that owned by non-root users are a few levels in depth.
          # But we know the leaf files/directories are always owned by non-root users.
          # Traverse to the last leaf file/directory and get the user ownership.
          user=$(find ${resolved_path} 2>/dev/null | tail -1 | xargs stat -c "%u:%U")
          if [ -z "$user" ]; then
            echo "failed to find owner information for $resolved_path"
          else
            echo "owner of $resolved_path: $user"
            user_id=$(echo "$user" | cut -d':' -f1)
            user_name=$(echo "$user" | cut -d':' -f2)
            # If policy is not local, it's possible that the user info of the log owner does not exist in the pod.
            # We create a temporary user inside the pod in order to facilitate cleanup as the log owner.
            if [[ "$policy" != "local" || "$user_name" == "UNKNOWN" ]]; then
              temp_user="tempuser_$user_id"
              echo "'$user_id' is not a known user, going to create '$temp_user' as a temporary user for the cleanup job"
              useradd -u $user_id $temp_user
              user_name=$temp_user
            fi
            echo "going to delete ${resolved_path} as '$user_name'"
            if [ -d "$resolved_path" ] && [[ $(basename "$resolved_path") == cs_* ]]; then
              # special handling of the cached compile to mitigate racy read-writes
              if [[ $resolved_path != *_cleanup ]]; then
                su $user_name bash -c "mv -f ${resolved_path} ${resolved_path}_cleanup 2>/dev/null"
              fi
              su $user_name bash -c "rm -rf ${resolved_path}_cleanup 2>/dev/null"
            else
              su $user_name bash -c "rm -rf ${resolved_path} 2>/dev/null"
            fi
          fi
        fi
      done

      resolved_paths=$(eval "find $path -maxdepth 0" 2>/dev/null)
      set -e
      if [ -n "${resolved_paths}" ]; then
        _status="failed"
        eval "echo Failed to clean up $path"
      else
        _status="done"
        eval "echo Successfully cleaned up $path"
      fi
    fi

    local logs=$(jq -r ".logs | map(if .path == \"${path}\" then .status = \"${_status}\" else . end)" <<< "${cleanup_status}")
    cleanup_status=$(jq -r ".logs = $logs" <<< "${cleanup_status}")
    echo "${cleanup_status}" > ${status_file}
  done < <(jq -c '.logs[] | select(.plan == "delete" and .status == "pending")' ${spec_file})
}
