#!/usr/bin/env bash

# this is necessary since crontab won't have the env var
export PATH="${PATH}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"

set -eo pipefail

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

# During registry cleanup, we first list images that are LRU'ed.
ecr_domain="171496337684.dkr.ecr.us-west-2.amazonaws.com"
registry_domain="registry.local"
registry_url="https://127.0.0.1:5000"
manifest_header="Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"

function get_registry_disk_usage() {
  # We retrieve registry disk usage before and after the cleanup.
  # After cleanup, we restart the registry and wait for the new pods to come up before attempting to retrieve disk usage.
  # The "--sort-by" option is to avoid choosing an old pod that was in a terminating state.
  local registry_pod=$(kubectl -n kube-system get pod -lapp=private-registry --sort-by=.metadata.creationTimestamp -oname | tail -1)
  if [ "$(has_multiple_mgmt_nodes)" == "true" ]; then
    kubectl -n kube-system exec -t ${registry_pod} -- df -h /var/lib/registry
  else
    kubectl -n kube-system exec -t ${registry_pod} -- du -sh /var/lib/registry
  fi

  function get_image_disk_usage() {
    local image=$1
    if [ -n "${in_smoke_test}" ]; then
      return 0
    fi

    local tags=$(eval curl -ks "${registry_url}/v2/${image}/tags/list" | jq -r '.tags // [] | .[]')
    if [ -z "${tags}" ]; then
      echo "No tags found for ${image}"
    else
      declare -A unique_layers
      local total_bytes=0
      local num_images=0
      for tag in $tags; do
        num_images=$((num_images + 1))

        # Extract layers and their sizes from the manifest
        local layers=$(curl -ks -H "$manifest_header" "${registry_url}/v2/${image}/manifests/${tag}" | jq -c '.layers // [] | .[] | {digest: .digest, size: .size}')

        for layer in $layers; do
          local digest=$(echo "$layer" | jq -r '.digest')
          local size=$(echo "$layer" | jq -r '.size')

          # Check if the layer is already accounted for
          if [[ -z "${unique_layers[$digest]}" ]]; then
            unique_layers[$digest]=$size
            total_bytes=$((total_bytes + size))
          fi
        done
      done

      # Display the total bytes of all unique layers
      echo "Total size of unique layers for all ${num_images} tags in '${image}': $(human_readable_bytes ${total_bytes})"
    fi
  }

  get_image_disk_usage cbcore
  get_image_disk_usage custom-worker
  get_image_disk_usage cluster-server
  get_image_disk_usage job-operator
}

function scrape_cluster() {
  function format_output() {
    sort -k3,3 -r -k4,4 | awk -F ' ' '!seen[$3]++' | sort -r -k 4 | column -t
  }

  if [ -n "${in_smoke_test}" ]; then
    echo "in smoke test, going to use mock data instead of actual scraping"
    return 0
  fi

  # todo: optimize by default with csctl and fallback to kubectl
  # Get all wsjobs with their completionTime or creationTimestamp and sort the cbcore images by time
  # SDK jobs might have only worker pods, so we check either Coordinator or Worker container for
  # cbcore image.
  go_template="{{range .items}}{{.metadata.namespace}} \
{{.metadata.name}} {{if .spec.wsReplicaSpecs.Coordinator.template.spec.containers}}\
{{(index .spec.wsReplicaSpecs.Coordinator.template.spec.containers 0).image}}\
{{else if .spec.wsReplicaSpecs.Worker.template.spec.containers}}\
{{(index .spec.wsReplicaSpecs.Worker.template.spec.containers 0).image}}{{else}}{{\"\"}}{{end}} \
{{if .status.completionTime}}{{.status.completionTime}}{{else}}{{.metadata.creationTimestamp}}\
{{end}}{{\"\\n\"}}{{end}}"
  if ! kubectl get wsjobs -A \
    -o go-template --template="$go_template" | \
    grep "${registry_domain}\|${ecr_domain}" | format_output > ${cbcore_last_used_outfile}; then
    echo "unable to yield an updated ${cbcore_last_used_outfile}"
  fi

  # Get all execute wsjobs with their creationTimestamp and sort the custom-worker images by time
  # Prior to 2.2, worker pods only have one container. Starting from 2.2, worker pods have both worker and streamer containers, where streamer is the second container.
  # Using index "-1" below to be backward compatible.
  if ! kubectl get wsjobs -lk8s.cerebras.com/wsjob-job-type=execute -A \
    -ojsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name} {.spec.wsReplicaSpecs.Worker.template.spec.containers[-1].image} {.metadata.creationTimestamp}{"\n"}{end}' | \
    grep "${registry_domain}" | format_output > ${custom_worker_last_used_outfile}; then
    echo "unable to yield an updated ${custom_worker_last_used_outfile}"
  fi

  # Get all rs with their creationTimestamp and sort them by them by time
  if ! kubectl get rs -A \
    -ojsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name} {.spec.template.spec.containers[0].image} {.metadata.creationTimestamp}{"\n"}{end}' | \
    grep "${registry_domain}\|${ecr_domain}" | format_output > ${rs_last_used_outfile}; then
    echo "unable to yield an updated ${rs_last_used_outfile}"
  fi

  # Get all image build jobs with their startTime and sort them by time
  if ! kubectl get job -A -lapp=image-builder \
    -ojsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name} {.metadata.annotations.image-builder/destination-image} {.status.startTime} {.status.conditions[-1].type}{"\n"}{end}' | \
    sort -r -k 4 | column -t > ${image_build_jobs_outfile}; then
    echo "unable to yield an updated ${image_build_jobs_outfile}"
  fi
}

function scrape_registry_agnostic_references() {
  infile="$1"
  outfile="$2"
  declare -A images

  while IFS= read -r line; do
    prefix=$(echo "$line" | awk '{print $1 "  " $2}')
    image_reference=$(echo "$line" | awk '{print $3}' | sed 's/^[^/]*\///')
    timestamp=$(echo "$line" | awk '{for(i=4; i<=NF; i++) printf $i " "; print ""}' | sed 's/^"\(.*\)"\s*$/\1/')

    if [[ -z ${images[$image_reference]} ]] || [[ $timestamp > ${images[$image_reference]} ]]; then
      images[$image_reference]="$prefix  $image_reference  $timestamp"
    fi
  done < "$infile"

  temp_file=$(mktemp)
  for key in "${!images[@]}"; do
    echo "${images[$key]}" >> "$temp_file"
  done

  cat ${temp_file} | format_output > ${outfile}
  rm -f ${temp_file}
}

function scrape_cleanables() {
  image=$1
  outfile=$2
  image_count_limit=$3
  whitelist_file=$4

  should_keep=${image_count_limit}
  tags_to_keep=()
  cleanup_spec=$(jq --arg i "$image" '.["images"][$i] += {}' <<< "${cleanup_spec}")
  echo "$cleanup_spec" > ${metadata_dir}/registry_cleanup_spec.json

  function _update_cleanup_spec() {
    plan=$1
    tag=$2
    reason="${@:3}"
    if [ "$plan" == "keep" ]; then
      _status="done"
    else
      _status="pending"
    fi
    cleanup_spec=$(jq --arg i "$image" \
      --arg t "$tag" \
      --arg p "$plan" \
      --arg r "$reason" \
      --arg s "$_status" \
      '.["images"][$i] += {($t): {"plan": $p, "reason": $r, "status": $s}}' <<< "${cleanup_spec}")
    echo "$cleanup_spec" > ${metadata_dir}/registry_cleanup_spec.json
  }

  function _should_keep() {
    tag=$1
    reason="${@:2}"
    _update_cleanup_spec keep "$tag" "$reason"

    should_keep=$((should_keep - 1))
    tags_to_keep+=("$tag")
  }

  function _should_delete() {
    tag=$1
    reason="${@:2}"
    _update_cleanup_spec delete "$tag" "$reason"

    # Update the cleanup spec to include the digest in order to execute later
    if [ -n "$in_smoke_test" ]; then
      digest=""
    else
      digest_blob=$(curl --retry 3 --retry-delay 3 -sk -I -H "$manifest_header" "${registry_url}/v2/${image}/manifests/${tag}")
      if echo "${digest_blob}" | grep -q docker-content-digest; then
        digest=$(echo "${digest_blob}" | grep docker-content-digest | awk '{print ($2)}' | tr -d '\r')
      else
        digest=""
      fi
    fi
    cleanup_spec=$(echo "$cleanup_spec" | jq --arg i "$image" --arg t "$tag" --arg d "$digest" '.["images"][$i][$t] += {"docker_content_digest": $d}')

    # When scraping cleanables for custom worker images, we will also scrape the corresponding image build jobs for cleanup.
    if [ "${image}" == "custom-worker" ]; then
      cleanup_spec=$(echo "$cleanup_spec" | jq --arg i "$image" --arg t "$tag" '.["images"][$i][$t] += {"image_build_jobs": []}')
      imgjob_id="imgjob-$(echo -n ${tag} | md5sum | cut -d ' ' -f 1)"
      if grep -q ${imgjob_id} ${image_build_jobs_outfile}; then
        namespaces=$(grep ${imgjob_id} ${image_build_jobs_outfile} | awk '{print $1}')
        for ns in ${namespaces[@]}; do
          cleanup_spec=$(jq --arg i "$image" \
            --arg t "$tag" \
            --arg p "image_build_jobs" \
            --arg ns "$ns" \
            --arg id "$imgjob_id" \
            '.["images"][$i][$t][$p] += [{"namespace": $ns, "job_id": $id, "status": "pending"}]' <<< "${cleanup_spec}")
        done
      fi
    fi
    echo "$cleanup_spec" > ${metadata_dir}/registry_cleanup_spec.json
  }

  echo ""

  # A better grep, in case "image" has the same value as the namespace
  if grep -q "${image}:[^[:space:]]" ${outfile}; then
    last_used_tags=( $(grep "${image}:[^[:space:]]" ${outfile} | awk '{print $3}' | cut -d':' -f2) )
  else
    last_used_tags=()
  fi

  if [ -n "${in_smoke_test}" ]; then
    echo "in smoke test, going to use the mock registry_tags.json"
    registry_tags=$(jq -r ".\"$image\" | @tsv" ${mocks_dir}/registry_tags.json)
  else
    registry_tags_json=$(curl --retry 3 --retry-delay 3 -ks "${registry_url}/v2/${image}/tags/list" | jq -r '(.tags // [])')
    echo "$registry_tags_json" > "${metadata_dir}/${image}-registry-tags.json"
    registry_tags=$(jq -r '. | @tsv' <<< "$registry_tags_json")
  fi

  delete_candidates=()
  for tag in ${registry_tags}; do
    delete_candidates+=("$tag")
  done
  echo "delete candidate(s) for ${image}: ${delete_candidates[@]}"

  function unset_from_delete_candidates() {
    tag=$1
    for i in "${!delete_candidates[@]}"; do
      if [[ "${delete_candidates[$i]}" == "${tag}" ]]; then
        unset 'delete_candidates[$i]'
      fi
    done
  }

  last_used_tags_non_whitelisted=()
  # whitelist tags
  for tag in ${last_used_tags[@]}; do
    if [[ " ${delete_candidates[*]} " == *" $tag "* ]]; then
      if [ -n "$whitelist_file" ] && grep -q "$tag" "$whitelist_file"; then
        reason="the image is protected by the whitelist file"
        _should_keep "$tag" "$reason"
        unset_from_delete_candidates "$tag"
      else
        last_used_tags_non_whitelisted+=("$tag")
      fi
    fi
  done

  # List the images that by their last used time
  for tag in ${last_used_tags_non_whitelisted[@]}; do
    if [[ " ${delete_candidates[*]} " == *" $tag "* ]]; then
      # A better grep with a trailing whitespace to eliminate similar tags
      last_used_at=$(grep "${image}:${tag} " ${outfile} | awk '{print $NF}')
      if [ ${should_keep} -gt 0 ]; then
        reason="the image was last used at ${last_used_at} (MRU'ed)"
        _should_keep "$tag" "$reason"
      else
        reason="the image was last used at ${last_used_at} (LRU'ed)"
        _should_delete "$tag" "$reason"
      fi
      unset_from_delete_candidates "$tag"
    fi
  done

  # There can be images in registry but not used in any wsjob records or replicasets
  for tag in ${delete_candidates[@]}; do
    if [ ${should_keep} -gt 0 ]; then
      reason="the number of '${image}' images (${#tags_to_keep[@]}) has not reached the limit ($image_count_limit)"
      _should_keep "$tag" "$reason"
    elif [ -n "$whitelist_file" ] && grep -q "$tag" "$whitelist_file"; then
      reason="the image is protected by the whitelist file"
      _should_keep "$tag" "$reason"
    else
      reason="the number of '${image}' images has reached the limit ($image_count_limit)"
      _should_delete "$tag" "$reason"
    fi
  done

  # Add failed image build jobs to the cleanup spec if they had started more than 24 hours ago
  if [ "${image}" == "custom-worker" ]; then
    time_threshold=$((24 * 60 * 60))
    current_time=$(date +%s)

    while IFS= read -r line; do
      ns=$(echo "${line}" | awk '{print $1}')
      imgjob_id=$(echo "${line}" | awk '{print $2}')
      job_creation_time=$(echo "${line}" | awk '{print $4}')
      if [[ "$(uname -m)" == "arm64" ]]; then
        # Convert the date string to epoch on MacOS
        epoch=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "${job_creation_time}" "+%s")
      else
        # Convert the date string to epoch on Linux
        epoch=$(date -u -d "${job_creation_time}" "+%s")
      fi
      time_diff=$(( current_time - epoch ))
      if [ ${time_diff} -le ${time_threshold} ]; then
        plan="keep"
        reason="The failed job has not reached the retention deadline (24 hours)"
        _status="done"
      else
        plan="delete"
        reason="The failed job has reached the retention deadline (24 hours)"
        _status="pending"
      fi

      cleanup_spec=$(jq --arg ns "$ns" \
        --arg id "$imgjob_id" \
        --arg p "$plan" \
        --arg r "$reason" \
        --arg s "$_status" \
        '.["failed_image_build_jobs"] += [{"namespace": $ns, "job_id": $id, "plan": $p, "reason": $r, "status": $s}]' <<< "${cleanup_spec}")
        echo "$cleanup_spec" > ${metadata_dir}/registry_cleanup_spec.json
    done < <(awk '$5 == "Failed"' "${image_build_jobs_outfile}")
  fi

  echo "expected remaining '$image' image tag(s) after successful cleanup: ${tags_to_keep[@]}"
}

function validate_cleanup_spec() {
  if [ -n "$in_smoke_test" ]; then
    echo ""
    for k in $(jq '.images | keys[]' ${metadata_dir}/registry_cleanup_spec.json); do
      tags_in_spec=$(jq -cr ".[\"images\"][$k] | keys | sort" ${metadata_dir}/registry_cleanup_spec.json)
      tags_in_registry=$(jq -cr ".[$k] | sort" ${mocks_dir}/registry_tags.json)
      result=$(jq -n --argjson j1 "${tags_in_spec}" --argjson j2 "${tags_in_registry}" '$j1 == $j2')
      if [ "$result" != "true" ]; then
        echo "ERROR: The $k image cleanup spec did not match the original tag list in the smoke test!"
        echo "Tags in spec: ${tags_in_spec}"
        echo "Tags in the original registry tag list: ${tags_in_registry}"
        return 1
      else
        echo "The $k image cleanup spec matched the original tag list in the smoke test."
      fi
    done

    echo ""
    cat ${metadata_dir}/registry_cleanup_spec.json
    result=$(jq -n \
      --argjson j1 "$(jq -c '.' ${mocks_dir}/expected_registry_cleanup_spec.json)" \
      --argjson j2 "$(jq -c '.' ${metadata_dir}/registry_cleanup_spec.json)" \
      '$j1 == $j2')
    if [ "$result" != "true" ]; then
      echo "ERROR: The registry cleanup spec did not match the expected value!"
      diff ${metadata_dir}/registry_cleanup_spec.json ${mocks_dir}/expected_registry_cleanup_spec.json
      return 1
    else
      echo "The registry cleanup spec matched the expected value in the smoke test."
    fi
  fi
}

function delete_failed_image_jobs() {
  job_counter=0
  while IFS= read -r j; do
    ns=$(jq -r '.namespace' <<< "$j")
    id=$(jq -r '.job_id' <<< "$j")
    if [ -z "$dryrun" ] && [ -z "$in_smoke_test" ]; then
      kubectl -n ${ns} delete job ${id} --ignore-not-found
      j=$(jq '.status = "done"' <<< "$j")
    else
      j=$(jq '.status = "skipped"' <<< "$j")
    fi

    cleanup_status=$(jq --argjson j "$j" --argjson c "$job_counter" '.["failed_image_build_jobs"][$c] |= $j' <<< "$cleanup_status")
    echo "$cleanup_status" > ${metadata_dir}/registry_cleanup_status.json
    job_counter=$(( job_counter+1 ))
  done < <(jq -c '.["failed_image_build_jobs"][] | select(.plan == "delete" and .status == "pending")' <<< "${cleanup_spec}")
}

function delete_images() {
  image_name=$1
  while IFS= read -r t; do
    _status=$(jq -r '.["images"]["'$image_name'"]["'$t'"]["status"]' <<< "${cleanup_spec}")
    if [ "${_status}" == "done" ]; then continue; fi

    tag="$t"
    digest=$(jq -r '.["images"]["'$image_name'"]["'$t'"]["docker_content_digest"] // ""' <<< "${cleanup_spec}")
    if [ -z "$dryrun" ] && [ "${digest}" != "" ]; then
      curl -sk -X DELETE "${registry_url}/v2/${image_name}/manifests/${digest}"
      echo "deleted manifest ${digest} (${image_name}:${tag} [$?])"
      cleanup_status=$(jq '.["images"]["'$image_name'"]["'$t'"]["status"] = "done"' <<< "${cleanup_status}")
    else
      cleanup_status=$(jq '.["images"]["'$image_name'"]["'$t'"]["status"] = "skipped"' <<< "${cleanup_status}")
    fi
    echo "$cleanup_status" > ${metadata_dir}/registry_cleanup_status.json

    image_build_jobs=$(jq '.["images"]["'$image_name'"]["'$t'"]["image_build_jobs"] // []' <<< "${cleanup_spec}")
    job_counter=0
    while IFS= read -r j; do
      ns=$(jq -r '.namespace' <<< "$j")
      id=$(jq -r '.job_id' <<< "$j")
      if [ -z "$dryrun" ] && [ -z "$in_smoke_test" ]; then
        kubectl -n ${ns} delete job ${id} --ignore-not-found
        j=$(jq '.status = "done"' <<< "$j")
      else
        j=$(jq '.status = "skipped"' <<< "$j")
      fi

      cleanup_status=$(jq --argjson j "$j" \
        --argjson c "$job_counter" \
        '.["images"]["'$image_name'"]["'$t'"]["image_build_jobs"][$c] = $j' <<< "${cleanup_status}")
      echo "$cleanup_status" > ${metadata_dir}/registry_cleanup_status.json
      job_counter=$(( job_counter+1 ))
    done < <(jq -cr '.[]' <<< "${image_build_jobs}")
  done < <(jq -cr '.["images"]["'$image_name'"] | keys[]' <<< "${cleanup_spec}")
}

function validate_cleanup_status() {
  if [ -n "$in_smoke_test" ]; then
    echo ""
    cat ${metadata_dir}/registry_cleanup_status.json

    result=$(jq -n \
      --argjson j1 "$(jq -c '.' ${mocks_dir}/expected_registry_cleanup_status.json)" \
      --argjson j2 "$(jq -c '.' ${metadata_dir}/registry_cleanup_status.json)" \
      '$j1 == $j2')
    if [ "$result" != "true" ]; then
      echo "ERROR: The registry cleanup status did not match the expected value!"
      diff ${metadata_dir}/registry_cleanup_status.json ${mocks_dir}/expected_registry_cleanup_status.json
      return 1
    else
      echo "The registry cleanup status matched the expected value in the smoke test."
    fi
  fi
}

if [ -z "${in_smoke_test}" ] && [ "$(is_lead_control_plane)" == "false" ]; then
  exit 0
fi

export mocks_dir="${script_dir}/mocks"
rm -rf ${metadata_dir}
mkdir -p ${metadata_dir}

if [ -z "$in_smoke_test" ] && [ -z "$dryrun" ]; then
  exec >>${metadata_dir}/registry-clean.log 2>&1
fi

if [ -z "${in_smoke_test}" ]; then
  get_registry_disk_usage
  get_utc_current_time
  metadata_prefix=${metadata_dir}

  # In rel-2.4, we have introduced annotations for cbcore and custom worker images in the wsjob/lock annotations.
  # TODO: In rel-2.5, updating the following whitelisting mechanism with just by inspecting lock annotations through `kubectl get rl -A`.

  # The whitelist file contains the images that are currently being used by the pending/running jobs.
  # TODO: If time permits, it'd be very valuable to integrate the image rotation with the operator.
  cbcore_whitelist_file="${metadata_dir}/cbcore_whitelist.out"
  custom_worker_whitelist_file="${metadata_dir}/custom_worker_whitelist.out"
  cluster_server_whitelist_file="${metadata_dir}/cluster_server_whitelist.out"
  touch ${cbcore_whitelist_file} ${custom_worker_whitelist_file} ${cluster_server_whitelist_file}

  # Use lock objects to identify pending/running jobs
  active_locks_file="${metadata_dir}/active_locks.out"
  if kubectl get rl &>/dev/null; then
    kubectl get rl -A --no-headers >"${active_locks_file}"
    while IFS= read -r line; do
      namespace=$(awk '{print $1}' <<<"${line}")
      name=$(awk '{print $2}' <<<"${line}")
      wsjob_obj=$(kubectl -n ${namespace} get wsjob ${name} --ignore-not-found -ojson)

      cbcore_tag=$(jq -r '.metadata.annotations["cerebras/version-wsjob"] // ""' <<<"${wsjob_obj}")
      if [ -z "${cbcore_tag}" ]; then
        # All jobs at least have one coordinator pod or one worker pod.
        # Fallback mechanism for older jobs.
        # TODO: Remove the fallback mechanism in rel-2.7.
        cbcore_tag=$(jq -r '.spec.wsReplicaSpecs | .Coordinator // .Worker | .template.spec.containers[0].image // ""' <<<"${wsjob_obj}" | cut -d':' -f2)
      fi
      if [ -n "${cbcore_tag}" ] && ! grep -q "${cbcore_tag}" "${cbcore_whitelist_file}"; then
        echo "${cbcore_tag}" >>"${cbcore_whitelist_file}"
        echo "Whitelisting cbcore tag '${cbcore_tag}' due to being used in jobs."
      fi

      custom_worker_tag=$(jq -r '.metadata.annotations["cerebras/version-user-sidecar"] // ""' <<<"${wsjob_obj}")
      if [ -z "${custom_worker_tag}" ]; then
        # Worker sidecar only exists in training execute job.
        # Activation sidecar only exists in inference execute job.
        # Fallback mechanism for older jobs.
        # TODO: Remove the fallback mechanism in rel-2.7.
        custom_worker_tag=$(jq -r '.spec.wsReplicaSpecs | .Worker // .Activation | .template.spec.containers[-1].image // ""' <<<"${wsjob_obj}" | cut -d':' -f2)
      fi
      if [ -n "${custom_worker_tag}" ] && ! grep -q "${custom_worker_tag}" "${custom_worker_whitelist_file}"; then
        echo "${custom_worker_tag}" >>"${custom_worker_whitelist_file}"
        echo "Whitelisting custom worker tag '${custom_worker_tag}' due to being used in jobs."
      fi
    done <"${active_locks_file}"
  fi

  # In rel-2.5, we have introduced annotations for sidecar images in the wsjob/lock annotations.
  # TODO: Remove the following whitelisting mechanism in rel-2.7.
  # Legacy note:
  # Use image build jobs completed in the last 24 hours to whitelist the recently-built custom worker images.
  # Larger compiles could take more than 8 hours. The image build job could finish in minutes but not be used until the execute job starts.
  # The 24-hour grace period protects the images from being deleted before they are used.
  recently_built_custom_workers="${metadata_dir}/recently_built_custom_workers.out"
  retention_sec=$((24 * 3600))
  if [ -n "${in_canary_test}" ]; then
    # In canary test, we test the cleanup routine by deleting the custom worker images.
    retention_sec=0
  fi
  kubectl get job -A -lapp=image-builder --no-headers -ojson | jq -r --argjson retention_sec $retention_sec '
    .items[] |
    select(.status.completionTime != null) |
    select(((now - ( .status.completionTime | fromdateiso8601)) | floor) < $retention_sec) |
    .metadata.annotations["image-builder/destination-image"] // ""' | cut -d':' -f2 > "${recently_built_custom_workers}"
  while IFS= read -r line; do
    if [ -n "${line}" ] && ! grep -q "${line}" "${custom_worker_whitelist_file}"; then
      echo "${line}" >> "${custom_worker_whitelist_file}"
      echo "Whitelisting custom worker tag '${line}' due to recently built."
    fi
  done < "${recently_built_custom_workers}"

  # Use the cluster-server deployments to whitelist the cluster-server and default cbcore images.
  cluster_servers=$(kubectl get deploy -A -lapp.kubernetes.io/name=cluster-server -ojson | jq -cr '.items[] | select(.status.replicas // 0 > 0 or .metadata.namespace  == "job-operator")')
  while IFS= read -r line; do
    session=$(jq -r '.metadata.namespace' <<<"${line}")
    cluster_server_tag=$(jq -r '.spec.template.spec.containers[0].image // ""' <<<"${line}" | cut -d':' -f2)
    # We update from WSJOB_DEFAULT_COORDINATOR_IMAGE to WSJOB_DEFAULT_IMAGE in release 3.0.0
    # TODO: remove old environment variable in release 3.2.0
    default_cbcore_tag=$(jq -r '.spec.template.spec.containers[0].env[] | select(.name == "WSJOB_DEFAULT_IMAGE" or .name == "WSJOB_DEFAULT_COORDINATOR_IMAGE") | .value // ""' <<<"${line}"  | cut -d':' -f2)
    if [ -n "${cluster_server_tag}" ] && ! grep -q "${cluster_server_tag}" "${cluster_server_whitelist_file}"; then
      echo "${cluster_server_tag}" >> "${cluster_server_whitelist_file}"
      echo "Whitelisting cluster server tag '${cluster_server_tag}' due to being used in session '${session}'."
    fi
    if [ -n "${default_cbcore_tag}" ] && ! grep -q "${default_cbcore_tag}" "${cbcore_whitelist_file}"; then
      echo "${default_cbcore_tag}" >> "${cbcore_whitelist_file}"
      echo "Whitelisting cbcore tag '${default_cbcore_tag}' due to being used in session '${session}'."
    fi
  done <<< "${cluster_servers}"

  # Use the debugviz-server helm value to white list one additional cbcore image.
  debugviz_server_values=$(helm -n job-operator get values debugviz-server -ojson || true 2>/dev/null)
  if [ -n "${debugviz_server_values}" ]; then
    debugviz_server_tag=$(jq -r '.image // ""' <<<"${debugviz_server_values}" | cut -d':' -f2)
    if [ -n "${debugviz_server_tag}" ] && ! grep -q "${debugviz_server_tag}" "${cbcore_whitelist_file}"; then
      echo "${debugviz_server_tag}" >> "${cbcore_whitelist_file}"
      echo "Whitelisting cbcore tag '${debugviz_server_tag}' due to being used in debugviz server."
    fi
  fi
else
  metadata_prefix=${mocks_dir}
  custom_worker_whitelist_file="${mocks_dir}/custom_worker_whitelist.out"
fi

cbcore_last_used_outfile="${metadata_prefix}/cbcore_last_used.out"
rs_last_used_outfile="${metadata_prefix}/rs_last_used.out"
custom_worker_last_used_outfile="${metadata_prefix}/custom_worker_last_used.out"
image_build_jobs_outfile="${metadata_prefix}/image_build_jobs.out"
touch ${cbcore_last_used_outfile} ${rs_last_used_outfile} ${custom_worker_last_used_outfile} ${image_build_jobs_outfile}

cleanup_spec='{"failed_image_build_jobs": []}'
echo "$cleanup_spec" > ${metadata_dir}/registry_cleanup_spec.json

registry_agnostic_cbcore_last_used_outfile="${metadata_dir}/registry_agnostic_cbcore_last_used.out"
registry_agnostic_rs_last_used_outfile="${metadata_dir}/registry_agnostic_rs_last_used.out"

scrape_cluster
scrape_registry_agnostic_references ${cbcore_last_used_outfile} ${registry_agnostic_cbcore_last_used_outfile}
scrape_registry_agnostic_references ${rs_last_used_outfile} ${registry_agnostic_rs_last_used_outfile}
get_utc_current_time

if [ -z "${in_smoke_test}" ]; then
  config_file="${script_dir}/../configs/cleanup-configs.json"
  keep_n_cbcore=$(jq -r '.keep_n_cbcore' $config_file)
  keep_n_custom_worker=$(jq -r '.keep_n_custom_worker' $config_file)
  keep_n_cluster_server=$(jq -r '.keep_n_cluster_server' $config_file)
  keep_n_job_operator=$(jq -r '.keep_n_job_operator' $config_file)
  # In canary test, we test the cleanup routine by deleting the custom worker images.
  if [ -n "${in_canary_test}" ]; then
    keep_n_custom_worker=0
  fi
else
  keep_n_cbcore=10
  keep_n_custom_worker=10
  keep_n_cluster_server=20
  keep_n_job_operator=20
fi

echo "Planning to keep ${keep_n_cbcore} cbcore, ${keep_n_custom_worker} custom-worker, ${keep_n_cluster_server} cluster-server, and ${keep_n_job_operator} job-operator images"
scrape_cleanables cbcore ${registry_agnostic_cbcore_last_used_outfile} ${keep_n_cbcore} ${cbcore_whitelist_file}
scrape_cleanables custom-worker ${custom_worker_last_used_outfile} ${keep_n_custom_worker} ${custom_worker_whitelist_file}
scrape_cleanables cluster-server ${registry_agnostic_rs_last_used_outfile} ${keep_n_cluster_server} ${cluster_server_whitelist_file}
scrape_cleanables job-operator ${registry_agnostic_rs_last_used_outfile} ${keep_n_job_operator} ""
validate_cleanup_spec
get_utc_current_time

# Up till this point, we have only created a cleanup spec and have not modified/deleted
# any images from the registry or Kubernetes resources.
cleanup_status="${cleanup_spec}"
echo "$cleanup_status" > ${metadata_dir}/registry_cleanup_status.json

delete_failed_image_jobs
delete_images cbcore
delete_images custom-worker
delete_images cluster-server
delete_images job-operator
validate_cleanup_status
get_utc_current_time

if [ -z "$dryrun" ] && [ -z "$in_smoke_test" ]; then
  # We only need to GC the first pod, since registry instances in the Ceph-enabled clusters would the same content and
  # registry instances in the hostpath-storage-based clusters would only have one registry pod.
  registry_pod=$(kubectl get pod -n kube-system -lapp=private-registry --no-headers | grep Running | awk '{print $1}' | head -1)

  echo "Triggering garbage collection for registry"
  kubectl exec "${registry_pod}" -nkube-system -- registry garbage-collect -m /etc/docker/registry/config.yml

  # We need to kill the registry pod to invalidate the in-memory cache.
  # Failing to do so would result in manifest re-push failure for images that had been garbage collected.
  # https://github.com/distribution/distribution/issues/1803
  kubectl -n kube-system rollout restart daemonset private-registry
  kubectl -n kube-system rollout status daemonset private-registry --timeout=1m
  get_utc_current_time
fi

if [ -z "$in_smoke_test" ]; then
  get_registry_disk_usage
  get_utc_current_time
fi
cleanup_metadata "$script_dir"
