#!/usr/bin/env bash

# this is necessary since crontab won't have the env var
export PATH="${PATH}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"

set -eo pipefail

dryrun=${dryrun:-}
in_smoke_test=${in_smoke_test:-}
in_canary_test=${in_canary_test:-}

export keep_last_n_dryrun=3
export keep_last_n_smoke_test=3
# keep one-week worth of history for debug
export keep_last_n_days_non_dryrun=7
export cleanup_scripts_dir="/n1/cluster-mgmt/cleanup"
export ns="job-operator"

function get_hostname() {
  hostname -s
}
export node_name=$(get_hostname)

function human_readable_bytes() {
  local bytes=$1
  local unit=("B" "KB" "MB" "GB" "TB" "PB")
  local index=0

  while ((bytes > 1024)); do
    ((bytes /= 1024))
    ((index++))
  done

  if ((index == 0)); then
    printf "%d%s\n" "$bytes" "${unit[$index]}"
  else
    local original_bytes=$1
    local divisor=1
    for ((i=0; i<index; i++)); do
      ((divisor *= 1024))
    done
    local quotient=$((original_bytes * 1000 / divisor))
    local int_part=$((quotient / 1000))
    local frac_part=$((quotient % 1000))

    # Round up if the last of the 3 decimal digits is 5 or greater.
    if ((frac_part % 10 >= 5)); then
      ((frac_part += 10))
    fi

    frac_part=$((frac_part / 10))

    # If the frac_part is 100, increase the integer part.
    if ((frac_part == 100)); then
      ((int_part++))
      frac_part=0
    fi

    printf "%d.%02d%s\n" "$int_part" "$frac_part" "${unit[$index]}"
  fi
}

function get_all_mgmt_nodes() {
  if kubectl get po >/dev/null 2>&1; then
    kubectl get nodes -lk8s.cerebras.com/node-role-management= --no-headers
  fi
}

function is_current_node_control_plane() {
  if get_all_mgmt_nodes | grep -wq "$(get_hostname)"; then
    echo true
  else
    echo false
  fi
}

function has_multiple_mgmt_nodes() {
  local num_mgmt_nodes=$(get_all_mgmt_nodes | wc -l)
  ((num_mgmt_nodes > 1)) && echo true || echo false
}

function is_lead_control_plane() {
  if [ "$(is_current_node_control_plane)" == "false" ]; then
    echo false
  else
    local lead_cp=$(get_all_mgmt_nodes | grep -v NotReady | awk '$3 == "control-plane"' | awk '{print $1}' | head -1)
    [ "${lead_cp}" == "$(get_hostname)" ] && echo true || echo false
  fi
}

function cleanup_metadata() {
  local dir=$1
  local pattern keep_max

  pushd "$dir" >/dev/null
  if [ -n "$dryrun" ]; then
    pattern="^dryrun"
    keep_max="$keep_last_n_dryrun"
  elif [ -n "$in_smoke_test" ]; then
    pattern="^smoke-test"
    keep_max="$keep_last_n_smoke_test"
  else
    pattern="^metadata"
    dates_to_keep=$(ls -1 | grep "$pattern" | cut -d'-' -f2 | cut -c 1-8 | uniq | head -n ${keep_last_n_days_non_dryrun})
    keep_max=0
    for d in $dates_to_keep; do
      keep_max=$(( keep_max + $(ls -1 | grep -c "$pattern-$d") ))
    done

    # add latest symlink for easier access
    if ls -1 | grep -q "$pattern"; then
      rm -f latest
      ln -s $(ls -1 | grep "$pattern" | tail -1) latest
    fi
  fi

  total=$(ls -1 | grep "$pattern" | wc -l)
  limit=$((total - keep_max))
  ls -1 | grep "$pattern" | awk -v limit="$limit" 'NR <= limit' | xargs -I {} rm -r {}

  popd >/dev/null
}

function get_utc_current_time() {
  date -u +%Y%m%dT%H%M%SZ
}