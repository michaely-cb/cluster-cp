#!/usr/bin/env bash

# this is necessary since crontab won't have the env var
export PATH="${PATH}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"
NERDCTL=/usr/local/bin/nerdctl

set -eo pipefail

if [ -n "$in_smoke_test" ]; then
  exit 0
fi

df -h /var/lib/containerd/

date_to_seconds() {
  # remove TZ if present
  local date_no_tz=$(echo "$1" | sed -E 's/ [A-Z]+$//')
  date -d "$date_no_tz" +%s
}

cleanup_old_containers() {
  local query="$1"
  local namespace_option="$2"
  local killed_containers=""

  for container_id in $($NERDCTL $namespace_option container ls -a --no-trunc --format '{{json .}}' | jq -cr "$query" | jq -r '.ID'); do
    local container=$($NERDCTL $namespace_option container inspect "$container_id" | jq -c)
    local created_at=$(echo "$container" | jq -r '.[0].Created')
    local created_at_seconds=$(date -d "$created_at" +%s)

    local seven_days_ago=$(date -d '7 days ago' +%s)
    if [[ "$created_at_seconds" -lt "$seven_days_ago" ]]; then
      if [ -z "$dryrun" ]; then
        $NERDCTL $namespace_option kill "$container_id" || true
      fi
      killed_containers="$killed_containers $container_id"
      echo "Killed container $container_id"
    fi
  done

  # Clean up exited containers if any were killed
  if [ -z "$dryrun" ] && [ -n "$killed_containers" ]; then
    sleep 3  # give a few seconds to await container Exited
    $NERDCTL $namespace_option container ls -a --no-trunc --format '{{ json . }}' | jq -c 'select(.Status | startswith("Exited"))' | jq -r '.ID' | xargs -r $NERDCTL $namespace_option rm || true
  fi
}

# Kill any container in the k8s namespace that was using the default cbcore entrypoint and was more than 7 days old
# https://cerebras.atlassian.net/browse/SW-128176
echo "Cleaning up old cbcore containers in the k8s namespace"
cleanup_old_containers 'select(.Command == "\"/cbcore/bin/entrypoint\"")' "-nk8s.io"

# Kill any container in the default namespace that was more than 7 days old
echo "Cleaning up old containers in the default namespace"
cleanup_old_containers "." "-ndefault"

# Delete images more than a day old. May fail if there are running containers with the given image which is OK
one_day_ago=$(date -d '1 day ago' +%s)
for image_id in $($NERDCTL images -q); do
  created_at=$($NERDCTL image inspect "$image_id" | jq -r '.[0].Created')
  created_at_seconds=$(date_to_seconds "$created_at")

  if [[ "$created_at_seconds" -lt "$one_day_ago" ]]; then
    if [ -z "$dryrun" ]; then
      $NERDCTL rmi "$image_id" || true
    fi
    echo "deleted image $image_id in the default containerd namespace"
  fi
done

# clean ctr images/containers
# Ideally, kubelet would garabage collect BEFORE going into a DISKPRESSURE state
# and therefore making the node unschedulable but it does not- so we clean up
# the images pre-emptively.
# THRESHOLD=50 since cbcores routinely get to 20GB while nodes generally have a
# 450GB root disk - should allow 5 cbcores added between hourly cleanups without
# disk pressure
# Note that this threshold must be no greater than the local log storage threshold,
# which currently is at 50, to ensure the containerd cleanup always goes first
# TODO: SW-188626
# We may want to revisit the optimization if the disk presure remains somewhat
# regular and disruptive.
DISK_USAGE_THRESHOLD=50
DISK_USAGE=$(df --output=pcent /var/lib/containerd/ | tail -n1 | tr -d '%')
if [ "${DISK_USAGE}" -lt ${DISK_USAGE_THRESHOLD} ]; then
  echo "Disk usage:${DISK_USAGE}% is less than ${DISK_USAGE_THRESHOLD}%, not cleaning images"
  exit 0
fi

# cleanup containers + dangling images
if [ -z "$dryrun" ]; then
  $NERDCTL -nk8s.io container prune -f
  $NERDCTL -nk8s.io image prune -f
fi

# cleanup unused images
used_images=$($NERDCTL -nk8s.io ps --format='{{json .Image}}')
# calculate the timestamp of one hour ago and filter images for unused ones
filter_time=$(($(date +%s) - 3600))
while IFS= read -r image_json; do
  image_date=$(echo "$image_json" | jq -r '.CreatedAt' | awk '{print $1, $2, $3}')
  image_timestamp=$(date -d "$image_date" +%s)
  repo=$(echo "$image_json" | jq -r '.Repository')
  tag=$(echo "$image_json" | jq -r '.Tag')
  image_id=$(echo "$image_json" | jq -r '.ID')

  # skip orphan/reserved images
  if echo "$repo:$tag" | grep -Eiq "<none>|registry:|pause:|coredns:|busybox:|python:"; then
    echo "skip image $repo:$tag with name filter"
    continue
  fi

  # skip images created in last hour
  if [ "$image_timestamp" -ge "$filter_time" ]; then
    echo "skip image $repo:$tag created in last hour"
    continue
  fi

  if ! echo "$used_images" | grep -wq "$repo:$tag"; then
    echo "start deleting unused image $repo:$tag | $image_id"
    if [ -z "$dryrun" ]; then
      # force flag is only to delete images with same ids and will still fail if image in use as safeguard
      $NERDCTL -nk8s.io rmi -f "$image_id" || echo "delete $repo:$tag | $image_id failed"
    fi
  fi
done < <($NERDCTL -nk8s.io image ls --format=json)

# cleanup orphan images if exists
# not needed technically but in case of legacy leftover
# should remove in future
$NERDCTL -n k8s.io images | grep none | awk '{print $3}' | sort | uniq >/tmp/none.img
$NERDCTL -n k8s.io images | grep -v none | awk '{print $3}' | sort | uniq >/tmp/named.img
while IFS= read -r image_id; do
  echo "start deleting orphan image $image_id"
  if [ -z "$dryrun" ]; then
    $NERDCTL -nk8s.io rmi -f "$image_id" || echo "delete $image_id failed"
  fi
done < <(comm -23 /tmp/none.img /tmp/named.img)

df -h /var/lib/containerd/
