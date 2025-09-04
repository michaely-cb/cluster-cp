#!/usr/bin/env bash

#
# There is a bug in kubernetes that prevents an orphan pod directory from getting cleaned
# up. See https://github.com/kubernetes/kubernetes/issues/105536. This causes spilling
# in kubelet logs. The messages like
#      "There were many similar errors. Turn up verbosity to see them"
# appear every 2 seconds. This cleanup scripts try to clean the volumes for these pods,
# so these pod directories can be removed.

# this is necessary since crontab won't have the env var
export PATH="${PATH}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"
dryrun=${dryrun:-}

export script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source ${script_dir}/../common.sh

set -x

# redirect stdout/stderr to a file because cron will not send email
rm -f /tmp/clean-up-pod-volumes.log 2>/dev/null
if [ -z "$dryrun" ]; then
 exec >>/tmp/clean-up-pod-volumes.log 2>&1
fi
get_utc_current_time

KUBELET_HOME="/var/lib"
fetch_pods_list="True"

# kubelet seems to spill logs like "There were many similar errors. Turn up verbosity to see them" every 2 seconds
# for one orphaned pod. When the pod directory can be finally removed, the logs for another orphaned pod will appear.
# We try to a few rounds in one clean up. Hopefully to catch more orphaned pods in one run.
for ((i=1; i<=5; i++)); do
  orphan_exists="false"
  for pod_id in `journalctl -u kubelet --since $(date +%Y-%m-%d) | grep "There were many similar errors. Turn up verbosity to see them" | tail -1 | awk '{print $23}' | sed 's/\\\"//g'`; do
    pod_dir="${KUBELET_HOME}/kubelet/pods/$pod_id"

    # If the volume directory does not exist, skip.
    if [ ! -d ${pod_dir}/volumes/ ]; then
      continue
    fi

    if [ "$fetch_pods_list" == "True" ]; then
      kubectl get pods -A -o custom-columns=PodName:.metadata.name,PodNamespace:.metadata.namespace,PodUID:.metadata.uid > /tmp/all-pods-list
      fetch_pods_list="False"
    fi

    # Skip cleanup if the pod is in Running or Pending phase.
    pod_name_namespace=$(grep $pod_id /tmp/all-pods-list)
    if [ -n "$pod_name_namespace" ]; then
      pod_name=$(echo "$pod_name_namespace" | awk '{print $1}')
      pod_namespace=$(echo "$pod_name_namespace" | awk '{print $2}')

      phase=$(kubectl get pod "$pod_name" -n "$pod_namespace" -ojsonpath={'.status.phase'})
      if [ "$phase" == "Running" ] || [ "$phase" == "Pending" ]; then
        echo "Skip cleaning up pod ($pod_id, $pod_name, $pod_namespace) since it is in $phase phase"
        continue
      fi
    fi

    echo "Found orphan pod $pod_id in kubelet logs. Need cleaning up"
    orphan_exists="true"

    # umount subpath if exist
    if [ -d ${pod_dir}/volume-subpaths/ ]; then
      mnt_paths=`mount | grep ${pod_dir}/volume-subpaths/ | awk '{print $3}'`
      for mnt_path in $mnt_paths; do
        echo "umount subpath $mnt_path"
        umount "$mnt_path"
      done
    fi

    # Find all volumes in `${pod_dir}/volumes/` directory. Unmount them if they are mount points,
    # and remove their directories afterwards.
    volume_types=`ls ${pod_dir}/volumes/`
    for volume_type in $volume_types; do
      sub_volumes=`ls -A ${pod_dir}/volumes/$volume_type`
      if [ -n "$sub_volumes" ]; then
        echo "${pod_dir}/volumes/$volume_type contains volume: $sub_volumes"
        for sub_volume in $sub_volumes; do
          # check sub_volume path is mounted or not
          sub_volume_path="${pod_dir}/volumes/$volume_type/$sub_volume"
          findmnt $sub_volume_path
          if [ "$?" != "0" ]; then
            echo "${sub_volume_path} is not mounted, just need to remove"
          else
            echo "${sub_volume_path} is mounted, umount it and remove"
            umount "${sub_volume_path}"
          fi
          rm -rf "${sub_volume_path}"
        done
      fi
    done
  done

  if [ "$orphan_exists" = "false" ]; then
    break
  fi
  # wait for 2 seconds
  sleep 2
done
