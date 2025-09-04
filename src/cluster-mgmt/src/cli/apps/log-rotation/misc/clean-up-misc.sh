#!/usr/bin/env bash

# this is necessary since crontab won't have the env var
export PATH="${PATH}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"

set -eo pipefail

export script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source ${script_dir}/../common.sh

if [ "$(is_lead_control_plane)" != "true" ]; then
  exit 0
fi

# redirect stdout/stderr to a file because cron will not send email
rm -f /tmp/clean-up-misc.log 2>/dev/null
if [ -z "$dryrun" ]; then
  exec >>/tmp/clean-up-misc.log 2>&1
fi
get_utc_current_time

# cleanup kube-system jobs if created > 24h to reduce alert noise, keep 1d for investigation
jobs=$(kubectl get jobs -nkube-system -ojson | jq -r '.items[] | select((.metadata.creationTimestamp | fromdate) < (now - 86400)) | .metadata.name')
for job in $jobs; do
  kubectl delete job "$job" -nkube-system --ignore-not-found
done

# cleanup config DS fully ready
if kubectl get daemonset cluster-server-config -njob-operator &>/dev/null; then
  DESIRED=$(kubectl get daemonset cluster-server-config -njob-operator -o jsonpath='{.status.desiredNumberScheduled}')
  READY=$(kubectl get daemonset cluster-server-config -njob-operator -o jsonpath='{.status.numberReady}')
  if [ "$DESIRED" -eq "$READY" ]; then
    echo "All pods of DaemonSet cluster-server-config are ready. Deleting DaemonSet..."
    kubectl delete daemonset cluster-server-config -njob-operator --ignore-not-found
  fi
fi

# cleanup deploy-tools-node DS fully ready
USER_NAMESPACE_LABEL="user-namespace"
user_namespaces=$(kubectl get namespace -l "${USER_NAMESPACE_LABEL}=" -o=jsonpath="{range .items[*]}{.metadata.name}{'\n'}{end}")
deploy_tools_node_ds="deploy-tools-node"
for ns in job-operator $user_namespaces; do
  if kubectl get daemonset $deploy_tools_node_ds -n$ns &>/dev/null; then
    DESIRED=$(kubectl get daemonset $deploy_tools_node_ds -n$ns -o jsonpath='{.status.desiredNumberScheduled}')
    READY=$(kubectl get daemonset $deploy_tools_node_ds -n$ns -o jsonpath='{.status.numberReady}')
    if [ "$DESIRED" -eq "$READY" ]; then
      echo "All pods of DaemonSet $deploy_tools_node_ds in namespace $ns are ready. Deleting DaemonSet..."
      kubectl delete daemonset $deploy_tools_node_ds -n$ns --ignore-not-found
    fi
  fi
done

# 2.2+ job operator handles deletes automatically. This can be removed once 2.1 is no longer in use
# cleanup orphan cluster-role/binding
for cr in cluster-server-cluster-role log-export-cluster-role manager-role proxy-role wsjob-init-role metrics-reader; do
  for crname in $(kubectl get clusterrole -oname | grep $cr | cut -d/ -f2); do
    ns=${crname/-$cr/}
    if ! kubectl get ns "$ns"; then
      echo "removing clusterrole $crname"
      kubectl delete clusterrole $crname
    fi
  done
done
for crb in cluster-server-cluster-rolebinding log-export-cluster-rolebinding manager-rolebinding proxy-rolebinding wsjob-init-rolebinding; do
  for crbname in $(kubectl get clusterrolebinding -oname | grep $crb | cut -d/ -f2); do
    ns=${crbname/-$crb/}
    if ! kubectl get ns "$ns"; then
      echo "removing clusterrolebinding $crbname"
      kubectl delete clusterrolebinding $crbname
    fi
  done
done
# Clean up released ceph volumes. Delete PV retention policy not working SW-112442
for pv in log-export cached-compile debug-artifact; do
  for pvname in $(kubectl get pv -oname | grep $pv | cut -d/ -f2); do
    ns=${pvname/$pv-/}
    ns=${ns/-pv/}
    if kubectl get ns "$ns" &>/dev/null; then
      continue
    fi
    if [ "Released" = "$(kubectl get pv $pvname -ojsonpath='{.status.phase}')" ]; then
      kubectl delete pv "$pvname"
    fi
  done
done

