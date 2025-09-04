#!/usr/bin/env bash

# this is necessary since crontab won't have the env var
export PATH="${PATH}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"

set -eo pipefail

export script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source ${script_dir}/../common.sh

# redirect stdout/stderr to a file because cron will not send email
rm -f /tmp/clean-up-etcd.log 2>/dev/null
if [ -z "$dryrun" ]; then
  exec >>/tmp/clean-up-etcd.log 2>&1
fi
get_utc_current_time

if [ "$(is_lead_control_plane)" == "true" ]; then
  etcd_pod=$(kubectl get po -nkube-system -lcomponent=etcd --no-headers | grep Running | awk '{print $1}' | head -n 1)
  # etcd defrag to release space: https://etcd.io/docs/v3.5/op-guide/maintenance/#defragmentation
  echo "Defraging etcd cluster from ${etcd_pod}"
  if [ -n "$dryrun" ] || [ -n "$in_smoke_test" ]; then exit; fi
  kubectl exec "${etcd_pod}" -nkube-system -- etcdctl defrag --cluster \
    --cacert=/etc/kubernetes/pki/etcd/ca.crt \
    --key=/etc/kubernetes/pki/etcd/server.key \
    --cert=/etc/kubernetes/pki/etcd/server.crt
fi
