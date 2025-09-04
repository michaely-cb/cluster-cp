#!/bin/bash

# this script is expected to always succeed so no need to 'set -e'
# no timeout set on script side since status update requires all pods entered init status which can vary for large jobs
# operator will handle timeout case with a global view
pod_name="$(POD_NAME)"
job_id="$(WSJOB_ID)"
namespace="$(NAMESPACE)"
expect_update_value="$(UPDATED_VALUE)"

# Note: we use the fully qualified name here to avoid search-path-based name resolution
#      (that would result in further suffixes being added, and we don't want to have this forwarded upstream)
#      See https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/#namespaces-and-dns
svc=cluster-server."$namespace".svc.cluster.local.:9000
cycle=5
error_count=0
max_errors=30
while true; do
  sleep $(((RANDOM % "$cycle") + 1))
  res=$(grpcurl -connect-timeout 3 -user-agent "$pod_name" \
    -d "{\"job_id\": \"$job_id\"}" -plaintext "$svc" cluster.cluster_mgmt_pb.ClusterManagement/GetJobInitStatus)
  echo "$(date -u): $res"
  grpc_status=$?
  if [ $grpc_status -ne 0 ]; then
    error_count=$((error_count + 1))
    # Abort early since keep waiting won't help, todo: optimize by mount cm and check content as fallback
    if [ $error_count -ge $max_errors ]; then
      echo "$(date -u): grpcurl failed $max_errors times consecutively, return early as best effort only"
      break
    fi
    continue
  else
    error_count=0
  fi

  # Check if config updated
  if echo "$res" | grep -q "$expect_update_value"; then
    # Extra sleep to ensure kubelet has watched the latest update
    sleep 3
    echo "$(date -u): config updated watched, init success"
    break
  fi
done
