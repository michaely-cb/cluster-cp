#!/usr/bin/env bash
set -e

# This script checks for cilium and multus status
# It's intended to be run as post cluster bring validation by cerebras OP

# for cilium it checks for its connectivity test:
# https://github.com/cilium/cilium-cli/blob/master/connectivity/suite.go
# https://github.com/cilium/cilium/blob/master/examples/kubernetes/connectivity-check/connectivity-check.yaml
# https://github.com/cilium/cilium/blob/master/.github/workflows/conformance-eks-v1.11.yaml#L315

# for multus, it launches DS for each NG and check connectivity of the data IP from the Nginx pod
# if there's no data IP, describe the pod and check if annotation exists
# or describe net-attach-def to check NIC name / subnet/ routing

CLUSTER_CONFIG="/opt/cerebras/cluster/cluster.yaml"
CLUSTER_PROPERTIES="/opt/cerebras/cluster/pkg-properties.yaml"

function get_image_version() {
  image=$1
  if [ -f "${CLUSTER_PROPERTIES}" ]; then
    yq -ojson ${CLUSTER_PROPERTIES} | jq -r ".properties.images.\"${image}\" // \"latest\""
  else
    kubectl get cm job-operator-cluster-env -ojson | jq -r ".metadata.annotations.\"${image}-tag\" // \"latest\""
  fi
}

SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE:-job-operator}
TEST_NAMESPACE=${TEST_NAMESPACE:-network-test}
ALPINE_IMAGE=registry.local/alpine-kubectl:$(get_image_version alpine-kubectl)
CBCORE_OVERRIDE=${CBCORE_OVERRIDE:-}

# todo: source global config to parse svc domain

function cilium-validation() {
  if [ -d /kind ]; then
    # kind doesn't support cilium
    echo "kind cluster detected, skipping cilium validation"
    return 0
  fi
  echo "validating cilium start"
  
  # Check for Cilium sync errors (detection only - no automatic remediation)
  echo "Checking for Cilium service synchronization errors..."
  local sync_error_count=0
  
  # Use cilium CLI for efficient cluster-wide status check
  if command -v cilium >/dev/null 2>&1; then
    sync_error_count=$(cilium status 2>/dev/null | grep -c "sync-lb-maps.*failing" 2>/dev/null || echo "0")
  else
    echo "Warning: cilium CLI not available, skipping sync error check"
    sync_error_count=0
  fi
  sync_error_count=$(echo "$sync_error_count" | tr -d '\n\r')

  if [ "$sync_error_count" -gt 0 ]; then
    echo "WARNING: Found Cilium sync errors on $sync_error_count instance(s)."
    echo "This typically occurs during upgrades when service LB maps are out of sync."
    echo ""
    echo "You will likely need to restart the nodes to resolve this issue (refer CS-966 for context) and clear cilium status"
    echo ""
    echo "Validation will continue, but network functionality may be impacted."
  else
    echo "No Cilium sync errors detected, proceeding with validation"
  fi
  
  local jsonmock_img="registry.local/quay/cilium/json-mock:v1.3.8"
  local alpinecurl_img="registry.local/quay/cilium/alpine-curl:v1.10.0"
  # test no policy only since we don't use any policy as of today Nov 2022
  local test_args=(--flow-validation=disabled --hubble=false --force-deploy --test-namespace=cilium-test)
  if nerdctl -n k8s.io image pull --hosts-dir /etc/containerd/certs.d/ "${alpinecurl_img}" >/dev/null 2>&1; then
    local coredns_image=$(kubectl get deploy -nkube-system coredns -ojson | jq .spec.template.spec.containers[0].image -r)
    test_args+=(--dns-test-server-image="$coredns_image")
    test_args+=(--curl-image="${alpinecurl_img}")
    test_args+=(--json-mock-image="${jsonmock_img}")
    # the excluded actions talk to public DNS and don't work in internetless
    test_args+=(--test 'no-policies,!/pod-to-cidr,!/pod-to-world')
  else
    test_args+=(--test no-policies)
  fi

  if kubectl get nodes | grep -q NotReady; then
    echo "Some nodes are not ready. Skip cilium status check, and only check ready cilium pods"
    all_nodes=$(kubectl get nodes --no-headers | wc -l)
    not_ready_nodes=$(kubectl get nodes | grep NotReady | wc -l)
    ready_nodes=$((all_nodes - not_ready_nodes))
    ready_cilium_pods=$(kubectl -n kube-system get ds cilium -ojsonpath='{.status.numberReady}')
    timeout 1200 bash -c -- "while [[ $ready_cilium_pods -lt $ready_nodes ]] ; do
      echo 'Waiting for cilium pods to be in running state, expecting at least $ready_nodes, seeing $ready_cilium_pods'
      sleep 10
      ready_cilium_pods=$(kubectl -n kube-system get ds cilium -ojsonpath='{.status.numberReady}')
    done"
    echo "Done checking ready cilium pods: $ready_cilium_pods pods are ready."
  else

    # cilium tests might start pods on non-control-plane nodes. If the cluster only have control plane nodes,
    # cilium test will fail due to pods in Pending state. We will skip the connectivity test in this case, and
    # only do `cilium status` check.
    total_nodes=$(kubectl get nodes --no-headers | wc -l)
    control_plane_nodes=$(kubectl get nodes -l node-role.kubernetes.io/control-plane --no-headers | wc -l)
    if [ "$total_nodes" -eq "$control_plane_nodes" ]; then
      echo "Skipping cilium connectivity test, as there are no worker nodes in the cluster."
      echo "Validating cilium status only"
      if ! cilium status --wait; then
        echo "cilium status check failed, please check cilium pods and logs"
        return 1
      else
        echo "cilium status check success"
        return 0
      fi
    fi
    cilium connectivity test "${test_args[@]}"
    echo "validating cilium success"
    kubectl delete ns -lapp.kubernetes.io/name=cilium-cli --wait=false
  fi
}

function multus-validation() {
  local new_only=false
  local label_str=""
  if [ -n "$list_arg" ]; then
    new_only=true
    label_str="cerebras/multus-selective-test"

    trap "kubectl label nodes --all cerebras/multus-selective-test-" EXIT
    while read -r n; do
      if [ -n "$n" ]; then
        kubectl label node "${n}" cerebras/multus-selective-test=""
      fi
    done <"$list_filename"
  elif [ -n "$incremental_arg" ]; then
    label_str="cerebras/incremental-new"
    new_only=true
  fi

  if $new_only && [ "$(kubectl get nodes -l "$label_str" -ojson | jq '.items|length')" -eq 0 ]; then
    echo "multus-validation: Skipping test because there are no nodes are marked for testing"
    return 0
  fi
  if kubectl get net-attach-def -n"$SYSTEM_NAMESPACE" >/dev/null 2>&1; then
    if [ "$(kubectl get net-attach-def -n"$SYSTEM_NAMESPACE" -ojson | jq '.items|length')" -eq 0 ]; then
      echo "no net-attach-def found, skip validating multus"
      return 0
    fi
  else
    echo "no net-attach-def CRD installed, skip validating multus"
    return 0
  fi

  echo "validating multus start"
  local errors nginx_pods nginx_missing_attach data_ip pod nginx_po

  kubectl get pods -n ingress-nginx -lapp.kubernetes.io/name=ingress-nginx -ojson >.nginx.json
  nginx_pods=$(jq -r '.items[] | .metadata.name' .nginx.json)
  nginx_missing_attach=$(jq -r '.items[] | select(
    .metadata.annotations."k8s.v1.cni.cncf.io/network-status" == null or
    (.metadata.annotations."k8s.v1.cni.cncf.io/network-status" | fromjson | .[1].ips[0] == null)
  ) | .metadata.name' .nginx.json)
  rm -f .nginx.json
  if [ -z "${nginx_pods}" ]; then
    echo "error: nginx not deployed, please check nginx install step"
    return 1
  fi
  if [ -n "${nginx_missing_attach}" ]; then
    echo "error: data IP not found for nginx pods: $nginx_missing_attach, please check nginx install step"
    return 1
  fi

  kubectl delete ns $TEST_NAMESPACE --ignore-not-found=true
  kubectl create namespace "$TEST_NAMESPACE"

  if [ -d /kind ]; then
    start_multus_test multus-data-net "kubernetes.io/hostname" "Exists"
  else
    start_multus_test multus-data-net "k8s.cerebras.com/node-role-broadcastreduce" "DoesNotExist"
    if [ "$(kubectl get nodes -l k8s.cerebras.com/node-role-activation -ojson | jq '.items|length')" -ne 0 ]; then
      # run secondary on both ACT if ACT exists
      if kubectl get net-attach-def -n$SYSTEM_NAMESPACE multus-data-net-1 >/dev/null 2>&1; then
        start_multus_test multus-data-net-1 "k8s.cerebras.com/node-role-activation" "Exists" new_only label_str
      fi
    fi
  fi

  kubectl delete ns "$TEST_NAMESPACE"
  if [ -n "$list_arg" ]; then
    trap - EXIT
    kubectl label nodes --all cerebras/multus-selective-test-
  fi

  echo "Validating multus success!"
  return 0
}

function start_multus_test() {
  errors=0
  nad=$1
  role=$2
  operand=$3
  new_only=$4
  label_str=$5

  ds_yaml=$(mktemp)
  cat <<EOF >$ds_yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: $nad
  labels:
    k8s-app: multus-test
spec:
  selector:
    matchLabels:
      k8s-app: multus-test
      net-attach: $nad
  template:
    metadata:
      annotations:
        k8s.v1.cni.cncf.io/networks: $SYSTEM_NAMESPACE/$nad
      labels:
        k8s-app: multus-test
        net-attach: $nad
    spec:
      affinity:
        nodeAffinity:
          # avoid pure BR nodes (OK to schedule on BR if single-box fake BR)
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: $role
                operator: $operand
EOF

  if [[ "$new_only" == "true" ]]; then
    cat <<EOF >>$ds_yaml
              - key: $label_str
                operator: Exists
EOF
  fi
  cat <<EOF >>$ds_yaml
      containers:
        - name: test
          command: ["nc", "-v", "-lk", "-p", "9000", "-e", "true"]
          image: "${ALPINE_IMAGE}"
          ports:
            - containerPort: 9000
      tolerations:
        - key: node-role.kubernetes.io/master
          operator: Exists
          effect: NoSchedule
        - key: node-role.kubernetes.io/control-plane
          operator: Exists
          effect: NoSchedule
EOF

  kubectl -n "$TEST_NAMESPACE" apply -f $ds_yaml
  kubectl rollout status -n"$TEST_NAMESPACE" ds "$nad" --timeout=180s
  if [ "$(kubectl get ds -n"$TEST_NAMESPACE" "$nad" -ojson | jq .status.numberReady)" -eq 0 ]; then
    echo "error: expected at least one node to launch the multus test pod, but none matched. Check node selectors in $ds_yaml"
    return 1
  fi
  rm $ds_yaml

  for pod in $(kubectl get pods -l "k8s-app=multus-test" -n"$TEST_NAMESPACE" -ojson | jq .items[].metadata.name -r); do
    data_ip=$(kubectl get po "$pod" -n"$TEST_NAMESPACE" -ojson | jq '.metadata.annotations."k8s.v1.cni.cncf.io\/network-status"' -r | jq .[1].ips[0] -r)
    if [ "$data_ip" = "null" ]; then
      echo "error: data IP not found for pod $pod, please check by describe pod"
      return 1
    fi

    for nginx_po in ${nginx_pods}; do
      echo "Running: kubectl exec -it -ningress-nginx $nginx_po -- nc -z -w 10 $data_ip 9000"
      if ! kubectl exec  -ningress-nginx "$nginx_po" -- nc -z -w 10 "$data_ip" 9000; then
        echo "error: checking connectivity failed on $pod, please check pod/net-attach-def"
        return 1
      fi
    done
  done
  return 0
}

function systems_created_24h_ago() {
  for ts in $(kubectl get systems -ojsonpath='{.items[*].metadata.creationTimestamp}'); do
    if [ $(($(date -u +"%s") - $(date -u -d "$ts" +"%s"))) -gt 86400 ]; then
      return 1
    fi
  done
  return 0
}

function has_active_jobs() {
  # locks may be in error or pending state besides granted - but all these cases indicate activity
  test -n "$(kubectl get rl -A -oname 2>/dev/null)"
}

# print out dangling netns for debug purpose
# ideally this can also check for duplicate netns/ip
function dangling_netns_check() {
  workdir=$(mktemp -d)
  yq '.nodes[].name' ${CLUSTER_CONFIG} >"$workdir/nodes"
  NODE_LIST="$workdir/nodes"

  function retry_pssh {
    local attempt max rc
    attempt=0
    max=5
    while true; do
      "$@" && break
      rc=$?
      if [ $rc -ne 1 ]; then
        return $rc
      fi
      attempt=$((attempt + 1))
      if [[ $attempt -gt $max ]]; then
        return 1
      fi
      echo "Command failed, retry $attempt/$max"
      echo ""
      sleep 1
    done
  }
  # shellcheck disable=SC2016
  retry_pssh pssh -p 300 -x '-o StrictHostKeyChecking=no' -h "$NODE_LIST" --inline '
             error=false
             for ns in $(ip netns | awk "{print $1}"); do
               ip_info=$(ip netns exec $ns ip a show dev net1 2>/dev/null)
               if [ -z "$ip_info" ]; then
                 continue  # Skip if no IP info is available
               fi

               pid_count=$(ip netns pids $ns 2>/dev/null | wc -l)
               # at least 1 proc should exist
               # note: even if 1, it maybe dangling and just the pause process
               # keep <1 to reduce noise but can tune to <=1 for further debug
               # can also add ip exist check since if ns but no ip, then not critical
               if [ -e /var/run/netns/$ns ] && [ $pid_count -le 1 ]; then
                 # sleep to reduce race condition that netns is tearing down
                 sleep 5
                 pid_count=$(ip netns pids $ns 2>/dev/null | wc -l)
                 if [ -e /var/run/netns/$ns ] && [ $pid_count -lt 1 ]; then
                   error=true
                   ls -l /var/run/netns/$ns
                   echo "$ns pid count: $pid_count"
                   echo "$ip_info" | grep "inet " | awk "{print \$2}"
                   echo "---------------------"
                 fi
               fi
             done
             if $error; then exit 1; fi
             ' | grep -v "SUCCESS" >"$workdir/netns" || true
  if [ -s "$workdir/netns" ]; then
    echo "error: dangling netns found"
    cat "$workdir/netns"
    return 1
  else
    echo "ok: netns check success"
    return 0
  fi
  rm -rf "$workdir"
}

# Scans through virtual ranges, detecting IP addresses that are in-use but not
# registered with multus/whereabouts, returning 1 if rogue addresses are found
# Unregistered ip can happen due to race condition from GC
# or from dangling network NS left by force cleanup like OOMKill
function data_ip_reservation_check() {
  if ! kubectl get overlappingrangeipreservations >/dev/null 2>&1; then
    echo "overlappingrangeipreservations CRD not installed, skip validating multus ip ranges"
    return 0
  fi
  local workdir virtual_ranges virtual_start virtual_end non_k8_ips range_count current_range
  workdir=$(mktemp -d)
  echo "$workdir"
  kubectl get overlappingrangeipreservations -A >"${workdir}/k8_ips"
  cat "${workdir}/k8_ips" | tr -s ' ' | cut -d' ' -f2 | sort | uniq >"$workdir/k8_ips_sorted"

  # iterate through virtual ranges and scan for ICMP replies
  virtual_ranges=$(kubectl get cm cluster -n"$SYSTEM_NAMESPACE" -ojsonpath='{.data.clusterConfiguration\.yaml}' |
    yq -ojson |
    jq -r '[
            (.groups[] | [(.switchConfig.virtualStart // "xxx"), (.switchConfig.virtualEnd // "xxx")]),
            ((.v2Groups // [])[] | .vlans[] | [(.virtualStart // "xxx"), (.virtualEnd // "xxx")])
    ] | .[] | @csv' |
    grep -v 'xxx' |
    tr -d '"' |
    sort |
    uniq)
  range_count=$(wc -l <<<$virtual_ranges)
  current_range=1

  echo "scanning ${range_count} ranges"
  error_found=false
  for range in $virtual_ranges; do
    virtual_start=$(cut -d, -f1 <<<"$range")
    virtual_end=$(cut -d, -f2 <<<"$range")
    echo "scanning ${current_range}/${range_count} - ${virtual_start} to ${virtual_end}"
    if which fping &>/dev/null; then
      fping -a -g -q -r 0 -p 100 -t 100 $virtual_start $virtual_end >$workdir/pingable_ips || true
    else
      nerdctl -n k8s.io container run --network=host -t --rm -v ${workdir}:/host/ ${ALPINE_IMAGE} \
        sh -c "fping -a -g -q -r 0 -p 100 -t 100 $virtual_start $virtual_end > /host/pingable_ips" || true
    fi
    cat "${workdir}/pingable_ips" | sort | uniq >"$workdir/pingable_ips_sorted"
    wc -l "$workdir/pingable_ips_sorted"
    non_k8_ips=$(comm -13 "$workdir/k8_ips_sorted" "$workdir/pingable_ips_sorted")
    if [ -n "$non_k8_ips" ]; then
      echo "$non_k8_ips" | while IFS= read -r ip; do
        if ping -c 1 -W 1 "$ip" >/dev/null; then
          if ! kubectl get overlappingrangeipreservations "$ip" -nkube-system &>/dev/null; then
            echo "error: data $ip in use but not reserved by whereabouts!"
            error_found=true
          fi
        fi
      done
    fi
    echo "Done scanning ${current_range}/${range_count} - ${virtual_start} to ${virtual_end}"
    current_range=$((current_range + 1))
  done

  rm -rf "$workdir"
  if ! "$error_found"; then
    echo "ok: data ip reservation check success"
    return 0
  else
    echo "error: data ip reservation check failed"
    return 1
  fi
}

function cs-validation() {
  if [ -d /kind ]; then
    # kind cluster does not have systems to validate
    echo "kind cluster detected, skipping system validation"
    return 0
  fi
  local clear_state cbcore have_image pod_name tempdir
  local new_only=false

  if [ -n "$list_arg" ]; then
    new_only=true
    # list_filename populated in calling function by getopts
  elif [ -n "$incremental_arg" ]; then
    INCREMENTAL_CLUSTER_JSON="${INCREMENTAL_DIR}/incremental-cluster.json"
    new_only=true
    list_filename=$INCREMENTAL_DIR/systems_added
  fi

  tempdir=$(mktemp -d)
  trap "rm -rf $tempdir" EXIT RETURN  # Ensure cleanup at the end

  if [ -z "${CBCORE_OVERRIDE}" ]; then
    cbcore=$(helm get values -n"$SYSTEM_NAMESPACE" cluster-server -ojson | jq .wsjob.image -r)
    # Starting from cluster-3.0.0, cluster-server doesn't include a cbcore image tag any more.
    # We will skip the system validation in that case.
    if [ -z "$cbcore" ]; then
      echo "WARNING: cbcore image not found in cluster-server helm values, please set CBCORE_OVERRIDE to specify the image"
      echo "Skipping system validation"
      return 0
    fi
    cbcore="registry.local/${cbcore//*cbcore/cbcore}"
  else
    cbcore=${CBCORE_OVERRIDE}
  fi
  # first pull the cbcore image needed to run config_api
  have_image="true"

  clear_state="false"
  if systems_created_24h_ago && ! has_active_jobs; then
    echo "New cluster bring up detected, clearing cm state before checking cm status"
    clear_state="true"
  fi

  # Pull the cbcore image
  echo "Checking availability of cbcore image ${cbcore}. This may take some time..."
  if ! nerdctl -n k8s.io pull "$cbcore" &>/dev/null; then
    echo "WARNING: could not pull $cbcore, skipping cm tests"
    have_image="false"
  fi

  # iterate over all the systems
  if [ -n "$INCREMENTAL_CLUSTER_JSON" ]; then
    cp "$INCREMENTAL_CLUSTER_JSON" .cluster.json
  else
    kubectl get cm cluster -n"$SYSTEM_NAMESPACE" -ojsonpath='{.data.clusterConfiguration\.yaml}' | yq -ojson >.cluster.json
  fi

  local systems temp_systems
  jq -r '.systems[] | .name + "@" + .controlAddress' .cluster.json > "${tempdir}/system_cm.txt"
  temp_systems=$(jq -r '.systems[] | .name' .cluster.json)
  echo "===1. Systems to be validated ==="
  while read -r system; do
    # if we have a target list, skip this system if it's not in the list
    if $new_only && [ -n "$list_filename" ] && ! grep -q -w "$system" "$list_filename"; then
      continue
    fi
    [[ -z "$system" ]] && continue
    systems+="$system"$'\n'
  done <<< "$temp_systems"

  # Remove the last newline character from $systems
  systems=$(echo "$systems" | sed '$d')

  # Get total number of Kubernetes nodes with the specific label
  local num_k8s_nodes
  num_k8s_nodes=$(kubectl get nodes -lk8s.cerebras.com/node-role-coordinator -oname | wc -l)
  if [ "$num_k8s_nodes" -le 0 ]; then
    echo "Error: No management nodes found in the cluster"
    return 1
  fi

  # Split systems into batches
  local total_systems num_pods systems_per_batch
  num_portgroups=3
  total_systems=$(echo "$systems" | wc -l)
  num_pods=${num_k8s_nodes:-5} # Default to the number of labeled nodes, fallback to 5 if no nodes found
  systems_per_batch=$(( (total_systems + num_pods - 1) / num_pods ))

  declare -a batches
  local batch_id=0

  while read -r system; do
    local batch_index=$(( batch_id / systems_per_batch ))
    batches[batch_index]+="${system}"$'\n'
    batch_id=$((batch_id + 1))
  done <<< "$systems"

  # Remove any empty batches before processing
  for i in "${!batches[@]}"; do
    if [ -z "${batches[$i]}" ]; then
      unset 'batches[$i]'
    fi
  done

  echo "Total systems: $total_systems, total management nodes: $num_k8s_nodes, systems per batch: $systems_per_batch"

  echo "Processing systems in $((batch_index + 1)) batches"
  echo "Batches:"
  for i in "${!batches[@]}"; do
    echo "  $i:"
    sed 's/^/    /' <<< ${batches[$i]}
  done

  # Function to process a single batch
  process_batch() {
    local batch_index="$1"
    local systems="${batches[$batch_index]}"
    local pod_name="system-test-$batch_index-$(date +%s)"
    local error_file="$tempdir/error_batch_$batch_index.log"
    touch "$error_file"

    echo "Starting pod $pod_name to process batch ${batch_index}"

    # Create the pod
    cat <<EOF | kubectl apply -f-
apiVersion: v1
kind: Pod
metadata:
  name: $pod_name
  labels:
    k8s-app: network-validation
  annotations:
    k8s.v1.cni.cncf.io/networks: job-operator/multus-data-net
  namespace: $TEST_NAMESPACE
spec:
  terminationGracePeriodSeconds: 1
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: k8s.cerebras.com/node-role-coordinator
            operator: Exists
  tolerations:
  - effect: NoSchedule
    key: node-role.kubernetes.io/master
    operator: Equal
  - effect: NoSchedule
    key: node-role.kubernetes.io/control-plane
    operator: Equal
  containers:
  - name: test
    image: $cbcore
    command:
    - sleep
    - infinity
    env:
    - name: cerebras_log_level
      value: DEBUG
    securityContext:
      capabilities:
        add:
        - "IPC_LOCK"
    resources:
      limits:
        "rdma/hca_devices": "1"
      requests:
        memory: 192Mi
        cpu: 100m
EOF

    echo "awaiting test pod bring up. This may take up to 5 minutes"

    # Wait for the pod to be ready
    if ! kubectl wait -n "$TEST_NAMESPACE" --for=condition=Ready "pod/$pod_name" --timeout=5m; then
      echo "test pod bring up failed, leaving pod $TEST_NAMESPACE/$pod_name for inspection"
      kubectl get events -n "$TEST_NAMESPACE" --sort-by=.metadata.creationTimestamp | grep "$pod_name"
      return 1
    fi
    echo "test pod $TEST_NAMESPACE/$pod_name running"

    # Process systems in the batch
    for system in $systems ""; do
      # Skip if $system is empty
      [[ -z "$system" ]] && continue

      cm_addr=$(grep -E "^${system}@" "${tempdir}/system_cm.txt" | cut -d@ -f2)
      echo "Processing system $system with cm_addr $cm_addr in batch $batch_index"
      if [[ "$cm_addr" == \~* ]]; then
        echo "cm_addr '$cm_addr' is invalid, skipping"
        echo "$system: System address is invalid" >> "$error_file"
        continue
      fi

      cm_ip=${cm_addr//:*/}
      if ! ping -c 3 -w 10 "$cm_ip"; then
        echo "Error: cannot reach ip $cm_ip for $system"
        echo "$system: Cannot reach system IP" >> "$error_file"
        errors=$((errors + 1))
        continue
      fi

      if $have_image; then
        if $clear_state; then
          if ! kubectl exec -n"$TEST_NAMESPACE" -i "$pod_name" -- bash -c 'cd $(mktemp -d) && timeout -s9 30 config_api -s '$cm_addr' -c clear_state || { cat *.log* && false ; }'; then
            echo "Error: failed to clear state for $system/$cm_addr"
            echo "$system: Error occurred while running config_api clear_state" >> "$error_file"
            errors=$((errors + 1))
            continue
          else
            echo "cleared $system state"
          fi
        fi

        for i in $(seq 1 $num_portgroups); do
          echo "=== Running config_api $i of $num_portgroups times for system [$system] ==="
          if ! kubectl exec -n"$TEST_NAMESPACE" -i "$pod_name" -- bash -c 'cd $(mktemp -d) && timeout -s9 30 config_api -s '$cm_addr' -c config_topo || { cat *.log* && false ; }'; then
            echo "Error: cannot connect via config_api $system/$cm_addr"
            echo "$system: Error occurred while running config_api config_topo" >> "$error_file"
            errors=$((errors + 1))
            break
          fi
        done
      else
        echo "Skipping cm daemon check for $system"
        echo "$system: Skipping cm daemon check because $cbcore image is not available" >> "$error_file"
      fi
    done
  }

  delete_cbcore_pod() {
    kubectl delete ns "$TEST_NAMESPACE" &>/dev/null || :
  }

  # Start processing batches in parallel
  kubectl create namespace "$TEST_NAMESPACE" --dry-run=client -oyaml | kubectl apply -f- &>/dev/null
  pids=()
  for batch_index in "${!batches[@]}"; do
    process_batch "$batch_index" &
    pids+=( "$!" )
  done
  trap delete_cbcore_pod EXIT RETURN

  declare -A error_summary

  # Wait for all parallel jobs to finish and check their exit statuses
  for pid_index in "${!pids[@]}"; do
    if ! wait "${pids[$pid_index]}"; then
      echo "Process batch $pid_index failed execution!"
      for system in ${batches[$pid_index]}; do
        error_summary["$system"]="Background task failed"
      done
    fi
  done

  echo "All batches completed"

  echo "Consolidating error logs..."
  cat "$tempdir"/error_batch_*.log > "$tempdir/consolidated_errors.log"
  rm -f "$tempdir/error_batch_*.log"

  while IFS=: read -r system error_message; do
     # Skip if $system is empty
    [[ -z "$system" ]] && continue
    error_summary["$system"]="$error_message"
  done < "$tempdir/consolidated_errors.log"

  rm $tempdir/consolidated_errors.log

  echo "=== Summary ==="
  echo "Total errors: ${#error_summary[@]}"
  # Summarize errors
  if [ ${#error_summary[@]} -gt 0 ]; then
    echo "Summary of Errors by Type:"
    declare -A error_groups
    for system in "${!error_summary[@]}"; do
      error_type="${error_summary[$system]}"
      error_groups["$error_type"]+="${system} "
    done

    for error_type in "${!error_groups[@]}"; do
      echo "Error Type: $error_type"
      echo "Affected Systems: ${error_groups[$error_type]}"
      echo ""
    done
    return 1
  else
    echo "Validating systems ok"
  fi
}

function pod_address_check {
  if [ -d /kind ]; then
    # kind cluster, pods with host network will have 100G (multus) IPs
    echo "kind cluster detected, skipping pod address check"
    return 0
  fi
  parentnet=$(yq '.v2Groups[0].vlans[0].parentnet // .groups[0].switchConfig.parentnet // ""' ${CLUSTER_CONFIG})
  if [ -z "${parentnet}" ]; then
    echo "Can't find the 100G network (switchConfig.parentnet) from the cluster.yaml file."
    exit 0
  fi

  if ! kubectl get pod -A -ojsonpath="{range .items[*]}{.spec.nodeName},{.metadata.namespace},{.metadata.name},{.status.podIPs[*].ip}{'\n'}" | python3 -c "
import ipaddress as ip, sys
subnet = \"$parentnet\"
for line in sys.stdin:
  node, ns, pod, addr = line.rstrip().split(',')
  if not addr:
    continue
  if ip.ip_address(addr) in ip.ip_network(subnet):
    print(f'Pod {ns}/{pod} on {node} incorrectly assigned to 100G network with address {addr}')
    sys.exit(1)"; then
    echo "Pod IP Address Range Check failed: some pods are on the 100G subnet instead of the expected 1G subnet."
    return 1
  else
    echo "Pod IP Address Range Check success: all pod IP addresses are on the 1G subnet as expected."
    return 0
  fi
}

function die() {
    echo "Fatal error: $*" >&2
    exit 1
}

function show_help() {
  cat <<EOF
Usage: network-validation FLAGS TEST

Validates network configuation of Kubernetes related components.

FLAGS
  -l, --list <filename>    Test only the system names listed in the file.
  -n, --names <list>       Test only the system or node names listed in the comma-separated list.
                           e.g. --names system1,system2. An alternative to passing in a --list file

  -i, --incremental        [Internal flag] Test only the nodes and systems marked as part of an incremental deploy.

  -h, --help               Show this help message and exit.

TEST
  <not specified>     Defaults to running cilium, multus, cs, and pod-address-check

  cilium              Validate cilium install using an offline subset of official cilium CNI suite

  multus              Validate multus (data network) configuration correctness. Opens TCP
                      connections from nginx instances to a temporary test pods with multus
                      interface on each node

  whereabouts         Validate that all IPs in the whereabouts reserved IP range are not reachable
                      if they are not reserved by whereabouts

  cs                  Validate that system control IP is pingable. If a cbcore image is available,
                      a test pod will be started which runs read API commands on the system to
                      validate various control endpoints can interface with pods correctly.
                      Set envvar CBCORE_OVERRIDE to use a non-default cbcore image if needed.

  pod-address-check   Validates that all Pod IPs are assigned to a network range that is not within
                      the data network.
EOF
  exit 0
}

incremental_arg=""
list_arg=""
list_filename=""

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help)
      show_help
      ;;
    -i|--incremental)
      incremental_arg="true"
      shift
      ;;
    -n|--names)
      if [ -z "$2" ]; then
        die "--names flag requires an argument"
      fi
      # Create a temporary file with one name per line
      list_arg="true"
      list_filename=$(mktemp)
      echo "$2" | tr ',' '\n' > "$list_filename"
      shift 2
      ;;
    -l|--list)
      if [ -z "$2" ]; then
        die "--list requires an argument"
      fi
      if [ -n "$list_arg" ]; then
        die "Can't specify both --nodes and --list options"
      fi
      list_arg="true"
      list_filename="$2"
      shift 2
      ;;
    -*)
      die "Unknown option: $1, use --help for options"
      ;;
    *)
      # Not an option; assume positional arguments begin here.
      break
      ;;
  esac
done

TEST="$1"
shift || true

# Validate option usage
if [ -n "$incremental_arg" ] && [ -n "$list_arg" ]; then
  die "Can't specify both -i/--incremental and -l/--list options"
fi

if [ -n "$incremental_arg" ] && [ -z "$INCREMENTAL_DIR" ]; then
  die "-i/--incremental cannot be used outside of an incremental deploy process"
fi

if [ -n "$list_arg" ] && [ ! -f "$list_filename" ]; then
  die "-l/--list file '$list_filename' is not readable"
fi

if [ -n "$1" ]; then
  # Doesn't make a ton of sense to have the flags before the test, this should be cleaned up to
  # have flags which apply to specific tests (--names) only be parsed after the test name
  die "unknown argument after ${TEST} '${1}'. Flags must be specified before the test and only 0 or 1 tests can be specified"
fi

case "$TEST" in
  cilium)
    cilium-validation
    ;;
  multus)
    multus-validation
    ;;
  multus-ips|whereabouts)  # multus-ips is deprecated; use whereabouts instead
    data_ip_reservation_check
    ;;
  netns)
    dangling_netns_check
    ;;
  cs)
    cs-validation
    ;;
  pod-address-check)
    pod_address_check
    ;;
  "" )
    echo "No test given. Running cilium, multus, cs, and pod address validations"
    cilium-validation
    multus-validation
    # data_ip_reservation_check - left off by default
    # dangling_netns_check - left off by default
    cs-validation
    pod_address_check
    ;;
  *)
    echo "undefined test $TEST, options: cilium, multus, whereabouts, netns, cs, pod-address-check"
    exit 1
    ;;
esac
