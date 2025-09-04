# Common functions related to cluster nodes

if [ -n "$INCREMENTAL_DIR" ]; then
  export NODE_LIST="${INCREMENTAL_DIR}/new_all"
  export STORAGE_NODE_LIST="${INCREMENTAL_DIR}/new_storage"
  export MGMT_NODE_LIST="${INCREMENTAL_DIR}/new_mgmt"
  export NOT_BR_NODE_LIST="${INCREMENTAL_DIR}/new_not_br"
  # k8s/setup.sh labels all nodes with mgmt role in yaml/json as coords in k8
  export CRD_NODE_LIST="${INCREMENTAL_DIR}/new_mgmt"
  export WORKER_NODE_LIST="${INCREMENTAL_DIR}/new_worker"
  export UNREACHABLE_LIST="${INCREMENTAL_DIR}/unreachable_ips.list"
else
  export NODE_LIST="${PKG_GEN_PATH}/node_ips.list"
  export STORAGE_NODE_LIST="${PKG_GEN_PATH}/storage_node_ips.list"
  export MGMT_NODE_LIST="${PKG_GEN_PATH}/mgmt_node_ips.list"
  export NOT_BR_NODE_LIST="${PKG_GEN_PATH}/not_br_nodes.list"
  export CRD_NODE_LIST="${PKG_GEN_PATH}/crd_node_ips.list"
  export WORKER_NODE_LIST="${PKG_GEN_PATH}/worker_node_ips.list"
  export UNREACHABLE_LIST="${PKG_GEN_PATH}/unreachable_ips.list"
fi

if [ -x /usr/local/bin/pssh-async ]; then
  PSSH_SCRIPT=/usr/local/bin/pssh-async
else
  PSSH_SCRIPT=/usr/bin/pssh
fi

if [ -x /usr/local/bin/pscp-async ]; then
  PSCP_SCRIPT=/usr/local/bin/pscp-async
else
  PSCP_SCRIPT=/usr/bin/pscp.pssh
fi

declare -A unit_to_bytes
unit_to_bytes["Ki"]=1024
unit_to_bytes["Mi"]=$((1024 * 1024))
unit_to_bytes["Gi"]=$((1024 * 1024 * 1024))
unit_to_bytes["Ti"]=$((1024 * 1024 * 1024 * 1024))
unit_to_bytes["K"]=1000
unit_to_bytes["M"]=$((1000 * 1000))
unit_to_bytes["G"]=$((1000 * 1000 * 1000))
unit_to_bytes["T"]=$((1000 * 1000 * 1000 * 1000))
export unit_to_bytes

# Convert k8s memory resource quantity to bytes
convert_quantity_to_bytes() {
  local input=$1
  local number=${input//[!0-9]/}
  local unit=${input//[0-9]/}

  if [[ -v unit_to_bytes[$unit] ]]; then
    local bytes=$(($number * "${unit_to_bytes[$unit]}"))
    echo "$bytes"
  else
    echo "$number"
  fi
}

function get_node_address_query() {
  jq -r '
    .items[] |
    (.status.addresses// [])[] |
    select(.type == "InternalIP") |
    .address
  '
}

function get_node_ips() {
  # helper function for pssh used like
  # > node_ips
  # > pssh -h node_ips.list ....
  if ! $KUBECTL get po &>/dev/null; then
    yq '.nodes[].name' ${CLUSTER_CONFIG} >${NODE_LIST}
    return 0
  fi
  $KUBECTL get nodes -ojson | get_node_address_query >${NODE_LIST}
}

function get_node_count() {
  local current_count=$(yq '.nodes|length' ${CLUSTER_CONFIG})
  local node_count_override=$(yq '.properties.nodeCount // ""' "${CLUSTER_PROPERTIES}" 2>/dev/null)
  if [ -n "$node_count_override" ] && [ "$node_count_override" -gt "$current_count" ]; then
    echo "Using node count override from cluster properties: $node_count_override (current: $current_count)" >&2
    echo "$node_count_override"
  else
    echo "$current_count"
  fi
}

function get_system_count() {
  local current_count=$(yq '.systems|length' ${CLUSTER_CONFIG})
  local system_count_override=$(yq '.properties.systemCount // ""' "${CLUSTER_PROPERTIES}" 2>/dev/null)
  if [ -n "$system_count_override" ] && [ "$system_count_override" -gt "$current_count" ]; then
    echo "Using system count override from cluster properties: $system_count_override (current: $current_count)" >&2
    echo "$system_count_override"
  else
    echo "$current_count"
  fi
}

function set_pssh_max_threads() {
  node_count=$(get_node_count)
  # by default, one third at a time
  export MAX_PSSH_THREADS=$((node_count / 3))
  if [[ "$MAX_PSSH_THREADS" -lt 10 ]]; then
    export MAX_PSSH_THREADS=10
  fi
}

function get_enable_management_separation() {
  # if "mgmtSeparationEnforced" was not set in an existing cluster properties file, set it to false
  if [ -f "${CLUSTER_PROPERTIES}" ] && [ "$(yq e '.properties.multiMgmtNodes | has("mgmtSeparationEnforced")' "${CLUSTER_PROPERTIES}")" == "false" ]; then
    $YQ -i '.properties.multiMgmtNodes.mgmtSeparationEnforced = false' "${CLUSTER_PROPERTIES}"
  fi
  get_cluster_properties | jq -r '.properties.multiMgmtNodes.mgmtSeparationEnforced // false'
}

function get_node_mgmt_ip() {
  local node=${1-$(hostname)}
  # fallback to local parse for new cluster
  # or compile pod where k8s can be installed on 100g but nginx is only exposing 1g due to legacy reason
  if ! $KUBECTL get po &>/dev/null || ! has_multiple_nodes; then
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=3 "${node}" hostname -i | grep -oE "\b([0-9]{1,3}\.){3}[0-9]{1,3}\b" | head -n1
    return 0
  fi
  $KUBECTL get node "${node}" -o json | jq -r '.status.addresses[] | select(.type == "InternalIP") | .address'
}

function get_mgmt_node_ips() {
  if ! $KUBECTL get po &>/dev/null; then
    yq '.nodes[] | select(.role == "management" or .role == "coordinator") | .name' ${CLUSTER_CONFIG} >${MGMT_NODE_LIST}
    return 0
  fi
  label="k8s.cerebras.com/node-role-management"
  $KUBECTL get nodes -o=json -l${label} --sort-by=.metadata.name | get_node_address_query >${MGMT_NODE_LIST}
  if [ "$(get_enable_management_separation)" == "true" ]; then
    label="k8s.cerebras.com/node-role-coordinator"
    $KUBECTL get nodes -o=json -l${label} --sort-by=.metadata.name | get_node_address_query >>${MGMT_NODE_LIST}
  fi
}

function get_mgmt_node_data_ips() {
  yq -r '.nodes[] | select(.role == "management" or .role == "coordinator") | .networkInterfaces[0].address // ""' ${CLUSTER_CONFIG} | paste -sd, -
}

function get_all_mgmt_nodes() {
  local mgmt
  if [ -n "$INCREMENTAL_DIR" ]; then
    mgmt=$(<$MGMT_NODE_LIST)
    echo $mgmt
  else
    mgmt=$(yq '.nodes[] | select(.role == "management" or .role == "coordinator") | .name' ${CLUSTER_CONFIG})
    if [ -z "$mgmt" ]; then
      # compile pod case
      yq '.nodes[] | select(.role == "any") | .name' ${CLUSTER_CONFIG}
    else
      echo "$mgmt"
    fi
  fi
}

function get_not_br_nodes() {
  yq -r '.nodes[] | select(.role != "broadcastreduce") | .name' ${CLUSTER_CONFIG} >${NOT_BR_NODE_LIST}
}

# has_large_mem_mgmt_nodes return true if there is at least 1 resolvable mgmt node and all
# resolvable mgmt nodes have memory greater than 450Gi
function has_large_mem_mgmt_nodes() {
  local mem nodes mem_threshold
  mem_threshold=$((450 * 1024 * 1024 * 1024))
  nodes=0
  for node in $(get_all_mgmt_nodes); do
    mem=$($KUBECTL get nodes "$node" -ojsonpath='{.status.allocatable.memory}' --ignore-not-found 2>/dev/null)
    if [ -z "$mem" ]; then
      continue
    fi
    mem=$(convert_quantity_to_bytes "$mem")
    if [ "$mem" -lt "$mem_threshold" ]; then
      return 1
    fi
    nodes=$((nodes + 1))
  done
  [ "$nodes" -gt 0 ]
}

function min_group_memx_count() {
  yq . -ojson /opt/cerebras/cluster/cluster.yaml |
    jq -r '[.nodes | group_by(.properties.group)[] | (map(select(.role == "memory")) | length) | select(. > 0)] | min? // 0'
}

# compile pod has only 1 node
function has_multiple_nodes() {
  count=$(yq '.nodes|length' ${CLUSTER_CONFIG})
  if [ "${count}" -gt 1 ]; then
    return 0
  fi
  return 1
}

function has_multiple_mgmt_nodes() {
  [ "$(get_mgmt_node_count)" -gt 1 ]
}

function get_mgmt_node_count() {
  yq '[.nodes[] | select(.role == "management" or .role == "coordinator")] | length' ${CLUSTER_CONFIG}
}

function has_dedicated_crd_nodes() {
  local crd_node_count=$(yq '[.nodes[] | select(.role == "management" and .properties.storage-type != "ceph")] | length' ${CLUSTER_CONFIG})
  has_multiple_mgmt_nodes && [ "$crd_node_count" -gt 0 ]
}

function has_worker_nodes() {
  local worker_node_count=$(yq '[.nodes[] | select(.role == "worker")] | length' ${CLUSTER_CONFIG})
  [ "$worker_node_count" -gt 0 ]
}

function has_br_nodes() {
  local br_node_count=$(yq '[.nodes[] | select(.role == "broadcastreduce")] | length' ${CLUSTER_CONFIG})
  [ "$br_node_count" -gt 1 ]
}

function get_storage_node_ips() {
  if ! $KUBECTL get po &>/dev/null; then
    yq '.nodes[] | select(.properties.storage-type == "ceph") | .name' ${CLUSTER_CONFIG} >${STORAGE_NODE_LIST}
    return 0
  fi
  label="storage-type=ceph"
  $KUBECTL get nodes -l${label} -ojson --sort-by=.metadata.name | get_node_address_query >${STORAGE_NODE_LIST}
}

function get_worker_node_ips() {
  if ! $KUBECTL get po &>/dev/null; then
    yq '.nodes[] | select(.role == "worker") | .name' ${CLUSTER_CONFIG} >${WORKER_NODE_LIST}
    return 0
  fi
  label="k8s.cerebras.com/node-role-worker"
  $KUBECTL get nodes -o=json -l${label} | get_node_address_query >${WORKER_NODE_LIST}
}

function get_crd_node_ips() {
  if ! $KUBECTL get po &>/dev/null; then
    yq '.nodes[] | select(.role == "coordinator") | .name' ${CLUSTER_CONFIG} >${CRD_NODE_LIST}
    return 0
  fi
  label="k8s.cerebras.com/node-role-coordinator"
  $KUBECTL get nodes -o=json -l${label} | get_node_address_query >${CRD_NODE_LIST}
}

function get_crd_node_count() {
  if ! $KUBECTL get po &>/dev/null; then
    yq '.nodes[] | select(.role == "coordinator") | .length' ${CLUSTER_CONFIG}
    return 0
  fi
  label="k8s.cerebras.com/node-role-coordinator"
  $KUBECTL get nodes -l${label} --no-headers | wc -l
}

function get_prom_memory_override() {
  yq '.properties.prometheus.memoryMultiplier // ""' "${CLUSTER_PROPERTIES}" 2>/dev/null
}

function thanos_node_port() {
  yq '.properties.prometheus.thanos.nodePort // ""' "${CLUSTER_PROPERTIES}" 2>/dev/null
}

function thanos_external_store() {
  yq '.properties.prometheus.thanos.extraStore // ""' "${CLUSTER_PROPERTIES}" 2>/dev/null
}

function cluster_has_internet_access() {
  # Not all clusters have worker nodes. For example, inference clusters
  # does not have worker nodes. In the event where worker nodes are not
  # present, we will use the coordinator nodes for image build activities.

  # retry once for intermittent failures
  cmd="ping -c 1 -W 1 pypi.org || ping -c 1 -W 1 pypi.org"
  if has_worker_nodes; then
    PSSH_WORKER $cmd
  else
    PSSH_CRD $cmd
  fi
}

MAX_PSSH_THREADS=${MAX_PSSH_THREADS:-10}

function retry() {
  local attempt max rc
  attempt=0
  max=${MAX_RETRY:-3}
  shift
  while true; do
    "$@" && break
    rc=$?
    attempt=$((attempt + 1))
    if [[ $attempt -gt $max ]]; then
      return 1
    fi
    echo "Command failed with error code: $rc, retry $attempt/$max"
    sleep 1
  done
}

# Even with 10 threads, the "int" error still occurs. This should have been
# fix in versions 2.3.1+ but I still see it in 2.3.1 so maybe it is the python
# version. Probably, the installer itself should run in a container...
# Run with retry as workaround for now
# https://github.com/lilydjwg/pssh/issues/116
# retry for pssh error only with error code 1
# new flakiness error code 5 seen with "kex_exchange_identification: Connection closed by remote host"
# Also see another flaky error code 4 in CI tests. Allow retry for error code 4 as well.
function retry_pssh {
  local attempt max rc
  attempt=0
  max=${MAX_RETRY:-10}

  while true; do
    output=$("$@" 2>&1) && break
    rc=$?
    if [[ $rc -ne 1 && $rc -ne 5 && $rc -ne 4 ]]; then
      echo "$output"
      echo "Failed with error code: $rc, abort as non-retryable"
      return $rc
    fi
    check_node_status || true # logs error if nodes were NotReady
    attempt=$((attempt + 1))
    if [[ $attempt -gt $max ]]; then
      echo "$output"
      echo "Failed with error code: $rc, $max retries attempted"
      return 1
    fi
    echo "Command failed, retry $attempt/$max"
    sleep 1
  done
}

PSSH_HOSTS_WITH_OUTDIR() {
  local hosts=$1
  local outdir=$2
  shift 2
  if [[ ! -s "$hosts" ]]; then
    echo "hostfile $hosts not found or empty, skipping pssh -h $hosts -o $outdir $*"
    return
  fi
  retry_pssh $PSSH_SCRIPT --timeout=300 -o "$outdir" -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h "${hosts}" --inline "$@"
}

PSSH_HOSTS() {
  local hosts=$1
  shift
  if [[ ! -s "$hosts" ]]; then
    echo "hostfile $hosts not found or empty, skipping pssh -h $hosts $*"
    return
  fi
  retry_pssh $PSSH_SCRIPT --timeout=300 -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h "${hosts}" --inline "$@"
}

PSCP_HOSTS() {
  local hosts=$1
  shift
  if [[ ! -s "$hosts" ]]; then
    echo "hostfile $hosts not found or empty, skipping pscp -h $hosts $*"
    return
  fi
  retry_pssh $PSCP_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h "${hosts}" "$@"
}

function PSSH {
  retry_pssh $PSSH_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${NODE_LIST} --inline "$@"
}

function PSCP {
  if [ -s "$NODE_LIST" ]; then
    retry_pssh $PSCP_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${NODE_LIST} "$@"
  fi
}

function PSSH_WORKER {
  retry_pssh $PSSH_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${WORKER_NODE_LIST} --inline "$@"
}

function PSSH_CRD {
  retry_pssh $PSSH_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${CRD_NODE_LIST} --inline "$@"
}

function PSCP_CRD {
  if [ -s "$CRD_NODE_LIST" ]; then
    retry_pssh $PSCP_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${CRD_NODE_LIST} "$@"
  fi
}

function PSSH_STORAGE {
  retry_pssh $PSSH_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${STORAGE_NODE_LIST} --inline "$@"
}

function PSCP_STORAGE {
  if [ -s "$STORAGE_NODE_LIST" ]; then
    retry_pssh $PSCP_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${STORAGE_NODE_LIST} "$@"
  fi
}

function PSSH_MGMT {
  if [[ -s "${MGMT_NODE_LIST}" ]]; then
    retry_pssh $PSSH_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${MGMT_NODE_LIST} --inline "$@"
  fi
}

function PSCP_MGMT {
  if [ -s "$MGMT_NODE_LIST" ]; then
    retry_pssh $PSCP_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${MGMT_NODE_LIST} "$@"
  fi
}

function PSSH_NOT_BR {
  retry_pssh $PSSH_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${NOT_BR_NODE_LIST} --inline "$@"
}

function PSCP_NOT_BR {
  if [ -s "$NOT_BR_NODE_LIST" ]; then
    retry_pssh $PSCP_SCRIPT -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no' -h ${NOT_BR_NODE_LIST} "$@"
  fi
}

# This is only for debugging purpose.
# if you need to ssh by name on customer side, you can run this function to add the hosts resolving
# note: this may change the default ip resolving if default host ip is different than k8s node ip
function add_hosts_dns {
  ETC_HOSTS=$($KUBECTL get nodes -ojsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address} {.metadata.name}{"\n"}{end}' | column -t -s' ' | sort)
  echo "$ETC_HOSTS" >/tmp/k8s_etc_hosts

  cp /etc/hosts /tmp/hosts
  while read -r line; do
    # remove existing entry for this line
    IP="${line%% *}"
    HOSTNAME="${line##* }"
    sed -i "/^$IP.*/d" /tmp/hosts
    sed -i "/^.*$HOSTNAME\$/d" /tmp/hosts

    echo "$line" >>/tmp/hosts
  done </tmp/k8s_etc_hosts
  $CP /etc/hosts /etc/hosts.bak
  $CP /tmp/hosts /etc/hosts
}

function await_condition() {
  local duration=$1
  local timeout_time=$((duration + SECONDS))
  shift
  while ! $@; do
    if [ "$SECONDS" -gt "$timeout_time" ]; then
      echo "timed out after $duration seconds waiting for successful condition: $@"
      return 1
    fi
    sleep 1
  done
}

get_ds_max_unavailable() {
  local total_nodes=$(get_node_count)
  local twenty_percent=$(echo "scale=0; 0.20 * $total_nodes / 1" | bc)
  local batch_size=100
  # We allow at most min(20%, 100) pods being unavailable
  # If 20% is 0 (too few nodes), we will use batch size of 1
  if [ $twenty_percent -eq 0 ]; then
    batch_size=1
  elif [ $twenty_percent -lt $batch_size ]; then
    batch_size=$twenty_percent
  fi

  num_batches=$(echo "$total_nodes / $batch_size" | bc)
  echo "$batch_size $num_batches"
}

function get_unreachable_nodes() {
  if [ -z "$1" ]; then
    # Always take the full list, even if it's an incremental install, so we can skip testing down nodes
    node_list="${PKG_GEN_PATH}/node_ips.list"
  else
    node_list=$1
  fi

  rm -fr .ssh-check/
  ${PSSH_SCRIPT} -p "${MAX_PSSH_THREADS}" -x '-o StrictHostKeyChecking=no -o ConnectTimeout=3' -h ${node_list} -e .ssh-check/ ls >/dev/null || true
  # Can't use a simple ls - sometimes pssh will create an empty error file when the connection succeeded
  # Only consider a file with content (generally a ssh timeout message) as proof of a problem
  find .ssh-check/ -type f -size +0 -printf "%P\n" | sort >${UNREACHABLE_LIST}
}

# Whether a given package has been copied to the node running this script.
function package_copied() {
  local expected_hash="$1"
  local local_path="$2"
  local local_hash=""

  if [ -e "$local_path" ]; then
    local_hash=$(find "${local_path}" -type f -exec sha256sum {} \; 2>/dev/null | awk '{print $1}' | sort | sha256sum | awk '{print $1}')
  fi

  if [[ "$local_hash" != "$expected_hash" ]]; then
    return 1
  else
    return 0
  fi
}

# Check if the artifacts in the given src_path exists in dest_path on all nodes specified in node_list file, and
# copy the artifacts if the nodes do not have them. If some nodes already have the artifacts, they are skipped.
check_and_copy() {
  # src_path/dest_path could be a file or a directory.
  local src_path=$1
  local dest_path=$2
  local node_list=$3
  local nodes_sh_file=$4
  local tmp_path="/tmp/pb3"
  if [ -d /kind ]; then
    tmp_path="/home/pb3"
  fi

  local staging_dir="$tmp_path/staging"
  local src_hash=$(find "$src_path" -type f -exec sha256sum {} \; 2>/dev/null | awk '{print $1}' | sort | sha256sum | awk '{print $1}')

  mkdir -p "$staging_dir/pssh_logs"
  rm -rf "$staging_dir/pssh_logs/"
  rm -f "$staging_dir/nodes_to_copy.list"

  PSSH_HOSTS "$node_list" "mkdir -p $tmp_path"
  PSCP_HOSTS "$node_list" "$nodes_sh_file" "$tmp_path/nodes.sh"
  PSSH_HOSTS_WITH_OUTDIR "${node_list}" "$staging_dir/pssh_logs" "source $tmp_path/nodes.sh; if ! package_copied ${src_hash} $dest_path; then find_min_max_speed_nic max; fi"
  for node in $(ls "$staging_dir/pssh_logs"); do
    if [ -s "$staging_dir/pssh_logs/$node" ]; then
      cat "$staging_dir/pssh_logs/$node" >>"$staging_dir/nodes_to_copy.list"
    fi
  done

  local dest_dir=$(dirname "$dest_path")
  if [[ -s "$staging_dir/nodes_to_copy.list" ]]; then
    PSSH_HOSTS "$staging_dir/nodes_to_copy.list" "mkdir -p $dest_dir"
    PSCP_HOSTS "$staging_dir/nodes_to_copy.list" -r "$src_path" "$dest_dir"
    echo "copying ${src_path} to ${dest_path} complete"
  fi
}

find_min_max_speed_nic() {
  # Find either the min and max speed NICs on the node.
  local min_or_max=${1:-"max"} # "max" or "min"

  local max_speed=0
  local max_ip=""

  local min_speed=$((2 ** 63 - 1))
  local min_ip=""

  for iface in $(ls /sys/class/net | egrep -v 'lo|idrac'); do
    # Check if interface is physical (has a /device directory)
    if [ -d "/sys/class/net/$iface/device" ] || [[ "$iface" =~ ^bond[0-9]+ ]] || [ -d /kind ]; then
      # Check if link is detected (alive)
      if ethtool "$iface" 2>/dev/null | grep -q "Link detected: yes"; then
        # Get speed (in Mb/s)
        local speed=$(ethtool "$iface" 2>/dev/null | awk '/Speed:/ {gsub(/[^0-9]/,"",$2); print $2}')
        # Find the IP
        local node_ip=$(ip -j -4 addr show "$iface" | jq -r '.[0].addr_info[0].local')
        if [[ -n "$speed" && "$speed" -gt "$max_speed" && -n "$node_ip" ]]; then
          max_speed=$speed
          max_ip="$node_ip"
        fi
        if [[ -n "$speed" && "$speed" -lt "$min_speed" && -n "$node_ip" ]]; then
          min_speed=$speed
          min_ip="$node_ip"
        fi
      fi
    fi
  done

  if [ "$min_or_max" == "max" ]; then
    if [ -z "$max_ip" ]; then
      echo "No active network interfaces found."
      return 1
    fi
    echo "$max_ip"
  else
    if [ -z "$min_ip" ]; then
      echo "No active network interfaces found."
      return 1
    fi
    echo "$min_ip"
  fi
}

# Control-plane node check helpers
function get_hostname() {
  hostname -s
}

function get_all_ctrl_plane_nodes() {
  if kubectl get po >/dev/null 2>&1; then
    kubectl get nodes -lnode-role.kubernetes.io/control-plane= --no-headers
  fi
}

function is_current_node_control_plane() {
  if get_all_ctrl_plane_nodes | grep -wq "$(get_hostname)"; then
    echo true
  else
    echo false
  fi
}

function _check_control_plane_node() {
  local attempts=5 sleep_secs=2
  for i in $(seq 1 $attempts); do
    if is_current_node_control_plane | grep -q "true"; then
      return 0
    fi
    echo "Control-plane check failed (attempt $i/$attempts), in case of api-server transient error, retrying in ${sleep_secs}s..."
    sleep $sleep_secs
  done
  echo "ERROR: This command must be run on a control plane management node."
  echo "Current node '$(get_hostname)' is not identified as a control plane management node."
  echo "Unable to verify control-plane role after ${attempts} attempts."

  exit 1
}
