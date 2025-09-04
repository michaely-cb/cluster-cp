# Common functions related to cluster config and cluster properties

export CFG_DIR="/opt/cerebras"
export PKG_DIR="${CFG_DIR}/packages"
export TOOLS_DIR="${CFG_DIR}/tools"
export CERTS_DIR="${CFG_DIR}/certs"
export CONFIG_DIR="${CFG_DIR}/cluster"
export CLUSTER_CONFIG="${CFG_DIR}/cluster/cluster.yaml"
export NETWORK_CONFIG="${CFG_DIR}/cluster/network_config.json"
export CLUSTER_PROPERTIES="${CFG_DIR}/cluster/pkg-properties.yaml"
export CLUSTER_PROPERTIES_TEMPLATE="cluster-pkg-properties.yaml.template"
export CLEANUP_SCRIPTS_DIR="/n1/cluster-mgmt/cleanup"
# todo: remove all occurrences of CTR, use nerdctl
export CTR="$(which sudo) $(which ctr)"
export MGMT_NODE_NAME=$(hostname)
# todo: populate the updated system namespace
export SYSTEM_NAMESPACE="job-operator"
export SYSTEM_NAMESPACE_LABEL="system-namespace"
export USER_NAMESPACE_LABEL="user-namespace"

function update_cluster_cm() {
  local verb="create"
  if kubectl -n${SYSTEM_NAMESPACE} get cm cluster &>/dev/null; then
    verb="replace"
  fi
  kubectl create cm cluster -n${SYSTEM_NAMESPACE} \
    --from-file=clusterConfiguration.yaml=${CLUSTER_CONFIG} --dry-run=client -o yaml | kubectl ${verb} -f-
}

function check_node_status() {
  # Check the state of the nodes - if there are some nodes in Ready state and some in NotReady,
  # add a failure - if all nodes are not ready, ignore since it might be the initial installation
  # where no nodes are ready since cilium is not installed yet.
  local nodestate=$($KUBECTL get nodes -o json | jq '{
    readyNodes: [.items[] | select(.status.conditions[] | select(.type == "Ready" and .status == "True")) | .metadata.name],
    notReadyNodes: [.items[] | select(.status.conditions[] | select(.type == "Ready" and .status != "True")) | .metadata.name]
  }')
  if [ "$(echo "$nodestate" | jq '.readyNodes | length')" != "0" ] &&
    [ "$(echo "$nodestate" | jq '.notReadyNodes | length')" != "0" ]; then
    echo "some nodes are not in Ready state, check 'kubectl get nodes'" >&2
    return 1
  fi
}

# _validate_cilium_ranges
# Compare the cilium ranges in the given property file with the current cilium config.
# If they don't match, print an error message and return 1.
function _validate_cilium_ranges() {
  new_cidr=""
  new_mask_size=""
  if [ -f "${CLUSTER_PROPERTIES}" ]; then
    new_cidr=$($YQ "${CLUSTER_PROPERTIES}" -ojson | jq -r '.properties.cilium.clusterPoolIPv4PodCIDRList // empty | join(" ")')
    new_mask_size=$($YQ "${CLUSTER_PROPERTIES}" -ojson | jq -r '.properties.cilium.clusterPoolIPv4MaskSize // empty')
  fi

  if $KUBECTL get cm cilium-config -n kube-system &>/dev/null; then
    cidr=$($KUBECTL get cm cilium-config -o=jsonpath='{.data.cluster-pool-ipv4-cidr}' -n kube-system)
    mask_size=$($KUBECTL get cm cilium-config -o=jsonpath='{.data.cluster-pool-ipv4-mask-size}' -n kube-system)
    if [ -n "${new_cidr}" ] && [ "${cidr}" != "${new_cidr}" ]; then
      echo "Error: cilium configuration mismatch for property file ${CLUSTER_PROPERTIES}" >&2
      echo "cilium cidr range mismatch: ${new_cidr} (expected ${cidr})" >&2
      return 1
    fi
    if [ -n "${new_mask_size}" ] && [ "${mask_size}" != "${new_mask_size}" ]; then
      echo "Error: cilium configuration mismatch for property file ${CLUSTER_PROPERTIES}" >&2
      echo "cilium cidr mask size mismatch: ${new_mask_size} (expected ${mask_size})" >&2
      return 1
    fi
    # use existing cilium range if no conflicts
    $TOUCH "${CLUSTER_PROPERTIES}" &&
      cidr_list=$(echo \""$cidr"\" | jq 'split(" ")') &&
      $YQ e -i ".properties.cilium.clusterPoolIPv4PodCIDRList = ${cidr_list}" "${CLUSTER_PROPERTIES}" &&
      $YQ e -i ".properties.cilium.clusterPoolIPv4MaskSize = ${mask_size}" "${CLUSTER_PROPERTIES}"
  fi
}

function get_templated_properties_filepath() {
  script_path=$(realpath "${BASH_SOURCE[0]}")
  script_dir=$(dirname "$script_path")
  if [ -f "${script_dir}/../${CLUSTER_PROPERTIES_TEMPLATE}" ]; then
    realpath "${script_dir}/../${CLUSTER_PROPERTIES_TEMPLATE}"
  else
    echo "skip not found ${CLUSTER_PROPERTIES_TEMPLATE}"
  fi
}

# usage: merge_cluster_properties BASE OUTPUT
# Merge properties from BASE into OUTPUT without overwrite
# Keep the original properties in OUTPUT if BASE conflicts
function merge_cluster_properties() {
  local base=$1
  local output=$2

  if ! [ -f "${output}" ]; then
    if ! [ -f "${base}" ]; then
      echo "Base file ${base} doesn't exist, can't merge to ${output}"
      return 1
    fi
    $CP "${base}" "${output}"
    return
  elif ! [ -f "${base}" ]; then
    return
  fi

  # merge base into output without overwrite
  $YQ eval-all --inplace 'select(fileIndex == 1) * select(fileIndex == 0)' "${output}" "${base}"
}

# merges defaults in template pkg-properties file with local pkg-properties file and validates output
function init_cluster_properties() {
  # merge default internal properties for colo only
  if [ "${CEREBRAS_INTERNAL_DEPLOY}" = "true" ]; then
    merge_cluster_properties "$PKG_COMMON_FULL_PATH/internal-pkg-properties.yaml" "${CLUSTER_PROPERTIES}"
  fi

  # merge default properties for all clusters
  templated_prop_filepath=$(get_templated_properties_filepath)
  merge_cluster_properties "${templated_prop_filepath}" "${CLUSTER_PROPERTIES}"

  if [ ! -f "${CLUSTER_PROPERTIES}" ]; then
    echo "File ${CLUSTER_PROPERTIES} does not exist. Example content can be found in ${templated_prop_filepath}" >&2
    return 1
  fi

  if has_multiple_mgmt_nodes; then
    required_props=()
    if [ ! -d /kind ]; then
      required_props+=("dataVip")
    fi
    required_props+=("pvcSize.registry")
    required_props+=("pvcSize.logExport")
    required_props+=("pvcSize.loki")
    required_props+=("pvcSize.cachedCompileSystem")
    required_props+=("pvcSize.cachedCompileUser")
    for prop in "${required_props[@]}"; do
      resp=$($YQ -e '.properties.multiMgmtNodes.'"$prop" <"${CLUSTER_PROPERTIES}" 2>/dev/null || true)
      if [ "$resp" == "null" ]; then
        echo "Fatal error: ${CLUSTER_PROPERTIES} missing required field of properties.multiMgmtNodes.$prop" >&2
        return 1
      elif [ -f "$templated_prop_filepath" ] && [[ "$prop" == "pvcSize"* ]]; then
        local prop_numeric_part prop_unit_part
        read prop_numeric_part prop_unit_part <<<$(get_resource_quantity "$resp")

        # For get_xx_pvc_size
        # Retrieves the PVC size from the cluster properties directly.
        # Uses the base capacity from the cluster's templated properties, defaulting to "xxxGi" if not set.

        # For _get_scaled_xxx_pvc_size
        # Calculates the scaled PVC size based on the cluster size.
        local expected_pvc_size expected_numeric_part expected_unit_part
        case "$prop" in
          pvcSize.registry)
            expected_pvc_size=$(_get_scaled_registry_pvc_size)
            ;;
          pvcSize.logExport)
            expected_pvc_size=$(_get_scaled_log_export_pvc_size)
            ;;
          pvcSize.loki)
            expected_pvc_size=$(_get_scaled_loki_pvc_size)
            ;;
          pvcSize.cachedCompileSystem)
            expected_pvc_size=$(_get_scaled_cached_compile_system_pvc_size)
            ;;
          pvcSize.cachedCompileUser)
            expected_pvc_size=$(_get_scaled_cached_compile_user_pvc_size)
            ;;
        esac

        read expected_numeric_part expected_unit_part <<<$(get_resource_quantity "$expected_pvc_size")
        read prop_numeric_part prop_unit_part <<<$(get_resource_quantity "$resp")

        if [ "$prop_unit_part" != "$expected_unit_part" ]; then
          echo "Unit of properties.multiMgmtNodes.$prop ($prop_unit_part) in ${CLUSTER_PROPERTIES} is different from ($expected_unit_part) in ${templated_prop_filepath}." >&2
          echo "This could be caused by improper update of the ${CLUSTER_PROPERTIES} file. Please update to use the same unit before proceeding." >&2
          exit 1
        elif [ "$prop_numeric_part" -lt "$expected_numeric_part" ]; then
          echo "WARNING: Updated .properties.multiMgmtNodes.$prop from $resp to $expected_pvc_size"
          $YQ -i ".properties.multiMgmtNodes.$prop = \"$expected_numeric_part$expected_unit_part\"" "${CLUSTER_PROPERTIES}"
        fi
      fi
    done
  elif [ ! -d /kind ]; then
    $YQ eval 'del .properties.multiMgmtNodes' --inplace "${CLUSTER_PROPERTIES}"
  fi

  if has_system_type "cs3"; then
    if [ "$(min_group_memx_count)" -eq 12 ]; then
      # in V2 networking, there is no memx nodes in depop groups
      if $YQ -e '.properties.clusterMgmt | has("resourceDetailsMemxSecondaryGroupCount")' "${CLUSTER_PROPERTIES}" &>/dev/null; then
        $YQ eval -i 'del(.properties.clusterMgmt.resourceDetailsMemxSecondaryGroupCount)' "${CLUSTER_PROPERTIES}"
      fi
    elif [ "$(min_group_memx_count)" -lt 12 ]; then
      # As of rel-2.4, this branch is only for backward compatibility handling.
      # Prior to rel-2.4, runtime had always used "smallestMemorySecondaryNodeGroup" for activation memory checks during compile.
      # This decision unnecessarily tightened the memory constraints for single-box jobs, due to less number of nodes in depop nodegroups.
      # This branch was not removed only because if we had removed the override value, the activation memory checks would be further
      # tightened - 4 nodes instead the current 8, which would lead to wide-spread confusion.
      # TODO: Update this branch to remove the hardcoded value in cluster properties in rel-2.5
      #
      # legacy note:
      # override secondary (depop) group count statically to 8 for cs3 clusters
      # once cluster-mgmt supports 2 NIC setup on depop group, the memx count for cs3
      # will revert to 4
      if ! $YQ -e '.properties.clusterMgmt | has("resourceDetailsMemxSecondaryGroupCount")' "${CLUSTER_PROPERTIES}" &>/dev/null; then
        $YQ -i '.properties.clusterMgmt.resourceDetailsMemxSecondaryGroupCount = 8' "${CLUSTER_PROPERTIES}"
      fi
    fi
  fi

  # override default compile / execute memory for clusters which have large
  # memory mgmt nodes
  if has_large_mem_mgmt_nodes; then
    if ! $YQ -e '.properties.resources | has("crdCompileMinMemoryGi")' "${CLUSTER_PROPERTIES}" &>/dev/null; then
      $YQ -i '.properties.resources.crdCompileMinMemoryGi = 90' "${CLUSTER_PROPERTIES}"
    fi
    if ! $YQ -e '.properties.resources | has("crdExecuteMinMemoryGi")' "${CLUSTER_PROPERTIES}" &>/dev/null; then
      $YQ -i '.properties.resources.crdExecuteMinMemoryGi = 0.05' "${CLUSTER_PROPERTIES}"
    fi
  fi

  # override default compile / execute memory for kind testing, kept low for OOM e2e tests
  if [ -d "/kind" ]; then
    if ! $YQ -e '.properties.resources | has("crdCompileMinMemoryGi")' "${CLUSTER_PROPERTIES}" &>/dev/null; then
      $YQ -i '.properties.resources.crdCompileMinMemoryGi = 0.05' "${CLUSTER_PROPERTIES}"
    fi
    if ! $YQ -e '.properties.resources | has("crdExecuteMinMemoryGi")' "${CLUSTER_PROPERTIES}" &>/dev/null; then
      $YQ -i '.properties.resources.crdExecuteMinMemoryGi = 0.05' "${CLUSTER_PROPERTIES}"
    fi
  fi
}

function get_cluster_config() {
  local config name tmpconfig
  config=$($KUBECTL get configmap -n ${SYSTEM_NAMESPACE} cluster -ojsonpath='{.data.clusterConfiguration\.yaml}')
  if [ -n "${config}" ]; then
    echo "Using cluster config from kubernetes" >&2
  else
    echo "Error: cluster config not found, is kubernetes installed & cluster initialized?" >&2
    return 1
  fi

  config=$(echo "${config}" | $YQ -o=json)
  name=$(echo "${config}" | jq -r '.name // empty')
  if [ -z "${name}" ]; then
    echo "Error: cluster config is invalid, field 'name' was not present" >&2
    echo "Cluster config: ${config}" >&2
    return 1
  fi
  mkdir -p ${CONFIG_DIR}
  tmpconfig=$(mktemp)
  echo "${config}" | $YQ -P >${tmpconfig}
  chmod a+r ${tmpconfig}
  $MV -f ${tmpconfig} ${CLUSTER_CONFIG}
}

# Properties that are relevant to a particular cluster. This is separate from cluster configs, since cluster configs
# could potentially generate ahead of time, while the properties are specific to each customer, and might not be available
# before hand.
function get_cluster_properties() {
  $YQ -ojson ${CLUSTER_PROPERTIES}
}

function get_cluster_templated_properties() {
  local fp=$(get_templated_properties_filepath)
  if ! [ -f "${fp}" ]; then
    echo "{}"
  else
    $YQ -ojson "${fp}"
  fi
}

function subnet_contains_ip() {
  local subnet="$1"
  local ip="$2"

  cat <<EOF | python3
import ipaddress as ip, sys

if ip.ip_address("$ip") not in ip.ip_network("$subnet"):
  sys.exit(1)
EOF
}

# Checks if coordinator/worker nodes can reach the internet and disable/enable image build accordingly
function update_image_build_status() {
  local comment
  local status
  local rc
  if cluster_has_internet_access; then
    echo "Image build has been enabled"
    status=false
    comment="Enabled by pkg-functions/cluster.sh::update_image_build_status since cluster had internet access"
  else
    echo "Image build has been disabled"
    status=true
    comment="Disabled by pkg-functions/cluster.sh::update_image_build_status since cluster did not have internet access"
  fi
  $YQ -i ".properties.customWorker.disabled = ${status}" "${CLUSTER_PROPERTIES}"
  $YQ -i ".properties.customWorker.disabled line_comment=\"${comment}\"" "${CLUSTER_PROPERTIES}"
}

function get_disable_custom_worker() {
  get_cluster_properties | jq -r '.properties.customWorker.disabled // "false"'
}

function get_disable_fabric_json_check() {
  get_cluster_properties | jq -r '.properties.clusterServer.disableFabricJsonCheck // "false"'
}

# disabling system namespace webhook can be done on a per-cluster basis depending on special scenarios (perfdrop, etc)
function get_disable_system_namespace_webhook() {
  local val=$(get_cluster_properties | jq -r '.properties.kubewebhook.disableSystemNamespaceWebhook // false')
  [ "$val" == "true" ]
}

function get_system_count() {
  $YQ -r '.systems | length // 0' ${CLUSTER_CONFIG}
}

function get_system_count_for_session() {
  local session=$1
  if [ "$session" != "$SYSTEM_NAMESPACE" ]; then
    kubectl get nsr "${session}" -ojson | jq '.status.systems | length'
  else
    kubectl get systems --no-headers | wc -l
  fi
}

function get_controlplane_data_ips() {
  for node in $($KUBECTL get nodes -lnode-role.kubernetes.io/control-plane="" -oname | cut -d/ -f2); do
    $YQ -r '.nodes[] | select(.name == "'$node'") | .networkInterfaces[0].address // ""' ${CLUSTER_CONFIG}
  done
}

function get_min_memx_per_pop_group() {
  get_cluster_properties | jq -r '.properties.clusterMgmt.minMemxPerPopulatedGroup // 10'
}

function get_memx_primary_group_count() {
  get_cluster_properties | jq -r '.properties.clusterMgmt.resoureDetailsMemxPrimaryGroupCount // 12'
}

function get_memx_secondary_group_count() {
  get_cluster_properties | jq -r '.properties.clusterMgmt.resourceDetailsMemxSecondaryGroupCount // 4'
}

function get_job_operator_namespace_scale_down_check_interval() {
  get_cluster_properties | jq -r '.properties.jobOperator.namespaceScaleDownCheckInterval // "1h"'
}

function get_data_vip() {
  get_cluster_properties | jq -r '.properties.multiMgmtNodes.dataVip // empty'
}

function get_resource_quantity() {
  local resource_quantity="$1"
  local numeric_part=$(echo "$resource_quantity" | sed 's/\([0-9]*\).*/\1/')
  local unit_part=$(echo "$resource_quantity" | sed 's/^[0-9]*//')

  echo "$numeric_part $unit_part"
}

# get scaled capacity based on sys count
# [1, 64] -> 1
# [65, 80] -> 1.25
# [81, 96] -> 1.5
# ....
function get_cluster_size_scaled_capacity() {
  local base_capacity="$1"
  local numeric_part unit_part
  read numeric_part unit_part <<<$(get_resource_quantity "$base_capacity")

  local sys_count=$(get_system_count)
  if [ "$sys_count" -le 64 ]; then
    multiply_factor=1
  else
    multiply_factor=$(echo "1+(($sys_count-65)/16 +1)*0.25" | bc)
  fi
  # return ceiling integer, 6.1=>7, 6.9=>7
  local scaled_numeric_part=$(echo "($numeric_part*$multiply_factor+0.99)/1" | bc)
  echo "$scaled_numeric_part$unit_part"
}

function get_apiserver_maxRequestsInflight() {
  local base_capacity=$(get_cluster_properties | jq -r '.properties.kubeApiServer.maxRequestsInflight // empty')
  if [ -z "$base_capacity" ]; then
    get_cluster_size_scaled_capacity "800"
  else
    echo "$base_capacity"
  fi
}

function get_apiserver_maxMutatingRequestsInflight() {
  local base_capacity=$(get_cluster_properties | jq -r '.properties.kubeApiServer.maxMutatingRequestsInflight // empty')
  if [ -z "$base_capacity" ]; then
    get_cluster_size_scaled_capacity "400"
  else
    echo "$base_capacity"
  fi
}

function get_cp_reserved_mem() {
  local base_capacity=$(get_cluster_properties | jq -r '.properties.controlplane.reservedMemory // empty')
  if [ -z "$base_capacity" ]; then
    if [ -d /kind ] || ! has_multiple_mgmt_nodes; then
      echo "1Gi"
      return 0
    fi
    base_capacity="10Gi"
    get_cluster_size_scaled_capacity "$base_capacity"
  else
    echo "$base_capacity"
  fi
}

function get_cp_reserved_cpu() {
  local base_capacity=$(get_cluster_properties | jq -r '.properties.controlplane.reservedCpu // empty')
  if [ -z "$base_capacity" ]; then
    if [ -d /kind ] || ! has_multiple_mgmt_nodes; then
      echo "500m"
      return 0
    fi
    base_capacity="3"
    get_cluster_size_scaled_capacity "$base_capacity"
  else
    echo "$base_capacity"
  fi
}

function get_registry_pvc_size() {
  get_cluster_properties | jq -r '.properties.multiMgmtNodes.pvcSize.registry // "200Gi"'
}

function _get_scaled_registry_pvc_size() {
  local base_capacity=$(get_cluster_templated_properties | jq -r '.properties.multiMgmtNodes.pvcSize.registry // "200Gi"')
  get_cluster_size_scaled_capacity $base_capacity
}

function get_system_cached_compile_pvc_size() {
  get_cluster_properties | jq -r '.properties.multiMgmtNodes.pvcSize.cachedCompileSystem // "1000Gi"'
}

function _get_scaled_cached_compile_system_pvc_size() {
  local base_capacity=$(get_cluster_templated_properties | jq -r '.properties.multiMgmtNodes.pvcSize.cachedCompileSystem // "1000Gi"')
  get_cluster_size_scaled_capacity $base_capacity
}

function get_user_cached_compile_pvc_size() {
  get_cluster_properties | jq -r '.properties.multiMgmtNodes.pvcSize.cachedCompileUser // "20Gi"'
}

function _get_scaled_cached_compile_user_pvc_size() {
  local base_capacity=$(get_cluster_templated_properties | jq -r '.properties.multiMgmtNodes.pvcSize.cachedCompileUser // "20Gi"')
  get_cluster_size_scaled_capacity $base_capacity
}

function get_kafka_pvc_size() {
  local base_capacity=$(get_cluster_templated_properties | jq -r '.properties.kafka.pvcSize // "10Gi"')
  get_cluster_size_scaled_capacity $base_capacity
}

function get_loki_pvc_size() {
  get_cluster_properties | jq -r '.properties.multiMgmtNodes.pvcSize.loki // "200Gi"'
}

function _get_scaled_loki_pvc_size() {
  local base_capacity=$(get_cluster_templated_properties | jq -r '.properties.multiMgmtNodes.pvcSize.loki // "200Gi"')
  get_cluster_size_scaled_capacity $base_capacity
}

function get_loki_retention_period() {
  get_cluster_properties | jq -r '.properties.loki.retention // "7d"'
}

function get_log_export_pvc_size() {
  get_cluster_properties | jq -r '.properties.multiMgmtNodes.pvcSize.logExport // "200Gi"'
}

function _get_scaled_log_export_pvc_size() {
  local base_capacity=$(get_cluster_templated_properties | jq -r '.properties.multiMgmtNodes.pvcSize.logExport // "200Gi"')
  get_cluster_size_scaled_capacity $base_capacity
}

function get_debug_artifact_pvc_size() {
  get_cluster_properties | jq -r '.properties.multiMgmtNodes.pvcSize.debugArtifact // "640Gi"'
}

function _get_scaled_debug_artifact_pvc_size() {
  local base_capacity=$(get_cluster_templated_properties | jq -r '.properties.multiMgmtNodes.pvcSize.debugArtifact // "640Gi"')
  get_cluster_size_scaled_capacity $base_capacity
}

function get_crd_compile_min_gi() {
  get_cluster_properties | jq -r '.properties.resources.crdCompileMinMemoryGi // "none"'
}

function get_crd_execute_min_gi() {
  get_cluster_properties | jq -r '.properties.resources.crdExecuteMinMemoryGi // "none"'
}

function get_num_ceph_nodes() {
  get_cluster_properties | jq -r '.properties.multiMgmtNodes.numCephNodes // 4'
}

function get_cilium_agent_mem_mi() {
  get_cluster_properties | jq -r '.properties.cilium.agentMemMi // 1000'
}

function get_num_kafka_nodes() {
  if has_multiple_mgmt_nodes; then
    get_cluster_properties | jq -r '.properties.kafka.numNodes // 3'
  else
    get_cluster_properties | jq -r '.properties.kafka.numNodes // 1'
  fi
}

function get_node_min_reserved_mem() {
  get_cluster_properties | jq -r '.properties.resources.nodeMinReservedMem // "5Gi"'
}

function get_disable_inference_job_sharing() {
  get_cluster_properties | jq -r '.properties.jobOperator.inferenceJobSharingDisabled // "false"'
}

function get_kafka_replication_factor() {
  local num_nodes=$(get_num_kafka_nodes)
  default_replication_factor=3
  if ((num_nodes < default_replication_factor)); then
    echo ${num_nodes}
  else
    echo ${default_replication_factor}
  fi
}

# todo: integrate with password update flow
function get_admin_svc_user() {
  get_cluster_properties | jq -r '.properties.adminSvc.user // "admin"'
}

function get_admin_svc_password() {
  get_cluster_properties | jq -r '.properties.adminSvc.password // "admin"'
}

function get_system_export_dev_metrics() {
  get_cluster_properties | jq -r '.properties.systemExporter.devMetrics // "true"'
}

function get_ipmi_metrics_user() {
  get_cluster_properties | jq -r '.properties.ipmi.metricsUser // "metrics"'
}

function get_ipmi_metrics_password() {
  get_cluster_properties | jq -r '.properties.ipmi.metricsPassword // "Metrics123"'
}

function get_max_train_fabric_freq_mhz() {
  get_cluster_properties | jq -r '.properties.clusterServer.maxTrainFabricFreqMhz // "900"'
}

function get_nfs_server_address() {
  # Defaulting to empty string, because we don't want it to be deployed on customer sites
  get_cluster_properties | jq -r '.properties.clusterMgmt.nfsServer.address // empty'
}

function get_additional_ingress() {
  get_cluster_properties | jq -r '.properties.clusterMgmt.additionalIngress // ""'
}

function netapp_is_available {
  df -PT /cb/tests/cluster-mgmt 2>/dev/null | grep -q "nfs"
}

function cblocal_is_available {
  df -PT /cblocal 2>/dev/null | grep -q "nfs"
}

function is_genoa_server {
  # AMD EPYC 9354P 3.25GHz, 32C/64T, 256M
  # Cache (280W) DDR5-4800
  if lscpu | grep -q "EPYC 9"; then
    return 0
  fi
  # default to milan
  # AMD EPYC 7713P 64-Core Processor AMD EPYC 7713P 64-Core Processor
  # 2.00GHz, 64C/128T, 32768K Cache (3200 MT/s Multi-bit ECC)
  return 1
}

# return data VIP if found or data ip
get_mgmt_data_ip() {
  local mgmt_ip=${MGMT_NODE_DATA_IP}

  if { has_multiple_mgmt_nodes && has_data_network; } || [ -d /kind ]; then
    mgmt_ip="$(get_data_vip)"
    if [ -z "$mgmt_ip" ]; then
      echo "warning: no vip configured for multi-mgmt node cluster, fall back to mgmt data IP" >&2
      mgmt_ip=${MGMT_NODE_DATA_IP}
    fi
  fi

  echo "$mgmt_ip"
}

# in namespace $1, await daemonset named $2 to be ready for $3 seconds max
# if new arg4 is notready_ok, tolerate notready nodes by ending the wait if the count of
#   ready pods + notready nodes >= expected DS size
# if new arg4 is unreachable_ok, tolerate unreachable nodes instead
# (use 'notready_ok' for noncritical packages, 'unreachable_ok' for critical packages)
await_ds_ready() {
  local namespace=$1
  local dsname=$2
  local endtime=$(($(date +%s) + ${3:-120}))
  local tolerate=$4

  local DS_JSON OBSERVED_GEN CURRENT_GEN DESIRED READY NODE_NOTREADY

  echo -n "await $dsname "
  while true; do
    echo -n "."
    if [ ${endtime} -lt "$(date +%s)" ]; then
      echo " timed out waiting for daemonset $namespace/$dsname"
      return 1
    fi
    DS_JSON=$($KUBECTL -n "$namespace" get ds "$dsname" -ojson)

    OBSERVED_GEN=$(jq -r '.status.observedGeneration' <<<"$DS_JSON")
    CURRENT_GEN=$(jq -r '.metadata.generation' <<<"$DS_JSON")
    DESIRED=$(jq -r '.status.desiredNumberScheduled' <<<"$DS_JSON")
    READY=$(jq -r '.status.numberReady' <<<"$DS_JSON")
    UPDATED=$(jq -r '.status.updatedNumberScheduled' <<<"$DS_JSON")

    if [ "$tolerate" = "notready_ok" ]; then
      OLDR=$READY
      NODE_NOTREADY=$($KUBECTL get nodes -ojson | jq '[.items[] | select(.status.conditions[] | select(.type == "Ready" and .status != "True")) | .metadata.name] | length')
      READY=$(( $READY + $NODE_NOTREADY ))
    elif [ "$tolerate" = "unreachable_ok" ]; then
      OLDR=$READY
      NODE_UNREACHABLE=$(wc -l < $UNREACHABLE_LIST)
      READY=$(( $READY + $NODE_UNREACHABLE ))
    fi

    if [[ "$OBSERVED_GEN" == "$CURRENT_GEN" && "$READY" -ge "$DESIRED" && "$DESIRED" == "$UPDATED" ]]; then
      echo " ready"
      break
    else
      sleep 1
    fi
  done
}

# Create a ssh keypair for PB3 deployment use if it doesn't exist yet.
create_pb3_ssh_keypair() {
  local namespace=$1

  if ! kubectl -n $namespace get secret cerebras-sshkey-pair 2>&1 > /dev/null; then
    if ! kubectl get ns $namespace >/dev/null 2>&1; then
      kubectl create ns $namespace
    fi
    rm -rf ./gen; mkdir -p ./gen
    ssh-keygen -t rsa -b 4096 -N "" -f ./gen/id_rsa
    kubectl -n $namespace create secret generic cerebras-sshkey-pair \
      --from-file=id_rsa=./gen/id_rsa --from-file=id_rsa.pub=./gen/id_rsa.pub
    # Add a new line in authorized_keys file to make sure the new public key is appended correctly
    { echo; cat ./gen/id_rsa.pub; } >> /root/.ssh/authorized_keys
    rm -rf ./gen
  fi
}

# Create or update SMTP credentials secret from pkg-properties
# Usage: create_smtp_credentials_secret <namespace> [secret-name]
create_smtp_credentials_secret() {
  local namespace="$1"
  local secret_name="${2:-smtp-credentials}"
  
  if [ ! -f "$CLUSTER_PROPERTIES" ]; then
    echo "Warning: pkg-properties.yaml not found at '$CLUSTER_PROPERTIES'. Secret '$secret_name' will not be created."
    return 1
  fi
  
  # Extract SMTP credentials
  local properties=$(get_cluster_properties)
  local smtp_username=$(echo "$properties" | jq -r '.properties.alertmanager.smtp.auth_username // empty')
  local smtp_password=$(echo "$properties" | jq -r '.properties.alertmanager.smtp.auth_password // empty')
  
  if [ -z "$smtp_username" ] || [ -z "$smtp_password" ]; then
    echo "Warning: SMTP credentials not found in pkg-properties. Secret '$secret_name' will not be created."
    return 1
  fi
  
  echo "Creating/updating SMTP credentials Secret '$secret_name' in namespace '$namespace'..."
  
  # Create or update Secret (using apply for idempotency)
  if $KUBECTL create secret generic "$secret_name" \
    --from-literal=username="$smtp_username" \
    --from-literal=password="$smtp_password" \
    -n "$namespace" --dry-run=client -o yaml | $KUBECTL apply -f -; then
    echo "Secret '$secret_name' created/updated in namespace '$namespace'"
    return 0
  else
    echo "Error: Failed to create/update secret '$secret_name' in namespace '$namespace'"
    return 1
  fi
}

_print_one_failed_ds_pod_logs() {
  local namespace=$1
  local ds_name=$2

  local notrunning_pods=$(kubectl -n $namespace get pod -lapp=$ds_name -ojson | jq -r '.items[] | select(.status.phase != "Running") | .metadata.name')
  for pod in $notrunning_pods; do
    if ! kubectl -n $namespace logs "$pod" --all-containers --tail=20; then
      echo "Can't retrieve the logs for pod $pod."
      kubectl -n $namespace describe pod "$pod" || true
    else
      echo "Logs for pod $pod is retrieved."
    fi
    # We just retrieve logs from one failed pod, to prevent from dumping too much logs
    break
  done
}

# Helper function for creating cluster package init daemonsets
# In namespace $1, using DS name $2, take script filename $3 with envvar substitutions string
# (or strings) starting with $5 and populate a CM. Then create the DS, and wait for a timeout of
# $4, but don't fail the install if the DS rollout isn't complete, in case there are NotReady
# nodes. They will eventually run the DS pod. If all pods completed before the timeout,
# delete the DS.
# Note that the caller only needs to supply non-standard envvars - the common ones are set here
# Users of this function must follow the usual installer layout - for an installer called foo,
#   there should be a foo-ds.yaml, which expects a foo-scripts CM, and runs a foo-installer.sh
installer_rollout_wait_or_warn() {
  local namespace=$1
  local ds_name=$2
  local script_path=$3
  local timeout=$4
  local tolerate=$5
  shift 5
  local envvars="$@"

  create_pb3_ssh_keypair "$namespace"

  # add the common installer args
  my_hostname=$(hostname)
  my_hostip=$(get_node_mgmt_ip $my_hostname)
  envvars="NAMESPACE=$namespace LEAD_MGMT_NODE=${my_hostname} LEAD_MGMT_NODE_IP=${my_hostip} TAG=$(get_image_version alpine-containerd)  $envvars"

  kubectl delete ds -n $namespace $ds_name --ignore-not-found
  kubectl create configmap ${ds_name}-scripts -n $namespace \
    --from-file=${script_path} --dry-run=client -oyaml | kubectl apply -f -
  env ${envvars} bash -c "cat ${ds_name}-ds.yaml | envsubst | kubectl apply -f -"

  echo "waiting maximum of ${timeout}s for $ds_name installation to complete"
  if ! await_ds_ready $namespace ${ds_name} $timeout $tolerate; then
    echo "Error: some installer pods failed in ${ds_name}, exiting and leaving the DS in place."
    _print_one_failed_ds_pod_logs $namespace $ds_name
    return 1
  else
    # All ready nodes ran the installer. Only delete the ds if there are no NotReady nodes.
    DS_JSON=$($KUBECTL -n "$namespace" get ds "$ds_name" -ojson)
    DESIRED=$(jq -r '.status.desiredNumberScheduled' <<<"$DS_JSON")
    READY=$(jq -r '.status.numberReady' <<<"$DS_JSON")

    if [[ "$READY" == "$DESIRED" ]]; then
      echo "All $ds_name pods completed, deleting the installer DS"
      kubectl delete ds -n $namespace $ds_name --ignore-not-found
    else
      echo "Some nodes are NotReady and have not yet run the installer. Continuing without deleting the $ds_name DS."
      _print_one_failed_ds_pod_logs $namespace $ds_name
    fi
  fi
}
