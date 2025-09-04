#!/bin/bash

set -o pipefail
cd "$(dirname "$0")"
SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE:-job-operator}
CLUSTER_CONFIG="/opt/cerebras/cluster/cluster.yaml"
CLUSTER_PROPERTIES="/opt/cerebras/cluster/pkg-properties.yaml"
CSCTL="csctl --namespace ${SYSTEM_NAMESPACE}"
YQ=yq
KUBECTL=kubectl
EDITOR="${EDITOR:-vim}"
INTERACTIVE=${INTERACTIVE:-1} # default to 1

### HELPERS ###

function get_image_version() {
  image=$1
  if [ -f "${CLUSTER_PROPERTIES}" ]; then
    yq -ojson ${CLUSTER_PROPERTIES} | jq -r ".properties.images.\"${image}\" // \"latest\""
  else
    kubectl get cm job-operator-cluster-env -ojson -n"${SYSTEM_NAMESPACE}" | jq -r ".metadata.annotations.\"${image}-tag\" // \"latest\""
  fi
}

# Check if dependency checks should be performed
# Returns 0 (true) if dependencies should be checked, 1 (false) if they should be skipped
_should_check_dependencies() {
  # Skip dependency checks if:
  # 1. No arguments provided
  # 2. First argument is --help or -h
  # 3. Second argument is --help or -h (for commands that take a subcommand)
  if [[ -z "$1" ]]; then
    return 1  # Skip checks - no arguments
  fi
  
  if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    return 1  # Skip checks - help requested for main command
  fi
  
  if [[ -n "$2" && ( "$2" == "--help" || "$2" == "-h" ) ]]; then
    return 1  # Skip checks - help requested for subcommand
  fi
  
  return 0  # Perform dependency checks
}

_check_dependency() {
  program=$1
  if ! which "$program" &>/dev/null; then
    echo "required dependency $program is not present, exiting"
    exit 1
  fi

  if [ ! -s "$(which $program)" ]; then
    echo "required dependency $program is an empty file, exiting"
    exit 1
  fi
}

_resolve_py3() {
  if which python3 &>/dev/null; then
    echo -n python3
    return
  else
    for mver in $(seq 17 -1 5); do
      if which "python3.${mver}" &>/dev/null; then
        echo -n "python3.${mver}"
        return
      fi
    done
  fi

  echo "python3 is not present" >&2
  exit 1
}

# Retrieve any existing cluster.yaml and store it in $CLUSTER_CONFIG
_get_cluster_config() {
  local config_yaml tmpconfig
  config_yaml=$(kubectl get cm cluster -n"$SYSTEM_NAMESPACE" -ojsonpath='{.data.clusterConfiguration\.yaml}' 2>/dev/null || true)
  if [ -n "$config_yaml" ]; then
    tmpconfig=$(mktemp)
    echo "$config_yaml" >"$tmpconfig"
    mv -f "$tmpconfig" "$CLUSTER_CONFIG"
  fi
}

_echo_cluster_cfg_json() {
  if kubectl get cm cluster -n"$SYSTEM_NAMESPACE" &>/dev/null; then
    kubectl get cm cluster -n"$SYSTEM_NAMESPACE" -ojsonpath='{.data.clusterConfiguration\.yaml}' | yq -ojson
  elif [ -f "$CLUSTER_CONFIG" ]; then
    yq -ojson <"CLUSTER_CONFIG"
  else
    return 1
  fi
}

_check_systems_idle() {
  # Check systems are unschedulable and not assigned to any jobs, returning any violations in `warnings` array
  local warnings recommendations

  echo -n "checking systems idle and unschedulable...\n"
  warnings=()
  recommendations=()
  system_status=$($CSCTL get cluster --system-only)
  for system in "$@"; do
    if ! kubectl get system "$system" --ignore-not-found; then
      warnings+=("system '$system' was not found")
      continue
    elif echo "$system_status" | grep "$system" | grep -v error; then
      warnings+=("system '$system' is in schedulable state but must be marked down before run this command")
      recommendations=("$0 update-system --name=$system --state=error --note='example: test system fabric diag' --user=\$USER")
      continue
    fi

    granted_job=$(kubectl get rl -A -ojson | jq -r --arg name "$system" '.items[] | select(.status.state =="Granted" and (.status.resourceGrants[] | .resources[] | .name == $name)) | .metadata.name')
    if [ -n "$granted_job" ]; then
      warnings+=("system $system is in-use by job $granted_job but must be idle before use")
      continue
    fi
  done

  if [ "${#warnings[@]}" -gt 0 ]; then
    echo "error"
    for warning in "${warnings[@]}"; do
      echo "  $warning"
    done

    if [ "${#recommendations[@]}" -gt 0 ]; then
      echo "try running:"
      for recommendation in "${recommendations[@]}"; do
        echo "  $recommendation"
      done
    fi
    return 1
  else
    echo "ok"
  fi
}

# _remove_disabled_swarmx_nics [CLUSTER_YAML in json format]
#    temporary hack to remove OCP nics from Swarmx nodes. NICs may be removed since their firmware currently supports
#    low bandwidth mode (50GPBs) when they should support high bandwidth mode after a pending firmware update
_remove_disabled_swarmx_nics() {
  if ! [ -f "${CLUSTER_PROPERTIES}" ]; then return 0; fi

  remove_swarmx_nics=$(yq '(.properties.disableSwarmxNICs // []) | .[]' "${CLUSTER_PROPERTIES}")
  if [ -z "${remove_swarmx_nics}" ]; then
    return 0
  fi

  for nic in $remove_swarmx_nics; do
    out=$(mktemp)
    jq --arg NAME "$nic" '
      .nodes |= map(
        if .role == "broadcastreduce" then
          . + { "networkInterfaces": [ .networkInterfaces[] | select(.name != $NAME) ] }
        else
          .
        end
      )' <"${1}" >"${out}"
    mv "${out}" "${1}"
  done
}

if [ -f csadm_test_helpers ]; then
  # override helper functions for test purposes
  source csadm_test_helpers
fi

### MAIN FUNCTIONS

# Whether it is safe to update cluster configmap. We consider it safe when the nodes/systems
# to be removed are not being used by a job. Any new nodes/systems to be added is considered
# as safe. We don't consider any updates as not safe, since
# 1. In regular PB flow, we should not update nodegroup assignments for existing nodes.
# 2. For internal system swap, we will do remove + add.
# 2. For internal node swap, we will do remove + add.
can_safely_update_cluster_config() {
  old_config_yaml=$1
  new_config_json=$2

  if [ ! -f "$old_config_yaml" ]; then
    # No existing cluster.yaml available. This is a new install.
    return 0
  fi

  old_nodes=$(cat $old_config_yaml | yq -r ".nodes[] | .name" | tr '\n' ' ')
  old_systems=$(cat $old_config_yaml | yq -r ".systems[] | .name" | tr '\n' ' ')

  new_nodes=$(cat $new_config_json | jq -r ".nodes[] | .name" | tr '\n' ' ')
  new_systems=$(cat $new_config_json | jq -r ".systems[] | .name" | tr '\n' ' ')

  removed_entries=""
  for old_node in $old_nodes; do
    if ! echo " $new_nodes " | grep -qF " $old_node "; then
      removed_entries="$removed_entries $old_node"
    fi
  done
  for old_system in $old_systems; do
    if ! echo " $new_systems " | grep -qF " $old_system "; then
      removed_entries="$removed_entries $old_system"
    fi
  done

  echo "Entries to be removed: $removed_entries"
  if [ -z "$removed_entries" ]; then
    return 0
  fi

  if which csctl &>/dev/null; then
    in_use_entries="$(csctl get cluster -ojson | jq -r '.items[] | select(.jobIds | length > 0) | .meta.name' |
      sort -n | uniq | tr '\n' ' ')"
  else
    in_use_entries=""
  fi

  if [ -z "$in_use_entries" ]; then
    return 0
  fi

  in_use_entries_to_remove=""
  for removed_entry in $removed_entries; do
    if echo " $in_use_entries " | grep -qF " $removed_entry "; then
      in_use_entries_to_remove="$in_use_entries_to_remove $removed_entry"
    fi
  done
  if [ -z "$in_use_entries_to_remove" ]; then
    return 0
  else
    echo "Entries to be removed are in use: $in_use_entries_to_remove"
    return 1
  fi
}

update_cluster_cm() {
  local verb="create"
  if kubectl -n${SYSTEM_NAMESPACE} get cm cluster &>/dev/null; then
    verb="replace"
  fi
  kubectl create cm cluster -n${SYSTEM_NAMESPACE} \
    --from-file=clusterConfiguration.yaml=${CLUSTER_CONFIG} --dry-run -o yaml | kubectl ${verb} -f-
}

update_cluster_config() {
  local UPDATE_CLUSTER_CONFIG_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME update-cluster-config [NETWORK_CONFIG_FILE] [--create-depop-groups=N] [--dry-run] [--safe-system-update]

    Convert the given network config file to cluster.yaml format that can be
    used by cluster management. Loads the resulting config into the cluster.

        NETWORK_CONFIG_FILE:   Optional path to the network config file to be
                               converted. If not specified, the default
                               /opt/cerebras/cluster/network_config.json will be used.

        --create-depop-groups=N: Optional flag intended for internal use. Splits nodegroups with
                               8 or more memx into depopulated groups (less than 10 memx) until N
                               additional groups have been created or the cluster has no more groups
                               to split. Setting N=all will attempt to create a group for each system
                               which is unaffiliated with a nodegroup.

        --dry-run[=PATH]:      Only write the config to PATH or /opt/cerebras/cluster/cluster.yaml
                               but do not load to the cluster. This is useful for debugging.

        --safe-system-update   Only proceed if the cluster config update will only add systems.
EOM
  )
  # parse arguments, there is an optional --dry-run flag and an optional positional argument for the network config file
  local network_json=""
  local dry_run_path=""
  local safe_system_update=false
  local flags
  for arg in "$@"; do
    if [ "$arg" = "--dry-run" ]; then
      dry_run_path="$CLUSTER_CONFIG"
    elif [[ "$arg" =~ ^--dry-run=(.*)$ ]]; then
      dry_run_path="${BASH_REMATCH[1]}"
    elif [ "$arg" = "--safe-system-update" ]; then
      safe_system_update=true
    elif [[ "$arg" =~ --create-depop-groups= ]]; then
      flags="$flags $arg"
    elif [ "$arg" = "--help" ] || [ "$arg" = "-h" ]; then
      echo "$UPDATE_CLUSTER_CONFIG_USAGE"
      return 0
    else
      if [ -n "$network_json" ]; then
        echo "unknown argument $arg" >&2
        exit 1
      fi
      network_json="$arg"
    fi
  done
  network_json=${network_json:-/opt/cerebras/cluster/network_config.json}

  if [ ! -f "$network_json" ]; then
    echo "file $network_json does not exist" >&2
    exit 1
  fi

  if ! [ -f csadm/convert_network.py ]; then
    echo "$(dirname $0)/csadm/convert_network.py does not exist" >&2
    exit 1
  fi

  _get_cluster_config

  set -e

  if $safe_system_update; then
    if ! [ -f csadm/csadm_list_helper.py ]; then
      echo "$(dirname $0)/csadm/csadm_list_helper.py does not exist" >&2
      exit 1
    fi

    workdir=$(mktemp -d)
    export INCREMENTAL_DIR=$workdir

    # csadm_list_helper.py calls k8_init.sh reconcile
    # also the update config step should happen last for incremental
    if ! $PY3 csadm/csadm_list_helper.py --workdir=$workdir --netjson=${network_json} --new-systems-only-check; then
      echo "Cannot safely update the cluster config because ${network_json} contained new nodes."
      exit 1
    else
      echo "${network_json} had no changed nodes, continuing with update-cluster-config."
    fi
  fi

  $PY3 csadm/convert_network.py "$network_json" $flags >/tmp/cluster.json
  # overlay old cluster.yaml::properties with new cluster.yaml::properties
  # since some props cannot be determined from network.json at present (management{VIP,ASN,RouterIP})
  if [ -f "${CLUSTER_CONFIG}" ] && [ "$(yq '.name' ${CLUSTER_CONFIG})" = "$(jq -r '.name' /tmp/cluster.json)" ]; then
    yq -ojson ".properties // {}" ${CLUSTER_CONFIG} >/tmp/cluster-properties-old.json
    jq -s '(.[0] * .[1].properties) as $mergedProps | .[1] | .properties = $mergedProps' /tmp/cluster-properties-old.json /tmp/cluster.json >/tmp/cluster.json.merged
    mv /tmp/cluster.json.merged /tmp/cluster.json
  fi

  _remove_disabled_swarmx_nics /tmp/cluster.json

  if ! can_safely_update_cluster_config "$CLUSTER_CONFIG" /tmp/cluster.json; then
    echo "error: It is not safe to update cluster config"
    return 1
  fi

  local output_path=${dry_run_path:-$CLUSTER_CONFIG}
  yq eval -oy -P /tmp/cluster.json >"${output_path}"
  echo "wrote ${output_path}" >&2

  touch "${CLUSTER_PROPERTIES}"

  set_pkg_prop() {
    local property_path="$1"
    local value="$2"
    local yaml_file="${CLUSTER_PROPERTIES}"
    local existing_value=$(yq e "${property_path}" "${yaml_file}")

    if [[ -z "${existing_value}" || "${existing_value}" == "null" ]]; then
      V="$value" yq e -i "${property_path} = env(V)" "${yaml_file}"
      echo "Set ${property_path} to ${value} in ${yaml_file}"
    else
      if [[ "${existing_value}" != "${value}" ]]; then
        echo "WARNING: updated value for ${property_path} differed from value in ${yaml_file}"
        echo "Current value: ${existing_value}"
        echo "Expected value: ${value}"
      fi
    fi
  }

  local pod_networks=$(jq '.environment.overlay_prefixes' "$network_json" | yq -P)
  set_pkg_prop ".properties.cilium.clusterPoolIPv4PodCIDRList" "${pod_networks}"

  local mask_size=$(jq '.environment.overlay_prefix_mask_size // 26' "$network_json")
  set_pkg_prop ".properties.cilium.clusterPoolIPv4MaskSize" "${mask_size}"

  local data_vip=$(jq -r '(.cluster_data_vip.vip | rtrimstr("/32")) // ""' "$network_json")
  if [ -n "$data_vip" ]; then
    set_pkg_prop ".properties.multiMgmtNodes.dataVip" "${data_vip}"
  fi

  local mgmt_vip=$(jq -r '.cluster_mgmt_vip.vip // ""' "$network_json")
  if [ -n "$mgmt_vip" ]; then
    set_pkg_prop ".properties.multiMgmtNodes.mgmtVip" "${mgmt_vip}"
  fi

  local node_asn=$(jq -r '.cluster_mgmt_vip.node_asn // ""' "$network_json")
  if [ -n "$node_asn" ]; then
    set_pkg_prop ".properties.multiMgmtNodes.mgmtNodeAsn" "${node_asn}"
  fi

  local router_asn=$(jq -r '.cluster_mgmt_vip.router_asn // ""' "$network_json")
  if [ -n "$router_asn" ]; then
    set_pkg_prop ".properties.multiMgmtNodes.mgmtRouterAsn" "${router_asn}"
  fi

  echo "wrote ${CLUSTER_PROPERTIES}" >&2

  if [ -n "$dry_run_path" ]; then
    echo "skipping load" >&2
    return 0
  fi

  if ! kubectl get nodes &>/dev/null; then
    return 0 # k8 is not initialized
  fi

  kubectl create ns "${SYSTEM_NAMESPACE}" &>/dev/null || true
  update_cluster_cm
  if [ "$?" != "0" ]; then
    echo "failed to load cluster config" >&2
    return 1
  fi
  echo "cluster config loaded" >&2
}

get_cluster() {
  local GET_CLUSTER_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME get-cluster [--system-only|--node-only] [--error-only]

   Get cluster state information including both systems/nodes

        --system-only: if specified, it will output only systems
        --node-only: if specified, it will output only nodes
        --error-only: if specified, it will output only error nodes/systems
EOM
  )
  if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "$GET_CLUSTER_USAGE"
    return 0
  fi

  $CSCTL get cluster "$@"
}

update_system() {
  local UPDATE_SYSTEM_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME update-system --name=<system_name> [--port=<port_name>] --state=<ok/error> --note="<reasoning>" --user="<username>"

    Example: $SCRIPT_NAME update-system --name=system0 --state=error --note="cm hung, please check with @alias/CBSD-xxxx/..." --user="cerebras"
    Update system health status. This is mainly used to exclude an error system from the schedulable resources
    Warning: after marked as error, the system will not be assigned for jobs until marked as ok.
    --user and --note is required when setting state to "error", optional when setting to "ok".
EOM
  )
  if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "$UPDATE_SYSTEM_USAGE"
    return 0
  fi

  local name=""
  local port=""
  local note=""
  local user=""
  local state="ok"
  local status="False"
  for i in "$@"; do
    case $i in
    --name=*)
      name="${i#*=}"
      shift # past argument=value
      ;;
    --port=*)
      port="${i#*=}"
      shift # past argument=value
      ;;
    --note=*)
      note="${i#*=}"
      shift # past argument=value
      ;;
    --state=*)
      state="${i#*=}"
      shift # past argument=value
      ;;
    --user=*)
      user="${i#*=}"
      shift # past argument=value
      ;;
    --*)
      echo "Unknown option $i"
      echo "$UPDATE_SYSTEM_USAGE"
      exit 1
      ;;
    *)
      echo "Unknown option $i, maybe you forget to quote note message with \"\"?"
      echo "$UPDATE_SYSTEM_USAGE"
      exit 1
      ;;
    esac
  done

  if [[ -z ${name} ]]; then
    echo "$UPDATE_SYSTEM_USAGE"
    exit 1
  fi
  if [[ -n ${port} ]]; then
    pattern="^n(0[0-9]|1[0-1])$"
    if ! [[ $port =~ $pattern ]]; then
      echo "error: $port is not a valid port name. Allowed ports: n00, n01, n02, ..., n11"
      exit 1
    fi
  fi
  case "${state}" in "ok" | "error")
    if [[ ${state} == "error" ]]; then
      status="True"
      if [[ -z ${note} ]]; then
        echo "error: please provide error msg, e.g. --note=\"cm hung, please check with @alias/CBSD-xxxx/...\""
        exit 1
      fi

      if [[ -z ${user} ]]; then
        echo "error: --user parameter is required when setting state to 'error'"
        echo "$UPDATE_SYSTEM_USAGE"
        exit 1
      fi
    fi
    ;;
  *)
    echo "Unknown state: '${state}'. Allowed states: 'ok', 'error'"
    exit 1
    ;;
  esac

  if ! kubectl get system "${name}" &>/dev/null; then
    echo "skipping update-system for '${name}' as it does not exist"
    exit 1
  fi

  # init system crd conditions if needed
  if ! kubectl get system "${name}" -ojson | grep -q conditions; then
    kubectl patch system "${name}" --type='json' --subresource='status' \
      -p='[{"op": "replace", "path": "/status/conditions", "value": []}]' 1>/dev/null
  fi

  local op="replace"
  local index
  local type
  local timestamp=$(date +%Y%m%d-%H%M%S)

  # Format note with user and timestamp only if user is provided
  if [[ -n ${user} ]]; then
    note="${user}@${timestamp}: ${note}"
  fi

  if [[ -n ${port} ]]; then
    type="CsadmSystemPortError_$port"
    note="Port $port was set to ${state}, ${note}"
  else
    type="CsadmSystemError"
    # for backwards compatible, no harm but can be removed later
    if [[ ${state} == "ok" ]]; then
      kubectl patch system "${name}" --type=merge -p '{"spec": {"unschedulable": false}}' &>/dev/null || true
      kubectl patch system "${name}" --subresource='status' --type='merge' -p '{"status": {"state": "OK"}}' &>/dev/null || true
    fi
  fi
  index=$(kubectl get system "${name}" -ojson | jq '.status.conditions | map(.type) | index('\"$type\"') // empty')
  if [ -z "$index" ]; then
    op="add"
    index="-"
  fi
  json_payload=$(jq -n \
    --arg op "$op" \
    --arg type "$type" \
    --arg status "$status" \
    --arg note "$note" \
    --arg reason "CsadmUpdate" \
    --arg transition_time "$(date -Is)" \
    --arg heartbeat_time "$(date -Is)" \
    '[{"op": "'$op'", "path": "/status/conditions/'$index'",
    "value": {"type": $type, "status": $status, "message": $note, "reason": $reason,
    "lastTransitionTime": $transition_time, "lastHeartbeatTime": $heartbeat_time}}]')
  kubectl patch system "${name}" --type='json' --subresource='status' -p="${json_payload}"
}

update_node() {
  local UPDATE_NODE_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME update-node --name=<node_name> [--nic=<nic_name>] --state=<ok/error> --note="<reasoning>" --user="<username>"

    Example: $SCRIPT_NAME update-node --name=node0 --state=error --note="100g route missing, please check with @alias/CBSD-xxxx/..." --user="cerebras"
    Update node health status. This is mainly used to exclude an unhealthy node/NIC from being scheduled.
    Warning: after marked as error, the node will not be assigned for jobs until marked as ok.
    --user and --note is required when setting state to "error", optional when setting to "ok".
EOM
  )
  if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "$UPDATE_NODE_USAGE"
    return 0
  fi

  local name=""
  local nic=""
  local note=""
  local user=""
  local state="ok"
  local status="False"
  for i in "$@"; do
    case $i in
    --name=*)
      name="${i#*=}"
      shift # past argument=value
      ;;
    --state=*)
      state="${i#*=}"
      shift # past argument=value
      ;;
    --nic=*)
      nic="${i#*=}"
      shift # past argument=value
      ;;
    --note=*)
      note="${i#*=}"
      shift # past argument=value
      ;;
    --user=*)
      user="${i#*=}"
      shift # past argument=value
      ;;
    --*)
      echo "Unknown option $i"
      echo "$UPDATE_NODE_USAGE"
      exit 1
      ;;
    *)
      echo "Unknown option $i, maybe you forget to quote note message with \"\"?"
      echo "$UPDATE_NODE_USAGE"
      exit 1
      ;;
    esac
  done

  if [[ -z ${name} ]]; then
    echo "$UPDATE_NODE_USAGE"
    exit 1
  fi

  if ! kubectl get node "${name}" &>/dev/null; then
    echo "skipping update-node for '${name}' as it does not exist"
    exit 1
  fi

  if [[ -n ${nic} ]]; then
    if ! yq -r '.nodes[] | select(.name == "'${name}'") | .networkInterfaces' /opt/cerebras/cluster/cluster.yaml | grep -qw "${nic}"; then
      echo "error: nic ${nic} doesn't exist on node ${name}"
      exit
    fi
  fi
  case "${state}" in "ok" | "error")
    if [[ ${state} == "error" ]]; then
      status="True"

      if [[ -z ${note} ]]; then
        echo "error: please provide error msg, e.g. --note=\"100g route missing, please check with @alias/CBSD-xxxx/...\""
        exit 1
      fi

      if [[ -z ${user} ]]; then
        echo "error: --user parameter is required when setting state to 'error'"
        echo "$UPDATE_NODE_USAGE"
        exit 1
      fi
    fi
    ;;
  *)
    echo "Unknown state: '${state}'. Allowed states: 'ok', 'error'"
    exit 1
    ;;
  esac

  local op="replace"
  local index
  local type
  local timestamp=$(date +%Y%m%d-%H%M%S)

  # Format note with user and timestamp only if user is provided
  if [[ -n ${user} ]]; then
    note="${user}@${timestamp}: ${note}"
  fi

  if [[ -n ${nic} ]]; then
    type="CsadmNodeNICError_$nic"
    note="NIC $nic was set to ${state}, ${note}"
  else
    type="CsadmNodeError"
    # for backwards compatible, no harm but can be removed later
    if [[ ${state} == "ok" ]]; then
      kubectl uncordon "${name}" 1>/dev/null
    fi
  fi
  index=$(kubectl get node "${name}" -ojson | jq '.status.conditions | map(.type) | index('\"$type\"') // empty')
  if [ -z "$index" ]; then
    op="add"
    index="-"
  fi
  json_payload=$(jq -n \
    --arg op "$op" \
    --arg type "$type" \
    --arg status "$status" \
    --arg note "$note" \
    --arg reason "CsadmUpdate" \
    --arg transition_time "$(date -Is)" \
    --arg heartbeat_time "$(date -Is)" \
    '[{"op": "'$op'", "path": "/status/conditions/'$index'",
    "value": {"type": $type, "status": $status, "message": $note, "reason": $reason,
    "lastTransitionTime": $transition_time, "lastHeartbeatTime": $heartbeat_time}}]')
  kubectl patch node "${name}" --type='json' --subresource='status' -p="${json_payload}"
}

get_namespace() {
  local GET_NAMESPACE_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME get-namespace [--name=<namespace>]

    Get namespaces and their system assignments.
EOM
  )

  local name=""
  for i in "$@"; do
    case $i in
    --name=*)
      name="${i#*=}"
      shift # past argument=value
      ;;
    --help)
      echo "$GET_NAMESPACE_USAGE"
      return 0
      ;;
    -h)
      echo "$GET_NAMESPACE_USAGE"
      return 0
      ;;
    --*)
      echo "Unknown option $i"
      echo "$GET_NAMESPACE_USAGE"
      exit 1
      ;;
    *) ;;
    esac
  done
  if [ -z "$name" ]; then
    # if none assigned, sort based on label will not get any output
    $CSCTL session list -owide
  else
    $CSCTL session get "$name" -owide
  fi
}

batch_update_nodes() {
  local UPDATE_NODE_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME batch-update-nodes <file_path> <ok/error> \"<note>\" \"<username>\"

    Example: $SCRIPT_NAME batch-update-nodes /tmp/nodes error "reinstall in progress" "cerebras"
    path needs to be abs file path
EOM
  )
  if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "$UPDATE_NODE_USAGE"
    return 0
  fi
  nodes_fp=$1
  state=$2
  note=$3
  user=$4
  if [ -z "$nodes_fp" ] || [ ! -f "$nodes_fp" ]; then
    echo "provided filepath:[$nodes_fp] does not exist, please provide the abs path"
  fi

  # get a cache status of all nodes
  csctl get node >/tmp/.node-cache
  while IFS= read -r node; do
    # skip node not in /tmp/.node-cache. These are the cluster nodes that are
    # in the inventory but not added to k8s yet
    if ! grep -q -w "${node}" /tmp/.node-cache; then
      continue
    fi
    # skip node already in current state to avoid overriding message
    if grep -w "${node}" /tmp/.node-cache | grep -q "${state}"; then
      continue
    fi
    # skip updating node to ok if it's marked down by other operation
    if [[ ${state} == "ok" ]]; then
      if ! grep -w "${node}" /tmp/.node-cache | grep -q "${note}"; then
        continue
      fi
    fi
    update_node --name="${node}" --state="${state}" --note="${note}" --user="${user}"
  done <"${nodes_fp}"
}

update_nodegroup() {
  local UPDATE_NODEGROUP_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME update-nodegroup --name=<nodegroup_name> --state=<ok/error> --note="<reasoning>" --user="<username>"

    Example: $SCRIPT_NAME update-nodegroup --name=nodegroup0 --state=error --note="nodegroup maintenance, please check with @alias/CBSD-xxxx/..."
    Update health status for all nodes in the specified nodegroup. This is mainly used to exclude an unhealthy nodegroup from being scheduled.
    Warning: after marked as error, all nodes in the nodegroup will not be assigned for jobs until marked as ok.
EOM
  )
  if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "$UPDATE_NODEGROUP_USAGE"
    return 0
  fi

  local name=""
  local note=""
  local user=""
  local state="ok"
  for i in "$@"; do
    case $i in
    --name=*)
      name="${i#*=}"
      shift # past argument=value
      ;;
    --state=*)
      state="${i#*=}"
      shift # past argument=value
      ;;
    --note=*)
      note="${i#*=}"
      shift # past argument=value
      ;;
    --user=*)
      user="${i#*=}"
      shift # past argument=value
      ;;
    --*)
      echo "Unknown option $i"
      echo "$UPDATE_NODEGROUP_USAGE"
      exit 1
      ;;
    *)
      echo "Unknown option $i, maybe you forget to quote note message with \"\"?"
      echo "$UPDATE_NODEGROUP_USAGE"
      exit 1
      ;;
    esac
  done

  if [[ -z ${name} ]]; then
    echo "$UPDATE_NODEGROUP_USAGE"
    exit 1
  fi

  # Check if nodegroup exists
  if ! $CSCTL get nodegroup "${name}" &>/dev/null; then
    echo "Nodegroup '${name}' does not exist. Use 'csctl get nodegroup' to list available nodegroups."
    exit 1
  fi

  # Get nodes in the nodegroup from .status.roleStatus.*.names
  local nodes
  nodes=$($CSCTL get nodegroup "${name}" -o yaml | $YQ -r '.status.roleStatus[]?.names[]?' | sort -u)
  
  if [ -z "$nodes" ]; then
    echo "No nodes found in nodegroup '${name}'"
    return 0
  fi

  local node_count=$(echo "$nodes" | wc -l)
  echo "Found $node_count nodes in nodegroup '${name}'"

  local nodes_fp="/tmp/.nodegroup-${name}-cache"
  echo "$nodes" > "$nodes_fp"

  batch_update_nodes "$nodes_fp" "$state" "$note" "$user"
}

create_namespace() {
  local CREATE_NAMESPACE_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME create-namespace --name=<namespace> [--systems=<system0,system1,...>]

    Create a namespace with optional system assignment. This is used for multibox sharing.

    Deprecated: These commands continue to work but only assign/unassign system resources.
                use 'csctl session' commands for improved control and visibility into
                namespace/session assignments.
EOM
  )

  local name=""
  local systems=""
  for i in "$@"; do
    case $i in
    --name=*)
      name="${i#*=}"
      shift # past argument=value
      ;;
    --systems=*)
      systems="${i#*=}"
      shift # past argument=value
      ;;
    --help)
      echo "$CREATE_NAMESPACE_USAGE"
      return 0
      ;;
    -h)
      echo "$CREATE_NAMESPACE_USAGE"
      return 0
      ;;
    --*)
      echo "Unknown option $i"
      echo "$CREATE_NAMESPACE_USAGE"
      exit 1
      ;;
    *) ;;
    esac
  done

  if [ -z "$name" ]; then
    echo "namespace name can't be empty"
    exit 1
  fi

  if [ "$name" == "${SYSTEM_NAMESPACE}" ]; then
    echo "name reserved as system namespace, please use another name"
    exit 1
  fi

  if kubectl create namespace "$name" --dry-run=client -o yaml 2>/dev/null | kubectl apply -f -; then
    echo "Namespace creation success"
    kubectl label namespace "${name}" user-namespace= --overwrite
  else
    echo "Namespace creation failed"
    exit 1
  fi
  if [ -n "$systems" ]; then
    if update_namespace --name="$name" --systems="$systems"; then
      echo "Assign systems $systems to $name success"
      return 0
    else
      echo "Assign systems $systems to $name failed"
      exit 1
    fi
  else
    # create an empty NSR
    update_namespace --name="$name" --mode=remove-all
  fi
}

delete_namespace() {
  local DELETE_NAMESPACE_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME delete-namespace --name=<namespace>

    Delete a namespace and release assigned systems.

    Deprecated: These commands continue to work but only assign/unassign system resources.
                use 'csctl session' commands for improved control and visibility into
                namespace/session assignments.
EOM
  )

  local name=""
  local systems=""
  for i in "$@"; do
    case $i in
    --name=*)
      name="${i#*=}"
      shift # past argument=value
      ;;
    --systems=*)
      systems="${i#*=}"
      shift # past argument=value
      ;;
    --help)
      echo "$DELETE_NAMESPACE_USAGE"
      return 0
      ;;
    -h)
      echo "$DELETE_NAMESPACE_USAGE"
      return 0
      ;;
    --*)
      echo "Unknown option $i"
      echo "$DELETE_NAMESPACE_USAGE"
      exit 1
      ;;
    *) ;;
    esac
  done
  if [ -z "$name" ]; then
    echo "namespace name can't be empty"
    exit 1
  fi
  if [ "$name" == "${SYSTEM_NAMESPACE}" ]; then
    echo "name reserved as system namespace, skip"
    exit 1
  fi

  echo "Start release systems"
  if kubectl delete nsr "$name" --ignore-not-found; then
    echo "Release systems successfully"
    if kubectl delete ns "$name" --ignore-not-found; then
      echo "Namespace delete successfully"
    else
      echo "Namespace delete failed"
      exit 1
    fi
  else
    echo "Release systems failed"
    exit 1
  fi
}

update_namespace() {
  local UPDATE_NAMESPACE_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME update-namespace --name=<namespace> --systems=<system0,system1,...> [--mode=<append|append-all|remove|remove-all>]

    Update namespace system assignments using system affinities. Default mode is overwrite.

    Deprecated: These commands continue to work but only assign/unassign system resources.
                use 'csctl session' commands for improved control and visibility into
                namespace/session assignments.

    Supported modes:
      append: append specified systems to the existing assignments.
      append-all: assign all systems.
      remove: release specified systems from the existing assignments.
      remove-all: release all assigned systems.
EOM
  )
  # keep update record
  echo "$(date): $SCRIPT_NAME update-namespace $*" >>/tmp/ns-update.log
  tail -n 100 /tmp/ns-update.log >/tmp/ns-update.log.tmp && mv /tmp/ns-update.log.tmp /tmp/ns-update.log

  echo "Warning: csadm.sh create/update/delete namespace commands are deprecated. Please use 'csctl session' commands instead"

  local name=""
  local systems=""
  local mode=""
  for i in "$@"; do
    case $i in
    --name=*)
      name="${i#*=}"
      shift # past argument=value
      ;;
    --systems=*)
      systems="${i#*=}"
      shift # past argument=value
      ;;
    --mode=*)
      mode="${i#*=}"
      shift # past argument=value
      ;;
    --help)
      echo "$UPDATE_NAMESPACE_USAGE"
      return 0
      ;;
    -h)
      echo "$UPDATE_NAMESPACE_USAGE"
      return 0
      ;;
    --*)
      echo "Unknown option $i"
      echo "$UPDATE_NAMESPACE_USAGE"
      exit 1
      ;;
    *) ;;
    esac
  done

  declare -A modes=(["append"]=1 ["append-all"]=1 ["remove"]=1 ["remove-all"]=1)
  if [[ -n "$mode" ]] && [[ -z ${modes[$mode]} ]]; then
    echo "Unknown mode: $mode, allowed modes: append|append-all|remove|remove-all"
    exit 1
  fi

  if [ -z "$name" ]; then
    echo "namespace can't be empty"
    exit 1
  fi

  run_csctl() {
    echo "Running 'csctl $@'"
    if ! eval csctl $@; then
      echo "Namespace update failed"
      return 1
    fi
    echo "Done"
  }

  local csctl_params="session debug-update $name"

  if [ "$mode" == "append-all" ]; then
    csctl_params="${csctl_params} -m set"
    local system_names=$(kubectl get system -ojsonpath='{range .items[*]}{.metadata.name},{end}' | sed 's/,$//')
    if [ -n "${system_names}" ]; then
      csctl_params="${csctl_params} --system-names=${system_names}"
    fi
    run_csctl $csctl_params
    return
  fi

  if [ "$mode" == "remove-all" ]; then
    csctl_params="${csctl_params} -m remove-all"
    run_csctl $csctl_params
    return
  fi

  if [ -z "$systems" ]; then
    echo "systems can't be empty for assignment"
    exit 1
  fi

  csctl_params="${csctl_params} --system-names ${systems}"

  if [ "$mode" = "remove" ]; then
    csctl_params="${csctl_params} -m remove"
  else
    csctl_params="${csctl_params} -m append"
  fi

  run_csctl $csctl_params || exit 1
  validate_assign "$name"
  return
}

# validate at least one populated(>4 MemX) rack + one rack with mgmt/crd node
validate_assign() {
  namespace=$1
  echo "System assignment succeeded, start validating assigned nodegroup resources"
  if [ "$($CSCTL get nodegroup -ojson |
    jq --arg namespace "$namespace" -r '[.items[] | select(.status.namespace==$namespace and .status.roleStatus.memoryx.count>4)]' |
    jq '[.[] | select(.status.roleStatus.management.count>0)]' | jq 'length')" -gt 0 ]; then
    echo "Validation nodegroup resources succeeded"
  else
    echo "Warning: no populated(>4 MemX) rack or mgmt node for namespace($namespace) after assignment"
  fi
}

# select nodes for upgrade by target version/session filter
select_upgrade_nodes() {
  # expected target version after upgrade done
  local target_version=$1
  # optional target session name
  local session=${2-""}
  local skip_sx_pick=${3-"false"}
  local ignore_running_jobs=${4-"false"}
  local ignore_sx_error=${5-"false"}

  if [ -z "${target_version}" ]; then
    echo "target version can't be empty"
    exit 1
  fi

  # build flag based on input
  filter_node_flag="--version!=${target_version} "
  if [ -n "${session}" ]; then
    filter_node_flag+="-n${session} "
  fi

  # check error nodes
  if [[ $(csctl get cluster --node-only ${filter_node_flag} --error-only -ojson | jq '.items |length') -gt 0 ]]; then
    echo "Warning: error nodes found, upgrade may fail on those nodes"
    csctl get cluster --node-only ${filter_node_flag} --error-only
  fi

  if [ -n "${session}" ]; then
    if ! csctl session get "${session}"; then
      echo "target session ${session} not found"
      exit 1
    fi

    # ensure no jobs running in session (unless ignore_running_jobs is set)
    if [[ $(csctl get job -n"${session}" -ojson | jq '.items |length') -gt 0 ]]; then
      csctl get job -n"${session}"
      if [ "${ignore_running_jobs}" == "false" ]; then
        echo "jobs still running in session ${session}, please cancel jobs first before upgrade"
        exit 1
      else
        echo "Warning: jobs still running in session ${session}, but ignoring due to --ignore-running-jobs flag"
      fi
    fi
    # pick Swarmx nodes by running fake job
    # todo: optimize to generate granted lock without requiring wsjob running
    local system_count=$(csctl session get "${session}" -ojson | jq '.state.resources.systems | length')
    if [ "${system_count}" -gt 1 ] && [ "${skip_sx_pick}" == "false" ]; then
      # sanity check
      if ! [ -f csadm/pickup-sx-wsjob.yaml ]; then
        echo "$(dirname $0)/csadm/pickup-sx-wsjob.yaml does not exist, please reinstall with latest csadm"
        exit 1
      fi
      pickup_sx_job_name="pickup-sx-for-upgrade"
      pick_up_yaml=/tmp/"${session}-pickup-sx-wsjob.yaml"
      cp csadm/pickup-sx-wsjob.yaml "${pick_up_yaml}"
      sed -i "s/SESSION/${session}/g" "${pick_up_yaml}"
      sed -i "s/TARGET/${target_version}/g" "${pick_up_yaml}"
      sed -i "s/SYSTEM/${system_count}/g" "${pick_up_yaml}"
      _clean_fake_job() {
        kubectl delete wsjob "${pickup_sx_job_name}" -n"${session}" --ignore-not-found &>/dev/null
      }
      _clean_fake_job
      trap _clean_fake_job RETURN EXIT SIGINT
      kubectl apply -f "${pick_up_yaml}"
      for ((i = 0; i < 12; i++)); do
        sleep 5
        status=$(csctl get job "${pickup_sx_job_name}" -n"${session}" -ojson | jq -r ".status.phase // empty")
        if [ "${ignore_sx_error}" != "false" ]; then
          echo "ignore_sx_error flag is set to true; treating error as a warning."
        fi
        if [ "${status}" != "RUNNING" ]; then
          if [ "${status}" == "FAILED" ]; then
            kubectl describe wsjob "${pickup_sx_job_name}" -n"${session}"
            echo "$(date) ${session}/${pickup_sx_job_name} job failed expectedly"
            if [ "${ignore_sx_error}" == "false" ]; then
              exit 1
            else
              echo "Warning: ignoring the 'job failed expectedly' error."
            fi
          fi
          if [ $i == 11 ]; then
            kubectl describe wsjob "${pickup_sx_job_name}" -n"${session}"
            echo "$(date) ${session}/${pickup_sx_job_name} job not running after 1min, abort"
            if [ "${ignore_sx_error}" == "false" ]; then
              exit 1
            else
              echo "Warning: ignoring the 'job not running after 1min' error"
            fi
          fi
          echo "$(date) ${session}/${pickup_sx_job_name} job not running yet (current:$status), recheck after 5s"
        else
          break
        fi
      done
    else
      echo "skip picking SX nodes"
    fi
  fi

  select_nodes_fp=${select_nodes_fp-"/tmp/upgrade-nodes-${session}"}
  csctl get cluster --node-only ${filter_node_flag} -ojson | jq -r '.items[] | .meta.name' >"${select_nodes_fp}"
  echo "complete node selection in ${select_nodes_fp}"

  # check picked nodes job status (unless ignore_running_jobs is set)
  if csctl get cluster --node-only ${filter_node_flag} | grep wsjob; then
    echo "selected nodes success but they have jobs running(SwarmX nodes are expected to be shared across sessions)"
    if [ "${ignore_running_jobs}" == "false" ]; then
      echo "please cancel jobs before upgrade"
      exit 1
    else
      echo "Warning: ignoring running jobs due to --ignore-running-jobs flag"
    fi
  fi
}

_test_system_fabric_diag() {
  local TEST_FABRIC_DIAG_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME test-system-fabric-diag [--dry-run] [--skip-fabric-diag]
                                       [--system-cred <user:password>]
                                       [--wafer-config <wafer_config>]
                                       [--output-dir <dirname>]
                                       [--wafer-diag-opts <wafer_diag_opts>]
                                       [ all | SYSTEM... ]

    Run fabric diag and wafer-diag test against given systems after checking the systems
    are not in use and taken out of scheduling.

        --dry-run: optional flag to run the diag flow preflight checks only
        --skip-fabric-diag: optional flag to skip fabric diag (run wafer-diag only)
        --system-cred <user:password>: Username and password to access system manager, if
                                       wafer configuration needs to be accessed
        --wafer-config <wafer_config>: Name of the wafer fabric configuration to test
                                       (Tests the current config if not provided)
        --wafer-diag-opts <opts>: Extra options to be passed to wafer-diag
        --output-dir: Existing output directory (uses a /tmp directory if not specified)

        all | SYSTEM...: arguments specifying to run diag on all systems or named systems

    To remove from scheduling, execute:

      $0 update-system --name=<system_name> --state=error --note='test fabric diag' --user=<username>

    and after running, allow the system to be scheduled again with

      $0 update-system --name=<system_name> --state=ok

    If needed, you can override the cbcore image by setting envvar FABRIC_DIAG_CBCORE_IMAGE
EOM
  )
  if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "$TEST_FABRIC_DIAG_USAGE"
    return 0
  fi

  # Runs the fabric_diag and wafer-diag test in a cbcore container on one or more systems
  # 0/ find cbcore image
  # 1/ check preconditions: exists, out of k8s scheduling, no active job locked
  # 2/ execute fabric diag and wafer-diag tests
  dry_run=false
  skip_fabric_diag=false
  credentials=""
  wafer_config=""
  output_dir=""
  wafer_diag_opts=""
  while [[ "$1" =~ ^- && ! "$1" == "--" ]]; do
    case $1 in
    --dry-run)
      dry_run=true
      shift # past argument=value
      ;;
    --skip-fabric-diag)
      skip_fabric_diag=true
      shift # past argument=value
      ;;
    --system-cred)
      credentials="$2"
      shift 2
      ;;
    --wafer-config)
      wafer_config="$2"
      shift 2
      ;;
    --output-dir)
      output_dir="$2"
      shift 2
      ;;
    --wafer-diag-opts)
      wafer_diag_opts="$2"
      shift 2
      ;;
    --help)
      echo "$TEST_FABRIC_DIAG_USAGE"
      return 0
      ;;
    -h)
      echo "$TEST_FABRIC_DIAG_USAGE"
      return 0
      ;;
    --*)
      echo "Unknown option $i"
      echo "$TEST_FABRIC_DIAG_USAGE"
      exit 1
      ;;
    *)
      break
      ;;
    esac
  done

  if [ -z "$credentials" ]; then
    if [ -n "$wafer_config" ]; then
      echo "Error: --wafer-config was provided, but '--system-cred <user:password>' was not."
      exit 1
    fi
    echo "Warning: '--system-cred <user:password>' not provided. Will not check system manager for wafer config or system status."
  fi

  set -e
  if [ -z "$output_dir" ]; then
    output_dir=$(mktemp -d)
  fi
  systems=()
  container_ids=()
  _cleanup() {
    for cid in "${container_ids[@]}"; do
      nerdctl -n k8s.io container kill "${cid}" &>/dev/null || true
      nerdctl -n k8s.io container rm "${cid}" &>/dev/null || true
    done

    # cleanup on completion since binaries produced can be large, 5Gi+
    if [ -n "$(ls -A "${output_dir}")" ]; then
      find "${output_dir}"/* -mindepth 1 -type f -not -name "*.txt" -not -name "*.pbbin" -not -name "*.log" -not -name "*.json" -exec rm -f {} \; 2>/dev/null
    fi
  }
  trap _cleanup RETURN EXIT SIGINT

  _echo_cluster_cfg_json >"${output_dir}/.cluster.json"
  if [ "$1" == "all" ]; then
    for s in $(jq -r '.systems[] | .name' ${output_dir}/.cluster.json); do systems+=("$s"); done
  else
    for s in "$@"; do systems+=("$s"); done
  fi
  if [ "${#systems[@]}" = "0" ]; then
    echo "error: no system names given as arguments"
    return 1
  fi

  echo "resolving cbcore image..."
  if [ -z "${FABRIC_DIAG_CBCORE_IMAGE}" ]; then
    FABRIC_DIAG_CBCORE_IMAGE=$(helm get values -n"$SYSTEM_NAMESPACE" cluster-server -ojson | jq .wsjob.image -r)
    FABRIC_DIAG_CBCORE_IMAGE="registry.local/${FABRIC_DIAG_CBCORE_IMAGE//*cbcore/cbcore}"
  fi
  echo "pulling cbcore image '${FABRIC_DIAG_CBCORE_IMAGE}'..."
  if ! nerdctl -n k8s.io image pull "${FABRIC_DIAG_CBCORE_IMAGE}" &>"${output_dir}/.cbcore.out"; then
    cat "${output_dir}/.cbcore.out"
    echo "Set FABRIC_DIAG_CBCORE_IMAGE to an available cbcore image and retry"
    return 1
  fi

  if ! _check_systems_idle "${systems[@]}" && ! $dry_run; then
    return 1
  fi

  cm_ips=()
  for system in "${systems[@]}"; do
    cm_ip=$(jq -e -r --arg name "$system" '.systems[] | select(.name == $name) | .controlAddress' "${output_dir}/.cluster.json")
    cm_ips+=("${cm_ip}")
    mkdir "${output_dir}/$system"

    # The support for these commands to check/set the system wafer config was added in 2.4.
    # Catch errors here so that we can print an informative error message before exiting.
    if [ -n "$credentials" ]; then
      set +e
      # If user specified a wafer config, check that the current active config matches it.
      if [ -n "$wafer_config" ]; then
        active_config=$(csadm.sh operate-systems "$system" "$credentials" wse_config_show | grep -v "csadm_operate_systems" | jq -r ".payload.${system}.active_config")
        if [ -z "$active_config" ] || [ "$active_config" = "N/A" ]; then
          echo "Error: Couldn't determine active wafer config. Does the system software have version 2.4 or higher?"
          return 1
        fi
        if [ "$active_config" != "$wafer_config" ]; then
          echo "Requested wafer config $wafer_config but active config is $active_config"
          wafer_set_cmd="./csadm.sh operate-systems ${system} ${credentials} wse_config_set ${wafer_config}"
          echo "Use command $wafer_set_cmd to set it, or log into the system to set it manually."
          return 1
        else
          echo "Confirmed active wafer config: $active_config"
        fi
      fi

      # Check that the system status is OK
      status=$(csadm.sh operate-systems "$system" "$credentials" system_show | grep -v "csadm_operate_systems" | jq -r ".payload.${system}")
      if [ "$status" != "OK" ]; then
        echo "System status is not OK"
        return 1
      fi

      # Restore exit on error behavior
      set -e
    fi
  done

  echo "starting diag_tests, writing output to ${output_dir}..."
  for ((i = 0; i < ${#systems[@]}; i++)); do
    cmd_array=(
      "set -o pipefail;"
      "set -e;"
    )
    if [ $skip_fabric_diag = false ]; then
      cmd_array+=(
        "mkdir fabric_diag && cd fabric_diag;"
        "fabric_diag --cmaddr ${cm_ips[i]} -o . ;"
        "cd .. && rm -rf fabric_diag;"
      )
    fi
    cmd_array+=(
      "mkdir wafer_diag && cd wafer_diag;"
      "wafer-diag --cmaddr ${cm_ips[i]} ${wafer_diag_opts} | tee out.log;"
      "cd .. && rm -rf wafer_diag;"
    )
    cmd="${cmd_array[*]}"
    cmd="${cmd//;/; }"

    if $dry_run; then
      echo "Dry run - would run command: ${cmd}"
      cmd=true # dummy cmd
    fi

    devices=""
    if [ -c /dev/infiniband/uverbs0 ]; then
      devices="--device /dev/infiniband/uverbs0"
    fi

    if [ -c /dev/infiniband/rdma_cm ]; then
      devices="${devices} --device /dev/infiniband/rdma_cm"
    fi

    # clean up stale container
    nerdctl -n k8s.io stop "diag-${systems[i]}" &>/dev/null || true
    nerdctl -n k8s.io rm "diag-${systems[i]}" &>/dev/null || true

    cid=$(nerdctl -n k8s.io run -d --name="diag-${systems[i]}" ${devices} --network=host -v "${output_dir}/${systems[i]}:/host" -w /host "${FABRIC_DIAG_CBCORE_IMAGE}" "$cmd")
    container_ids+=("$cid")
  done

  echo "awaiting diag_test completion (this will take 1-2h)... "
  failures=0
  recommendations=()
  for ((i = 0; i < ${#systems[@]}; i++)); do
    logpath="${output_dir}/${systems[i]}/"
    exit_code=$(nerdctl -n k8s.io container wait "${container_ids[i]}")

    if [ "${exit_code}" -ne "0" ]; then
      echo "${systems[i]} failed fabric diag, logs and artifacts in ${logpath}"
      nerdctl -n k8s.io logs "${container_ids[i]}" || true
      # If test failed in fabric_diag, show its log. wafer-diag logs already
      # print to stdout
      if [ -f "${logpath}/fabric_diag/fabric_diag.out" ]; then cat "${logpath}/fabric_diag/fabric_diag.out"; fi
      failures=$((failures + 1))
    else
      echo "${systems[i]} passed fabric diag, logs in ${logpath}"
      recommendations+=("$0 update-system --name=$system --state=ok")
    fi
  done

  if [ "${#recommendations[@]}" -gt 0 ]; then
    echo "re-add systems to scheduling:"
    for recommendation in "${recommendations[@]}"; do
      echo "  $recommendation"
    done
  fi

  echo "done, $failures failure(s)"

  return $failures
}

_reset_storage() {
  storage_reset_tool="$(dirname $0)/csadm/storage-reset.sh"
  if [ ! -f "$storage_reset_tool" ]; then
    echo "error: $storage_reset_tool not found"
    echo "please deploy cluster tools before proceeding"
    return 1
  fi
  $storage_reset_tool "$@"
}

_install_preflight() {
  exit_on_fail=$1
  echo "running preflight checks" >&2

  if ! SKIP_CHECK=1 source pkg-common.sh; then
    echo "failed to source pkg-common.sh" >&2
    return 1
  fi

  local failures=()

  if ! get_cluster_config &>/dev/null; then
    failures+=("failed to get cluster config")
  fi

  if [ "${#failures[@]}" != "0" ]; then
    if ! $exit_on_fail; then
      return 0
    fi
    echo "preflight checks failed:" >&2
    for failure in "${failures[@]}"; do
      echo "  $failure" >&2
    done
    return 1
  else
    echo "preflight checks passed" >&2
  fi
}

_install_validation() {
  if [ ! -x /opt/cerebras/tools/network-validation.sh ]; then
    echo "network validation script is not present" >&2
    return 1
  fi
  if [ ! -x /opt/cerebras/tools/software-validation.sh ]; then
    echo "software validation script is not present" >&2
    return 1
  fi

  echo "Starting network validation, this will take a few minutes..."
  if [ -n "$INCREMENTAL_DIR" ]; then
    inc_arg="-i"
  fi
  if ! /opt/cerebras/tools/network-validation.sh ${inc_arg} &>/tmp/.csadm.out; then
    cat /tmp/.csadm.out >&2
    echo "network validation failed" >&2
    return 1
  fi
  echo "network validation passed" >&2

  echo "Starting software validation..."
  if ! /opt/cerebras/tools/software-validation.sh &>/tmp/.csadm.out; then
    cat /tmp/.csadm.out >&2
    echo "software validation failed" >&2
    return 1
  fi
  echo "software validation passed" >&2
}

apply_user_auth_secret() {
  local namespace=$1
  local secret=$2

  cat <<EOM | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: kube-user-auth
  namespace: $namespace
stringData:
  secret: "$secret"
EOM
}

rotate_user_auth_secret() {
  local ROTATE_USER_AUTH_SECRET_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME rotate-user-auth-secret

  Replace the existing user authentication secret with a new one. The cluster admin
  also needs to copy this new secret to all user nodes to replace the old one.
EOM
  )

  if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "$ROTATE_USER_AUTH_SECRET_USAGE"
    return 0
  fi

  num_bytes=512
  random_bytes=$(head -c $num_bytes /dev/urandom | base64)

  # find all namespaces that have the secret kube-user-auth, and replace the secret.
  namespaces=$(kubectl get namespaces -luser-namespace -o json | jq -r '.items[] | .metadata.name' |
    xargs -I {} sh -c 'kubectl get secret --namespace={} --ignore-not-found | \
    grep -q 'kube-user-auth' && echo {}')
  for namespace in $namespaces; do
    apply_user_auth_secret "$namespace" "$random_bytes"
  done

  cat <<EOM
New secret created. Run the following steps to save the new secret in 'user-auth-secret' file and chmod 0640:

  kubectl -n $SYSTEM_NAMESPACE get secret kube-user-auth -ojsonpath='{.data.secret}' | base64 -d > user-auth-secret
  chmod 0640 user-auth-secret

EOM
}

_validate_hardware_network() {
  local VALIDATE_HARDWARE_NW_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME validate-hardware-network [--destination-type=<systems|nodes>]

    Run a ping check from servers in the cluster to applicable destinations.

    Supported optional destination types:
      systems: check ping connectivity from all nodes to all system 100G ports and VIPs
      nodes: check ping connectivity from all nodes to all node 100G ports
    If this option is not specified, connectivity will be checked to both
EOM
  )
  local dest_type=""
  for i in "$@"; do
    case $i in
    --destination-type=*)
      dest_type="${i#*=}"
      shift # past argument=value
      ;;
    --help)
      echo "$VALIDATE_HARDWARE_NW_USAGE"
      return 0
      ;;
    -h)
      echo "$VALIDATE_HARDWARE_NW_USAGE"
      return 0
      ;;
    --*)
      echo "Unknown option $i"
      echo "$VALIDATE_HARDWARE_NW_USAGE"
      exit 1
      ;;
    *) ;;
    esac
  done

  declare -A dest_types=(["systems"]=1 ["nodes"]=1)
  if [[ -n "$dest_type" ]] && [[ -z ${dest_types[$dest_type]} ]]; then
    echo "Unknown destination type: $dest_type, allowed destination types: systems|nodes"
    exit 1
  fi

  local NW_CFG=/opt/cerebras/cluster/network_config.json
  if ! [ -f $NW_CFG ]; then
    echo "Network configuration information not found at $NW_CFG"
    exit 1
  fi

  local NW_TOOL_DIR=/opt/cerebras/cluster-deployment/network_config
  local NW_TOOL=$NW_TOOL_DIR/network_config/tool.py
  if ! [ -f $NW_TOOL ]; then
    echo "Network validation tool has not been installed at $NW_TOOL_DIR on this cluster"
    exit 1
  fi

  local dtype_arg=""
  if [ "$dest_type" == "systems" ]; then
    dtype_arg="--destination_type systems"
    echo "Running ping check to systems"
  elif [ "$dest_type" == "nodes" ]; then
    dtype_arg="--destination_type nodes"
    echo "Running ping check to nodes"
  else
    echo "Running ping check to systems and nodes"
  fi
  echo "This may take several minutes..."

  pushd $NW_TOOL_DIR >/dev/null
  python3 -m network_config.tool node_tasks -c $NW_CFG $dtype_arg ping_check
  popd >/dev/null
}

_assert_system_idle_state() {
  tmp_dir=$(mktemp -d)
  systems=()

  _echo_cluster_cfg_json >"${tmp_dir}/.cluster.json"
  if [ "$1" == "all" ]; then
    for s in $(jq -r '.systems[] | .name' ${tmp_dir}/.cluster.json); do systems+=("$s"); done
  else
    for s in "$@"; do systems+=("$s"); done
  fi

  rm -rf $tmp_dir

  if [ "${#systems[@]}" = "0" ]; then
    echo "error: no system names given as arguments"
    return 1
  else
    echo "Checking systems: ${systems[@]}"
  fi

  if ! _check_systems_idle "${systems[@]}"; then
    return 1
  fi
}

_upload_system_images() {
  local system_images=("$@")
  success_count=0
  fail_count=0

  for system_image in "${system_images[@]}"; do
    IFS=':' read -r dns_name file_path <<<"$system_image"
    filename=$(basename "$file_path")
    cmd="curl -k -u $credentials https://${dns_name}/upgradeBundle/${filename} --upload-file ${file_path}"
    eval "$cmd"

    if [ $? -eq 0 ]; then
      echo "Upload of '$filename' to '$dns_name' successful."
      success_count=$((success_count + 1))
    else
      echo "Upload of '$filename' to '$dns_name' failed."
      fail_count=$((fail_count + 1))
    fi
  done

  echo "Upload complete. $success_count successful, $fail_count failed."
  if [ $fail_count -gt 0 ]; then
    return 1
  fi

  if [ $fail_count -gt 0 ] || [ $image_error_count -gt 0 ]; then
    return 1
  fi
}

install() {
  local INSTALL_USAGE=$(
    cat <<EOM
Usage:
  $SCRIPT_NAME install MANIFEST_FILE [--update-config[=CONFIG_FILE]]
      [--preflight] [--skip-k8s] [--remove-only] [--validate] [--debug]

  Install cluster components described in the given manifest json file.
  If the CONFIG_FILE contains
  MANIFEST_FILE: path to the manifest json file

        --update-config[=CONFIG_FILE]: Update the cluster config with the network
                       config. If CONFIG_FILE is not specified, the default
                       /opt/cerebras/cluster/network_config.json will be used.
                       Note: if this flag not specified, cluster config wont be updated.

        --preflight:   Run a preflight check before installing the components
                       in the manifest

        --k8s:         Deprecated, not in use any more, keep here to avoid erroring out.

        --skip-k8s:    Skip reconcile on k8s cluster creation/update/deletion.

        --remove-only: Only run through k8s reconcile step and config update, used
                       for removing nodes and systems from the cluster.

        --validate:    Validate the state of the cluster if the manifest is
                       successfully installed

        --debug:       Run in debug mode, which will print all output to stdout

        --yes:         Assume 'yes' to all confirmation prompts
EOM
  )
  if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "$INSTALL_USAGE"
    return 0
  fi

  local manifest_file=$1
  local debug_mode=false
  local update_config=""
  local preflight=false
  local validate=false
  local k8s=true
  local lockfile
  local yes=false
  local yes_flag=""
  local remove_only_flag=""
  shift

  for arg in "$@"; do
    if [ "$arg" = "--debug" ]; then
      debug_mode=true
    elif [ "$arg" = "--preflight" ]; then
      preflight=true
    elif [ "$arg" = "--k8s" ]; then
      k8s=true
    elif [ "$arg" = "--skip-k8s" ]; then
      k8s=false
    elif [ "$arg" = "--remove-only" ]; then
      remove_only_flag="--remove-only"
    elif [ "$arg" = "--validate" ]; then
      validate=true
    elif [ "$arg" = "--update-config" ]; then
      update_config=/opt/cerebras/cluster/network_config.json
    elif [[ "$arg" =~ ^--update-config=.*$ ]]; then
      update_config=${arg#--update-config=}
    elif [[ "$arg" =~ ^--namespace=.*$ ]]; then
      namespace=${arg#--namespace=}
      echo "namespace: $namespace"
    elif [[ "$arg" =~ ^--component=.*$ ]]; then
      echo "component: ${arg#--component=}"
    elif [ "$arg" = "--yes" ]; then
      yes=true
      yes_flag="-y"
    else
      echo "unknown argument $arg" >&2
      exit 1
    fi
  done

  if [ ! -f "$manifest_file" ]; then
    echo "manifest $manifest_file does not exist"
    exit 1
  fi
  manifest_dir="$(dirname "$manifest_file")"

  # Check if skipK8s is set to true in the manifest file
  if jq -e '.skipK8s == true' "$manifest_file" >/dev/null; then
    k8s=false
  fi

  incremental_install=false
  run_update_config_only=false
  # allow k8s reconcile to skip check for mostly new clusters
  if $k8s && jq -e '.componentPaths[] | select(test("k8s/.*"))' "$manifest_file" >/dev/null; then
    preflight=false
  fi

  if [ -n "$update_config" ] && [ -f $CLUSTER_CONFIG ]; then
    # Always run the incremental install's list helper first to determine
    # what kind of upgrade should be performed.
    workdir=$(mktemp -d)
    export INCREMENTAL_DIR=$workdir

    if $debug_mode; then
      echo "DEBUG: Incremental install scratch dir: $INCREMENTAL_DIR"
    fi
    if ! [ -f csadm/csadm_list_helper.py ]; then
      echo "$(dirname $0)/csadm/csadm_list_helper.py does not exist" >&2
      exit 1
    fi

    $PY3 csadm/csadm_list_helper.py --workdir=$workdir --netjson=${update_config}
    if [ $? -ne 0 ]; then
      echo "csadm_list_helper.py failed, exiting" >&2
      exit 1
    fi

    # Print some debug information about added/removed resources
    if [ "$debug_mode" == "true" ]; then
      function __print_change() {
        if [ -s "$2" ]; then
          echo "$1:"
          cat "$2" | sed 's/^/  /'
        fi
      }
      __print_change "nodes added" "$INCREMENTAL_DIR/new_all"
      __print_change "nodes removed" "$INCREMENTAL_DIR/k8_worker_remove"
      __print_change "systems added" "$INCREMENTAL_DIR/systems_added"
      __print_change "systems removed" "$INCREMENTAL_DIR/systems_removed"
    fi

    # If there are new nodes, do an incremental install
    if [ -s $INCREMENTAL_DIR/new_all ]; then
      nodecount=$(wc -l $INCREMENTAL_DIR/new_all | cut -d ' ' -f 1)
      echo "$nodecount new nodes detected in $update_config."
      echo "$SCRIPT_NAME will perform an incremental install, only installing"
      echo "to the new nodes."
      incremental_install=true
      if [ "$remove_only_flag" == "--remove-only" ]; then
        echo "ERROR: --remove-only flag is set, but there are unexpected new nodes found. Exiting."
        exit 1
      fi
    elif [ -s $INCREMENTAL_DIR/k8_worker_remove ]; then
      norm=$(wc -l $INCREMENTAL_DIR/k8_worker_remove | cut -d ' ' -f 1)
      echo "No new nodes, and $norm removed nodes detected in $update_config."
      if [ -s $INCREMENTAL_DIR/systems_added ] || [ -s $INCREMENTAL_DIR/systems_removed ]; then
        sysadd=$(wc -l $INCREMENTAL_DIR/systems_added | cut -d ' ' -f 1)
        sysrm=$(wc -l $INCREMENTAL_DIR/systems_removed | cut -d ' ' -f 1)
        echo "$sysadd new systems, and $sysrm removed systems detected in $update_config."
      fi
      echo "$SCRIPT_NAME will update the cluster config and remove nodes from k8s."
      incremental_install=true
      remove_only_flag="--remove-only"
    elif [ -s $INCREMENTAL_DIR/systems_added ] || [ -s $INCREMENTAL_DIR/systems_removed ]; then
      sysadd=$(wc -l $INCREMENTAL_DIR/systems_added | cut -d ' ' -f 1)
      sysrm=$(wc -l $INCREMENTAL_DIR/systems_removed | cut -d ' ' -f 1)
      echo "No new nodes, $sysadd new systems, and $sysrm removed systems detected in $update_config."
      echo "$SCRIPT_NAME will only update the cluster config and will not install anything."
      run_update_config_only=true
    else
      echo "No new nodes detected in $update_config."
      echo "$SCRIPT_NAME will install to all nodes in ${CLUSTER_CONFIG}."
      # unset INCREMENTAL_DIR, to make sure non incremental deploy path is taken.
      export INCREMENTAL_DIR=
    fi
    if ! $yes; then
      read -n 1 -p "Proceed? (y/N) " answer
      echo ""
      if [ "$answer" != "y" ] && [ "$answer" != "Y" ]; then
        exit 0
      fi
    fi
  fi

  if $run_update_config_only; then
    if $validate; then
      if ! _install_validation; then
        echo "Validation failed, will not proceed with cluster config update."
        exit 1
      fi
    fi

    if ! update_cluster_config "$update_config"; then
      echo "failed to update cluster config"
      exit 1
    else
      exit 0
    fi
  fi

  if ! $incremental_install && [ -n "$update_config" ]; then
    echo "updating cluster config from $update_config"
    if ! update_cluster_config "$update_config"; then
      echo "failed to update cluster config"
      exit 1
    fi
  fi

  # if --preflight, and the preflight checks fail, exit with error
  _install_preflight $preflight

  # only reconcile k8s step if enabled and k8s step involved
  if $k8s && jq -e '.componentPaths[] | select(test("k8s/.*"))' "$manifest_file" >/dev/null; then
    if ! k8s/k8_init.sh reconcile "$yes_flag" "$remove_only_flag"; then
      echo "error: failed to reconcile kubernetes cluster"
      exit 1
    fi
    if [ "$remove_only_flag" == "--remove-only" ]; then
      if ! update_cluster_config "$update_config"; then
        echo "failed to update cluster config"
        exit 1
      fi
      exit 0
    fi
  fi

  if $incremental_install; then
    # the update config step should happen last for incremental
    # Fail if we can't label the node - probably the user forgot --k8s
    for newnode in $(cat ${INCREMENTAL_DIR}/new_all); do
      kubectl label --overwrite=true node ${newnode} cerebras/incremental-new=''
    done
  fi

  local failed_components=()
  local completed_components=()

  for component_path in $(jq -r '.componentPaths | to_entries[] | "\(.value)"' "$manifest_file"); do
    component_name=$(basename $component_path)

    echo "$(date +%FT%T.%3N%z): installing ${component_name}"

    lockfile="/var/lock/csadm.${component_name}.lock"
    if [[ "$component_name" =~ cluster-server|job-operator ]] && [ -n "$namespace" ]; then
      lockfile="/var/lock/csadm.${component_name}.${namespace}.lock"
    fi

    pushd $manifest_dir &>/dev/null
    date +%FT%T.%3N%z >".${component_name}.log"
    (
      if ! flock -n 9; then
        echo "await install lock ${lockfile} for a max of 30 minutes" | tee -a ".${component_name}.log"
        echo "lock details: $(cat ${lockfile})" | tail -n1 | tee -a ".${component_name}.log"
        if ! flock -w1800 9; then
          echo "failed to obtain lock ${lockfile} after 30 minutes, exiting" | tee -a ".${component_name}.log"
          exit 1
        fi
      fi
      # add this proc's details to lock to track history of deploys for debugging while truncating the file to last 1000 lines
      sed -i -e :a -e '$q;N;999,$D;ba' "$lockfile"
      echo "LOCK   $(date -u +%s) pid=$$ user=$(whoami) user_dir=$(pwd -P)" >>"$lockfile"
      onexit() { echo "UNLOCK $(date -u +%s) pid=$$ user=$(whoami) user_dir=$(pwd -P)" >>"$lockfile"; }
      trap onexit EXIT

      exit_failure() {
        echo "$(date +%FT%T.%3N%z): install ${component_name} failed" | tee -a ".${component_name}.log"
        date +%FT%T.%3N%z >>".${component_name}.log"
        failed_components+=("$component_name")

        timestamp=$(date +%Y%m%d%H%M%S)
        retry_manifest_file="manifest.retry${timestamp}.json"
        # Create a JSON array from the completed_components shell array
        completed_json=$(jq -n --arg comps "${completed_components[*]}" '$comps | split(" ")')

        # Process the manifest: filter out any component path found in the completed_json array,
        # and add the "skipK8s" field to true.
        jq --argjson completedPaths "$completed_json" '
          .componentPaths |= map(
            select(
             (split("/") | last) as $name
              | ($completedPaths | index($name)) | not
            )
          )
          | .skipK8s = true
        ' "$manifest_file" > "$retry_manifest_file"

        echo "Some components failed to install. Please rerun the command with $retry_manifest_file. Example: [$SCRIPT_NAME install $retry_manifest_file]"
        exit 1
      }

      if $debug_mode; then
        namespace="$namespace" /bin/bash "${component_path}" 2>&1 | tee -a ".${component_name}.log"
        [[ ${PIPESTATUS[0]} -eq 0 ]] || exit_failure
      else
        if ! namespace="$namespace" /bin/bash "${component_path}" &>>".${component_name}.log"; then
          echo "$(date +%FT%T.%3N%z): install ${component_name} failed, log messages:"
          cat ".${component_name}.log"
          exit_failure
        fi
      fi
    ) 9>>"$lockfile" || exit 1

    completed_components+=("$component_name")
    popd &>/dev/null
  done

  if $validate; then
    if ! _install_validation; then
      echo "Validation failed, will not proceed with cluster config update."
      exit 1
    fi
  fi

  if $incremental_install; then
    for newnode in $(cat ${INCREMENTAL_DIR}/new_all); do
      kubectl label node ${newnode} cerebras/incremental-new-
    done

    # TODO: When we track partially successful deploys, don't recreate the config again-
    # use the config in the incremental dir, but delete any nodes that had a failure
    if ! update_cluster_config "$update_config"; then
      echo "failed to update cluster config"
      exit 1
    fi
  fi
}

_operate_systems() {
  if [ "$#" -lt 2 ]; then
    echo "error: no system selection or system credentials provided. e.g., ('all' or 'sys1,sys2') and 'user:pwd'"
    return 1
  fi

  system_selection="$1"
  system_cred="$2"
  shift 2
  py_args=("$@")

  tmp_dir=$(mktemp -d)
  systems=()

  _echo_cluster_cfg_json >"${tmp_dir}/.cluster.json"
  if [ "$system_selection" == "all" ]; then
    while IFS= read -r s; do systems+=("$s"); done < <(jq -r '.systems[] | .name' "${tmp_dir}/.cluster.json")
  else
    IFS=',' read -ra sys_array <<<"$system_selection"
    for s in "${sys_array[@]}"; do systems+=("$s"); done
  fi

  rm -rf $tmp_dir

  if [ "${#systems[@]}" = "0" ]; then
    echo "error: no system names given as arguments"
    return 1
  fi

  SCRIPT_PATH="csadm/csadm_operate_systems.py"
  if ! [ -f "$SCRIPT_PATH" ]; then
    echo "error: required file $SCRIPT_PATH does not exist" >&2
    exit 1
  fi

  SCRIPT_DIR=$(dirname "$SCRIPT_PATH")
  VENV_DIR="$SCRIPT_DIR/venv"
  PYTHON_BIN="$VENV_DIR/bin/python"

  if [ ! -d "$VENV_DIR" ]; then
    python -m venv "$VENV_DIR"
    if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
      "$PYTHON_BIN" -m pip install -r "$SCRIPT_DIR/requirements.txt"
    else
      echo "error: requirements.txt not found in $SCRIPT_DIR" >&2
      exit 1
    fi
  fi

  system_list=$(
    IFS=','
    echo "${systems[*]}"
  )

  "$PYTHON_BIN" "$SCRIPT_PATH" --system_list $system_list --system_cred "$system_cred" ${py_args[@]} | jq
}

_check_image_mismatch() {
  local dns_name
  for dns_name in "${systems[@]}"; do
    system_exists=$(echo "$output" | jq -r --arg sys "$dns_name" '.payload | has($sys)')

    if [ "$system_exists" != "true" ]; then
      echo "Error: System '$dns_name' not found in payload."
      image_mismatch_count=$((image_mismatch_count + 1))
      continue
    fi

    images_in_payload=$(echo "$output" | jq -r --arg sys "$dns_name" '.payload[$sys][]')

    if [ -z "$images_in_payload" ]; then
      echo "Error: No images found for system '$dns_name' in payload."
      image_mismatch_count=$((image_mismatch_count + 1))
      continue
    fi

    image_in_system_images="${system_images_map[$dns_name]}"

    image_found_in_payload=false
    while IFS= read -r image_payload; do
      if [[ "$image_payload" == "$image_in_system_images" ]]; then
        image_found_in_payload=true
        break
      fi
    done <<<"$images_in_payload"

    if ! $image_found_in_payload; then
      echo "Error: Image mismatch for system '$dns_name'. Expected '$image_in_system_images', but it was not found in payload."
      image_mismatch_count=$((image_mismatch_count + 1))
    fi
  done
}

_smart_uscd_update() {
  systems=()
  directory=""
  credentials=""

  # optional args
  do_precheck=true
  do_update=true

  while [ "$#" -gt 0 ]; do
    case "$1" in
    -d)
      directory="$2"
      shift 2
      ;;
    --system-cred)
      credentials="$2"
      shift 2
      ;;
    --image-upload-only)
      do_precheck=false
      shift
      ;;
    --precheck-only)
      do_update=false
      shift
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "Unknown option: $1"
      return 1
      ;;
    *)
      break
      ;;
    esac
  done

  if [ -z "$credentials" ]; then
    echo "error: '--system-cred <user:password>' is required"
    return 1
  fi

  if [ -z "$directory" ]; then
    echo "error: '-d <directory>' is required"
    return 1
  elif [[ ! "$directory" = /* ]]; then
    echo "error: '$directory' is not an absolute path"
    return 1
  elif [ ! -d "$directory" ]; then
    echo "error: '$directory' is not a directory"
    return 1
  fi

  if [ "$1" == "all" ]; then
    tmp_dir=$(mktemp -d)
    _echo_cluster_cfg_json >"${tmp_dir}/.cluster.json"
    for s in $(jq -r '.systems[] | .name' "${tmp_dir}/.cluster.json"); do
      systems+=("$s")
    done
    rm -rf "$tmp_dir"
  else
    IFS=',' read -ra sys_array <<<"$1"
    for s in "${sys_array[@]}"; do
      systems+=("$s")
    done
  fi

  if [ "${#systems[@]}" = "0" ]; then
    echo "error: no system names given as arguments"
    return 1
  fi

  declare -a system_images
  image_error_count=0

  for dns_name in "${systems[@]}"; do
    pattern="wse-info-system-${dns_name}-*.tar.gz"
    mapfile -t found_files < <(find "$directory" -maxdepth 1 -type f -name "$pattern")

    if [ ${#found_files[@]} -ne 1 ]; then
      echo "Error: Expected exactly one file for system '$dns_name' in directory '$directory', found ${#found_files[@]}."
      image_error_count=$((image_error_count + 1))
      continue
    fi

    file_path="${found_files[0]}"
    system_images+=("${dns_name}:${file_path}")
  done

  if [ $image_error_count -gt 0 ]; then
    echo "Error: $image_error_count systems had image errors."
    return 1
  fi

  # Try to upload images
  if ! _upload_system_images "${system_images[@]}"; then
    echo "error: failed to upload system images"
    return 1
  fi

  # Precheck
  if ! $do_precheck; then
    echo "Precheck has been skipped."
    return 0
  fi

  echo "Check system idle state for systems: ${systems[@]}"
  if ! output=$(_check_systems_idle "${systems[@]}" 2>&1); then
    echo "$output"
    echo "Failed system idle state check for systems: ${systems[@]}"
    return 1
  fi

  echo "Idle state check complete. Check system images for systems: ${systems[@]}"
  system_list=$(
    IFS=,
    echo "${systems[*]}"
  )
  if ! output=$(_operate_systems "$system_list" "$credentials" software_image_show 2>&1); then
    echo "$output"
    echo "Failed system image check for systems: ${systems[@]}"
    return 1
  else
    echo "System image check complete."
  fi

  declare -A system_images_map
  for entry in "${system_images[@]}"; do
    dns_name="${entry%%:*}"
    file_path="${entry#*:}"
    file_name=$(basename "$file_path")
    system_images_map["$dns_name"]="$file_name"
  done

  declare -a image_list
  for s in "${systems[@]}"; do
    image="${system_images_map[$s]}"
    if [ -z "$image" ]; then
      echo "Error: No image found for system '$s'"
      return 1
    fi
    image_list+=("$image")
  done

  image_list_str=$(
    IFS=,
    echo "${image_list[*]}"
  )

  image_mismatch_count=0
  _check_image_mismatch

  if [ $image_mismatch_count -gt 0 ]; then
    echo "Error: $image_mismatch_count systems had image mismatches."
    return 1
  else
    echo "Target images present on all systems."
  fi

  echo "Precheck completed successfully."

  echo "Update USCD images for systems: ${systems[@]}? This could take ~20 minutes"
  if ! $do_update; then
    echo "Update has been skipped."
    return 0
  fi

  echo "Updating USCD images for systems: $system_list with images $image_list_str"

  echo "Invoke _operate_systems $system_list $credentials standby_update_activate $image_list_str"

  if ! output=$(_operate_systems "$system_list" "$credentials" standby_update_activate "$image_list_str" 2>&1); then
    echo "$output"
    echo "Failed to update USCD images for systems: ${systems[@]}"
    return 1
  else
    echo "USCD image update complete."
  fi
}

### CLUSTER-OPERATOR CONTACT MANAGEMENT ###

# --- Configuration ---
CLUSTER_OPERATOR_CONTACTS_SECRET_NAME="cluster-operator-contacts"
CLUSTER_OPERATOR_CONTACTS_NAMESPACE="prometheus"

# Validation constants
VALID_ALERT_TYPES=("job" "cluster" "all")

_validate_severity_threshold() {
  local severity="$1"
  # Check if it's a number between 1 and 5
  if [[ "$severity" =~ ^[1-5]$ ]]; then
    return 0
  fi
  return 1
}

_validate_alert_types() {
  local alert_type="$1"

  # Check if the type is one of the valid types
  for valid_type in "${VALID_ALERT_TYPES[@]}"; do
    if [ "$alert_type" = "$valid_type" ]; then
      return 0
    fi
  done
  return 1
}

_create_secret() {
  # Check if secret already exists
  if kubectl get secret "$CLUSTER_OPERATOR_CONTACTS_SECRET_NAME" \
    -n "$CLUSTER_OPERATOR_CONTACTS_NAMESPACE" &>/dev/null; then
    echo "Error: Secret $CLUSTER_OPERATOR_CONTACTS_SECRET_NAME already exists"
    return 1
  fi

  # Create initial empty but valid structured configuration
  local config_yaml=$(cat <<EOF
contacts:
  email: []
  slack: []
  pagerduty: []
EOF
)

  # Create the secret with the base64 encoded config
  kubectl create secret generic "$CLUSTER_OPERATOR_CONTACTS_SECRET_NAME" \
    --from-literal=config="$config_yaml" \
    --namespace "$CLUSTER_OPERATOR_CONTACTS_NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -

  echo "Secret $CLUSTER_OPERATOR_CONTACTS_SECRET_NAME created with empty contact lists"
  echo "Use the 'bulk-upload' command to add contacts"
}

_get_config() {
  # Get and decode the secret data
  if ! CONFIG=$(kubectl get secret "$CLUSTER_OPERATOR_CONTACTS_SECRET_NAME" \
    -n "$CLUSTER_OPERATOR_CONTACTS_NAMESPACE" \
    -o jsonpath="{.data.config}" 2>/dev/null | base64 -d); then
    echo "Secret $CLUSTER_OPERATOR_CONTACTS_SECRET_NAME not found in namespace $CLUSTER_OPERATOR_CONTACTS_NAMESPACE" >&2
    return 1
  fi
  echo "$CONFIG"
}

_update_config() {
  local NEW_CONFIG="$1"

  # Get current config and create backup before any changes
  local current_config
  if ! current_config=$(_get_config); then
    echo "Error: Failed to get current configuration"
    return 1
  fi

  # Create backup of current config
  local backup_file="/tmp/contacts-backup-$(date +%Y%m%d-%H%M%S).yaml"
  echo "$current_config" > "$backup_file"
  echo "Created backup at $backup_file"

  # Always validate the new config first
  local validation_output
  validation_output=$(_validate_config "$NEW_CONFIG")
  if [ $? -ne 0 ]; then
    echo "Error: New configuration failed validation:" >&2
    echo "$validation_output" >&2
    return 1
  fi

  # Create/update the secret with the base64 encoded config
  if ! kubectl create secret generic "$CLUSTER_OPERATOR_CONTACTS_SECRET_NAME" \
    --from-literal=config="$NEW_CONFIG" \
    --namespace "$CLUSTER_OPERATOR_CONTACTS_NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -; then
    echo "Error: Failed to update secret. Restoring from backup..." >&2

    if ! _validate_config "$(cat "$backup_file")"; then
      echo "Critical Error: Backup validation failed. Manual intervention required."
      echo "Backup file location: $backup_file"
      return 1
    fi

    if ! kubectl create secret generic "$CLUSTER_OPERATOR_CONTACTS_SECRET_NAME" \
      --from-literal=config="$(cat "$backup_file")" \
      --namespace "$CLUSTER_OPERATOR_CONTACTS_NAMESPACE" \
      --dry-run=client -o yaml | kubectl apply -f -; then
      echo "Critical Error: Failed to restore from backup. Manual intervention required."
      echo "Backup file location: $backup_file"
      return 1
    fi

    echo "Successfully restored from backup"
    return 1
  fi

  echo "Secret $CLUSTER_OPERATOR_CONTACTS_SECRET_NAME updated in namespace $CLUSTER_OPERATOR_CONTACTS_NAMESPACE"
  return 0
}

_list_contacts() {
  local config
  local output_file=""

  # Parse arguments for output file
  if [ "$1" = "--dump-file" ]; then
    output_file="$2"
    if [ -z "$output_file" ]; then
      echo "Error: --dump-file requires a filename argument"
      return 1
    fi
  fi

  if ! config=$(_get_config); then
    return 1
  fi

  if [ -n "$output_file" ]; then
    # For dump-file, output the full config structure with proper formatting
    echo "$config" | yq eval -P '.' - > "$output_file"
    echo "Contacts configuration dumped to $output_file"
    echo "You can use this file with the bulk-upload command"
  else
    # For normal output, just show the contacts section with proper structure
    echo "$config" | yq eval '.contacts' -
  fi
}

_validate_config() {
  local config="$1"
  local errors=()

  # Validate basic YAML structure
  if ! echo "$config" | yq eval '.' &>/dev/null; then
    errors+=("Invalid YAML format")
    return 1
  fi

  # Validate contacts structure exists and is the only top-level key
  if ! echo "$config" | yq eval '.contacts' &>/dev/null; then
    errors+=("Missing 'contacts' root element")
    return 1
  fi
  # Validate each contact type
  for type in email slack pagerduty; do
    local contacts
    local seen_ids=""
    # Get the number of contacts for this type
    local contact_count
    contact_count=$(echo "$config" | yq eval ".contacts.$type | length" -)

    # If there are contacts, iterate through them by index
    if [ "$contact_count" != "0" ] && [ "$contact_count" != "null" ]; then
      for i in $(seq 0 $((contact_count - 1))); do
        local contact
        contact=$(echo "$config" | yq eval ".contacts.$type[$i]" -)

        # Skip empty contacts
        [ -z "$contact" ] && continue

        # Check for duplicate IDs
        local id
        id=$(echo "$contact" | yq eval '.id' -)
        if [ -n "$id" ] && [ "$id" != "null" ]; then
          if echo "$seen_ids" | grep -q "^${id}$"; then
            errors+=("Duplicate ID '$id' found in $type contacts")
          else
            seen_ids="${seen_ids}${id}"$'\n'
          fi
        fi

        # Check required fields
        if [ "$(echo "$contact" | yq eval '.id // "MISSING"' -)" = "MISSING" ]; then
          errors+=("Contact in '$type' section is missing required 'id' field")
        else
          local contact_id
          contact_id=$(echo "$contact" | yq eval '.id' -)

          if [ "$(echo "$contact" | yq eval '.severity_threshold // "MISSING"' -)" = "MISSING" ]; then
            errors+=("Contact '$contact_id' in '$type' section is missing required 'severity_threshold' field")
          fi

          if [ "$(echo "$contact" | yq eval '.alert_type // "MISSING"' -)" = "MISSING" ]; then
            errors+=("Contact '$contact_id' in '$type' section is missing required 'alert_type' field")
          fi
        fi

        # Validate severity threshold
        local severity
        severity=$(echo "$contact" | yq eval '.severity_threshold' -)
        if [ "$severity" != "null" ] && ! _validate_severity_threshold "$severity"; then
          errors+=("Invalid severity threshold '$severity'. Must be a number between 1 and 5")
        fi

        # Validate alert type
        local alert_type
        alert_type=$(echo "$contact" | yq eval '.alert_type' -)
        if [ "$alert_type" != "null" ] && ! _validate_alert_types "$alert_type"; then
          errors+=("Invalid alert type '$alert_type'. Must be one of: ${VALID_ALERT_TYPES[*]}")
        fi

        # Only validate rule_ids if it exists in the contact
        if echo "$contact" | yq eval 'has("rule_ids")' - | grep -q "true"; then
          local rule_ids_type
          rule_ids_type=$(echo "$contact" | yq eval '.rule_ids | type' -)
          if [ "$rule_ids_type" != "array" ]; then
            errors+=("rule_ids must be an array")
          fi
        fi
      done
    fi
  done

  # Validate that 'contacts' is the only top-level key
  local top_level_keys
  top_level_keys=$(echo "$config" | yq eval 'keys | .[]' -)
  while IFS= read -r key; do
    if [ "$key" != "contacts" ]; then
      errors+=("Invalid top-level key '$key'. Only 'contacts' is allowed")
    fi
  done <<< "$top_level_keys"

  # Validate that only allowed keys exist under contacts
  local contact_keys
  contact_keys=$(echo "$config" | yq eval '.contacts | keys | .[]' -)
  while IFS= read -r key; do
    case "$key" in
      "email"|"slack"|"pagerduty")
        ;;
      *)
        errors+=("Invalid key '$key' under contacts. Only 'email', 'slack', and 'pagerduty' are allowed")
        ;;
    esac
  done <<< "$contact_keys"

  if [ ${#errors[@]} -gt 0 ]; then
    echo "Configuration validation failed:"
    printf '%s\n' "${errors[@]}"
    return 1
  fi

  return 0
}

_bulk_upload() {
  local file="$1"
  if [ ! -f "$file" ]; then
    echo "Error: File $file does not exist"
    return 1
  fi

  # Read and update config
  local new_config
  new_config=$(cat "$file")
  _update_config "$new_config"
}

_add_contact() {
  local contact_type="$1"
  local contact_value="$2"
  local alert_type="${3:-cluster}"  # Default to cluster
  local severity_threshold="${4:-3}"  # Default to 3 (medium priority)

  # Validate contact type
  case "$contact_type" in
    "email"|"slack"|"pagerduty")
      ;;
    *)
      echo "Error: Invalid contact type. Must be one of: email, slack, pagerduty"
      return 1
      ;;
  esac

  # Validate alert type
  if ! _validate_alert_types "$alert_type"; then
    echo "Error: Invalid alert type. Must be one of: ${VALID_ALERT_TYPES[*]}"
    return 1
  fi

  # Validate severity threshold
  if ! _validate_severity_threshold "$severity_threshold"; then
    echo "Error: Invalid severity threshold. Must be a number between 1 and 5"
    return 1
  fi

  # Get current config
  local current_config
  if ! current_config=$(_get_config); then
    echo "Error: Failed to get current configuration"
    return 1
  fi

  # Generate new contact entry in JSON format
  local new_contact_json
  new_contact_json=$(jq -n \
    --arg id "$contact_value" \
    --argjson severity_threshold "$severity_threshold" \
    --arg alert_type "$alert_type" \
    '{
      id: $id,
      severity_threshold: $severity_threshold,
      alert_type: $alert_type
    }')

  # Create the updated config
  local updated_config
  updated_config=$(echo "$current_config" | yq eval ".contacts.$contact_type += [$new_contact_json]" -)

  # Update using the centralized function
  _update_config "$updated_config"
}

# Define the shared help text once
CLUSTER_OPERATOR_CONTACTS_HELP=$(cat <<EOM
  Manage the $CLUSTER_OPERATOR_CONTACTS_SECRET_NAME Secret in the $CLUSTER_OPERATOR_CONTACTS_NAMESPACE namespace.
  This Secret stores contact information for cluster operator notifications in an encrypted format.

  Actions:
    create           - Initialize an empty contacts configuration
    list-contacts    - List all contacts with their full attributes
                      Use --dump-file <filename> to save in a format suitable for bulk-upload
    bulk-upload      - Upload a YAML/JSON file containing multiple contacts
    validate-config  - Validate a configuration file against the schema
    add-contact      - Add a single contact with notification preferences

  Contact Types:
    email     - Email addresses for notifications
    slack     - Slack webhook URLs
    pagerduty - PagerDuty routing keys

  Severity Thresholds:
    5 - Lowest priority
    4 - Low priority
    3 - Medium priority (default)
    2 - High priority
    1 - Critical priority

  Alert Type:
    job     - Job-related alerts only
    cluster - Cluster-related alerts only (default)
    all     - All alert types

  Rule IDs:
    Comma-separated list of specific alert rule IDs that this contact should receive (e.g., cm-001,cm-002).
    If not specified, contact will receive all alerts matching their alert_type and severity_threshold.
EOM
)

manage_cluster_operator_contacts() {
  local MANAGE_CLUSTER_OPERATOR_CONTACTS_USAGE=$(cat <<EOM
Usage:
  $SCRIPT_NAME cluster-operator-contacts <action> [options]

$CLUSTER_OPERATOR_CONTACTS_HELP

  Examples:
    $SCRIPT_NAME cluster-operator-contacts create
    $SCRIPT_NAME cluster-operator-contacts list-contacts
    $SCRIPT_NAME cluster-operator-contacts list-contacts --dump-file contacts-backup.yaml
    $SCRIPT_NAME cluster-operator-contacts bulk-upload contacts.yaml
    $SCRIPT_NAME cluster-operator-contacts validate-config contacts.yaml
    $SCRIPT_NAME cluster-operator-contacts add-contact email "user@example.com" [alert-type] [severity-threshold]
    $SCRIPT_NAME cluster-operator-contacts add-contact slack "https://hooks.slack.com/xxx" cluster 4
    $SCRIPT_NAME cluster-operator-contacts add-contact pagerduty "routing-key-xxx" all 5
EOM
)

  local action="$1"
  shift

  case "$action" in
    "create")
      _create_secret
      ;;
    "list-contacts")
      _list_contacts "$@"
      ;;
    "bulk-upload")
      _bulk_upload "$1"
      ;;
    "validate-config")
      if [ -f "$1" ]; then
        local validation_output
        validation_output=$(_validate_config "$(cat "$1")")
        if [ $? -ne 0 ]; then
          echo "$validation_output" >&2
          return 1
        fi
        echo "Configuration is valid"
      else
        echo "Error: File $1 does not exist"
        return 1
      fi
      ;;
    "add-contact")
      if [ $# -lt 2 ]; then
        echo "Error: add-contact requires at least contact type and value"
        echo "$MANAGE_CLUSTER_OPERATOR_CONTACTS_USAGE"
        return 1
      fi
      _add_contact "$@"
      ;;
    "--help"|"-h")
      echo "$MANAGE_CLUSTER_OPERATOR_CONTACTS_USAGE"
      ;;
    *)
      echo "Invalid action. Usage:"
      echo "$MANAGE_CLUSTER_OPERATOR_CONTACTS_USAGE"
      exit 1
      ;;
  esac
}

### ALERTMANAGER ROUTING MANAGEMENT ###

# --- Configuration ---
ALERTMANAGER_NAMESPACE="prometheus"
ALERTMANAGER_SECRET_NAME="alertmanager-prometheus-alertmanager"
# This comment is added to custom routes/receivers in alertmanager.yaml, and
# used to identify them when deleting config added by this script:
CUSTOM_ALERT_ROUTING_COMMENT="Automatically added from pkg-properties by csadm on"
PROM_OPERATOR_SYNC_TIMEOUT_SECS=20

_confirm_or_abort() {
  while true; do
    >&2 read -p "$* [y/n]: " yn
    case $yn in
      [Yy]*) return 0 ;;
      [Nn]*) echo "Aborted"; return  1;;
    esac
  done
}


_alertmanager_show_routing() {
  $KUBECTL -n $ALERTMANAGER_NAMESPACE get secret $ALERTMANAGER_SECRET_NAME -ojson | jq -r ".data.\"alertmanager.yaml\"" | \
    base64 -d | yq '"Receivers:", .receivers, "\nRoutes:", .route.routes'
}

# Merge the routes and receivers from pkg-properties.yaml into alertmanager.yaml
_alertmanager_merge_custom_routing() {
  local interactive=$INTERACTIVE
  for arg in "$@"; do
    if [ "$arg" = "--yes" ]; then
      interactive=0
    else
      >&2 echo "Error: unknown argument: $arg"
      exit 1
    fi
  done

  if [ ! -f "${CLUSTER_PROPERTIES}" ]; then
    >&2 echo "Error: file does not exist: ${CLUSTER_PROPERTIES}"
    exit 1
  fi
  if [[ $($YQ '.properties.alertmanager.routing==null' "$CLUSTER_PROPERTIES") == "true" ]]; then
    >&2 echo ".properties.alertmanager.routing not configured in ${CLUSTER_PROPERTIES}. Exiting."
    exit 0
  fi

  local alertmanager_path=$(mktemp /tmp/alertmanager.XXXXXXXX.yaml)
  $KUBECTL -n $ALERTMANAGER_NAMESPACE get secret $ALERTMANAGER_SECRET_NAME -ojson | jq -r ".data.\"alertmanager.yaml\"" | base64 -d > "$alertmanager_path"
  cp "$alertmanager_path" "${alertmanager_path}.orig"

  local comment="${CUSTOM_ALERT_ROUTING_COMMENT} $(date +'%Y-%m-%d %H:%M:%S %Z')"

  # First, delete existing routes/receivers that were added by this script.
  $YQ "del(.route.routes[] | .. | select(lineComment==\"${CUSTOM_ALERT_ROUTING_COMMENT}*\") | parent)" -i "$alertmanager_path"
  $YQ "del(.receivers[] | .. | select(lineComment==\"${CUSTOM_ALERT_ROUTING_COMMENT}*\") | parent)" -i "$alertmanager_path"

  # Merge routes into alertmanager.yaml
  while IFS= read -r -d '' route_json ; do
    if [[ $($YQ ".route.routes | contains([${route_json}])" "$alertmanager_path") == "true" ]]; then
      >&2 echo Route $route_json already present in $ALERTMANAGER_SECRET_NAME alertmanager.yaml. Skipping.
    else
      # Insert this route at the top, right after the inhibit route.
      $YQ ".route.routes = .route.routes.[:1] + [${route_json}] + .route.routes.[1:]" -i "$alertmanager_path"
      # Force the route to continue.
      $YQ ".route.routes[1].continue = true" -i "$alertmanager_path"
      # Add a comment so we can identify this route in the future.
      v=$comment $YQ ".route.routes[1].continue line_comment = env(v)" -i "$alertmanager_path"
    fi
  done < <($YQ --nul-output e -o=j -I=0 '.properties.alertmanager.routing.routes[]' <"$CLUSTER_PROPERTIES")

  # Merge receivers into alertmanager.yaml
  while IFS= read -r -d '' receiver_json ; do
    if [[ $($YQ ".receivers | contains([${receiver_json}])" "$alertmanager_path") == "true" ]]; then
      >&2 echo Receiver $receiver_json already present in $ALERTMANAGER_SECRET_NAME alertmanager.yaml. Skipping.
    else
      $YQ ".receivers += [${receiver_json}]" -i "$alertmanager_path"
      v=$comment $YQ ".receivers[-1].name line_comment = env(v)" -i "$alertmanager_path"
    fi
  done < <($YQ --nul-output e -o=j -I=0 '.properties.alertmanager.routing.receivers[]' <"$CLUSTER_PROPERTIES")

  # Exit if nothing was merged into alertmanager.yaml
  if [[ $(diff -u "${alertmanager_path}.orig" "$alertmanager_path") == "" ]]; then
    >&2 echo "Nothing new to merged into alertmanager.yaml. Exiting."
    exit 0
  fi

  # Print diff and ask for confirmation.
  diff -u "${alertmanager_path}.orig" "$alertmanager_path"
  if [ "$interactive" == "1" ] || [ "$interactive" == "y" ]; then
    _confirm_or_abort "Confirm applying these changes?" || exit 1
  fi
  local apply_time=$(date --utc +"%Y-%m-%dT%H:%M:%SZ")
  $KUBECTL -n $ALERTMANAGER_NAMESPACE patch secret $ALERTMANAGER_SECRET_NAME -p '{"data": {"alertmanager.yaml": "'$(base64 -w0 -i "$alertmanager_path")'"}}' -v=1

  revert_merge_and_abort() {
    local message=$1
    >&2 echo "Error: ${message}"$'\n'"Aborting and reverting change to alertmanager.yaml."
    $KUBECTL -n $ALERTMANAGER_NAMESPACE patch secret $ALERTMANAGER_SECRET_NAME -p '{"data": {"alertmanager.yaml": "'$(base64 -w0 -i "${alertmanager_path}.orig")'"}}' -v=1
    >&2 echo "The new entry(ies) from this command may be invalid, so alertmanager.yaml was reverted to its original state. The new entries are still in ${CLUSTER_PROPERTIES}."
    >&2 echo "Please use the 'alertmanager-routing edit' command to fix or remove the bad entries in ${CLUSTER_PROPERTIES}."
    exit 1
  }

  # Find the prometheus-operator pod.
  local pod_name=$($KUBECTL get pods -nprometheus --field-selector='status.phase=Running' | grep prometheus-operator | awk '{print $1}')
  if [ "$pod_name" = "" ]; then
    revert_merge_and_abort "couldn't find a prometheus-operator pod after updating alertmanager.yaml. This could be because of invalid routes/receivers in ${CLUSTER_PROPERTIES}."
  fi
  # Wait for prometheus-operator to pick up the changes. First it will print the sync message, then it may print an error message.
  local sync_started=false
  local wait=$PROM_OPERATOR_SYNC_TIMEOUT_SECS
  while read -t $wait log_line ; do
    local about_to_sync=$(echo $log_line | grep 'level=info' | grep 'sync alertmanager')
    if [ ! "$about_to_sync" = "" ]; then
      sync_started=true
      wait=2 # Wait 2 more sec after sync for (maybe) error.
      continue
    fi
    local sync_failed=$(echo $log_line | grep 'level=error' | grep 'sync.*alertmanager')
    if [ ! "$sync_failed" = "" ]; then
      revert_merge_and_abort "failed to sync alertmanager. Error in pod ${pod_name} logs: ${sync_failed}"
    fi
  done < <(timeout $PROM_OPERATOR_SYNC_TIMEOUT_SECS $KUBECTL logs -nprometheus --since-time="$apply_time" -f $pod_name)

  if ! $sync_started; then
      revert_merge_and_abort "failed to sync alertmanager. Timed-out waiting for sync message in pod ${pod_name} logs."
  fi
}

_alertmanager_add_receiver() {
  name=$1
  address_type=$2
  address=$3
  if [ -z "$name" ] || [ -z "$address_type" ] || [ -z "$address" ]; then
    >&2 echo "Error: expected receiver name, address type and address."
    exit 1
  fi

  updated_props=$(mktemp /tmp/pkg-properties.XXXXXXXX.yaml)
  cp "$CLUSTER_PROPERTIES" $updated_props
  cp "$CLUSTER_PROPERTIES" "${updated_props}.orig"

  # Abort if the receiver already exists
  found_receiver=$(v=$name $YQ '.properties.alertmanager.routing.receivers[] | select(.name == env(v))' $updated_props)
  if ! [[ "$found_receiver" == "" ]]; then
    >&2 echo "Error: receiver already exists in $CLUSTER_PROPERTIES with same name (${name}):"
    echo -e "\n${found_receiver}\n" | >&2 sed 's/^/  /'
    >&2 echo "Delete this existing receiver (using alertmanager-routing edit) and try again."
    exit 1
  fi

  # Append the receiver (order doesn't matter)
  recv_id=$($YQ '.properties.alertmanager.routing.receivers | length' $updated_props)
  v=$name $YQ ".properties.alertmanager.routing.receivers[${recv_id}].name = env(v)" -i $updated_props
  local comment="Added by \`csadm alertmanager-routing add-receiver\` on $(date +'%Y-%m-%d %H:%M:%S %Z')"
  v=$comment $YQ ".properties.alertmanager.routing.receivers[${recv_id}].name line_comment = env(v)" -i $updated_props
  if [[ "$address_type" == "slack" ]]; then
    v=$address $YQ ".properties.alertmanager.routing.receivers[${recv_id}].slack_configs[0].api_url = env(v)" -i $updated_props
  elif [[ "$address_type" == "email" ]]; then
    v=$address $YQ ".properties.alertmanager.routing.receivers[${recv_id}].email_configs[0].to = env(v)" -i $updated_props
  elif [[ "$address_type" == "pagerduty" ]]; then
    v=$address $YQ ".properties.alertmanager.routing.receivers[${recv_id}].pagerduty_configs[0].routing_key = env(v)" -i $updated_props
  else
    >&2 echo "Error: unknown receiver address type '$address_type' for address '$address'"
    exit 1
  fi

  # Print diff and ask for confirmation.
  diff -u "$CLUSTER_PROPERTIES" $updated_props
  if [ "$INTERACTIVE" == "1" ] || [ "$INTERACTIVE" == "y" ]; then
    _confirm_or_abort "Confirm applying these changes?" || exit 1
  fi
  cp $updated_props "$CLUSTER_PROPERTIES"

  _alertmanager_merge_custom_routing --yes
}

_alertmanager_add_route() {
  receiver_name=$1
  if [[ -z "$receiver_name" ]]; then
    >&2 echo "Error: expected receiver name, but no argument provided"
    exit 1
  fi
  shift
  matchers=( "$@" )

  updated_props=$(mktemp /tmp/pkg-properties.XXXXXXXX.yaml)
  cp "$CLUSTER_PROPERTIES" $updated_props
  cp "$CLUSTER_PROPERTIES" "${updated_props}.orig"

  # Append this route to the custom routes
  route_id=$($YQ '.properties.alertmanager.routing.routes | length' $updated_props)
  v=$receiver_name $YQ ".properties.alertmanager.routing.routes[${route_id}].continue = true" -i $updated_props
  v=$receiver_name $YQ ".properties.alertmanager.routing.routes[${route_id}].receiver = env(v)" -i $updated_props
  local comment="Added by \`csadm alertmanager-routing add-receiver\` on $(date +'%Y-%m-%d %H:%M:%S %Z')"
  v=$comment $YQ ".properties.alertmanager.routing.routes[${route_id}].continue line_comment = env(v)" -i $updated_props
  for m in "${matchers[@]}"; do
    v=$m $YQ ".properties.alertmanager.routing.routes[${route_id}].matchers += [env(v)]" -i $updated_props
  done

  # Print diff and ask for confirmation.
  diff -u "$CLUSTER_PROPERTIES" $updated_props
  if [ "$INTERACTIVE" == "1" ] || [ "$INTERACTIVE" == "y" ]; then
    _confirm_or_abort "Confirm applying these changes?" || exit 1
  fi
  cp $updated_props "$CLUSTER_PROPERTIES"

  _alertmanager_merge_custom_routing --yes
}

_alertmanager_edit_custom_routing_yaml() {
  routing_yaml=$(mktemp /tmp/alertrouting.XXXXXXXX.yaml)
  $YQ '.properties.alertmanager.routing' "$CLUSTER_PROPERTIES" > $routing_yaml
  cp "$routing_yaml" "${routing_yaml}.orig"

  # Edit the custom routing YAML with the default text editor.
  $EDITOR "$routing_yaml"
  exit_code=$?
  if [[ $exit_code -ne 0 ]]; then
    >&2 echo "Editor $EDITOR returned a non-zero exit code. Aborting."
    exit $exit_code
  fi

  # Validate the YAML file after the edit.
  $YQ --exit-status 'tag == "!!map" or tag== "!!seq"' "$routing_yaml" > /dev/null
  exit_code=$?
  if [ $exit_code -ne 0 ]; then
    >&2 echo "Error: invalid YAML syntax in ${routing_yaml}. Please try again."
    exit $exit_code
  fi

  # Exit if the file wasn't changed.
  diff -u "${routing_yaml}.orig" "$routing_yaml"
  if [[ $(diff -u "${routing_yaml}.orig" "$routing_yaml") == "" ]]; then
    >&2 echo "No changes. Exiting."
    exit 0
  fi

  # Print diff and ask for confirmation.
  if [ "$INTERACTIVE" == "1" ] || [ "$INTERACTIVE" == "y" ]; then
    _confirm_or_abort "Confirm applying these changes?" || exit 1
  fi
  echo Applying...
  $YQ ".properties.alertmanager.routing=load(\"${routing_yaml}\")" -i "$CLUSTER_PROPERTIES"

  _alertmanager_merge_custom_routing --yes
}

# Define the shared help text once
ALERTMANAGER_ROUTING_HELP=$(cat <<EOM
  Manage the $ALERTMANAGER_SECRET_NAME Secret in the $ALERTMANAGER_NAMESPACE namespace.
  This Secret stores alertmanager.yaml, which contains alert routes and receivers.
  alertmanager.yaml is configured at deploy-time by helm-upgrade.sh using values
  from pkg-properties.yaml. The actions below merge new config values from
  pkg-properties.yaml into alertmanager.yaml, without needing to redeploy.
  Set the environment variable INTERACTIVE=0 to disable confirmation prompts.

  Actions:
    show          - Show the current alert routes and receivers in alertmanager.yaml
    add-receiver  - Add a new receiver with the specified name
    add-route     - Insert a route to a receiver, matching zero or more labels
    edit          - Open the custom routing YAML (from pkg-properties.yaml) in \$EDITOR=$EDITOR.
                    Use this to delete existing routes/receivers, or add new ones.
    reconcile     - Merge .alertmanager.routing from pkg-properties.yaml into alertmanager.yaml
                    Running add-receiver, add-route, or edit will automatically reconcile.

  Receiver types:
    email     - Email addresses for notifications
    slack     - Slack webhook URLs
    pagerduty - PagerDuty routing keys

  Matchers:
    Zero or more strings that match on alert labels. See:
    https://prometheus.io/docs/alerting/latest/configuration/#label-matchers
EOM
)
manage_alertmanager_routing() {
  local MANAGE_ALERTMANAGER_ROUTING_USAGE=$(cat <<EOM
Usage:
  $SCRIPT_NAME alertmanager-routing <action> [options]

$ALERTMANAGER_ROUTING_HELP

  Examples:
    $SCRIPT_NAME alertmanager-routing show
    $SCRIPT_NAME alertmanager-routing add-receiver <receiver-name> email "user@example.com"
    $SCRIPT_NAME alertmanager-routing add-receiver <receiver-name> slack "https://hooks.slack.com/xxx"
    $SCRIPT_NAME alertmanager-routing add-receiver <receiver-name> pagerduty "routing-key-xxx"
    $SCRIPT_NAME alertmanager-routing add-route <receiver-name> "alertname=PodError" "severity=sev1"
    $SCRIPT_NAME alertmanager-routing edit
    $SCRIPT_NAME alertmanager-routing reconcile
    $SCRIPT_NAME alertmanager-routing reconcile --yes # ignore confirmation prompts
EOM
)

  local action="$1"
  shift

  case "$action" in
    "show")
      _alertmanager_show_routing
      ;;
    "add-receiver")
      _alertmanager_add_receiver "$@"
      ;;
    "add-route")
      _alertmanager_add_route "$@"
      ;;
    "edit")
      _alertmanager_edit_custom_routing_yaml
      ;;
    "reconcile")
      _alertmanager_merge_custom_routing "$@"
      ;;
    "--help"|"-h")
      echo "$MANAGE_ALERTMANAGER_ROUTING_USAGE"
      ;;
    *)
      >&2 echo "Invalid action. Usage:"
      >&2 echo "$MANAGE_ALERTMANAGER_ROUTING_USAGE"
      exit 1
      ;;
  esac
}

### main ###

# Check dependencies unless help is being requested or no arguments provided
if _should_check_dependencies "$1" "$2"; then
  for program in 'jq' 'yq' 'pssh'; do
    _check_dependency "$program"
  done

  PY3=$(_resolve_py3)
fi

export SCRIPT_NAME="$0"
if [ "$1" = "install" ]; then
  shift
  install "$@"
elif [ "$1" = "update-cluster-config" ]; then
  shift
  update_cluster_config "$@"
elif [ "$1" = "get-cluster" ]; then
  shift
  get_cluster "$@"
elif [ "$1" = "batch-update-nodes" ]; then
  shift
  batch_update_nodes "$@"
elif [ "$1" = "update-nodegroup" ]; then
  shift
  update_nodegroup "$@"
elif [ "$1" = "update-node" ]; then
  shift
  update_node "$@"
elif [ "$1" = "update-system" ]; then
  shift
  update_system "$@"
elif [ "$1" = "update-namespace" ]; then
  shift
  update_namespace "$@"
elif [ "$1" = "get-namespace" ]; then
  shift
  get_namespace "$@"
elif [ "$1" = "create-namespace" ]; then
  shift
  create_namespace "$@"
elif [ "$1" = "delete-namespace" ]; then
  shift
  delete_namespace "$@"
elif [ "$1" = "test-system-fabric-diag" ]; then
  shift
  _test_system_fabric_diag "$@"
elif [ "$1" = "reset-storage" ]; then
  shift
  _reset_storage "$@"
elif [ "$1" = "rotate-user-auth-secret" ]; then
  shift
  rotate_user_auth_secret "$@"
elif [ "$1" = "validate-hardware-network" ]; then
  shift
  _validate_hardware_network "$@"
elif [ "$1" = "assert-idle-systems" ]; then
  shift
  _assert_system_idle_state "$@"
elif [ "$1" = "operate-systems" ]; then
  shift
  _operate_systems "$@"
elif [ "$1" = "select-upgrade-nodes" ]; then
  shift
  select_upgrade_nodes "$@"
elif [ "$1" = "smart-uscd-update" ]; then
  shift
  _smart_uscd_update "$@"
elif [ "$1" = "cluster-operator-contacts" ]; then
  shift
  manage_cluster_operator_contacts "$@"
elif [ "$1" = "alertmanager-routing" ]; then
  shift
  manage_alertmanager_routing "$@"
else
  cat <<EOM
Usage:
  $0 is the entry point script to CLI commands for cluster admins to manage a Cerebras cluster.
It supports many commands in this form:

  $0 <command> <command_options>

  '$0 <command> [--help|-h]' prints the usage of a command.

Existing commands:

  $0 install MANIFEST_FILE [--update-config[=CONFIG_FILE]] [--preflight] [--skip-k8s] [--validate] [--debug] [--yes]
    Install cluster components described in the given manifest json file.

  $0 update-cluster-config [NETWORK_CONFIG_FILE] [--create-depop-groups=N] [--dry-run] [--safe-system-update]
    Convert the given network config file to cluster.yaml format that can be
    used by cluster management.

  $0 get-cluster [--system-only|--node-only] [--error-only]
    Get cluster state information including both systems and nodes

  $0 update-node --name=<node_name> [--nic=<nic_name> ] --state=<ok/error> --note=\"<reasoning>\" --user=\"<username>\"
    Update node health status. This is mainly used to exclude an unhealthy node/NIC from being scheduled.
    Warning: after marked as error, the node/NIC will not be assigned for jobs until marked as ok.

  $0 batch-update-nodes <file_path> <ok/error> \"<note>\" \"<username>\"
    Batch update node health status with node line by line in the provided file.
    For updating to error, it will skip update if already in error to avoid overriding the note.
    For updating to ok, it will skip update if it has a different note than given one to avoid overriding.

  $0 update-nodegroup --name=<nodegroup_name> --state=<ok/error> --note=\"<reasoning>\"
    Update health status for all nodes in the specified nodegroup. This is mainly used to exclude an unhealthy nodegroup from being scheduled.
    Warning: after marked as error, all nodes in the nodegroup will not be assigned for jobs until marked as ok.

  $0 update-system --name=<system_name> [--port=<port_name>] --state=<ok/error> --note=\"<reasoning>\" --user=\"<username>\"
    Updates system health status. This is mainly used to exclude an error system/port from being scheduled/
    Warning: after marked as error, the system/port will not be assigned for jobs until marked as ok.
    Note: --user is required when setting state to \"error\", optional when setting to \"ok\".

  $0 create-namespace --name=<namespace> --systems=<system0,system1,...>
    Create a namespace with optional system assignment. This is used for multibox sharing.

  $0 delete-namespace --name=<namespace>
    Delete a namespace and release its assigned systems.

  $0 update-namespace --name=<namespace> --systems=<system0,system1,...> [--mode=<append|append-all|remove|remove-all>]
    Update a namespace's system assignments.

  $0 get-namespace [--name=<namespace>]
    Get namespaces and their system assignments.

  $0 test-system-fabric-diag [--dry-run] [ all | SYSTEM... ]
    Run fabric diag test against given systems after checking the systems are not in use and taken out of
    scheduling.

  $0 reset-storage [--help] [--job-logs] [--cached-compile] [--user-venvs]
        [--cluster-volumes] [--custom-worker-images] [--worker-cache] [--all]
        [--monitoring-data] [--job-history] [--dry-run] [--use-manifest FILE]
    Resets application storage, clearing any potentially sensitive data.

  $0 rotate-user-auth-secret
    Replace the existing user authentication secret with a new one. The cluster admin
    also needs to copy this new secret to all user nodes to replace the old one.

  $0 validate-hardware-network [--destination-type=<systems|servers>]
    Run a ping check from servers in the cluster to applicable destinations.

  $0 assert-idle-systems [ all | SYSTEM... ]
    Show the idle state of the systems in the cluster.

  $0 operate-systems [ all | SYSTEM... ] user:pwd [ args ]
    Operate on systems in the cluster by sending args to the system operation script.
    Must provide username and password information for the underlying systems.
    Note: requires asyncssh to be installed in the active python environment.

  $0 smart-uscd-update -d DIRECTORY --system-cred user:pwd [--image-upload-only | --precheck-only] [ all | SYSTEM... ]
    Update the USCD images on all systems in the cluster.

  $0 select-upgrade-nodes <target-version> [session-name] [skip-sx-pick] [ignore-running-jobs] [ignore-sx-pick-error]
    Choose nodes for upgrade, skip nodes already matching target version.
    If session name provided, will pick up nodes within session + SX nodes based on systems assigned.
    If skip-sx-pick set to non-empty, SX will not be picked as part of session upgrade, useful for cases SX upgraded separately.
    If ignore-running-jobs flag is provided, will ignore running jobs and continue (useful for batch creation workflows).
    If no session provided, will pick up nodes not in target version, useful for cases like unassigned/redundant nodes.
    If ignore-sx-pick-error flag is provided, will ignore sx-pick related errors and continue (warnings will be printed).

  $0 cluster-operator-contacts <action>
    Manage the $CLUSTER_OPERATOR_CONTACTS_SECRET_NAME Secret in the $CLUSTER_OPERATOR_CONTACTS_NAMESPACE namespace.
    Run '$0 cluster-operator-contacts --help' for more information.

  $0 alertmanager-routing <action>
    Manage the $ALERTMANAGER_SECRET_NAME Secret in the $ALERTMANAGER_NAMESPACE namespace.
    Run '$0 alertmanager-routing --help' for more information.
EOM
  exit 1
fi
