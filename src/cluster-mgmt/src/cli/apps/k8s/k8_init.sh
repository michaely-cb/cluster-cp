#!/bin/bash

# Initialization scripts for k8s clusters using kubeadm
set -e

THIS_FILE=$(readlink -f "${BASH_SOURCE[0]}")
THIS_DIR=$(dirname "$THIS_FILE")
HELPER_FILE=k8_init_helpers.sh
HELPER_PATH=${THIS_DIR}/${HELPER_FILE}
DEFAULT_LOG_PATH="${THIS_DIR}/.k8_init.$(date --utc +%s).log"
DEFAULT_RPM_PATH="$THIS_DIR/rpm"
RPM_PATH=${RPM_PATH-$DEFAULT_RPM_PATH}
source "${HELPER_PATH}"
SKIP_CHECK=true source "${THIS_DIR}/../pkg-common.sh"

DEFAULT_SERVICE_SUBNET="192.168.0.0/17"
# export all the params for yq env parsing in update_controlplane_all flow
export KUBE_APISERVER_MAX_REQ=$(get_apiserver_maxRequestsInflight)
export KUBE_APISERVER_MAX_MUTATING_REQ=$(get_apiserver_maxMutatingRequestsInflight)
# goawayChance should be disabled in a single controlplane case: there's nowhere to rebalance long-lived conns
export KUBE_APISERVER_GOAWAY_CHANCE=$(has_multiple_mgmt_nodes && _get_pkg_prop ".properties.kubeApiServer.goawayChance" "0.001" || echo "0.0")
export KUBE_APISERVER_AUDIT_POLICY_PATH="/etc/kubernetes/audit-policy.yaml"
export KUBE_APISERVER_AUDIT_LOG_DIR="/var/log/kubernetes/audit"
export KUBE_APISERVER_AUDIT_LOG_PATH="${KUBE_APISERVER_AUDIT_LOG_DIR}/audit.log"
export KUBE_APISERVER_AUDIT_MAX_BACKUP=$(_get_pkg_prop ".properties.kubeApiServer.auditLogMaxBackup" "5")
export KUBE_APISERVER_AUDIT_MAX_MB=$(_get_pkg_prop ".properties.kubeApiServer.auditLogMaxMb" "500")
export KUBE_CONTROLLER_MANAGER_KUBE_API_QPS=$(_get_pkg_prop ".properties.kubeControllerManager.kubeApiQps" "200")
export KUBE_CONTROLLER_MANAGER_KUBE_API_BURST=$(_get_pkg_prop ".properties.kubeControllerManager.kubeApiBurst" "300")
export ETCD_SNAPSHOT_COUNT=$(_get_pkg_prop ".properties.etcd.snapshotCount" "100000")
export ETCD_DB_QUOTA=$(_get_pkg_prop ".properties.etcd.quotaBackendBytes" "8589934592")
export KUBELET_API_QPS=$(_get_pkg_prop ".properties.kubelet.kubeApiQps" "50")
export KUBELET_API_BURST=$(_get_pkg_prop ".properties.kubelet.kubeApiBurst" "100")
export KUBELET_SHUTDOWN_GRACE_PERIOD=$(_get_pkg_prop ".properties.kubelet.shutdownGracePeriod" "30s")
export KUBELET_SHUTDOWN_GRACE_PERIOD_CRITICAL_PODS=$(_get_pkg_prop ".properties.kubelet.shutdownGracePeriodCriticalPods" "10s")
export KUBELET_EVICTION_MAX_GRACE_PERIOD=$(_get_pkg_prop ".properties.kubelet.evictionSoftMemoryAvailableGracePeriod" "60")
export KUBELET_EVICTION_SOFT_MEMORY=$(_get_pkg_prop ".properties.kubelet.evictionSoftMemoryAvailable" "500Mi")
export KUBELET_EVICTION_SOFT_MEMORY_GRACE_PERIOD=$(_get_pkg_prop ".properties.kubelet.evictionSoftMemoryAvailableGracePeriod" "30s")
export KUBELET_EVICTION_HARD_NODEFS=$(_get_pkg_prop ".properties.kubelet.evictionHardNodeFsAvailable" "10%")
export KUBELET_EVICTION_HARD_NODEFS_INODES=$(_get_pkg_prop ".properties.kubelet.evictionHardNodeFsInodesFree" "5%")
export KUBELET_EVICTION_HARD_IMAGEFS=$(_get_pkg_prop ".properties.kubelet.evictionHardImageFs" "15%")
export MAX_PODS_PER_NODE=110
# https://github.com/awslabs/amazon-eks-ami/blob/1edf383d6fa44d262524e896c5c66929502825d0/templates/al2/runtime/bootstrap.sh#L267C1-L278C2
# Calculates the amount of memory to reserve for kubeReserved in mebibytes. KubeReserved is a function of pod
# density so we are calculating the amount of memory to reserve for Kubernetes systems daemons by
# considering the maximum number of pods this instance type supports.
get_memory_mi_to_reserve() {
  echo $((11 * "${MAX_PODS_PER_NODE}" + 255))
}
export KUBE_RESERVED_MEM=$(get_memory_mi_to_reserve)Mi

# ALL_LIST - HOST of all nodes (including this one)
# ALL_VERSION_UPGRADE_LIST - HOST of all nodes (including this one) that need version upgrade
# CONTROLPLANE_ALL_LIST - HOST of all controlplane nodes (including this one)
# CONTROLPLANE_ADD_LIST - HOST of controlplane nodes (except this one) to add
# CONTROLPLANE_UPDATE_LIST - HOST of controlplane nodes (including this one) to update
# CONTROLPLANE_REMOVE_LIST - HOST of controlplane nodes (except this one) to remove
# CONTROLPLANE_VERSION_UPGRADE_LIST - HOST of controlplane nodes (including this one) that need version upgrade
# WORKER_ALL_LIST - HOST for all k8 worker nodes of cluster
# WORKER_ADD_LIST - HOST for all k8 worker nodes to add to cluster
# WORKER_UPDATE_LIST - HOST for k8 worker nodes to update from cluster
# WORKER_REMOVE_LIST - HOST for k8 worker nodes to remove from cluster
# WORKER_VERSION_UPGRADE_LIST - HOST for k8 worker nodes that need version upgrade
ALL_LIST=".all.list"
ALL_VERSION_UPGRADE_LIST=".all.version.list"
CONTROLPLANE_ALL_LIST=".controlplane.all.list"
CONTROLPLANE_ADD_LIST=".controlplane.add.list"
CONTROLPLANE_UPDATE_LIST=".controlplane.update.list"
CONTROLPLANE_VERSION_UPGRADE_LIST=".controlplane.version.list"
CONTROLPLANE_REMOVE_LIST=".controlplane.remove.list"
WORKER_ALL_LIST=".worker.all.list"
WORKER_ADD_LIST=".worker.add.list"
WORKER_UPDATE_LIST=".worker.update.list"
WORKER_REMOVE_LIST=".worker.remove.list"
WORKER_VERSION_UPGRADE_LIST=".worker.version.list"

# HELPER FUNCTIONS

function _finalize_lists() {
  # Remove any unreachable (by ssh) nodes from the lists, and then
  # delete empty lists (which disables their respective PSSH/PSCP function)
  # and prints a summary of the add/remove list contents

  for list_file in "${ALL_LIST}" "${CONTROLPLANE_ALL_LIST}" "${CONTROLPLANE_ADD_LIST}" "${CONTROLPLANE_UPDATE_LIST}" "${CONTROLPLANE_REMOVE_LIST}" \
    "${WORKER_ALL_LIST}" "${WORKER_ADD_LIST}" "${WORKER_UPDATE_LIST}" "${WORKER_REMOVE_LIST}"; do
    if [ ! -f ${list_file} ]; then continue; fi

    # Need to check that the variable is defined - will not be set for teardown
    if [ -n "${UNREACHABLE_LIST}" ] && [ -f ${UNREACHABLE_LIST} ]; then
      comm -23 ${list_file} ${UNREACHABLE_LIST} >.tmp.list
      mv .tmp.list ${list_file}
    fi

    if [ 0 -eq "$(wc -w <"${list_file}")" ]; then
      rm ${list_file}
    else
      echo "$(cut -d. -f3 <<<${list_file}) $(cut -d. -f2 <<<${list_file}) nodes: $(cat ${list_file} 2>/dev/null | wc -l)"
    fi
  done
  PSCP_HOSTS "${ALL_LIST}" "${HELPER_PATH}" "${TMP_PATH}/${HELPER_FILE}"
  PSSH_HOSTS "${ALL_LIST}" "mkdir -p ${TMP_PATH}/pb3"
  PSCP_HOSTS "${ALL_LIST}" "${THIS_DIR}/../pkg-functions/nodes.sh" "${TMP_PATH}/pb3/nodes.sh"
}

function lists_new_cluster() {
  echo "generating node lists for new cluster..."

  if ! [ -f "${CLUSTER_CONFIG}" ]; then
    echo "required configuration cluster.yaml not found, exiting"
    return 1
  fi
  rm -f ./.*.list

  yq -r '.nodes[] | select(.role == "management" and .properties.controlplane == "true") | .name' "${CLUSTER_CONFIG}" | sed "/^$(hostname)$/d" | sort >"${CONTROLPLANE_ADD_LIST}"
  yq -r '.nodes[] | select(.role != "management" or .properties.controlplane != "true") | .name' "${CLUSTER_CONFIG}" | sed "/^$(hostname)$/d" | sort >"${WORKER_ADD_LIST}"

  yq -r '.nodes[] | select(.role == "management" and .properties.controlplane == "true") | .name' "${CLUSTER_CONFIG}" | sort >"${CONTROLPLANE_ALL_LIST}"
  yq -r '.nodes[] | select(.role != "management" or .properties.controlplane != "true") | .name' "${CLUSTER_CONFIG}" | sort >"${WORKER_ALL_LIST}"

  yq -r '.nodes[] | .name' "${CLUSTER_CONFIG}" | sort >"${ALL_LIST}"
  _finalize_lists
}

function lists_existing_cluster() {
  echo "generating node lists for existing cluster..."

  if ! [ -f "${CLUSTER_CONFIG}" ]; then
    echo "required configuration cluster.yaml not found, exiting"
    return 1
  fi
  rm -f ./.*.list
  _clean() { rm -f ./.cluster*.list ./.k8*.list; }
  trap _clean RETURN

  yq -r '.nodes[] | .name' "${CLUSTER_CONFIG}" | sort >"${ALL_LIST}"
  yq -r '.nodes[] | select(.role == "management" and .properties.controlplane == "true") | .name' "${CLUSTER_CONFIG}" | sort >"${CONTROLPLANE_ALL_LIST}"
  yq -r '.nodes[] | select(.role != "management" or .properties.controlplane != "true") | .name' "${CLUSTER_CONFIG}" | sort >"${WORKER_ALL_LIST}"
  yq -r '.nodes[] | select(.role == "management" and .properties.controlplane == "true") | .name' "${CLUSTER_CONFIG}" | sed "/^$(hostname)$/d" | sort >".cluster_expect_cp.list"
  yq -r '.nodes[] | select(.role != "management" or .properties.controlplane != "true") | .name' "${CLUSTER_CONFIG}" | sed "/^$(hostname)$/d" | sort >".cluster_expect_worker.list"
  kubectl get node -oname -l'node-role.kubernetes.io/control-plane' | cut -d/ -f2 | sed "/^$(hostname)$/d" | sort >".k8_cp.list"
  kubectl get node -oname -l'!node-role.kubernetes.io/control-plane' | cut -d/ -f2 | sed "/^$(hostname)$/d" | sort >".k8_worker.list"

  # find add/removes
  comm -23 ".cluster_expect_cp.list" ".k8_cp.list" >${CONTROLPLANE_ADD_LIST}
  comm -23 ".k8_cp.list" ".cluster_expect_cp.list" >${CONTROLPLANE_REMOVE_LIST}
  comm -23 ".cluster_expect_worker.list" ".k8_worker.list" >${WORKER_ADD_LIST}
  comm -23 ".k8_worker.list" ".cluster_expect_worker.list" >${WORKER_REMOVE_LIST}

  # skip update for incremental build of adding/removal
  if [ ! -s "${CONTROLPLANE_ADD_LIST}" ] &&
    [ ! -s "${CONTROLPLANE_REMOVE_LIST}" ] &&
    [ ! -s "${WORKER_ADD_LIST}" ] &&
    [ ! -s "${WORKER_REMOVE_LIST}" ]; then
    comm -12 ".cluster_expect_cp.list" ".k8_cp.list" >${CONTROLPLANE_UPDATE_LIST}
    hostname >>${CONTROLPLANE_UPDATE_LIST}
    sort <${CONTROLPLANE_UPDATE_LIST} >.tmp.list
    mv .tmp.list ${CONTROLPLANE_UPDATE_LIST}
    comm -12 ".cluster_expect_worker.list" ".k8_worker.list" >${WORKER_UPDATE_LIST}
  fi

  # Find all the nodes unreachable by ssh
  get_unreachable_nodes $ALL_LIST

  _finalize_lists
}

function lists_teardown_targets() {
  echo "generating node lists for tearing down cluster..."

  rm -f ./.*.list
  _clean() { rm -f ./.cluster*.list .self.list; }
  trap _clean RETURN

  kubectl get node -oname -l'node-role.kubernetes.io/control-plane' | cut -d/ -f2 | sed "/^$(hostname)$/d" | sort >"${CONTROLPLANE_REMOVE_LIST}"
  kubectl get node -oname -l'!node-role.kubernetes.io/control-plane' | cut -d/ -f2 | sed "/^$(hostname)$/d" | sort >"${WORKER_REMOVE_LIST}"

  if ! [ -f "${CLUSTER_CONFIG}" ]; then
    echo "warning: ${CLUSTER_CONFIG} not found, only tearing down nodes identified from 'kubectl get nodes'"
    return
  fi

  # If cluster.yaml exists, then merge the nodes in cluster.yaml with the worker nodes, excluding the
  # nodes which are already in the controlplane list or are this node.
  yq -r '.nodes[] | .name' "${CLUSTER_CONFIG}" | sed "/^$(hostname)$/d" | sort >.cluster_nodes.list
  comm -23 .cluster_nodes.list "${CONTROLPLANE_REMOVE_LIST}" >>"${WORKER_REMOVE_LIST}"
  sort ${WORKER_REMOVE_LIST} | uniq >"${WORKER_REMOVE_LIST}".tmp
  mv "${WORKER_REMOVE_LIST}".tmp "${WORKER_REMOVE_LIST}"

  yq -r '.nodes[] | .name' "${CLUSTER_CONFIG}" | sort >"${ALL_LIST}"
  _finalize_lists
}

# filter cluster nodes with kubelet version below target version which require upgrade
function filter_upgrade_nodes() {
  # get min version across nodes as current version
  echo "generating node lists for nodes to be upgraded..."
  # cluster lowest node's version in case of nodes have different versions
  CLUSTER_K8S_VERSION="${LOCAL_K8S_VERSION}"
  echo "current version: ${CLUSTER_K8S_VERSION}"
  rm -f $ALL_VERSION_UPGRADE_LIST $CONTROLPLANE_VERSION_UPGRADE_LIST $WORKER_VERSION_UPGRADE_LIST
  touch $ALL_VERSION_UPGRADE_LIST
  touch $CONTROLPLANE_VERSION_UPGRADE_LIST
  touch $WORKER_VERSION_UPGRADE_LIST

  node_versions=$(kubectl get nodes -o=json | jq -r '.items[] | select(.metadata.labels["node-role.kubernetes.io/control-plane"] == "") | "\(.metadata.name) \(.status.nodeInfo.kubeletVersion | ltrimstr("v"))"')
  # add all cp nodes since kubelet version may not be equal to cp node server version
  # also there's idempotent check + cp nodes are limited
  while read -r name version; do
    if [ -z "$CLUSTER_K8S_VERSION" ] || is_semantic_version_higher "$CLUSTER_K8S_VERSION" "$version"; then
      CLUSTER_K8S_VERSION=$version
    fi
    echo "$name" >>"$CONTROLPLANE_VERSION_UPGRADE_LIST"
    echo "$name" >>"$ALL_VERSION_UPGRADE_LIST"
  done < <(echo "$node_versions")

  node_versions=$(kubectl get nodes -o=json | jq -r '.items[] | select(.metadata.labels["node-role.kubernetes.io/control-plane"] == null) | "\(.metadata.name) \(.status.nodeInfo.kubeletVersion | ltrimstr("v"))"')
  if [ -n "$node_versions" ]; then
    while read -r name version; do
      if [ -z "$CLUSTER_K8S_VERSION" ] || is_semantic_version_higher "$CLUSTER_K8S_VERSION" "$version"; then
        CLUSTER_K8S_VERSION=$version
      fi
      if [ "$version" != "$K8S_UPGRADE_VERSION" ]; then
        echo "$name" >>"$WORKER_VERSION_UPGRADE_LIST"
        echo "$name" >>"$ALL_VERSION_UPGRADE_LIST"
      fi
    done < <(echo "$node_versions")
  fi

  echo "current version after check: ${CLUSTER_K8S_VERSION}"
  sort "$ALL_VERSION_UPGRADE_LIST" >"${TMP_PATH}/all-version" && mv "${TMP_PATH}/all-version" "$ALL_VERSION_UPGRADE_LIST"
  sort "$CONTROLPLANE_VERSION_UPGRADE_LIST" >"${TMP_PATH}/cp-version" && mv "${TMP_PATH}/cp-version" "$CONTROLPLANE_VERSION_UPGRADE_LIST"
  sort "$WORKER_VERSION_UPGRADE_LIST" >"${TMP_PATH}/wrk-version" && mv "${TMP_PATH}/wrk-version" "$WORKER_VERSION_UPGRADE_LIST"
  export CLUSTER_K8S_VERSION
}

# Find all nodes in the specified node_list that need containerd upgrade based on the given version.
# The results are written to the specified nodes_to_upgrade_file.
_find_nodes_need_containerd_upgrade() {
  local ctr_version=$1
  local node_list=$2
  local nodes_to_upgrade_file=$3

  local staging_dir="${TMP_PATH}/k8s/staging"
  mkdir -p "$staging_dir/pssh_logs"
  rm -rf "$staging_dir/pssh_logs/"

  PSSH_HOSTS_WITH_OUTDIR "$node_list" "$staging_dir/pssh_logs" \
    "if ! containerd --version | awk '{print \$3}' | grep -q \"v$ctr_version\"; then source ${TMP_PATH}/pb3/nodes.sh; find_min_max_speed_nic max; fi"
  for node in $(ls "$staging_dir/pssh_logs"); do
    if [ -s "$staging_dir/pssh_logs/$node" ]; then
      cat "$staging_dir/pssh_logs/$node" >>"$nodes_to_upgrade_file"
    fi
  done
}

# MAIN FUNCTIONS

import_images() {
  for img in load-image*; do
    bash "${img}"
  done
}

import_rpms() {
  local hosts=${1-$ALL_VERSION_UPGRADE_LIST}
  # check for the existence of the rpm
  if [ ! -d "$RPM_PATH" ]; then
    echo "Directory $RPM_PATH for rpms does not exist."
    return 1
  fi

  if [ -z "$SKIP_RPM" ]; then
    # copy over rpms, try 100G network
    echo "start rpm copy"
    check_and_copy "${RPM_PATH}" "${TMP_PATH}/k8s/rpm" "${hosts}" "$THIS_DIR/../pkg-functions/nodes.sh"
    echo "rpm copy success"
  fi
}

teardown_cluster() {
  lists_teardown_targets
  local REBOOT_ON_CLEANUP=""
  local CONFIRM=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
    --reboot)
      REBOOT_ON_CLEANUP="--reboot"
      shift
      ;;
    -y)
      shift
      CONFIRM="yes"
      ;;
    *)
      echo "usage: $0 cleanup [--reboot] [-y]"
      exit 1
      ;;
    esac
  done

  rm -f .all.list
  test -f ${CONTROLPLANE_REMOVE_LIST} && { cat ${CONTROLPLANE_REMOVE_LIST} && echo ''; } >>.all.list
  test -f ${WORKER_REMOVE_LIST} && cat ${WORKER_REMOVE_LIST} >>.all.list
  if [ -z "${CONFIRM}" ]; then
    echo "WARNING: this will remove all k8s related files and directories from all nodes"
    echo "         and may reboot all nodes:"
    hostname
    if [ -f .all.list ]; then
      cat .all.list
    fi
    echo "         Are you sure you want to continue? (y/n)"
    read -r -n 1 -s
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      echo "aborting"
      exit 1
    fi
  fi

  if kubectl get ns rook-ceph &>/dev/null; then
    echo "ceph exists, start ceph cleanup first"
    CONFIRM=yes bash ceph-cleanup.sh
  fi

  # cleanup k8 resources which might otherwise prevent node cleanup (PVC for instance keep files
  # open which prevent umount) Could also try draining the nodes but this method appears to work well
  if kubectl get pods --all-namespaces &>/dev/null; then
    echo "Deleting all k8s resources for clean teardown (~3m)..."
    for ns in $(kubectl get namespace -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -vE '^(default|kube-system|kube-public|kube-node-lease)$'); do
      {
        echo "await delete namespace ${ns} for 2 minutes"
        if ! kubectl delete namespace "${ns}" --wait=true --timeout=120s && echo "namespace ${ns} deleted"; then
          echo "WARNING: failed to delete namespace ${ns} in time"
        fi
      } &
    done
    timeout 120 wait || true
    echo "all namespaces deleted"
  fi

  if [ ! -f .all.list ] || [ "$(wc -w <.all.list)" -eq 0 ]; then
    return
  fi
  PSSH_HOSTS ${WORKER_REMOVE_LIST} "${TMP_PATH}/${HELPER_FILE} cleanup ${REBOOT_ON_CLEANUP}"
  PSSH_HOSTS ${CONTROLPLANE_REMOVE_LIST} "${TMP_PATH}/${HELPER_FILE} cleanup ${REBOOT_ON_CLEANUP}"

  cleanup
  if [ -n "${REBOOT_ON_CLEANUP}" ]; then
    echo "await complete reboot (~15m)..."
    sleep 30
    local status
    local endtime=$(($(date +%s) + 900))
    local sshopts="-o ConnectTimeout=2 -oBatchMode=yes -oStrictHostKeyChecking=no"
    cp -f .all.list .await.list
    while [ $(date +%s) -lt $endtime ]; do
      rm -f .await.list.next
      for node in $(cat .await.list); do
        if ssh $sshopts "${node}" 'true' &>/dev/null; then
          status=$(ssh $sshopts "${node}" 'systemctl is-system-running' || true)
          if [[ "$status" =~ starting ]]; then
            echo "$node" >>.await.list.next
          else
            # note - sometimes nodes come up in degraded status, usually due to
            # /cb mount related issues that usually fix themselves in ~1 hour
            echo "$node booted, status: $status"
          fi
        else
          echo "$node" >>.await.list.next
        fi
      done

      if [ -f .await.list.next ]; then
        mv .await.list.next .await.list
      else
        rm -f .await.list
        break
      fi

      sleep 5
    done

    if [ -f .await.list ]; then
      echo "failed to reboot nodes in time"
      cat .await.list
      return 1
    fi

    echo "all nodes rebooted, you may also want to reboot this node..."
  fi
}

deps_setup() {
  bash "${THIS_DIR}/../binary-deps/apply-binary-deps.sh"
  bash "${THIS_DIR}/../registry/apply-registry.sh"
}

# run cleanup on nodes in case they are in a dirty state
prejoin_clean() {
  if [ -f ${CONTROLPLANE_ADD_LIST} ]; then
    PSSH_HOSTS ${CONTROLPLANE_ADD_LIST} "${TMP_PATH}/${HELPER_FILE} cleanup"
  fi

  if [ -f ${WORKER_ADD_LIST} ]; then
    PSSH_HOSTS ${WORKER_ADD_LIST} "${TMP_PATH}/${HELPER_FILE} cleanup"
  fi
}

prejoin_preflight() {
  local new_node_list=$1
  # First upgrade containerd if needed
  local current_containerd_version=$(containerd --version | awk '{print $3}')
  current_containerd_version="${current_containerd_version#v}"
  upgrade_containerd "${current_containerd_version}" "" "${new_node_list}"

  # Check for kubelet version mismatches
  local k8s_version_upgrade_needed=false
  local current_kubelet_version=$(kubelet --version | awk '{print $2}')
  local k8s_upgrade_nodes=$(mktemp)

  echo "Checking kubelet versions on new joining nodes..."

  while read -r node; do
    # Check kubelet version
    local node_kubelet_version
    if node_kubelet_version=$(ssh -n -o BatchMode=yes -o ConnectTimeout=5 "${node}" "kubelet --version 2>/dev/null | awk '{print \$2}'" 2>/dev/null); then
      if [[ "${node_kubelet_version}" != "${current_kubelet_version}" ]]; then
        k8s_version_upgrade_needed=true
        echo "${node}" >>"${k8s_upgrade_nodes}"
        echo "Node ${node} has kubelet version ${node_kubelet_version}, needs update to ${current_kubelet_version}"
      fi
    else
      # If unable to check version, assume RPM is needed
      k8s_version_upgrade_needed=true
      echo "${node}" >>"${k8s_upgrade_nodes}"
      echo "Unable to determine kubelet version on ${node}, marking for upgrade"
    fi
  done <"${new_node_list}"

  # Then upgrade kubelet if needed
  if [ "${k8s_version_upgrade_needed}" == true ]; then
    echo "Detected kubelet version mismatches in new nodes, upgrading before joining"
    import_rpms "${k8s_upgrade_nodes}"
    MAX_PSSH_THREADS=100 PSSH_HOSTS "${k8s_upgrade_nodes}" "K8S_INSTALL_VERSION=${K8S_INSTALL_VERSION} ${TMP_PATH}/${HELPER_FILE} upgrade_k8s_version $current_kubelet_version false false true"
  fi

  # Clean up temporary files
  rm -f "${k8s_upgrade_nodes}"
}

remove_workers() {
  # remove workers from k8s and run cleanup function on them if they are ssh'able. If they're
  # not ssh'able, it's likely they were cleaned up by an earlier cluster deployment flow.
  while read -r node; do
    kubectl delete node "${node}" --ignore-not-found
    if ssh -n -o BatchMode=yes -o ConnectTimeout=5 "${node}" exit &>/dev/null; then
      echo "${node}" >>".worker.remove.reachable.list"
    fi
  done <${WORKER_REMOVE_LIST}

  if [ -f ".worker.remove.reachable.list" ]; then
    PSSH_HOSTS ".worker.remove.reachable.list" "${TMP_PATH}/${HELPER_FILE} cleanup"
    rm ".worker.remove.reachable.list"
  fi
}

init_audit_policy() {
  mkdir -p "${KUBE_APISERVER_AUDIT_LOG_DIR}"
  mkdir -p /etc/kubernetes
  cat >/etc/kubernetes/audit-policy.yaml <<EOF
# Log all requests at the Metadata level.
apiVersion: audit.k8s.io/v1
kind: Policy
rules:
- level: Metadata
EOF
}

init_controlplane() {
  local my_ip=$(get_node_ip)
  local controlplane_addr=${my_ip}
  local controlplane_port=${APISERVER_PORT}
  local service_subnet=$(_get_cluster_prop "serviceSubnet" "$DEFAULT_SERVICE_SUBNET")

  if [ "${K8S_INSTALL_VERSION}" != "${LOCAL_K8S_VERSION}" ]; then
    echo "target new cluster version defined: ${K8S_INSTALL_VERSION}"
    upgrade_containerd "${CTR_VERSION}" "${CONTROLPLANE_ALL_LIST}" ""
    upgrade_k8s_version "${K8S_INSTALL_VERSION}" false false true "${RPM_PATH}"
  fi

  if [ "${IN_KIND}" == "true" ] || [ -f ${CONTROLPLANE_ADD_LIST} ]; then
    # HA setup
    controlplane_port=${HAPROXY_PORT}
    prepare_haproxy

    # skip CP kube-vip for kind
    if [ "${IN_KIND}" == "false" ]; then
      controlplane_addr=$(_must_get_pkg_prop ".properties.multiMgmtNodes.mgmtVip")
      cp -f kube-vip-template.yaml "${TMP_PATH}/"
      prepare_kube_vip "${controlplane_addr}" "${my_ip}"
    fi
  fi

  echo "controlplaneAddress: ${controlplane_addr}:${controlplane_port}"
  if [ -f ${CONTROLPLANE_ADD_LIST} ]; then
    echo "controlplaneMembers:"
    echo "$(hostname) (self)"
    cat ${CONTROLPLANE_ADD_LIST}
  fi
  echo "serviceNet: ${service_subnet}"
  echo "start initialization..."

  # starting 1.31, the existing string/string extra argument updated to structured extra arguments
  # https://kubernetes.io/docs/reference/config-api/kubeadm-config.v1beta4/
  # e.g.
  #  max-requests-inflight: "500"
  #  =>
  #  - name: max-requests-inflight
  #    value: "500"
  # this branch can be removed after all clusters go to 1.31+
  if is_semantic_version_higher "1.31.0" "${LOCAL_K8S_VERSION}"; then
    # controlPlaneEndpoint is the global endpoint for the cluster
    # which then load-balances the requests to each individual API server
    cat >"${TMP_PATH}/".k8s_config.yaml <<EOF
apiVersion: kubeadm.k8s.io/v1beta3
kind: ClusterConfiguration
kubernetesVersion: "v${LOCAL_K8S_VERSION}"
controlPlaneEndpoint: ${controlplane_addr}:${controlplane_port}
imageRepository: "registry.local"
networking:
  podSubnet: ""
  serviceSubnet: ${service_subnet}
apiServer:
  extraArgs:
    max-requests-inflight: "${KUBE_APISERVER_MAX_REQ}"
    max-mutating-requests-inflight: "${KUBE_APISERVER_MAX_MUTATING_REQ}"
    goaway-chance: "${KUBE_APISERVER_GOAWAY_CHANCE}"
    audit-policy-file: "${KUBE_APISERVER_AUDIT_POLICY_PATH}"
    audit-log-path: "${KUBE_APISERVER_AUDIT_LOG_PATH}"
    audit-log-maxbackup: "${KUBE_APISERVER_AUDIT_MAX_BACKUP}"
    audit-log-maxsize: "${KUBE_APISERVER_AUDIT_MAX_MB}"
  extraVolumes:
    - name: "audit-policy"
      hostPath: "${KUBE_APISERVER_AUDIT_POLICY_PATH}"
      mountPath: "${KUBE_APISERVER_AUDIT_POLICY_PATH}"
      readOnly: true
      pathType: File
    - name: "audit-log"
      hostPath: "${KUBE_APISERVER_AUDIT_LOG_DIR}"
      mountPath: "${KUBE_APISERVER_AUDIT_LOG_DIR}"
      pathType: DirectoryOrCreate
controllerManager:
  extraArgs:
    bind-address: 0.0.0.0
    kube-api-qps: "${KUBE_CONTROLLER_MANAGER_KUBE_API_QPS}"
    kube-api-burst: "${KUBE_CONTROLLER_MANAGER_KUBE_API_BURST}"
scheduler:
  extraArgs:
    bind-address: 0.0.0.0
dns:
  imageRepository: "registry.local/coredns"
  imageTag: "${COREDNS_TAG}"
etcd:
  local:
    imageTag: "${ETCD_TAG}"
    extraArgs:
      snapshot-count: "${ETCD_SNAPSHOT_COUNT}"
      quota-backend-bytes: "${ETCD_DB_QUOTA}"
    serverCertSANs:
    - 127.0.0.1
    peerCertSANs:
    - 127.0.0.1
---
apiVersion: kubeadm.k8s.io/v1beta3
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: ${my_ip}
  bindPort: ${APISERVER_PORT}
nodeRegistration:
  kubeletExtraArgs:
    node-ip: "${my_ip}"
    container-runtime-endpoint: "unix:///var/run/containerd/containerd.sock"
    pod-infra-container-image: "registry.local/pause:${PAUSE_TAG}"
    hostname-override: $(hostname)
    ${runtime_arg}
---
apiVersion: kubeproxy.config.k8s.io/v1alpha1
kind: KubeProxyConfiguration
metricsBindAddress: 0.0.0.0
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
failSwapOn: true
memorySwap:
  swapBehavior: ""
shutdownGracePeriod: ${KUBELET_SHUTDOWN_GRACE_PERIOD}
shutdownGracePeriodCriticalPods: ${KUBELET_SHUTDOWN_GRACE_PERIOD_CRITICAL_PODS}
kubeAPIQPS: ${KUBELET_API_QPS}
kubeAPIBurst: ${KUBELET_API_BURST}
serializeImagePulls: true
maxPods: ${MAX_PODS_PER_NODE}
kubeReserved:
  memory: ${KUBE_RESERVED_MEM}
evictionMaxPodGracePeriod: ${KUBELET_EVICTION_MAX_GRACE_PERIOD}
evictionSoft:
  memory.available: ${KUBELET_EVICTION_SOFT_MEMORY}
evictionSoftGracePeriod:
  memory.available: ${KUBELET_EVICTION_SOFT_MEMORY_GRACE_PERIOD}
evictionHard:
  nodefs.available: ${KUBELET_EVICTION_HARD_NODEFS}
  nodefs.inodesFree: ${KUBELET_EVICTION_HARD_NODEFS_INODES}
  imagefs.available: ${KUBELET_EVICTION_HARD_IMAGEFS}
EOF
  else
    cat >"${TMP_PATH}/".k8s_config.yaml <<EOF
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: "v${LOCAL_K8S_VERSION}"
controlPlaneEndpoint: ${controlplane_addr}:${controlplane_port}
imageRepository: "registry.local"
networking:
  podSubnet: ""
  serviceSubnet: ${service_subnet}
apiServer:
  extraArgs:
    - name: max-requests-inflight
      value: "${KUBE_APISERVER_MAX_REQ}"
    - name: max-mutating-requests-inflight
      value: "${KUBE_APISERVER_MAX_MUTATING_REQ}"
    - name: goaway-chance
      value: "${KUBE_APISERVER_GOAWAY_CHANCE}"
    - name: audit-policy-file
      value: "${KUBE_APISERVER_AUDIT_POLICY_PATH}"
    - name: audit-log-path
      value: "${KUBE_APISERVER_AUDIT_LOG_PATH}"
    - name: audit-log-maxbackup
      value: "${KUBE_APISERVER_AUDIT_MAX_BACKUP}"
    - name: audit-log-maxsize
      value: "${KUBE_APISERVER_AUDIT_MAX_MB}"
  extraVolumes:
    - name: "audit-policy"
      hostPath: "${KUBE_APISERVER_AUDIT_POLICY_PATH}"
      mountPath: "${KUBE_APISERVER_AUDIT_POLICY_PATH}"
      readOnly: true
      pathType: File
    - name: "audit-log"
      hostPath: "${KUBE_APISERVER_AUDIT_LOG_DIR}"
      mountPath: "${KUBE_APISERVER_AUDIT_LOG_DIR}"
      pathType: DirectoryOrCreate
controllerManager:
  extraArgs:
    - name: bind-address
      value: "0.0.0.0"
    - name: kube-api-qps
      value: "${KUBE_CONTROLLER_MANAGER_KUBE_API_QPS}"
    - name: kube-api-burst
      value: "${KUBE_CONTROLLER_MANAGER_KUBE_API_BURST}"
scheduler:
  extraArgs:
    - name: bind-address
      value: "0.0.0.0"
dns:
  imageRepository: "registry.local/coredns"
  imageTag: "${COREDNS_TAG}"
etcd:
  local:
    imageTag: "${ETCD_TAG}"
    extraArgs:
      - name: snapshot-count
        value: "${ETCD_SNAPSHOT_COUNT}"
      - name: quota-backend-bytes
        value: "${ETCD_DB_QUOTA}"
    serverCertSANs:
      - 127.0.0.1
    peerCertSANs:
      - 127.0.0.1
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: ${my_ip}
  bindPort: ${APISERVER_PORT}
nodeRegistration:
  kubeletExtraArgs:
    - name: node-ip
      value: "${my_ip}"
    - name: container-runtime-endpoint
      value: "unix:///var/run/containerd/containerd.sock"
    - name: pod-infra-container-image
      value: "registry.local/pause:${PAUSE_TAG}"
    - name: hostname-override
      value: "$(hostname)"
    ${runtime_arg}
---
apiVersion: kubeproxy.config.k8s.io/v1alpha1
kind: KubeProxyConfiguration
metricsBindAddress: 0.0.0.0
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
failSwapOn: true
memorySwap:
  swapBehavior: ""
shutdownGracePeriod: ${KUBELET_SHUTDOWN_GRACE_PERIOD}
shutdownGracePeriodCriticalPods: ${KUBELET_SHUTDOWN_GRACE_PERIOD_CRITICAL_PODS}
kubeAPIQPS: ${KUBELET_API_QPS}
kubeAPIBurst: ${KUBELET_API_BURST}
serializeImagePulls: true
maxPods: ${MAX_PODS_PER_NODE}
kubeReserved:
  memory: ${KUBE_RESERVED_MEM}
evictionMaxPodGracePeriod: ${KUBELET_EVICTION_MAX_GRACE_PERIOD}
evictionSoft:
  memory.available: ${KUBELET_EVICTION_SOFT_MEMORY}
evictionSoftGracePeriod:
  memory.available: ${KUBELET_EVICTION_SOFT_MEMORY_GRACE_PERIOD}
evictionHard:
  nodefs.available: ${KUBELET_EVICTION_HARD_NODEFS}
  nodefs.inodesFree: ${KUBELET_EVICTION_HARD_NODEFS_INODES}
  imagefs.available: ${KUBELET_EVICTION_HARD_IMAGEFS}
EOF
  fi
  if [ -f "${CONTROLPLANE_ADD_LIST}" ]; then
    yq e -i '.apiServer.certSANs += ["'"${controlplane_addr}"'"]' "${TMP_PATH}/.k8s_config.yaml"
  fi

  init_audit_policy

  kubeadm init --skip-token-print --config "${TMP_PATH}/".k8s_config.yaml ${EXTRA_ARGS}
  mkdir -p ~/.kube
  cp /etc/kubernetes/admin.conf ~/.kube/config

  if [ -f "${CONTROLPLANE_ADD_LIST}" ]; then
    if [ "${IN_KIND}" == "false" ]; then
      await_containers_ready "kube-vip-$(hostname)"
    fi
    await_check_success "curl -k https://${controlplane_addr}:${controlplane_port}/readyz"
  fi

  cat <<EOF
done, now run commands

join_controlplane_all
join_worker_all
EOF
}

join_controlplane_all() {
  if [ ! -f "${CONTROLPLANE_ADD_LIST}" ]; then
    echo "${CONTROLPLANE_ADD_LIST} not found, skipping join controlplane nodes"
    return
  fi

  prejoin_preflight "${CONTROLPLANE_ADD_LIST}"

  # refresh the cert key for controlplane since it expires after 2 hours
  local cert_key=$(kubeadm certs certificate-key | tee "${TMP_PATH}/".k8s_cert_key)
  kubeadm init phase upload-certs --upload-certs --certificate-key "${cert_key}"

  PSSH_HOSTS "${CONTROLPLANE_ADD_LIST}" "mkdir -p $(dirname $CLUSTER_CONFIG); mkdir -p ${KUBE_APISERVER_AUDIT_LOG_DIR}; mkdir -p /etc/kubernetes"
  PSCP_HOSTS "${CONTROLPLANE_ADD_LIST}" /etc/kubernetes/audit-policy.yaml /etc/kubernetes/audit-policy.yaml
  PSCP_HOSTS "${CONTROLPLANE_ADD_LIST}" $CLUSTER_CONFIG $CLUSTER_CONFIG
  PSCP_HOSTS "${CONTROLPLANE_ADD_LIST}" $CLUSTER_PKG_PROPS $CLUSTER_PKG_PROPS
  PSCP_HOSTS "${CONTROLPLANE_ADD_LIST}" kube-vip-template.yaml "${TMP_PATH}/"
  PSCP_HOSTS "${CONTROLPLANE_ADD_LIST}" haproxy.yaml "${TMP_PATH}/"
  PSCP_HOSTS "${CONTROLPLANE_ADD_LIST}" /etc/haproxy/haproxy.cfg "${TMP_PATH}/"
  PSCP_HOSTS "${CONTROLPLANE_ADD_LIST}" "${TMP_PATH}/.k8s_cert_key" "${TMP_PATH}/"

  # run this serially as it prevents issues when joining etcd in parallel
  local JOIN_CMD=$(kubeadm token create --print-join-command)
  while IFS="" read -r host; do
    echo "join controlplane: ${host}"
    ssh -n -o ConnectTimeout=2 -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "${TMP_PATH}/${HELPER_FILE} join_controlplane $JOIN_CMD"
    echo "done ${host}"
  done <"${CONTROLPLANE_ADD_LIST}"

  echo "done, now run join_worker_all"
}

update_controlplane_all() {
  init_audit_policy
  PSSH_HOSTS "${CONTROLPLANE_ALL_LIST}" "mkdir -p ${KUBE_APISERVER_AUDIT_LOG_DIR}; mkdir -p /etc/kubernetes"
  PSCP_HOSTS "${CONTROLPLANE_ALL_LIST}" "${KUBE_APISERVER_AUDIT_POLICY_PATH}" "${KUBE_APISERVER_AUDIT_POLICY_PATH}"

  local my_ip=$(get_node_ip)
  local controlplane_addr=${my_ip}
  local controlplane_port=${APISERVER_PORT}
  if has_multiple_mgmt_nodes; then
    if [ "${IN_KIND}" == "false" ]; then
      controlplane_addr=$(_must_get_pkg_prop ".properties.multiMgmtNodes.mgmtVip")
    fi
    controlplane_port=${HAPROXY_PORT}
  fi
  # export for yq update purpose
  export CP_ENDPOINT=${controlplane_addr}:${controlplane_port}
  export ETCD_TAG=${ETCD_TAG}
  export COREDNS_TAG=${COREDNS_TAG}

  # Update controlplane configuration
  # 1/ update the k8s config file which is refered to during kubeadm upgrades
  # 2/ update each controlplane's static pods using kubeadm
  PSSH_HOSTS "${CONTROLPLANE_ALL_LIST}" "mkdir -p $(dirname $CLUSTER_CONFIG)"
  PSCP_HOSTS "${CONTROLPLANE_ALL_LIST}" $CLUSTER_CONFIG $CLUSTER_CONFIG
  PSCP_HOSTS "${CONTROLPLANE_ALL_LIST}" $CLUSTER_PKG_PROPS $CLUSTER_PKG_PROPS
  local tmp_dir
  tmp_dir=$(mktemp -d)
  echo "tmp kubeadm config: $tmp_dir"
  kubectl get cm -n kube-system kubeadm-config -ojsonpath='{.data.ClusterConfiguration}' | yq . >"${tmp_dir}/orig.yaml"
  if is_semantic_version_higher "1.31.0" "${CLUSTER_K8S_VERSION}"; then
    yq '
    .controlPlaneEndpoint = strenv(CP_ENDPOINT) |
    .imageRepository = "registry.local" |
    .dns.imageRepository = "registry.local/coredns" |
    .dns.imageTag = strenv(COREDNS_TAG) |
    .etcd.local.imageTag = strenv(ETCD_TAG) |
    .etcd.local.extraArgs["snapshot-count"] = strenv(ETCD_SNAPSHOT_COUNT) |
    .etcd.local.extraArgs["quota-backend-bytes"] = strenv(ETCD_DB_QUOTA) |
    .apiServer.extraArgs["max-requests-inflight"] = strenv(KUBE_APISERVER_MAX_REQ) |
    .apiServer.extraArgs["max-mutating-requests-inflight"] = strenv(KUBE_APISERVER_MAX_MUTATING_REQ) |
    .apiServer.extraArgs["goaway-chance"] = strenv(KUBE_APISERVER_GOAWAY_CHANCE) |
    .apiServer.extraArgs["audit-policy-file"] = strenv(KUBE_APISERVER_AUDIT_POLICY_PATH) |
    .apiServer.extraArgs["audit-log-path"] = strenv(KUBE_APISERVER_AUDIT_LOG_PATH) |
    .apiServer.extraArgs["audit-log-maxbackup"] = strenv(KUBE_APISERVER_AUDIT_MAX_BACKUP) |
    .apiServer.extraArgs["audit-log-maxsize"] = strenv(KUBE_APISERVER_AUDIT_MAX_MB) |
    .apiServer.extraVolumes[0]["name"] = "audit-policy" |
    .apiServer.extraVolumes[0]["hostPath"] = strenv(KUBE_APISERVER_AUDIT_POLICY_PATH) |
    .apiServer.extraVolumes[0]["mountPath"] = strenv(KUBE_APISERVER_AUDIT_POLICY_PATH) |
    .apiServer.extraVolumes[0]["readOnly"] = true |
    .apiServer.extraVolumes[0]["pathType"] = "File" |
    .apiServer.extraVolumes[1]["name"] = "audit-log" |
    .apiServer.extraVolumes[1]["hostPath"] = strenv(KUBE_APISERVER_AUDIT_LOG_DIR) |
    .apiServer.extraVolumes[1]["mountPath"] = strenv(KUBE_APISERVER_AUDIT_LOG_DIR) |
    .apiServer.extraVolumes[1]["pathType"] = "DirectoryOrCreate" |
    .controllerManager.extraArgs["kube-api-burst"] = strenv(KUBE_CONTROLLER_MANAGER_KUBE_API_BURST) |
    .controllerManager.extraArgs["kube-api-qps"] = strenv(KUBE_CONTROLLER_MANAGER_KUBE_API_QPS)
  ' "${tmp_dir}/orig.yaml" >"${tmp_dir}/modified.yaml"
  else
    # note!!! below arg names should be in alphabetic order to avoid unnecessary diff which will trigger unnecessary update
    yq '
    .controlPlaneEndpoint = strenv(CP_ENDPOINT) |
    .imageRepository = "registry.local" |
    .dns.imageRepository = "registry.local/coredns" |
    .dns.imageTag = strenv(COREDNS_TAG) |
    .etcd.local.imageTag = strenv(ETCD_TAG) |
    .etcd.local.extraArgs |= (
      map(select(.name != "quota-backend-bytes")) +
      [{ "name": "quota-backend-bytes", "value": strenv(ETCD_DB_QUOTA) }]
    ) |
    .etcd.local.extraArgs |= (
      map(select(.name != "snapshot-count")) +
      [{ "name": "snapshot-count", "value": strenv(ETCD_SNAPSHOT_COUNT) }]
    ) |
    .apiServer.extraArgs |= (
      map(select(.name != "audit-log-maxbackup")) +
      [{ "name": "audit-log-maxbackup", "value": strenv(KUBE_APISERVER_AUDIT_MAX_BACKUP) }]
    ) |
    .apiServer.extraArgs |= (
      map(select(.name != "audit-log-maxsize")) +
      [{ "name": "audit-log-maxsize", "value": strenv(KUBE_APISERVER_AUDIT_MAX_MB) }]
    ) |
    .apiServer.extraArgs |= (
      map(select(.name != "audit-log-path")) +
      [{ "name": "audit-log-path", "value": strenv(KUBE_APISERVER_AUDIT_LOG_PATH) }]
    ) |
    .apiServer.extraArgs |= (
      map(select(.name != "audit-policy-file")) +
      [{ "name": "audit-policy-file", "value": strenv(KUBE_APISERVER_AUDIT_POLICY_PATH) }]
    ) |
    .apiServer.extraArgs |= (
      map(select(.name != "goaway-chance")) +
      [{ "name": "goaway-chance", "value": strenv(KUBE_APISERVER_GOAWAY_CHANCE) }]
    ) |
    .apiServer.extraArgs |= (
      map(select(.name != "max-mutating-requests-inflight")) +
      [{ "name": "max-mutating-requests-inflight", "value": strenv(KUBE_APISERVER_MAX_MUTATING_REQ) }]
    ) |
    .apiServer.extraArgs |= (
      map(select(.name != "max-requests-inflight")) +
      [{ "name": "max-requests-inflight", "value": strenv(KUBE_APISERVER_MAX_REQ) }]
    ) |
    .apiServer.extraVolumes[0].name = "audit-policy" |
    .apiServer.extraVolumes[0].hostPath = strenv(KUBE_APISERVER_AUDIT_POLICY_PATH) |
    .apiServer.extraVolumes[0].mountPath = strenv(KUBE_APISERVER_AUDIT_POLICY_PATH) |
    .apiServer.extraVolumes[0].readOnly = true |
    .apiServer.extraVolumes[0].pathType = "File" |
    .apiServer.extraVolumes[1].name = "audit-log" |
    .apiServer.extraVolumes[1].hostPath = strenv(KUBE_APISERVER_AUDIT_LOG_DIR) |
    .apiServer.extraVolumes[1].mountPath = strenv(KUBE_APISERVER_AUDIT_LOG_DIR) |
    .apiServer.extraVolumes[1].pathType = "DirectoryOrCreate" |
    .controllerManager.extraArgs |= (
      map(select(.name != "kube-api-burst")) +
      [{ "name": "kube-api-burst", "value": strenv(KUBE_CONTROLLER_MANAGER_KUBE_API_BURST) }]
    ) |
    .controllerManager.extraArgs |= (
      map(select(.name != "kube-api-qps")) +
      [{ "name": "kube-api-qps", "value": strenv(KUBE_CONTROLLER_MANAGER_KUBE_API_QPS) }]
    )
  ' "${tmp_dir}/orig.yaml" >"${tmp_dir}/modified.yaml"
  fi
  if ! diff "${tmp_dir}/orig.yaml" "${tmp_dir}/modified.yaml"; then
    echo "kubeadm config updated, updating node manifests"
    kubectl create cm -n kube-system kubeadm-config -oyaml --dry-run=client --from-file=ClusterConfiguration="${tmp_dir}/modified.yaml" | kubectl apply -f-
    for node in $(cat ${CONTROLPLANE_ALL_LIST}); do
      echo "updating controlplane config on $node"
      if ! ssh -n -o BatchMode=yes -o ConnectTimeout=5 "${node}" "${TMP_PATH}/${HELPER_FILE} update_controlplane_manifests"; then
        echo "update controlplane config failed, reverting kubeadm-config"
        kubectl create cm -n kube-system kubeadm-config -oyaml --dry-run=client --from-file=ClusterConfiguration="${tmp_dir}/orig.yaml" | kubectl apply -f-
        return 1
      fi
    done
  else
    echo "kubeadm config unchanged, not updating node manifests"
  fi

  echo "updating haproxy/vip config if needed"
  if [ "${IN_KIND}" == "true" ] || has_multiple_mgmt_nodes; then
    # update haproxy static pod on controlplane nodes if multi-mgmt node
    prepare_haproxy_cfg
    PSCP_HOSTS "${CONTROLPLANE_ALL_LIST}" haproxy.cfg.updated "${TMP_PATH}/haproxy.cfg"
    PSCP_HOSTS "${CONTROLPLANE_ALL_LIST}" haproxy.yaml "${TMP_PATH}/"
    PSSH_HOSTS "${CONTROLPLANE_ALL_LIST}" "${TMP_PATH}/${HELPER_FILE} prepare_haproxy ${controlplane_addr} check"
    # populate to all workers if necessary since kubeadm upgrade doesn't take care of kubelet restart
    # use a small batch size to avoid traffic spikes
    MAX_PSSH_THREADS=100 PSSH_HOSTS "${WORKER_ALL_LIST}" "${TMP_PATH}/${HELPER_FILE} populate_endpoint ${controlplane_addr} ${controlplane_port}"

    # skip CP kube-vip for kind
    if [ "${IN_KIND}" == "true" ]; then
      return 0
    fi

    # update kube-vip static pod on controlplane nodes if multi-mgmt node
    PSCP_HOSTS "${CONTROLPLANE_ALL_LIST}" kube-vip-template.yaml "${TMP_PATH}/"
    PSSH_HOSTS "${CONTROLPLANE_ALL_LIST}" "${TMP_PATH}/${HELPER_FILE} prepare_kube_vip ${controlplane_addr}"
  fi
}

join_worker_all() {
  if [ ! -f "${WORKER_ADD_LIST}" ]; then
    echo "${WORKER_ADD_LIST} not found, skipping join worker nodes"
    return
  fi

  prejoin_preflight "${WORKER_ADD_LIST}"

  local JOIN_CMD=$(kubeadm token create --print-join-command)
  PSSH_HOSTS ${WORKER_ADD_LIST} "${TMP_PATH}/${HELPER_FILE}" join_worker "$JOIN_CMD ${EXTRA_ARGS}"
}

update_worker_all() {
  local tmp_dir
  tmp_dir=$(mktemp -d)
  echo "tmp kubelet config: $tmp_dir"
  kubectl get cm -n kube-system kubelet-config -ojsonpath='{.data.kubelet}' | yq . >"${tmp_dir}/orig.yaml"
  yq '
    .kubeAPIQPS = env(KUBELET_API_QPS) |
    .kubeAPIBurst = env(KUBELET_API_BURST) |
    .shutdownGracePeriod = strenv(KUBELET_SHUTDOWN_GRACE_PERIOD) |
    .shutdownGracePeriodCriticalPods = strenv(KUBELET_SHUTDOWN_GRACE_PERIOD_CRITICAL_PODS) |
    .maxPods = env(MAX_PODS_PER_NODE) |
    .memorySwap["swapBehavior"] = "" |
    .failSwapOn = true |
    .kubeReserved["memory"] = strenv(KUBE_RESERVED_MEM) |
    .evictionMaxPodGracePeriod = env(KUBELET_EVICTION_MAX_GRACE_PERIOD) |
    .evictionSoft["memory.available"] = strenv(KUBELET_EVICTION_SOFT_MEMORY) |
    .evictionSoftGracePeriod["memory.available"] = strenv(KUBELET_EVICTION_SOFT_MEMORY_GRACE_PERIOD) |
    .evictionHard["nodefs.available"] = strenv(KUBELET_EVICTION_HARD_NODEFS) |
    .evictionHard["nodefs.inodesFree"] = strenv(KUBELET_EVICTION_HARD_NODEFS_INODES) |
    .evictionHard["imagefs.available"] = strenv(KUBELET_EVICTION_HARD_IMAGEFS)
  ' "${tmp_dir}/orig.yaml" >"${tmp_dir}/modified.yaml"
  if ! diff "${tmp_dir}/orig.yaml" "${tmp_dir}/modified.yaml"; then
    echo "kubelet config updated, updating node manifests"
    kubectl create cm -n kube-system kubelet-config -oyaml --dry-run=client --from-file=kubelet="${tmp_dir}/modified.yaml" | kubectl apply -f-
    # populate to all nodes if necessary since kubeadm upgrade doesn't take care of kubelet restart
    if [ -f "${CONTROLPLANE_ALL_LIST}" ]; then
      if ! PSSH_HOSTS "${CONTROLPLANE_ALL_LIST}" "${TMP_PATH}/${HELPER_FILE} update_worker"; then
        echo "update worker failed, reverting kubelet-config"
        kubectl create cm -n kube-system kubelet-config -oyaml --dry-run=client --from-file=kubelet="${tmp_dir}/orig.yaml" | kubectl apply -f-
        return 1
      fi
    fi
    # use a small batch size to avoid traffic spikes
    if [ -f "${WORKER_ALL_LIST}" ]; then
      if ! MAX_PSSH_THREADS=100 PSSH_HOSTS "${WORKER_ALL_LIST}" "${TMP_PATH}/${HELPER_FILE} update_worker"; then
        echo "update worker failed, reverting kubelet-config"
        kubectl create cm -n kube-system kubelet-config -oyaml --dry-run=client --from-file=kubelet="${tmp_dir}/orig.yaml" | kubectl apply -f-
        return 1
      fi
    fi
  else
    echo "kubelet config unchanged, not updating workers"
  fi
}

# upgrade k8s version from current version to target version
# in case of fatal failure, refer to
# https://v1-30.docs.kubernetes.io/docs/tasks/administer-cluster/kubeadm/kubeadm-upgrade/#recovering-from-a-failure-state
function version_upgrade() {
  echo "Start version upgrade"

  # upgrade with one version at a time
  local last_ver="$CLUSTER_K8S_VERSION"
  for ver in 1.25.16 1.26.15 1.27.16 1.28.13 1.29.9 1.30.4 1.31.10 1.32.6; do
    # skip versions <= current, skip unnecessary patch version check
    if ! is_semantic_version_higher "$ver" "$CLUSTER_K8S_VERSION" "skip_patch_version"; then
      continue
    fi
    # stop after target version reached
    if is_semantic_version_higher "$ver" "$K8S_UPGRADE_VERSION"; then
      break
    fi

    echo "start upgrading to $ver"
    # note: may need to add drain node + uncordon
    # upgrade first cp node
    # upgrade other cp one by one
    echo "upgrading controlplane local node $(hostname) to $ver"
    upgrade_k8s_version "$ver" "is_cp" "first_cp"
    for node in $(cat $CONTROLPLANE_VERSION_UPGRADE_LIST); do
      echo "upgrading controlplane node $node to $ver"
      if [ "$node" == "$(hostname)" ]; then
        continue
      fi
      if ! ssh -n -o BatchMode=yes -o ConnectTimeout=5 "${node}" "${TMP_PATH}/${HELPER_FILE} upgrade_k8s_version $ver is_cp"; then
        echo "upgrade controlplane k8s version on $node failed"
        return 1
      fi
      echo "complete upgrade controlplane k8s version on $node"
    done
    # upgrade workers in batch
    echo "upgrading all worker nodes to $ver"
    MAX_PSSH_THREADS=100 PSSH_HOSTS "${WORKER_VERSION_UPGRADE_LIST}" "${TMP_PATH}/${HELPER_FILE} upgrade_k8s_version $ver"
    # wait till versions converge before next upgrade
    await_check_success "if kubectl get nodes | grep $last_ver; then false; fi"
    echo "successfully upgraded all nodes to $ver"
    last_ver=$ver
  done
}

function upgrade_containerd() {
  local ctr_version=${1}
  local cp_nodes_file=${2-$CONTROLPLANE_ALL_LIST}
  local worker_nodes_file=${3-$WORKER_ALL_LIST}
  local upgrade_file="${RPM_PATH}/containerd-$ctr_version-linux-amd64.tar.gz"

  local tmp_path="$THIS_DIR/tmp"
  rm -rf "$tmp_path"
  mkdir -p "$tmp_path"

  # find control plane nodes that don't have the right containerd version
  _find_nodes_need_containerd_upgrade "$ctr_version" "${cp_nodes_file}" "$tmp_path/control_plane_nodes_to_upgrade.list"

  # find worker nodes that don't have the right containerd version
  _find_nodes_need_containerd_upgrade "$ctr_version" "${worker_nodes_file}" "$tmp_path/worker_nodes_to_upgrade.list"

  if [ ! -s "$tmp_path/control_plane_nodes_to_upgrade.list" ] && [ ! -s "$tmp_path/worker_nodes_to_upgrade.list" ]; then
    echo "No nodes need containerd upgrade, exiting."
    return 0
  fi

  if [ ! -f "$upgrade_file" ]; then
    echo "Error: containerd binary file $upgrade_file does not exist"
    return 1
  fi

  cat >$tmp_path/containerd_upgrade_node.sh <<EOF
set -e
containerd_version=\$(containerd --version | awk '{print \$3}')
if [ "\$containerd_version" != "v$ctr_version" ]; then
    tmp_path_node="${TMP_PATH}/containerd-$ctr_version"
    mkdir -p "\$tmp_path_node"
    tar xfz "${TMP_PATH}/containerd-$ctr_version-linux-amd64.tar.gz" -C "\$tmp_path_node"

    /bin/mv -f \$tmp_path_node/bin/* /usr/local/bin/.
    systemctl daemon-reload
    systemctl restart containerd
    timeout=\$(date -d "30 seconds" +%s)
    while ! systemctl status containerd | grep -q 'Active: active (running)'; do
      remaining_time=\$((timeout - \$(date +%s)))
      if [ \$remaining_time -le 0 ]; then
        echo "Containerd is not running after 30 seconds, exiting."
        exit 1
      fi
      sleep 2
    done
    rm -rf "\$tmp_path_node"
    rm -rf "${TMP_PATH}/containerd-$ctr_version-linux-amd64.tar.gz"
else
    echo "containerd $ctr_version is already installed"
fi
EOF

  PSCP_HOSTS "$tmp_path/control_plane_nodes_to_upgrade.list" "$tmp_path/containerd_upgrade_node.sh" "${TMP_PATH}/containerd_upgrade_node.sh"
  PSCP_HOSTS "$tmp_path/worker_nodes_to_upgrade.list" "$tmp_path/containerd_upgrade_node.sh" "${TMP_PATH}/containerd_upgrade_node.sh"

  check_and_copy "$upgrade_file" "${TMP_PATH}/containerd-$ctr_version-linux-amd64.tar.gz" "$tmp_path/control_plane_nodes_to_upgrade.list" "$THIS_DIR/../pkg-functions/nodes.sh"
  check_and_copy "$upgrade_file" "${TMP_PATH}/containerd-$ctr_version-linux-amd64.tar.gz" "$tmp_path/worker_nodes_to_upgrade.list" "$THIS_DIR/../pkg-functions/nodes.sh"

  # On the safe side, we upgrade containerd on control plane nodes one at a time.
  for node in $(cat "$tmp_path/control_plane_nodes_to_upgrade.list"); do
    echo "Upgrading ctr on control plane node $node"
    ssh -n -o ConnectTimeout=2 -o BatchMode=yes -o StrictHostKeyChecking=no "$node" "bash ${TMP_PATH}/containerd_upgrade_node.sh"
  done

  echo "Upgrading ctr on all worker nodes"
  PSSH_HOSTS "$tmp_path/worker_nodes_to_upgrade.list" "bash ${TMP_PATH}/containerd_upgrade_node.sh"
}

upload_config() {
  # Upload config if not already uploaded
  kubectl create ns ${SYSTEM_NAMESPACE} &>/dev/null || true
  if ! kubectl get cm -n ${SYSTEM_NAMESPACE} cluster &>/dev/null; then
    kubectl create cm cluster -n ${SYSTEM_NAMESPACE} --from-file=clusterConfiguration.yaml=${CLUSTER_CONFIG}
  fi
}

preflight() {
  if ! [ -f "${CLUSTER_CONFIG}" ]; then
    echo "error: required file ${CLUSTER_CONFIG} is not present"
    return 1
  fi

  local tmpd=$(mktemp -d)
  local myname=$(hostname)
  yq '.nodes[] | select(.role == "management") | .name' "${CLUSTER_CONFIG}" >"$tmpd/nodes-mgmt.list"

  # ensure that there's either: 1 mgmt node and we're running on it (must be controlplane then) OR
  # there's multiple mgmt nodes and we're executing on one of the mgmt nodes
  if [ $(wc -w <"$tmpd/nodes-mgmt.list") -eq 1 ]; then
    if ! grep -Fxq "$myname" "$tmpd/nodes-mgmt.list"; then
      echo "error: this script is being executed on host $myname but must be executed on the management host:"
      cat $tmpd/nodes-mgmt.list
      return 1
    fi
  else
    yq '.nodes[] | select(.role == "management" and .properties.controlplane == "true") | .name' "${CLUSTER_CONFIG}" >"$tmpd/nodes-cp.list"
    if ! grep -Fxq "$myname" "$tmpd/nodes-cp.list"; then
      echo "error: this script is being executed on host $myname but must be executed on a management node which has property 'controlplane: true':"
      cat $tmpd/nodes-cp.list
      return 1
    fi
  fi
}

__is_new_cluster() {
  local manifest_dir="/etc/kubernetes/manifests"
  # Return 0 (true for new cluster) if manifest dir or no files exist
  if [ ! -d "$manifest_dir" ] || [ "$(ls -A "$manifest_dir" 2>/dev/null | wc -l)" -eq 0 ]; then
    return 0
  fi
  # Check API server healthz endpoint up to 5 times
  for _ in {1..5}; do
    if curl -ks https://localhost:6443/healthz &>/dev/null; then
      return 1  # Not a new cluster — API server is healthy
    fi
    sleep 2
  done
  return 0
}

# Calculate CoreDNS replicas based on cluster size
# Minimum 2 replicas for HA, then scale based on node count
calculate_coredns_replicas() {
  local system_count=$(get_system_count)
  local min_replicas=2
  if has_multiple_mgmt_nodes; then
    min_replicas=3
  fi
  # Keep the default of 3 replicas up to 64 systems, then add 1 replica per 64 systems [rounding up]
  if [ "$system_count" -gt 64 ]; then
    replicas=$((min_replicas + (system_count + 63) / 64))
  else
    replicas=$min_replicas
  fi
  echo "$replicas"
}

reconcile() {
  local yes_flag=${1:-""}
  local remove_only_flag=${2:-""}
  local CONFIRM=""
  local remove_only=false
  if [ "$yes_flag" == "--yes" ] || [ "$yes_flag" == "-y" ]; then
    CONFIRM="yes"
  elif [ "$yes_flag" != "" ]; then
    echo "error: invalid argument $yes_flag, expecting --yes or -y"
    exit 1
  fi
  if [ "$remove_only_flag" == "--remove-only" ]; then
    remove_only=true
  elif [ "$remove_only_flag" != "" ]; then
    echo "error: invalid argument $remove_only_flag, expecting --remove-only"
    exit 1
  fi

  preflight

  local new_cluster="false"
  if __is_new_cluster; then
    new_cluster="true"
  fi

  if $new_cluster; then
    lists_new_cluster
    if [ -z "$SKIP_CLEAN" ]; then
      cleanup
      prejoin_clean
    fi
    deps_setup
    import_images
    init_controlplane
    join_controlplane_all
    join_worker_all
    upload_config
  else
    # reconciling an existing cluster
    if [ -z "$INCREMENTAL_DIR" ]; then
      # update/upgrade flow
      lists_existing_cluster
    else
      # incremental flow
      echo "incremental install flow starts"
      # If we have an arg, then we're invoked by the incremental deploy process.
      # In this case, csadm_list_helper creates our lists. Use those and don't create our own.
      CONTROLPLANE_ADD_LIST="$INCREMENTAL_DIR/k8_controlplane_new"
      CONTROLPLANE_UPDATE_LIST="$INCREMENTAL_DIR/k8_controlplane_update"
      CONTROLPLANE_REMOVE_LIST="$INCREMENTAL_DIR/k8_controlplane_remove"
      WORKER_ADD_LIST="$INCREMENTAL_DIR/k8_worker_new"
      WORKER_REMOVE_LIST="$INCREMENTAL_DIR/k8_worker_remove"
      ALL_LIST="$INCREMENTAL_DIR/k8_all"
      for lf in $CONTROLPLANE_ADD_LIST $CONTROLPLANE_UPDATE_LIST $CONTROLPLANE_REMOVE_LIST $WORKER_ADD_LIST $WORKER_REMOVE_LIST; do
        if [ -f "$lf" ]; then
          cat "$lf" >>"${ALL_LIST}"
        fi
      done
      sort -u "${ALL_LIST}" -o "${ALL_LIST}"
      _finalize_lists
    fi

    if [ -f ${CONTROLPLANE_REMOVE_LIST} ]; then
      echo "warning: controlplane removal requested but not supported, skipping remove controlplane:"
      cat ${CONTROLPLANE_REMOVE_LIST}
    fi
    if [ -f ${WORKER_REMOVE_LIST} ]; then
      remove_workers
      # return early for remove only case
      if [ ! -f "$CONTROLPLANE_ADD_LIST" ] && [ ! -f "$WORKER_ADD_LIST" ]; then
        exit 0
      fi
    fi

    if $remove_only; then
      # return early for remove only case
      echo "remove only mode, early exit reconcile"
      exit 0
    fi

    # incremental add flow, skip update/upgrade for incremental add case
    if [ -f ${CONTROLPLANE_ADD_LIST} ] || [ -f ${WORKER_ADD_LIST} ]; then
      prejoin_clean
      deps_setup
      import_images
      if [ -f ${CONTROLPLANE_ADD_LIST} ]; then
        join_controlplane_all
      fi
      if [ -f ${WORKER_ADD_LIST} ]; then
        join_worker_all
      fi
    else
      # update/upgrade flow
      deps_setup
      filter_upgrade_nodes
      import_images
      update_controlplane_all
      update_worker_all
      if [ "$K8S_UPGRADE_VERSION" != "$CLUSTER_K8S_VERSION" ]; then
        if [ -z "${CONFIRM}" ]; then
          echo "Note: this will upgrade k8s version from $CLUSTER_K8S_VERSION to $K8S_UPGRADE_VERSION."
          echo "         Are you sure you want to continue? (y/n)"
          read -r -n 1 -s
          if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "aborting"
            exit 1
          fi
        fi
        upgrade_containerd "${CTR_VERSION}"
        import_rpms
        version_upgrade
      else
        echo "all nodes have consistent version:$K8S_UPGRADE_VERSION, no upgrade needed"
      fi
    fi
    upload_config
  fi

  # pin coredns to cp nodes and scale replicas
  coredns_replicas=$(calculate_coredns_replicas)
  kubectl patch deployment -nkube-system coredns -p '{
    "spec": {
      "replicas": '"$coredns_replicas"',
      "template": {
        "spec": {
          "nodeSelector": {
            "node-role.kubernetes.io/control-plane": ""
          },
          "containers": [{
            "name": "coredns",
            "resources": {
              "requests": {
                "cpu": "100m",
                "memory": "100Mi"
              },
              "limits": {
                "cpu": "500m",
                "memory": "500Mi"
              }
            }
          }]
        }
      }
    }
  }'

  echo "k8 reconcile complete"
  if [ -s ${UNREACHABLE_LIST} ]; then
    echo "Some unreachable nodes were skipped:"
    cat ${UNREACHABLE_LIST}
  fi
}

# MAIN

__main() {
  helptext=$(
    cat >&1 <<EOF
Usage: $0 COMMAND

Main commands:
  reconcile
    If Kubernetes is not installed, attempt to install it based on the configuration specified in
    ${CLUSTER_CONFIG}. If Kubernetes is already running, add any nodes specified in the configuration
    that haven't yet joined the cluster or remove worker nodes which are not specified.
    If --remove-only flag is specified, the reconcile command will only remove worker nodes.

Debug commands:
  lists_new_cluster
  lists_existing_cluster
  lists_teardown_targets
    Generate ${CONTROLPLANE_ADD_LIST}, ${CONTROLPLANE_REMOVE_LIST}, ${WORKER_ADD_LIST}, ${WORKER_REMOVE_LIST} files according to
    the information in ${CLUSTER_CONFIG} and the k8 cluster. The *.list files are used by other
    helper commands. Run this command prior to running other helper commands.
      * new_cluster: adds nodes according to contents of ${CLUSTER_CONFIG}
      * existing_cluster: adds/removes nodes according to contents of ${CLUSTER_CONFIG}. Does not remove existing controlplane nodes.
      * teardown_cluster: removes all nodes according to contents of ${CLUSTER_CONFIG} and the existing k8 cluster.
    Manually edit *.list files if there are special considerations for your use-case.

  teardown_cluster [--reboot] [-y]
    Perform cleanup on all nodes. Run 'lists_teardown_targets' prior to this to generate teardown candidates.
    Options:
      --reboot                Optional. Reboots nodes and awaits peer restart before restarting this node
      -y                      Optional. Forces the teardown without prompting first

  init_controlplane
    Create a control plane on this node.

    Cluster configurations (${CLUSTER_CONFIG}::properties):
      * serviceSubnet       Defaults to ${DEFAULT_SERVICE_SUBNET}. Can be overridden with 'serviceSubnet: CIDR'.
      * controlplaneAddress Defaults to $(get_node_ip). Required for a high-availability setup: 'managementVIP: IP'.
      * managementRouterIP  Required for a BGP-based high-availability configuration: 'managementRouterIP: IP'.
      * managementASN       Required for a BGP-based high-availability configuration: 'managementASN: ASN'.

  join_controlplane_all
    Join all control plane nodes (only necessary for a high-availability cluster).

  update_controlplane_all
    Update all control plane nodes.

  join_worker_all
    Join all worker nodes.

  update_worker_all
    Update all worker nodes.

  cleanup [--reboot]
    On current node only, remove Kubernetes and previous configurations . Node will reboot if '--reboot' option is used.
    Rebooting is advisable when tearing down a high-availability cluster as it resets all network changes.

Global Vars
   K8S_UPGRADE_VERSION   overrides target upgrade k8s version for k8s upgrade of existing cluster
   K8S_INSTALL_VERSION   overrides target install k8s version for k8s install of new cluster
   RPM_PATH              overrides default path for k8 RPMs for k8 upgrade ($DEFAULT_RPM_PATH)
   SKIP_CLEAN            set to 1 to disable kubeadm reset during cluster init
   SKIP_IMPORT           set to 1 to disable image import steps
   SKIP_RPM              set to 1 to disable copying RPMs to hosts for k8 upgrade

EOF
  )
  CMD="$1"
  shift

  if [[ ! "$CMD" =~ ^(reconcile|teardown_cluster|lists_new_cluster|lists_existing_cluster|lists_teardown_targets|init_controlplane|join_controlplane_all|join_worker_all|update_controlplane_all|cleanup)$ ]]; then
    echo "$helptext"
    exit 1
  fi

  cd "$(dirname ${THIS_FILE})"
  $CMD $@
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then

  # log all commands and stdout/err to a log file for debug purposes
  echo "[$(date "+%Y-%m-%d %H:%M:%S")] user issued command: $0 $(printf ' %q' "$@")" >>"${DEFAULT_LOG_PATH}"
  export PS4='[$(date "+%Y-%m-%d %H:%M:%S")] '
  exec 5>>"${DEFAULT_LOG_PATH}"
  BASH_XTRACEFD=5
  set -x

  __main "$@" > >(tee -a "${DEFAULT_LOG_PATH}") 2>&1

  set +x
fi
