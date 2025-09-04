#!/bin/bash
set -e

# Standalone helpers for k8s cluster initialization
IN_KIND="false"
TMP_PATH="/tmp"
# For 1.32, kernel requires 4.19+, skip check for now
EXTRA_ARGS="--ignore-preflight-errors=SystemVerification -v=5"
# can't run function within tmp in kind, e.g. "bash: /tmp/k8_init.sh: Permission denied"
# also kind will delete tmp after ssh exit
if [ -d /kind ]; then
  IN_KIND="true"
  TMP_PATH="/home"
  EXTRA_ARGS="--ignore-preflight-errors=all -v=5"
fi
SKIP_RENEW_CERT="--certificate-renewal=false"
SYSTEM_NAMESPACE="job-operator"
HELPER_FILE=k8_init_helpers.sh
KV_TEMPLATE="${TMP_PATH}/kube-vip-template.yaml"
HAPROXY_YAML="${TMP_PATH}/haproxy.yaml"
UPDATED_HAPROXY_CFG="${TMP_PATH}/haproxy.cfg"
CLUSTER_CONFIG=/opt/cerebras/cluster/cluster.yaml
CLUSTER_PKG_PROPS=/opt/cerebras/cluster/pkg-properties.yaml
APISERVER_PORT=${APISERVER_PORT-"6443"}
HAPROXY_PORT=${HAPROXY_PORT-"8443"}
CP_VIP_PORT=30901

# ctr expected version
CTR_VERSION="1.7.20"
declare -A SUPPORTED_K8S_VERSIONS
LATEST_VERSION="1.32.6"
SUPPORTED_K8S_VERSIONS["1.24"]="1.24.4"
SUPPORTED_K8S_VERSIONS["1.25"]="1.25.16"
SUPPORTED_K8S_VERSIONS["1.26"]="1.26.15"
SUPPORTED_K8S_VERSIONS["1.27"]="1.27.16"
SUPPORTED_K8S_VERSIONS["1.28"]="1.28.13"
SUPPORTED_K8S_VERSIONS["1.29"]="1.29.9"
SUPPORTED_K8S_VERSIONS["1.30"]="1.30.4"
SUPPORTED_K8S_VERSIONS["1.31"]="1.31.10"
SUPPORTED_K8S_VERSIONS["1.32"]="1.32.6"
# etcd/coredns/pause images have to be explicit declared for init install
# can not use '--config' with '--kubernetes-version' together
declare -A ETCD_VERSIONS
ETCD_VERSIONS["1.24"]="3.5.3-0"
ETCD_VERSIONS["1.25"]="3.5.9-0"
ETCD_VERSIONS["1.26"]="3.5.10-0"
ETCD_VERSIONS["1.27"]="3.5.12-0"
ETCD_VERSIONS["1.28"]="3.5.12-0"
ETCD_VERSIONS["1.29"]="3.5.12-0"
ETCD_VERSIONS["1.30"]="3.5.12-0"
ETCD_VERSIONS["1.31"]="3.5.15-0"
ETCD_VERSIONS["1.32"]="3.5.16-0"
declare -A PAUSE_VERSIONS
PAUSE_VERSIONS["1.24"]="3.7"
PAUSE_VERSIONS["1.25"]="3.8"
PAUSE_VERSIONS["1.26"]="3.9"
PAUSE_VERSIONS["1.27"]="3.9"
PAUSE_VERSIONS["1.28"]="3.9"
PAUSE_VERSIONS["1.29"]="3.9"
PAUSE_VERSIONS["1.30"]="3.9"
PAUSE_VERSIONS["1.31"]="3.10"
PAUSE_VERSIONS["1.32"]="3.10"
declare -A COREDNS_VERSIONS
COREDNS_VERSIONS["1.24"]="v1.8.6"
COREDNS_VERSIONS["1.25"]="v1.9.3"
COREDNS_VERSIONS["1.26"]="v1.9.3"
COREDNS_VERSIONS["1.27"]="v1.10.1"
COREDNS_VERSIONS["1.28"]="v1.10.1"
COREDNS_VERSIONS["1.29"]="v1.11.1"
COREDNS_VERSIONS["1.30"]="v1.11.1"
COREDNS_VERSIONS["1.31"]="v1.11.3"
COREDNS_VERSIONS["1.32"]="v1.11.3"
# current node's k8s major version
K8S_VERSION_MAJOR_MINOR=$(kubelet --version | awk '{print $2}' | sed 's/^v//' | awk -F. '{print $1"."$2}')
# current node's expected version
LOCAL_K8S_VERSION=${SUPPORTED_K8S_VERSIONS[$K8S_VERSION_MAJOR_MINOR]}
# for upgrade case target version
K8S_UPGRADE_VERSION=${K8S_UPGRADE_VERSION-"${LATEST_VERSION}"}
# for new install case target version
K8S_INSTALL_VERSION=${K8S_INSTALL_VERSION-"${LOCAL_K8S_VERSION}"}
# export for yq update purpose
export COREDNS_TAG=${COREDNS_VERSIONS[$K8S_VERSION_MAJOR_MINOR]}
export ETCD_TAG=${ETCD_VERSIONS[$K8S_VERSION_MAJOR_MINOR]}
export PAUSE_TAG=${PAUSE_VERSIONS[$K8S_VERSION_MAJOR_MINOR]}
# 1.24 requires this flag but >=1.27 doesn't allow it
# this is the target k8s version if specified
if [ "$K8S_INSTALL_VERSION" == "1.24.4" ]; then
  runtime_arg="container-runtime: remote"
else
  runtime_arg=""
fi

function get_1g_device_name() {
  # Heuristic to find highest speed interface with an IP that's UP. In CG's, there may be a 1G and 25G interface
  # in which case we'd use the 25G interface. Ignore any interface 100G or higher.

  __find_1g_device() {
    for dev in /sys/class/net/*; do
      devname=$(basename "$dev")
      # Only allow physical or bond interfaces
      [[ ! -d "$dev/device" && ! -d "$dev/bonding" ]] && continue
      # Skip down interfaces
      [[ "$(cat "$dev/operstate")" != "up" ]] && continue
      # Ensure the device has an IP associated with it
      ip -4 -o addr show dev "$devname" | grep -q 'inet ' || continue
      # Choose devices 0 < speed < 100G
      speed=$(cat "$dev/speed" 2>/dev/null)
      [[ "$speed" =~ ^[0-9]+$ && "$speed" -ge 0 && "$speed" -lt 100000 ]] && echo "$speed $devname"
    done | sort -n | tail -n1 | awk '{print $2}'
  }

  rv=$(__find_1g_device)
  if [ -n "$rv" ]; then
    echo "$rv"
  else
    if [ -d /kind ]; then
      # kind has no 1G device, so return eth0
      echo "eth0"
      return
    else
      echo "error: unable to find 1G device name" >&2
      return 1
    fi
  fi
}

function get_node_ip() {
  # kubectl will work on the controlplane hosts after initial install.
  # for workers, or on initial install, fallback to the 1G device IP
  local node_obj=$(kubectl get node "$(hostname -s)" -ojson 2>/dev/null || true)
  if [ -n "$node_obj" ]; then
    jq -r '.status.addresses[] | select(.type == "InternalIP") | .address' <<<"$node_obj"
  else
    ip -j address show dev "$(get_1g_device_name)" |
      jq -r '.[0].addr_info[0].local' |
      head -n1 |
      tr -d '\n'
  fi
}

function _get_cluster_prop() {
  if [ ! -f ${CLUSTER_CONFIG} ]; then
    echo "error: expected file ${CLUSTER_CONFIG} not present" >&2
    return 1
  fi

  local val=$(yq '.properties["'${1}'"] // ""' ${CLUSTER_CONFIG})
  if [ -z "$val" ]; then
    if [ $# -eq 2 ]; then
      val="$2"
    else
      echo "error: required property ${CLUSTER_CONFIG}::properties.$1 was not present" >&2
      return 1
    fi
  fi
  echo -n "$val"
}

# _get_pkg_prop PROP_PATH DEFAULT_VAL
#    prints the value at PROP_PATH from pkg-properties.yaml or else prints the
#    given default value
function _get_pkg_prop() {
  local prop_path default_val
  prop_path=$1
  default_val=$2
  if [ ! -f ${CLUSTER_PKG_PROPS} ]; then
    echo "$default_val"
    return 0
  fi

  DEFAULT_VAL="$default_val" yq "${prop_path} // env(DEFAULT_VAL)" "${CLUSTER_PKG_PROPS}"
}

# _must_get_pkg_prop PROP_PATH
#    retrieve prop at PROP_PATH or print to stderr and return 1 if not present
function _must_get_pkg_prop() {
  local key="${1}"
  local default="${2:-NOT_SET}"
  if [ ! -f ${CLUSTER_PKG_PROPS} ]; then
    echo "Fatal error: file ${CLUSTER_PKG_PROPS} not found, required for prop ${1}" >&2
    return 1
  fi
  v=$(yq "${key} // \"${default}\"" "${CLUSTER_PKG_PROPS}")
  if [ "$v" = "NOT_SET" ]; then
    echo "${CLUSTER_PKG_PROPS} missing required property ${1}" >&2
    return 1
  fi
  echo "$v"
}

function is_semantic_version_higher() {
  local a=$1
  local b=$2
  local skip_patch_version=${3-"false"}
  # Split versions a and b into major, minor, patch
  IFS='.' read -r a_major a_minor a_patch <<<"$a"
  IFS='.' read -r b_major b_minor b_patch <<<"$b"
  # Compare each part of the version in sequence
  if ((a_major > b_major)); then
    return 0
  elif ((a_major < b_major)); then
    return 1
  fi
  if ((a_minor > b_minor)); then
    return 0
  elif ((a_minor < b_minor)); then
    return 1
  fi
  if [ "$skip_patch_version" == "false" ]; then
    if ((a_patch > b_patch)); then
      return 0
    elif ((a_patch < b_patch)); then
      return 1
    fi
  fi
  # If all parts are equal, versions are the same, return 1 (not higher)
  return 1
}

# await_containers_ready POD_NAME [OLD_RESOURCE_VERSION]
#   Checks if a given kube-system pod has condition ContainersReady and if its
#   resourceVersion not equal to the old version. Times out after 2 minutes.
function await_containers_ready() {
  local PODNAME="$1"
  local OLD_RV="${2:-IGNORE}"
  local NAMESPACE="kube-system"
  local TIMEOUT=$(($(date +%s) + 120))
  local SLEEP_INTERVAL=5

  while true; do
    local POD_JSON POD_RV CONTAINERS_READY
    if POD_JSON=$(kubectl get pod -n $NAMESPACE $PODNAME -ojson); then
      POD_RV=$(jq -r '.metadata.resourceVersion' <<<"$POD_JSON")
      CONTAINERS_READY=$(jq -r '(.status.conditions // [])[] | select(.type == "ContainersReady") | .status' <<<"$POD_JSON")

      if [[ "$CONTAINERS_READY" == "True" && ("$OLD_RV" == "IGNORE" || "$POD_RV" != "$OLD_RV") ]]; then
        echo "Pod $PODNAME/$POD_RV ContainersReady"
        return 0
      else
        echo "Pod $PODNAME/$POD_RV/$CONTAINERS_READY, waiting for ContainersReady and resourceVersion != $OLD_RV."
      fi
    else
      echo "Failed to get status for pod $PODNAME, retrying..."
    fi

    sleep $SLEEP_INTERVAL

    if [ "$(date +%s)" -ge $TIMEOUT ]; then
      echo "Timeout reached, pod $PODNAME did not become Running."
      return 1
    fi
  done
}

function await_check_success() {
  local cmd=$1
  local timeout=${2-60}
  local deadline=$(($(date +%s) + "$timeout"))
  local SLEEP_INTERVAL=5

  echo "checking: $cmd"
  while true; do
    sleep $SLEEP_INTERVAL
    if eval "$cmd"; then
      echo
      echo "check succeeded with $cmd "
      return 0
    fi

    if [ "$(date +%s)" -ge $deadline ]; then
      echo
      echo "check timed out with $cmd"
      return 1
    fi
  done
}

function import_image() {
  ctr -n k8s.io image import "${1}"
}

function cleanup() {
  local reboot=$([ "$1" == "--reboot" ] && echo "true" || true)

  # retry since sometimes removing controlplane instances fails the first time
  retries=0
  max_retries=3
  while ! timeout 180 kubeadm reset -f &>${TMP_PATH}/kubeadm.out; do
    retries=$((retries + 1))
    if [ "${retries}" -eq "${max_retries}" ]; then
      cat ${TMP_PATH}/kubeadm.out
      echo "failed to reset kubeadm after $max_retries tries..."
      return 1
    fi
    echo "kubeadm reset failed $retries/$max_retries times, try again..."
    umount -f /var/lib/kubelet/pods/*/volumes/* || true
  done

  iptables -F &>/dev/null
  iptables -t nat -F &>/dev/null
  iptables -t mangle -F &>/dev/null
  iptables -X &>/dev/null
  {
    ip link | grep cilium | tr '@' ' ' | tr ':' ' ' | awk '{print $2}' |
      xargs -n1 -I{} ip link delete {} &>/dev/null
  } || true

  # remove IPs installed by kube-vip by delete ips not in `hostname -i`
  dev=$(get_1g_device_name)
  for ip in $(ip -4 -o addr show dev "$dev" | awk '{print $4}'); do
    ip_no_mask="${ip%%/*}"
    if ! hostname -i | grep -qw "$ip_no_mask"; then
      echo "Deleting $ip from $dev"
      ip addr del "$ip" dev "$dev"
    else
      echo "Keeping $ip (matches hostname -i)"
    fi
  done

  systemctl stop kubelet &>/dev/null

  rm -rf ~/.kube ~/.docker
  { systemctl disable nginx-snat 2>/dev/null && rm -f /lib/systemd/system/nginx-snat.service; } || true
  rm -rf /etc/kubernetes /etc/cni/* /etc/haproxy /tmp/haproxy*
  rm -rf /var/lib/kubelet /var/lib/etcd
  rm -rf /usr/local/bin/cilium /usr/local/bin/helm
  rm -rf /opt/cerebras/certs /opt/cerebras/tools /opt/cerebras/appliance /opt/cerebras/config* /root/.cs
  rm -rf /n0/cluster-mgmt /n0/log-export /n0/wsjob /n1/* &>/dev/null || true
  rm -f /var/lock/csadm*

  # cleanup all leftover containers/netns
  nerdctl stop $(nerdctl ps -q 2>/dev/null) 2>/dev/null || true
  nerdctl -nk8s.io stop $(nerdctl -nk8s.io ps -q 2>/dev/null) 2>/dev/null || true
  nerdctl -n k8s.io image prune --all -f || true
  ip -all netns delete || true
  # nerdctl can leave stale state, e.g.
  # FATA[0000] name "registry" is already used by ID .. which doesn't exist
  rm -rf /var/lib/nerdctl

  swapoff -a

  # reset containerd
  systemctl stop containerd
  rm -rf /etc/containerd/certs.d
  sed -i 's/k8s.gcr.io/registry.local/g' /etc/containerd/config.toml
  sed -i 's/registry.k8s.io/registry.local/g' /etc/containerd/config.toml
  sed -i "s|\"registry\.local/pause:[^\"]*\"|\"registry.local/pause:${PAUSE_TAG}\"|g" /etc/containerd/config.toml
  rm -rf /var/lib/containerd && mkdir -p /var/lib/containerd
  systemctl start containerd

  if [ -n "${reboot}" ]; then
    nohup sh -c "sleep 1 && reboot" >/dev/null 2>&1 &
    echo "rebooting!"
    exit
  else
    # kill orphaned registry processes
    pkill registry || :
  fi
}

# update static pod manifest
function update_static_manifest() {
  local pod_name=$1
  local wait_time=$2
  local force_update=${3-"false"}
  local wait_for_new_deploy=${4-"false"}

  mkdir /etc/kubernetes/manifests -p
  if ! [ -f /etc/kubernetes/manifests/"${pod_name}.yaml" ]; then
    # Create the static pod for the first time
    cp -f "${pod_name}.yaml" /etc/kubernetes/manifests/"${pod_name}.yaml"
    if [ "$wait_for_new_deploy" != "false" ]; then
      await_containers_ready "${pod_name}-$(hostname)"
      sleep "$wait_time"
    fi
  elif [ "$force_update" != "false" ] || ! diff "${pod_name}.yaml" /etc/kubernetes/manifests/"${pod_name}.yaml" 2>/dev/null; then
    # Update static pod and await the next generation of pod running
    OLD_RESOURCE_VERSION=$(kubectl get pod "${pod_name}-$(hostname)" -ojsonpath='{.metadata.resourceVersion}' -n kube-system || echo "0")
    mv /etc/kubernetes/manifests/"${pod_name}.yaml" "${pod_name}.yaml.old"
    cp -f "${pod_name}.yaml" /etc/kubernetes/manifests/"${pod_name}.yaml"
    if systemctl status kubelet &>/dev/null; then
      # If we're not in a unit-test container:
      echo "updated ${pod_name}.yaml, await successful restart of static pods..."
      await_containers_ready "${pod_name}-$(hostname)" "$OLD_RESOURCE_VERSION"
      echo "${pod_name} containers ready. Wait ${wait_time} seconds for routes/network to stabilize..."
      sleep "$wait_time"
      echo "finished waiting"
    fi
  else
    echo "${pod_name} config unchanged"
  fi
}

function is_haproxy_enabled() {
  enable_haproxy="$(_must_get_pkg_prop '.properties.multiMgmtNodes.enableHaproxy' 'yes')"
  if [ "$IN_KIND" == "true" ] || [ "$enable_haproxy" != "no" ]; then
    return 0
  fi
  return 1
}

# update endpoint on kubeconfig/kubelet if necessary
function populate_endpoint() {
  controlplane_addr=${1}
  controlplane_port=${2}
  if [ -f /root/.kube/config ]; then
    sed -i -E "s|https://.*443|https://${controlplane_addr}:${controlplane_port}|g" /root/.kube/config
  fi
  if [ -f /etc/kubernetes/admin.conf ]; then
    sed -i -E "s|https://.*443|https://${controlplane_addr}:${controlplane_port}|g" /etc/kubernetes/admin.conf
  fi
  if [ -f /etc/kubernetes/kubelet.conf ] && ! grep -q "${controlplane_addr}:${controlplane_port}" /etc/kubernetes/kubelet.conf; then
    sed -i -E "s|https://.*443|https://${controlplane_addr}:${controlplane_port}|g" /etc/kubernetes/kubelet.conf
    systemctl restart kubelet
    # reduce chances of concurrent kubelet traffic spikes
    sleep 5
  fi
}

function prepare_haproxy_cfg() {
  if [ -f "${UPDATED_HAPROXY_CFG}" ]; then
    mv ${UPDATED_HAPROXY_CFG} haproxy.cfg.updated
  fi
  if [ -f "${HAPROXY_YAML}" ]; then
    mv ${HAPROXY_YAML} haproxy.yaml
  fi
  if ! grep kube-apiserver-0 haproxy.cfg.updated -q 2>/dev/null; then
    cp haproxy.cfg haproxy.cfg.updated
    nodes=$(yq -r '.nodes[] | select(.role == "management" and .properties.controlplane == "true") | .name' "${CLUSTER_CONFIG}" | sort)
    index=0
    for node in $nodes; do
      node_ip=$(ssh -o StrictHostKeyChecking=no "${node}" "${TMP_PATH}/${HELPER_FILE} get_node_ip")
      echo "$node ip: $node_ip"
      if [ -z "$node_ip" ]; then
        echo "invalid $node ip: $node_ip"
        exit 1
      fi
      echo "    server kube-apiserver-${index} ${node_ip}:${APISERVER_PORT} check inter 2s fall 3 rise 2 verify none" >>haproxy.cfg.updated
      index=$((index + 1))
    done
  fi
}

function prepare_haproxy() {
  local controlplane_addr=${1}
  local check=${2-"false"}
  if ! is_haproxy_enabled; then
    populate_endpoint "${controlplane_addr}" "${APISERVER_PORT}"
    return 0
  fi

  prepare_haproxy_cfg
  mkdir -p /etc/haproxy
  cfg_updated=""
  if ! [ -f /etc/haproxy/haproxy.cfg ] || ! diff haproxy.cfg.updated /etc/haproxy/haproxy.cfg 2>/dev/null; then
    mv haproxy.cfg.updated /etc/haproxy/haproxy.cfg
    cfg_updated="true"
    echo "haproxy cfg updated"
  fi
  update_static_manifest haproxy 3 $cfg_updated
  if [ "$check" != "false" ]; then
    await_check_success "curl -k https://127.0.0.1:${HAPROXY_PORT}/readyz"
  fi
  populate_endpoint "${controlplane_addr}" "${HAPROXY_PORT}"
}

function prepare_kube_vip() {
  local CONTROLPLANE_ADDR=$1
  local BGP_ROUTER_INTERFACE=${BGP_ROUTER_INTERFACE}
  local ASN
  local PEER_ASN
  local ROUTER_IP
  local MANIFEST=/etc/kubernetes/manifests/kube-vip.yaml

  [ -n "$CONTROLPLANE_ADDR" ] || return 1

  # as a safety measure, get the old router interface from the kube-vip manifest, if it exists
  if [ -z "${BGP_ROUTER_INTERFACE}" ]; then
    if [ -f "${MANIFEST}" ]; then
      BGP_ROUTER_INTERFACE=$(yq '.spec.containers[0].env[] | select(.name == "bgp_routerinterface") | .value' "${MANIFEST}")
    fi
    if [ -z "${BGP_ROUTER_INTERFACE}" ]; then
      BGP_ROUTER_INTERFACE=$(get_1g_device_name)
    fi
  fi

  ASN=$(_must_get_pkg_prop ".properties.multiMgmtNodes.mgmtNodeAsn")
  PEER_ASN=$(_must_get_pkg_prop ".properties.multiMgmtNodes.mgmtRouterAsn")
  if _must_get_pkg_prop ".properties.multiMgmtNodes.mgmtRouterIp" &>/dev/null; then
    ROUTER_IP="$(_must_get_pkg_prop '.properties.multiMgmtNodes.mgmtRouterIp')"
    # Check if ROUTER_IP contains commas therefore multiple peers
    # If so, we se the bgp_peers variable instead of bgp_peer_address
    if [[ "$ROUTER_IP" == *","* ]]; then
      BGP_PEERS_LIST=""
      IFS=',' read -ra ROUTER_IPS <<<"$ROUTER_IP"
      for ip in "${ROUTER_IPS[@]}"; do
        # Add each routerID:PEER_ASN::false to the config
        if [ -n "$BGP_PEERS_LIST" ]; then
          BGP_PEERS_LIST="${BGP_PEERS_LIST},"
        fi
        BGP_PEERS_LIST="${BGP_PEERS_LIST}${ip}:${PEER_ASN}::false"
      done
      # Clear ROUTER_IP since we're using BGP_PEERS_LIST instead
      ROUTER_IP="''"
    fi
  else
    # infer the router IP - generally switches are configured to allow peering to the vlan address. If not, the pkg_prop should be configured for override
    ROUTER_IP=$(ip -j route show | jq -r --arg DEV "${BGP_ROUTER_INTERFACE}" '.[] | select(.dev == $DEV and .gateway != null) | .gateway' | sort | uniq)
    if [[ $(wc -w <<<"${ROUTER_IP}") -ne 1 ]]; then
      ROUTER_IP=""
    fi
  fi
  if [ -z "$ROUTER_IP" ] && [ -z "$BGP_PEERS_LIST" ]; then
    echo "error: unable to determine 1G BGP peer. No default gateway set and no pkg property 'properties.multiMgmtNodes.mgmtRouterIp'"
    echo "please configure a default gateway to 1G router or set ${CLUSTER_PKG_PROPS}::properties.multiMgmtNodes.mgmtRouterIp"
    return 1
  fi

  if ! [ -f "${KV_TEMPLATE}" ]; then
    echo "expected template file $KV_TEMPLATE does not exist!"
    return 1
  fi

  if [ -z "$BGP_PEERS_LIST" ]; then
    echo "creating kube-vip manifest for vip=${CONTROLPLANE_ADDR} on interface=${BGP_ROUTER_INTERFACE}, asn=${ASN} with peer ip=${ROUTER_IP}, asn=${PEER_ASN}"
  else
    echo "creating kube-vip manifest for vip=${CONTROLPLANE_ADDR} on interface=${BGP_ROUTER_INTERFACE}, asn=${ASN} with peers=${BGP_PEERS_LIST}"
  fi
  BGP_ROUTER_INTERFACE=${BGP_ROUTER_INTERFACE} \
    BGP_PEER_AS=${PEER_ASN} \
    BGP_AS=${ASN} \
    BGP_PEERS_LIST=${BGP_PEERS_LIST} \
    BGP_PEER_ADDRESS=${ROUTER_IP} \
    VIP=${CONTROLPLANE_ADDR} \
    envsubst <${KV_TEMPLATE} >kube-vip.yaml
  local wait_time=$(yq '.spec.containers[].env[] | select(.name == "bgp_hold_time") | .value // 30' kube-vip.yaml)
  wait_time=$((wait_time + 3))
  update_static_manifest kube-vip $wait_time
}

# update_controlplane_manifests
#  Runs kubeadm upgrade routine on this node intending to update the manifests
#  with whatever config is in the kubeadm-config configmap.
function update_controlplane_manifests() {
  revert_vip() {
    if [ -f /etc/kubernetes/kube-vip.yaml ]; then
      mv -f /etc/kubernetes/kube-vip.yaml /etc/kubernetes/manifests/
    fi
  }

  # temporarily halt vip from this server - during upgrade, should not accept controlplane requests
  if [ -f /etc/kubernetes/manifests/kube-vip.yaml ]; then
    mv -f /etc/kubernetes/manifests/kube-vip.yaml /etc/kubernetes/
    trap revert_vip EXIT RETURN
    VIP_IP=$(yq -r '.spec.containers[0].env[] | select(.name == "WATCH_IP") | .value' /etc/kubernetes/kube-vip.yaml)
    VIP_INF=$(yq -r '.spec.containers[0].env[] | select(.name == "vip_interface") | .value' /etc/kubernetes/kube-vip.yaml)
    ip a del "${VIP_IP}/32" dev "${VIP_INF}" || true
    echo "await vip teardown" && sleep 10
    await_check_success "nc -z ${VIP_IP} ${CP_VIP_PORT}"
  fi

  # This is a no-op if the kubeadm config did not change.
  kubeadm upgrade node $SKIP_RENEW_CERT
  systemctl restart kubelet
  await_check_success "curl -k https://127.0.0.1:6443/readyz"
}

function update_worker() {
  # create to avoid kubelet spamming about missing manifests dir
  mkdir -p /etc/kubernetes/manifests
  sleep $(((RANDOM % 5) + 1))
  # This is a no-op if the kubeadm config did not change.
  # ensure restart kubelet as failure can be transient
  kubeadm upgrade node $SKIP_RENEW_CERT
  sleep $(((RANDOM % 5) + 1))
  systemctl restart kubelet
}

join_controlplane() {
  local MY_IP=$(get_node_ip)
  local K8_IP_PORT=$3
  local K8_IP=$(echo "$K8_IP_PORT" | cut -d':' -f1)
  local K8_PORT=$(echo "$K8_IP_PORT" | cut -d':' -f2)
  local TOKEN=$5
  local HASH=$7

  if ! ping -W3 -c1 "${K8_IP}" &>/dev/null; then
    echo "${K8_IP} is not pingable, are routes set up correctly?"
    ip route
    exit 1
  fi

  echo "apiVersion: kubeadm.k8s.io/v1beta3
kind: JoinConfiguration
controlPlane:
  certificateKey: $(cat ${TMP_PATH}/.k8s_cert_key | xargs)
discovery:
  bootstrapToken:
    apiServerEndpoint: $K8_IP_PORT
    token: $TOKEN
    caCertHashes:
    - $HASH
nodeRegistration:
  kubeletExtraArgs:
    node-ip: $MY_IP
    container-runtime-endpoint: unix:///var/run/containerd/containerd.sock
    pod-infra-container-image: registry.local/pause:$PAUSE_TAG
    hostname-override: $(hostname)
    ${runtime_arg}" >${TMP_PATH}/join.yaml

  kubeadm join "${K8_IP_PORT}" --config ${TMP_PATH}/join.yaml ${EXTRA_ARGS}
  mkdir -p ~/.kube
  cp /etc/kubernetes/admin.conf ~/.kube/config

  # deploy haproxy first to ensure ability to talk with local endpoint
  echo "creating haproxy..."
  prepare_haproxy "$K8_IP" check

  if [ "${IN_KIND}" == "true" ]; then
    return 0
  fi
  # kube-vip is created on each controlplane node to advertise this node as a VIP-holder
  echo "creating kube-vip..."
  prepare_kube_vip "$K8_IP" "$MY_IP"
  await_containers_ready "kube-vip-$(hostname)"
}

join_worker() {
  local MY_IP=$(get_node_ip)
  local K8_IP_PORT=$3
  local K8_IP=${K8_IP_PORT%%:*}
  local TOKEN=$5
  local HASH=$7

  # idempotent join
  if systemctl is-active --quiet kubelet; then
    echo "Kubelet is already running"
    return 0
  fi

  if ! ping -W3 -c1 "${K8_IP}" &>/dev/null; then
    echo "${K8_IP} is not pingable, are routes set up correctly?"
    ip route
    exit 1
  fi

  echo "apiVersion: kubeadm.k8s.io/v1beta3
kind: JoinConfiguration
discovery:
  bootstrapToken:
    apiServerEndpoint: $K8_IP_PORT
    token: $TOKEN
    caCertHashes:
    - $HASH
nodeRegistration:
  taints:
    - key: node.cilium.io/agent-not-ready
      effect: NoExecute
  kubeletExtraArgs:
    node-ip: $MY_IP
    container-runtime-endpoint: unix:///var/run/containerd/containerd.sock
    pod-infra-container-image: registry.local/pause:$PAUSE_TAG
    hostname-override: $(hostname)
    ${runtime_arg}" >${TMP_PATH}/join.yaml

  mkdir -p /etc/kubernetes/manifests
  kubeadm join "$K8_IP_PORT" --config ${TMP_PATH}/join.yaml ${EXTRA_ARGS}
}

function upgrade_k8s_version() {
  local target_version="${1#v}"
  local is_cp=${2-"false"}
  local first_cp=${3-"false"}
  local new_node=${4-"false"}
  local rpm_path=${5-"$TMP_PATH/k8s/rpm"}
  local version_cache_file="$TMP_PATH/k8s_version_info"
  local local_client_version=$(kubectl version -ojson 2>/dev/null | jq -r ".clientVersion.gitVersion" | sed 's/^v//')
  local local_version=$local_client_version
  if [ "$is_cp" != "false" ]; then
    if ! kubectl version 1>/dev/null; then
      echo "controlPlane node api-server down, can't get server version, exit"
      return 1
    fi
    # for CP case, version is on both client + static pod, use the lower one
    local local_server_version=$(kubectl version -ojson 2>/dev/null | jq -r ".serverVersion.gitVersion" | sed 's/^v//')
    if is_semantic_version_higher "$local_client_version" "$local_server_version"; then
      local_version=$local_server_version
    fi
  fi
  # skip version check for new reinstall case
  if [ "${K8S_INSTALL_VERSION}" != "${LOCAL_K8S_VERSION}" ]; then
    echo "force new install detected, proceed to install"
  else
    if is_semantic_version_higher "$local_version" "$target_version"; then
      echo "Warning: target version $target_version < local version $local_version, skip upgrade $(hostname)"
      return 0
    elif [ "$local_version" == "$target_version" ]; then
      # for equal case, only skip if target version already found in cache file meaning to avoid partial install
      if [ -f "$version_cache_file" ] && grep -q "$target_version" "$version_cache_file"; then
        echo "Warning: Version $target_version already processed, skip upgrade on $(hostname)"
        return 0
      fi
    fi
  fi

  local target_major_minor_version=$(echo "$target_version" | awk -F '.' '{print $1"."$2}')
  # for kind test case, kind node has only debian release
  if grep -i debian /etc/os-release && [ -d /kind ]; then
    echo "deb [trusted=yes] https://pkgs.k8s.io/core:/stable:/v${target_major_minor_version}/deb/ /" >/etc/apt/sources.list.d/kubernetes.list
    DEBIAN_FRONTEND=noninteractive dpkg --configure -a
    apt-get update
    apt-get install -y kubelet="${target_version}*" kubectl="${target_version}*" kubeadm="${target_version}*" -o Dpkg::Options::="--force-confold" || true
  else
    rpm -Uvh --force --replacepkgs "$rpm_path/${target_version}/*"
  fi
  export LOCAL_K8S_VERSION="${target_version}"
  export COREDNS_TAG=${COREDNS_VERSIONS[$target_major_minor_version]}
  export ETCD_TAG=${ETCD_VERSIONS[$target_major_minor_version]}
  export PAUSE_TAG=${PAUSE_VERSIONS[$target_major_minor_version]}
  if [ "${target_version}" != "1.24.4" ]; then
    if [ -f /var/lib/kubelet/kubeadm-flags.env ]; then
      sed -i 's/--container-runtime=remote //g' /var/lib/kubelet/kubeadm-flags.env
    fi
  fi
  if ! grep -wq "pause:${PAUSE_TAG}" /etc/containerd/config.toml; then
    sed -i -E "s/pause:3\.[0-9]{1,2}/pause:${PAUSE_TAG}/g" /etc/containerd/config.toml && systemctl restart containerd
  fi
  if [ -f /var/lib/kubelet/kubeadm-flags.env ]; then
    if ! grep -wq "pause:${PAUSE_TAG}" /var/lib/kubelet/kubeadm-flags.env; then
      sed -i -E "s/pause:3\.[0-9]{1,2}/pause:${PAUSE_TAG}/g" /var/lib/kubelet/kubeadm-flags.env
    fi
  fi
  # skip upgrade/check for new node which joins later
  if [ "$new_node" != "false" ]; then
    return
  fi

  # upgrading servers
  if [ "$first_cp" != "false" ]; then
    local tmp_dir
    tmp_dir=$(mktemp -d)
    kubectl get cm -n kube-system kubeadm-config -ojsonpath='{.data.ClusterConfiguration}' | yq . >"${tmp_dir}/orig.yaml"
    # update image tag with current version first before upgrading
    yq '
      .dns.imageTag = strenv(COREDNS_TAG) |
      .etcd.local.imageTag = strenv(ETCD_TAG)
    ' "${tmp_dir}/orig.yaml" >"${tmp_dir}/modified.yaml"
    kubectl create cm -n kube-system kubeadm-config -oyaml --dry-run=client --from-file=ClusterConfiguration="${tmp_dir}/modified.yaml" | kubectl apply -f-
    kubeadm upgrade apply "v${target_version}" $SKIP_RENEW_CERT -y $EXTRA_ARGS
    systemctl daemon-reload
    systemctl restart kubelet
  else
    # always restart kubelet in case of transient failure to ensure version reflected to api-server
    sleep $(((RANDOM % 5) + 1))
    kubeadm upgrade node $SKIP_RENEW_CERT
    systemctl daemon-reload
    sleep $(((RANDOM % 5) + 1))
    systemctl restart kubelet
  fi
  # ensure cp node server up before proceeding
  if [ "$is_cp" != "false" ]; then
    await_check_success "curl -k https://127.0.0.1:6443/readyz"
  fi
  # Append the target version to the cache file after successful upgrade
  echo "$target_version" >>"$version_cache_file"
}

# Runtime vars

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  CMD=$1
  shift

  if [[ "$CMD" =~ ^(upgrade_k8s_version|cleanup|join_worker|update_worker|join_controlplane|prepare_kube_vip|prepare_haproxy|populate_endpoint|update_controlplane_manifests|get_node_ip|import_image|await_containers_ready|await_check_success)$ ]]; then
    $CMD "$@"
  else
    echo "Invalid command: $CMD $*"
  fi
fi
