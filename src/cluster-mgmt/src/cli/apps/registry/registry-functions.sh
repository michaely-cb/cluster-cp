#!/usr/bin/env bash

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"
# skip check due to static registry can be installed before k8s install
SKIP_CHECK=true source ../pkg-common.sh

function is_static_registry() {
  local STATIC=0
  local K8=1
  if [ -n "$FORCE_STATIC" ]; then
    echo "override detected, forcing static registry" >&2
    return $STATIC
  fi

  if kubectl get deploy -n kube-system private-registry &>/dev/null ||
    kubectl get ds -n kube-system private-registry &>/dev/null; then
    echo "k8 registry already installed, use k8 registry" >&2
    return $K8
  fi

  has_cni=$({ kubectl get ds -n kube-system cilium &>/dev/null || kubectl get ds -n kube-system kindnet &>/dev/null; } && echo "true" || echo "false")
  if ! $has_cni; then
    echo "cni not installed, use static registry" >&2
    return $STATIC
  fi

  if ! has_multiple_mgmt_nodes; then
    echo "cni installed, use k8 registry" >&2
    return $K8
  fi

  # ensure pvc was created (ceph) and that the 100G vip was created
  has_mmn_deps=$({ kubectl get pvc -n kube-system registry-pvc &>/dev/null && kubectl get -n kube-system ds kube-vip &>/dev/null; } && echo "true" || echo "false")
  if $has_mmn_deps; then
    echo "multi-mgmt node dependencies installed, use k8 registry" >&2
    return $K8
  fi
  echo "multi-mgmt node dependencies not installed, use static registry" >&2
  return $STATIC
}

function get_registry_cert() {
  cert_b64=$(kubectl get secret registry-tls-secret -n kube-system --ignore-not-found -o=jsonpath='{.data.tls\.crt}')
  if [ "$?" -ne 0 ] || [ -z "${cert_b64}" ]; then
    return 1
  fi
  echo "${cert_b64}" | base64 -d >registry_tls.crt
}

# Check that the cert was issued for this cluster specific domain, and includes
# the mgmt node's hostname as one of the SANs.
# usage: $@ = list of data IPs that host the registry
function validate_existing_cert() {
  if ! get_registry_cert; then
    return 1
  fi

  local issuer=$(openssl x509 -in registry_tls.crt -nocert -noout -issuer)
  local expected_issuer="issuer=CN = ${service_domain}"
  if [[ "$issuer" != $expected_issuer* ]]; then
    echo "invalid cert issued for ${issuer}, expecting ${service_domain}" >&2
    return 1
  fi

  local sans=$(openssl x509 -in registry_tls.crt -nocert -ext=subjectAltName)
  for node_ip in "$@"; do
    if [[ "$sans" != *"IP Address:${node_ip}"* ]]; then
      echo "invalid cert sans ${sans}, expected address ${node_ip} not included" >&2
      return 1
    fi
  done

  if [[ "$sans" != *DNS:${REGISTRY_URL}* ]]; then
    echo "invalid cert sans ${sans}, expected DNS name ${REGISTRY_URL} not included" >&2
    return 1
  fi

  return 0
}

# usage: args = list of data IPs that host the registry where the first IP is pushable, subsequent IPs are mirrors
function create_registry_secret() {
  REGISTRY_URL=${registry_url}

  local subjectAltName="DNS:*.$REGISTRY_URL,DNS:$REGISTRY_URL,DNS:$MGMT_NODE_NAME,IP:${MGMT_NODE_IP}"
  for ip in "$@"; do
    subjectAltName="${subjectAltName},IP:${ip}"
  done

  cat <<EOF >ssl.conf
[req]
distinguished_name=req
[san]
subjectAltName=${subjectAltName}
EOF
  openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
    -keyout registry_tls.key \
    -out registry_tls.crt -subj "/CN=$service_domain" \
    -extensions san -config ssl.conf

  # for static registry in new cluster where k8s not installed yet, skip secrete upload
  if ! kubectl get po &>/dev/null && is_static_registry; then
    return 0
  fi

  # delete the secret if it exists.
  kubectl delete secret tls registry-tls-secret -n kube-system \
    --ignore-not-found
  # delete registry deployment/ds if it exists to get around the issue of the
  # old deployment pod not picking up the new secret fast enough during
  # redeployment.
  for deploy_type in ds deploy; do
    kubectl delete $deploy_type -nkube-system private-registry --ignore-not-found
    # wait for previous deploy termination to avoid port contention
    kubectl wait --timeout=1m --for=delete $deploy_type -nkube-system private-registry
  done

  kubectl create secret tls registry-tls-secret \
    --key registry_tls.key \
    --cert registry_tls.crt \
    -n kube-system
}

# creates directory certs.d in the current directory, containing the registry configurations.
# usage: args = list of data IPs that host the registry where the first IP is pushable, subsequent IPs are mirrors
function create_certs_config() {
  local CA_CERT_PATH=${CERTS_DIR}/registry_tls.crt
  local CLUSTER_REGISTRY_URL=${registry_url}

  primary_endpoint="${1}:5000"
  local mirrors_doc="registry-mirrors.toml"
  if [ ${#} -gt 1 ]; then
    echo "" >$mirrors_doc
    shift
    cat <<EOF >${mirrors_doc}

# Mirrors (resolve/pull only)
# Improves availability of containerd pull operations during vip downtime.
# Note: Push operations can only be performed by 1 entry in the hosts file so
# cannot make mirrors pushable.
EOF
    # if dnsmasq on deployment node found, update
    if systemctl status dnsmasq && grep -q "/etc/hosts.d" /etc/dnsmasq.conf; then
      # recreate file if exists
      echo "" >registry.conf.new
      for endpoint in "$@"; do
        echo "$endpoint $registry_url" >>registry.conf.new
      done
      mv registry.conf.new /etc/hosts.d/registry.conf
      systemctl restart dnsmasq
      sleep 3
    fi
    # if registry can be resolved on non-VIP, add as primary pull host which will get better load balance
    if ping -W1 -c1 "$registry_url" | grep "bytes of data" | grep -v "$primary_endpoint"; then
      cat <<EOF >>${mirrors_doc}
[host."$registry_url:5000"]
  capabilities = ["pull", "resolve"]
  ca = "${CA_CERT_PATH}"
EOF
    fi

    # still add fallback to individual nodes which will be shuffled to get some degree of balance if dns not available
    for ((i = 0; i < $#; i++)); do
      cat <<EOF >>${mirrors_doc}
[host."ENDPOINT-REPLACE-$i:5000"]
  capabilities = ["pull", "resolve"]
  ca = "${CA_CERT_PATH}"
EOF
    done
  fi

  # clean up local cache
  rm -rf certs.d

  # see https://github.com/containerd/containerd/blob/main/docs/hosts.md
  # for details on the certs.d directory structure.
  mkdir -p certs.d/registry.k8s.io
  cat <<EOF >certs.d/registry.k8s.io/hosts.toml
# Enable registry.k8s.io as the primary mirror for registry.k8s.io
# See: https://github.com/kubernetes/k8s.io/issues/3411

server = "https://registry.k8s.io"

[host."registry.k8s.io"]
  capabilities = ["pull", "resolve"]
EOF

  mkdir -p certs.d/"${CLUSTER_REGISTRY_URL}"
  capability='"pull", "resolve", "push"'
  if [ ${#} -gt 1 ]; then
    capability='"resolve", "push"'
  fi
  cat <<EOF >certs.d/"${CLUSTER_REGISTRY_URL}"/hosts.toml
server = "https://${CLUSTER_REGISTRY_URL}"

[host."${primary_endpoint}"]
  capabilities = [${capability}]
  ca = "${CA_CERT_PATH}"
$(cat ${mirrors_doc})
EOF
}

# usage: args = list of data IPs that host the registry where the first IP is pushable, subsequent IPs are mirrors
function update_containerd_certs() {
  local lead_node_only=$1
  shift

  if [ "$lead_node_only" = "lead_node_only" ]; then
    echo $MGMT_NODE_DATA_IP >.lead_node_ip
    NODE_LIST="${PWD}/.lead_node_ip"
  fi
  if validate_existing_cert "$@"; then
    echo "Existing cert is valid, skip creation"
  else
    create_registry_secret "$@"
  fi

  create_certs_config "$@"

  # Always copy over certs.d and config.toml even when the cert exists. This is needed when
  # new nodes are added to the cluster.
  echo "start copying registry certs to every node"
  PSSH "mkdir -p ${CERTS_DIR} && rm -rf /etc/containerd/certs.d"
  PSCP registry_tls* "${CERTS_DIR}"
  PSCP -r certs.d "/etc/containerd/"
  if [ ${#} -gt 1 ]; then
    shift
    # shuffle mirrors in case dnsmasq not available/down
    cmd="shuffled_ips=(\$(printf \"%s\n\" $* | shuf)); \
        for ((i=0; i<\${#shuffled_ips[@]}; i++)); do sed -i s/ENDPOINT-REPLACE-\$i/\${shuffled_ips[\$i]}/g \
          /etc/containerd/certs.d/${registry_url}/hosts.toml; done"
    PSSH "$cmd"
  fi

  if [ -s config.toml ]; then
    echo "replacing containerd config on every node"
    PSCP config.toml "/etc/containerd/config.toml"
    PSSH "systemctl restart containerd"
  else
    # empty containerd configuration in local registry/, skip override.
    # resorting to splicing config_path variable in existing configuration
    echo "patching existing containerd config on every node"
    PSSH "cp '/etc/containerd/config.toml' '/etc/containerd/config.toml.original' && \
            sed -i -e 's/config_path = \"\"/config_path = \"\/etc\/containerd\/certs.d\/\"/g' '/etc/containerd/config.toml' && \
            systemctl restart containerd"
  fi
  await_condition 10 stat /run/containerd/containerd.sock
}
