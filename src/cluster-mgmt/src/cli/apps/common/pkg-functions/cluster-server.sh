# Common functions relevant to cluster-server

export USER_AUTH_SECRET_DIR="/root/.cerebras"

get_fabric_type() {
  local fabric_type=$($KUBECTL -n ${SYSTEM_NAMESPACE} set env deployment/cluster-server --list | grep COMPILE_SYSTEM_TYPE | cut -d '=' -f2)
  if [ -z "$fabric_type" ]; then
    fabric_type="cs2"
  fi
  echo "$fabric_type"
}

generate_csctl_cfg() {
  local config_v2_fp="$1"
  # When "ns" is not passed, a config_v2 with empty certificate authority will be generated.
  # This is needed for usernode configs reset.
  local ns="$2"
  local use_nodeport="${3:-false}"
  # Base authority value is namespace agnostic
  local base_authority=$($KUBECTL -n ${SYSTEM_NAMESPACE} get ingress cluster-server -ojson | jq -r '.spec.rules[0].host // empty')

  local server_addr
  if $use_nodeport; then
    local nodeport=$(
      $KUBECTL get svc -n ingress-nginx ingress-nginx-controller \
        -o jsonpath='{.spec.ports[?(@.name=="https")].nodePort}'
    )
    server_addr="localhost:${nodeport}"
  else
    server_addr="$(get_mgmt_data_ip):443"
  fi

  fabric_type=$(get_fabric_type)

    # this is csctl compatible config
    cat <<EOF >"$config_v2_fp"
{
  "clusters": [
    {
      "name": "$cluster_name",
      "server": "$server_addr",
      "fabricType": "$fabric_type",
      "authority": "${base_authority}",
      "namespaces": []
    }
  ],
  "contexts": [
    {
      "cluster": "$cluster_name",
      "name": "$cluster_name"
    }
  ],
  "currentContext": "$cluster_name"
}
EOF

  if [ -n "$ns" ]; then
    local secret_name=$($KUBECTL -n $ns get ingress cluster-server -ojsonpath='{.spec.tls[0].secretName}')
    if [ "$?" != "0" ] || [ -z "$secret_name" ]; then
      echo "error: secret of cluster server ingress not found in '$ns' namespace, was cluster-server deployed?" >&2
      return 1
    fi
    local cluster_server_cert_data=$($KUBECTL -n $ns get secret "$secret_name" -ojsonpath='{.data.tls\.crt}')
    if [ "$?" != "0" ] || [ -z "$cluster_server_cert_data" ]; then
      echo "error: cluster server cert not found in '$ns' namespace, was cluster-server deployed?" >&2
      return 1
    fi
    temp_file=$(mktemp)
    jq --arg ns "$ns" --arg cert_data "$cluster_server_cert_data" '.clusters[0].namespaces = [{"name": $ns, "certificateAuthorityData": $cert_data}]' "$config_v2_fp" >"$temp_file"
    mv -f "$temp_file" "$config_v2_fp"
  else
    echo "skip adding certificate authority data due to usernode configs reset is in progress"
  fi

  chmod 644 "$config_v2_fp"
}
