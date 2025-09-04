# Common functions related to multus

export DEFAULT_NET_ATTACH="multus-data-net"

function has_data_network() {
  yq -e '.v2Groups[0].vlans[0].parentnet // .groups[0].switchConfig.parentnet' ${CLUSTER_CONFIG} &>/dev/null
}

function get_data_network() {
  yq '.v2Groups[0].vlans[0].parentnet // .groups[0].switchConfig.parentnet // ""' ${CLUSTER_CONFIG}
}

function should_install_multus() {
  # kind cluster always installs multus
  if [ -d "/kind" ]; then
    return 0
  fi

  # note: function only applies to clusters where switchConfig is populated
  local parentnet=$(get_data_network)
  if [ -z "${parentnet}" ]; then
    return 1
  fi

  # check if k8 was installed on 100G network - shouldn't have multus then
  local node_ip=$($KUBECTL get nodes -ojson |
    jq -r '.items[] | (.status.addresses// [])[] | select(.type == "InternalIP") | .address' | head -n1)

  ! subnet_contains_ip "${parentnet}" "${node_ip}"
}
