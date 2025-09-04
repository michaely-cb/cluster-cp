# Command functions related to systems

function has_multiple_systems() {
  count=$(yq '.systems|length' ${CLUSTER_CONFIG})
  if [ "${count}" -gt 1 ]; then
    return 0
  fi
  return 1
}

function has_system_type() {
  local system_type=$1
  local system_types=$(yq '.systems[].type' ${CLUSTER_CONFIG} | sort | uniq)
  if [ $(wc -w <<<"$system_types") -gt 1 ]; then
    return 1
  fi
  [ "$system_types" == "$system_type" ]
}

# Return the common system type for all CS-x systems in cluster.yaml, if they have the same type.
# Otherwise, return unknown.
function get_system_type() {
  # By default, we exclude the systems where the control addresses are compile only or kapi.
  # For example: "~ws_compile_only~:9000"
  # We'd only consider them only when there is no other systems in the cluster.
  system_types=$(yq -r '.systems[] | select(.controlAddress | test("~") | not) | .type // ""' $CLUSTER_CONFIG)
  if [ -z "$system_types" ]; then
    system_types=$(yq -r '.systems[] | .type // ""' $CLUSTER_CONFIG)
  fi
  system_type="unknown"
  for type in $system_types; do
    if [ "$system_type" = "unknown" ]; then
      system_type=$type
      continue
    fi

    if [ "$system_type" != "$type" ]; then
      system_type="unknown"
      break
    fi
  done
  echo $system_type
}

function get_system_mgmt_addr() {
  local admin_user=$1
  local admin_pwd=$2
  local system_name=$3
  local tag=$(get_image_version alpine-containerd)

  network_info=$($NERDCTL -n k8s.io run --rm registry.local/alpine-containerd:$tag sshpass -p $admin_pwd \
    ssh -q -oStrictHostKeyChecking=no -oConnectTimeout=10 $admin_user@$system_name 'network show --output-format=json')
  ip_address=$(echo $network_info | jq -r '.mgmt.ipAddress')
  echo $ip_address | cut -d'/' -f1
}


function update_system_mgmt_addrs() {
  # Make sure that the managment addressse for the systems are in cluster.yaml.
  system_mgmt_addrs=$(yq -r '.systems[] | .managementAddress // ""' $CLUSTER_CONFIG)
  num_mgmt_addrs=$(echo $system_mgmt_addrs | wc -w)
  num_systems=$(yq -r '.systems | length' $CLUSTER_CONFIG)

  if [ "$num_mgmt_addrs" = "$num_systems" ]; then
    return
  fi

  admin_user=$(get_admin_svc_user)
  admin_pwd=$(get_admin_svc_password)
  system_no=0
  updated="false"
  while [ $system_no -lt $num_systems ]; do
    system_mgmt_addr=$(yq -r ".systems[$system_no] | .managementAddress // \"\"" $CLUSTER_CONFIG)
    system_control_addr=$(yq -r ".systems[$system_no] | .controlAddress // \"\"" $CLUSTER_CONFIG)
    if [[ "$system_control_addr" == ~* ]]; then
      system_no=$(($system_no + 1))
      continue
    fi

    if [ -z "$system_mgmt_addr" ]; then
      system_name=$(yq -r ".systems[$system_no] | .name // \"\"" $CLUSTER_CONFIG)
      system_mgmt_addr=$(get_system_mgmt_addr $admin_user $admin_pwd $system_name)
      if [ -n "$system_mgmt_addr" ]; then
        yq e -i ".systems[$system_no].managementAddress = \"$system_mgmt_addr\"" ${CLUSTER_CONFIG}
        updated="true"
      fi
    fi
    system_no=$(($system_no + 1))
  done

  if [ "$updated" = "true" ]; then
    update_cluster_cm
  fi
}

function create_system_admin_secret() {
  local system_admin_user=$1
  local system_admin_password=$2
  local namespace=$3
  cat << EOM | $KUBECTL -n $namespace apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: system-basic-auth
type: kubernetes.io/basic-auth
stringData:
  username: $system_admin_user
  password: $system_admin_password
EOM
}
