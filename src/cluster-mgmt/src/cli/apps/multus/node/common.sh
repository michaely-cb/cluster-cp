# Define common functions for multus deployment
function create_net_attach_config() {
  # Name can be empty
  local name="$1"
  local group="$2"
  local vlan="$3"
  local nic="$4"
  local config_file="$5"
  local output_file="$6"
  local template_file=${7:-"net-attach-config.jsontemplate"}
  local switch_config_file="switch-config.json"

  if [ -z "$nic" ]; then
    echo "invalid input: 'nic' is empty" >&2
    return 1
  fi

  # convert role to vlan name
  if [ "$vlan" == "broadcastreduce" ]; then
    vlan="swarmx"
  else
    vlan="memx"
  fi

  GROUP=$group VLAN=$vlan yq -ojson '.v2Groups[] | select(.name == strenv(GROUP)) | .vlans[] | select(.name == strenv(VLAN))' ${config_file} >"${switch_config_file}"
  if [ ! -s "${switch_config_file}" ]; then
    GROUP=$group yq -ojson '.groups[] | select(.name == strenv(GROUP)) | .switchConfig' ${config_file} >"${switch_config_file}"
    if [ ! -s "${switch_config_file}" ]; then
      echo "invalid switch configuration: $group does not have a switchConfig" >&2
      return 1
    fi
  fi

  required_fields='.subnet, .parentnet, .gateway, .virtualStart, .virtualEnd'
  if ! jq -e -r '['"$required_fields"'] | map(. != null and . != "") | all' "$switch_config_file" &>/dev/null; then
    echo "invalid switch configuration: $group does not have all the required fields ($required_fields)" >&2
    return 1
  fi

  local gateway=$(jq -r '.gateway // ""' $switch_config_file)
  local start=$(jq -r '.virtualStart // ""' $switch_config_file)
  local end=$(jq -r '.virtualEnd // ""' $switch_config_file)
  local subnet=$(jq -r '.subnet // ""' $switch_config_file)
  local parentnet=$(jq -r '.parentnet' $switch_config_file)

  if [ -z "$nic" ]; then
    nic=$(jq -r '.'$role'NIC // ""' $switch_config_file)
    if [ -z "$nic" ]; then
      echo "invalid input: nic name is not found in both node and group switch configuration"
      return 1
    fi
  fi

  echo "$group :: parentnet=$parentnet, subnet=$subnet, nic=$nic, start=$start, end=$end"

  local nad_json
  nad_json=$(jq -n \
    --arg name "$name" \
    --arg nic "$nic" \
    --arg subnet "${subnet}" \
    --arg start_ip "${start}" \
    --arg end_ip "${end}" \
    --arg parentnet "${parentnet}" \
    "$(cat $template_file)")

  if [ "$parentnet" != "$subnet" ]; then
    echo "$group :: +gw=${gateway}"
    nad_json=$(jq --arg net "${parentnet}" --arg gw "${gateway}" \
      '.ipam.routes += [{"dst": $net, "gw": $gw}]' <<<"$nad_json")
  fi

  echo "$nad_json" >"${output_file}.tmp"
  mv "${output_file}.tmp" "${output_file}"
}
