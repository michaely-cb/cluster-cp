#!/bin/bash

# update the url for your own testing
url='https://alertmanager.mb-systemf107.cerebras.com/api/v1/alerts'

mode=""
type=""
name=""
device=""
alert_type=""
for i in "$@"; do
  case $i in
  --mode=*)
    mode="${i#*=}"
    shift # past argument=value
    ;;
  --type=*)
    type="${i#*=}"
    shift # past argument=value
    ;;
  --name=*)
    name="${i#*=}"
    shift # past argument=value
    ;;
  --nic=*)
    device="${i#*=}"
    shift # past argument=value
    ;;
  --port=*)
    device="${i#*=}"
    shift # past argument=value
    ;;
  --*)
    echo "Unknown option $i"
    echo "usage:
      $0 --mode=<fire/resolve> --type=<system/node/switch> --name=<node-name/system-name/group-name> [--nic=<nic-name>] [--port=<port-name>]
    "
    exit 1
    ;;
  *) ;;
  esac
done

generate_post_data() {
  cat <<EOF
[{
  "status": "firing",
  "labels": {
    "alertname": "TestAlert-${name}",
    "type": "${alert_type}",
    "instance": "${name}",
    "device": "${device}",
    "severity":"warning",
    "cluster_mgmt": "true"
  },
  "annotations": {
    "summary": "Test alert firing with ${alert_type} on ${name} ${device}"
  },
  $1
  $2
  "generatorURL": "http://test"
}]
EOF
}

if [[ $type == "system" ]]; then
  alert_type="SystemError"
  if [[ -n ${device} ]]; then
    alert_type="SystemPortError"
  fi
elif [[ $type == "node" ]]; then
  alert_type="NodeError"
  if [[ -n ${device} ]]; then
    alert_type="NodeNICError"
  fi
elif [[ $type == "switch" ]]; then
  alert_type="NodeSwitchPortError"
else
  echo "Wrong type: $mode, supported ones: 'system'/'node'/'switch' "
fi

startsAt='"startsAt" : "'$(date -u "+%FT%TZ")'",'
if [[ $mode == "fire" ]]; then
  echo "Firing alert ${name} "
  POST_DATA=$(generate_post_data "${startsAt}")
  echo "${POST_DATA}"
  curl -k $url --data "$POST_DATA"
elif [[ $mode == "resolve" ]]; then
  echo "Resolving alert ${name} "
  endsAt='"endsAt" : "'$(date -u "+%FT%TZ")'",'
  POST_DATA=$(generate_post_data "${startsAt}" "${endsAt}")
  echo "${POST_DATA}"
  curl -k $url --data "$POST_DATA"
else
  echo "Wrong mode: $mode, supported ones: 'fire'/'resolve' "
fi
