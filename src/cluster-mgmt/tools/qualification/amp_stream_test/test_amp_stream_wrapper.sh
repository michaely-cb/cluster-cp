#!/usr/bin/env bash

# Launch a wsjob with chief, weight and br services, each with some custom
# commands.

set -e

function usage() {
    echo "Usage: $0 [--help] [--num-wafers] [--streamers] [--schedules] [--sample-size] [--eth-ports] [--streamer-ids]"
}

function die() {
    printf '%s\n' "$1" >&2
    exit 1
}

CLUSTER_CFG_JSON="/opt/cerebras/cfg/br_cfg_json"
STREAMER_IDS=""
ETH_PORTS=""

function get_inputs() {
  while :; do
    case $1 in
      -h|-\?|--help)
        usage
        exit
        ;;
      --streamers)
        if [ "$2" ]; then
          STREAMERS=$2
          shift 2
        else
          die 'ERROR: "--streamers" requires a non-empty integer argument'
        fi
        ;;
      --streamers=*)
        STREAMERS="${1#*=}"
        shift
        ;;
      --schedules)
        if [ "$2" ]; then
          SCHEDULES=$2
          shift 2
        else
          die 'ERROR: "--schedules" requires a non-empty integer argument'
        fi
        ;;
      --schedules=*)
        SCHEDULES="${1#*=}"
        shift
        ;;
      --sample-size)
        if [ "$2" ]; then
          SAMPLE_SIZE=$2
          shift 2
        else
          die 'ERROR: "--sample-size" requires a non-empty integer argument'
        fi
        ;;
      --sample-size=*)
        SAMPLE_SIZE="${1#*=}"
        shift
        ;;
      --eth-ports)
        if [ "$2" ]; then
          ETH_PORTS=$2
          shift 2
        else
          die 'ERROR: "--eth-ports" requires a non-empty integer argument'
        fi
        ;;
      --eth-ports=*)
        ETH_PORTS="${1#*=}"
        shift
        ;;
      --streamer-ids)
        if [ "$2" ]; then
          STREAMER_IDS=$2
          shift 2
        else
          die 'ERROR: "--streamer-ids" requires a non-empty integer argument'
        fi
        ;;
      --streamer-ids=*)
        STREAMER_IDS="${1#*=}"
        shift
        ;;
      --)
        shift
        ;;
      -?*)
        printf 'WARN: Unknown option (ignored): %s\n' "$1" >&2
        shift
        ;;
      *)
        break
        ;;
    esac
  done
}

function validate_input() {
  if [[ "${STREAMER_IDS}" == "" ]]; then
    echo "--streamer-ids needs to be set"
    exit 1
  fi

  if [[ "${ETH_PORTS}" == "" ]]; then
    echo "--eth-ports needs to be set"
    exit 1
  fi

  STREAMER_IDS=(${STREAMER_IDS})
  ETH_PORTS=(${ETH_PORTS})
  if [[ "${#STREAMER_IDS[@]}" != "${#ETH_PORTS[@]}" ]]; then
    echo "--streamer-ids and --eth-ports must have same number of elements"
  fi
}

# Wait for all streamers with the same streamer id to complete.
# When all streamers complete, a file ${streamer_id}.done will be
# created under the top job directory. This function waits for that
# file to show up in the directory.
function wait_for_streamer_id() {
  local streamer_id=$1
  while true; do
    if [[ -f "${PWD}../${streamer_id}.done" ]]; then
      echo "Streamer ${streamer_id} completed"
      break
    fi
    echo "Waiting for streamer ${streamer_id} to complete"
    sleep 1
  done
}

function run_test() {
  cp ${PWD}/../wio_groups.json wio_groups.json
  set -o pipefail
  for id in "${!STREAMER_IDS[@]}"; do
    echo "Running test_amp_stream on port ${ETH_PORTS[id]}"
    test_amp_stream -v -r streamer -C  ${CLUSTER_CFG_JSON} -s ${STREAMERS} -i ${STREAMER_IDS[id]} \
      -S ${SCHEDULES} -b ${SAMPLE_SIZE} -p simple -z 0 -a 0 -G -E ${ETH_PORTS[id]} \
       2>&1 | tee -a "${PWD}/${MY_POD_NAME}.log"
  done
}

function wait_for_programming() {
  while true; do
    pgm_done=$(cat /opt/cerebras/state_cfg/pgm_done)
    if [[ "${pgm_done}" == "true" ]]; then
      echo "Programming completed"
      break
    fi
    echo "Waiting for programming to complete"
    sleep 5
  done
}

get_inputs "$@"
validate_input

# It appears that workingDir is handled different in 1.6 and master by the cluster-server/job-operator.
# We check the current workingDir. If it is not role specific, we create the role-specific directory,
# and cd into it before running the rest of the commands.
if [[ ${PWD} == *weight-${REPLICA_ID} ]]; then
  echo "Current workingDir ${PWD}"
else
  mkdir -p ${PWD}/weight-${REPLICA_ID}
  cd ${PWD}/weight-${REPLICA_ID}
  echo "Change workingDir to ${PWD}"
fi

wait_for_programming
run_test