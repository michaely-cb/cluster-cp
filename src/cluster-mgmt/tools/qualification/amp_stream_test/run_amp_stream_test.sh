#!/usr/bin/env bash

# Launch a wsjob with chief, weight and br services, each with some custom
# commands.

function usage() {
    echo "Usage: $0 [--help] [--num-wafers] [--pg] [--streamers] [--schedules] [--sample-size]"
    echo "Run test_amp_stream to validate the traffic to CS-2 ports."
    echo "Arguments:"
    echo "  --num-wafers: Number of wafers in this test. Default to 2"
    echo "  --pg: Either 1 or 4 for PortGroups"
    echo "  --streamers: Number of streamers in the test. Default to 40 for PG1 and 10 for PG4"
    echo "  --schedules: Number of schedules. Default to 10000"
    echo "  --sample-size: Sample size. Default to 65536 for PG1 and 262144 for PG4"
}

function die() {
    printf '%s\n' "$1" >&2
    exit 1
}

set -e

SCRIPT_PATH=$(dirname "$0")
cd ${SCRIPT_PATH}

NUM_WAFERS=2
ELF=""
STREAMERS=0
SCHEDULES=0
SAMPLE_SIZE=0
PG=1

PSSH_MAX_THREADS=10

function get_inputs() {
  while :; do
    case $1 in
      -h|-\?|--help)
        usage
        exit
        ;;
      --num-wafers)
        if [ "$2" ]; then
          NUM_WAFERS=$2
          shift 2
        else
          die 'ERROR: "--num-wafers" requires a non-empty integer argument'
        fi
        ;;
      --num-wafers=*)
        NUM_WAFERS="${1#*=}"
        shift
        ;;
      --pg)
        if [ "$2" ]; then
          PG=$2
          shift 2
        else
          die 'ERROR: "--pg" requires a non-empty argument'
        fi
        ;;
      --pg=*)
        PG="${1#*=}"
        shift
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

function validate_inputs() {
  if [[ "${PG}" != "1" ]] && [[ "${PG}" != "4" ]]; then
    echo "--pg is the number of port groups. Must be either 1 or 4."
    exit 1
  fi
}

function set_default() {
  if [[ "${SCHEDULES}" == "0" ]]; then
    SCHEDULES=10000
  fi

  if [[ "${SAMPLE_SIZE}" == "0" ]]; then
    if [[ "${PG}" == "1" ]]; then
      SAMPLE_SIZE=65536
    else
      SAMPLE_SIZE=262144
    fi
  fi

  if [[ "${STREAMERS}" == "0" ]]; then
    if [[ "${PG}" == "1" ]]; then
      STREAMERS=40
    else
      STREAMERS=10
    fi
  fi

  ETH_PORTS="0 1 2 0 1 2"
  if [[ "${PG}" == "1" ]]; then
    STREAMER_IDS="0 1 2 20 21 22"
  else
    STREAMER_IDS="0 1 2 5 6 7"
  fi
}

get_inputs "$@"
validate_inputs
set_default

ELF="40-WIOS-PG${PG}-bench.elf"
WIO_FLOWS="40-WIOS-PG${PG}-wio_flows.json"
JOB_YAML="amp-stream.yaml"

OPERATOR_NS="job-operator"
CBCORE_IMAGE=$(kubectl -n ${OPERATOR_NS} get deployments.apps cluster-server -o json | \
  jq -r '.spec.template.spec.containers[0].env[] | select(.name=="WSJOB_DEFAULT_IMAGE" or .name=="WSJOB_DEFAULT_COORDINATOR_IMAGE") | .value')

MOUNT_DIR=$(kubectl -n ${OPERATOR_NS} get deployments.apps cluster-server -o json | \
  jq -r '.spec.template.spec.containers[0].env[] | select(.name=="DEFAULT_WORKDIR_HOSTPATH") | .value')
if [[ "${MOUNT_DIR}" == "" ]] || [[ "${MOUNT_DIR}" == "null" ]]; then
  MOUNT_DIR="/n1/wsjob"
fi

AMP_STREAM_WORKDIR="$MOUNT_DIR/qualification/amp_stream_test"

JOB_NAME="amp-stream"
JOB_MOUNT_DIR="${AMP_STREAM_WORKDIR}/${JOB_NAME}"

CLUSTER_CFG_JSON="/opt/cerebras/cfg/br_cfg_json"
ALL_MEMX_NODES_FILE="${SCRIPT_PATH}/all_mem_nodes.txt"
ALL_BR_NODES_FILE="${SCRIPT_PATH}/all_br_nodes.txt"
MAX_PSSH_THREADS=10

function copy_artifacts() {
  local logdir=/tmp/pssh
  local pssh_opts=(-p ${MAX_PSSH_THREADS} -x "-o StrictHostKeyChecking=no" -e $logdir -o $logdir)

  if ! pssh -h ${ALL_MEMX_NODES_FILE} "${pssh_opts[@]}" --inline "mkdir -p ${AMP_STREAM_WORKDIR}" &>/dev/null; then
    cat $logdir/* 1>&2
    echo "ERROR: failed to mkdir ${AMP_STREAM_WORKDIR} on all memoryX nodes"
    return 1
  fi
  if ! pssh -h ${ALL_MEMX_NODES_FILE} "${pssh_opts[@]}" --inline "chmod a+w ${AMP_STREAM_WORKDIR}" &>/dev/null; then
    cat $logdir/* 1>&2
    echo "ERROR: failed to chmod ${AMP_STREAM_WORKDIR} on all memoryX nodes"
    return 1
  fi
  if ! pscp.pssh -h ${ALL_MEMX_NODES_FILE} "${pssh_opts[@]}" ${ELF} ${AMP_STREAM_WORKDIR}/$(basename ${ELF}) &>/dev/null; then
    cat $logdir/* 1>&2
    echo "ERROR: failed to copy ${ELF} on all memoryX nodes"
    return 1
  fi
  if ! pscp.pssh -h ${ALL_MEMX_NODES_FILE} "${pssh_opts[@]}" \
    ${SCRIPT_PATH}/test_amp_stream_wrapper.sh ${AMP_STREAM_WORKDIR}/test_amp_stream_wrapper.sh &>/dev/null; then
    cat $logdir/* 1>&2
    echo "ERROR: failed to copy test_amp_stream_wrapper.sh on all memoryX nodes"
    return 1
  fi
  if ! pscp.pssh -h ${ALL_BR_NODES_FILE} "${pssh_opts[@]}" \
    ${SCRIPT_PATH}/start_br.sh ${AMP_STREAM_WORKDIR}/start_br.sh &>/dev/null; then
    cat $logdir/* 1>&2
    echo "ERROR: failed to copy start_br.sh on all BR nodes"
    return 1
  fi
  if ! pscp.pssh -h ${ALL_BR_NODES_FILE} "${pssh_opts[@]}" \
    ${SCRIPT_PATH}/ws_servers.py ${AMP_STREAM_WORKDIR}/ws_servers.py &>/dev/null; then
    cat $logdir/* 1>&2
    echo "ERROR: failed to copy ws_servers.py on all BR nodes"
    return 1
  fi
  if ! pscp.pssh -h ${ALL_MEMX_NODES_FILE} "${pssh_opts[@]}" \
    ${SCRIPT_PATH}/load_elf.sh ${AMP_STREAM_WORKDIR}/load_elf.sh &>/dev/null; then
    cat $logdir/* 1>&2
    echo "ERROR: failed to copy load_elf.sh on all memoryX nodes"
    return 1
  fi
  if ! pscp.pssh -h ${ALL_MEMX_NODES_FILE} "${pssh_opts[@]}" \
    ${SCRIPT_PATH}/${WIO_FLOWS} ${AMP_STREAM_WORKDIR}/wio_flows.json &>/dev/null; then
    cat $logdir/* 1>&2
    echo "ERROR: failed to copy ${WIO_FLOWS} on all memoryX nodes"
    return 1
  fi
}

function create_amp_stream_job() {
  AMP_STREAM_WORKDIR=${AMP_STREAM_WORKDIR} JOB_NAME=${JOB_NAME} STREAMERS=${STREAMERS} STREAMER_IDS=${STREAMER_IDS} \
    CBCORE_IMAGE=${CBCORE_IMAGE} NUM_WAFERS=${NUM_WAFERS} ELF=${ELF} SCHEDULES=${SCHEDULES} SAMPLE_SIZE=${SAMPLE_SIZE} \
    ETH_PORTS=${ETH_PORTS} envsubst < ${SCRIPT_PATH}/amp-stream.template.yaml > $JOB_NAME.yaml
  cat $JOB_NAME.yaml | kubectl apply -f -
}

function wait_for_programming() {
  local CHIEF_ID=$1

  # wait for configmap cluster-details-config-${JOB_NAME}
  while true; do
    if kubectl -n $OPERATOR_NS get cm cluster-details-config-${JOB_NAME} 1> /dev/null 2> /dev/null; then
      break
    fi
  done

  local CHIEF_NODE=$(kubectl -n $OPERATOR_NS get cm cluster-details-config-${JOB_NAME} -o \
    jsonpath='{.data.cluster_details_cfg_json}' | \
    jq -r ".tasks[] | select(.taskType==\"CHF\") | .taskMap[${CHIEF_ID}].addressBook.taskNodeName")
  local WSE_IP=$(kubectl -n $OPERATOR_NS get cm cluster-details-config-${JOB_NAME} -o \
    jsonpath='{.data.cluster_details_cfg_json}' | \
    jq -r ".tasks[] | select(.taskType==\"WSE\") | .taskMap[${CHIEF_ID}].addressBook.wseIp")

  if [[ "$CHIEF_NODE" == "" ]]; then
    CHIEF_NODE="localhost"
  fi

  while ! ssh ${CHIEF_NODE} ls ${JOB_MOUNT_DIR}/chief-${CHIEF_ID}/pgm_done 2>/dev/null; do
    echo "Waiting for programming to complete on system ${WSE_IP}"

    # check if the job failed
    status=$(kubectl get wsjob -n ${OPERATOR_NS} ${JOB_NAME} -ojsonpath='{.status.conditions[-1].type}')
    if [[ "${status}" == "Succeeded" ]] || [[ "${status}" == "Failed" ]]; then
      echo "Job ${JOB_NAME} completed with status ${status}"
      return 1
    fi
    sleep 5
  done
  echo "Programming completed on system ${WSE_IP}"
}

function copy_wio_groups() {
  # Copy wio_groups.json from the chief-0 node to the management node, and copy over
  # to all weight node for the test_amp_stream to find.
  echo "Copying wio_groups to all memoryX node"
  local CHIEF0_NODE=$(kubectl -n $OPERATOR_NS get cm cluster-details-config-${JOB_NAME} -o \
    jsonpath='{.data.cluster_details_cfg_json}' | \
    jq -r ".tasks[] | select(.taskType==\"CHF\") | .taskMap[0].addressBook.taskNodeName")
  scp ${CHIEF0_NODE}:${JOB_MOUNT_DIR}/chief-0/wio_groups.json ${JOB_MOUNT_DIR}
  pscp.pssh -h ${ALL_MEMX_NODES_FILE} "${pssh_opts[@]}" \
    ${JOB_MOUNT_DIR}/wio_groups.json ${AMP_STREAM_WORKDIR}/wio_groups.json
  echo "Copied wio_groups to all memoryX node"
}

function wait_for_weight() {
  local WEIGHT_ID=$1

  while true; do
    status=$(kubectl get pod -n ${OPERATOR_NS} ${JOB_NAME}-weight-${WEIGHT_ID} -ojsonpath='{.status.phase}')
    if [[ "${status}" == "Succeeded" ]]; then
      echo "Pod ${JOB_NAME}-weight-${WEIGHT_ID} succeeded"
      job_result=0
      return 0
    elif [[ "${status}" == "Failed" ]]; then
      echo "Pod ${JOB_NAME}-weight-${WEIGHT_ID} failed"
      job_result=1
      return 1
    fi

    echo "$(date +"%T"):Waiting to ${JOB_NAME}-weight-${WEIGHT_ID} to complete"

    # check if the job failed
    status=$(kubectl get wsjob -n ${OPERATOR_NS} ${JOB_NAME} -ojsonpath='{.status.conditions[-1].type}')
    if [[ "$?" != "0" ]]; then
      echo "Job ${JOB_NAME} has been deleted"
      return 1
    fi
    if [[ "${status}" == "Succeeded" ]] || [[ "${status}" == "Failed" ]]; then
      echo "Job ${JOB_NAME} completed with status ${status}"
      return 1
    fi
    sleep 3
  done
}

function cleanup_past_run() {
  kubectl delete wsjob -n ${OPERATOR_NS} amp-stream --ignore-not-found >/dev/null
  pssh -h ${ALL_MEMX_NODES_FILE} -p ${MAX_PSSH_THREADS} --inline "rm -rf ${AMP_STREAM_WORKDIR}/*"
  pssh -h ${ALL_BR_NODES_FILE} -p ${MAX_PSSH_THREADS} --inline "rm -rf ${AMP_STREAM_WORKDIR}/*"
}

function main() {
  set -e

  job_result=0
  cleanup() {
    trap - SIGINT RETURN EXIT
    if [[ "${job_result}" == "0" ]]; then
      kubectl delete wsjob -n ${OPERATOR_NS} amp-stream --ignore-not-found >/dev/null
    fi
  }
  trap cleanup SIGINT RETURN EXIT

  kubectl get nodes -l k8s.cerebras.com/node-role-memory= \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' > ${ALL_MEMX_NODES_FILE}
  kubectl get nodes -l k8s.cerebras.com/node-role-broadcastreduce= \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' > ${ALL_BR_NODES_FILE}

  cleanup_past_run
  copy_artifacts

  # create configmap for coordination
  kubectl -n $OPERATOR_NS create configmap ${JOB_NAME}-state-cfg \
    --from-literal=pgm_done=false -oyaml --dry-run=client | kubectl apply -f -

  create_amp_stream_job

  # wait for programming to complete and mark `pgm_done` to true.
  for i in $(seq 0 $((NUM_WAFERS-1))); do
    wait_for_programming ${i}
  done
  copy_wio_groups

  kubectl -n $OPERATOR_NS create configmap ${JOB_NAME}-state-cfg \
    --from-literal=pgm_done=true -oyaml --dry-run=client | kubectl apply -f -

  # We can't wait for the job to complete, since BR processes do not complete on their own.
  # We wait for the weight pod to complete instead.
  # wait for the job to complete
  if ! wait_for_weight 0; then
    job_result=1
  fi

  echo "Test completed"
}

main
