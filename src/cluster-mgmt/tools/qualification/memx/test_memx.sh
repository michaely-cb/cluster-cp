#!/usr/bin/env bash

#
# Entry point script to initiate memoryX node qualification tests.
#
# This script initiates the memoryX qualification test on all memoryX nodes
# in parallel.
#
function usage() {
  echo "Usage: $0 [--keep-artifacts]"
  echo
  echo "Run memoryX node qualification tests on all memoryX nodes in the cluster."
  echo "If '--keep-artifacts' is specified, the test data will be kept."
}

SCRIPT_PATH=$(dirname "$0")
cd ${SCRIPT_PATH}

MAX_PSSH_THREADS=10
OPERATOR_NS="job-operator"
MEMX_WORKDIR="/n1/wsjob/qualification/memx"
TEST_ARTIFACT_FILE="$SCRIPT_PATH/test_artifacts.tar.gz"
ALL_MEMX_NODES_FILE="$SCRIPT_PATH/all_memx_nodes.txt"
TEST_SCRIPT_FILE="test_one_memx.sh"
FAILED_NODES=""
KEEP_ARTIFACTS="false"

# copy over all artifacts needed to run the qualification test on each memoryX node.
function copy_artifacts() {
  kubectl get nodes -l k8s.cerebras.com/node-role-memory= \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' > ${ALL_MEMX_NODES_FILE}

  pssh -h ${ALL_MEMX_NODES_FILE} -p ${MAX_PSSH_THREADS} --inline "mkdir -p ${MEMX_WORKDIR}"
  pssh -h ${ALL_MEMX_NODES_FILE} -p ${MAX_PSSH_THREADS} --inline "chmod a+w ${MEMX_WORKDIR}"
  pscp.pssh -h ${ALL_MEMX_NODES_FILE} -p ${MAX_PSSH_THREADS} ${TEST_ARTIFACT_FILE} $MEMX_WORKDIR/${TEST_ARTIFACT_FILE}
  pssh -h ${ALL_MEMX_NODES_FILE} -p ${MAX_PSSH_THREADS} --inline "tar xfz ${MEMX_WORKDIR}/${TEST_ARTIFACT_FILE} -C ${MEMX_WORKDIR}"
}

# Create a memx-test pod on a given memoryX node.
function create_memx_test_pod() {
  local MEMX_NODE=$1
  local POD_NAME="memx-test-${MEMX_NODE}"
  local CBCORE_IMAGE=$(kubectl -n ${OPERATOR_NS} get deployments.apps cluster-server -o json | \
    jq -r '.spec.template.spec.containers[0].env[] | select(.name=="WSJOB_DEFAULT_IMAGE" or .name=="WSJOB_DEFAULT_COORDINATOR_IMAGE") | .value')
  echo "Running with cbcore ${CBCORE_IMAGE}"

  local REGCRED=""
  if [[ ${CBCORE_IMAGE} == 171496337684.dkr.ecr.us-west-2.amazonaws.com* ]]; then
    REGCRED=$(cat << EOF
  imagePullSecrets:
    - name: regcred
EOF
)
  fi

  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_NAME}
  namespace: job-operator
  labels:
    k8s-app: memx-test
spec:
  containers:
  - command:
    - /bin/bash
    - ${TEST_SCRIPT_FILE}
    image: ${CBCORE_IMAGE}
    imagePullPolicy: IfNotPresent
    name: ws
    env:
    - name: MY_POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    volumeMounts:
    - mountPath: ${MEMX_WORKDIR}
      name: workdir-volume
    workingDir: ${MEMX_WORKDIR}
${REGCRED}
  nodeName: ${MEMX_NODE}
  nodeSelector:
    k8s.cerebras.com/node-role-memory: ""
  restartPolicy: Never
  volumes:
  - hostPath:
      path: ${MEMX_WORKDIR}
      type: DirectoryOrCreate
    name: workdir-volume
EOF
}

function copy_results() {
  mkdir -p $MEMX_WORKDIR
  ALL_MEMX_NODES=$(cat ${ALL_MEMX_NODES_FILE})
  for node in ${ALL_MEMX_NODES}; do
    if rsync ${node}:${MEMX_WORKDIR}/memx-test*.out ${MEMX_WORKDIR} 2> /dev/null; then
      echo "Copied ${MEMX_WORKDIR}/memx-test*.out from node ${node}"
    fi
    if rsync ${node}:${MEMX_WORKDIR}/dbg_wgt_*.err ${MEMX_WORKDIR} 2> /dev/null; then
      echo "Copied ${MEMX_WORKDIR}/dbg_wgt_*.err from node ${node}"
    fi
  done
}

function run_tests() {
  local ALL_MEMX_NODES=$(cat ${ALL_MEMX_NODES_FILE})
  for node in ${ALL_MEMX_NODES}; do
    create_memx_test_pod ${node}
  done

  echo "$(date +"%T"): Wait for the tests to complete"
  for node in ${ALL_MEMX_NODES}; do
    while true; do
      status=$(kubectl get pod -n ${OPERATOR_NS} memx-test-${node} -ojsonpath='{.status.phase}')
      if [[ "${status}" == "Succeeded" ]]; then
        job_result=0
        break
      elif [[ "${status}" == "Failed" ]]; then
        job_result=1
        break
      fi

      sleep 3
    done

    if [[ $job_result -eq 1 ]]; then
        echo "Test on ${node} failed!"
        FAILED_NODES="${FAILED_NODES} ${node}"
    else
      echo "Test on ${node} succeeded"
    fi
  done
}

function main() {
  set -e

  cleanup() {
    trap - SIGINT RETURN EXIT
    if [[ "$FAILED_NODES" == "" ]]; then
      kubectl delete pod -n ${OPERATOR_NS} -lk8s-app=memx-test &>/dev/null
      if [[ "${KEEP_ARTIFACTS}" == "false" ]]; then
        pssh -h ${ALL_MEMX_NODES_FILE} -p ${MAX_PSSH_THREADS} --inline "rm -rf ${MEMX_WORKDIR}/*"
      fi
    fi
  }
  trap cleanup SIGINT RETURN EXIT

  # clean up leftover pods from previous test.
  kubectl delete pod -n ${OPERATOR_NS} -lk8s-app=memx-test &>/dev/null

  echo "$(date +"%T"): Copy test artifacts"
  copy_artifacts

  echo "$(date +"%T"): Start the test on each memoryX node"
  run_tests

  echo "$(date +"%T"): Copy test results to ${MEMX_WORKDIR}"
  copy_results

  if [[ "${FAILED_NODES}" == "" ]]; then
    echo "$(date +"%T"): Tests completed: all tests succeeded"
  else
    echo "$(date +"%T"): Tests failed on some nodes:${FAILED_NODES}"
  fi
}

while :; do
  case $1 in
    -h|-\?|--help)
      usage
      exit
      ;;
    --keep-artifacts)
      KEEP_ARTIFACTS="true"
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

main
