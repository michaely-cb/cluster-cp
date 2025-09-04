#!/bin/bash

set -eo pipefail

declare -A roleMapping
roleMapping[coordinator]=crd
roleMapping[worker]=wrk
roleMapping[weight]=wgt
roleMapping[activation]=act
roleMapping[command]=cmd
roleMapping[broadcastreduce]=br
roleMapping[chief]=chf
roleMapping[kvstorageserver]=kvss
roleMapping[swdriver]=swdriver
ROLE=${roleMapping[${REPLICA_TYPE}]}

# Kubelet does not allow "workingDir" to be specified for restricted directories on NFS.
# To work around that, operator sets the environment variable so we can change directory
# after the pod launches.
# For example, if we have
# - /cb/tests/foo/wsjob-123/coordinator-0 as the working dir
# - /cb/tests/foo has permission 700
# - /cb/tests is used as nfs volume
# then kubelet will run into "RunContainerError" as it tries to "mkdir" for the working dir.
if [ -n "$REPLICA_WORKDIR" ]; then
  cd $REPLICA_WORKDIR
fi

# Important Note
#
# 'WSJOB_SCRIPT_ROOT' is a pod-level environment variable set during deploying the cluster server.
# By default, 'WSJOB_SCRIPT_ROOT' should be set to '/opt/cerebras/cluster_mgmt', which is a mounted
# path that refers to a ConfigMap (wsjob-entrypoint-scripts).
if [ "$SYSTEM_TYPE" == "KAPI" ] && [ "${WS_JOB_MODE}" == "COMPILE" ]; then
  # An example of this value looks like /cb/tests/cluster-mgmt/compile_root/opt/cerebras/cached_compile
  # In the event of a KAPI run, users can stage the fabric JSON under this directory ahead of time.
  # The coordinator will skip pulling fabric from system when the fabric JSON is already in its workdir.
  cp ${COMPILE_ROOT_DIR}/${NAMESPACE}/${RELATIVE_COMPILE_DIR}/fabric.json .
elif [ "$SYSTEM_TYPE" == "SYSEMU" ]; then
  export CEREBRAS_WKR_CONNECT_RETRY_DELAY="100"
  export CEREBRAS_WKR_CONNECT_TIMEOUT="60000"
  export CEREBRAS_WKR_CONNECT_RETRIES="12"
fi

LOG_NAME="dbg_${ROLE}_${REPLICA_ID}"

if [ "${IS_USER_SIDECAR}" == "TRUE" ]; then
  LOG_NAME="dbg_usc_${REPLICA_ID}"

  # Ref: https://github.com/Cerebras/monolith/blob/master/src/cluster_mgmt/src/cluster/server/charts/sitecustomize.py
  export PYTHONPATH=${PYTHONPATH}:${WSJOB_SCRIPT_ROOT}
fi

echo "Workdir location of this run: ${PWD}" >> ${LOG_NAME}.out

if [ -n "${CBCORE_OVERRIDE_SCRIPT}" ]; then
  echo "Debug script is defined: ${CBCORE_OVERRIDE_SCRIPT}" >> ${LOG_NAME}.out
  if [ -f "${CBCORE_OVERRIDE_SCRIPT}" ]; then
    echo "Applying ${CBCORE_OVERRIDE_SCRIPT}" >> ${LOG_NAME}.out
    source ${CBCORE_OVERRIDE_SCRIPT}
    echo "Applied ${CBCORE_OVERRIDE_SCRIPT}" >> ${LOG_NAME}.out
  fi
fi

# use exec to replace the process instead of creating a new subprocess
# this allows for easier signals forwarding
exec python3 ${WSJOB_SCRIPT_ROOT}/${WSJOB_RUN_SCRIPT_NAME} ${ROLE} > >(tee -a "${LOG_NAME}.out") 2> >(tee -a "${LOG_NAME}.err" >&2)
