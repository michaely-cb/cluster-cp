#!/usr/bin/env bash

# Launch a wsjob with chief, weight and br services, each with some custom
# commands.

set -e

CLUSTER_CFG_JSON="/opt/cerebras/cfg/br_cfg_json"

# It appears that workingDir is handled different in 1.6 and master by the cluster-server/job-operator.
# We check the current workingDir. If it is not role specific, we create the role-specific directory,
# and cd into it before running the rest of the commands.
if [[ ${PWD} == *broadcastreduce-${REPLICA_ID} ]]; then
  echo "Current workingDir ${PWD}"
else
  mkdir -p ${PWD}/broadcastreduce-${REPLICA_ID}
  cd ${PWD}/broadcastreduce-${REPLICA_ID}
  echo "Change workingDir to ${PWD}"
fi

python ../ws_servers.py --start-br-node --cluster_cfg_filename=${CLUSTER_CFG_JSON} --br-id=${REPLICA_ID}
