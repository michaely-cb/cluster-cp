#!/bin/bash
TOOL_HOME=/opt/cbmon/
TIERING_FILE=/opt/cbmon/network.json
CBSSH_FILE=/etc/cb-ssh.conf
CBMON_CONF=/etc/cbmon.conf

if ! [ -f ${TIERING_FILE} ] ; then
  >&2 echo "Make sure the network.json file is present at ${TIERING_FILE}"
  exit 1
fi

export PATH=${PATH}:/opt/cbmon/bin

NUM_WORKERS=`jq -r '.worker_nodes[].name' $TIERING_FILE | wc -l`
NUM_MEMX=`jq -r '.memoryx_nodes[].name' $TIERING_FILE | wc -l`
NUM_SWARMX=`jq -r '.swarmx_nodes[].name' $TIERING_FILE | wc -l`

echo "Make sure the network.json file is present in the PATH of ${TIERING_FILE}"

##Generating the list-all##
#jq -r '.worker_nodes[].name' $TIERING_FILE > list-all
#jq -r '.memoryx_nodes[].name' $TIERING_FILE >> list-all
#jq -r '.swarmx_nodes[].name' $TIERING_FILE >> list-all

(echo "CbmonHome=/opt/cbmon" && echo "CbmonEmail=NO") > $CBMON_CONF

(echo -en "workers:\t" ; jq -r '.worker_nodes | map(.name) | join(",")' $TIERING_FILE ) > $CBSSH_FILE
(echo -en "memx:\t" ; jq -r '.memoryx_nodes | map(.name) | join(",")' $TIERING_FILE ) >> $CBSSH_FILE
(echo -en "swarmx:\t" ; jq -r '.swarmx_nodes | map(.name) | join(",")' $TIERING_FILE ) >> $CBSSH_FILE

echo "Number of Worker nodes: $NUM_WORKERS"
echo "Number of MemX nodes: $NUM_MEMX"
echo "Number of SwarmX nodes: $NUM_SWARMX"

echo "Running Cerebras Cluster Checks"

echo "########################"
echo "########################"
echo "Checking all the Workers"
echo "########################"
echo "########################"
for ((i=0; i<$NUM_WORKERS; i++))
do
  NODE=$(jq -r '.worker_nodes['$i'].name' $TIERING_FILE)
  mkdir -p $TOOL_HOME/var/workers/$NODE/
  cp por/workers/out-standard $TOOL_HOME/var/workers/$NODE/
  echo "Running checks on $NODE"
  $(cbmon -wt workers/$NODE -- cb-linux --sysinfo netoff $NODE) &
done
wait

echo "########################"
echo "########################"
echo "Checking all the MemoryX"
echo "########################"
echo "########################"
for ((i=0; i<$NUM_MEMX; i++))
do
  NODE=$(jq -r '.memoryx_nodes['$i'].name' $TIERING_FILE)
  mkdir -p $TOOL_HOME/var/memx/$NODE/
  cp por/memx/out-standard $TOOL_HOME/var/memx/$NODE/
  echo "Running checks on $NODE"
  $(cbmon -wt memx/$NODE -- cb-linux --sysinfo netoff $NODE) &
done
wait

echo "########################"
echo "########################"
echo "Checking all the SwarmX"
echo "########################"
echo "########################"
for ((i=0; i<$NUM_SWARMX; i++))
do
  NODE=$(jq -r '.swarmx_nodes['$i'].name' $TIERING_FILE)
  mkdir -p $TOOL_HOME/var/swarmx/$NODE/
  cp por/swarmx/out-standard $TOOL_HOME/var/swarmx/$NODE/
  echo "Running checks on $NODE"
  $(cbmon -wt swarmx/$NODE -- cb-linux --sysinfo netoff $NODE) &
done
wait

echo "Generating Report for Cerebras Cluster Configuration Checks"
cd $TOOL_HOME/var && cb-check -ar
