#!/bin/bash
if  grep -q "cbmon" ~/.bashrc ; then
  echo 'The validation executables are already added to the path' ;
else
  echo "export PATH=${PATH}:/opt/cbmon/bin" >> ~/.bashrc && source ~/.bashrc ;
  echo 'Added the validation executables to the path';
fi

kubectl get cm cluster -n job-operator -o yaml > /opt/cbmon/int.yaml
grep -vwE "clusterConfiguration.yaml:" int.yaml > cluster.yaml
rm int.yaml

TOOL_HOME=/opt/cbmon/
CONFIG_FILE=/opt/cbmon/cluster.yaml
CBSSH_FILE=/etc/cb-ssh.conf
CBMON_CONF=/etc/cbmon.conf

if ! [ -f ${CONFIG_FILE} ] ; then
  >&2 echo "Make sure the network.json file is present at ${CONFIG_FILE}"
  exit 1
fi

export PATH=${PATH}:/opt/cbmon/bin

NUM_WORKERS=`yq '.data.nodes[] | select (.role=="worker") |.name' $CONFIG_FILE | wc -l`
NUM_MEMX=`yq '.data.nodes[] | select (.role=="memory") |.name' $CONFIG_FILE | wc -l`
NUM_SWARMX=`yq '.data.nodes[] | select (.role=="broadcastreduce") |.name' $CONFIG_FILE | wc -l`

##Generating the list-all##
yq '.data.nodes[] | select (.role=="worker") |.name' $CONFIG_FILE > list-all
yq '.data.nodes[] | select (.role=="memory") |.name' $CONFIG_FILE >> list-all
yq '.data.nodes[] | select (.role=="broadcastreduce") |.name' $CONFIG_FILE >> list-all

(echo "CbmonHome=/opt/cbmon" && echo "CbmonEmail=NO") > $CBMON_CONF

(echo -en "workers:\t"; yq -r '.data.nodes[] | select (.role=="worker") | .name' $CONFIG_FILE | tr '\n' ',' ; echo "\n") > $CBSSH_FILE
(echo -en "memx:\t"; yq -r '.data.nodes[] | select (.role=="memory") | .name' $CONFIG_FILE | tr '\n' ','  ; echo "\n") >> $CBSSH_FILE
(echo -en "swarmx:\t"; yq -r '.data.nodes[] | select (.role=="broadcastreduce") | .name' $CONFIG_FILE | tr '\n' ',') >> $CBSSH_FILE
#(echo -en "workers:\t" ; jq -r '.worker_nodes | map(.name) | join(",")' $CONFIG_FILE ) > $CBSSH_FILE
#(echo -en "memx:\t" ; jq -r '.memoryx_nodes | map(.name) | join(",")' $CONFIG_FILE ) >> $CBSSH_FILE
#(echo -en "swarmx:\t" ; jq -r '.swarmx_nodes | map(.name) | join(",")' $CONFIG_FILE ) >> $CBSSH_FILE

echo "Number of Worker nodes: $NUM_WORKERS"
echo "Number of MemX nodes: $NUM_MEMX"
echo "Number of SwarmX nodes: $NUM_SWARMX"

echo "Running Cerebras Cluster Checks"


echo "########################"
echo "########################"
echo "Checking the Management Nodes"
echo "########################"
echo "########################"

readarray mgmt_nodes < <(yq -P -o=j -I=0 '.data.nodes[] | select (.role=="management") | .name' $CONFIG_FILE)
for mgmt_node in "${mgmt_nodes[@]}";
do
  MGMT_NODE=`echo "$mgmt_node" | tr -d '"'`
  mkdir -p $TOOL_HOME/var/management/$MGMT_NODE/
  cp por/management/out-standard $TOOL_HOME/var/management/$MGMT_NODE/
  echo "Running checks on $MGMT_NODE"
  $(cbmon -wt management/$MGMT_NODE -- cb-linux --sysinfo netoff $MGMT_NODE) &
done
wait

echo "########################"
echo "########################"
echo "Checking all the Workers"
echo "########################"
echo "########################"
readarray workers < <(yq -P -o=j -I=0 '.data.nodes[] | select (.role=="worker") | .name' $CONFIG_FILE)
for worker in "${workers[@]}";
do
  NODE=`echo "$worker" | tr -d '"'`
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

readarray memx_nodes < <(yq -P -o=j -I=0 '.data.nodes[] | select (.role=="memory") | .name' $CONFIG_FILE)
for memx in "${memx_nodes[@]}";
do
  NODE=`echo "$memx" | tr -d '"'`
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

readarray swarmx_nodes < <(yq -P -o=j -I=0 '.data.nodes[] | select (.role=="broadcastreduce") | .name' $CONFIG_FILE)
for swarmx in "${swarmx_nodes[@]}";
do
  NODE=`echo "$swarmx" | tr -d '"'`
  mkdir -p $TOOL_HOME/var/swarmx/$NODE/
  cp por/swarmx/out-standard $TOOL_HOME/var/swarmx/$NODE/
  echo "Running checks on $NODE"
  $(cbmon -wt swarmx/$NODE -- cb-linux --sysinfo netoff $NODE) &
done
wait

echo "Generating Report for Cerebras Cluster Configuration Checks"
cd $TOOL_HOME/var && cb-check -ar
