#!/bin/bash
if  grep -q "cbmon" ~/.bashrc ; then
  echo 'The validation executables are already added to the path' ;
else
  echo "export PATH=${PATH}:/opt/cbmon/bin" >> ~/.bashrc && source ~/.bashrc ;
  echo 'Added the validation executables to the path';
fi
#export PATH=${PATH}:/opt/cbmon/bin
TOOL_HOME=/opt/cbmon/
CONFIG_FILE=/opt/cerebras/cluster/master-config.yaml
CBSSH_FILE=/etc/cb-ssh.conf
CBMON_CONF=/etc/cbmon.conf

if ! [ -f ${CONFIG_FILE} ] ; then
  >&2 echo "Make sure the master-config.yaml file is present at ${CONFIG_FILE}"
  exit 1
fi

export PATH=${PATH}:/opt/cbmon/bin

NUM_MGMT=`yq -r '.servers[] | select (.role=="MG") | .name' $CONFIG_FILE | wc -l`
NUM_WORKERS=`yq -r '.servers[] | select (.role=="WK") | .name' $CONFIG_FILE | wc -l`
NUM_MEMX=`yq -r '.servers[] | select (.role=="MX") | .name' $CONFIG_FILE | wc -l`
NUM_SWARMX=`yq -r '.servers[] | select (.role=="SX") | .name' $CONFIG_FILE | wc -l`

##Generating the list-all##
yq -r '.servers[] | select (.role=="MG") | .name' $CONFIG_FILE > list-all
yq -r '.servers[] | select (.role=="WK") | .name' $CONFIG_FILE >> list-all
yq -r '.servers[] | select (.role=="MX") | .name' $CONFIG_FILE >> list-all
yq -r '.servers[] | select (.role=="SX") | .name' $CONFIG_FILE >> list-all

rm -rf ~/.ssh/known_hosts
for ip in `cat list-all`; do
    ssh -oStrictHostKeyChecking=no $ip hostname
done

(echo "CbmonHome=/opt/cbmon" && echo "CbmonEmail=NO") > $CBMON_CONF

(echo -en "workers:\t"; yq -r '.servers[] | select (.role=="WK") | .name' $CONFIG_FILE | tr '\n' ',' ; printf "\n") > $CBSSH_FILE
(echo -en "memx:\t"; yq -r '.servers[] | select (.role=="MX") | .name' $CONFIG_FILE | tr '\n' ','  ; printf "\n") >> $CBSSH_FILE
(echo -en "swarmx:\t"; yq -r '.servers[] | select (.role=="SX") | .name' $CONFIG_FILE | tr '\n' ',' ; printf "\n") >> $CBSSH_FILE

echo "Number of Management nodes: $NUM_MGMT"
echo "Number of Worker nodes: $NUM_WORKERS"
echo "Number of MemoryX nodes: $NUM_MEMX"
echo "Number of SwarmX nodes: $NUM_SWARMX"

echo "Running Cerebras Cluster Checks"


echo "########################"
echo "########################"
echo "Checking the Management Nodes"
echo "########################"
echo "########################"
readarray mgmt_nodes < <(yq -P -o=j -I=0 '.servers[] | select (.role=="MG") | .name' $CONFIG_FILE)
for mgmt_node in "${mgmt_nodes[@]}";
do
  MGMT_NODE=`echo "$mgmt_node" | tr -d '"'`
  mkdir -p $TOOL_HOME/var/management/$MGMT_NODE/
  cp $TOOL_HOME/por/management/out-standard $TOOL_HOME/var/management/$MGMT_NODE/
  echo "Running checks on $MGMT_NODE"
  $(cbmon -wt management/$MGMT_NODE -- cb-linux --sysinfo netoff $MGMT_NODE) &
done
wait

echo "########################"
echo "########################"
echo "Checking all the Workers"
echo "########################"
echo "########################"
readarray workers < <(yq -P -o=j -I=0 '.servers[] | select (.role=="WK") | .name' $CONFIG_FILE)
for worker in "${workers[@]}";
do
  NODE=`echo "$worker" | tr -d '"'`
  mkdir -p $TOOL_HOME/var/workers/$NODE/
  cp $TOOL_HOME/por/workers/out-standard $TOOL_HOME/var/workers/$NODE/
  echo "Running checks on $NODE"
  $(cbmon -wt workers/$NODE -- cb-linux --sysinfo netoff $NODE) &
done
wait

echo "########################"
echo "########################"
echo "Checking all the MemoryX"
echo "########################"
echo "########################"
readarray memx_nodes < <(yq -P -o=j -I=0 '.servers[] | select (.role=="MX") | .name' $CONFIG_FILE)
for memx in "${memx_nodes[@]}";
do
  NODE=`echo "$memx" | tr -d '"'`
  mkdir -p $TOOL_HOME/var/memx/$NODE/
  cp $TOOL_HOME/por/memx/out-standard $TOOL_HOME/var/memx/$NODE/
  echo "Running checks on $NODE"
  $(cbmon -wt memx/$NODE -- cb-linux --sysinfo netoff $NODE) &
done
wait

echo "########################"
echo "########################"
echo "Checking all the SwarmX"
echo "########################"
echo "########################"
readarray swarmx_nodes < <(yq -P -o=j -I=0 '.servers[] | select (.role=="SX") | .name' $CONFIG_FILE)
for swarmx in "${swarmx_nodes[@]}";
do
  NODE=`echo "$swarmx" | tr -d '"'`
  mkdir -p $TOOL_HOME/var/swarmx/$NODE/
  cp $TOOL_HOME/por/swarmx/out-standard $TOOL_HOME/var/swarmx/$NODE/
  echo "Running checks on $NODE"
  $(cbmon -wt swarmx/$NODE -- cb-linux --sysinfo netoff $NODE) &
done
wait

sleep 5

echo "Generating Report for Cerebras Cluster Configuration Checks"
cd $TOOL_HOME/var && cb-check -ar
#cd $TOOL_HOME/var && cb-check -a
