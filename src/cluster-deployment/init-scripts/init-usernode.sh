#!/bin/bash
#
# Configure the current server as a usernode.
# This assumes the current server is running the standard deploying OS image,
# and all usernode set up related files are under the /opt/cerebras/usernode/
# directory.
#
# Usage: init-usernode.sh --usernode_hostname <usernode_hostname> --mgmt_node_ip <management_node_ip> [--no_confirm]

set -e

USAGE="Usage: init-usernode.sh --usernode_hostname <usernode_hostname> --mgmt_node_ip <management_node_ip>"

# Extract usernode hostname and management node ip
USERNODE_NAME=
MGMT_NODE_IP=
no_confirm=0

while [ ! $# -eq 0 ]; do
  case "$1" in
    -u|--usernode_hostname)
      USERNODE_NAME=$2
      shift 2
      ;;

    -m|--mgmt_node_ip)
      # Usernode may not have DNS entry for management node hostname, hence
      # using IP.
      MGMT_NODE_IP=$2
      shift 2
      ;;

    --no_confirm)
      # Reboot without prompt
      no_confirm=1
      shift 1
      ;;

    -h|--help)
      echo $USAGE
      exit 0
      ;;

    -*)
      echo "ERROR: $1 is not a valid flag"
      echo $USAGE
      exit 1
      ;;

    *)
      echo "ERROR: $1 is not a valid argument"
      echo $USAGE
      exit 1
      ;;
  esac
done

if [[ -z "$USERNODE_NAME" ]]; then
   echo "ERROR: <usernode_hostname> is not defined"
   echo $USAGE
   exit 1
fi

if [[ -z "$MGMT_NODE_IP" ]]; then
   echo "ERROR: <management_node_ip> is not defined"
   echo $USAGE
   exit 1
fi

# Launch the real config script
cd /opt/cerebras/usernode
ansible-playbook init-usernode.yml --extra-vars "{usernode_hostname: $USERNODE_NAME, management_node: $MGMT_NODE_IP}"

# Reboot due to potential infiniband to ethernet change
echo "Reboot requested due to potential infiniband to ethernet change."
if [ $no_confirm -eq 0 ]; then
  read -p "Press <enter> to reboot."
fi

/sbin/reboot
