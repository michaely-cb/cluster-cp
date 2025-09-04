#!/bin/bash

#
# Install the binary-deps related files on all nodes. This script
# assumes that these files have been installed on the lead managment node
# already. The script simply copies over these files from the lead management
# node.
#

set -e
set -x

if [ "$LEAD_MGMT_NODE" == "$MY_NODE_NAME" ]; then
  echo "Skip lead management node $LEAD_MGMT_NODE"
  exit 0
fi

mkdir -p /root/.ssh
# There is a chance for known hosts conflict if we do not clean it up.
rm -f /root/.ssh/known_hosts

SSH_OPTION="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o UserKnownHostsFile=/dev/null"
SSH_KEY=/etc/sshkey-secret-volume/id_rsa
if [ -f "${SSH_KEY}" ]; then
  SSH_OPTION="${SSH_OPTION} -i ${SSH_KEY}"
fi
SCP="scp -p -r ${SSH_OPTION}"

# Unlikely, but don't fail if nerdctl isn't installed on the host
set +e
host_nerdctl_version=$(/host-root/usr/local/bin/nerdctl -v 2>/dev/null)
set -e
if [[ ! $host_nerdctl_version = *"$NEW_NERDCTL_VERSION"* ]]; then
  # Don't flood the lead node with ssh connections
  delay=$(($RANDOM % 5))
  sleep $delay
  $SCP $LEAD_MGMT_NODE_IP:/usr/local/bin/nerdctl /host-root/usr/local/bin
fi
