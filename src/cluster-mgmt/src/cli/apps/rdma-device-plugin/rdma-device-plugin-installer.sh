#!/bin/bash

set -e
set -x

mkdir -p /root/.ssh
# There is a chance for known hosts conflict if we do not clean it up.
rm -f /root/.ssh/known_hosts

SSH_OPTION="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o UserKnownHostsFile=/dev/null"
SSH_KEY=/etc/sshkey-secret-volume/id_rsa
if [ -f "${SSH_KEY}" ]; then
  SSH_OPTION="${SSH_OPTION} -i ${SSH_KEY}"
fi
SCP="scp -p -r ${SSH_OPTION}"

# make sure the ssh public key is in the node's authoriazed_keys file.
if ! ssh $SSH_OPTION $MY_NODE_IP "date"; then
  cat /etc/sshkey-secret-volume/id_rsa.pub >> /host-root/root/.ssh/authorized_keys
fi

# apply containerd limit change
if [ ! "$LEAD_MGMT_NODE" == "$MY_NODE_NAME" ]; then
  mkdir -p /host-root/etc/systemd/system/containerd.service.d
  $SCP $LEAD_MGMT_NODE_IP:/etc/systemd/system/containerd.service.d/roce-limitmemlock-infinity.conf /host-root/etc/systemd/system/containerd.service.d/
fi
ssh $SSH_OPTION $MY_NODE_IP systemctl daemon-reload
ssh $SSH_OPTION $MY_NODE_IP systemctl restart containerd

# verify new value
ssh $SSH_OPTION $MY_NODE_IP "systemctl show containerd | grep -q LimitMEMLOCK=infinity || { hostname ; exit 1 ; }"
