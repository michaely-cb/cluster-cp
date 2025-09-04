#!/usr/bin/env bash
#
# Script to create net-attach-def config files for the node.
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

staging_root="/tmp/cluster/multus"
dest_dir="/host-root/$DEST_DIR"
legacy_dest_dir="/host-root/etc/cni/net.d"

rm -fr $staging_root
mkdir -p /tmp/cluster
$SCP ${LEAD_MGMT_NODE_IP}:${staging_root} $staging_root

if [ -n "$INCREMENTAL_DIR" ]; then
    $SCP ${LEAD_MGMT_NODE_IP}:${INCREMENTAL_DIR} /
    bash ${staging_root}/node/node-net-attach-def.sh $dest_dir $INCREMENTAL_DIR
    return 0
fi

bash ${staging_root}/node/node-net-attach-def.sh $dest_dir
# TODO: Remove this in rel-2.5 - see net-attach-def.sh
rm -f ${legacy_dest_dir}/*-data-net*
