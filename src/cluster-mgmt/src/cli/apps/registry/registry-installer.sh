#!/bin/bash

set -e
set -x

REGISTRY_URL=${REGISTRY_URL:-"registry.local"}

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

rm -rf /host-root/etc/containerd/certs.d
rm -fr /host-root/${CERTS_DIR}
$SCP $LEAD_MGMT_NODE_IP:${CERTS_DIR} /host-root/${CERTS_DIR}
$SCP $LEAD_MGMT_NODE_IP:/etc/containerd/certs.d /host-root/etc/containerd
$SCP $LEAD_MGMT_NODE_IP:/etc/containerd/config.toml /host-root/etc/containerd

commas="${REGISTRY_IPS//[^,]}"
if [ ${#commas} -gt 1 ]; then
  readarray -t shuffled_ips < <(echo "$REGISTRY_IPS" | tr , "\n" | shuf)
  for ((i=0; i<${#shuffled_ips[@]}; i++)); do
    sed -i s/ENDPOINT-REPLACE-${i}/${shuffled_ips[${i}]}/g \
        /host-root/etc/containerd/certs.d/${REGISTRY_URL}/hosts.toml
  done
fi

# make sure the ssh public key is in the node's authoriazed_keys file.
if ! ssh $SSH_OPTION $MY_NODE_IP "date"; then
  cat /etc/sshkey-secret-volume/id_rsa.pub >> /host-root/root/.ssh/authorized_keys
fi

# When containerd is restarted, kubelet could lose the state of the init container and mistakenly
# thinks the init container is still running while it has already completed. Restarting kubelet
# after containerd restart will fix this issue.
ssh $SSH_OPTION $MY_NODE_IP "
  systemctl restart containerd \
  && timeout 60 bash -c 'until systemctl is-active --quiet containerd; do sleep 1; done' \
  && systemctl restart kubelet \
  || exit 1
"
echo "$MY_NODE_IP successfully restarted containerd and kubelet"
