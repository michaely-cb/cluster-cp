#!/bin/bash

set -e
set -x

if [ "$LEAD_MGMT_NODE" == "$MY_NODE_NAME" ]; then
  echo "Early exit for lead management node $LEAD_MGMT_NODE"
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

if ssh $SSH_OPTION $LEAD_MGMT_NODE_IP "kubectl get node $MY_NODE_NAME --show-labels | grep -q node-role-management"; then
  is_management_node=true
fi

function setup_logrotate() {
  $SCP $LEAD_MGMT_NODE_IP:/etc/logrotate.d/cluster-mgmt /host-root/etc/logrotate.d/

  # logrotate runs daily by default but some logs grow quite quickly so we
  # run it hourly instead. NOTE: OK to have daily + hourly logrotate, it will
  # not run twice on 24h mark.
  cp /host-root/etc/cron.daily/logrotate /host-root/etc/cron.hourly || cp /host-root/etc/cron.hourly/logrotate /host-root/etc/cron.daily
}

function setup_hourly_cleanup() {
  if [ "$is_management_node" = true ]; then
    $SCP ${LEAD_MGMT_NODE_IP}:/etc/cron.hourly/etcd-cleanup /host-root/etc/cron.hourly/
    chmod 755 /host-root/etc/cron.hourly/etcd-cleanup

    # this line is only for multi-mgmt nodes, but that's the only way it is reachable anyway
    $SCP ${LEAD_MGMT_NODE_IP}:/etc/cron.hourly/clean-up-pod-volumes.sh /host-root/etc/cron.hourly/clean-up-pod-volumes.sh
    chmod 755 /host-root/etc/cron.hourly/clean-up-pod-volumes.sh

    $SCP ${LEAD_MGMT_NODE_IP}:/etc/cron.hourly/registry-cleanup /host-root/etc/cron.hourly/
    chmod 755 /host-root/etc/cron.hourly/registry-cleanup
  fi

  mkdir -p /host-root/${CLEANUP_SCRIPTS_DIR}
  $SCP ${LEAD_MGMT_NODE_IP}:${CLEANUP_SCRIPTS_DIR}/logs.zip /host-root/${CLEANUP_SCRIPTS_DIR}
  unzip -o /host-root/${CLEANUP_SCRIPTS_DIR}/logs.zip -d /host-root/${CLEANUP_SCRIPTS_DIR}

  $SCP ${LEAD_MGMT_NODE_IP}:/etc/cron.hourly/log-cleanup /host-root/etc/cron.hourly
  chmod 755 /host-root//etc/cron.hourly/log-cleanup
}

function setup_daily_cleanup() {
  if [ "$is_management_node" = true ]; then
    $SCP ${LEAD_MGMT_NODE_IP}:/etc/cron.daily/clean-up-misc /host-root/etc/cron.daily
    chmod 755 /host-root/etc/cron.daily/clean-up-misc
  fi
}

function setup_cleanup_scripts_dir() {
  mkdir -p /host-root/${CLEANUP_SCRIPTS_DIR}/configs
  $SCP ${LEAD_MGMT_NODE_IP}:${CLEANUP_SCRIPTS_DIR}/common.sh /host-root/${CLEANUP_SCRIPTS_DIR}/common.sh
  if [ "$is_management_node" = true ]; then
    $SCP ${LEAD_MGMT_NODE_IP}:${CLEANUP_SCRIPTS_DIR}/check-cleanup-history.sh /host-root/${CLEANUP_SCRIPTS_DIR}/
  fi
  $SCP ${LEAD_MGMT_NODE_IP}:${CLEANUP_SCRIPTS_DIR}/configs/cleanup-configs.json /host-root/${CLEANUP_SCRIPTS_DIR}/configs/cleanup-configs.json
}


setup_logrotate
setup_cleanup_scripts_dir
setup_hourly_cleanup
setup_daily_cleanup
