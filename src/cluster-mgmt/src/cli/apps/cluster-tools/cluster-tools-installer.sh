#!/bin/bash

#
# Install the tools related files in the management node. This script
# assumes that these files have been installed on the lead managment node
# already. The script simply copies over these files from the lead management
# node.
#

set -e
set -x

# Atomically copy a given script to dest_path.
atomic_copy_script() {
  local script=$1
  local staging_dir=$2
  local dest_path=$3

  if [ -d "$script" ]; then
    echo "atomic_copy_script doesn't work for directories."
    return 1
  fi

  cp "$script" "$staging_dir"
  mv -f "$staging_dir/$(basename $script)" "$dest_path"
}

# Atomically copy a given directory to dest_path.
atomic_copy_dir() {
  local src_dir=$1
  local staging_dir=$2
  local dest_dir=$3

  if [ ! -d "$src_dir" ]; then
    echo "$src_dir is not a directory."
    return 1
  fi

  rm -rf "$staging_dir/$(basename $src_dir)"
  cp -r "$src_dir" "$staging_dir"
  rm -rf "$dest_dir.bkup"
  if [ -d "$dest_dir" ]; then
    mv -f "$dest_dir" "$dest_dir.bkup"
  fi
  mv -f "$staging_dir/$(basename $src_dir)" "$dest_dir"
}

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

mkdir -p /host-root/opt/cerebras/tools
tmpdir=$(mktemp -d)

tools_dir="$tmpdir/tools"
staging_dir="$tmpdir/staging"
mkdir -p "$tools_dir" "$staging_dir"

if [ "$SYSTEM_NAMESPACE" == "$DEPLOY_NAMESPACE" ]; then
  # Copy over the tools scripts
  $SCP $LEAD_MGMT_NODE_IP:/opt/cerebras/tools/* "$tools_dir"
  atomic_copy_dir "$tools_dir" "$staging_dir" "/host-root/opt/cerebras/tools"

  if [ -d "/host-root/opt/cerebras/tools/csadm" ]; then
    atomic_copy_dir "/host-root/opt/cerebras/tools/csadm" "$staging_dir" "/host-root/usr/local/bin/csadm"
  fi
  if [ -f "/host-root/opt/cerebras/tools/csadm.sh" ]; then
    atomic_copy_script "/host-root/opt/cerebras/tools/csadm.sh" "$staging_dir" "/host-root/usr/local/bin/csadm.sh"
  fi

  # Copy over the get-cerebras-token binary
  $SCP $LEAD_MGMT_NODE_IP:/usr/local/bin/get-cerebras-token "$tmpdir/get-cerebras-token"
  atomic_copy_script "$tmpdir/get-cerebras-token" "$staging_dir" "/host-root/usr/local/bin/get-cerebras-token"
  chmod 4751 /host-root/usr/local/bin/get-cerebras-token

  if [ "$INITIAL_DEPLOY" != "true" ]; then
    # Copy over the config, config_v2
    $SCP $LEAD_MGMT_NODE_IP:/opt/cerebras/config_v2 "$tmpdir/config_v2"
    atomic_copy_script "$tmpdir/config_v2" "$staging_dir" "/host-root/opt/cerebras/config_v2"

    # Copy over the user_auth file
    mkdir -p /host-root/root/.cerebras
    secret_dir="$tmpdir/.cerebras"
    mkdir -p "$secret_dir"
    $SCP $LEAD_MGMT_NODE_IP:/root/.cerebras/* "$secret_dir"
    atomic_copy_dir "$secret_dir" "$staging_dir" "/host-root/root/.cerebras"
  fi
else
  # Copy over the csctl
  $SCP $LEAD_MGMT_NODE_IP:/opt/cerebras/tools/csctl* "$tools_dir"
  csctl_files=$(ls $tools_dir/csctl*)
  for csctl_file in $csctl_files; do
    atomic_copy_script "$csctl_file" "$staging_dir" "/host-root/opt/cerebras/tools/$(basename $csctl_file)"
  done
fi

# Copy over csctl binaries
csctl_files=$(ls /host-root/opt/cerebras/tools/csctl*)
for csctl_file in $csctl_files; do
  atomic_copy_script "$csctl_file" "$staging_dir" "/host-root/usr/local/bin/$(basename $csctl_file)"
done
