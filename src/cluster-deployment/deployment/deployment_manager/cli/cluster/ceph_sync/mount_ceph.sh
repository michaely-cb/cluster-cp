#!/usr/bin/env bash
#
# mount_ceph.sh  ‒ mount a CephFS filesystem via the Linux kernel driver
#
# Usage:
#   ./mount_ceph.sh <ceph_conf_path> <keyring_path> <mount_point>
#
# Example:
#   ./mount_ceph.sh /etc/ceph/src/ceph.conf /etc/ceph/src/keyring /mnt/src

set -euo pipefail

CEPH_CONF="$1"
KEYRING_FILE="$2"
MOUNT_POINT="$3"

# Extract mon_host line (comma-separated) from ceph.conf
MONS=$(grep mon_host "$CEPH_CONF" | cut -d= -f2 | xargs | tr ' ' ',')
# Extract the raw secret key from the keyring (assumes [client.admin] key format)
KEY=$(awk '/key =/ {print $3; exit}' "$KEYRING_FILE")
FS_NAME="ceph-filesystem"

echo "INFO: Preparing to mount CephFS at $MOUNT_POINT"
mkdir -p "$MOUNT_POINT"

# If already mounted, unmount first
if mountpoint -q "$MOUNT_POINT"; then
  echo "WARNING: $MOUNT_POINT already mounted; unmounting..."
  umount "$MOUNT_POINT"
  echo "SUCCESS: Unmounted $MOUNT_POINT"
fi

echo "INFO: Mounting CephFS via kernel driver..."
echo "Command: mount -t ceph ${MONS}:/ $MOUNT_POINT -o name=admin,secret=$KEY,mds_namespace=$FS_NAME"
mount -t ceph "${MONS}:/" "$MOUNT_POINT" \
  -o name=admin,secret="$KEY",mds_namespace="$FS_NAME" \
  || { echo "ERROR: mount failed" >&2; exit 1; }

echo "SUCCESS: Mounted CephFS $CEPH_CONF → $MOUNT_POINT"
