#!/usr/bin/env bash
# unmount_ceph.sh <mount_point>
set -euo pipefail
MOUNT_POINT="$1"
SUDO_CMD=""
if [[ $EUID -ne 0 ]] && command -v sudo &> /dev/null; then
    SUDO_CMD="sudo"
elif [[ $EUID -ne 0 ]]; then
     echo "Error: unmount requires root privileges or sudo."
    exit 1
fi

if findmnt --target "$MOUNT_POINT" > /dev/null; then
  echo "Info: Unmounting $MOUNT_POINT..."
  $SUDO_CMD umount "$MOUNT_POINT"
  # TODO(agam): Consider rmdir if the mount point was created by the script and should be cleaned.
  # $SUDO_CMD rmdir "$MOUNT_POINT" || true 
else
  echo "Info: $MOUNT_POINT was not mounted."
fi