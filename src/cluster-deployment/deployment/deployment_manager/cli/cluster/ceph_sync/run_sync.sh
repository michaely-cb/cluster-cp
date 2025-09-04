#!/bin/bash
set -euo pipefail

# CephFS Sync Orchestration Script
# This script orchestrates the mounting, syncing, and unmounting of CephFS volumes
# Used by both single-pod and single-node execution modes

echo "Starting CephFS sync orchestration"

# Environment variables (set by caller)
VOLUMES_TO_SYNC="${VOLUMES_TO_SYNC:-}"
SRC_MNT="${SRC_MNT:-/mnt/src_ceph}"
DST_MNT="${DST_MNT:-/mnt/dst_ceph}"
CLEANUP_MOUNTS="${CLEANUP_MOUNTS:-true}"
SRC_CEPH_CONF="${SRC_CEPH_CONF:-/etc/ceph/src/ceph.conf}"
SRC_KEYRING="${SRC_KEYRING:-/etc/ceph/src/keyring}"
DST_CEPH_CONF="${DST_CEPH_CONF:-/etc/ceph/dst/ceph.conf}"
DST_KEYRING="${DST_KEYRING:-/etc/ceph/dst/keyring}"

# Script directory (where the shell scripts are located)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Configuration:"
echo "  - Source mount: $SRC_MNT"
echo "  - Destination mount: $DST_MNT"
echo "  - Cleanup mounts: $CLEANUP_MOUNTS"
echo "  - Volumes to sync: $VOLUMES_TO_SYNC"
echo "  - Script directory: $SCRIPT_DIR"

# Parse volumes JSON
if [ -z "$VOLUMES_TO_SYNC" ]; then
    echo "ERROR: VOLUMES_TO_SYNC environment variable is required"
    exit 1
fi

echo "Parsing volumes to sync..."
volumes=$(echo "$VOLUMES_TO_SYNC" | jq -r '.[] | @base64')

# Mount source CephFS
echo "Mounting source CephFS..."
"$SCRIPT_DIR/mount_ceph.sh" "$SRC_CEPH_CONF" "$SRC_KEYRING" "$SRC_MNT"

# Mount destination CephFS
echo "Mounting destination CephFS..."
"$SCRIPT_DIR/mount_ceph.sh" "$DST_CEPH_CONF" "$DST_KEYRING" "$DST_MNT"

# Sync each volume
echo "Starting volume synchronization..."
for volume in $volumes; do
    decoded=$(echo "$volume" | base64 -d)
    group=$(echo "$decoded" | jq -r '.group')
    subvol=$(echo "$decoded" | jq -r '.subvol')
    
    echo "Syncing group=$group, subvolume=$subvol"
    "$SCRIPT_DIR/sync_volume.sh" "$group" "$subvol" "$SRC_MNT" "$DST_MNT"
done

# Cleanup mounts if requested
if [ "$CLEANUP_MOUNTS" = "true" ]; then
    echo "Cleaning up mounts..."
    "$SCRIPT_DIR/unmount_ceph.sh" "$SRC_MNT" || true
    "$SCRIPT_DIR/unmount_ceph.sh" "$DST_MNT" || true
fi

echo "CephFS sync orchestration completed successfully"