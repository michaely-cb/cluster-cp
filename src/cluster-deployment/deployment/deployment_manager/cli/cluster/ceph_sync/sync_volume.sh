#!/usr/bin/env bash
#
# sync_volume.sh  ‒ rsync one CephFS subvolume directory to another
#
# Usage:
#   ./sync_volume.sh <group_name> <subvolume_name> <src_mount> <dst_mount>
#
# Example:
#   ./sync_volume.sh cached-compile-group cached-compile-subvolume /mnt/src /mnt/dst

set -euo pipefail

GROUP="$1"
SUBVOL="$2"
SRC_MNT="$3"
DST_MNT="$4"

SRC_BASE="$SRC_MNT/volumes/$GROUP/$SUBVOL"
if [ ! -d "$SRC_BASE" ]; then
  echo "WARNING: Source subvolume path not found: $SRC_BASE" >&2
  exit 0
fi

# Find exactly one (first) UUID under source
readarray -t SRC_LIST < <( ls -1 "$SRC_BASE" 2>/dev/null )
if [ ${#SRC_LIST[@]} -eq 0 ]; then
  echo "WARNING: No UUID directory under $SRC_BASE, skipping." >&2
  exit 0
fi
if [ ${#SRC_LIST[@]} -gt 1 ]; then
  echo "WARNING: Multiple UUIDs under $SRC_BASE: ${SRC_LIST[*]}. Using: ${SRC_LIST[0]}" >&2
fi
SRC_UUID="${SRC_LIST[0]}"
SRC_PATH="$SRC_BASE/$SRC_UUID"

DST_BASE="$DST_MNT/volumes/$GROUP/$SUBVOL"
if [ ! -d "$DST_BASE" ]; then
  echo "WARNING: Destination subvolume path not found: $DST_BASE" >&2
  exit 0
fi

# Find exactly one (first) UUID under destination
readarray -t DST_LIST < <( ls -1 "$DST_BASE" 2>/dev/null )
if [ ${#DST_LIST[@]} -eq 0 ]; then
  echo "WARNING: No UUID directory under $DST_BASE, skipping." >&2
  exit 0
fi
if [ ${#DST_LIST[@]} -gt 1 ]; then
  echo "WARNING: Multiple UUIDs under $DST_BASE: ${DST_LIST[*]}. Using: ${DST_LIST[0]}" >&2
fi
DST_UUID="${DST_LIST[0]}"
DST_PATH="$DST_BASE/$DST_UUID"

echo "INFO: Rsyncing $SRC_PATH/ → $DST_PATH/"
rsync -avh --progress "$SRC_PATH/" "$DST_PATH/"
echo "SUCCESS: Rsync complete for $GROUP/$SUBVOL (UUID: $SRC_UUID)"

