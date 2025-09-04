#!/bin/bash
# Script to clean and reset NVMe drives for RAID
# Handles some common cleanup cases when dealing with servers reprovisioned from IT setup cases
set -euo pipefail

RAID_DEVICE=/dev/md127
RAID_MOUNTPOINT=/${RAID_MOUNTPOINT:-n0}
IFS=' ' read -r -a RAID_MEMBER_DISKS <<< "${RAID_MEMBER_DISKS:-nvme0n1 nvme1n1 nvme2n1}"

raid_member_disks=()
for dev in ${RAID_MEMBER_DISKS[@]}; do
  raid_member_disks+=("/dev/$dev")
done

# for simplicity, run this script on all nodes but exit early if the devices don't exist
missing=0
for disk in "${raid_member_disks[@]}"; do
  if [ ! -b "$disk" ]; then
    echo "Device $disk not found."
    missing=1
  fi
done

if [ "$missing" -eq 1 ]; then
  echo "One or more RAID devices are missing. Skipping cleanup."
  exit 0
fi

# Remove old RAID device if present
if [ -e "${RAID_DEVICE}" ]; then
  echo "removing old RAID mounts"
  swapoff -a
  umount "$RAID_MOUNTPOINT" || true
  mdadm --stop ${RAID_DEVICE}
  sleep 10
fi

# Remove any device-mapper LVM mappings from prior IT install
# they persist on the NVME disks even if the root disk received a fresh install
if dmsetup ls | grep -q -v root; then
  echo "removing old IT RAID LVM configurations"
  pool_names=$(dmsetup ls | grep -v root | awk '{print $1}')
  for name in $pool_names; do
    dmsetup remove "$name"
    sleep 5
  done
fi

echo "cleaning RAID devices..."
for disk in "${raid_member_disks[@]}"; do
  echo "attempting blkdiscard on $disk; falling back to dd if not supported"
  blkdiscard "$disk" || dd if=/dev/zero of="$disk" bs=1M count=100 status=progress

  sleep 2
done

echo "disk cleanup complete"
