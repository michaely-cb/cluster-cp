#!/usr/bin/env bash

# Script for creating raid array on NVMe drives - adapted from old colo kickstart post script
set -e

RAID_MOUNTPOINT=${RAID_MOUNTPOINT:-n0}
IFS=' ' read -r -a RAID_MEMBER_DISKS <<< "${RAID_MEMBER_DISKS:-nvme0n1 nvme1n1 nvme2n1}"

raid_devices_count=${#RAID_MEMBER_DISKS[@]}
raid_member_disks=()
raid_member_disks_p1=()
for dev in ${RAID_MEMBER_DISKS[@]}; do
  raid_member_disks+=("/dev/$dev")
  raid_member_disks_p1+=("/dev/$dev"p1)
done

echo "Creating RAID array on NVMe drives"
echo "RAID mountpoint: $RAID_MOUNTPOINT"
echo "RAID disks: ${raid_member_disks[@]}"

echo "stopping raid"
if [ -e /dev/md127 ]; then
  swapoff -a
  umount /$RAID_MOUNTPOINT ||:
  mdadm --stop /dev/md127
  sleep 90
fi

for dev in ${raid_member_disks[@]}; do
  echo "Reformatting $dev"
  dd if=/dev/zero of="$dev" bs=1M count=1024
  sleep 1
  # Create partitions on nvme drives
  parted -s "$dev" mklabel gpt
  sleep 1
  parted -s "$dev" mkpart primary 0% 100%
  sleep 1
  parted -s "$dev" set 1 raid on

  sleep 3
done

# Create RAID0 of NVME drive partitions
echo "Creating RAID"
yes | mdadm --create "/dev/md/$RAID_MOUNTPOINT" --level=0 --raid-devices=$raid_devices_count --chunk=32768 ${raid_member_disks_p1[@]}

# Create filesystem
mkfs.xfs -i size=2048 -d agsize=1048544m -L $RAID_MOUNTPOINT "/dev/md/$RAID_MOUNTPOINT"

mkdir -p "/$RAID_MOUNTPOINT"
