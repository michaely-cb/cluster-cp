#!/bin/ash
set -euo pipefail

# ---- Packages ----
# Enable community repo line
sed -i -E 's/^#(.*v3.*community)/\1/' /etc/apk/repositories
apk upgrade --no-cache -a
apk add --no-cache \
  e2fsprogs e2fsprogs-extra \
  lsblk qemu-img \
  util-linux \
  cloud-utils-growpart \
  parted \
  exfatprogs \
  fuse \
  fuse-exfat

# ---- Disk Layout ----
DISK=/dev/sda
PART_IDX=4
DISK_PART=${DISK}${PART_IDX}

# create sda4 occupying all the remaining space and create an exfat FS on it
parted -s "$DISK" -- mkpart primary 10% 100%
partprobe "$DISK" || true
growpart "${DISK}" "$PART_IDX" || true
# this sets the partition type to NTFS/exFAT which is needed to mount on mac/windows
sfdisk --part-type /dev/sda 4 0x07
partprobe "$DISK" || true

mkdir -p /files
mkfs.exfat --volume-label=CBDEPLOY "${DISK_PART}"
UUID=$(blkid -s UUID -o value "$DISK_PART")
echo "UUID=$UUID /files exfat rw,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other,blksize=4096 0 0" >> /etc/fstab

# Add fuse module and reboot for effect...
printf '%s\n' fuse > /etc/modules
rc-update add modules boot
reboot