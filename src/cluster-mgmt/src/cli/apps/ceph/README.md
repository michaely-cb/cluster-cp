# ceph installation

The ceph installation has been integrated in the overall deployment/package flow. However, there are a few manual steps that needs to be done to prepare the SSDs to be
used by ceph for storage.

## Turn off swaps on management nodes

The command looks like
```
swapoff /n0/swapdir/extended_swapfile
swapoff /n0/swapdir/swapfile
```

## Prepare SSD disks for ceph

Currently, SSD disks on the managment nodes are set up with RAID0. This step is to take
those disks out of RAID0, and clear it.

```
# unmount /n0
umount /n0

# stop the RAID0 device
mdadm --stop /dev/md127
mdadm --remove /dev/md127  # This might not be needed, but it didn't hurt.

# Clear the disk
mdadm --zero-superblock /dev/nvme0n1p1 /dev/nvme1n1p1 /dev/nvme2n1p1

```
