# Script for creating raid array on NVMe drives - adapted from old colo kickstart post script
set -e

nvme_drives=$(ls /dev/nvme?n1)
echo "RAID disks are:" $nvme_drives

# Stopping a RAID set up as /dev/$(raid) and mounted on /n0
stop_raid()
{
    raid=$1
    sed -i "/$raid/d" /etc/fstab
    sed -i "/\/n0/d" /etc/fstab
    # Prevent any new job from using the RAID
    systemctl daemon-reload

    # Kill any jobs using the mount
    set +e  # lsof returns non-zero exit value.
    jobs=$(lsof -t +d /n0)
    set -e
    if [ -n "$jobs" ]; then
        echo "Killing jobs using the RAID:" $jobs
        kill -9 $jobs || true
    fi

    umount /n0
    systemctl daemon-reload
    mdadm --stop /dev/$raid
}

echo "Stopping existing RAID"
if [ -e /dev/md127 ]; then
    # Old RAID
    stop_raid md127
fi
if [ -e /dev/n0_pool ]; then
    # New RAID
    # TODO: support deleting LVM-based RAID
    echo "LVM-based RAID already created.  Not supporting re-creating it"
    exit 0
fi

echo "Creating RAID"

for drive in $nvme_drives; do
    wipefs -a -f $drive
    pvcreate $drive
done

mkdir -p /n0
vgcreate n0_pool $nvme_drives
lvcreate -n n0 -l 100%FREE n0_pool
mkfs.xfs /dev/n0_pool/n0
echo '/dev/n0_pool/n0 /n0 xfs defaults 0 0' >> /etc/fstab
systemctl daemon-reload
mount -a
chmod 777 /n0

echo "Created RAID successfully"
