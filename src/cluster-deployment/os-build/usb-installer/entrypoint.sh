#!/usr/bin/env sh

set -euo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin


### ENV ###

BOOT_DEVICE=
BOOT_MOUNT_PART=

NVME_COUNT=$(ls /sys/block | grep -c '^nvme' || echo "0")

INSTALL_FUNCTION=


### Helpers ###

fail_to_shell() {
    echo "error: $1"
    echo "exiting to shell..."
    exec /bin/ash
}

is_bootable_device() {
    # Check if the device exists and it is not a removable device like a USB flash drive
    local dev_name
    dev_name=$(basename "$1")
    if [[ -b "$1" ]]; then
        [[ $(cat "/sys/block/$dev_name/removable") -eq 0 ]]
    else
        return 1
    fi
}

set_boot_disk_params() {
    # Simple checks to ensure we deploy the OS image to the right boot drive.
    # This is not foolproof though, as we may have drives that are not seated
    # properly.
    # The clean way is for the caller to specify the boot drive.

    if is_bootable_device /dev/sda ; then
        BOOT_DEVICE=/dev/sda
        BOOT_MOUNT_PART=/dev/sda1
        
    # Check if the NVMe block device /dev/nvme0n1 exists, and as a simple heuristic,
    # that the number of NVMe devices is not 3, which is the case for RAID setup.
    # This needs to be refined once we have better idea of the new server config.
    elif is_bootable_device /dev/nvme0n1 && [[ $NVME_COUNT -ne 3 ]]; then
        BOOT_DEVICE=/dev/nvme0n1
        BOOT_MOUNT_PART=/dev/nvme0n1p1
    else
        return 1
    fi
}

validate_sha() {
    local shafile=${1}.sha256
    if ! [ -f "$shafile" ]; then
        echo "skipping ${1} sha256 validation, ${shafile} not found"
        return 0
    fi
    echo "validating ${pkg} shasum..."
    cd $(dirname $1)
    if ! sha256sum -c ${shafile}; then
        fail_to_shell "invalid shasum for ${1}. This drive may be corrupted or the install media changed. Verify ${shafile} is correct"
    fi
    cd
}

install_from_os_bundle() {
    local bundle=/files/cluster-os-bundle.tar
    validate_sha $bundle
    # TODO: read path from manifest
    tar xf "${bundle}" os_image/rocky8-base.tar.gz -O | tar -xOzf - | dd of=${BOOT_DEVICE} bs=1M
    sync
}

install_from_cluster_package() {
    local pkg=$(ls /files/ | grep -E '^Cluster.*.tar.gz$')
    pkg=/files/${pkg}
    validate_sha $pkg
    # extract the os bundle, then extract the os image, and untar/copy to disk
    tar xzOf "${pkg}" cluster-os-bundle.tar | tar xf - os_image/rocky8-base.tar.gz -O | tar -xOzf - | dd of=${BOOT_DEVICE} bs=1M
    sync
}

set_install_function() {
    if [ -f /files/cluster-os-bundle.tar ]; then
        INSTALL_FUNCTION=install_from_os_bundle
    elif [[ $(ls /files/ | grep -E -c '^Cluster.*.tar.gz$') -eq 1 ]]; then
        INSTALL_FUNCTION=install_from_cluster_package
    else
        return 1
    fi
}

### Entrypoint ###


echo ""
echo "Welcome to the Cerebras Cluster OS Installer"
echo ""


if ! set_boot_disk_params; then
    echo "block devices:"
    lsblk
    fail_to_shell "failed to find suitable root disk, check block devices"
fi

if ! set_install_function; then
    echo "ls /files:"
    ls -al /files
    fail_to_shell "failed to find suitable OS image, ensure /files has a Cluster.tar.gz or cluster-os-bundle.tar loaded"
fi

trap /bin/ash SIGINT
echo "Going to install OS to ${BOOT_DEVICE} in 10 seconds. Cancel to shell with ctl+c"
sleep 10
trap - SIGINT

echo "Installing to ${BOOT_DEVICE}, this will take a few minutes"

set -x
if ! $INSTALL_FUNCTION; then
    set +x
    fail_to_shell "failed to install OS, check output"
fi
set +x
reboot
