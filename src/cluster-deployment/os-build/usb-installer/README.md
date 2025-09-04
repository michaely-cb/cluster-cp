# USB installer

Contains a packer script for building a boot Alpine linux disk image.

The image has an exfat partition which allows Mac/Windows users to mount the
/files partition and copy Cluster.tar.gz or other artifacts to the drive without
needing to re-build the installer.

The image can be built with a Cluster.tar.gz file in the /files partition or it
can be left empty in the case that the USB installer should be updated by
the operator who is installing the OS in the field.

## Building

To use this image, ensure you have access to the build server (see os-build/README.md).
Then run build.sh and convert the resulting image into raw format and dd it to a
USB flash drive. The drive can then be booted on a server which is in BIOS boot mode.

## Burning to a USB

Convert the image on the build node to raw format and then copy it to a flash drive.

Triple check that you're burning to the correct disk otherwise you could lose your system...

```
qemu-img convert -f qcow2 -O raw /n0/builds/usb-installer-xxx.qcow2 usb-installer-xxx.img

dd if=usb-installer-xxx.img of=/dev/disk4 bs=4M status=progress conv=fsync
```

# Testing

There are 2 parts to testing
1. testing the packer build
2. testing the burned flash drive installs the OS

To test (1), you can run the build.sh script with --debug. This will step through packer's
build steps. If you need ssh access to the virtual machine, then ssh to the build node
and connect to the alpine VM while packer is paused in a step after it has started
the VM. Packer will print the IP of this VM or you can use virsh to find it. You
can ssh to the VM using credentials root/root and troubleshoot commands on the fly.

You can also test the resulting alpine image on the build node or any node with libvirt:

```
IMG=PATH_TO_QCOW2_FILE

virt-install --name usb-installer-test \
  --ram 4096 \
  --vcpus 4 \
  --disk path=$IMG,format=qcow2,bus=virtio \
  --import \
  --network network=test,model=virtio \
  --os-variant alpinelinux3.10

# then use virt-manager with x-forwarding to connect to the console
# or use 'virsh domifaddr usb-installer-test' and ssh to the IP of the VM

# when finished, cleanup
virsh destroy usb-installer-test
virsh undefine usb-installer-test
virsh vol-delete --pool builds $IMG
```
