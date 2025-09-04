## OS Build

This directory contains Packer configurations for the custom OS images used in cluster deployment

Currently we build the following images:

- `rocky8-base`: a Rocky 8.5 image with all internet-dependent packages/software pre-installed
- `usb-installer`: a disk image containing a bootable alpine install and a copy of the `rocky8-base` image, to be burned onto USB drives

Images are built on a KVM hypervisor using Packer's libvirt plugin, and provisioning is generally done through Ansible, with some minimal shell commands as needed.

### How to build

1. Download a [Packer](https://developer.hashicorp.com/packer/downloads) binary (v1.14.* as of 2025-08)
1. Obtain SSH keys for the KVM hypervisor
1. Run `./build.sh <dir> --release <release>`, where `dir` is one of the image directories listed above, `release` is the release of security patches to use.  E.g. `./build.sh rocky8-base/ --release 2.4.0`


Notes:
- The user needs to have root access to the mirror server (`sc-r9ra14-s9`) with passwordless login. This can be done by adding the user's public key into the `/root/.ssh/authorized_keys` file in the mirror server.
- `build.sh` requires a clean git state to run (to ignore this check, use `--skip-commit` which will produce images with a `-dirty` suffix)
- Builds will always update their respective `manifest.json` file - when preparing to merge a branch, be sure to clean up the file so that only the latest clean build for the image is listed
- `./build.sh rocky8-base/` has an extra `--create-patches` option to create the latest security patch tar ball, which can be used for subsequent OS image and usb installer build.  The created OS image is still functional.  However, it will be larger than a normal OS image build, hence not recommended for deployment purpose.  When the `--create-patches` option is used, the `--release` option will be ignored.
- Use `--debug` option to step through the build process.  This allows the user to log in to the VM during the build process to examine the VM status.

### Outputs

Builds are available (tentatively) [here](http://sc-r9ra14-s9.cerebrassc.local/builds/).

The security patch tar ball, if built, will be at [here](http://sc-r9ra14-s9.cerebrassc.local/repos/Rocky8/).

### How to update the release

First build the OS image and patches as described above. Make sure the files are available on build server's http server.
Then update the path and shasum in the manifest.yaml for the new artifacts and check in the changes.