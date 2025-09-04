#!/bin/bash

set -eo pipefail

DEBUG=false
RELEASE=0.0.0
PACKAGE=
while [ ! $# -eq 0 ]; do
  case "$1" in
    --help | -h)
      echo "$0 [--release RELEASE] [--package PACKAGE] [--debug]"
      echo ""
      echo "   Issues packer command to build the usb-installer"
      echo ""
      echo "   --release RELEASE   e.g. 0.0.0, used in filename of built image"
      echo "   --package PATH      path to package to include in /files partition of disk"
      echo "   --debug             turn on packer debug"
      exit 0
      ;;
    # tag the usb imagetag with a specific release
    --release | -r)
      shift
      RELEASE=$1
      ;;
    # payload to include
    --package | -p)
      shift
      PACKAGE=$1
      ;;
    # step through the packer build process
    --debug | -d)
      DEBUG=true
      ;;
    -*)
      echo "ERROR: $1 is not a valid flag"
      exit 1
      ;;
  esac
  shift
done

if [ -z "$PACKAGE" ]; then
  echo "WARNING: creating image with empty /files partition"
  echo "         use --package [Cluster.tar.gz|cluster-os-bundle.tar] file to include deploy artifacts"
  PACKAGE="/tmp/empty"
  touch /tmp/empty
elif ! [ -f ${PACKAGE} ]; then
  echo "package $PACKAGE does not exist!"
  exit 1
fi

if [[ "$DEBUG" = true ]] ; then
  export PACKER_FLAGS=-debug
fi

# run packer
export PKR_libvirt_server="sc-r9ra14-s9.cerebrassc.local"
export PKR_VAR_sshkey=${PKR_VAR_sshkey:-$HOME/.ssh/id_rsa}
export PKR_VAR_user=$USER
export PKR_VAR_imagetag="${RELEASE}-$(git rev-parse --short HEAD)"
export PKR_VAR_package_path="${PACKAGE}"

packer init .
packer build -timestamp-ui $PACKER_FLAGS .

artifact_path=$(cat manifest.json | jq -r '.builds[-1].artifact_id')
artifact_id=$(basename $artifact_path)
echo ""
echo "created ${artifact_path} on ${PKR_libvirt_server}"
echo "convert the image with"
echo ""
echo "    qemu-img convert -f qcow2 -O raw ${artifact_path} ${artifact_id%.*}.img"
echo ""

rm -f /tmp/empty &>/dev/null || true
