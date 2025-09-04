#!/bin/bash

set -xe

if [[ -z "$DEFAULT_PASSWORD" ]]; then
   echo "DEFAULT_PASSWORD env is not defined, set it with default password"
   exit 1
fi

MIRROR_SERVER=sc-r9ra14-s9.cerebrassc.local
CLUSTER_DEPLOY_DIR=$GITTOP/src/cluster_deployment
BUILDTMP=$GITTOP/build
CLUSTER_TMP_FILES=$BUILDTMP/cluster-deploy-files/files
mkdir -p $CLUSTER_TMP_FILES
OS_MANIFEST=$CLUSTER_DEPLOY_DIR/os-build/rocky8-base/manifest.json
OS_IMAGE_PATH=$(jq -r '.builds[-1].artifact_id' $OS_MANIFEST)
RAW_IMAGE_PATH=/tmp/rocky8.img
IMAGE_TAR=rocky8-base.tar.gz
IMAGE_TAR_PATH=/tmp/rocky8-base.tar
SSHPASS="sshpass -p $DEFAULT_PASSWORD ssh lab@$MIRROR_SERVER"
$SSHPASS qemu-img convert -f qcow2 -O raw $OS_IMAGE_PATH $RAW_IMAGE_PATH
$SSHPASS tar -cf $IMAGE_TAR_PATH -C /tmp rocky8.img
$SSHPASS pigz -9 $IMAGE_TAR_PATH
$SSHPASS rm -f $RAW_IMAGE_PATH
sshpass -p $DEFAULT_PASSWORD rsync lab@$MIRROR_SERVER:$IMAGE_TAR_PATH.gz $CLUSTER_TMP_FILES/$IMAGE_TAR
$SSHPASS rm -f $IMAGE_TAR_PATH.gz
