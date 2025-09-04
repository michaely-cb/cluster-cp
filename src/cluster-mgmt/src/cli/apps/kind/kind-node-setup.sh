#!/usr/bin/env bash

set -e

# Install SSH on kind node. This is required to mimic a colo so that we can
# run packaging scripts in exactly the same way as they are run in production.
# NOTE: the package install steps could be moved to the Dockerfile, but this
# requires infra setup.

# Install required deps
apt-get update && apt-get install \
    bc \
    bsdmainutils \
    cron \
    gettext \
    inetutils-ping \
    logrotate \
    pssh \
    python3-yaml \
    rsync \
    ssh \
    sshpass \
    unzip \
    vim \
    wget \
    zip \
    -y

# Setup SSH
link /usr/bin/parallel-scp /usr/bin/pscp.pssh
link /usr/bin/parallel-ssh /usr/bin/pssh
< /dev/zero ssh-keygen -t rsa -C "root@kind-cluster" -q -P ""
cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
# Set .ssh permissions otherwise the sshd service will not start.
chown root /root/.ssh/authorized_keys
chgrp root /root/.ssh/authorized_keys
chmod 0600 /root/.ssh/authorized_keys
chmod 0700 /root/.ssh
# Configure ssh to allow password access
mkdir -p /etc/ssh/sshd_config.d
echo 'PermitRootLogin yes' >/etc/ssh/sshd_config.d/override.conf
echo 'PasswordAuthentication yes' >>/etc/ssh/sshd_config.d/override.conf

# finally add yq - have to check arch - arm64 is the first clause
YQ_URL="https://github.com/mikefarah/yq/releases/download/v4.28.2"
ARCH="amd64"
if [ "$(uname -m)" = "aarch64" ] ; then ARCH="arm64" ; fi
wget ${YQ_URL}/yq_linux_$ARCH -O /usr/bin/yq
chmod a+x /usr/bin/yq
