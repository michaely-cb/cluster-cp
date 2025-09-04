#!/bin/bash

# start sshd
systemctl enable ssh
systemctl start ssh

# restart kubelet - something about installing ssh seems to temporarily breaks kubelet
systemctl restart kubelet

exec "$@"