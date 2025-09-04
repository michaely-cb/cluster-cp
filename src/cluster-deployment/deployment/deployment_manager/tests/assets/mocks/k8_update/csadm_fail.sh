#!/usr/bin/env bash

if [ "$1" == "update-cluster-config" ]; then
  touch /opt/cerebras/cluster/cluster.yaml
  exit 0
fi

# mock csadm.sh install
touch .apply-k8s.sh.log
touch .apply-cilium.sh.log
exit 1