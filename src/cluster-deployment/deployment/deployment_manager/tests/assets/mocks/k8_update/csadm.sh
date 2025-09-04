#!/usr/bin/env bash

# mock csadm.sh install
cat <<EOF > /opt/cerebras/cluster/cluster.yaml
kind: cluster-configuration
name: multibox-test
systems:
  - name: xs10000
    type: cs3
nodes:
  - name: test-wse001-mg-sr01
    role: management
    networkInterfaces:
      - name: ens2f0np0
        address: 10.251.32.29
    properties:
      group: nodegroup0
      controlplane: "true"
groups:
  - name: nodegroup0
EOF

touch .apply-k8s.sh.log
touch .apply-cilium.sh.log