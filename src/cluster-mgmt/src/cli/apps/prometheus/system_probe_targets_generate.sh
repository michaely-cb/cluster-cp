#!/bin/bash

set -e

template='
apiVersion: monitoring.coreos.com/v1
kind: Probe
metadata:
  name: cluster-mgmt-system-exporter
  namespace: prometheus
spec:
  interval: 1m
  module: ssh
  prober:
    path: /probe
    url: cluster-mgmt-system-exporter:8006
  scrapeTimeout: 1m
  targets:
    staticConfig:
      static:
'

if [ -z "$KUBECTL" ]; then KUBECTL="kubectl"; fi

for system in $($KUBECTL get cm cluster -ojson -n"${SYSTEM_NAMESPACE}" | jq '.data."clusterConfiguration.yaml"' -r |
  yq -ojson | jq ".systems[]" -r | jq '.name+","+.managementAddress+","+.controlAddress'); do
  template=$(echo "$template" | yq eval ".spec.targets.staticConfig.static += [$system]")
done
echo "$template" | $KUBECTL apply -f -
