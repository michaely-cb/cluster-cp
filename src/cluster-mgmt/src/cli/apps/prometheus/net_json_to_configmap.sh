#!/bin/bash

set -e

msg_prefix="network json configmap updater:"
net_json_path=/opt/cerebras/cluster/network_config.json
pkg_properties_path=/opt/cerebras/cluster/pkg-properties.yaml

# Annoying kube facts:
# There is a 256kB limit for configmaps updated with 'apply', and a 1MB limit
# for configmaps created with 'apply' or 'create'
# You can't 'replace' a configmap that doesn't exist yet.

# The snmp-target-list is switch host names only so 256K ought to be enough for
#   anybody - we can use apply.
# The interface-watch-list will exceed 1MB for a 100 system cluster, so we now
#   serve this in a simple script that gunzips the netjson.gz file.
# The full net json on a 24-system cluster is just over 1M, so we need to gzip
#   the file before create/replace. The gzipped file is about 44kB.

make_snmp_cms ()
{
    gzip $tmpdir/network_config.json

    # create the snmp-target-list CM
    python3 /usr/local/bin/generate_interface_list.py -o $tmpdir
    $KUBECTL create configmap snmp-target-list --from-file=snmp-sd.json=$tmpdir/snmp-sd.json -n prometheus --dry-run=client -oyaml | kubectl apply -f -

    if [ $get_cm_retval -ne 0 ]; then
        $KUBECTL create configmap -n prometheus linkmon-network-config-json --from-literal=netjson_file_time=${netjson_file_time} --from-literal=pkgprops_file_time=${pkgprops_file_time} --from-file=$tmpdir/network_config.json.gz --from-file=$tmpdir/published_stamps
    else
        $KUBECTL create configmap -n prometheus linkmon-network-config-json --from-literal=netjson_file_time=${netjson_file_time} --from-literal=pkgprops_file_time=${pkgprops_file_time} --from-file=$tmpdir/network_config.json.gz --from-file=$tmpdir/published_stamps --dry-run=client -oyaml | $KUBECTL replace configmap -n prometheus linkmon-network-config-json -f-
    fi
}

make_usernode_probes ()
{
    $KUBECTL delete --ignore-not-found=true probe -n prometheus cluster-mgmt-node-ipmi-exporter-usernode
    $KUBECTL delete --ignore-not-found=true probe -n prometheus cluster-mgmt-usernode-node-exporter

    usernode_count=$(jq '.user_nodes | length' $tmpdir/network_config.json)
    if [ "$usernode_count" -gt 0 ]; then
      jq -r .user_nodes[].name $tmpdir/network_config.json > $tmpdir/usernode-targets.yaml
      
      cat << EOF > $tmpdir/usernode-ipmi-probe.yaml
apiVersion: monitoring.coreos.com/v1
kind: Probe
metadata:
  name: cluster-mgmt-node-ipmi-exporter-usernode
  namespace: prometheus
spec:
  interval: 1m
  module: health
  prober:
    url: cluster-mgmt-node-ipmi-exporter:8007
    path: /probe
  scrapeTimeout: 1m
  targets:
    staticConfig:
      static:
EOF
      cat << EOF > $tmpdir/usernode-node-exporter-probe.yaml
apiVersion: monitoring.coreos.com/v1
kind: Probe
metadata:
  name: cluster-mgmt-usernode-node-exporter
  namespace: prometheus
spec:
  interval: 1m
  module: health
  prober:
    path: /metrics
    url: RELABEL_TARGET_STRING
  scrapeTimeout: 1m
  metricRelabelings:
    # Drop metrics from interfaces we don't care about.
    # This is only for user nodes. See values.yaml for the corresponding
    # interface dropping for other nodes.
    - sourceLabels: [device]
      regex: (lo|lxc|mac|nerdctl|docker|cilium|idrac).*
      action: drop
  targets:
    staticConfig:
      relabelingConfigs:
        - sourceLabels: [instance]
          targetLabel: __address__
          # action: replace (DEFAULT)
          # replacement: $1 (DEFAULT)
      static:
EOF
      for i in `cat $tmpdir/usernode-targets.yaml`; do
        echo "      - $i-ipmi" >> $tmpdir/usernode-ipmi-probe.yaml

        # so that we see the IP address as the instance label for node metrics
        ip=`getent ahosts $i | awk '{ print $1; exit }'`
        if [ -n "$ip" ]; then
          echo "      - $ip:9100" >> $tmpdir/usernode-node-exporter-probe.yaml
        fi
      done
    
      $KUBECTL create -f $tmpdir/usernode-ipmi-probe.yaml
      $KUBECTL create -f $tmpdir/usernode-node-exporter-probe.yaml
    fi
}

set +e # Don't exit if the CM doesn't exist
netjson_cm_time=$($KUBECTL get configmap -n prometheus linkmon-network-config-json -ojsonpath="{.data.netjson_file_time}" 2>/dev/null)
get_cm_retval=$?
pkgprops_cm_time=$($KUBECTL get configmap -n prometheus linkmon-network-config-json -ojsonpath="{.data.pkgprops_file_time}" 2>/dev/null)
set -e

enable_monitoring="false"
if [ -f "$pkg_properties_path" ]; then
    enable_monitoring=$(yq -r '.properties.usernode.enable_monitoring // "false"' $pkg_properties_path)
fi

tmpdir=$(mktemp -d)

if [ -f $net_json_path ]; then
    netjson_file_time=$(stat -L -c %Y $net_json_path)
    pkgprops_file_time=$(stat -L -c %Y $pkg_properties_path)
    # update if either file has a different modtime than the version in the CM
    # use -ne instead of -gt to handle updates when a file is a symlink
    if [ $get_cm_retval -ne 0 ] || [[ $netjson_file_time -ne $netjson_cm_time ]] || [[ $pkgprops_file_time -ne $pkgprops_cm_time ]]; then
        echo "$msg_prefix network json or pkg properties file has different mtime than configmap and needs update or configmap does not exist, creating new configmap"
        cp $net_json_path $tmpdir/network_config.json
        yq -r '.properties.clusterMgmt.publishedStamps // ["default"] | .[]' $pkg_properties_path >$tmpdir/published_stamps
        if [ "$enable_monitoring" == "true" ]; then
          make_usernode_probes
        fi
        make_snmp_cms
    fi
else
    if [ $get_cm_retval -ne 0 ]; then
        echo "$msg_prefix No network_config.json and no existing configmap, creating configmap with empty network_config.json"

        echo "{}" >$tmpdir/network_config.json
        touch $tmpdir/published_stamps
        if [ "$enable_monitoring" == "true" ]; then
          make_usernode_probes
        fi
        make_snmp_cms
    else
        echo "$msg_prefix $net_json_path does not exist but a configmap already exists, doing nothing"
    fi
fi
rm -fr $tmpdir
