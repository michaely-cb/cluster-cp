#!/bin/bash

# This standalone script is not part of the deploy/upgrade process. It only
# updates the Cluster Mgmt alerting rules and Slack notification settings.
#
# Instructions:
# 0. Ensure yq --version >= v4.33.3. If not, copy it to the mgmt node and update
#    the YQ= var is this script. This script did NOT work with 4.27.5.
# 1. In the cluster's pkg-properties.yaml, set the Slack URL:
#    .properties.alertmanager.receivers.control_plane_slack = 'https://hooks.slack.com/services/...'
# 2. Copy this script and config files to the same dir on the mgmt node:
#    scp standalone-update-cluster-mgmt-rules.sh rules.yaml slack-notification.tmpl alertmanager-slack-config-base.yaml root@MGMT_NODE:
# 3. SSH into the mgmt node, cd to the dir, and run this script.

# Path to Cluster Mgmt's rules.yaml (relative to this script on the mgmt node)
RULES_YAML=rules.yaml
SLACK_CONFIG_BASE=alertmanager-slack-config-base.yaml
CLUSTER_PROPERTIES="/opt/cerebras/cluster/pkg-properties.yaml"
CLUSTER_CONFIG="/opt/cerebras/cluster/cluster.yaml"
KUBECTL=kubectl
YQ=/usr/local/bin/yq

function validate_yaml_file() {
  yaml_file="$1"
  $YQ --exit-status 'tag == "!!map" or tag== "!!seq"' "${yaml_file}" > /dev/null
  exit_code=$?
  if [ $exit_code -ne 0 ]; then
    >&2 echo "Error: parsing ${yaml_file}"
    exit $exit_code
  fi
}

validate_yaml_file "${RULES_YAML}"
validate_yaml_file "${SLACK_CONFIG_BASE}"
validate_yaml_file "${CLUSTER_PROPERTIES}"
validate_yaml_file "${CLUSTER_CONFIG}"

# Read the Slack API URL from pkg-properties.yaml
properties=$($YQ --exit-status -ojson . ${CLUSTER_PROPERTIES})
control_plane_slack=$(echo $properties | jq -r '.properties.alertmanager.receivers.control_plane_slack // empty')
if [ ! -n "${control_plane_slack}" ]; then
  >&2 echo "Error: control plane Slack API URL not found in pkg-properties.yaml."
  exit 1
fi

# Read the cluster domain name from cluster.yaml
cluster_name=$($YQ -r '.name' ${CLUSTER_CONFIG})
service_domain=$($YQ -r '.properties.serviceDomain // .serviceDomain // "cerebrassc.local"' ${CLUSTER_CONFIG})
grafana_url="https://grafana.${cluster_name}.${service_domain}/"

slack_route='{"continue":true,"matchers":["control_plane_oncall=true"],"receiver":"control_plane_slack_receiver"}'

echo > slack-receiver.yaml
$YQ '.name="control_plane_slack_receiver"' -i slack-receiver.yaml
$YQ ".slack_configs[0]=load(\"${SLACK_CONFIG_BASE}\")" -i slack-receiver.yaml
v=${control_plane_slack} yq ".slack_configs[0].api_url=env(v)" -i slack-receiver.yaml

# Patch alertmanager.yaml with Slack receiver and route
$KUBECTL -n prometheus get secret alertmanager-prometheus-alertmanager -ojson | jq -r ".data.\"alertmanager.yaml\"" | base64 -d | $YQ > orig.alertmanager.yaml
cp ./orig.alertmanager.yaml ./alertmanager.yaml
if [[ $($YQ ".route.routes | contains([${slack_route}])" alertmanager.yaml) == "true" ]]; then
  >&2 echo Slack route already configured in alertmanager.yaml. Skipping.
else
  $YQ ".route.routes = .route.routes.[:1] + [${slack_route}] + .route.routes.[1:]" -i alertmanager.yaml
fi
if [[ $($YQ '.receivers | contains([load("slack-receiver.yaml")])' alertmanager.yaml) == "true" ]]; then
  >&2 echo Slack receiver already configured in alertmanager.yaml. Skipping.
else
  # Delete any other receivers with the same name
  $YQ 'del(.receivers[] | select(.name == "control_plane_slack_receiver"))' -i alertmanager.yaml
  $YQ '.receivers += [load("slack-receiver.yaml")]' -i alertmanager.yaml
fi
if [[ "$(diff orig.alertmanager.yaml alertmanager.yaml)" == "" ]]; then
  >&2 echo No changes to alertmanager.yaml. Skip patching.
else
  $KUBECTL -nprometheus create secret generic alertmanager-prometheus-alertmanager --save-config --dry-run=client --from-file=alertmanager.yaml --from-file=slack-notification.tmpl -o yaml | $KUBECTL apply -f -
fi

# Load rules.yaml into the prometheus-cluster-mgmt prometheusrules (idempotent operation)
$KUBECTL get prometheusrule -n prometheus prometheus-cluster-mgmt -oyaml | $YQ > prometheusrules-prometheus-cluster-mgmt.yaml
$YQ '.spec.groups = load("'"${RULES_YAML}"'").additionalPrometheusRulesMap.cluster-mgmt.groups' prometheusrules-prometheus-cluster-mgmt.yaml > updated.prometheusrules-prometheus-cluster-mgmt.yaml
# Add cluster domain name to dashboard_url, if missing (i.e. just the URI starting with /d/...):
$YQ '(.. | select(key == "annotations") | .. | select(key == "dashboard_url*")) |= sub("^/(d/)", "'"${grafana_url}"'${1}")' -i updated.prometheusrules-prometheus-cluster-mgmt.yaml
$KUBECTL apply -f updated.prometheusrules-prometheus-cluster-mgmt.yaml