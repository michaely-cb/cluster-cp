#!/usr/bin/env bash
# This deploy script is based on the template in ../../cli/apps/alert-router/alert-router-deploy.sh.jinja2

SCRIPT_PATH=$(dirname $0)
cd $SCRIPT_PATH

set -e

if [[ -z "${ALERT_ROUTER_TAG}" ]]; then
  >&2 echo 'Error: environment variable not set: ALERT_ROUTER_TAG'
  exit 1
fi

# Create the K8s Config-Map
kubectl apply -f ../../cli/apps/alert-router/alert-classification.yaml

# Create the K8s Secret (if it doesn't already exist)
SECRET_NAME="cluster-operator-contacts"
K8S_NAMESPACE="prometheus"

if kubectl get secret "$SECRET_NAME" -n  "$K8S_NAMESPACE" >/dev/null 2>&1; then
    echo "Secret '$SECRET_NAME' already exists in namespace '$K8S_NAMESPACE'."
else
    echo "Secret '$SECRET_NAME' not found, creating ..."

    kubectl create secret generic "$SECRET_NAME" -n "$K8S_NAMESPACE" \
        --from-literal=config="$(cat contacts.yaml)"

    echo "Secret '$SECRET_NAME' created in namespace '$K8S_NAMESPACE'"
fi


# Create the K8s Deployment
cluster_name="wsperfdrop" # TODO: make this dynamic
cluster_domain_name="wsperfdrop.cerebrassc.local" # TODO: make this dynamic

echo "Deploying alert router with tag=$ALERT_ROUTER_TAG cluster=$cluster_name domain=$cluster_domain_name"

# We can't use `sed -i`, because it's not portable to MacOS
sed -e "s/\${ALERT_ROUTER_TAG}/${ALERT_ROUTER_TAG}/g" \
    -e "s/\${CLUSTER_NAME}/${cluster_name}/g" \
    -e "s/\${CLUSTER_DOMAIN_NAME}/${cluster_domain_name}/g" \
    test-kind-deploy.yaml > ./test-kind-deploy.updated.yaml

kubectl apply -f test-kind-deploy.updated.yaml

echo "Alert router deployed successfully"