#!/usr/bin/env bash

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"
source "../pkg-common.sh"

if [ -n "$INCREMENTAL_DIR" ]; then
  exit 0
fi

function get_prom_cert() {
  local SERVICE_NAME=$1
  cert_b64=$($KUBECTL get secret ${SERVICE_NAME}-general-tls -n prometheus --ignore-not-found -o=jsonpath='{.data.tls\.crt}')
  if [ "$?" -ne 0 ] || [ -z "${cert_b64}" ]; then
    return 1
  fi
  echo "${cert_b64}" | base64 -d >${SERVICE_NAME}-general-tls.crt
}

function create_secret() {
  local SERVICE_NAME=$1

  subjectAltName="DNS:${SERVICE_NAME}.${cluster_name}.${service_domain},DNS:${SERVICE_NAME}.${service_domain}"
  # For internal deployment, keep cerebras.com for backward compatibility.
  if [ -n "${CEREBRAS_INTERNAL_DEPLOY}" ]; then
    subjectAltName="${subjectAltName},DNS:${SERVICE_NAME}.${cluster_name}.cerebras.com,DNS:${SERVICE_NAME}.cerebras.com"
  fi

  local additional_ingress=$(get_additional_ingress)
  if [[ -n "$additional_ingress" ]]; then
    subjectAltName="${subjectAltName},DNS:${SERVICE_NAME}.${additional_ingress}.${service_domain}"
  fi

  cat <<EOF >./ssl.conf
[req]
distinguished_name=req
[san]
subjectAltName=${subjectAltName}
EOF

  openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
    -keyout ${SERVICE_NAME}_tls.key \
    -out ${SERVICE_NAME}_tls.crt -subj "/CN=$service_domain" \
    -extensions san -config ./ssl.conf

  $KUBECTL create secret tls ${SERVICE_NAME}-general-tls \
    --key ${SERVICE_NAME}_tls.key \
    --cert ${SERVICE_NAME}_tls.crt \
    -n prometheus
}

function validate_or_recreate_cert() {
  local SERVICE_NAME=$1
  if ! get_prom_cert ${SERVICE_NAME}; then
    return 1
  fi

  local issuer=$(openssl x509 -in ${SERVICE_NAME}-general-tls.crt -nocert -noout -issuer)
  local expected_issuer="issuer=CN = ${service_domain}"

  local additional_ingress=$(get_additional_ingress)
  local additional_ingress_in_tls=true
  if [[ -n "$additional_ingress" ]]; then
    if ! openssl x509 -in "${SERVICE_NAME}-general-tls.crt" -noout -ext subjectAltName | grep "$additional_ingress.$service_domain" >/dev/null; then
      additional_ingress_in_tls=false
    fi
  fi

  if [[ "$issuer" != $expected_issuer* ]] || ! $additional_ingress_in_tls; then
    if [[ "$issuer" != $expected_issuer* ]]; then
      echo "invalid cert issued for ${issuer}, expecting ${service_domain}" >&2
    else
      echo "missing additional ingress in cert for ${additional_ingress}.$service_domain" >&2
    fi

    $KUBECTL delete secret ${SERVICE_NAME}-general-tls -n prometheus
    # recreate the secret.
    create_secret ${SERVICE_NAME}
  fi
  return 0
}

$KUBECTL create namespace prometheus --dry-run=client -o yaml 2>/dev/null | $KUBECTL apply -f -

if ! $KUBECTL get secret prometheus-general-tls -n prometheus >/dev/null; then
  create_secret prometheus
else
  echo "promethues-general-tls already exists. Validate the secret and recreate as needed"
  validate_or_recreate_cert prometheus
fi

if ! $KUBECTL get secret grafana-general-tls -n prometheus >/dev/null; then
  create_secret grafana
else
  echo "grafana-general-tls already exists. Validate the secret and recreate as needed"
  validate_or_recreate_cert grafana
fi

if ! $KUBECTL get secret alertmanager-general-tls -n prometheus >/dev/null; then
  create_secret alertmanager
else
  echo "alertmanager-general-tls already exists. Validate the secret and recreate as needed"
  validate_or_recreate_cert alertmanager
fi

if has_multiple_mgmt_nodes; then
  if ! kubectl get secret ruler-general-tls -n prometheus >/dev/null; then
    create_secret ruler
  else
    echo "ruler-general-tls already exists. Validate the secret and recreate as needed"
    validate_or_recreate_cert ruler
  fi

  if ! kubectl get secret query-general-tls -n prometheus >/dev/null; then
    create_secret query
  else
    echo "query-general-tls already exists. Validate the secret and recreate as needed"
    validate_or_recreate_cert query
  fi
fi
