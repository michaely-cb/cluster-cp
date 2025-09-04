#!/usr/bin/env bash

# Modified from ../prometheus/create-tls-secrets.sh

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"
source "../pkg-common.sh"

if [ -n "$INCREMENTAL_DIR" ]; then
  exit 0
fi

function get_loki_cert() {
  local SERVICE_NAME=$1
  cert_b64=$($KUBECTL get secret ${SERVICE_NAME}-general-tls -n loki --ignore-not-found -o=jsonpath='{.data.tls\.crt}')
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
    -n loki
}

function validate_or_recreate_cert() {
  local SERVICE_NAME=$1
  if ! get_loki_cert ${SERVICE_NAME}; then
    return 1
  fi

  local issuer=$(openssl x509 -in ${SERVICE_NAME}-general-tls.crt -nocert -noout -issuer)
  local expected_issuer="issuer=CN = ${service_domain}"

  if [[ "$issuer" != $expected_issuer* ]]; then
    echo "invalid cert issued for ${issuer}, expecting ${service_domain}" >&2

    $KUBECTL delete secret ${SERVICE_NAME}-general-tls -n loki
    # recreate the secret.
    create_secret ${SERVICE_NAME}
  fi
  return 0
}

$KUBECTL create namespace loki --dry-run=client -o yaml 2>/dev/null | $KUBECTL apply -f -

if ! $KUBECTL get secret loki-general-tls -n loki >/dev/null; then
  create_secret loki
else
  echo "loki-general-tls already exists. Validate the secret and recreate as needed"
  validate_or_recreate_cert loki
fi