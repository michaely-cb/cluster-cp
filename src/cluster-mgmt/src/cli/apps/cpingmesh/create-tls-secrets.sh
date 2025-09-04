#!/usr/bin/env bash

function get_cping_viz_cert() {
  local SERVICE_NAME=$1
  cert_b64=$($KUBECTL get secret ${SERVICE_NAME}-general-tls -n cpingmesh --ignore-not-found -o=jsonpath='{.data.tls\.crt}')
  if [ "$?" -ne 0 ] || [ -z "${cert_b64}" ]; then
    return 1
  fi
  echo "${cert_b64}" | base64 -d >${SERVICE_NAME}-general-tls.crt
}

function create_secret() {
  local SERVICE_NAME=$1

  subjectAltName="DNS:${SERVICE_NAME}.${cluster_name}.${service_domain},DNS:${SERVICE_NAME}.${service_domain}"

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
    -n cpingmesh
}

function validate_or_recreate_cert() {
  local SERVICE_NAME=$1
  if ! get_cping_viz_cert ${SERVICE_NAME}; then
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

  if [[ "$issuer" != $expected_issuer* ]] || ! additional_ingress_in_tls; then
    if [[ "$issuer" != $expected_issuer* ]]; then
      echo "invalid cert issued for ${issuer}, expecting ${expected_issuer}" >&2
    else
      echo "missing additional ingress ${additional_ingress}.${service_domain} in cert" >&2
    fi

    $KUBECTL delete secret ${SERVICE_NAME}-general-tls -n cpingmesh
    # recreate the secret.
    create_secret ${SERVICE_NAME}
  fi
  return 0
}

if ! $KUBECTL get secret cpingmesh-general-tls -n cpingmesh >/dev/null; then
  create_secret cpingmesh
else
  echo "cpingmesh-general-tls already exists. Validate the secret and recreate as needed"
  validate_or_recreate_cert cpingmesh
fi
