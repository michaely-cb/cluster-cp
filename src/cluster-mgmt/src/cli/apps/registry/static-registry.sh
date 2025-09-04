#!/usr/bin/env bash
set -e
set -x


# Start the registry container with logging
nerdctl -n k8s.io run --detach --network host --name registry \
  -v /n0/cluster-mgmt/registry:/var/lib/registry \
  -v "${CERTS_DIR}":/root/registry \
  -v /n0/cluster-mgmt/registry-logs:/var/log/registry \
  --env REGISTRY_HTTP_TLS_CERTIFICATE=/root/registry/registry_tls.crt \
  --env REGISTRY_HTTP_TLS_KEY=/root/registry/registry_tls.key \
  --env REGISTRY_HTTP_DEBUG_ADDR=":5001" \
  --env REGISTRY_STORAGE_FILESYSTEM_MAXTHREADS=10000 \
  --env REGISTRY_LOG_ACCESSLOG_DISABLED=true \
  --env REGISTRY_LOG_FORMATTER=json \
  --env REGISTRY_STORAGE_DELETE_ENABLED=true \
  --entrypoint /bin/sh "${registry_url}"/"${registry_image}" -c "registry serve /etc/docker/registry/config.yml > >(tee -a /var/log/registry/registry.log) 2> >(tee -a /var/log/registry/registry.log >&2)"

curl -sS --fail --retry-delay 1 --retry 3 --retry-connrefused --connect-timeout 10 "${MGMT_NODE_DATA_IP}":5001/debug/health
echo "static registry container started" >&2
