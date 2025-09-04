#!/bin/sh

# TODO: consolidate this utils with the one in prepull
CURRENT_DIRECTORY="$( dirname -- "$0"; )"
source ${CURRENT_DIRECTORY}/shell-utils.sh

hosts_config="/etc/containerd/certs.d/${PRIVATE_REGISTRY_SERVER_HOSTNAME}/hosts.toml"
auth_option="--hosts-dir /etc/containerd/certs.d"

CTR="ctr -n k8s.io"
CTR_RETRIES=3
if [ -n "${CTR_DEBUG}" ]; then
  CTR="${CTR} --debug"
fi

tmp_file="/tmp/out"
flush_stderr() {
  if [ -f ${tmp_file} ]; then
    cat ${tmp_file} >&2
  fi
}

resolve_registry_endpoint() {
  # Example hosts.toml
  # [host."x.x.x.x:5000"] -> x.x.x.x:5000
  grep "^\[host\." ${hosts_config} | head -1 | awk -F'[][]' '{print $2}' | cut -d '"' -f2
}

resolve_registry_ip() {
  # Example hosts.toml
  # [host."x.x.x.x:5000"] -> x.x.x.x
  resolve_registry_endpoint | cut -d: -f1
}

resolve_registry_crt() {
  # Example hosts.toml
  # [host."x.x.x.x:5000"]
  #   capabilities = ["pull", "resolve", "push"]
  #   ca = "/opt/cerebras/certs/registry_tls.crt"
  # Output: /opt/cerebras/certs/registry_tls.crt
  grep -A2 "^\[host\." ${hosts_config} | tail -1 | cut -d '"' -f2
}

resolve_base_image_name_tag() {
  # ecr-org/a/b/c/image:x-y-z -> a/b/c/image:x-y-z
  echo "${BASE_IMAGE#*/}"
}

resolve_base_image_tag() {
  # some-org:5000/a/b/c/image:x-y-z -> x-y-z
  echo "${BASE_IMAGE##*:}"
}
