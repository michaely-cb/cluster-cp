#!/usr/bin/env bash

read -r -d '' USAGE_TEXT << EOM
Authenticate and Authorize an external user

usage: $0 {USER_NAME} read

Script to add authentication and read-only authorization for an external user.

This script should be run on the management node. The generated artifact
is a {USER_NAME}-config file, located on the directory this script is invoked.

Example Usage

  # authenticate and authorize 'my_user' with read access
  $0 my_user read
EOM

generate_certs() {
  user=$1

  # Create a private key
  openssl genrsa -out "${user}.key" 2048 && chmod 0600 ${user}.key

  # Generate a Certificate Signing Request (CSR) using the created key
  openssl req -new -key "${user}.key" -out "${user}.csr" -subj "/CN=${user}" && \
    chmod 0600 ${user}.csr

  # Generate a self-signed certificate using the CSR
  openssl x509 -req \
    -in "${user}.csr" \
    -CA /etc/kubernetes/pki/ca.crt \
    -CAkey /etc/kubernetes/pki/ca.key \
    -CAcreateserial \
    -out "${user}.crt" \
    -days 3600 && \
    chmod 0600 ${user}.crt
}

generate_read_roles() {
  user=$1 
  # generate ClusterRole/ClusterRoleBinding for read-only access
cat <<EOM > ${user}-roles.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: user:${user}
rules:
- apiGroups:
  - '*'
  resources:
  - 'nodes'
  - 'pods'
  - 'services'
  - 'endpoints'
  - 'events'
  - 'daemonsets'
  - 'deployments'
  - 'statefulsets'
  - 'ingresses'
  - 'wsjobs'
  - 'jobs'
  verbs:
  - "get"
  - "list"
  - "watch"
- nonResourceURLs:
  - '*'
  verbs:
  - "get"
  - "list"
  - "watch"
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: user:${user}
subjects:
- kind: User
  name: ${user}
  apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: ClusterRole
  name: user:${user}
  apiGroup: rbac.authorization.k8s.io
EOM

  kubectl apply -f ${user}-roles.yaml
}

generate_read_config() {
  user=$1
  key_file=${user}.key
  crt_file=${user}.crt

  local context_name=$(kubectl config current-context)

  local cluster_name=$(kubectl config view --raw \
    -o=go-template='{{range .contexts}}{{if eq .name "'''${context_name}'''"}}{{ index .context "cluster" }}{{end}}{{end}}')

  local cluster_ca_cert=$(kubectl config view --raw \
    -o=go-template='{{range .clusters}}{{if eq .name "'''${cluster_name}'''"}}"{{with index .cluster "certificate-authority-data" }}{{.}}{{end}}"{{ end }}{{ end }}')

  local cluster_endpoint=$(kubectl config view --raw \
    -o=go-template='{{range .clusters}}{{if eq .name "'''${cluster_name}'''"}}{{ .cluster.server }}{{end}}{{ end }}')

  local user_key=$(cat ${key_file} | base64 --wrap=0)
  local user_crt=$(cat ${crt_file} | base64 --wrap=0)

  touch ${user}-config
  chmod 0600 ${user}-config

  cat << EOM > ${user}-config
apiVersion: v1
kind: Config
current-context: ${user}-context
contexts:
- name: ${user}-context
  context:
    cluster: ${cluster_name}
    user: ${user}
    namespace: job-operator
clusters:
- name: ${cluster_name}
  cluster:
    certificate-authority-data: ${cluster_ca_cert}
    server: ${cluster_endpoint}
users:
- name: ${user}
  user:
    client-certificate-data: ${user_crt}
    client-key-data: ${user_key}
EOM
  rm ${user}.key ${user}.csr ${user}.crt
}

validate_config() {
  user=$1
  if ! kubectl get pods --kubeconfig=${user}-config &> /dev/null; then
    echo "ERROR: ${user}-config is not valid."
    return 1
  else
    echo "SUCCESS: ${user}-config is validated."
    return 0
  fi
}

authz_read() {
  user=$1

  generate_certs ${user}

  generate_read_roles ${user}
  generate_read_config ${user}

  if ! validate_config ${user}; then
    return 1
  fi

  return 0
}

check_prerequisites() {
  if ! which openssl &> /dev/null; then
    echo "ERROR: 'openssl' is not installed"
    return 1
  fi

  if ! which kubectl &> /dev/null; then
    echo "ERROR: 'kubectl' is not installed"
    return 1
  fi
  return 0
}

main() {
  if [ "$#" != "2" ] ; then echo "$USAGE_TEXT" ; exit ; fi

  if ! check_prerequisites; then
    exit 1
  fi

  user=$1
  access=$2

  case "${access}" in
    read)
      if ! authz_read ${user}; then
        exit 1
      fi
    ;;
    *)
      echo "unknown access request '$access', options: read"
      echo "$USAGE_TEXT"
      exit 1
  esac
}

main $@
