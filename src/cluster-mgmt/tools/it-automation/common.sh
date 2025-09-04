#!/bin/bash

# Must execute from within monolith
if [ -z "$GITTOP" ]; then
    echo "please run from a monolith checkout"
    exit 1
fi

# check DEPLOY_ROOT_PASS
if [[ -z $DEPLOY_ROOT_PASS ]]; then
    echo "please set DEPLOY_ROOT_PASS envvar"
    exit 1
fi

get_devinfra_db_env() {
    if [[ "$1" =~ ^system ]]; then
        export CLUSTER_NAME="mb-$1"
    else
        export CLUSTER_NAME=$1
    fi
    echo "using multibox system id=$CLUSTER_NAME"
    export CLUSTER_JSON=$(curl -s http://systems-db.devinfra.cerebras.aws/api/v1/search/multiboxes?_id=$CLUSTER_NAME)
    export MGMTNODE="$(jq -r '.data[].management_hosts[]' <<<$CLUSTER_JSON | head -n1)"
    export USERNODE="$(jq -r '.data[].user_hosts[]' <<<$CLUSTER_JSON)"
    export SYSTEM="$(jq -r '.data[].systems[]' <<<$CLUSTER_JSON)"

    echo "Extracted values from devinfra DB:"
    echo "MGMTNODE=$MGMTNODE"
    echo "USERNODE=$USERNODE"
    echo "SYSTEM=$SYSTEM"

    export sshopts="-oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -oConnectTimeout=10"
    export sshmgmt="sshpass -p $DEPLOY_ROOT_PASS ssh $sshopts root@$MGMTNODE"
    export scpmgmt="sshpass -p $DEPLOY_ROOT_PASS scp $sshopts"
    export sshuser="sshpass -p $DEPLOY_ROOT_PASS ssh $sshopts root@$USERNODE"
    export sshsys="ssh $sshopts root@$SYSTEM"
    export scpsys="scp $sshopts"
}

retry() {
  local retries=$1
  local delay=$2
  shift 2  # Remove first two args (retries/delay), leaving the command

  local attempt=1
  until "$@"; do
    if (( attempt >= retries )); then
      echo "Command \"$@\" failed after $retries attempts" >&2
      return 1
    fi
    echo "Attempt $attempt failed for command \"$@\". Retrying in $delay seconds..." >&2
    sleep "$delay"
    attempt=$(( attempt + 1 ))
  done
}
