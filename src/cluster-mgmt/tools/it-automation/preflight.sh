#!/bin/bash

# Preflight check for single box cluster install.
#
# * Checks for node ssh/scp capability for all nodes in devinfraDB entry for this cluster
#   Reasoning for scp call is seen cases where ssh is reachable but scp calls are very slow
#   due to file system errors, requiring a node reboot.
# * Checks nodes can reach mgmt node's data IP. Catches issues with data interfaces down
# * Checks if system is in upgrading mode - awaits completion of upgrade for 60 minutes if started.
#   This avoids issues where a failed DeployCluster call which initiated system upgrade does not
#   fail a later step to activate the system.
#
# USAGE:
#    preflight.sh CLUSTER_NAME
#
# EXAMPLE:
#    preflight.sh mb-wsperfdrop1
#

cd "$(dirname "$0")"
source common.sh

get_devinfra_db_env "$1"

check_nodes() {
    local rand_file_path=$(mktemp)
    local rand_file=$(basename "$rand_file_path")
    dd if=/dev/urandom of="$rand_file_path" bs=1M count=64

    local mgmt_data_ip_json=$($sshmgmt "ip -j a")
    # assume for colo, 100G addrs start with 10. and interface names start with "e"
    local mgmt_data_ip=$(jq -r '.[] | select((.addr_info[0].local // "") | test("^10.")) |
                                      select((.ifname // "") | test("^e")) |
                                      .addr_info[0].local' <<<"$mgmt_data_ip_json" |
        head -n1 || true)

    local errors=""
    print_err() {
        echo "$1"
        errors="${errors}\n$1"
    }

    for node in $(jq -r '.data[] | [(.memoryx_hosts//[])[], (.management_hosts//[])[], (.worker_hosts//[])[], (.user_hosts//[])[]] | @tsv' <<<$CLUSTER_JSON); do
        if ! retry 3 1 timeout 30 sshpass -p "$DEPLOY_ROOT_PASS" scp $sshopts "$rand_file_path" "root@$node:/tmp/$rand_file"; then
            print_err "error: $node failed scp call, check node health"
            continue
        else
            echo "$node can execute scp"
        fi

        if ! retry 3 1 sshpass -p "$DEPLOY_ROOT_PASS" ssh $sshopts "root@$node" "rm -f /tmp/$rand_file"; then
            print_err "error: $node failed ssh call, check node health"
            continue
        else
            echo "$node can execute ssh"
        fi

        if ! retry 3 1 sshpass -p "$DEPLOY_ROOT_PASS" ssh $sshopts "root@$node" \
            "if ! which yq; then wget https://github.com/mikefarah/yq/releases/download/v4.27.5/yq_linux_amd64 -O /usr/local/bin/yq && chmod +x /usr/local/bin/yq; fi"; then
            print_err "error: $node failed to wget yq"
            continue
        else
            echo "$node wget yq if needed"
        fi

        if ! retry 3 1 sshpass -p "$DEPLOY_ROOT_PASS" ssh $sshopts "root@$node" "which yq"; then
            print_err "error: $node failed yq check, check whether yq is installed"
            continue
        else
            echo "$node has yq installed"
        fi

        if ! retry 3 1 sshpass -p "$DEPLOY_ROOT_PASS" ssh $sshopts "root@$node" 'timeout 5m sh -c "if ! df -t nfs -t nfs4 /cb/tests ; then mount -a ; df -t nfs -t nfs4 /cb/tests ; fi"'; then
            print_err "error: $node failed nfs mount check, check node nfs mounts"
            continue
        else
            echo "$node has /cb/tests nfs mounted"
        fi

        if [ -n "$mgmt_data_ip" ]; then
            if ! retry 3 1 sshpass -p "$DEPLOY_ROOT_PASS" ssh $sshopts "root@$node" "ping -c1 -W3 $mgmt_data_ip"; then
                print_err "error: $node failed ping mgmt node data ip $mgmt_data_ip test, check data interface health"
                continue
            else
                echo "$node can execute ping mgmt data ip"
            fi
        fi
    done

    rm -f "$rand_file_path"

    if [ -n "$errors" ]; then
        echo -e "$errors"
        return 1
    fi
    echo "node preflight passed"
}

function check_system() {
    # ensure system completes any ongoing upgrade before proceeding
    local timeout_time swstatus
    timeout_time=$((SECONDS + (60 * 60)))
    while [[ "$SECONDS" -lt $timeout_time ]]; do
        swstatus=$($sshsys cs software show --output-format json)
        swstatus=$(jq -r '.upgradeEvents[0].type // "Unknown"' <<<"$swstatus")
        if [ "$swstatus" == "EventUpgradeStarted" ]; then
            echo "$SYSTEM upgrade in progress, status=$swstatus"
            sleep 10
        else
            echo "$SYSTEM status = $swstatus, system preflight passed"
            return 0
        fi
    done
    echo "timed out waiting for upgrade status, status:"
    $sshsys cs software show
    return 1
}

set -e

check_nodes
check_system
echo "preflight passed"
