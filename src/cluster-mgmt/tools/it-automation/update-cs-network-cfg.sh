#!/bin/bash

# Finds the CS2 network-cfg.json for the cluster and loads it to the CS2.
#
# Additionally, this updates the cluster network_config.json to replace any
# instance of systemfXXX with the SYSTEM configured for this cluster.
#
# USAGE:
#    update-cs-network-cfg.sh CLUSTER_NAME [--reset]
#
# If --reset is set, it standby/activate cycles the system. Do not use --reset
# if you plan on updating the CS2 image later since it is redundant.
#
# If the CS2 network-cfg.json is not found, this script will NOT error out.
#
# This is a short term fix. In the long run, there is a PB tool that will handle
# these changes.

cd "$(dirname "$0")"
source common.sh
set -eox pipefail

get_devinfra_db_env "$1"

if [ -z "$SYSTEM" ] || ! [[ "$SYSTEM" =~ ^(systemf|xs) ]] ; then
    echo "System in devinfraDB doesn't match pattern '^(systemf|xs)' actual='$SYSTEM', exiting"
    exit 0
fi

# This isn't an offical path, I just copied it one-off. Generally there will
# be one of these files per bring-up cluster
CS_NETWORK_PATH="/opt/cerebras/cluster/cs-network-cfg.json"

# update_network_cfg CLUSTER_NAME SYSTEM_NAME - swaps the system and cluster name in the network file
update_network_cfg() {
    CLUSTER_NETWORK_PATH=/opt/cerebras/cluster/network_config.json
    if ! $sshmgmt test -f $CLUSTER_NETWORK_PATH ; then
        echo "warning: file $CLUSTER_NETWORK_PATH was not present on the mgmt node, skipping update network reconfig"
        return 0
    fi
    local cluster_name=$1
    local system_name=$(cut -d. -f1 <<<"$2")

    # check if the system name is currently present before swapping
    if $sshmgmt "jq -r '(.systems[] // []).name' $CLUSTER_NETWORK_PATH" | grep -q -w "$system_name" ; then
        echo "System name $system_name already present in $CLUSTER_NETWORK_PATH, skipping update"
        return 0
    fi

    mkdir -p tmp
    trap 'rm -rf tmp' RETURN EXIT
    # make a backup
    $sshmgmt cp -f $CLUSTER_NETWORK_PATH "${CLUSTER_NETWORK_PATH}.$( date +%Y%m%d_%H%M%S )"
    $scpmgmt root@$MGMTNODE:$CLUSTER_NETWORK_PATH tmp/network_config.json

    # update the system name and maybe IP in the network config
    jq --arg name $system_name 'if .systems != null then (.systems[].name = $name) else . end' tmp/network_config.json > tmp/network_config.json.tmp
    mv -f tmp/network_config.json.tmp tmp/network_config.json
    jq --arg name $system_name 'if .system_connections != null then (.system_connections[].system_name = $name) else . end' tmp/network_config.json > tmp/network_config.json.tmp
    mv -f tmp/network_config.json.tmp tmp/network_config.json

    local cs_mgmt_ip=$($sshsys hostname -i || true)
    if [ -n "$cs_mgmt_ip" ] ; then
        jq --arg ip $cs_mgmt_ip 'if .systems != null then (.systems[].management_address=$ip) else . end' tmp/network_config.json > tmp/network_config.json.tmp
        mv -f tmp/network_config.json.tmp tmp/network_config.json
    fi

    # update the cluster name in the network config
    jq --arg name $cluster_name '.name=$name' tmp/network_config.json > tmp/network_config.json.tmp
    mv -f tmp/network_config.json.tmp tmp/network_config.json

    $scpmgmt tmp/network_config.json root@$MGMTNODE:$CLUSTER_NETWORK_PATH
}

# update_cs_network_cfg - updates cs network config to:
#   1/ default config at /opt/cerebras/cluster/cs-network-config.json if present on mgmt node
#   2/ deployment manager cs network config if a single one is found under /opt/cerebras/cluster-deployment
update_cs_network_cfg() {
    local cs_net=${CS_NETWORK_PATH}
    if ! $sshmgmt test -f "$CS_NETWORK_PATH"; then
        # Search for alternative CS network config files
        $sshmgmt mkdir -p /opt/cerebras/cluster-deployment
        CS_NETWORK_CANDIDATES=$($sshmgmt find /opt/cerebras/cluster-deployment -name 'network-cfg.json')
        CS_NETWORK_FILE_COUNT=$(echo "$CS_NETWORK_CANDIDATES" | wc -w)

        # Handle different scenarios based on the candidate count
        case "$CS_NETWORK_FILE_COUNT" in
            0)
                echo "Default CS_NETWORK_PATH=${CS_NETWORK_PATH} was not found and no search candidate found. Skipping CS network config."
                return 0
                ;;
            1)
                cs_net="${CS_NETWORK_CANDIDATES}"
                echo "Found CS_NETWORK_PATH=${CS_NETWORK_PATH}"
                ;;
            *)
                echo "Error: Multiple CS_NETWORK_PATH found. Please copy the correct CS network file to ${CS_NETWORK_PATH}"
                echo "Options:"
                echo "$CS_NETWORK_CANDIDATES"
                return 1
                ;;
        esac
    fi

    cs_net_json=$(mktemp)
    $sshmgmt cat "$cs_net" > "$cs_net_json"
    $scpsys "$cs_net_json" "root@$SYSTEM:/tmp/network-cfg.json.$CLUSTER_NAME"
    if $sshsys diff -q "/tmp/network-cfg.json.$CLUSTER_NAME" /var/lib/tftpboot/network-cfg.json ; then
        echo "Existing system config matches target config"
    else
        echo "new network-cfg.json:"
        cat "$cs_net_json"
        $sshsys cp -f /var/lib/tftpboot/network-cfg.json /var/lib/tftpboot/network-cfg.json.old
        $sshsys cp -f "/tmp/network-cfg.json.$CLUSTER_NAME" /var/lib/tftpboot/network-cfg.json
    fi
}

# call system reset if the reported IPs of the system differ from the IPs described
# in the network-cfg of the system
reset_system_if_network_changed() {
    # double check that the system actually has been reset to have these addresses
    local ip_actual ip_desired cs_net_json
    ip_actual=$(mktemp)
    ip_desired=$(mktemp)

    $sshsys cs network show --output-format json | jq -r '.data[] | select(.ifaceName != "CTRL") | .ipAddress' | sort > "$ip_actual"
    cs_net_json=$($sshsys cat /var/lib/tftpboot/network-cfg.json)
    jq -r '.data | to_entries | .[] | .value.inet | select(. != null)' <<< "$cs_net_json" | sort > "$ip_desired"

    if diff -q "$ip_actual" "$ip_desired" ; then
        echo "Existing system ips match desired ips, not reseting"
        return 0
    fi

    echo "Actual IPs and desired IPs differ"
    echo "Actual IPs:"
    cat "$ip_actual"
    echo "Desired IPs:"
    cat "$ip_desired"

    python3 reset-system.py "$SYSTEM"
}

update_network_cfg "${1}" "$SYSTEM"
update_cs_network_cfg
if [ "$2" == "--reset" ] ; then
    reset_system_if_network_changed
fi