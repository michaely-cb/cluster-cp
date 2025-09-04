#!/usr/bin/env bash

# Installer for the usernode. This should be part of a user-pkg-<version>.tar.gz
# package that is downloaded from the mgmt node.

# Set to avoid differences in customer env PATH variable defaults
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin


CFG_DIR="/opt/cerebras"
WHL_DIR="$CFG_DIR/wheels"
TOOL_DIR="$CFG_DIR/tools"
CERTS_DIR="$CFG_DIR/certs"
USER_AUTH_SECRET_DIR="/root/.cerebras"
BIN_DIR="/usr/local/bin"
PIP3_PATH=""
SYSTEM_NAMESPACE="job-operator"

set -eo pipefail

function _find_pip3() {
    local rhPip3=$(find /opt/rh -type f -name 'pip3' 2>/dev/null)
    local defaultPip3=""
    if which pip3; then
        defaultPip3=$(which pip3)
    fi
    for pip3 in $defaultPip3 $rhPip3; do
        local pipPythonVersion=$($pip3 --version | grep -o 'python [0-9]*\.[0-9]*' | awk '{print $2}')
        local majorVersion=$(echo $pipPythonVersion | cut -d. -f1)
        local minorVersion=$(echo $pipPythonVersion | cut -d. -f2)

        if [ $majorVersion -ge 3 ]; then
            if [ $minorVersion -ge 7 ]; then
                PIP3_PATH=$(which $pip3)
                break
            fi
        fi
    done
}

function _ensure_deps() {
    if ! which jq > /dev/null ; then
        echo "Error: missing dependencies: jq" >&2
        exit 1
    fi
    local jqVersion=$(jq --version | cut -d- -f2)
    local jqMajor=$(echo "$jqVersion" | cut -d. -f1)
    local jqMinor=$(echo "$jqVersion" | cut -d. -f2)
    if [ -z "$jqMajor" ] || [ -z "$jqMinor" ] || [ "$jqMajor" -lt 1 ] || [ "$jqMinor" -lt 6 ] ; then
        echo "Error: jq version 1.6 or greater is required, got '$jqVersion'" >&2
        exit 1
    fi
    if ! which unzip > /dev/null ; then
        echo "Error: missing dependencies: unzip" >&2
        exit 1
    fi

    _find_pip3
    if [ -z "$PIP3_PATH" ]; then
        echo "Warning: Can't find python version that is 3.7 or higher"
    fi

    if [ ! -f install.json ] ; then
        echo "Error: missing install.json. Check that the correct user-pkg-VERSION.tar.gz was copied from mgmt node"
        exit 1
    fi
}

function _check_mounts() {
    # Use `df` as a sanity check for verifying network mounts 
    local timeout=5
    if ! timeout $timeout df -h >/dev/null 2>&1; then 
        echo "Error: sanity check for network mounts failed ($timeout seconds) timeout. This usually indicates network issues. Check dmesg or mount statuses"
        exit 1
    fi
}

# _ip_in_range START END TARGET -> 0 if TARGET in [START,END]
_ip_in_range() {
    ipv4_to_i() {
        local IFS=.
        read -r i1 i2 i3 i4 <<< "$1"
        echo "$((i1 * 256**3 + i2 * 256**2 + i3 * 256 + i4))"
    }

    local start_ip=$(ipv4_to_i "$1")
    local end_ip=$(ipv4_to_i "$2")
    local target_ip=$(ipv4_to_i "$3")

    [ ${target_ip} -ge ${start_ip} ] && [ ${target_ip} -le ${end_ip} ]
}


function _require_root() {
    if [ "$(id -u)" != "0" ]; then
        echo "error: this script must be run as root" >&2
        exit 1
    fi
}


function _validate_install() {
    local check_csctl=$1
    local error_message=$(cat <<EOF
Error validating usernode installation.

Common issues include the management node not being reachable from the usernode,
the configuration in the usernode installer not matching the configuration of
the cluster, or the cluster software being unhealthy.

You can try:
1. Regenerating the usernode installer and reinstalling on the usernode.
2. Checking the health of the cluster by running a health check on the cluster
   management node.
3. Checking your network configuration between the usernode and the cluster.
EOF
)

    if [ ! -f "$CFG_DIR/config_v2" ] ; then
        echo "error: config file not found" >&2
        return 1
    fi

    local mgmt_ip=$(jq -r '.clusters[0].server' "$CFG_DIR/config_v2" | cut -d':' -f1)
    local has_ping=$(which ping &>/dev/null && echo "true" || echo "false")
    if $has_ping ; then
        if ! ping -w3 -c1 "$mgmt_ip" > /dev/null ; then
            echo "error: failed to ping cluster IP $mgmt_ip" >&2
            echo "$error_message" >&2
            return 1
        else
            echo "successfully pinged cluster API IP $mgmt_ip"
        fi
    fi

    if [ -f install.json ] && $has_ping ; then
        for line in $(jq -r '.managementDataIPs // {} | to_entries | .[] | [.key,.value] | @csv' install.json | tr -d \") ; do
            local ip=${line//,*/}
            local node=${line//*,/}
            if ! ping -w3 -c1 "${ip}" > /dev/null ; then
                echo "error: failed to ping ${node}/${ip}" >&2
                echo "$error_message" >&2
                return 1
            else
                echo "successfully pinged management node data IP ${node}/${ip}"
            fi
        done
    fi

    local effective_host_ip=$(ip route get "${mgmt_ip}" | sed -n -e 's/^.*src \([^ ]*\).*$/\1/p')
    if [ -f install.json ] && [[ $effective_host_ip =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]] ; then
        local ranges=$(jq -r '.reservedIpRanges[] // [] | @csv' install.json | tr -d \")
        for line in ${ranges} ; do
            local ip_start=${line//,*/}
            local ip_end=${line//*,/}
            if _ip_in_range "$ip_start" "$ip_end" "$effective_host_ip" ; then
                echo "error: usernode IP $effective_host_ip conflicts with the appliance cluster's reserved range ($ip_start to $ip_end)" >&2
                echo "usernode IP must fall outside this reserved range" >&2
                echo "$error_message" >&2
                return 1
            fi
        done
        echo "successfully validated usernode IP $effective_host_ip not overlapping with reserved ranges ${ranges//$'\n'/ }"
    fi

    if [ -n "$check_csctl" ] ; then
        namespaces=$(cat "config_v2" | jq -r '.clusters[0].namespaces[] | .name')
        # There should be only 1 TLS cert from the packaged config_v2
        # If config_v2 is empty, for loop helps exit gracefully
        for ns in $namespaces; do
            local output=$(mktemp)
            if ! "${BIN_DIR}/csctl" --namespace "$ns" get jobs &>output; then
                echo "error: csctl validation failed for namespace '$ns'"
                cat "$output" && rm -f "$output"
                error_message="Error validating csctl installation.

Common issues include:
- the embedded TLS certificate in $CFG_DIR/config_v2 not matching the ingress
  secret of the '$ns' namespace
- csctl not matching the software version of the cluster server
- the usernode network interfaces using IPs reserved for the appliance

You can try:
1. Comparing the TLS certificate value of the '$ns' namespace in
   $CFG_DIR/config_v2 with the ingress secret in the appliance.
2. Comparing csctl software version with the cluster server software version
   in the appliance.
3. Changing usernode IP assignment if error messages indicate a problem.

If csctl version aligns with the cluster server software version, please redeploy
usernode config using the usernode installer and try again."
                echo "$error_message"
                return 1
            else
                echo "successfully called csctl against namespace '$ns'"
            fi
            rm -f "$output"
        done
    fi
}

function _config_pip() {
    local extra_index_url=""
    if $PIP3_PATH config get --global global.extra-index-url &>/dev/null; then
        extra_index_url=$($PIP3_PATH config get --global global.extra-index-url)
    fi

    local pytorch_cpu_url="https://download.pytorch.org/whl/cpu"

    if [ -f "/etc/pip.conf" ]; then
        if [ "${extra_index_url}" = "${pytorch_cpu_url}" ]; then
            echo "pip config global.extra-index-url is already set"
            return
        fi
        echo "replacing pip config global.extra-index-url from '${extra_index_url}' with '${pytorch_cpu_url}'"
    else
        echo "setting pip config global.extra-index-url to '${pytorch_cpu_url}'"
    fi
    if $PIP3_PATH config set --global global.extra-index-url $pytorch_cpu_url 2>/dev/null; then
        echo "pip config global.extra-index-url is set to '${pytorch_cpu_url}'"
    fi
}

function install() {
    _require_root
    _check_mounts
    _ensure_deps

    # check that we're running in a directory that looks like a user package
    local pkg_name=$(basename $(pwd))
    if ! echo "$pkg_name" | grep -q '^user-pkg-' ; then
        echo "error: not in a user package directory" >&2
        echo "please run this script from within an user-pkg-<version> directory" >&2
        exit 1
    fi

    echo "installing $pkg"
    local skip_install_default_csctl="true"
    local enable_usernode_monitoring="false"

    # In release 2.1+, the clients only need config_v2.
    # We still place the old config and TLS cert so the older clients could benefit from this handling.
    echo "configuring node"
    if [ -f install.json ] ; then
        if [ -f "$CFG_DIR/config_v2" ]; then
            echo "Original $CFG_DIR/config_v2:"
            cat "$CFG_DIR/config_v2"
        fi

        echo "configuring certs"

        # iterate over the certs in the install.json file and install them
        mkdir -p $CERTS_DIR
        for cert in $(jq -r '.certs | keys[]' install.json) ; do
            echo "installing $CERTS_DIR/$cert"
            jq -r ".certs[\"$cert\"] | @base64d" install.json > $CERTS_DIR/$cert
        done

        # update config to point to the cert rather than embedding the cert
        # and delete the key certificateAuthorityData from the config
        config_v2_new="config_v2.new"
        if [ $(jq -r ".overwriteUsernodeConfigs" install.json) == "true" ] || [ ! -f "$CFG_DIR/config_v2" ] || [ ! -s "$CFG_DIR/config_v2" ]; then
            # If config_v2 does not exist or is empty, or the deployment was set to overwrite cert authorities, then directly copy config_v2 to destination
            cp "config_v2" "$config_v2_new"
        elif [ $(jq -r ".currentContext" "config_v2") != $(jq -r ".currentContext" "$CFG_DIR/config_v2") ]; then
            # When a user node gets moved from a cluster to another, we should no longer keep the existing config.
            cp "config_v2" "$config_v2_new"
        else
            # Merge the namespace entries by grouping by the namespace name and dedup.
            jq --arg server "$(jq -r '.clusters[0].server' config_v2)" \
              '.clusters[0].namespaces = ((input.clusters[0].namespaces + .clusters[0].namespaces) | group_by(.name) | map(.[0])) |
               .clusters[0].server = $server
            ' "$CFG_DIR/config_v2" config_v2 > "$config_v2_new"

            # TODO: Clean up namespace cert authorities that no longer work, possibly due to namespace was deleted in the appliance
        fi

        total_namespaces=$(jq '.clusters[0].namespaces | length' "$config_v2_new")
        # Filter out system namespace when other user namespaces are present
        if [ "$total_namespaces" -gt 1 ]; then
            if jq -e --arg ns "$SYSTEM_NAMESPACE" '.clusters[0].namespaces[] | select(.name == $ns)' "$config_v2_new" >/dev/null; then
                echo "info: system namespace ($SYSTEM_NAMESPACE) found in config_v2, filtering it out for user node."
                jq --arg ns "$SYSTEM_NAMESPACE" '.clusters[0].namespaces |= map(select(.name != $ns))' "$config_v2_new" > "$config_v2_new.tmp" && mv "$config_v2_new.tmp" "$config_v2_new"
            fi
        fi

        echo "installing $CFG_DIR/config_v2"
        if [ ! -s "$config_v2_new" ]; then
            echo "error: $config_v2_new is empty, abort config replacement"
            exit 1
        else
            chmod 644 "$config_v2_new"
            # Leave a backup for debugging purposes
            cp "$config_v2_new" "$config_v2_new".bk
            mv -f "$config_v2_new" "$CFG_DIR/config_v2"
        fi

        # Add the domains for this cluster to usernode's /etc/hosts.
        # Note: avoid using sed in-place as creating a new inode breaks KIND-based usernode
        #   e.g. sed: cannot rename /etc/sedW49GtT: Device or resource busy
        # This is probably because KIND mounts /etc/hosts as a read-only file to manipulate this node's networking.
        cp /etc/hosts /tmp/hosts
        for domain in $(jq -r '.domains[]' install.json) ; do
            sed -i "/ $domain /d" /tmp/hosts
            sed -i "/ $domain\$/d" /tmp/hosts
        done
        local clusterIp=$(jq -r '.ip' install.json)
        sed -i "/^$clusterIp /d" /tmp/hosts
        echo "$clusterIp $(jq -r '.domains | @csv' install.json | tr ',' ' ' | tr -d '"')" >> /tmp/hosts
        cat /tmp/hosts > /etc/hosts

        if [ $(jq -r ".updateConfigOnly" install.json) == "false" ]; then
            if [ ! -d "$WHL_DIR" ]; then
                mkdir -p "$WHL_DIR"
            fi

            new_whl_files=$(find . -maxdepth 1 -name '*.whl')
            old_whl_files=$(find "$WHL_DIR" -maxdepth 1 -name '*.whl')
            if echo "$new_whl_files" | grep -q '.'; then
                echo "Backing up old wheels to $WHL_DIR/old"
                rm -rf "$WHL_DIR/old"
                mkdir -p "$WHL_DIR/old"
                echo "$old_whl_files" | xargs -n1 -r -I {} mv {} "$WHL_DIR/old"
                echo "Copying wheels to $WHL_DIR"
                echo "$new_whl_files" | xargs -I {} cp {} "$WHL_DIR"
            else
                echo "No .whl files found. Doing nothing."
            fi
        fi

        skip_install_default_csctl="$(jq -r '.skipInstallDefaultCsctl' install.json)"
        enable_usernode_monitoring="$(jq -r '.enableUsernodeMonitoring' install.json)"
    fi

    mkdir -p $TOOL_DIR
    local installed_csctl=""
    if [ -f ./csctl_executable ]; then
        echo "installing csctl in ${BIN_DIR}"
        src_csctl="./csctl_executable"

        # "csctl version 2.0.2-202310292240-20-0d7a2e0e+0d7a2e0eec" -> "2.0"
        # "csctl version 0.0.0-johndoe+c481ba92cd" -> "0.0"
        chmod 755 "$src_csctl"
        csctl_version=$("$src_csctl" --version | awk '{ print $3 }' | cut -d. -f1-2)

        # We will also install "csctl" if instructed from the install.json.
        # We only perform validation if the default "csctl" executable was installed.
        if [ "$skip_install_default_csctl" == "false" ]; then
            cp -p "$src_csctl" "${BIN_DIR}/csctl"
            installed_csctl="1"
        fi
        cp -p "$src_csctl" "${BIN_DIR}/csctl${csctl_version}"

        echo "copying csctl docs to $TOOL_DIR"
        for doc in csctl*.md; do
            if [ -e "$doc" ]; then
                base="${doc%.md}"
                newdoc="${base}-${csctl_version}.md"
                cp "$doc" "$TOOL_DIR/${newdoc}"
            fi
        done

        # Clean up existing documentation
        if [ -f "$TOOL_DIR/csctl.md" ]; then
            rm "$TOOL_DIR/csctl.md"
        fi
        if [ -f "$TOOL_DIR/csctl-reference.md" ]; then
            rm "$TOOL_DIR/csctl-reference.md"
        fi
    else
        echo "installer is missing csctl binary, skipping csctl install" >&2
    fi
    
    if [ "$enable_usernode_monitoring" == "true" ]; then
        setup_usernode_monitoring
    fi

    if [ -f get-cerebras-token ]; then
        local token_bin_exists=false
        if [ -f "$BIN_DIR/get-cerebras-token" ]; then
            chmod u+x get-cerebras-token
            local old_version=$($BIN_DIR/get-cerebras-token version)
            local new_version=$(./get-cerebras-token version)
            # Before 2.0, there is no version support in get-cerebras-token. Assuming it is v1.
            if [[ $old_version == Token* ]]; then
                old_version="version v1"
            fi

            if [ "$old_version" = "$new_version" ]; then
              echo "get-cerebras-token already exists in $BIN_DIR"
              token_bin_exists=true
            fi
        fi
        if ! $token_bin_exists; then
            # Rename command seems to handle `text file busy` error better than cp or mv.
            echo "installing get-cerebras-token in $BIN_DIR"
            cp get-cerebras-token "$BIN_DIR/get-cerebras-token.copy"
            chmod 4751 "$BIN_DIR/get-cerebras-token.copy"
            mv -f "$BIN_DIR/get-cerebras-token.copy" "$BIN_DIR/get-cerebras-token"
        fi
        rm -f get-cerebras-token
    fi

    if [ -f user-auth-secret ]; then
        echo "saving user-auth-secret"
        mkdir -p "$USER_AUTH_SECRET_DIR"
        chmod 0750 "$USER_AUTH_SECRET_DIR"
        if [ -f "$USER_AUTH_SECRET_DIR/user-auth-secret" ] && cmp user-auth-secret "$USER_AUTH_SECRET_DIR/user-auth-secret" 1>/dev/null; then
            echo "user-auth-secret already installed"
        else
            cp user-auth-secret "$USER_AUTH_SECRET_DIR/user-auth-secret.copy"
            chmod 0640 "$USER_AUTH_SECRET_DIR/user-auth-secret.copy"
            mv -f "$USER_AUTH_SECRET_DIR/user-auth-secret.copy" "$USER_AUTH_SECRET_DIR/user-auth-secret"
        fi
        rm -f user-auth-secret
    fi

    if [ -f /etc/security/limits.conf ]; then
        echo "configuring max open files in /etc/security/limits.conf"

        # PyTorch opens a large number of file handles when checkpointing
        cp /etc/security/limits.conf /etc/security/limits.conf.original
        sed -i '/.*\(hard\|soft\)[[:space:]]\+nofile.*/d' /etc/security/limits.conf
        echo "* hard nofile 20000" >> /etc/security/limits.conf
        echo "* soft nofile 20000" >> /etc/security/limits.conf

        # PyTorch and Tensorflow can unpredictably use a large number of threads
        sed -i '/.*\(hard\|soft\)[[:space:]]\+nproc.*/d' /etc/security/limits.conf
        echo "* hard nproc 16384" >> /etc/security/limits.conf
        echo "* soft nproc 16384" >> /etc/security/limits.conf
    fi

    # Only config pip when pip3 exists. In some unittests, we might not have pip3 available.
    if [ -n "$PIP3_PATH" ]; then
        _config_pip
    fi
    _validate_install $installed_csctl
}

function setup_usernode_monitoring() {
    echo "Setting up usernode monitoring..."

    local is_kind="false"
    if [ -f install.json ]; then
        is_kind="$(jq -r '.isKind' install.json)"
    fi

    if [ "$is_kind" == "true" ]; then
        echo "Skipping monitoring setup on KIND-based usernode."
        return
    fi

    if [ -f ./node_exporter ]; then
        echo "installing node_exporter in ${BIN_DIR}"
        src_node_exporter="./node_exporter"

        # With running node_exporter service, cp results in "Text file busy" error.
        if systemctl is-active --quiet node_exporter.service; then
            systemctl stop node_exporter.service
        fi

        chmod 755 "$src_node_exporter"
        cp -p "$src_node_exporter" "${BIN_DIR}/node_exporter"

        cat > /etc/systemd/system/node_exporter.service << 'EOF'
[Unit]
Description=Prometheus Node Exporter
Wants=network-online.target
After=network-online.target

[Service]
ExecStart=/usr/local/bin/node_exporter \
  --collector.textfile.directory /var/lib/node_exporter/textfile_collector \
  --collector.cpu.info \
  --collector.processes

[Install]
WantedBy=multi-user.target
EOF
        systemctl daemon-reload
        systemctl enable --now node_exporter.service
    else
        echo "installer is missing node_exporter binary, skipping node_exporter install" >&2
    fi

    if [ -f ./session_exporter.sh ]; then
        echo "installing session_exporter in ${BIN_DIR}"
        src_session_exporter="./session_exporter.sh"

        chmod 755 "$src_session_exporter"
        cp -p "$src_session_exporter" "${BIN_DIR}/session_exporter.sh"

        cat > /etc/systemd/system/session_exporter.service << 'EOF'
[Unit]
Description=Cerebras session membership exporter
After=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/session_exporter.sh

[Install]
WantedBy=multi-user.target
EOF
        cat > /etc/systemd/system/session_exporter.timer << 'EOF'
[Unit]
Description=Run session_exporter every minute

[Timer]
OnBootSec=30s
OnUnitActiveSec=1m

[Install]
WantedBy=timers.target
EOF
        systemctl daemon-reload
        systemctl enable --now session_exporter.timer
    else
        echo "installer is missing session_exporter.sh, skipping session_exporter install" >&2
    fi

    echo "Usernode monitoring setup complete"
}


function main() {
    local usage=$(cat <<EOF
$0
    An installer for install Cerebras usernode packages. This script should be
    run as root in the directory containing usernode artifacts which were copied
    from the mgmt node.

    Usage:

        install (or no args): install the package

        help: prints this help message

        enable-usernode-monitoring: sets up monitoring stack to begin collecting metrics
EOF
)

    local cmd="$1"
    if [ "$cmd" = "help" ] ; then
        echo "$usage"
        exit
    elif [ "$cmd" = "install" ] || [ -z "$cmd" ] ; then
        mkdir -p $CFG_DIR
        exec 200>/$CFG_DIR/.usernode_install_lock
        flock -w 60 200 || { echo "Unable to lock file $CFG_DIR/.usernode_install_lock, exiting"; exit 1; }
        install "$@"
        exit
    elif [ "$cmd" = "enable-usernode-monitoring" ] ; then
        setup_usernode_monitoring
        exit
    fi

    echo "$usage"
    exit 1
}

main "$@"
