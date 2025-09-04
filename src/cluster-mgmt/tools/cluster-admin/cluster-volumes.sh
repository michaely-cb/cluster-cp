#!/usr/bin/env bash

VOLUME_CM="cluster-server-volumes"
: ${NAMESPACE="job-operator"}
: ${SYSTEM_NAMESPACE="job-operator"}
: ${KUBECONTEXT=""}
read -r -d '' USAGE_TEXT << EOM
Cluster Volumes Tool

usage: $0 [get|put|delete|copy-from|test] {VOLUME_NAME} {FLAGS}

Admin tool to view, create, update, delete, copy user managed volumes on the Cerebras
multibox cluster.

Executing this script requires that you are on the management node of the
Cerebras multibox cluster. SSH into the management node and copy this file
there to execute.

Example Usage

  # lists all volumes
  $0 get

  # Creates or updates a volume with a particular NFS server address and in
  # readonly mode.
  $0 put myvol --container-path /cb/share/mldata --server 10.10.10.10 --server-path /share/mldata --readonly

  # Creates or updates a volume with a particular NFS server address and labels
  # the volume with '"allow-venv": "true"'. When user venv copying is needed,
  # the cluster server looks up volumes with this label and uses one of them
  # as the copying destination.
  $0 put myvol --container-path /cb/share/mldata --server 10.10.10.10 --server-path /share/mldata --allow-venv

  # Deletes a volume. Note this does not delete any data and any job that is
  # currently running or queued to run that has referenced this volume will
  # continue to run using the removed volume.
  $0 delete myvol

  # Tests a volume can be mounted by a container on the cluster.
  $0 test myvol

  # Copies all volumes to a namespace from source namespace.
  $0 copy-from <source-namespace>

  # For Namespace usage, add [NAMESPACE=<name>] when running the above cmds.
  NAMESPACE=test $0 get
EOM

echo "running in NAMESPACE: ${NAMESPACE} and KUBECONTEXT: ${KUBECONTEXT}"
if [ -n "${KUBECONTEXT}" ] ; then
    echo "running with KUBECONTEXT: ${KUBECONTEXT}"
fi

_check_prereqs() {
    if ! which kubectl &>/dev/null ; then
        echo 'missing prereq: kubectl. Are you running on the management node?'
        exit 1
    fi

    if ! which jq &>/dev/null ; then
        echo 'missing prereq: jq. Are you running on the management node?'
        exit 1
    fi

    # find major.minor version greater than 1.5 = https://regex101.com/r/xVnUqo/1
    if ! [[ $(jq --version | cut -d'-' -f2) =~ ^(1\.([6-9]{1}|[1-9]{1}[0-9]+))|([2-9]{1}[0-9]*\..*)$ ]] ; then
        echo 'jq version too low: requires version 1.6 or greater, got:' $(jq --version | cut -d'-' -f2)
        exit 1
    fi

    KUBE_ERR=$(kubectl --context=$KUBECONTEXT -n $NAMESPACE get cm 2>&1)
    if [ "$?" != "0" ] ; then
        echo "unable to execute kubectl: $KUBE_ERR"
        echo 'Are you running on the management node?'
        exit 1
    fi

    return 0
}


# _ip_in_range START END TARGET -> 0 if TARGET in [START,END]
_ip_in_range() {
    local start_ip=$1
    local end_ip=$2
    local target_ip=$3
    python3 -c "
import sys
from ipaddress import ip_address

start, end, target = sys.argv[1:4]
if ip_address(start) <= ip_address(target) <= ip_address(end):
    sys.exit(0)
sys.exit(1)
" "$start_ip" "$end_ip" "$target_ip"
}


# _ip_in_cidr CIDR TARGET -> 0 if TARGET in CIDR
_ip_in_cidr() {
    local cidr=$1
    local target_ip=$2
    python3 -c "
import sys
from ipaddress import ip_address, ip_network

cidr, target = sys.argv[1:3]
if ip_address(target) in ip_network(cidr):
    sys.exit(0)
sys.exit(1)
" "$cidr" "$target_ip"
}


# _resolve_ip TARGET translates HOSTNAME/IP target to an IPv4 IP address or outputs nothing if not possible
_resolve_ip() {
    local maybe_ip=$(getent ahostsv4 "$1" | grep STREAM | awk '{print $1}')
    if [[ $maybe_ip =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
        echo "$maybe_ip"
    fi
}

# _virtual_range_check TARGET returns 0 if TARGET does NOT fall in any of the multus' virtual ranges
#   or within the cilium range
_virtual_range_check() {
    local target virtual_ranges virtual_start virtual_end cilium_range
    target=$1
    v2GroupsCount=$(kubectl get cm cluster -ojsonpath='{.data.clusterConfiguration\.yaml}' |
                         yq -ojson |
                         jq -r '.v2Groups | length')
    if [ "$v2GroupsCount" -gt 0 ]; then
        virtual_ranges=$(kubectl get cm cluster --context=$KUBECONTEXT -n"$SYSTEM_NAMESPACE" -ojsonpath='{.data.clusterConfiguration\.yaml}' |
            yq -ojson |
            jq -r '.v2Groups[] | .vlans[] | [.virtualStart, .virtualEnd] | @csv' |
            grep -v 'null' |
            tr -d '"' |
            sort |
            uniq)
    else
        virtual_ranges=$(kubectl get cm cluster --context=$KUBECONTEXT -n"$SYSTEM_NAMESPACE" -ojsonpath='{.data.clusterConfiguration\.yaml}' |
            yq -ojson |
            jq -r '.groups[] | [.switchConfig.virtualStart, .switchConfig.virtualEnd] | @csv' |
            grep -v 'null' |
            tr -d '"' |
            sort |
            uniq)
    fi
    for range in $virtual_ranges ; do
        virtual_start=$(cut -d, -f1 <<<"$range")
        virtual_end=$(cut -d, -f2 <<<"$range")
        if _ip_in_range "$virtual_start" "$virtual_end" "$target" ; then
            echo "$target falls within multus range ${virtual_start} to ${virtual_end}" >&2
            return 1
        fi
    done
    cilium_range=$(kubectl get cm -n kube-system cilium-config -ojsonpath='{.data.cluster-pool-ipv4-cidr}' || true)
    if [ -n "$cilium_range" ] ; then
        if _ip_in_cidr "$cilium_range" "$target" ; then
            echo "$target falls within cilium range ${cilium_range}" >&2
            return 1
        fi
    fi
}

k8s() {
    kubectl --context=$KUBECONTEXT -n $NAMESPACE "$@"
    return $?
}

k8s_without_namespace() {
    kubectl --context=$KUBECONTEXT "$@"
    return $?
}

list_volumes() {
    # cerebras_internal is a flag that is used to filter out volumes that are
    # not intended for user consumption. This is used by cerebras installation
    # scripts to remove internal volumes at the end of the installation.
    local cerebras_internal="false"

    # allow_venv is a flag that is used to filter out volumes that are intended
    # for venv mounting to gain dependency parity between the user node and worker
    # Python environment. This is used during runtime to identify where the user
    # node Python site packages should be relocated to in an NFS location. The worker
    # environment will respect those mounted site packages during runtime.
    local allow_venv="false"

    # workdir_logs is a flag that is used to filter out volumes that are intended
    # for wsjob workdir logging purposes.
    local workdir_logs="false"

    # cached_compile is a flag that is used to filter out volumes that are intended
    # for cached compiles.
    local cached_compile="false"

    for arg in "$@"; do
        if [ "$arg" = "--cerebras-internal" ]; then
            cerebras_internal="true"
        fi
        if [ "$arg" = "--allow-venv" ]; then
            allow_venv="true"
        fi
        if [ "$arg" = "--workdir-logs" ]; then
            workdir_logs="true"
        fi
        if [ "$arg" = "--cached-compile" ]; then
            cached_compile="true"
        fi
    done

    CMDATA=$(k8s get cm $VOLUME_CM -ojsonpath='{.data}' --ignore-not-found)
    if [ "" = "$CMDATA" ] ; then
        echo "no volumes configured" >&2
        exit 0
    fi

    local filters=()
    if [ "$cerebras_internal" = "true" ]; then
        filters+=('.labels["cerebras-internal"] == "true"')
    fi
    if [ "$allow_venv" = "true" ]; then
        filters+=('.labels["allow-venv"] == "true"')
    fi
    if [ "$workdir_logs" = "true" ]; then
        filters+=('.labels["workdir-logs"] == "true"')
    fi
    if [ "$cached_compile" = "true" ]; then
        filters+=('.labels["cached-compile"] == "true"')
    fi

    local filter=""
    if [ ${#filters[@]} -eq 0 ]; then
        filter="select(true)"
    else
        filter="select("
        for x in "${filters[@]}" ; do filter="${filter}${x} and " ; done
        filter=${filter%" and "}")"
    fi

    jq -r '(["NAME","TYPE","CONTAINER_PATH","SERVER","SERVER_PATH","READONLY","ALLOW_VENV"],
            (to_entries[] | ([.key, (.value | fromjson | '"$filter"' |
            [.type,.containerPath // "-",.server // "-",.serverPath // "-",.readonly // false,.labels["allow-venv"] // false] )] | flatten ))) |
            select(length > 1) |
            @tsv' <<< "${CMDATA}" | column -t
}

put_volume() {
    read -r -d '' USAGE_TEXT << EOM
put volume

usage:
  # Create/update NFS volume
  $0 put VOLUME_NAME --container-path <containerPath> --server <nfsIpOrName> --server-path <serverPath> [--readonly] \
      [--allow-venv] [--workdir-logs] [--cached-compile] [--create-dir-if-not-exists] [--permission <octal>] [--force] [--cerebras-internal]

  # Create/update hostPath volume
  $0 put VOLUME_NAME --container-path <containerPath> [--readonly]

Creates or updates a volume accessible to the cluster.

The positional argument VOLUME_NAME must be an RFC 1035 label format (lowercase
alphanumeric and dash characters, less than 64 characters total).

Requires argument '--server' to be set to an NFS volume. The IPs of the nodes in
the Cerebras cluster should be allowed to access NFS. This configuration must be
made on the NFS server.

Argument '--server-path' defaults to "/" and when --readonly is not specified, the
volume will be mounted as readwrite.

One typical usecase of cluster volumes is to mount NFS-based dataset to streamers for data ingestion to the appliance
workflow. Cluster volumes also support other advanced use cases for example Python virtual environment mounting, NFS-based
workdirs and NFS-based cached compiles.

The '--allow-venv' flag is used to filter out volumes that are intended for venv. A venv-allowed volume is mandated as
a fallback policy to gain dependency parity when image build fails.
The '--workdir-logs' flag is used to filter out volumes that are intended for wsjob workdir logging purposes. This is
an advanced support for persisting job logs over NFS. This is needed especailly for jobs which dump a significant amount
of logs (more than few hundred GB) where local storage could no longer hold.
The '--cached-compile' flag is used to filter out volumes that are intended for cached compiles. This is an advanced
support to persist cached compile over NFS.

The '--create-dir-if-not-exists' flag is used to create the directory on the volume if it does not exist.
The '--permission' flag is used to set the permission of the volume. The value should be in octal format.
The '--force' flag is used to override the existing volume configuration and re-run volume validation test.
The '--cerebras-internal' flag is used to filter out volumes that are only relevant internal usage in Cerebras.

Cluster admins can use the '--permission' flags to set permission of the volume.
This allows certain group of users to write to the volume with correct permissions from usernodes. A typical use case
is the appliance client copies the local venv to the venv volume to achieve dependency parity between the usernode and
worker Python environment.

Also supports hostPath volumes. The hostPath volume's path must exist on the
worker nodes prior to adding the volume. This is to ensure that permissions are
set properly on the volumes as kubelet will create directories as root which may
cause problems.

Prior to committing the update, this script will test reachability of the volume
using the "$0 test VOLUME_NAME" command.

Example Usage

  # create or update a volume mounting on "/" and in readwrite mode
  put myvol --container-path /cb --server 10.10.10.10

  # create or update a volume with a particular NFS server address and in readonly mode
  put myvol --container-path /cb/share/mldata --server 10.10.10.10 --server-path /share/mldata --readonly

  # create a hostPath volume. /cb/share/mldata must already exist on the worker nodes.
  put myvol --container-path /cb/share/mldata

EOM
    if [ "$#" = "0" ] ; then
        echo "$USAGE_TEXT"
        exit 0
    fi

    local name server serverPath cerebrasInternal allowVenv force
    name=$1
    shift
    server=""
    serverPath="/"
    readonly="false"
    workdirLogs="false"
    cachedCompile="false"
    allowVenv="false"
    createDirIfNotExists="false"
    permission="-"
    force="false"
    cerebrasInternal="false"

    while [ $# -gt 0 ] ; do
        case $1 in
        --server)
            shift
            server=$1
            ;;

        --server-path)
            shift
            serverPath=$1
            ;;

        --container-path)
            shift
            containerPath=$1
            ;;

        --readonly)
            export readonly_="true"
            ;;

        --workdir-logs)
            workdirLogs="true"
            ;;

        --cached-compile)
            cachedCompile="true"
            ;;

        --allow-venv)
            allowVenv="true"
            ;;

        --create-dir-if-not-exists)
            createDirIfNotExists="true"
            ;;

        --permission)
            shift
            permission=$1
            ;;

        --force)
            force="true"
            ;;

        --cerebras-internal)
            cerebrasInternal="true"
            ;;

        *)
            echo "unknown option: $1" ; echo "$USAGE_TEXT"
            exit 1
            ;;
        esac
    shift
    done

    if [[ -z "$containerPath" ]]; then
        echo "error: expected a '--container-path' argument, but there was none provided"
        exit 1
    fi

    # validate configmap name to ensure it satisfies k8s constraint for volume names
    # https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names
    if ! [[ "$name" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || ! [[ ${#name} -le 63 ]]; then
        echo "error: Name '${name}' is invalid."
        echo "Names must be a valid RFC 1123 label name (only lowercase alpha-numeric characters and '-' and not start or end in a special character)."
        exit 1
    fi

    local json_template labels volume_json existing_sum update_sum escaped_json
    json_template='{"type": $type, "containerPath": $containerPath, "readonly": $readonly, "labels": $labels'
    if [ "$server" = "" ] ; then
        type="hostPath"
        json_template=$json_template'}'
    else
        type="nfs"
        json_template=$json_template', "server": $server, "serverPath": $serverPath}'
    fi

    labels=""
    if [ "$cerebrasInternal" = "true" ]; then
        labels+='"cerebras-internal": "true",'
    fi
    if [ "$allowVenv" = "true" ]; then
        labels+='"allow-venv": "true",'
    fi
    if [ "$workdirLogs" = "true" ]; then
        labels+='"workdir-logs": "true",'
    fi
    if [ "$cachedCompile" = "true" ]; then
        labels+='"cached-compile": "true",'
    fi
    labels="{$(echo $labels | sed 's/,$//')}"

    volume_json=$(jq --arg type "$type" \
                 --arg containerPath "$containerPath" \
                 --argjson readonly "$readonly" \
                 --arg server "$server" \
                 --arg serverPath "$serverPath" \
                 --argjson labels "$labels" \
                 -n "$json_template")

    existing_sum=$(k8s get cm $VOLUME_CM --ignore-not-found -ojsonpath='{.data.'"$name"'}' | jq -S . | md5sum)
    update_sum=$(echo "$volume_json" | jq -S . | md5sum)
    if ! $force && [ "$existing_sum" == "$update_sum" ] ; then
        echo "volume '$name' already exists, skipping create. Use --force to override and re-run volume validation test"
        exit 0
    fi

    if ! _test_volume "$name" "$type" "$containerPath" "$readonly" "$serverPath" "$server" "$createDirIfNotExists" "$permission" ; then
        echo "error: failed to mount volume (name=$name, type=$type, containerPath=$containerPath, readonly=$readonly, serverPath=$serverPath, server=$server, createDir=$createDirIfNotExists, permission=$permission)"
        echo "aborting"
        exit 1
    fi

    if ! k8s get cm $VOLUME_CM &>/dev/null ; then
        k8s create configmap $VOLUME_CM --from-literal="$name"="${volume_json}" &> /dev/null
        k8s label configmap/$VOLUME_CM 'k8s.cerebras.com/user-managed=true' &> /dev/null
    else
        escaped_json=$(jq -c '. | tojson' <<< "$volume_json")
        k8s patch configmap $VOLUME_CM --type=strategic -p '{"data":{"'$name'":'$escaped_json'}}'
    fi

    return $?
}

delete_volume() {
    read -r -d '' USAGE_TEXT << EOM
delete volume

usage: delete VOLUME_NAME

Deletes a volume. Does nothing if the volume does not exist. Does not delete any
data on the target volume, and any running jobs referencing this volume will
continue to access the volume until the job terminates.

Example Usage

  # delete a volume "myvol"
  delete myvol

EOM
    if [ "$#" = "0" ] ; then
        echo "$USAGE_TEXT"
        exit 0
    fi
    k8s patch configmap $VOLUME_CM --type=json -p='[{"op": "remove", "path": "/data/'"$1"'"}]' &>/dev/null
    return $?
}

copy_volumes() {
    read -r -d '' USAGE_TEXT << EOM
copy volume

usage: copy-from SOURCE_NAMESPACE

Copies the configuration of all volumes from source namespace to a destination
namespace. If the volume configuration exist in destination namespace prior to 
running this command, then it gets overridden by volume configurations from 
source namespace. The destination volumes will be created with the same type,
containerPath, server, serverPath, and readonly settings as the source volume.

Example Usage

  # copy all volumes configs to current namespace $NAMESPACE from defined source namespace
  copy-from <source-namespace>

EOM
    if [ "$#" != "1" ] ; then
        echo "$USAGE_TEXT"
        exit 1
    fi

    local dest=$NAMESPACE
    local source=$1

    # 1. Check if source namespace and confimap existssts
    k8s get ns $source  > /dev/null 2>&1
    if [ "$?" = "1" ] ; then
        echo "Source namespace [$source] for copy operation does not exist" >&2
        exit 1
    fi

    CMDATA=$(k8s_without_namespace -n $source get cm $VOLUME_CM  --ignore-not-found -ojsonpath='{.data}')
    if [ "$CMDATA" = "" ] ; then
        echo "No volumes configured in [$source]" >&2
        exit 1
    fi

    # 2. Check if destination namespace exists
    k8s get ns $dest > /dev/null 2>&1
    if [ "$?" = "1" ] ; then
        echo "Destination namespace [$dest] does not exist" >&2
        exit 1
    fi

    # 3. Check if destination namespace has volumes already configured
    #    If yes, then prompt user to overwrite or not
    DEST_CMDATA=$(k8s get cm $VOLUME_CM  --ignore-not-found -ojsonpath='{.data}' | jq -r 'keys | join(",")')
    if [ "$DEST_CMDATA" != "" ] ; then
        read -p "$VOLUME_CM configmap in destination namespace '$dest' already exists with following list of volumes: '$DEST_CMDATA'. Do you want to overwrite all of them? (y/n): " confirm
        if [ "$confirm" != "y" ] ; then
            echo "'copy-from' Operation cancelled."
            exit 1
        fi
        echo "Overwriting existing $VOLUME_CM configmap in namespace $dest"
    else
        echo "Creating new configmap $VOLUME_CM in namespace $dest"
    fi
    k8s_without_namespace -n $source get cm $VOLUME_CM -o yaml | sed "s/namespace: $source/namespace: $dest/" | k8s replace --force -f - &> /dev/null
    return $?
}

test_volume() {
        read -r -d '' USAGE_TEXT << EOM
test volume

Tests if a configured volume can be mounted by a containers running on worker nodes.
Outputs errors encounted while attempting to mount the volume or prints a success message
on completion. Tests can take up to 15s to complete.

usage: test VOLUME_NAME

Example Usage

  # test mountability of a volume "myvol"
  test myvol

EOM
    if [ "$#" = "0" ] ; then
        echo "$USAGE_TEXT"
        exit 0
    fi
    local volumeName=$1

    CMDATA=$(k8s get configmap $VOLUME_CM -o=jsonpath='{.data}' --ignore-not-found)
    if [ "$CMDATA" = "" ] ; then
        echo "no volumes configured" >&2
        exit 1
    fi
    VOLJSON=$(jq -r ".[\"$volumeName\"] | fromjson" 2> /dev/null <<< "$CMDATA")
    if [ "$?" != "0" ] || [ -z "$VOLJSON" ] ; then
        echo "error reading volume '$1'" >&2
        exit 1
    fi

    volumeType=$(jq -r ".type" <<< "$VOLJSON")
    containerPath=$(jq -r ".containerPath" <<< "$VOLJSON")
    readonly=$(jq -r ".readonly // false" <<< "$VOLJSON")
    # nfs-specific
    serverPath=$(jq -r ".serverPath" <<< "$VOLJSON")
    server=$(jq -r '.server // ""' <<< "$VOLJSON")

    createDirIfNotExists="false"
    permission="-"
    if ! _test_volume "$volumeName" "$volumeType" "$containerPath" "$readonly" "$serverPath" "$server" "$createDirIfNotExists" "$permission"; then
        echo "error testing volume $volumeName" >&2
        exit 1
    fi
    exit 0
}

_test_volume() {
    local dsname="test-mount-volume-$(date +%s-%N)"
    local volumeName=$1
    local volumeType=$2
    local containerPath=$3
    local readonly=${4:-false}
    local serverPath=$5
    local server=$6
    local createDir=$7
    local permission=$8

    # check whether given server conflicts with multus virtual range
    if [ -n "$server" ] ; then
        local server_ip=$(_resolve_ip "$server")
        if [ -n "$server_ip" ] ; then
            echo "checking if server $server_ip conflicts with reserved ranges" >&2
            if ! _virtual_range_check "$server_ip" ; then
                echo "Error: server $server (ip=$server_ip) falls within cluster reserved address range" >&2
                echo "Please ensure this IP is outside this range. Contact Cerebras for support." >&2
                return 1
            fi
        else
            echo "warning: skipping virtual range check because $server could not be resolved to an IPv4 address" >&2
        fi
    fi

    # check whether given volume is mountable by k8s on worker nodes
    _cleanup() {
        k8s delete ds $dsname --ignore-not-found --force &> /dev/null
        trap - INT TERM RETURN
        if [ "$1" -ne 0 ] ; then exit "$1"; fi
    }
    trap "_cleanup 1" INT TERM
    trap "_cleanup 0" RETURN

    local serverPathRoot="${serverPath}"
    local containerPathRoot="${containerPath}"
    if [ "$createDir" = "true" ] ; then
        # input - containerPath: "/cb/tests/cluster-mgmt/foo", serverPath: "/tests/cluster-mgmt/foo"
        # output after removing common suffix - containerPathRoot: "/cb/tests", serverPathRoot: "/tests"
        # the script uses the containerPathRoot and serverPathRoot to create the new directory for the volume
        #
        # if a non-existing server path is used as NFS volume, the NFS server would reject the volume creation:
        # mount.nfs: mounting cerebras-storage:/tests/foo/bar failed, reason given by server: No such file or directory

        # Find common base path for serverPath and containerPath
        _is_top_level_path() {
            [[ "$(echo "$1" | grep -o "/" | wc -l)" -eq 1 ]]
        }
        while [[ -n "$serverPathRoot" && -n "$containerPathRoot" ]]; do
            # Get the last segment of both paths
            serverSegment=$(basename $serverPathRoot)
            containerSegment=$(basename $containerPathRoot)

            # Check if the last segment matches and the paths are not just root
            if [[ "$serverSegment" == "$containerSegment" ]] && ! _is_top_level_path "$serverPathRoot" && ! _is_top_level_path "$containerPathRoot"; then
                # Remove the last segment from both paths
                serverPathRoot=$(dirname $serverPathRoot)
                containerPathRoot=$(dirname $containerPathRoot)
            else
                # Stop if the segments are different
                break
            fi
        done
    fi

    local spec="
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: $dsname
  namespace: $NAMESPACE
  labels:
    k8s-app: test-volume-mount
    volume-test: $dsname
spec:
  selector:
    matchLabels:
      volume-test: $dsname
  template:
    metadata:
      labels:
        volume-test: $dsname
    spec:
      terminationGracePeriodSeconds: 1
      # tolerations exist to allow running on the control-plane in the case of a single node cluster
      tolerations:
        - key: node-role.kubernetes.io/master
          operator: Exists
          effect: NoSchedule
        - key: node-role.kubernetes.io/control-plane
          operator: Exists
          effect: NoSchedule
      containers:
      - name: testmount
        image: registry.local/busybox:1.34.1
        command: [ 'sleep', '100' ]
"
    if [ "$volumeType" == "nfs" ] ; then
        spec+="
        volumeMounts:
        - mountPath: $containerPathRoot
          name: nfs-$volumeName
      volumes:
      - name: nfs-$volumeName
        nfs:
          path: $serverPathRoot
          readOnly: $readonly
          server: $server
"
    else # hostPath
        spec+="
        volumeMounts:
        - mountPath: $containerPath
          name: hostpath-$volumeName
      volumes:
      - name: hostpath-$volumeName
        hostPath:
          path: $containerPath
          type: Directory
"
    fi

    # extract node name, labels, and ready status from k8s for later computation
    nodes=$(k8s get nodes -ojson | jq -r '.items[] | select(.status.conditions[].type == "Ready") |
        {name: .metadata.name, labels: .metadata.labels, ready: .status.conditions[] | select(.type == "Ready").status } ' | jq -n ". | [inputs]")
    spec+="
      nodeSelector:
        'k8s.cerebras.com/node-role-worker': ''
"
    has_ready_workers="false"
    if [ "$(echo $nodes | jq -r '.[] | select(.labels | has("k8s.cerebras.com/namespace")) | .name' | wc -w)" -gt "0" ]; then
        if [ "$(echo $nodes | jq --arg NAMESPACE "$NAMESPACE" -r '.[] | select(.labels."k8s.cerebras.com/namespace" == $NAMESPACE) |
            .name' | wc -w)" -gt "0" ]; then
            if [ "$(echo $nodes | jq --arg NAMESPACE "$NAMESPACE" -r '.[] | select(.labels."k8s.cerebras.com/namespace" == $NAMESPACE and
                .labels."k8s.cerebras.com/node-role-worker" == "" and .ready == "True") | .name' | wc -w)" -gt "0" ]; then
                has_ready_workers="true"
            fi

            spec+="
        'k8s.cerebras.com/namespace': '$NAMESPACE'
"
        else
            echo "WARNING: this cluster has enabled namespace isolation, but namespace '$NAMESPACE' does not contain any node."
            echo "WARNING: this test will run on all worker nodes."
            if [ "$(echo $nodes | jq -r '.[] | select(.labels."k8s.cerebras.com/node-role-worker" == "" and .ready == "True") | .name' | wc -w)" -gt "0" ]; then
                has_ready_workers="true"
            fi
        fi
    fi

    if [ "$has_ready_workers" == "false" ]; then
        # Some namespaces such as inference namespaces might not have worker nodes. Return okay in that case.
        # Also, if all worker nodes are in NotReady state, return okay.
        echo "skip the volume test since there are no ready workers"
        return 0
    fi

    echo "$spec" | k8s apply -f - &> /tmp/.test-volume.log
    if [ "$?" != "0" ] ; then
        echo "error creating test pod" >&2
        cat /tmp/.test-volume.log >&2
        rm /tmp/.test-volume.log >&2
        return 1
    fi

    echo "testing volume is mountable on kubernetes. This may take up to 60 seconds..." >&2
    if ! k8s rollout status ds/$dsname --timeout=60s &> /dev/null ; then
        echo "test failed to complete, there was probably a mount error, see logs and events:" >&2
        # show max of 3 failed pod logs...
        local failed_pods=$(k8s get pod -l"volume-test=$dsname" --no-headers | grep -v Running | head -n3 | awk '{print $1}')
        for pod in ${failed_pods} ; do
            echo
            echo "${pod} running on node $(k8s get pod ${pod} -ojsonpath='{.spec.nodeName}'):"
            k8s logs "$pod" -c testmount >&2
            k8s get events --sort-by='.lastTimestamp' --field-selector involvedObject.name=$pod -ojson | jq -r '.items[] | .message' | tail -n 5 >&2
        done
        return 1
    fi

    local running_pod=$(k8s get pod -l"volume-test=$dsname" --no-headers | grep -w Running | head -n1 | awk '{print $1}')
    if [ -z "$running_pod" ] ; then
        echo "error: test pod not running" >&2
        return 1
    fi
    if ! k8s exec $running_pod -- test -d $containerPath && [ "$createDir" = "true" ] ; then
        k8s exec $running_pod -- mkdir -p $containerPath
        echo "created directory $serverPath on $server" >&2
    fi
    if [ "$permission" != "-" ] && [ "$(k8s exec $running_pod -- stat -c '%a' $containerPath)" -ne "$permission" ] ; then
      if ! k8s exec $running_pod -- chmod -R $permission $containerPath; then
        echo "error: failed to chmod on volume $volumeName with permission $permission"
        return 1
      fi
    fi

    echo "$volumeName mounted" >&2
    echo "success" >&2
    return 0
}


main() {
    if [ "$#" = "0" ] ; then echo "$USAGE_TEXT" ; exit ; fi

    _check_prereqs
    case "$1" in
        get)
            shift
            list_volumes $@
        ;;
        put)
            shift
            put_volume $@
        ;;
        delete)
            shift
            delete_volume $@
        ;;
        copy-from)
            shift
            copy_volumes $@
        ;;
        test)
            shift
            test_volume $@
        ;;
        *)
            echo "unknown command '$1', options: get, put, delete"
            echo "$USAGE_TEXT"
            exit 1
    esac
}

main $@