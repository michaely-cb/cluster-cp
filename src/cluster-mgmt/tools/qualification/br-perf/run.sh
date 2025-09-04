#!/usr/bin/env bash

# load_image takes an image file and list of nodes and loads the image to them
function load_image() {
    if [ ! -f "$1" ]; then
        echo "ERROR: br_load_img IMAGE: argument '$1' is not a file"
        return 1
    fi
    local image="$1"
    shift
    local hosts_file=$(mktemp)
    echo "$@" | xargs -n1 >> "$hosts_file"

    local logdir=/tmp/pssh
    local pssh_opts=(-p 12 -x "-o StrictHostKeyChecking=no" -h $hosts_file -e $logdir -o $logdir)
    rm -rf $logdir && mkdir -p $logdir
    if ! pscp.pssh "${pssh_opts[@]}" $image /tmp/iperf.tar &>/dev/null ; then
        cat $logdir/* 1>&2
        echo "ERROR: failed to copy image to nodes"
        return 1
    fi

    if ! pssh "${pssh_opts[@]}" -i "ctr -n k8s.io i import /tmp/iperf.tar && rm /tmp/iperf.tar" &>/dev/null ; then
        cat $logdir/* 1>&2
        echo "ERROR: failed to load image on nodes"
        return 1
    fi
}

# unload_image takes a list of nodes and unloads the image from them
function unload_image() {
    local hosts_file=$(mktemp)
    echo "$@" | xargs -n1 >> "$hosts_file"
    pssh -p 12 -i -h $hosts_file -l root -x "-o StrictHostKeyChecking=no" \
        "ctr -n k8s.io i rm registry.local/alpine-iperf:0.0.1" &>/dev/null
    return $?
}

# make_node_tests PORT_START PARALLELISM BR_NODE MEM_NODE [BR_IP...]
#
# Creates pods to be launched for each 100G NIC on the BR node in serial order.
# E.g. NIC0 test will run to completion before NIC1 test is started.
#
# Arguments:
#   PORT_START: The first iperf port, uses up to PORT_STATE + PARALLELISM ports
#   PARALLELISM: The number of iperf clients to run in parallel
#   BR_NODE: The node to run the iperf server on
#   MEM_NODE: The node to run the iperf client on
#   BR_IP: List of BR_NODE 100G NIC IPs to test
function make_node_tests() {
    local PORT_START=$1
    local PARALLELISM=$2
    local BR_NODE=$3
    local MEM_NODE=$4
    shift ; shift ; shift ; shift
    local BR_IPS=($@)

    mkdir -p ${BR_NODE}
    echo ${MEM_NODE} > ${BR_NODE}/mem_node.txt

    IPERF_IMAGE="registry.local/alpine-iperf:0.0.1"
    # Ensure the MEM_NODE binds to the 100G interface
    MEM_IP=$(ssh ${MEM_NODE} ip -j a | jq -r '.[] | select(.mtu == 9000 and .addr_info[0].scope =="global") | .addr_info[0].local' | head -n1)
    for i in "${!BR_IPS[@]}"; do
        local BR_IP=${BR_IPS[$i]}
        mkdir -p ${BR_NODE}/${BR_IP}

        for PORT_IDX in $(seq 0 $(($PARALLELISM - 1)) ) ; do
            IPERF_PORT=$(( ${PORT_START} + ${PORT_IDX}))
            # run iperf client for 30 seconds omitting the first 5 seconds
            # zero-copy emperically improves performance. Target bitrate -b0 is infinite but expect 100Gps in aggregate on 100G cards
            # parallelism is done on the level of Pods not iperf with -P option since -P4 didn't give as good performance as multiplying
            # the number of pods.
            cat <<EOF >> ${BR_NODE}/${BR_IP}/clients.yaml
apiVersion: v1
kind: Pod
metadata:
  name: iperf-client-${BR_NODE}-${PORT_IDX}
  namespace: kube-system
  labels:
    k8s-app: iperf
    role: client
    test-id: "${BR_IP}"
    replica-id: "${PORT_IDX}"
spec:
  hostNetwork: true
  restartPolicy: Never
  nodeName: ${MEM_NODE}
  containers:
    - name: iperf-client
      workingDir: /home
      image: "${IPERF_IMAGE}"
      imagePullPolicy: Never
      args: [ "-c", "${BR_IP}", "-B", "${MEM_IP}", "-p", "${IPERF_PORT}", "-Z", "-N", "-O5", "-t30", "-l128k", "-b0", "-P1", "-J" ]
      resources:
        requests:
          cpu: 1
          memory: 1G
        limits:
          cpu: 1
          memory: 1G
---
EOF
            cat <<EOF >> ${BR_NODE}/${BR_IP}/servers.yaml
apiVersion: v1
kind: Pod
metadata:
  name: iperf-server-${BR_NODE}-${PORT_IDX}
  namespace: kube-system
  labels:
    k8s-app: iperf
    role: server
    test-id: "${BR_IP}"
spec:
  hostNetwork: true
  restartPolicy: Never
  nodeName: ${BR_NODE}
  containers:
    - name: iperf-server
      workingDir: /home
      image: "${IPERF_IMAGE}"
      imagePullPolicy: Never
      args: [ "-s", "-p", "${IPERF_PORT}", "-B", "${BR_IP}", "-i5" ]
      ports:
      - containerPort: ${IPERF_PORT}
        hostPort: ${IPERF_PORT}
      resources:
        requests:
          cpu: 1
          memory: 1G
        limits:
          cpu: 1
          memory: 1G
---
EOF
        done
    done
}

# make_cluster_tests -m mem_node0 mem_node1 ... -b br_node0 br_node1 ...
# Creates a set of tests for each br node in directories structured like BR_NODE/IP
# where IP is the IP address of a 100G NIC.
function make_cluster_tests() {
    local START_PORT=5001
    local PARALLELISM=8
    local MEM_NODES=()
    local BR_NODES=()
    while [[ $# -gt 0 ]]; do
        case $1 in
            -m)
                shift
                while [[ $# -gt 0 && $1 != -* ]]; do
                    MEM_NODES+=($1)
                    shift
                done
                ;;
            -b)
                shift
                while [[ $# -gt 0 && $1 != -* ]]; do
                    BR_NODES+=($1)
                    shift
                done
                ;;
            *)
                echo "ERROR: unknown argument $1"
                return 1
                ;;
        esac
    done

    for i in "${!BR_NODES[@]}"; do
        local BR_NODE=${BR_NODES[$i]}
        local MEM_NODE=${MEM_NODES[$((i % ${#MEM_NODES[@]}))]}
        local BR_IPS=($(ssh -o StrictHostKeyChecking=no ${BR_NODE} ip -j a | \
            jq -r '.[] | select(.mtu == 9000 and .addr_info[0].scope =="global") | .addr_info[0].local'))
        make_node_tests $START_PORT $PARALLELISM ${BR_NODE} ${MEM_NODE} "${BR_IPS[@]}"
        START_PORT=$(( ${START_PORT} + ${PARALLELISM} ))
    done
}

# run_node_nic_test BR_NODE BR_IP
#   Runs the iperf tests for the BR_NODE and BR_IP. Outputs the results to
#   the BR_NODE/BR_IP directory.
function run_node_nic_test() {
    if [ $# -ne 2 ] || [ ! -d $1/$2 ]; then
        echo "run_node_nic_test: missing BR_NODE or BR_IP argument"
        return 1
    fi

    BR_NODE=$1
    BR_IP=$2

    pushd ${BR_NODE}/${BR_IP} 1>/dev/null

    cleanup() {
        trap - RETURN
        kubectl delete pod --wait=true -n kube-system -lk8s-app=iperf,test-id=${BR_IP} &>/dev/null
        popd &>/dev/null
    }
    trap cleanup RETURN

    if [ $(find . -maxdepth 1 -name "*.err" | wc -l) -ne 0 ]; then
        >&2 echo "run_node_nic_test: ${BR_NODE}/${BR_IP} had errors from previous run, retrying..."
        rm -f ./*.err ./*-iperf.json
    fi

    if [ $(find . -maxdepth 1 -name "*-iperf.json" | wc -l) -ne 0 ]; then
        >&2 echo "run_node_nic_test: ${BR_NODE}/${BR_IP} had results from previous run, skipping"
        return 0
    fi

    echo "Starting iperf servers on ${BR_NODE}:${BR_IP}"
    kubectl apply -f servers.yaml &>/dev/null
    for POD in $(kubectl get pod -oname -n kube-system -lk8s-app=iperf,role=server,test-id=${BR_IP}) ; do
        if ! kubectl wait ${POD} -n kube-system --timeout=1m --for=jsonpath='{.status.phase}'='Running' &>/dev/null ; then
            >&2 kubectl describe ${POD} -n kube-system
            >&2 echo failed to start server ${POD}
            return 1
        fi
    done

    kubectl apply -f clients.yaml &>/dev/null
    for POD in $(kubectl get pod -oname -n kube-system -lk8s-app=iperf,role=client,test-id=${BR_IP}) ; do
        POD_JSON=$(kubectl wait ${POD} -n kube-system --timeout=1m --for=jsonpath='{.status.phase}'='Succeeded' -ojson 2>/dev/null)
        POD_NAME=$(echo $POD | cut -d/ -f2)
        if [ $? -ne 0 ] ; then
            echo "${POD} failed" > ${POD_NAME}.err
            kubectl logs -n kube-system ${POD_NAME} &>> ${POD_NAME}.err
            >&2 echo "${POD} failed, logs in $(pwd -P)/${POD_NAME}.err, first 10 lines:"
            >&2 head -n10 ${POD_NAME}.err | sed -e 's/^/   /'
            return 1
        fi

        REPLICA_ID=$(echo ${POD_JSON} | jq -r '.metadata.labels["replica-id"]')
        kubectl -nkube-system logs ${POD} > ${REPLICA_ID}-iperf.json
        err=$(jq -r '.error' ${REPLICA_ID}-iperf.json)
        if [ $? -ne 0 ] || [ "${err}" != "null" ]; then
            touch ${POD_NAME}.err
            >&2 echo "iperf error ${BR_NODE}:${BR_IP}: ${err}."
            return 1
        fi
    done
    echo "Finished iperf clients on ${BR_NODE}:${BR_IP}"
    
    cleanup
    return 0
}

# run_node_test BR_NODE
#   Runs the iperf tests for all NICS on the BR_NODE. Outputs the results to
#   the BR_NODE/IP directory.
function run_node_test() {
    if [ $# -ne 1 ] || [ ! -d $1 ]; then
        echo "run_node_test: missing BR_NODE argument"
        return 1
    fi
    BR_NODE=$1
    MEM_NODE=$(cat ${BR_NODE}/mem_node.txt)
    ERROR_COUNT=0

    # lock the mem node from being used by other tests
    # https://stackoverflow.com/questions/24388009/linux-flock-how-to-just-lock-a-file
    {
        >&2 echo "$$ ${MEM_NODE} lock await"
        flock -w$(( 60 * 20 )) -x 200
        if [ $? -ne 0 ]; then
            >&2 echo "$$ ${MEM_NODE} lock timeout"
            return 1
        fi
        >&2 echo "$$ ${MEM_NODE} lock acquired"
        echo "Running tests ${MEM_NODE} -> ${BR_NODE}"
        for BR_IP in $(ls -d ${BR_NODE}/*/) ; do
            if ! run_node_nic_test ${BR_NODE} $(echo ${BR_IP} | cut -d/ -f2) ; then
                echo "Failed to run tests ${MEM_NODE} -> ${BR_NODE}:${BR_IP}"
                ERROR_COUNT=$(( ${ERROR_COUNT} + 1 ))
            fi
        done
    } 200>$MEM_NODE.lock
    >&2 echo "$$ ${MEM_NODE} lock released"
    return $ERROR_COUNT
}

# parse_node_nic_results BR_NODE BR_IP
#   Reports the results of the tests for the BR_NODE and BR_IP. Returns a comma
#   delimited string of the BR_NODE, BR_IP, MEM_NODE, each GBPS, and the sum of
#   the GBPS.
function parse_node_nic_results() {
    if [ $# -ne 2 ] || [ ! -d $1/$2 ]; then
        >&2 echo "br_run_test: missing BR_NODE or BR_IP argument"
        return 1
    elif [ $(ls $1/$2/*-iperf.json 2>/dev/null | wc -l) -eq 0 ]; then
        >&2 echo "br_report_test: no iperf results found for $1/$2"
        return 1
    elif [ $(ls $1/$2/*.err 2>/dev/null | wc -l) -ne 0 ]; then
        >&2 echo "br_report_test: errors found for $1/$2"
        return 1
    fi

    function bps_to_gbps() {
        awk -v bps=$1 'BEGIN { printf "%.2f", bps / 1000000000 }'
    }

    declare -a BPS
    for FILE in $1/$2/*-iperf.json ; do
        BPS+=($(bps_to_gbps $(jq -r '.end.sum_received.bits_per_second' ${FILE})))
    done
    BPS_SUM=$(echo ${BPS[@]} | awk '{s=0; for (i=1; i<=NF; i++) s+=$i; print s}')

    MEM_NODE=$(cat $1/mem_node.txt)
    echo "${1},${2},${MEM_NODE},${BPS_SUM}"
}

function parse_cluster_results() {
    # iterate through the given input array args and print the results
    echo "BR_NODE,BR_IP,MEM_NODE,GBPS" > results.csv
    for BR_NODE in "$@" ; do
        for BR_IP in $(ls -d ${BR_NODE}/*/) ; do
            parse_node_nic_results ${BR_NODE} $(echo ${BR_IP} | cut -d/ -f2) >> results.csv
        done
    done
    column -t -s, < results.csv
    >&2 echo "results saved to results.csv"
}

function main() {
    USAGE="Usage: $0 [-m mem_node,mem_node,...] [-M mem_node_file] [-b br_node,br_node,...] [-B br_node_file] [--report-only] [--clean]"
    MEM_NODES=()
    BR_NODES=()
    REPORT_ONLY=false
    CLEAN=false
    while [ $# -gt 0 ]; do
        case "$1" in
            -m)
                shift
                MEM_NODES+=($(echo $1 | tr ',' ' '))
                ;;
            -M)
                shift
                MEM_NODES+=($(cat $1 | tr '\n' ' '))
                ;;
            -b)
                shift
                BR_NODES+=($(echo $1 | tr ',' ' '))
                ;;
            -B)
                shift
                BR_NODES+=($(cat $1 | tr '\n' ' '))
                ;;
            --report-only)
                REPORT_ONLY=true
                ;;
            --clean)
                CLEAN=true
                ;;
            *)
                echo "Unknown argument: $1"
                exit 1
                ;;
        esac
        shift
    done

    if [ ${#BR_NODES[@]} -eq 0 ]; then
        echo "Must specify at least one br node"
        echo $USAGE
        exit 1
    fi

    if [ "${REPORT_ONLY}" = true ]; then
        parse_cluster_results "${BR_NODES[@]}"
        exit 0
    fi

    if [ ${#MEM_NODES[@]} -eq 0 ]; then
        echo "Must specify at least one mem node"
        echo $USAGE
        exit 1
    fi

    echo "loading images..."
    if ! load_image image.tar "${BR_NODES[@]}" "${MEM_NODES[@]}" ; then
        echo "Failed to load image"
        return 1
    fi

    if [ "${CLEAN}" = true ] ; then
        echo "clean up old tests..."
        rm -rf "${BR_NODES[@]}"
    fi

    echo "building tests..."
    if ! make_cluster_tests -b "${BR_NODES[@]}" -m "${MEM_NODES[@]}" ; then
        echo "Failed to make tests"
        return 1
    fi

    echo "running tests..."

    rm -f *.lock # clean up in case a previous run exited abnormally
    declare -a PIDS
    cleanup() {
        kill "${PIDS[@]}" &>/dev/null
        kubectl delete pod --wait=true -n kube-system -lk8s-app=iperf &>/dev/null
        rm -f ./*.lock &>/dev/null
    }
    trap cleanup EXIT
    for BR_NODE in ${BR_NODES[@]} ; do
        run_node_test ${BR_NODE} &
        PIDS+=($!)
    done
    local FAIL=0
    for PID in ${PIDS[@]} ; do
        wait ${PID}
        if [ "$?" != "0" ] ; then
            FAIL=$((FAIL + 1))
        fi
    done
    trap - EXIT

    echo "cleaning up..."
    cleanup
    unload_image "${BR_NODES[@]}" "${MEM_NODES[@]}"

    if [ ${FAIL} -gt 0 ] ; then
        echo "Failed ${FAIL} br tests, not reporting results."
        echo
        echo "You can re-run the test and it will retry any failed nodes and "
        echo "skip already completed nodes. Alternatively, you can run with the "
        echo "--report-only flag to report the results of the completed tests."
        echo
        echo "These directories contain errors:"
        echo
        find . -type f -name '*.err' -exec dirname {} \; | sort | uniq
        echo
        return ${FAIL}
    fi

    parse_cluster_results "${BR_NODES[@]}"

    # return 1 if any of the rows in results.csv have a GBPS_SUM less than 97
    awk -F, 'NR>1 && $NF<97 {exit 1}' results.csv
    return $?
}

main "$@"
