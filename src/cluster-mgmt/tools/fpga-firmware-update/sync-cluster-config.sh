#! /usr/bin/env bash

set -e

YQ_MIN_VER="4.25.1"
SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE:-job-operator}

function usage {
    echo "$0 <system>"
    echo
    echo "  Updates the cluster configuration file to reflect the current firmware state of a cs-system."
    echo
    echo "  Assumptions"
    echo "  - executing on control-plane node of the appliance kubernetes cluster"
    echo "  - cs system is accessible via ssh root@<system>"
    echo "  - 'yq' is installed"
    echo
    echo "  Example"
    echo
    echo "    $0 systemf47"
}

function check_prereq {
    if [[ "" = $(command -v kubectl) ]] ; then
        echo "kubectl not present on system"
        echo "please ensure you are running on the control-plane node of the kubernetes cluster"
        exit 1
    fi

    if [[ "" = $(command -v yq) ]] ; then
        echo "yq not present on system"
        echo "see https://github.com/mikefarah/yq#install for install instructions"
        exit 1
    elif [[ $(yq -V | grep -o  '[0-9]\+.[0-9]\+.[0-9]\+') < "${YQ_MIN_VER}" ]] ; then
        echo "yq version was below the minimum required (${YQ_MIN_VER})"
        exit 1
    fi

}

function main {
    if [[ "$#" -ne 1 ]] ; then
        usage $@
        exit 1
    fi

    check_prereq

    # get cs machine mode
    local SYSTEM=$1
    local SYSTEM_TYPE="cs2"
    local EXECMODE=$(ssh root@${SYSTEM} -C cs config execmode show --output-format=json | yq '.execmode')
    if [[ "${EXECMODE}" = "PIPELINED" ]] ; then
        SYSTEM_TYPE="cs2-pipeline"
    fi

    # update k8s config map
    local WORK_DIR=$(mktemp -d)
    kubectl get cm -n "${SYSTEM_NAMESPACE}" cluster -oyaml > ${WORK_DIR}/cm.yaml
    yq '.data["clusterConfiguration.yaml"]' ${WORK_DIR}/cm.yaml > ${WORK_DIR}/cluster-before.yaml
    yq "(.systems[] | select(.name == \"${SYSTEM}\") | .type) |= \"${SYSTEM_TYPE}\"" ${WORK_DIR}/cluster-before.yaml > ${WORK_DIR}/cluster-after.yaml
    if [[  "" != $(diff -q ${WORK_DIR}/cluster-before.yaml ${WORK_DIR}/cluster-after.yaml)  ]] ; then
        local yaml_contents=$(cat ${WORK_DIR}/cluster-after.yaml)
        yq ".data[\"clusterConfiguration.yaml\"] = \"${yaml_contents}\"" ${WORK_DIR}/cm.yaml > ${WORK_DIR}/cm-updated.yaml
        kubectl apply -f ${WORK_DIR}/cm-updated.yaml
        # restart job-operator since it doesn't pick up cm updates automatically
        kubectl delete pods -n "${SYSTEM_NAMESPACE}" -lcontrol-plane=controller-manager
        echo "done"
    else
        echo "config up to date, nothing to do"
    fi
}

main $@