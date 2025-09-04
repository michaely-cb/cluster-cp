#!/usr/bin/env bash

set -e

THIS_FILE=$(readlink -f "${BASH_SOURCE[0]}")
THIS_FILENAME=$(basename "$THIS_FILE")
K8_CERT_DIR="${K8_CERT_DIR:-/etc/kubernetes/pki}"


should_rotate() {
    # check the expiration on some particular controlplane cert as in practice, they're all generated at the same time
    if [ -n "${FORCE_ROTATE}" ] ; then
        return 0
    fi

    # Use file creation time (in local tz) since notBefore seems to be fixed at k8 cluster create time
    create_time_seconds=$(date -d "$(stat -c '%y' "${K8_CERT_DIR}/apiserver.crt")" +%s)

    # Return 0 if the cert is more than 90 days old. Check exists to avoid regenerating
    # cert if `csadm.sh install` is run multiple times during an install / regenerate roughly once per release cycle
    test "$create_time_seconds" -le $(date -d "-90 days" +%s)
}



rotate_controlplane_certs() {
    # rotates certs using kubeadm and restarts control plane components in order to pick up new certs

    # backup old certs
    rm -f "${K8_CERT_DIR}/k8s-pki-backup.tar"
    tar -cf k8s-pki-backup.tar -C $(dirname "${K8_CERT_DIR}") $(basename ${K8_CERT_DIR})
    mv k8s-pki-backup.tar "${K8_CERT_DIR}"

    cd "${K8_CERT_DIR}"

    # TODO: manually rotate certs with an expiration period longer than the default 1 year
    kubeadm certs renew all
    cp /etc/kubernetes/admin.conf "$HOME/.kube/config"
    kubectl config set-context --current --namespace "${SYSTEM_NAMESPACE}"

    # restart containers associated with the controlplane so that new certs are gathered, await controlplane availability
    # note: static pod restarts cannot use kubectl, see
    # https://kubernetes.io/docs/tasks/administer-cluster/kubeadm/kubeadm-certs/#manual-certificate-renewal
    components=(etcd kube-apiserver kube-controller-manager kube-scheduler)
    for container in "${components[@]}" ; do
        cid=$(nerdctl -n k8s.io container ls | grep "${container}" | grep -v pause | awk '{print $1}')
        if [ -z "${cid}" ] ; then
            echo "warning: container id for $container was not found, not restarting"
            continue
        fi
        echo "restarting $container/$cid"
        nerdctl -nk8s.io container kill "${cid}" &>/dev/null
    done

    systemctl restart kubelet
    sleep 5
    echo "await pods available..."
    for container in "${components[@]}" ; do
      retries=0
      while true; do
        if status=$(kubectl get -n kube-system po "${container}-$(hostname)" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null); then
            if [ "$status" = "True" ]; then
                echo "${container}-$(hostname) is Ready"
                break
            fi
        fi
        sleep 1
        retries=$(( retries + 1 ))
        if [ "$retries" -gt 180 ]; then
            echo "error: pod ${container}-$(hostname) not ready within 180s, controlplane likely unhealthy"
            return 1
        fi
      done
    done

    echo "done rotating"
}


if [ "$1" = "rotate_controlplane_certs" ] ; then
    rotate_controlplane_certs
else
    if ! should_rotate ; then
        echo "skip k8s certificate rotation, set env FORCE_ROTATE=1 to override"
        exit 0
    fi

    cd $(dirname "$0")
    source "../pkg-common.sh"

    echo "rotating k8 controlplane certificates. There will be ~30 seconds of downtime on non-HA clusters"
    kubectl get nodes -lnode-role.kubernetes.io/control-plane -oname | cut -d/ -f2 > .cp_nodes.list
    NODE_LIST=.cp_nodes.list PSCP "$THIS_FILE" /tmp/
    for node in $(cat .cp_nodes.list) ; do
        echo "rotating node $node"
        ssh -oStrictHostKeyChecking=no "$node" "bash /tmp/$THIS_FILENAME rotate_controlplane_certs"
    done
fi