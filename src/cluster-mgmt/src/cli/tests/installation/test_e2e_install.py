import json
import logging
import pathlib
from common.context import load_cli_ctx

logger = logging.getLogger("cs_cluster.test.installation.e2e_install")
current_dir = pathlib.Path(__file__).parent.absolute()


def test_0_new_cluster(kinder_cluster: str):
    ctx = load_cli_ctx(None, kinder_cluster)

    # validation on the cert rotation
    _, mod_time_before, _ = ctx.cluster_ctr.must_exec("stat -c '%Y' /etc/kubernetes/pki/apiserver.crt")
    rv, so, se = ctx.cluster_ctr.exec("bash -c 'FORCE_ROTATE=1 /home/pkg/k8s/rotate_certs.sh'")
    assert rv == 0, f"failed to rotate controlplane certs, {so} {se}"

    _, mod_time_after, _ = ctx.cluster_ctr.must_exec("stat -c '%Y' /etc/kubernetes/pki/apiserver.crt")
    assert int(mod_time_before) < int(mod_time_after), "certs were not rotated"
    ctx.cluster_ctr.must_exec("kubectl get nodes")  # ensure api server still works

    # ensure doesn't rotate if not forced to
    rv, so, _ = ctx.cluster_ctr.exec("bash -c '/home/pkg/k8s/rotate_certs.sh'")
    assert rv == 0, f"failed to rotate controlplane certs, {so} {se}"
    _, mod_time_idempotent, _ = ctx.cluster_ctr.must_exec("stat -c '%Y' /etc/kubernetes/pki/apiserver.crt")
    assert mod_time_after == mod_time_idempotent, "certs were rotated but should not have been"


def test_1_upgrade(
        kinder_cluster: str):
    ctx = load_cli_ctx(None, kinder_cluster)
    # update envs to ensure upgrade on cp manifest, haproxy, kubelet
    rv, so, _ = ctx.cluster_ctr.exec("bash -c 'yq -i \".properties.etcd.quotaBackendBytes = \"7589934592\"\" "
                                     "/opt/cerebras/cluster/pkg-properties.yaml'")
    assert rv == 0, f"failed to update pkg props for etcd, {so}"
    rv, so, _ = ctx.cluster_ctr.exec(
        "bash -c 'yq -i \".properties.kubeApiServer.maxRequestsInflight = \"500\"\" "
        "/opt/cerebras/cluster/pkg-properties.yaml'")
    assert rv == 0, f"failed to update pkg props for api-server, {so}"
    rv, so, _ = ctx.cluster_ctr.exec(
        "bash -c 'yq -i \".properties.kubelet.kubeApiQps = \"100\"\" /opt/cerebras/cluster/pkg-properties.yaml'")
    assert rv == 0, f"failed to update pkg props for kubelet, {so}"
    ctx.cluster_ctr.exec_cluster("bash -c 'sed -i \"s/apiserver/test/g\" /etc/haproxy/haproxy.cfg'")

    rv, so, err = ctx.cluster_ctr.exec("bash -c 'K8S_UPGRADE_VERSION=1.31.10 /home/pkg/k8s/k8_init.sh reconcile -y'")
    _, logs, _ = ctx.cluster_ctr.exec("bash -c 'journalctl -u kubelet -S -1m'")
    assert rv == 0, f"failed to upgrade cluster: {err} \n\n\n std: {so} \n\n\n kubelet logs: {logs}"

    rv, so, _ = ctx.cluster_ctr.exec("bash -c 'curl -k https://127.0.0.1:8443/readyz'")
    assert rv == 0, f"failed to check cp node readiness, {so}"
    rv, so, _ = ctx.cluster_ctr.exec("bash -c 'grep max-requests-inflight=500 "
                                     "/etc/kubernetes/manifests/kube-apiserver.yaml'")
    assert rv == 0, f"failed to validate api-server manifest update, {so}"
    rv, so, _ = ctx.cluster_ctr.exec(
        "bash -c 'grep quota-backend-bytes=7589934592 /etc/kubernetes/manifests/etcd.yaml'")
    assert rv == 0, f"failed to validate etcd manifest update, {so}"
    rv, so, _ = ctx.cluster_ctr.exec("bash -c 'grep \"kubeAPIQPS: 100\" /var/lib/kubelet/config.yaml'")
    assert rv == 0, f"failed to validate kubelet config update, {so}"
    rv, so, _ = ctx.cluster_ctr.exec("bash -c 'grep apiserver /etc/haproxy/haproxy.cfg'")
    assert rv == 0, f"failed to validate haproxy update, {so}"

    rv, so, _ = ctx.cluster_ctr.exec("kubectl get no")
    assert rv == 0 and "1.31" in so and "1.29" not in so, so


def test_2_teardown_reinstall(
        kinder_cluster: str):
    ctx = load_cli_ctx(None, kinder_cluster)

    rv, so, err = ctx.cluster_ctr.exec("bash -c '/home/pkg/k8s/k8_init.sh teardown_cluster -y'")
    _, logs, _ = ctx.cluster_ctr.exec("bash -c 'journalctl -u kubelet -S -1m'")
    assert rv == 0, f"failed to teardown cluster: {err} \n\n\n std: {so} \n\n\n kubelet logs: {logs}"

    rv, so, err = ctx.cluster_ctr.exec("bash -c '/home/pkg/k8s/k8_init.sh reconcile -y'")
    _, logs, _ = ctx.cluster_ctr.exec("bash -c 'journalctl -u kubelet -S -1m'")
    assert rv == 0, f"failed to reinstall k8s: {err} \n\n\n std: {so} \n\n\n kubelet logs: {logs}"

    rv, so, _ = ctx.cluster_ctr.exec("bash -c 'curl -k https://127.0.0.1:8443/readyz'")
    assert rv == 0, f"failed to check cp node readiness, {so}"
