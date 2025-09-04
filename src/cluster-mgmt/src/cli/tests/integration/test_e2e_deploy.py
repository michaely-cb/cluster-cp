import contextlib
import copy
import gzip
import ipaddress
import json
import logging
import pathlib
import pytest
import random
import requests
import shlex
import shutil
import subprocess as sp
import tarfile
import textwrap
import time
import zipfile
from functools import wraps
from typing import Callable

import kubernetes.client
import yaml
from click.testing import CliRunner
from click.testing import Result
from kubernetes.client import V1PodList
from kubernetes.client.api import core_v1_api
from tests.integration.conftest import kind_incremental_cluster
from tests.integration.conftest import incr_kind_netjson

from common import DockerImage
from common import NFS_SHARE_DIR
from common import PAUSE_37_IMAGE
from common import REGISTRY_URL
from common.cluster import DockerClusterController
from common.cluster import DirCopyCmd
from common.cluster import FileCopyCmd
from common.cluster import MkdirCmd
from common.context import load_cli_ctx
from common.context import CliCtx
from common.models import Cluster
from common.models import NetworkInterface
from common.models import Node
from common.models import SwitchConfig
from commands.package import _gather_built_wheels
from cs_cluster import cli

from .conftest import CB_DOCKER_CFG
from .conftest import CLI_BASE_DIR
from .conftest import TEST_PULL_IMAGE
from .conftest import use_usernode
from .conftest import _assert_cbcore_image_set
from .conftest import assert_usernode_configured
from .conftest import get_cluster_controlplane_ctr
from .conftest import get_cluster_worker_ctr

logger = logging.getLogger("cs_cluster.test.e2e_deploy")
current_dir = pathlib.Path(__file__).parent.absolute()


@contextlib.contextmanager
def multiple_mgmt_nodes(ctx, k8s: kubernetes.client.ApiClient):
    core_v1 = kubernetes.client.CoreV1Api(k8s)

    # change all nodes to management role in cluster.yaml
    config_map = core_v1.read_namespaced_config_map(name="cluster", namespace="job-operator")
    original_data = config_map.data.copy()
    cluster = yaml.safe_load(config_map.data.get("clusterConfiguration.yaml", "{}"))
    for node in cluster.get("nodes", []):
        node["role"] = "management"
    config_map.data["clusterConfiguration.yaml"] = yaml.safe_dump(cluster)

    core_v1.replace_namespaced_config_map(name="cluster", namespace="job-operator", body=config_map)
    try:
        ctx.exec(
            "kubectl label nodes --all k8s.cerebras.com/node-role-management=''")  # just in case job-operator not deployed
        yield
    finally:
        config_map.data = original_data
        config_map.metadata.resource_version = None
        core_v1.replace_namespaced_config_map(name="cluster", namespace="job-operator", body=config_map)


def test_network_validation_multus(ctx, cs_cluster: Callable[[str], Result]):
    result = cs_cluster("deploy cluster-tools --force")
    assert result.exit_code == 0, result.stdout

    _, so, _ = ctx.cluster_ctr.must_exec("/opt/cerebras/tools/network-validation.sh multus")
    assert "Validating multus success!" in so


def test_multus_nad_generation(ctx):
    exit_code, so, _ = ctx.cluster_ctr.must_exec(
        "bash -c 'CLUSTER_CONFIG=/home/pkg/multus/node/test-cluster.yaml WORKDIR=/home/multus/node "
        "bash /home/pkg/multus/node/node-net-attach-def.sh create_net_attaches'")
    assert exit_code == 0, so

    # inherit range from sw-0/memx
    _, so, _ = ctx.cluster_ctr.must_exec(
        "yq -e '.ipam.range' /home/multus/node/act-0/dest_dir/multus-data-net.conf")
    assert "10.250.50.0/25" in so
    # first nic picked
    _, so, _ = ctx.cluster_ctr.must_exec(
        "yq -e '.master' /home/multus/node/act-0/dest_dir/multus-data-net.conf")
    assert "eth0" in so
    # inherit range from sw-1/memx
    _, so, _ = ctx.cluster_ctr.must_exec(
        "yq -e '.ipam.range' /home/multus/node/act-0/dest_dir/multus-data-net-1.conf")
    assert "10.250.10.0/25" in so
    # second nic picked
    _, so, _ = ctx.cluster_ctr.must_exec(
        "yq -e '.master' /home/multus/node/act-0/dest_dir/multus-data-net-1.conf")
    assert "eth1" in so


def test_k8_init_await_containers_ready(kind_cluster: str, tmp_path):
    ctx = load_cli_ctx(None, kind_cluster)
    pause_pod_str = textwrap.dedent(f"""
    apiVersion: v1
    kind: Pod
    metadata:
        name: pause-pod
        namespace: kube-system
        labels:
            version: v0
    spec:
        containers:
        - name: pause-container
          image: {PAUSE_37_IMAGE}
    """)
    pause_pod = pathlib.Path(tmp_path) / "pause-pod.yaml"
    pause_pod.write_text(pause_pod_str)
    pod_name = f"pause-pod-{ctx.cluster_name}-control-plane"

    # Test case 1: test await without an explict RV change
    ctx.cluster_ctr.exec_cmds([
        FileCopyCmd(ctx.apps_dir / "k8s" / "k8_init_helpers.sh", "/home/k8_init_helpers.sh"),
        FileCopyCmd(ctx.apps_dir / "k8s" / "kube-vip-template.yaml", "/tmp/kube-vip-template.yaml"),
        FileCopyCmd(pause_pod, "/etc/kubernetes/manifests/pause-pod.yaml"),
    ])
    _, so, _ = ctx.cluster_ctr.must_exec(f"/home/k8_init_helpers.sh await_containers_ready {pod_name}")
    logger.info(f"pause-pod restarted once: \n{so}")

    # Test case 2: obtain the current RV and test await with RV after updating the test pod
    _, resource_version, _ = ctx.cluster_ctr.must_exec(
        f"kubectl get pods -n kube-system {pod_name} -ojsonpath='{{.metadata.resourceVersion}}'"
    )
    ctx.cluster_ctr.must_exec("sed -i 's/version: v0/version: v1/g' /etc/kubernetes/manifests/pause-pod.yaml")
    _, so, _ = ctx.cluster_ctr.must_exec(
        f"/home/k8_init_helpers.sh await_containers_ready {pod_name} {resource_version}")
    logger.info(f"pause-pod restarted twice: \n{so}")
    _, next_resource_version, _ = ctx.cluster_ctr.must_exec(
        f"kubectl get pods -n kube-system {pod_name} -ojsonpath='{{.metadata.resourceVersion}}'"
    )
    assert next_resource_version != resource_version

    ctx.cluster_ctr.must_exec("rm -f /etc/kubernetes/manifests/pause-pod.yaml")


def test_deploy_job_operator_user_namespace(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        job_operator_image):
    ctx = load_cli_ctx(None, kind_cluster)
    # add a label which would be overwritten by the deploy command
    label_value = f"{int(time.time())}"
    ctx.cluster_ctr.must_exec(
        f"kubectl label crd wsjobs.jobs.cerebras.com --overwrite pytest={label_value}"
    )

    ctx.cluster_ctr.must_exec("kubectl delete deploy job-operator-controller-manager -njob-operator --ignore-not-found")

    cmd = f"deploy job-operator --namespace pytest --force"
    if job_operator_image:
        cmd += f" --image={job_operator_image}"

    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout

    _, label_value_after, _ = ctx.cluster_ctr.must_exec(
        "kubectl get crd wsjobs.jobs.cerebras.com -ojsonpath='{.metadata.labels.pytest}'"
    )
    assert label_value == label_value_after, "CRD should not be updated"

    rv, _, _ = ctx.cluster_ctr.exec(
        "kubectl get ns pytest "
    )
    assert rv != 0, "should not deploy NSed operator by default"

    rv, _, _ = ctx.cluster_ctr.exec(
        "kubectl get deploy -njob-operator job-operator-controller-manager"
    )
    assert rv == 0, "should deploy system operator by default"


def test_deploy_job_operator_with_orchestration_pkg(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        tmp_path: pathlib.Path):
    """Test the happy path of job-operator deployment with the orchestration package."""
    ctx = load_cli_ctx(None, kind_cluster)
    image_tag = "test-2"
    pkg_name = f"cluster-orchestration-{image_tag}"
    pkg_path = f"{tmp_path}/{pkg_name}"
    ctx.must_exec(f"mkdir -p {pkg_path}")
    result = cs_cluster(f"package job-operator --pkg-path {pkg_path}")
    assert result.exit_code == 0, result.stdout

    # create fake common-images directory
    ctx.must_exec(f"mkdir -p {pkg_path}/common-images/")
    ctx.must_exec(f"touch {pkg_path}/common-images/all_images.txt")
    ctx.must_exec(f"tar cfz {pkg_name}.tar.gz {pkg_name}", cwd=tmp_path)
    # the build directory is under $GITTOP/build
    build_dir = pathlib.Path(__file__).parent.parent.parent.parent.parent.parent.parent / "build"
    ctx.must_exec(f"cp {tmp_path}/{pkg_name}.tar.gz {build_dir}/{pkg_name}.tar.gz")
    result = cs_cluster(f"deploy job-operator --image registry.local/job-operator:{image_tag}")
    assert result.exit_code == 0, result.stdout


def test_deploy_cluster_server_with_cbcore(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        k8s: kubernetes.client.ApiClient,
        cluster_server_image):
    # remove tools beforehand to ensure cluster-volumes.sh is re-installed after cluster-server deploy
    ctx = load_cli_ctx(None, kind_cluster)
    ctx.cluster_ctr.must_exec("rm -rf /opt/cerebras/tools")
    ctx.cluster_ctr.must_exec("yq -i 'del(.properties.customWorker)' /opt/cerebras/cluster/pkg-properties.yaml")

    cbcore_image = f"busybox:test-{random.randint(0, 10000000)}"
    cmd = f"deploy cluster-server --force --cbcore-image={cbcore_image} " \
          f"--pull-ecr-image --disable-user-auth --sdf"
    if cluster_server_image:
        cmd += f" --image={cluster_server_image}"

    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout
    _assert_cbcore_image_set(k8s, cbcore_image)

    netv1 = kubernetes.client.NetworkingV1Api(k8s)
    ingress = netv1.read_namespaced_ingress("cluster-server", "job-operator")
    hosts = [r.host for r in ingress.spec.rules]
    assert "cluster-server.cerebrassc.local" in hosts
    assert "localhost" in hosts

    # check cluster-volumes.sh was re-installed
    exit_code, _, _ = ctx.cluster_ctr.exec("test -x /opt/cerebras/tools/cluster-volumes.sh")
    assert exit_code == 0, "cluster-volumes.sh should have been installed but was not"

    # check custom worker was set properly
    _, so, _ = ctx.cluster_ctr.must_exec(
        "yq '.properties.customWorker.disabled' /opt/cerebras/cluster/pkg-properties.yaml")
    assert so == "false", f"expected customWorker to be enabled but was .properties.customWorker.disabled: {so}"


def test_deploy_cluster_server_with_cbcore_load(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        k8s: kubernetes.client.ApiClient,
        cluster_server_image,
):
    cbcore_image = TEST_PULL_IMAGE
    cmd = f"deploy cluster-server --force --load --cbcore-image={cbcore_image} " \
          f"--pull-ecr-image --disable-user-auth --sdf"
    if cluster_server_image:
        cmd += f" --image={cluster_server_image}"

    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout
    _assert_cbcore_image_set(k8s, REGISTRY_URL + "/" + TEST_PULL_IMAGE.split("/", 1)[-1])


def test_deploy_cluster_server_without_cbcore(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        k8s: kubernetes.client.ApiClient,
        cluster_server_image,
):
    """
    In release 3.0.0, cbcore image is no longer mandated in cluster server deployment.
    We allow end users to specify a cbcore image as a modelzoo argument.
    """
    cmd = f"deploy cluster-server --force " \
          f"--pull-ecr-image --disable-user-auth --sdf"
    if cluster_server_image:
        cmd += f" --image={cluster_server_image}"

    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout
    _assert_cbcore_image_set(k8s, None)


def test_deploy_cluster_server_force_secret_recreate(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        k8s: kubernetes.client.ApiClient,
        cluster_server_image):
    cbcore_image = f"busybox:test-{random.randint(0, 10000000)}"
    cmd = f"deploy cluster-server --recreate-tls-secret --cbcore-image={cbcore_image} " \
          f"--pull-ecr-image --disable-user-auth --sdf"
    if cluster_server_image:
        cmd += f" --image={cluster_server_image}"

    result = cs_cluster(cmd)
    assert result.exit_code == 0
    corev1 = kubernetes.client.CoreV1Api(k8s)
    s0 = corev1.read_namespaced_secret("cluster-server-grpc-secret-v2", "job-operator")

    time.sleep(1)
    result = cs_cluster(cmd)
    assert result.exit_code == 0
    s1 = corev1.read_namespaced_secret("cluster-server-grpc-secret-v2", "job-operator")
    assert s1.metadata.resource_version != s0.metadata.resource_version


def test_deploy_cluster_server_with_orchestration_pkg(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        tmp_path: pathlib.Path):
    """Test the happy path of cluster-server deployment with the orchestration package."""
    ctx = load_cli_ctx(None, kind_cluster)
    image_tag = "test-1"
    pkg_name = f"cluster-orchestration-{image_tag}"
    pkg_path = f"{tmp_path}/{pkg_name}"
    ctx.must_exec(f"mkdir -p {pkg_path}")
    result = cs_cluster(f"package cluster-server --pkg-path {pkg_path} --cbcore {TEST_PULL_IMAGE}")
    assert result.exit_code == 0, result.stdout

    # create fake common-images directory
    ctx.must_exec(f"mkdir -p {pkg_path}/common-images/")
    ctx.must_exec(f"touch {pkg_path}/common-images/all_images.txt")
    ctx.must_exec(f"tar cfz {pkg_name}.tar.gz {pkg_name}", cwd=tmp_path)
    # the build directory is under $GITTOP/build
    build_dir = pathlib.Path(__file__).parent.parent.parent.parent.parent.parent.parent / "build"
    ctx.must_exec(f"cp {tmp_path}/{pkg_name}.tar.gz {build_dir}/{pkg_name}.tar.gz")
    result = cs_cluster(f"deploy cluster-server --image registry.local/cluster-server:{image_tag}")
    assert result.exit_code == 0, result.stdout


def _assert_cleanup(k8s):
    corev1 = core_v1_api.CoreV1Api(k8s)
    alive = []
    for retry in range(3):
        pods: V1PodList = corev1.list_pod_for_all_namespaces()
        for pod in pods.items:
            if pod.status.phase in {"Running", "Pending"} and "image-load" in pod.metadata.name:
                alive.append(pod.metadata.name)
        if not alive:
            return
        time.sleep(retry)  # k8s can take a little time to clean up DS
        alive = []
    assert not alive, f"expected no image-load pods to exist, but found: {alive}"


def test_deploy_image_from_pullable(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str):
    with get_cluster_controlplane_ctr(kind_cluster) as ctr:
        ctr.must_exec(f"sh -c 'rm -rf {NFS_SHARE_DIR}/*.tar'")

        cmd = f"deploy image {TEST_PULL_IMAGE}"
        result = cs_cluster(cmd)
        assert result.exit_code == 0, result.stdout

        ret, so, se = ctr.exec(
            "bash -c 'source /home/pkg/pkg-common.sh >/dev/null && check_image_in_registry busybox 1.34.1'")
        assert ret == 0, f"failed to check image in registry, {so},{se}"

        _, tars, _ = ctr.must_exec(f"find {NFS_SHARE_DIR}/ -name '*.tar'")
        assert len(tars.strip().split()) == 0, f"Expected no tar files in nfs share, found {tars}"


def test_deploy_image_from_pullable_preload(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        k8s: kubernetes.client.ApiClient):
    cmd = f"deploy image {TEST_PULL_IMAGE} --preload"
    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout

    _assert_cleanup(k8s)


def test_deploy_image_from_file(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        tmp_path,
        k8s: kubernetes.client.ApiClient):
    img_path = f"{tmp_path}/image.tar"
    sp.check_call(shlex.split(f"docker image pull {TEST_PULL_IMAGE}"))
    sp.check_call(shlex.split(f"docker image save {TEST_PULL_IMAGE} -o {img_path}"))

    cmd = f"deploy image {img_path} --preload"
    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout

    _assert_cleanup(k8s)


def test_deploy_image_pull_fail_exits(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str):
    result = cs_cluster(f"deploy image {DockerImage.of(TEST_PULL_IMAGE).repo}:not-exists")
    assert result.exit_code != 0


def test_deploy_image_systems_fail_not_configured(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str):
    result = cs_cluster(f"deploy image {TEST_PULL_IMAGE} --systems systemf88")
    assert result.exit_code != 0
    assert "system systemf88 not found" in str(result.exception)


def test_deploy_cluster_tools_multiple_mgmt_nodes(
        cs_cluster: Callable[[str], Result],
        k8s: kubernetes.client.ApiClient,
        kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    with multiple_mgmt_nodes(ctx, k8s):
        ctx.cluster_ctr.exec_cluster("rm -rf /opt/cerebras/tools")

        result = cs_cluster("deploy cluster-tools --force")
        assert result.exit_code == 0

        statuses = ctx.cluster_ctr.exec_cluster("ls -al /opt/cerebras/tools")
        assert len(statuses) > 1, f"did not execute on all nodes, expecting at least 2, got: {statuses.keys()}"
        for node, (rv, so, se,) in statuses.items():
            assert rv == 0, f"node {node} failed to list dir: {so}"
            # check if network/software validation scripts exist
            for file in ["network-validation.sh", "software-validation.sh", "csadm.sh"]:
                assert file in so, f"{node} missing file {file}"

        # double check the files are not empty
        for file in ["network-validation.sh", "software-validation.sh", "csadm.sh"]:
            statuses = ctx.cluster_ctr.exec_cluster(f"stat --printf='%s' /opt/cerebras/tools/{file}")
            assert len(statuses) > 1, f"did not execute on all nodes, expecting at least 2, got: {statuses.keys()}"
            for node, (rv, so, _,) in statuses.items():
                assert rv == 0, f"node {node} file {file} failed to stat: {so}"
                assert int(so) != 0, f"node {node} file {file} is empty"


def test_deploy_image_systems_affinity(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        k8s: kubernetes.client.ApiClient):
    ctx = load_cli_ctx(None, kind_cluster)
    original_doc = ctx.resolve_file("cluster.yaml").read_text()
    cluster_cfg = Cluster(**yaml.safe_load(original_doc))
    sys_name = cluster_cfg.systems[0].name

    result = cs_cluster(f"deploy image {TEST_PULL_IMAGE} --systems {sys_name}")
    assert result.exit_code == 0, result.stdout
    _assert_cleanup(k8s)


def test_deploy_kube_webhook(
        cs_cluster: Callable[[str], Result],
        ctx: CliCtx,
):
    # try avoid deploy all of ceph since the images are very large, just webhook which includes validation
    def deploy_webhook():
        result = cs_cluster("deploy kube-webhook")
        assert result.exit_code == 0, result.stdout_bytes.decode("utf-8")

    ctr = ctx.cluster_ctr
    try:
        ctr.must_exec(
            "yq -i '.properties.kubewebhook.disableSystemNamespaceWebhook=false' "
            "/opt/cerebras/cluster/pkg-properties.yaml")
        deploy_webhook()

        _, so, _ = ctr.must_exec("kubectl get pods -n kube-system -lapp=kube-webhook -oname")
        assert len(so.strip().split("\n")) == 1, f"expected 1 pod deployed, got: {so}"

        _, rv1, _ = ctr.must_exec(
            "kubectl get deploy -n kube-system kube-webhook -ojsonpath='{.metadata.resourceVersion}'")

        # ensure idempotent redeploy
        deploy_webhook()
        _, rv2, _ = ctr.must_exec(
            "kubectl get deploy -n kube-system kube-webhook -ojsonpath='{.metadata.resourceVersion}'")
        assert rv1 == rv2, \
            "expected deployment resource unchanged during re-deploy with no config update but " \
            f"deploy resource version changed {rv1} -> {rv2}"
        rv, _, _ = ctr.exec("kubectl get validatingwebhookconfiguration system-deploy-admission")
        assert rv == 0

        # ensure disabling webhook works as expected
        ctr.must_exec(
            "yq -i '.properties.kubewebhook.disableSystemNamespaceWebhook=true' "
            "/opt/cerebras/cluster/pkg-properties.yaml")
        deploy_webhook()
        _, so, _ = ctr.exec("kubectl get validatingwebhookconfiguration system-deploy-admission -oname")
        assert so == "", so

        # default resources
        _, so, _ = ctr.exec("kubectl describe deploy -nkube-system coredns")
        assert "170Mi" in so, so
        assert "100m" in so, so

        # inject envs
        ctr.must_exec(
            "kubectl set env deploy -nkube-system kube-webhook "
            "memory.kube-system.coredns=200Mi genoa_server=true")
        ctr.must_exec("kubectl rollout status deploy -nkube-system kube-webhook")

        # memory update, cpu remains since not pined to mgmt nodes
        ctr.must_exec("kubectl label deploy -nkube-system coredns foo=bar")
        time.sleep(1)
        _, so, _ = ctr.exec("kubectl describe deploy -nkube-system coredns")
        assert "200Mi" in so, so
        assert "100m" in so, so

        # cpu update after pined to mgmt nodes
        ctr.must_exec(
            "kubectl patch deployment -nkube-system coredns -p "
            "'{\"spec\": {\"template\": {\"spec\": {\"nodeSelector\": {\"node-role.kubernetes.io/control-plane\": \"\"}}}}}'")
        time.sleep(1)
        _, so, _ = ctr.exec("kubectl describe deploy -nkube-system coredns")
        assert "200Mi" in so, so
        assert "50m" in so, so
    finally:
        ctr.must_exec("kubectl delete validatingwebhookconfiguration system-deploy-admission --ignore-not-found")
        ctr.exec("kubectl set env deploy -nkube-system kube-webhook genoa_server=false")


def _create_fake_build_dir(tmp_path: pathlib.Path, csctl_version: str) -> pathlib.Path:
    # create a fake artifacts folder
    artifacts = tmp_path / f"artifacts{csctl_version}"
    artifacts.mkdir()
    with zipfile.ZipFile(artifacts / "cerebras_appliance-0.whl", "w") as zf:
        zf.writestr("cerebras_appliance/bin/raw/csctl", f"echo 'csctl version {csctl_version}-fake'")
    artifacts.joinpath("pytorch-1.whl").write_text("fake wheel 1 contents")
    # make a directory with a wheel and then symlink it in the basedir to test resolution of symlinks
    wheel_dir = artifacts / "wheel_dir"
    wheel_dir.mkdir()
    wheel_dir.joinpath("pytorch-2.whl").write_text("fake wheel 2 contents")
    artifacts.joinpath("pytorch-2-symlink.whl").symlink_to(wheel_dir / "pytorch-2.whl")
    artifacts.joinpath("buildinfo-cbcore.json").write_text(json.dumps({
        "toolchain": str(tmp_path),
        "version": "test",
        "buildid": str(int(time.time())),
    }))
    # create a fake cluster-tools tar.gz file and add csctl to it
    tar_gz_path = artifacts / "cluster-mgmt-tools-x.tar.gz"
    with tarfile.open(tar_gz_path, "w:gz") as tar:
        fp = artifacts / "csctl"
        fp.write_text(f"echo 'csctl version {csctl_version}-fake'")
        tar.add(fp, arcname="csctl")
        fp.unlink()
    return artifacts


def test_deploy_usernode_user_namespace_success(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        cluster_server_image: str,
        tmp_path: pathlib.Path,
        system_namespace: str = "job-operator"):
    ns = "user"
    cmd = f"--cluster {kind_cluster} deploy cluster-server --force " \
          f"--pull-ecr-image --disable-user-auth --namespace={ns}"
    if cluster_server_image:
        cmd += f" --image={cluster_server_image}"

    image = f"busybox:test-{random.randint(0, 10000000)}"
    result = CliRunner().invoke(cli, shlex.split(cmd), env={"WSJOB_IMAGE": image})
    assert result.exit_code == 0

    ctx = load_cli_ctx(None, kind_cluster)
    with use_usernode(ctx) as host_port:
        dockername, port = host_port

        # deploy fake resources to a fake usernode
        artifacts = _create_fake_build_dir(tmp_path, "0.0.0")
        result = cs_cluster(
            f"deploy usernode --hosts localhost --port {port} --password='' "
            f"--release-artifact-path {artifacts.absolute()} --namespace={ns}")
        assert result.exit_code == 0, result

        assert_usernode_configured(dockername, kind_cluster, [ns])

        so = ctx.must_exec(f"docker exec -t {dockername} ls -1 /usr/local/bin")
        files = [l.strip() for l in so.strip().split('\n')]
        assert "csctl" not in files
        assert "csctl0.0" not in files

        so = ctx.must_exec(f"docker exec -t {dockername} ls -1 /opt/cerebras/tools")
        files = [l.strip() for l in so.strip().split('\n')]
        assert "csctl-0.0.md" not in files
        assert "csctl-reference-0.0.md" not in files

        # should work if called again
        result = cs_cluster(
            f"deploy usernode --hosts localhost --port {port} --password='' "
            f"--release-artifact-path {artifacts.absolute()} --namespace={ns}")
        assert result.exit_code == 0, result.stdout

    with use_usernode(ctx) as host_port:
        dockername, port = host_port
        # deploy fake resources to a fake usernode
        artifacts = _create_fake_build_dir(tmp_path, "2.1.0")
        result = cs_cluster(
            f"deploy usernode --hosts localhost --port {port} --password='' "
            f"--release-artifact-path {artifacts.absolute()} --namespace={ns}")
        assert result.exit_code == 0, result

        assert_usernode_configured(dockername, kind_cluster, [ns])

        so = ctx.must_exec(f"docker exec -t {dockername} ls -1 /usr/local/bin")
        files = [l.strip() for l in so.strip().split('\n')]
        assert "csctl" in files
        assert "csctl2.1" in files

        so = ctx.must_exec(f"docker exec -t {dockername} ls -1 /opt/cerebras/tools")
        files = [l.strip() for l in so.strip().split('\n')]
        assert "csctl-2.1.md" in files
        assert "csctl-reference-2.1.md" in files

        # should work if called again
        result = cs_cluster(
            f"deploy usernode --hosts localhost --port {port} --password='' "
            f"--release-artifact-path {artifacts.absolute()} --namespace={ns}")
        assert result.exit_code == 0, result.stdout


def test_deploy_usernode_system_namespace_success(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        tmp_path: pathlib.Path,
        use_custom_cluster_cm,
        system_namespace: str = "job-operator"):
    ctx = load_cli_ctx(None, kind_cluster)
    with use_usernode(ctx) as host_port:
        dockername, port = host_port

        # deploy fake resources to a fake usernode
        artifacts = _create_fake_build_dir(tmp_path, "0.0.0")
        result = cs_cluster(
            f"deploy usernode --hosts localhost --port {port} --password='' "
            f"--release-artifact-path {artifacts.absolute()}")
        assert result.exit_code == 0, result

        assert_usernode_configured(dockername, kind_cluster, [system_namespace])

        # check that the resources were deployed
        so = ctx.must_exec(f"docker exec -t {dockername} ls -1 /opt/cerebras/wheels")
        files = [l.strip() for l in so.strip().split('\n')]
        assert "cerebras_appliance-0.whl" in files
        assert "pytorch-1.whl" in files
        assert "pytorch-2-symlink.whl" in files
        so = ctx.must_exec(f"docker exec -t {dockername} cat /opt/cerebras/wheels/pytorch-2-symlink.whl")
        assert "fake wheel 2 contents" in so

        so = ctx.must_exec(f"docker exec -t {dockername} ls -1 /usr/local/bin")
        files = [l.strip() for l in so.strip().split('\n')]
        assert "csctl" in files
        assert "csctl0.0" in files

        so = ctx.must_exec(f"docker exec -t {dockername} ls -1 /opt/cerebras/tools")
        files = [l.strip() for l in so.strip().split('\n')]
        assert "csctl-0.0.md" in files
        assert "csctl-reference-0.0.md" in files

        # should work if called again
        result = cs_cluster(
            f"deploy usernode --hosts localhost --port {port} --password='' "
            f"--release-artifact-path {artifacts.absolute()}")
        assert result.exit_code == 0, result.stdout

        so = ctx.must_exec(f"docker exec -t {dockername} cat /etc/security/limits.conf")
        assert so.count("* hard nofile 20000") == 1

        # validate that an unreachable mgmt IP fails validation
        with use_custom_cluster_cm() as (cluster, update,):
            cluster.nodes.append(
                Node("mock-mgmt", role="management", networkInterfaces=[NetworkInterface("enomock", "10.240.1.126")]))
            update(cluster)
            result = cs_cluster(f"deploy usernode --hosts=localhost --port={port} --password='' --config-only")
            logger.info(result)
            assert result.exit_code == 1, f"expected failure to deploy usernode but was success, {result.stdout}"
            assert "error: failed to ping" in str(result.exception)


def test_stage_usernode_skip_wheels(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        system_namespace: str = "job-operator"):
    ctx = load_cli_ctx(None, kind_cluster)
    ctx.cluster_ctr.must_exec("bash -c 'rm -rf /opt/cerebras/packages'")

    with use_usernode(ctx) as host_port:
        dockername, port = host_port
        with DockerClusterController([dockername]) as ctr:
            ctr.must_exec("rm -rf /opt/cerebras/wheels")

            result = cs_cluster(f"deploy usernode --hosts=localhost --port={port} --password='' --config-only")
            assert result.exit_code == 0, f"failed to deploy usernode {result.exit_code}, {result.stdout}"

            ctr.must_exec("[ ! -d /opt/cerebras/wheels ]")
        assert_usernode_configured(dockername, kind_cluster, [system_namespace])


def test_stage_usernode_validate_ip_range(
        cs_cluster: Callable[[str], Result],
        use_custom_cluster_cm,
        use_cilium_ip_range,
        ctx: CliCtx):
    ctx.cluster_ctr.must_exec("bash -c 'rm -rf /opt/cerebras/packages'")

    with use_usernode(ctx) as host_port:
        dockername, port = host_port
        with DockerClusterController([dockername]) as ctr:
            _, so, _ = ctr.must_exec("hostname -i")
            usernode_ip = ipaddress.ip_address(so.strip())

            # set cluster ip multus range as including the usernode's ip and ensure install fails
            # subnets don't reall
            with use_custom_cluster_cm() as (cluster, update,):
                cluster.groups[0].switchConfig = SwitchConfig(
                    subnet="10.0.0.0/16",
                    gateway="10.1.0.254",
                    parentnet="10.1.0.0/16",
                    virtualStart=str(usernode_ip - 10),
                    virtualEnd=str(usernode_ip + 10),
                )
                update(cluster)
                result = cs_cluster(f"deploy usernode --hosts=localhost --port={port} --password='' --config-only")
                logger.info(result)
                assert result.exit_code == 1, f"expected failure to deploy usernode but was success, {result.stdout}"
                assert "usernode IP must fall outside this reserved range" in str(result.exception)
                assert str(usernode_ip - 10) in str(result.exception)

            # set a cilium range that contains the usernode IP
            fake_net_addr = ipaddress.ip_address(int(ipaddress.ip_address(usernode_ip)) >> 8 << 8)
            fake_range = str(fake_net_addr) + "/24"
            with use_cilium_ip_range(fake_range):
                result = cs_cluster(f"deploy usernode --hosts=localhost --port={port} --password='' --config-only")
                logger.info(result)
                assert result.exit_code == 1, f"expected failure to deploy usernode but was success, {result.stdout}"
                assert "usernode IP must fall outside this reserved range" in str(result.exception)
                assert str(fake_net_addr) in str(result.exception)


def test_stage_usernode_skip_config_parallel(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        system_namespace: str = "job-operator"):
    # mimic jenkins parallel build
    ctx = load_cli_ctx(None, kind_cluster)
    ctx.cluster_ctr.must_exec("bash -c 'rm -rf /opt/cerebras/packages'")

    with use_usernode(ctx) as host_port:
        dockername, port = host_port
        cmd = str(f"python3 '{CLI_BASE_DIR}/cs_cluster.py' -v --cluster {kind_cluster} "
                  f"deploy usernode --hosts=localhost --port={port} --password='' --config-only")
        pcmd = " & ".join([cmd] * 4) + " & wait"

        try:
            sp.check_output(pcmd, shell=True, stderr=sp.STDOUT)
        except sp.CalledProcessError as e:
            logger.error(f"failed to run {cmd} in parallel: {e.output}")
            raise

        assert_usernode_configured(dockername, kind_cluster, [system_namespace])


def disable_built_wheels(func):
    # in CI, we need to ensure that wheels don't appear to be present if they're
    # built at an earlier stage
    @wraps(func)
    def wrapper(*args, **kwargs):
        undo_rename = []
        for path in _gather_built_wheels():
            disabled_path = path.with_suffix(".whl.disabled")
            path.rename(disabled_path)
            undo_rename.append(dict(src=disabled_path, dst=path))
        try:
            return func(*args, **kwargs)
        finally:
            for paths in undo_rename:
                paths["src"].rename(paths["dst"])

    return wrapper


@disable_built_wheels
def test_stage_usernode_without_wheels_succeeds(cs_cluster: Callable[[str], Result], kind_cluster: str,
                                                system_namespace: str = "job-operator"):
    ctx = load_cli_ctx(None, kind_cluster)
    ctx.cluster_ctr.must_exec("bash -c 'rm -rf /opt/cerebras/packages/user-pkg*'")

    with use_usernode(ctx) as host_port:
        dockername, port = host_port
        result = cs_cluster(f"deploy usernode --hosts=localhost --port={port} --password=''")
        assert result.exit_code == 0, result.stdout

        assert_usernode_configured(dockername, kind_cluster, [system_namespace])


def test_deploy_log_rotation(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str):
    result = cs_cluster(f"deploy log-rotation")
    assert result.exit_code == 0, result.stdout

    ctx = load_cli_ctx(None, kind_cluster)
    ctx.cluster_ctr.must_exec("stat /etc/cron.hourly/log-cleanup")
    ctx.cluster_ctr.must_exec("stat /etc/cron.hourly/registry-cleanup")


def test_deploy_static_registry(kind_cluster: str):
    # delete deployed registry if it exists
    ctx = load_cli_ctx(None, kind_cluster)
    ctx.cluster_ctr.must_exec("kubectl delete -n kube-system ds private-registry --ignore-not-found")

    rv, stdout, stderr = ctx.cluster_ctr.exec("bash -c 'FORCE_STATIC=1 /home/pkg/registry/install.sh'")
    assert rv == 0, f"{stdout}\n{stderr}"
    ctx.cluster_ctr.must_exec("nerdctl -n k8s.io container inspect registry")

    # static registry deploy should be idempotent
    rv, stdout, stderr = ctx.cluster_ctr.exec("bash -c 'FORCE_STATIC=1 /home/pkg/registry/install.sh'")
    assert rv == 0, f"{stdout}\n{stderr}"
    ctx.cluster_ctr.must_exec("nerdctl -n k8s.io container inspect registry")

    # reinstall private registry
    rv, stdout, stderr = ctx.cluster_ctr.exec('/home/pkg/registry/install.sh')
    assert rv == 0, f"{stdout}\n{stderr}"
    rv, stdout, _ = ctx.cluster_ctr.exec("nerdctl -n k8s.io container inspect registry")
    assert rv == 1, stdout


def test_deploy_colovore_secrets_cron(cs_cluster: Callable[[str], Result], kind_cluster: str):
    # mimic a real colovore token if it's not mounted
    ctr = DockerClusterController([f"{kind_cluster}-control-plane"]).open()
    try:
        ctr.must_exec(f"sh -c 'test -s {CB_DOCKER_CFG} || echo \"{{}}\" > {CB_DOCKER_CFG}'")
        result = cs_cluster(f'deploy colovore-secrets-cron')
        assert result.exit_code == 0

        def check_cronjobs() -> bool:
            status, stdout, _ = ctr.must_exec("kubectl get cronjob -n job-operator -o yaml")
            if status != 0:
                return False
            ## FIXME: add back a check for the nfs server address (either pass in an override, or mock it some other way)
            if "regcred-refresh" not in stdout: ## or "cerebras-storage" not in stdout:
                return False
            return True

        wait_for_condition(check_cronjobs, error_message="Timed out waiting for cronjobs to be created")
    finally:
        ctr.close()


def test_deploy_image_tar_file(cs_cluster: Callable[[str], Result], kind_cluster: str, tmp_path: pathlib.Path):
    pull_default_image(tmp_path)
    bb_tar = f"bb:latest-{random.randint(0, 100000)}.tar"
    with open(f"{tmp_path}/{bb_tar}", "wb") as f:
        sp.check_call(f"docker save {TEST_PULL_IMAGE}".split(), stdout=f)
    result = cs_cluster(f"deploy image {tmp_path}/{bb_tar}")
    assert result.exit_code == 0


def test_deploy_image_targz_file(cs_cluster: Callable[[str], Result], kind_cluster: str, tmp_path: pathlib.Path):
    pull_default_image(tmp_path)
    # .docker = tar.gz
    bb = f"bb:latest-{random.randint(0, 100000)}"
    with open(f"{tmp_path}/{bb}.tar", "wb") as f:
        sp.check_call(f"docker save {TEST_PULL_IMAGE}".split(), stdout=f)
    with open(f"{tmp_path}/{bb}.tar", 'rb') as f_in:
        with gzip.open(f'{tmp_path}/{bb}.docker', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    result = cs_cluster(f"deploy image {tmp_path}/{bb}.docker")
    assert result.exit_code == 0


def test_deploy_log_scrapping(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    cmd = f"deploy log-scraping --force"
    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout

    ctx.cluster_ctr.must_exec(f"sh -c 'helm status loki -nloki|grep deployed'")
    ctx.cluster_ctr.must_exec(f"sh -c 'helm status fluent-bit -nfluent-bit|grep deployed'")


def test_deploy_prometheus(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    cmd = f"deploy prometheus --force"
    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout

    ctx.cluster_ctr.must_exec(f"sh -c 'helm status prometheus -nprometheus|grep deployed'")
    # Remove deployment packages to free up space
    _, usage, _ = ctx.cluster_ctr.must_exec(f"du -sh /home/pkg/prometheus")
    logger.info(f"Prometheus pkg disk usage: {usage}")
    ctx.cluster_ctr.must_exec(f"rm -rf /home/pkg/prometheus")


def test_deploy_cpingmesh(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    cmd = f"deploy cpingmesh --force"
    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout

    # Remove deployment packages to free up space
    _, usage, _ = ctx.cluster_ctr.must_exec(f"du -sh /home/pkg/cpingmesh")
    logger.info(f"Cpingmesh pkg disk usage: {usage}")
    ctx.cluster_ctr.must_exec(f"rm -rf /home/pkg/cpingmesh")


@pytest.mark.skip(reason="jnode disk is too small, 50G only")
def test_deploy_ceph(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    cmd = f"deploy ceph --force"
    result = cs_cluster(cmd)
    _, logs, _ = ctx.cluster_ctr.exec(
        "bash -c 'lsblk && df -h && du -sh /home/ && nerdctl -nk8s.io images && kubectl describe node'")
    logger.info(f"ceph install logs: {logs}")
    assert result.exit_code == 0, f"failed to install ceph: {result.stdout}, logs: {logs}"


@pytest.mark.skip(reason="jnode disk is too small, 50G only")
def test_registry_sync(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    cmd = f"deploy registry-sync --force"
    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout


def test_deploy_debugviz_server(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    # TODO: Update to use production build when available
    debugviz_img_file = "/cb/artifacts/builds/cbcore/202508302329-5547-7c87d670/debugviz-0.0.0-202508302329-5547-7c87d670.docker"
    cmd = f"deploy debugviz-server --force --image-file {debugviz_img_file}"
    result = cs_cluster(cmd)
    assert result.exit_code == 0, result.stdout

    ctx.cluster_ctr.must_exec(f"sh -c 'helm status debugviz-server -njob-operator|grep deployed'")


def test_csadm_preflight_ok(ctx: CliCtx, cs_cluster: Callable[[str], Result]):
    result = cs_cluster("deploy --preflight cluster-tools --force")  # any package will do
    assert result.exit_code == 0, result.stdout


def test_csadm_preflight_pkg_properties_not_set(cs_cluster: Callable[[str], Result], kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    ctx.cluster_ctr.must_exec("sh -c 'rm -f /home/pkg/manifest.*'")

    assert 0 == cs_cluster("deploy cluster-tools --force").exit_code  # ensure csadm.sh is present
    ctx.cluster_ctr.must_exec("rm -f /home/pkg/internal-pkg-properties.yaml "
                              "/home/pkg/cluster-pkg-properties.yaml.template "
                              "/opt/cerebras/cluster/pkg-properties.yaml")

    # now running csadm preflight should fail
    rv, stdout, stderr = (
        ctx.cluster_ctr.exec("sh -c 'cd /home/pkg && ./csadm.sh install manifest.json.* --preflight --yes'"))
    assert rv != 0, f"{stdout}\n{stderr}"
    assert "pkg-properties.yaml" in stderr, f"{stdout}\n{stderr}"


def test_csadm_get_scaled_capacity(cs_cluster: Callable[[str], Result], kind_cluster: str):
    with get_cluster_controlplane_ctr(kind_cluster) as ctr:
        ctr.exec_cmds([
            MkdirCmd("/home/common"),
            DirCopyCmd(f"{CLI_BASE_DIR}/apps/common", "/home/common")
        ])

        _, so, se = ctr.must_exec(
            "bash -c 'source /home/common/pkg-common.sh >/dev/null && get_cluster_size_scaled_capacity 30Gi'")
        assert so.strip() == "30Gi", f"{so},{se}"

        cluster_yaml = 'systems: [' + ",".join(["0" for _ in range(64)]) + "]"
        ctr.must_exec(f"bash -c \"echo '{cluster_yaml}' > /home/sys-64.yaml\"")
        _, so, se = ctr.must_exec(
            "bash -c 'source /home/common/pkg-common.sh >/dev/null && CLUSTER_CONFIG=/home/sys-64.yaml get_cluster_size_scaled_capacity 500m'")
        assert so.strip() == "500m", f"{so},{se}"

        cluster_yaml = 'systems: [' + ",".join(["0" for _ in range(65)]) + "]"
        ctr.must_exec(f"bash -c \"echo '{cluster_yaml}' > /home/sys-65.yaml\"")
        _, so, se = ctr.must_exec(
            "bash -c 'source /home/common/pkg-common.sh >/dev/null && CLUSTER_CONFIG=/home/sys-65.yaml get_cluster_size_scaled_capacity 6'")
        assert so.strip() == "8", f"{so},{se}"

        cluster_yaml = 'systems: [' + ",".join(["0" for _ in range(81)]) + "]"
        ctr.must_exec(f"bash -c \"echo '{cluster_yaml}' > /home/sys-81.yaml\"")
        _, so, se = ctr.must_exec(
            "bash -c 'source /home/common/pkg-common.sh >/dev/null && CLUSTER_CONFIG=/home/sys-81.yaml get_cluster_size_scaled_capacity 3Gi'")
        assert so.strip() == "5Gi", f"{so},{se}"
        _, so, se = ctr.must_exec(
            "bash -c 'source /home/common/pkg-common.sh >/dev/null && CLUSTER_CONFIG=/home/sys-81.yaml get_apiserver_maxRequestsInflight'")
        assert so.strip() == "1200", f"{so},{se}"
        _, so, se = ctr.must_exec(
            "bash -c 'source /home/common/pkg-common.sh >/dev/null && CLUSTER_CONFIG=/home/sys-81.yaml get_apiserver_maxMutatingRequestsInflight'")
        assert so.strip() == "600", f"{so},{se}"
        _, so, se = ctr.must_exec(
            "bash -c 'source /home/common/pkg-common.sh >/dev/null && CLUSTER_CONFIG=/home/sys-81.yaml _get_scaled_registry_pvc_size'")
        assert so.strip() == "300Gi", f"{so},{se}"


def test_csadm_select_upgrade_nodes(cs_cluster: Callable[[str], Result],
                                    kind_cluster: str, system_namespace="job-operator"):
    ctx = load_cli_ctx(None, kind_cluster)
    fp = "/home/upgrade-nodes"

    # pick nodes cluster wide
    ctx.cluster_ctr.must_exec(f"csctl session update {system_namespace} --system-count=0 -y")
    rv, _, err = ctx.cluster_ctr.exec(f"bash -c 'select_nodes_fp={fp} csadm.sh select-upgrade-nodes fake'")
    assert rv == 0, err
    for no in ctx.cluster_cfg.nodes:
        ctx.cluster_ctr.must_exec(f"bash -c 'cat {fp} && grep {no.name} {fp}'")

    # pick nodes for session
    ctx.cluster_ctr.must_exec(f"csctl session update {system_namespace} --system-count=2 -y")
    rv, _, err = ctx.cluster_ctr.exec(f"bash -c "
                                      f"'select_nodes_fp={fp} csadm.sh select-upgrade-nodes fake {system_namespace}'")
    assert rv == 0, err
    for no in ctx.cluster_cfg.nodes:
        ctx.cluster_ctr.must_exec(f"bash -c 'cat {fp} && grep {no.name} {fp}'")
    ctx.cluster_ctr.must_exec(f"kubectl delete wsjob --all -n{system_namespace}")


def test_csadm_update_health(cs_cluster: Callable[[str], Result], kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)

    # expect failure due to missing error msg
    rv, _, _ = ctx.cluster_ctr.exec(f"csadm.sh update-node --name={ctx.cluster_ctr.control_plane_name()} --state=error --user='test'")
    assert rv != 0
    #expect failure due to missing username
    rv, _, _ = ctx.cluster_ctr.exec(f"csadm.sh update-node --name={ctx.cluster_ctr.control_plane_name()} --state=error --note='test only'")
    assert rv != 0
    # update node to err
    ctx.cluster_ctr.must_exec(f"csadm.sh update-node --name={ctx.cluster_ctr.control_plane_name()} "
                              f"--state=error --note='test only' --user='test'")
    # cluster status updated
    ctx.cluster_ctr.must_exec(f"sh -c 'csctl get cluster --error-only|grep {ctx.cluster_ctr.control_plane_name()}'")
    # update node to ok
    ctx.cluster_ctr.must_exec(f"csadm.sh update-node --name={ctx.cluster_ctr.control_plane_name()} --state=ok")
    # cluster error cleared
    rv, _, _ = ctx.cluster_ctr.exec(
        f"sh -c 'csctl get node --error-only|grep {ctx.cluster_ctr.control_plane_name()}'")
    assert rv != 0
    # expect failure due to non-existing node
    rv, _, _ = ctx.cluster_ctr.exec(f"csadm.sh update-node --name=fake --state=error --note='test only' --user='test'")
    assert rv != 0

    # expect failure due to missing error msg
    rv, _, _ = ctx.cluster_ctr.exec(f"csadm.sh update-node --name={ctx.cluster_ctr.control_plane_name()} "
                                    f"--nic=eth0 --state=error --user='test'")
    assert rv != 0
    # expect failure due to missing username
    rv, _, _ = ctx.cluster_ctr.exec(f"csadm.sh update-node --name={ctx.cluster_ctr.control_plane_name()} "
                                    f"--nic=eth0 --state=error --note='test only'")
    assert rv != 0
    # update node NIC to err
    ctx.cluster_ctr.must_exec(
        f"csadm.sh update-node --name={ctx.cluster_ctr.control_plane_name()} "
        f"--nic=eth0 --state=error --note='test only' --user='test'")
    # cluster status updated
    ctx.cluster_ctr.exec("sh -c 'csctl get cluster --error-only|grep eth0'")
    assert rv != 0
    # update node NIC to ok
    ctx.cluster_ctr.must_exec(
        f"csadm.sh update-node --name={ctx.cluster_ctr.control_plane_name()} --nic=eth0 --state=ok")
    # cluster error cleared
    rv, _, _ = ctx.cluster_ctr.exec(
        f"sh -c 'csctl get cluster --error-only|grep {ctx.cluster_ctr.control_plane_name()}'")
    assert rv != 0
    # expect failure due to non-existing nic
    rv, _, _ = ctx.cluster_ctr.exec(
        f"csadm.sh update-node --name={ctx.cluster_ctr.control_plane_name()} --nic=eth1 "
        f"--state=error --note='test only' --user='test'")

    # expect failure due to missing error msg
    rv, _, _ = ctx.cluster_ctr.exec(f"csadm.sh update-system --name=kapi-0 "
                                    f"--nic=eth0 --state=error --user='test'")
    assert rv != 0
    # expect failure due to missing username
    rv, _, _ = ctx.cluster_ctr.exec(f"csadm.sh update-system --name=kapi-0 "
                                    f"--nic=eth0 --state=error --note='test only'")
    assert rv != 0
    # update system to err
    ctx.cluster_ctr.must_exec(f"csadm.sh update-system --name=kapi-0 --state=error --note='test only' --user='test'")
    # cluster status updated
    ctx.cluster_ctr.must_exec("sh -c 'csctl get cluster --error-only|grep kapi-0'")
    # update system to ok
    ctx.cluster_ctr.must_exec(f"csadm.sh update-system --name=kapi-0 --state=ok")
    # cluster error cleared
    rv, _, _ = ctx.cluster_ctr.exec("sh -c 'csctl get cluster --error-only|grep kapi-0'")
    assert rv != 0
    # expect failure due to non-existing system
    rv, _, _ = ctx.cluster_ctr.exec(f"csadm.sh update-system --name=fake --state=error --note='test only' --user='test'")

    # expect failure due to missing error msg
    rv, _, _ = ctx.cluster_ctr.exec(f"csadm.sh update-system --name=kapi-0 "
                                    f"--port=n01  --state=error --user='test'")
    assert rv != 0
    # expect failure due to missing username
    rv, _, _ = ctx.cluster_ctr.exec(f"csadm.sh update-system --name=kapi-0 "
                                    f"--port=n01  --state=error --note='test only'")
    assert rv != 0
    # update system port to err
    ctx.cluster_ctr.must_exec("csadm.sh update-system --name=kapi-0 --port=n01 --state=error --note='test only' --user='test'")
    # cluster status updated
    ctx.cluster_ctr.must_exec("sh -c 'csctl get cluster --error-only|grep n01'")
    # update system port to ok
    ctx.cluster_ctr.must_exec("csadm.sh update-system --name=kapi-0 --port=n01 --state=ok")
    # cluster error cleared
    rv, _, _ = ctx.cluster_ctr.exec("sh -c 'csctl get cluster --error-only|grep n01'")
    assert rv != 0
    # expect failure due to non-existing port
    rv, _, _ = ctx.cluster_ctr.exec("csadm.sh update-system --name=kapi-0 --port=n1 --state=error --note='test only' --user='test'")
    assert rv != 0
    rv, _, _ = ctx.cluster_ctr.exec("csadm.sh update-system --name=kapi-0 --port=n12 --state=error --note='test only' --user='test'")
    assert rv != 0
    rv, _, _ = ctx.cluster_ctr.exec("csadm.sh update-system --name=kapi-0 --port=n012 --state=error --note='test only' --user='test'")
    assert rv != 0

    # Test case: Set node to state ok with an error message and verify the message is present
    test_error_message = "test message - state is ok"
    ctx.cluster_ctr.must_exec(f"csadm.sh update-node --name={ctx.cluster_ctr.control_plane_name()} "
                              f"--state=ok --note='{test_error_message}'")
    # Verify the error message is present in the node status
    _, stdout, _ = ctx.cluster_ctr.must_exec("csctl get node")
    assert test_error_message in stdout, f"Expected error message '{test_error_message}' not found in node status"

    # Test case: Set system to state ok with an error message and verify the message is present
    ctx.cluster_ctr.must_exec(f"csadm.sh update-system --name=kapi-0 --state=ok --note='{test_error_message}'")

    # Verify the error message is present in the system status
    _, stdout, _ = ctx.cluster_ctr.must_exec("csctl get system")
    assert test_error_message in stdout, f"Expected error message '{test_error_message}' not found in system status"




def test_csadm_update_nodegroup(cs_cluster: Callable[[str], Result], kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    nodegroup_name = ctx.cluster_cfg_k8s.groups[0].name
    _, nodegroup_node_names_str, _ = ctx.cluster_ctr.must_exec(
        f"sh -c 'csctl get nodegroup {nodegroup_name} -o yaml | yq -r \'.status.roleStatus[]?.names[]?\' | sort -u'")
    # Split the newline-separated string into a list of node names
    nodegroup_node_names = [
        name.strip() for name in nodegroup_node_names_str.split('\n') if name.strip()]

    # expect failure due to non-existing nodegroup
    rv, _, _ = ctx.cluster_ctr.exec(
        f"csadm.sh update-nodegroup --name=fake --state=error --note='test only' --user='test'")
    assert rv != 0

    # expect failure due to missing error note
    rv, _, _ = ctx.cluster_ctr.exec(
        f"csadm.sh update-nodegroup --name={nodegroup_name} --state=error")
    assert rv != 0

    # update nodegroup to error state
    ctx.cluster_ctr.must_exec(f"csadm.sh update-nodegroup --name={nodegroup_name} "
                              f"--state=error --note='test only' --user='test'")

    # expect all nodegroup nodes to be in error state
    for node_name in nodegroup_node_names:
        ctx.cluster_ctr.must_exec(
            f"sh -c 'csctl get node --error-only | grep {node_name}'")

    # update nodegroup to ok state
    ctx.cluster_ctr.must_exec(
        f"csadm.sh update-nodegroup --name={nodegroup_name} --state=ok")

    # expect all nodegroup nodes to be in ok state
    for node_name in nodegroup_node_names:
        rv, _, _ = ctx.cluster_ctr.exec(
            f"sh -c 'csctl get node --error-only | grep {node_name}'")
        assert rv != 0


def test_csadm_update_namespace(cs_cluster: Callable[[str], Result], kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    ctx.cluster_ctr.must_exec("csadm.sh create-namespace --name=test-0")

    # release all systems
    ctx.cluster_ctr.must_exec("csadm.sh update-namespace --name=job-operator --mode=remove-all")

    # create ns with all systems
    ctx.cluster_ctr.must_exec("csadm.sh update-namespace --name=test-0 --systems=kapi-0,kapi-1")

    # create ns without systems
    ctx.cluster_ctr.must_exec("csadm.sh create-namespace --name=test-1")

    # expect update failure due to conflicts
    rv, _, _ = ctx.cluster_ctr.exec("csadm.sh update-namespace --name=test-1 --mode=append-all")
    assert rv != 0

    # release one system
    ctx.cluster_ctr.must_exec("csadm.sh update-namespace --name=test-0 --systems=kapi-1 --mode=remove")

    # check assign status
    ctx.cluster_ctr.must_exec("sh -c 'csadm.sh get-namespace --name=test-0|grep kapi-0'")
    ctx.cluster_ctr.must_exec("sh -c '[ ! $(csadm.sh get-namespace --name=test-0|grep kapi-1) ]'")

    # release all systems
    ctx.cluster_ctr.must_exec("csadm.sh update-namespace --name=test-0 --mode=remove-all")
    ctx.cluster_ctr.must_exec("sh -c '[ ! $(csadm.sh get-namespace --name=test-0|grep kapi-0) ]'")

    # assign all
    ctx.cluster_ctr.must_exec("csadm.sh update-namespace --name=test-1 --mode=append-all")
    ctx.cluster_ctr.must_exec("sh -c 'csadm.sh get-namespace --name=test-1|grep kapi-0'")
    ctx.cluster_ctr.must_exec("sh -c 'csadm.sh get-namespace --name=test-1|grep kapi-1'")

    # delete ns and auto release systems
    ctx.cluster_ctr.must_exec("csadm.sh delete-namespace --name=test-1")

    # deleted ns not found
    rv, _, _ = ctx.cluster_ctr.exec("csadm.sh get-namespace --name=test-1")
    assert rv != 0

    # assign released system from deleted ns
    ctx.cluster_ctr.must_exec("csadm.sh update-namespace --name=test-0 --mode=append-all")
    ctx.cluster_ctr.must_exec("sh -c 'csadm.sh get-namespace --name=test-0|grep kapi-0'")
    ctx.cluster_ctr.must_exec("sh -c 'csadm.sh get-namespace --name=test-0|grep kapi-1'")

    ctx.cluster_ctr.must_exec("csadm.sh delete-namespace --name=test-0")
    ctx.cluster_ctr.must_exec("csadm.sh update-namespace --name=job-operator --mode=append-all")


def test_not_ready_node_deploy(cs_cluster: Callable[[str], Result], kind_cluster: str, cluster_server_image):
    with get_cluster_controlplane_ctr(kind_cluster) as ctr:
        with get_cluster_worker_ctr(kind_cluster) as worker:

            # Remove the existing ClusterDeployNotAppliedClusterServer condition from the worker node.
            _, index, _ = ctr.must_exec(
                f"bash -c \"kubectl get node {kind_cluster}-worker -ojson | jq -r '.status.conditions | "
                "map(.type) | index(\\\"ClusterDeployNotAppliedClusterServer\\\") // empty'\""
            )
            ctr.must_exec(
                f"bash -c \"kubectl patch node {kind_cluster}-worker --type='json' --subresource='status' "
                f"-p='[{{'op': 'remove', 'path': '/status/conditions/{index}'}}]'\""
            )

            worker.must_exec("systemctl stop kubelet")
            # It takes about 40 seconds for NotReady state to show up.
            time.sleep(41)

            cbcore_image = f"busybox:test-{random.randint(0, 10000000)}"
            cmd = f"deploy cluster-server --force --cbcore-image={cbcore_image} " \
                  f"--pull-ecr-image --disable-user-auth --sdf"
            if cluster_server_image:
                cmd += f" --image={cluster_server_image}"

            result = cs_cluster(cmd)
            assert result.exit_code == 0, result.stdout

            # The worker node should not have condition `ClusterDeployNotAppliedClusterServer`.
            _, so, _ = ctr.must_exec(
                f"bash -c \"kubectl get node {kind_cluster}-worker -ojson | jq -r '.status.conditions[] | "
                f"select(.type == \\\"ClusterDeployNotAppliedClusterServer\\\") | .status'\""
            )
            assert so == f""

            # restart kubelet at the work node
            worker.must_exec("systemctl start kubelet")

            # wait for cluster-server-config pods to be in running state.
            retries = 0
            while True:
                assert retries < 10, "Wait too long for cluster-server-config pods to be in Running state"
                _, so, _ = ctr.must_exec(
                    f"bash -c \"kubectl get pods --field-selector=status.phase!=Running -n job-operator "
                    f"-lapp=cluster-mgmt-config -ojson | jq -r '.items[] | .spec.nodeName'\""
                )
                if so == "":
                    break
                retries += 1
                time.sleep(1)

            # The worker node should have condition `ClusterDeployNotAppliedClusterServer` set to False.
            _, so, _ = ctr.must_exec(
                f"bash -c \"kubectl get node {kind_cluster}-worker -ojson | jq -r '.status.conditions[] | "
                f"select(.type == \\\"ClusterDeployNotAppliedClusterServer\\\") | .status'\""
            )
            assert so == "False"

def test_csadm_cluster_operator_contacts(cs_cluster: Callable[[str], Result], kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    assert 0 == cs_cluster("deploy cluster-tools --force").exit_code  # ensure csadm.sh is present

    # Big-bang: delete pre-existing secret [if any], and start over
    ctx.cluster_ctr.must_exec("kubectl delete secret -n prometheus cluster-operator-contacts --ignore-not-found")

    # Create the namespace if needed
    rv, _, _ = ctx.cluster_ctr.exec("kubectl get ns prometheus")
    if rv != 0:
        ctx.cluster_ctr.must_exec("kubectl create ns prometheus")

    # Create empty contacts
    ctx.cluster_ctr.must_exec("csadm.sh cluster-operator-contacts create")

    # Test that we can add a single contact with default values and list it out
    ctx.cluster_ctr.must_exec("csadm.sh cluster-operator-contacts add-contact email foo@example.com")
    rv, out, err = ctx.cluster_ctr.exec("csadm.sh cluster-operator-contacts list-contacts")
    assert rv == 0, err
    assert "- id: foo@example.com" in out, "Failed to add initial contact, stdout: %s, stderr: %s" % (out, err)

    # Verify that we can validate a config file
    new_file_contents = """contacts:
  email:
    - id: foo@example.com
      severity_threshold: 5
      alert_type: cluster
    - id: heythere@someplace.org
      severity_threshold: 4
      alert_type: job
"""

    # Format the command properly for remote execution
    cmd = f"sh -c 'cat > /tmp/contacts.yaml <<EOF\n{new_file_contents}\nEOF'"
    ctx.cluster_ctr.must_exec(cmd)
    rv, _, err = ctx.cluster_ctr.exec("csadm.sh cluster-operator-contacts validate-config /tmp/contacts.yaml")
    assert rv == 0, err

    # Test that we can overwrite the whole list via bulk-upload
    rv, _, err = ctx.cluster_ctr.exec("csadm.sh cluster-operator-contacts bulk-upload /tmp/contacts.yaml")
    assert rv == 0, err

    # Verify the list
    rv, out, err = ctx.cluster_ctr.exec("csadm.sh cluster-operator-contacts list-contacts")
    assert rv == 0, err
    assert "- id: foo@example.com" in out, f"Failed to add first contact from bulk-upload, stdout: {out}, err: {err}"
    assert "- id: heythere@someplace.org" in out, f"Failed to add second contact from bulk-upload, stdout: {out}, err: {err}"

    # Example of validation failure: duplicates
    new_file_contents = """contacts:
  email:
    - id: foo@example.com
      severity_threshold: 5
      alert_type: cluster
    - id: bar@example.com
      severity_threshold: 4
      alert_type: job
    - id: foo@example.com
      severity_threshold: 4
      alert_type: job
"""
    ctx.cluster_ctr.must_exec(f"sh -c 'cat > /tmp/contacts.yaml <<EOF\n{new_file_contents}\nEOF'")
    rv, out, err = ctx.cluster_ctr.exec("csadm.sh cluster-operator-contacts validate-config /tmp/contacts.yaml")
    assert rv != 0, err
    assert "Duplicate ID 'foo@example.com' found in email contacts" in err, "Expected duplicate ID error"

    # Example of validation failure: missing required fields
    new_file_contents = """contacts:
  email:
    - id: foo@example.com
      alert_type: cluster
"""
    ctx.cluster_ctr.must_exec(f"sh -c 'cat > /tmp/contacts.yaml <<EOF\n{new_file_contents}\nEOF'")
    rv, out, err = ctx.cluster_ctr.exec("csadm.sh cluster-operator-contacts validate-config /tmp/contacts.yaml")
    assert rv != 0, err
    assert "missing required 'severity_threshold' field" in err, "Expected missing severity_threshold error"

    # Example of validation failure: missing top-level contacts
    new_file_contents = """email:
    - id: foo@example.com
      alert_type: cluster
      severity_threshold: 3
"""
    ctx.cluster_ctr.must_exec(f"sh -c 'cat > /tmp/contacts.yaml <<EOF\n{new_file_contents}\nEOF'")
    rv, out, err = ctx.cluster_ctr.exec("csadm.sh cluster-operator-contacts validate-config /tmp/contacts.yaml")
    assert rv != 0, err
    assert "Invalid top-level key 'email'. Only 'contacts' is allowed" in err, "Expected missing contacts error"

    # Example of validation failure: mis-spelled notification target type
    new_file_contents = """contacts:
  emal:
    - id: foo@example.com
      severity_threshold: 5
      alert_type: cluster
"""
    ctx.cluster_ctr.must_exec(f"sh -c 'cat > /tmp/contacts.yaml <<EOF\n{new_file_contents}\nEOF'")
    rv, out, err = ctx.cluster_ctr.exec("csadm.sh cluster-operator-contacts validate-config /tmp/contacts.yaml")
    assert rv != 0, err
    assert "Invalid key 'emal' under contacts." in err, "Expected missing invalid-key error"


def test_cluster_alert_notification(cs_cluster: Callable[[str], Result], kind_cluster: str):
    """Test the full alert notification flow:
    0. Install alert-router and prometheus
    1. Install amtool
    2. Configure a notification contact
    3. Create a fake alert
    4. Verify the alert was processed
    """
    ctx = load_cli_ctx(None, kind_cluster)

    # Ensure prerequisites are installed
    # Note: we rely on (1) cluster-tools already being present, and (2) prometheus being installed by a previous test
    result = cs_cluster("deploy alert-router --force")
    assert result.exit_code == 0, result.stdout

    # Configure a test notification contact
    #
    # Create the contacts configuration
    test_contact_config = """contacts:
  email:
    - id: test@example.com
      severity_threshold: 4
      alert_type: cluster
"""
    ctx.cluster_ctr.must_exec(f"sh -c 'cat > /tmp/test_contacts.yaml <<EOF\n{test_contact_config}\nEOF'")
    
    # Validate and upload the configuration
    ctx.cluster_ctr.must_exec("csadm.sh cluster-operator-contacts validate-config /tmp/test_contacts.yaml")
    ctx.cluster_ctr.must_exec("csadm.sh cluster-operator-contacts bulk-upload /tmp/test_contacts.yaml")

    # Verify the contact was added
    _, out, _ = ctx.cluster_ctr.must_exec("csadm.sh cluster-operator-contacts list-contacts")
    logger.info(f"Contact list => {out}")
    assert "test@example.com" in out, "Failed to add test contact"

    # Sleep before we send an alert (allow for contact-info propagation)
    # This is a bit arbitrary, but we need to wait for the alert-router to pick up the new contact
    # [15s works on dev-server, seems to take longer in CI]
    
    def check_contact_propagation():
        _, logs, _ = ctx.cluster_ctr.exec(
            "kubectl logs -n prometheus -l app=alert-router --tail=100")
        logger.info(f"alert-router logs (with contact) => {logs}")
        return "test@example.com" in logs
    
    wait_for_condition(
        check_contact_propagation,
        timeout_secs=60,
        error_message="Timed out waiting for contact to propagate to alert-router"
    )

    # Get alertmanager cluster-IP
    res, alertMgrIP, err = ctx.cluster_ctr.exec("kubectl get svc -n prometheus prometheus-alertmanager -o jsonpath='{.spec.clusterIP}'")
    logger.info(f"DebugAgam: alert-mgr IP => {res}, {out}, {err}")

    # Get alertrouter cluster-IP
    res, alertRouterIP, err = ctx.cluster_ctr.exec("kubectl get svc -n prometheus alert-router -o jsonpath='{.spec.clusterIP}'")
    logger.info(f"DebugAgam: alert-router IP => {res}, {out}, {err}")

    # Create another fake alert by directly POSTing to the alertmanager API
    alertmanager_url = f"http://{alertMgrIP}:9093/api/v2/alerts"
    timestamp = int(time.time())
    alert_data = [{
        "status": "firing",
        "labels": {
            "alertname": "SystemWarningEvent",
            "instance": "test-instance",
            "severity": "warning"
        },
        "annotations": {
            "summary": f"Test Alert -- ALERTMGR POST -- {timestamp}"
        }
    }]
    headers = {
        "Content-Type": "application/json"
    }

    alert_json = json.dumps(alert_data)
    alertmanager_curl_command = f"""curl -X POST -H "Content-Type: application/json" -d '{alert_json}' {alertmanager_url}"""
    ctx.cluster_ctr.must_exec(alertmanager_curl_command)

    # Create yet another fake alert, this time POSTing to the AlertRouter directly
    alert_router_url = f"http://{alertRouterIP}:8080/webhook"
    timestamp = int(time.time())
    # Format the payload as Alertmanager would
    alert_data = {
        "version": "4",
        "groupKey": "{}:{}",
        "status": "firing",
        "receiver": "alert-router",
        "groupLabels": {
            "alertname": "SystemWarningEvent"
        },
        "commonLabels": {
            "alertname": "SystemWarningEvent",
            "instance": "test-instance",
            "severity": "warning"
        },
        "commonAnnotations": {
            "summary": f"Test Alert -- DIRECT POST -- {timestamp}"
        },
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "SystemWarningEvent",
                    "instance": "test-instance",
                    "severity": "warning"
                },
                "annotations": {
                    "summary": f"Test Alert -- DIRECT POST -- {timestamp}"
                }
            }
        ]
    }
    alert_router_json = json.dumps(alert_data)
    alert_router_curl_command = f"""curl -X POST -H "Content-Type: application/json" -d '{alert_router_json}' {alert_router_url}"""
    ctx.cluster_ctr.must_exec(alert_router_curl_command)

    # Sanity check: logs
    _, logs, _ = ctx.cluster_ctr.must_exec(
        "kubectl logs -n prometheus -l alertmanager=prometheus-alertmanager --tail=100")
    logger.info(f"alertmanager logs => {logs}")  
    
    # Check alert-router logs to verify the alert was processed
    # To be sure to receive, we must wait more than the `group_wait` interval in AlertManager
    logs = ""
    
    def check_alert_router_logs():
        nonlocal logs
        _, logs, _ = ctx.cluster_ctr.exec(
            "kubectl logs -n prometheus -l app=alert-router --tail=100")
        logger.info(f"alert-router logs => {logs}")
        
        # Check if both alerts and the contact are in the logs
        return ("Test Alert -- ALERTMGR POST" in logs and 
                "Test Alert -- DIRECT POST" in logs and 
                "test@example.com" in logs)
    
    wait_for_condition(
        check_alert_router_logs,
        timeout_secs=60,
        error_message="Timed out waiting for alerts to appear in alert-router logs"
    )
    
    # Check for evidence that the alert was received and processed
    assert "Test Alert -- ALERTMGR POST" in logs, "Alert (1) not found in alert-router logs"
    assert "Test Alert -- DIRECT POST" in logs, "Alert (2) not found in alert-router logs"

    # Verify that the configured contact shows up in the alert-router logs
    assert "test@example.com" in logs, "Contact not found in alert-router logs"


def pull_default_image(tmp_path: pathlib.Path):
    with open(f"{tmp_path}/image-pull.log", "w") as f:
        sp.check_call(f"docker pull {TEST_PULL_IMAGE}".split(), stdout=f)


def wait_for_condition(check_fn: Callable[[], bool], timeout_secs: float = 10, 
                      error_message: str = "Condition not met") -> None:
    """Wait for a condition with exponential backoff.
    
    Args:
        check_fn: Function that returns True when condition is met
        timeout_secs: Maximum time to wait in seconds
        error_message: Message to show if condition times out
    """
    start = time.time()
    attempt = 0
    while time.time() - start < timeout_secs:
        if check_fn():
            return
        ## Basic backoff, capped at 1s
        time.sleep(min(2 ** attempt * 0.1, 1.0))
        attempt += 1
    assert False, error_message

@pytest.mark.skip(reason="jnode disk is too small, 50G only")
def test_incremental_deploy(kind_incremental_cluster: str):
    ctx = load_cli_ctx(None, kind_incremental_cluster)
    new_node_name = f"{kind_incremental_cluster}-worker2"
    
    cluster_path = CLI_BASE_DIR / "clusters" / kind_incremental_cluster
    network_config_path = cluster_path / "network_config.json"
    cluster_config_path = "/opt/cerebras/cluster/cluster.yaml"
   
    with open(cluster_path / "cluster.yaml", 'r') as f:
        cluster = yaml.safe_load(f)
        switch_config = cluster["groups"][0]["switchConfig"] 

    network_config_path.write_text(incr_kind_netjson(kind_incremental_cluster, switch_config))

    ctx.cluster_ctr.exec_cmds([
        FileCopyCmd(network_config_path, "/opt/cerebras/cluster/network_config.json"),
    ])
     
    new_node_name = f"{kind_incremental_cluster}-worker2"
    rv, stdout, stderr = ctx.cluster_ctr.must_exec(f"kubectl taint nodes {new_node_name} key1=value1:NoSchedule-")
    assert rv == 0, f"{stdout}\n{stderr}"

    # Perform incremental deploy with the new node in netjson
    rv, stdout, stderr = (
        ctx.cluster_ctr.exec('sh -c "cd /home/pkg && jq -s \'{\"componentPaths\": map(.componentPaths) | add | unique}\' manifest.json.* > manifest.json"'))   
    assert rv == 0, f"failed to merge manifest.json.* into single manifest.json. error: {stdout}\n{stderr}"

    # Check that new node is not present in cluster.yaml file
    rv, stdout, stderr = ctx.cluster_ctr.exec(
        f"sh -c 'cd /home/pkg && grep -q \"{new_node_name}\" \"{cluster_config_path}\"'"
    )
    assert rv != 0, f"{stdout}\n{stderr}"

    rv, stdout, stderr = (
        ctx.cluster_ctr.exec("sh -c 'cd /home/pkg && ./csadm.sh install \"$(pwd)/manifest.json\" --validate --debug --yes --update-config --skip-k8s'"))
    assert rv == 0, f"{stdout}\n{stderr}"

    # Check that the new node is in the cluster.yaml file
    rv, stdout, stderr = ctx.cluster_ctr.exec(
        f"sh -c 'cd /home/pkg && grep -q \"{new_node_name}\" \"{cluster_config_path}\"'"
    )
    assert rv == 0, f"{stdout}\n{stderr}"
