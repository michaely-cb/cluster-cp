
import logging
import pathlib
import pytest
import random
import shlex
import time
from typing import Callable

import kubernetes.client
from click.testing import CliRunner
from click.testing import Result

from common.cluster import DockerClusterController
from common.cluster import FileCopyCmd
from common.context import load_cli_ctx
from cs_cluster import cli
from common.context import CliCtx

from .conftest import TEST_PULL_IMAGE
from .conftest import use_usernode
from .conftest import assert_cluster_server_ready
from .conftest import _assert_cbcore_image_set
from .conftest import assert_usernode_configured
from .conftest import get_cluster_controlplane_ctr

logger = logging.getLogger("cs_cluster.test.e2e_csctl")
current_dir = pathlib.Path(__file__).parent.absolute()

MAKE_JOB_TIMEOUT_SECS = 30   ## duration after which to fail/timeout making new jobs

def _assert_cluster_readiness(kind_cluster, cmd: str, k8s: kubernetes.client.ApiClient, ns: str) -> CliCtx:
    """
    Helper that:
        - sets cbcore image
        - checks ingress
        - checks that the cluster is ready to deploy jobs
    Returns a CLI context
    """
    image = f"busybox:test-{random.randint(0, 10000000)}"
    result = CliRunner().invoke(cli, shlex.split(cmd), env={"WSJOB_IMAGE": image})
    assert result.exit_code == 0
    _assert_cbcore_image_set(k8s, image, ns)

    netv1 = kubernetes.client.NetworkingV1Api(k8s)
    ingress = netv1.read_namespaced_ingress("cluster-server", ns)
    hosts = [r.host for r in ingress.spec.rules]
    assert f"{ns}.cluster-server.cerebrassc.local" in hosts

    ctx = load_cli_ctx(None, kind_cluster)
    assert_cluster_server_ready(ctx, ns)

    return ctx


def test_csctl_with_namespace_isolation(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        k8s: kubernetes.client.ApiClient,
        cluster_server_image,
        system_namespace: str = "job-operator"):
    ns = "user"
    cmd = f"--cluster {kind_cluster} deploy cluster-server --force " \
          f"--pull-ecr-image --disable-user-auth --namespace={ns} --use-isolated-dashboards"
    if cluster_server_image:
        cmd += f" --image={cluster_server_image}"

    ctx = _assert_cluster_readiness(kind_cluster, cmd, k8s, ns)

    job = f"""
apiVersion: "jobs.cerebras.com/v1"
kind: WSJob
metadata:
  name: test
  namespace: {ns}
spec:
  runPolicy:
    cleanPodPolicy: Running
  numWafers: 0
  wsReplicaSpecs:
    Coordinator:
      replicas: 1
      template:
        spec:
          containers:
            - name: ws
              image: {TEST_PULL_IMAGE}
              command:
                - "sleep"
                - "30000"
"""
    ctx.write_file("/tmp/job.yaml", job)
    with get_cluster_controlplane_ctr(kind_cluster) as ctr:
        ctr.exec_cmds([FileCopyCmd("/tmp/job.yaml", "/job.yaml")])
    ctx.cluster_ctr.must_exec(f"kubectl apply -f /job.yaml")
    ctx.cluster_ctr.must_exec(f"csctl get job test --namespace {ns}")

    ctx.cluster_ctr.must_exec("bash -c 'rm -rf /opt/cerebras/packages'")

    with use_usernode(ctx) as host_port:
        dockername, port = host_port
        with DockerClusterController([dockername]) as ctr:
            ctr.must_exec("rm -rf /opt/cerebras/wheels")

            def _deploy_usernode_cmd(ns, overwrite_usernode_configs, reset_usernode_configs,
                                     skip_install_default_csctl):
                cmd = f"deploy usernode --hosts=localhost --port={port} --password='' --config-only --namespace={ns} "
                if reset_usernode_configs:
                    cmd += "--reset-usernode-configs "
                elif overwrite_usernode_configs:
                    cmd += "--overwrite-usernode-configs "
                # Production environment never uses this option
                # This is more for CIF runs not having to step on each other's toes
                if skip_install_default_csctl:
                    cmd += "--skip-install-default-csctl "
                return cmd

            # Set up csctl on the user node by deploying with the system namespace
            result = cs_cluster(_deploy_usernode_cmd(
                ns=system_namespace,
                overwrite_usernode_configs=True,
                reset_usernode_configs=False,
                skip_install_default_csctl=False))
            assert result.exit_code == 0, f"failed to deploy usernode {result.exit_code}, {result.stdout}"
            assert_usernode_configured(dockername, kind_cluster, [system_namespace])

            # Retrieve a job from the user namespace through the system namespace ingress
            rv, _, se = ctr.exec(f"sh -c 'csctl --namespace {ns} get job test'")
            assert rv == 0, se

            # Retrieve a non-existing job from the user namespace through the system namespace ingress
            rv, so, se = ctr.exec(f"sh -c 'csctl --namespace {ns} get job non-existing'")
            assert rv != 0, so
            assert "not found" in se, se

            # Label a job from the user namespace through the system namespace ingress
            rv, _, se = ctr.exec(f"sh -c 'csctl --namespace {ns} label job test foo=bar'")
            assert rv == 0, se

            # Retrieve jobs based on label through the system namespace ingress
            rv, so, se = ctr.exec(f"sh -c 'csctl --namespace {ns} get job -l foo=bar -a'")
            assert rv == 0, se
            assert ns in so
            assert "test" in so
            assert "foo=bar" in so

            # Cancel a job from the user namespace through the system namespace ingress
            rv, _, se = ctr.exec(f"sh -c 'csctl --namespace {ns} cancel job test --force'")
            assert rv == 0, se

            # Csctl get cluster through the system namespace ingress
            rv, so, se = ctr.exec("sh -c 'csctl get cluster'")
            assert rv == 0, se
            assert "kapi-0" in so
            assert "kapi-1" in so
            assert f"{kind_cluster}-control-plane" in so
            assert f"{kind_cluster}-worker" in so

            # Csctl get nodegroups through the system namespace ingress
            rv, so, se = ctr.exec("sh -c 'csctl get nodegroups'")
            assert rv == 0, se
            assert "nodegroup0" in so
            assert system_namespace in so

            # Deploy the user node to enable the certificate authority of the user namespace
            result = cs_cluster(_deploy_usernode_cmd(
                ns=ns,
                overwrite_usernode_configs=False,
                reset_usernode_configs=False,
                skip_install_default_csctl=False))
            assert result.exit_code == 0, f"failed to deploy usernode {result.exit_code}, {result.output}"
            # The user node should have one certificate authorities
            # NOTE: With the new filtering behavior, this will remove the system namespace
            assert_usernode_configured(dockername, kind_cluster, [ns])

            # Retrieve a job from the user namespace through the system namespace ingress
            rv, _, se = ctr.exec(f"sh -c 'csctl --namespace {ns} get job test'")
            assert rv == 0, se

            # Retrieve a job from the user namespace through the user namespace ingress
            rv, _, se = ctr.exec("sh -c 'csctl get job test'")
            assert rv == 0, se

            # Retrieve a non-existing job from the user namespace through the user namespace ingress
            rv, so, se = ctr.exec("sh -c 'csctl get job non-existing'")
            assert rv != 0, so
            assert "not found" in se, se

            # Retrieve a job from the user namespace through the user namespace ingress (without namespace argument)
            rv, _, se = ctr.exec("sh -c 'csctl get job test'")
            assert rv == 0, se

            # Label a job from the user namespace through the user namespace ingress
            rv, _, se = ctr.exec("sh -c 'csctl label job test foo='")
            assert rv == 0, se

            # Retrieve jobs based on label through the user namespace ingress
            rv, so, se = ctr.exec("sh -c 'csctl get job -l foo= -a'")
            assert rv == 0, se
            assert ns in so
            assert "test" in so
            assert "foo=" in so

            # Cancel a job from the user namespace through the user namespace ingress
            rv, _, se = ctr.exec("sh -c 'csctl cancel job test --force'")
            assert rv == 0, se

            # Csctl get cluster through the user namespace ingress
            rv, so, se = ctr.exec("sh -c 'csctl get cluster'")
            assert rv == 0, se
            assert "kapi-0" not in so
            assert "kapi-1" not in so
            assert f"{kind_cluster}-control-plane" not in so
            assert f"{kind_cluster}-worker" not in so

            # Csctl get nodegroups through the user namespace ingress
            rv, so, se = ctr.exec("sh -c 'csctl get nodegroups'")
            assert rv == 0, se
            assert "nodegroup0" not in so
            assert ns not in so

            # Get Csctl creation datetime
            rv, so, se = ctr.exec(f'sh -c "stat -c %Y $(which csctl)"')
            assert rv == 0, se
            csctl_original_creation_datetime = so

            # Redeploy usernode but skip installing default csctl
            result = cs_cluster(_deploy_usernode_cmd(
                ns=ns,
                overwrite_usernode_configs=True,
                reset_usernode_configs=False,
                skip_install_default_csctl=True))
            assert result.exit_code == 0, f"failed to deploy usernode {result.exit_code}, {result.stdout}"
            rv, so, se = ctr.exec(f'sh -c "stat -c %Y $(which csctl)"')
            assert rv == 0, se
            assert csctl_original_creation_datetime == so

            # Redeploy usernode and do not skip installing default csctl (production expectation)
            result = cs_cluster(_deploy_usernode_cmd(
                ns=ns,
                overwrite_usernode_configs=True,
                reset_usernode_configs=False,
                skip_install_default_csctl=False))
            assert result.exit_code == 0, f"failed to deploy usernode {result.exit_code}, {result.stdout}"
            rv, so, se = ctr.exec(f'sh -c "stat -c %Y $(which csctl)"')
            assert rv == 0, se
            assert csctl_original_creation_datetime != so

            # Query certificate authority data so we can compare later on
            rv, cert_auth_data, se = ctr.exec(
                "sh -c 'jq -r '.clusters[0].namespaces[0].certificateAuthorityData' /opt/cerebras/config_v2'")
            assert rv == 0, se

            # Modify certificate authority prior to redeploying user node
            config_mod_cmd = (
                f"jq -r '.clusters[0].namespaces = [{{\"name\": \"{ns}\", \"certificateAuthorityData\": \"stale-data\"}}]' "
                "/opt/cerebras/config_v2 > /opt/cerebras/config_v2.new && "
                "mv /opt/cerebras/config_v2.new /opt/cerebras/config_v2"
            )
            rv, _, se = ctr.exec(f"sh -c '{config_mod_cmd}'")
            assert rv == 0, se

            # Redeploy with the same user namespace again and assert no duplicate certificate authorities
            # for the same namespace
            result = cs_cluster(_deploy_usernode_cmd(
                ns=ns,
                overwrite_usernode_configs=True,
                reset_usernode_configs=False,
                skip_install_default_csctl=False))
            assert result.exit_code == 0, f"failed to deploy usernode {result.exit_code}, {result.stdout}"
            assert_usernode_configured(dockername, kind_cluster, [ns])

            # Certificate authority data should be restored
            rv, new_cert_auth_data, se = ctr.exec(
                f"sh -c 'jq -r '.clusters[0].namespaces[0].certificateAuthorityData' /opt/cerebras/config_v2'")
            if se:
                logger.error(f"failed to get certificate authority data from /opt/cerebras/config_v2")
            assert rv == 0, f"expected certificate authority data to be successfully retrieved but failed"
            assert cert_auth_data == new_cert_auth_data, f"expected certificate authority data to be successfully restored but failed with {se}"

            # Csctl should work for the user namespace
            rv, _, se = ctr.exec(f"sh -c 'csctl get versions'")
            assert rv == 0, se

            # Reset usernode configs
            result = cs_cluster(_deploy_usernode_cmd(
                ns=ns,
                overwrite_usernode_configs=False,
                reset_usernode_configs=True,
                skip_install_default_csctl=False))
            assert result.exit_code == 0, f"failed to deploy usernode {result.exit_code}, {result.stdout}"
            assert_usernode_configured(dockername, kind_cluster, [])

            # Csctl should fail after config reset
            rv, so, se = ctr.exec(f"sh -c 'csctl get versions'")
            assert rv != 0, so
            assert "does not have access to any namespace" in se


def _make_job(ctx, tmp_path: str, kind_cluster: str, job_id: str, system_namespace: str,
              wsjob_label_val: str, wsjob_label_key="labels.k8s.cerebras.com/wsjob-label"):
    """
    Create a job with the given job_id, system_namespace, and wsjob_label
    """
    job = f"""
apiVersion: "jobs.cerebras.com/v1"
kind: WSJob
metadata:
  name: {job_id}
  namespace: {system_namespace}
  labels:
    {wsjob_label_key}: {wsjob_label_val}
spec:
  runPolicy:
    cleanPodPolicy: Running
  numWafers: 0
  wsReplicaSpecs:
    Coordinator:
      replicas: 1
      template:
        spec:
          containers:
            - name: ws
              image: {TEST_PULL_IMAGE}
              command:
                - "sleep"
                - "30000"
"""
    ctx.write_file(f"{tmp_path}/{job_id}.yaml", job)
    with get_cluster_controlplane_ctr(kind_cluster) as ctr:
        ctr.exec_cmds([FileCopyCmd(f"{tmp_path}/{job_id}.yaml", f"/{job_id}.yaml")])
        ctx.cluster_ctr.must_exec(f"kubectl apply -f /{job_id}.yaml")
        ctx.cluster_ctr.must_exec(f"csctl get job {job_id} --namespace {system_namespace}")
    logger.info(f"created job {job_id} in namespace {system_namespace} "
                f"with label {wsjob_label_key}:{wsjob_label_val}")


def _make_multiple_jobs(ctx, tmp_path, kind_cluster, system_namespace, wsjob_label, num_jobs):
    """
    Create multiple jobs with the given job_id, system_namespace, and wsjob_label, and wait for them to be created
    """
    job_names = []
    for i in range(num_jobs):
        job_name = f"test-{random.randint(0, 10000000)}-{i}"
        job_names.append(job_name)
        _make_job(ctx, tmp_path, kind_cluster, job_name, system_namespace, wsjob_label)
    start_time = time.time()
    while True:
        rv, so, se = ctx.cluster_ctr.exec(
            f"sh -c 'csctl get job -l wsjob-label={wsjob_label}'"
        )
        assert rv == 0, se
        found_jobs = []
        for job_name in job_names:
            if job_name in so:
                found_jobs.append(job_name)
        if len(found_jobs) == num_jobs:
            break
        if time.time() - start_time > MAKE_JOB_TIMEOUT_SECS:
            raise Exception(f"csctl failed to create {num_jobs} jobs within {MAKE_JOB_TIMEOUT_SECS} seconds; found jobs: {found_jobs}, so = {so}, se = {se}")
        logger.info(f"waiting for {num_jobs} jobs to be created; found jobs: {found_jobs}, so = {so}, se = {se}")
        time.sleep(2)


def _mk_deploy_cmd(kind_cluster, system_namespace, cluster_server_image):
    cmd = f"--cluster {kind_cluster} deploy cluster-server --force " \
          f"--pull-ecr-image --disable-user-auth --namespace={system_namespace}"
    if cluster_server_image:
        cmd += f" --image={cluster_server_image}"
    return cmd


def test_csctl_job_priority_e2e(
        cs_cluster: Callable[[str], Result],
        kind_cluster: str,
        k8s: kubernetes.client.ApiClient,
        tmp_path: pathlib.Path,
        cluster_server_image,
        system_namespace: str = "job-operator"):
    cmd = _mk_deploy_cmd(kind_cluster, system_namespace, cluster_server_image)

    ctx = _assert_cluster_readiness(kind_cluster, cmd, k8s, system_namespace)

    wsjob_label = f"priority-test-{random.randint(0, 10000000)}"

    test_1 = f"test-{random.randint(0, 10000000)}"
    test_2 = f"test-{random.randint(0, 10000000)}"
    test_3 = f"test-{random.randint(0, 10000000)}"

    _make_job(ctx, tmp_path, kind_cluster, test_1, system_namespace, wsjob_label)
    time.sleep(1)
    _make_job(ctx, tmp_path, kind_cluster, test_2, system_namespace, wsjob_label)
    time.sleep(1)
    _make_job(ctx, tmp_path, kind_cluster, test_3, system_namespace, wsjob_label)
    time.sleep(1)

    # List jobs
    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl job list -l wsjob-label={wsjob_label} | grep \"P2 (299)\" | wc -l'")
    assert rv == 0, se
    assert so == "3", so

    # Update job priority using priority value
    rv, _, se = ctx.cluster_ctr.exec(f"sh -c 'csctl job set-priority {test_1} 150'")
    assert rv == 0, se

    # Get job priority
    # test_1 should be displayed first due to higher priority
    rv, _, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl job list -l wsjob-label={wsjob_label} | sed '1d' | head -1 | grep \"P1 (150)\"'")
    assert rv == 0, se

    # test_3 should be displayed last due to later creation time than test_2
    rv, _, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl job list -l wsjob-label={wsjob_label} | tail -1 | grep \"{test_3}\"'")
    assert rv == 0, se

    # Update job priority using priority bucket
    rv, _, se = ctx.cluster_ctr.exec(f"sh -c 'csctl job set-priority {test_2} p0'")
    assert rv == 0, se

    # Get job priority
    # test-2 should be displayed first due to higher priority
    rv, _, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl job list -l wsjob-label={wsjob_label} | sed '1d' | head -1 | grep \"P0 (99)\"'")
    assert rv == 0, se

    # Cancel a job
    rv, _, se = ctx.cluster_ctr.exec(f"sh -c 'csctl cancel job {test_1}'")
    assert rv == 0, se
    time.sleep(5)

    # Should fail to update priority of an ended job
    rv, so, se = ctx.cluster_ctr.exec(f"sh -c 'csctl job set-priority {test_1} p1'")
    assert rv != 0, so
    assert "ended" in se

    # Cancel other jobs as cleanup
    rv, _, se = ctx.cluster_ctr.exec(f"sh -c 'csctl cancel job {test_2}'")
    assert rv == 0, se
    rv, _, se = ctx.cluster_ctr.exec(f"sh -c 'csctl cancel job {test_3}'")
    assert rv == 0, se


def test_csctl_max_jobs(cs_cluster: Callable[[str], Result],
                        kind_cluster: str,
                        k8s: kubernetes.client.ApiClient,
                        tmp_path: pathlib.Path,
                        cluster_server_image,
                        system_namespace: str = "job-operator"):
    cmd = _mk_deploy_cmd(kind_cluster, system_namespace, cluster_server_image)
    ctx = _assert_cluster_readiness(kind_cluster, cmd, k8s, system_namespace)

    wsjob_label = f"limit-test-{random.randint(0, 10000000)}"
    _make_multiple_jobs(ctx, tmp_path, kind_cluster, system_namespace, wsjob_label, 10)

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl get job -l wsjob-label={wsjob_label} --max-jobs=5 | grep test | wc -l'"
    )
    assert rv == 0, se
    assert so == "5", so

    # when outputting JSON, wsjob-label would also match with grep test, so need to grep name
    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl get job -l wsjob-label={wsjob_label} --max-jobs=5 -o json | grep test | grep name | wc -l'"
    )
    assert rv == 0, se
    assert so == "5", so

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl get job -l wsjob-label={wsjob_label} | grep test | wc -l'"
    )
    assert rv == 0, se
    assert so == "10", so

    # when outputting yaml, wsjob-label would also match with grep test, so need to grep name
    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl get job -l wsjob-label={wsjob_label} -o yaml | grep test | grep name | wc -l'"
    )
    assert rv == 0, se
    assert so == "10", so


def test_csctl_filter_workflow(cs_cluster: Callable[[str], Result],
                        kind_cluster: str,
                        k8s: kubernetes.client.ApiClient,
                        tmp_path: pathlib.Path,
                        cluster_server_image,
                        system_namespace: str = "job-operator"):
    cmd = _mk_deploy_cmd(kind_cluster, system_namespace, cluster_server_image)
    ctx = _assert_cluster_readiness(kind_cluster, cmd, k8s, system_namespace)

    test_1 = f"test-{random.randint(0, 10000000)}"
    test_2 = f"test-{random.randint(0, 10000000)}"
    test_3 = f"test-{random.randint(0, 10000000)}"
    _make_job(ctx, tmp_path, kind_cluster, test_1, system_namespace,
        "wf-0", "cerebras/workflow-id")
    time.sleep(1)
    _make_job(ctx, tmp_path, kind_cluster, test_2, system_namespace,
        "wf-0", "cerebras/workflow-id")
    time.sleep(1)
    _make_job(ctx, tmp_path, kind_cluster, test_3, system_namespace,
        "wf-1", "cerebras/workflow-id")
    time.sleep(1)

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl get job --workflow=wf-0'"
    )
    assert rv == 0, se
    assert test_1 in so, so
    assert test_2 in so, so
    assert test_3 not in so, so


# sanity test on csctl
def test_csctl(cs_cluster: Callable[[str], Result], kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    ctx.cluster_ctr.must_exec("csctl get volume")
    ctx.cluster_ctr.must_exec("csctl get version")
    ctx.cluster_ctr.must_exec("csctl get nodegroup")
    ctx.cluster_ctr.must_exec("csctl get cluster")
    ctx.cluster_ctr.must_exec("csctl get job")
    ctx.cluster_ctr.must_exec("csctl get worker-cache")
    ctx.cluster_ctr.must_exec("csctl session list")
