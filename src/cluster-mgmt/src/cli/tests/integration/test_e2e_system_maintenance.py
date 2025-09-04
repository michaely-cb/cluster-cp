import json
import logging
import pathlib
import random
import shlex
import subprocess as sp
import tarfile
import time
import zipfile

from typing import Callable
import kubernetes.client

from click.testing import CliRunner
from click.testing import Result
from common import ECR_URL
from common.cluster import DockerClusterController
from common.context import load_cli_ctx
from cs_cluster import cli
from .conftest import (
    assert_cluster_server_ready,
    get_cluster_server_image,
    use_usernode
)

import pytest

logger = logging.getLogger("cs_cluster.test.e2e_system_maintenance")

def must_exec_cli(cmd):
    logger.info(f"exec cli cmd: {cmd}")
    result = CliRunner().invoke(cli, shlex.split(cmd))
    assert result.exit_code == 0, result.output

@pytest.fixture(scope="session")
def mock_rel_artifact_dir(
    kind_cluster: str, tmp_path_factory,
) -> pathlib.Path:
    # Create a fake artifacts folder with a real cluster-server image
    artifacts = tmp_path_factory.mktemp("release-artifacts")

    # Prepare a cluster-server-test.docker file
    server_image = get_cluster_server_image(kind_cluster)
    cmds = [
        f"docker image save -o {artifacts}/cluster-server-test.tar {server_image}",
        f"gzip -f {artifacts}/cluster-server-test.tar",
        f"mv {artifacts}/cluster-server-test.tar.gz {artifacts}/cluster-server-test.docker",
    ]
    for cmd in cmds:
        logger.info(f"running command on host: {cmd}")
        sp.check_call(shlex.split(cmd))

    # Prepare cbcore reference in buildinfo
    artifacts.joinpath("buildinfo-cbcore.json").write_text(
        json.dumps(
            {
                "toolchain": str(artifacts),
                "version": "test",
                "buildid": str(int(time.time())),
                "dockerids": [
                    f"{ECR_URL}/dummy-image:fake",
                    f"{ECR_URL}/cbcore:fake",
                ],
            }
        )
    )

    # Prepare fake wheels
    with zipfile.ZipFile(artifacts / "cerebras_appliance-0.whl", "w") as zf:
        zf.writestr(
            "cerebras_appliance/bin/raw/csctl",
            "echo 'csctl version 2.1.0-fake'",
        )
    artifacts.joinpath("pytorch-1.whl").write_text("fake wheel 1 contents")
    # make a directory with a wheel and then symlink it in the basedir to test resolution of symlinks
    wheel_dir = artifacts / "wheel_dir"
    wheel_dir.mkdir()
    wheel_dir.joinpath("pytorch-2.whl").write_text("fake wheel 2 contents")
    artifacts.joinpath("pytorch-2-symlink.whl").symlink_to(
        wheel_dir / "pytorch-2.whl"
    )

    # Prepare a fake cluster-tools tar.gz file and add csctl to it
    tar_gz_path = artifacts / "cluster-mgmt-tools-x.tar.gz"
    with tarfile.open(tar_gz_path, "w:gz") as tar:
        fp = artifacts / "csctl"
        fp.write_text("echo 'csctl version 2.1.0-fake'")
        tar.add(fp, arcname="csctl")
        fp.unlink()
    return artifacts

def _mk_deploy_cmd(kind_cluster, system_namespace, cluster_server_image):
    cmd = f"--cluster {kind_cluster} deploy cluster-server --force " \
          f"--pull-ecr-image --disable-user-auth --namespace={system_namespace}"
    if cluster_server_image:
        cmd += f" --image={cluster_server_image}"
    return cmd

def test_system_maintenance_api_e2e(
    cs_cluster: Callable[[str], Result],
    kind_cluster: str,
    k8s: kubernetes.client.ApiClient,
    tmp_path: pathlib.Path,
    cluster_server_image,
    mock_rel_artifact_dir: pathlib.Path,
    system_namespace: str = "job-operator"
):
    cmd = _mk_deploy_cmd(kind_cluster, system_namespace, cluster_server_image)

    ctx = load_cli_ctx(None, kind_cluster)
    session_api = f"--cluster {kind_cluster} session"
    user_session = f"user-{random.randint(0, 10000000)}"
    default_spec = "--parallel-compile-count 0 --parallel-execute-count 0 --wsjob-log-preferred-storage-type local"
    deploy_options = f"--rel-path {mock_rel_artifact_dir}"

    assert_cluster_server_ready(ctx, system_namespace)

    with use_usernode(ctx) as host_port:
        dockername, port = host_port
        with DockerClusterController([dockername]) as ctr:
                # Unassign systems from systemNS
                cmd = f"{session_api} update {system_namespace} --system-count 0 {default_spec}"
                result = CliRunner().invoke(cli, shlex.split(cmd))
                assert result.exit_code == 0, result.output
                assert "kapi-1" not in result.output
                assert "kapi-0" not in result.output

                # Create sessions
                usernode_host_options = f"--usernodes=localhost --usernode-port={port} --usernode-password=''"
                usernode_options = f"{usernode_host_options} --usernode-config-only"
                cmd = f"{session_api} create {user_session} --system-count 2 {default_spec} {deploy_options} {usernode_options}"
                result = CliRunner().invoke(cli, shlex.split(cmd))
                assert result.exit_code == 0, result.output
                assert user_session in result.output
                assert "kapi-0" in result.output
                assert "kapi-1" in result.output

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance list -a -n {user_session} | wc -l'")
    assert rv == 0, se
    # Expect headers only
    assert so == "1", so

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance create membist -n {user_session} --systems kapi-0 --dryrun'")
    assert rv == 0, se
    workflow_id1 = so.splitlines()[-1].split()[-1]
    assert workflow_id1.startswith("wflow-")
    
    # Add a 2 second sleep before creating next job
    time.sleep(2)

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance list -n {user_session} | wc -l'")
    assert rv == 0, se
    # Expect headers and one system maintenance workflow
    assert so == "2", so

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance get {workflow_id1} -ojson -n {user_session}'")
    assert rv == 0, se
    response_json = json.loads(so)
    assert len(response_json["jobs"]) == 1
    assert response_json["jobs"][0]["systems"] == ["kapi-0"]
    assert response_json["jobs"][0]["labels"]["cerebras/system-maintenance-type"] == "membist"

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance create vddc set --vddc-offset -30 -n {user_session} --systems ALL --dryrun'")
    assert rv == 0, se
    workflow_id2 = so.splitlines()[-1].split()[-1]
    assert workflow_id2.startswith("wflow-")
    
    # Add another 2 second sleep before creating the third job
    time.sleep(2)
    
    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance get {workflow_id2} -ojson -n {user_session}'")
    assert rv == 0, se
    response_json = json.loads(so)
    assert len(response_json["jobs"]) == 2

    found_sys0 = False
    found_sys1 = False
    for job in response_json["jobs"]:
        assert job["labels"]["cerebras/system-maintenance-type"] == "vddc"
        found_sys0 = found_sys0 or job["systems"] == ["kapi-0"]
        found_sys1 = found_sys1 or job["systems"] == ["kapi-1"]
    assert found_sys0 and found_sys1

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance create vddc unset -n {user_session} --systems ALL --dryrun'")
    assert rv == 0, se
    workflow_id3 = so.splitlines()[-1].split()[-1]
    assert workflow_id3.startswith("wflow-")

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance get {workflow_id3} -ojson -n {user_session}'")
    assert rv == 0, se
    response_json = json.loads(so)
    assert len(response_json["jobs"]) == 2

    found_sys0 = False
    found_sys1 = False
    for job in response_json["jobs"]:
        assert job["labels"]["cerebras/system-maintenance-type"] == "vddc"
        found_sys0 = found_sys0 or job["systems"] == ["kapi-0"]
        found_sys1 = found_sys1 or job["systems"] == ["kapi-1"]
    assert found_sys0 and found_sys1

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance list -ojson -n {user_session}'")
    assert rv == 0, se
    response_json = json.loads(so)
    # Expect three system maintenance workflows
    assert len(response_json["workflows"]) == 3
    
    wait_for_workflows_completion(ctx, user_session)

    # Test wafer-diag workflow
    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance create wafer-diag -n {user_session} --systems kapi-0 --dryrun --testmode'")
    assert rv == 0, se
    workflow_id4 = so.splitlines()[-1].split()[-1]
    assert workflow_id4.startswith("wflow-")
    
    # Verify wafer-diag workflow was created
    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance list -n {user_session} | wc -l'")
    assert rv == 0, se
    # Expect headers and one system maintenance workflow (just wafer-diag, others completed)
    assert so == "2", f"Expected 2 lines but got {so}"

    # Get and validate wafer-diag workflow details
    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance get {workflow_id4} -ojson -n {user_session}'")
    assert rv == 0, se
    response_json = json.loads(so)
    assert len(response_json["jobs"]) == 1
    assert response_json["jobs"][0]["systems"] == ["kapi-0"]
    assert response_json["jobs"][0]["labels"]["cerebras/system-maintenance-type"] == "wafer-diag"
    
    wait_for_workflows_completion(ctx, user_session)
    
    # Get ALL completed workflows for timing analysis
    rv, so, se = ctx.cluster_ctr.exec(
    f"sh -c 'csctl system-maintenance list -a -ojson -n {user_session}'")
    assert rv == 0, se

    if not so.strip():
        logger.info("No workflows found (empty output)")
        all_workflows_response = {"workflows": []}
    else:
        all_workflows_response = json.loads(so)

    expected_workflows = 4
    actual_workflows = len(all_workflows_response.get('workflows', []))
    assert actual_workflows == expected_workflows, f"Expected {expected_workflows} workflows but got {actual_workflows}"

    logger.info("All system maintenance workflows completed successfully")
    
    first_sys0_job_completion_time = ""
    first_sys1_job_execution_time = ""
    first_sys1_job_completion_time = ""
    second_sys0_job_execution_time = ""
    second_sys0_job_completion_time = ""
    second_sys1_job_execution_time = ""
    third_sys0_job_execution_time = ""
    fourth_sys0_job_execution_time = ""

    for workflow in all_workflows_response["workflows"]:
        if workflow["workflowId"] == workflow_id1:
            for job in workflow["jobs"]:
                if job["systems"] == ["kapi-0"]:
                    first_sys0_job_completion_time = job["completionTime"]
        elif workflow["workflowId"] == workflow_id2:
            for job in workflow["jobs"]:
                if job["systems"] == ["kapi-1"]:
                    first_sys1_job_execution_time = job["executionTime"]
                    first_sys1_job_completion_time = job["completionTime"]
                elif job["systems"] == ["kapi-0"]:
                    second_sys0_job_execution_time = job["executionTime"]
                    second_sys0_job_completion_time = job["completionTime"]
        elif workflow["workflowId"] == workflow_id3:
            for job in workflow["jobs"]:
                if job["systems"] == ["kapi-1"]:
                    second_sys1_job_execution_time = job["executionTime"]
                elif job["systems"] == ["kapi-0"]:
                    third_sys0_job_execution_time = job["executionTime"]
                    third_sys0_job_completion_time = job["completionTime"]
        elif workflow["workflowId"] == workflow_id4:
            for job in workflow["jobs"]:
                if job["systems"] == ["kapi-0"]:
                    fourth_sys0_job_execution_time = job["executionTime"]

    assert first_sys0_job_completion_time <= second_sys0_job_execution_time
    assert second_sys0_job_completion_time <= third_sys0_job_execution_time
    assert first_sys1_job_completion_time <= second_sys1_job_execution_time
    assert third_sys0_job_completion_time <= fourth_sys0_job_execution_time

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'kubectl -n {user_session} delete wsjob --all'")
    assert rv == 0, se

    rv, so, se = ctx.cluster_ctr.exec(
        f"sh -c 'csctl system-maintenance list -a -ojson -n {user_session}'")
    assert rv == 0, se
    response_json = json.loads(so)
    # Expect no more system maintenance workflows
    assert len(response_json["workflows"]) == 0

    # cleanup
    must_exec_cli(f"{session_api} update {user_session} --system-count 0")
    must_exec_cli(f"{session_api} update {system_namespace} --system-count 2")
    must_exec_cli(f"{session_api} delete {user_session} --skip-usernode-cleanup")


def wait_for_workflows_completion(ctx, user_session, deadline_seconds=120):
    """Wait for all active workflows to complete"""
    wait_time_seconds = 0
    while wait_time_seconds < deadline_seconds:
        rv, so, se = ctx.cluster_ctr.exec(
            f"sh -c 'csctl system-maintenance list -ojson -n {user_session}'")
        assert rv == 0, se
        response_json = json.loads(so)
        
        if len(response_json["workflows"]) == 0:
            logger.info("All workflows completed")
            break
        else:
            # Debug info
            rv, so, se = ctx.cluster_ctr.exec(
                f"sh -c 'csctl system-maintenance list -n {user_session}'")
            assert rv == 0, se
            logger.info(f"Waiting for workflows to complete: {so}")

        time.sleep(5)
        wait_time_seconds += 5

    assert wait_time_seconds < deadline_seconds, f"Workflows did not complete within {deadline_seconds} seconds"
    return wait_time_seconds
