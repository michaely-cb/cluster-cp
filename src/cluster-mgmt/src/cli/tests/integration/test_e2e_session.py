import json
import logging
import pathlib
import random
import shlex
import subprocess as sp
import tarfile
import time
import zipfile

from click.testing import CliRunner

from common import ECR_URL
from common.cluster import DockerClusterController
from common.context import load_cli_ctx
from cs_cluster import cli
from .conftest import (
    get_cluster_server_image,
    use_usernode,
    assert_cluster_server_ready,
)

import pytest


logger = logging.getLogger("cs_cluster.test.e2e_session")
current_dir = pathlib.Path(__file__).parent.absolute()


def assert_eventually(timeout, interval, desc=""):
    now = time.time()
    endtime = now + timeout
    while now < endtime:
        yield now
        delay = (now + interval) - time.time()
        if delay > 0:
            time.sleep(delay)
        now = time.time()
    assert False, f"failed to complete in time: {desc}"

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


def test_session_api_e2e(
    kind_cluster: str, run_with_caplog, tmp_path: pathlib.Path, mock_rel_artifact_dir: pathlib.Path, system_ns: str = 'job-operator'
):
    """This test exercises the session CRUD API through cs_cluster.py"""
    ctx = load_cli_ctx(None, kind_cluster)
    session_api = f"--cluster {kind_cluster} session"
    user_session = f"user-{random.randint(0, 10000000)}"
    default_spec = "--parallel-compile-count 0 --parallel-execute-count 0 --wsjob-log-preferred-storage-type local"
    volume_spec = "--container-path /tmp --allow-venv --use-hostpath"
    deploy_options = f"--rel-path {mock_rel_artifact_dir}"

    assert_cluster_server_ready(ctx, system_ns)

    # Should not be able to delete the systemNS
    cmd = f"{session_api} delete {system_ns} --skip-usernode-cleanup"
    rv, so, se = run_with_caplog(cmd)
    assert rv != 0, so
    assert "cannot be deleted" in se

    # Delete all user namespaces first
    # This is needed mostly for interactively debugging the e2e tests in an iterative manner
    # Note: we exclude the unassigned resources from the deletion
    cmd = f"csctl session list -ojson | jq -r '.items[] |.name' | grep -v '*unassigned' | xargs -I {{}} csctl session delete {{}}"
    ctx.cluster_ctr.exec(f'bash -c "{cmd}"')

    # Set up the systemNS not to own any system
    cmd = f"{session_api} update {system_ns} --system-count 0"
    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, se
    assert "kapi-0,kapi-1" not in so
    assert "nodegroup0" not in so

    # Set up the systemNS to own two systems
    cmd = f"{session_api} update {system_ns} --system-count 2 {default_spec}"
    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, se
    assert "kapi-0,kapi-1" in so
    assert "nodegroup0" in so

    cmd = f"{session_api} list --unassigned-only -oyaml"
    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, se
    assert "kapi-0" not in so
    assert "kapi-1" not in so
    assert "nodegroup0" not in so

    # systemNS should have two systems (kapi-0, kapi-1) and 1 nodegroup (nodegroup0)
    cmd = f"{session_api} list"
    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, se
    assert "kapi-0,kapi-1" in so
    assert "nodegroup0" in so

    # Same output from the 'get' api
    cmd = f"{session_api} get {system_ns}"
    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, se
    assert "kapi-0,kapi-1" in so
    assert "nodegroup0" in so

    # systemNS nodegroups should have nodegroup0
    cmd = f"{session_api} list-nodegroups"
    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, se
    assert "nodegroup0" in so

    # Same output by applying the name filter
    cmd = f"{session_api} list-nodegroups --session-name {system_ns}"
    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, se
    assert "nodegroup0" in so

    with use_usernode(ctx) as host_port:
        dockername, port = host_port
        with DockerClusterController([dockername]) as ctr:
            usernode_host_options = f"--usernodes=localhost --usernode-port={port} --usernode-password=''"
            usernode_options = f"{usernode_host_options} --usernode-config-only"

            # Failed to create a volume in a session that has not been created
            cmd = f"{session_api} create-volume --session-name {user_session} --volume-name tmp-vol {volume_spec}"
            rv, so, se = run_with_caplog(cmd)
            assert rv != 0, se
            assert "Please create the session first" in se

            # We need to first unassign a system from systemNS prior to re-assigning
            cmd = f"{session_api} create {user_session} --system-count 1 {default_spec} {deploy_options} {usernode_options}"
            rv, so, se = run_with_caplog(cmd)
            assert rv != 0, se
            assert "cluster only has 0 available of 2 total systems" in se

            # We should see two NSRs regardless
            cmd = f"{session_api} list"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert system_ns in so
            assert user_session in so

            # Unassign systems from systemNS
            cmd = f"{session_api} update {system_ns} --system-count 0 {default_spec}"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "kapi-0" not in so
            assert "kapi-1" not in so
            assert "nodegroup0" not in so

            cmd = f"{session_api} list --unassigned-only -oyaml"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "kapi-0" in so
            assert "kapi-1" in so
            assert "nodegroup0" in so

            # Same output by applying the name filter
            cmd = f"{session_api} list-nodegroups --session-name {system_ns}"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "nodegroup0" in so
            assert system_ns not in so

            # Create the new session with kapi-0 (fail given already created)
            cmd = f"{session_api} create {user_session} --system-count 1 {default_spec} {deploy_options} {usernode_options}"
            rv, so, se = run_with_caplog(cmd)
            assert rv != 0, se
            assert "already exists" in se

            # Update the session with kapi-0
            cmd = f"{session_api} update {user_session} --system-count 1 {default_spec} {deploy_options} {usernode_options}"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert user_session in so
            assert "kapi-0" in so
            assert "nodegroup0" in so

            # Cluster server should be running in the new session
            exit_code, _, se = ctx.cluster_ctr.exec(f"kubectl -n {user_session} get deploy cluster-server")
            assert exit_code == 0

            # Cluster server in the new session should use the proper tag
            server_image = get_cluster_server_image(kind_cluster)
            server_image_tag = server_image.split(":")[-1]
            exit_code, so, se = ctx.cluster_ctr.exec(
                f"kubectl -n {user_session} get deploy cluster-server " \
                "-o jsonpath='{.spec.template.spec.containers[0].image}'"
            )
            assert exit_code == 0, se
            assert so.strip().endswith(server_image_tag)

            # Cluster server in the new session should use the expected cbcore image
            exit_code, so, se = ctx.cluster_ctr.exec(
                f"kubectl -n {user_session} get deploy cluster-server " \
                "-o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"WSJOB_DEFAULT_IMAGE\")].value}' "
            )
            assert exit_code == 0, se
            assert so.strip().endswith("cbcore:fake")

            # Cluster server role created
            exit_code, _, se = ctx.cluster_ctr.exec(
                f"kubectl -n {user_session} get role cluster-server-ns-role log-export-ns-role image-builder-role"
            )
            assert exit_code == 0, se

            # Cluster roles created
            exit_code, _, se = ctx.cluster_ctr.exec(
                f"kubectl get clusterrole {user_session}-cluster-server-cluster-role {user_session}-log-export-cluster-role"
            )
            assert exit_code == 0, se

            # User session should have nodegroup0, which has system affinity to kapi-0
            cmd = f"{session_api} list-nodegroups"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert user_session in so
            assert "nodegroup0" in so

            # Same output by applying the name filter
            cmd = f"{session_api} list-nodegroups --session-name {user_session}"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert user_session in so
            assert "nodegroup0" in so

            # User node should be properly deployed
            rv, so, se = ctr.exec(
                "sh -c 'jq -r \".clusters[].namespaces[].name\" /opt/cerebras/config_v2'"
            )
            assert rv == 0, se
            assert (
                so.strip() == user_session
            ), "expected the user node to be properly set up but failed"

            # Create a volume
            cmd = f"{session_api} create-volume --session-name {user_session} --volume-name tmp-vol {volume_spec}"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se

            # Update a volume
            cmd = f"{session_api} update-volume --session-name {user_session} --volume-name tmp-vol {volume_spec} --readonly"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se

            # List volumes
            cmd = f"{session_api} list-volumes --session-name {user_session}"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "tmp-vol" in so

            # Test the volume
            cmd = f"{session_api} test-volume --session-name {user_session} --volume-name tmp-vol"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "success" in se

            # Delete the volume
            cmd = f"{session_api} delete-volume --session-name {user_session} --volume-name tmp-vol"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se

            # Fail to see the deleted volume from list
            cmd = f"{session_api} list-volumes --session-name {user_session}"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "tmp-vol" not in se
            assert "no volumes configured" in se

            # Update the user NSR to have too many systems
            cmd = f"{session_api} update {user_session} --system-count 3"
            rv, so, se = run_with_caplog(cmd)
            assert rv != 0, se
            assert "session update failed" in se

            # Unassign both systems from systemNS
            cmd = f"{session_api} update {system_ns} --system-count 0"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "kapi-0" not in so
            assert "kapi-1" not in so
            assert "nodegroup0" not in so

            # Assign both systems to the user NSR
            cmd = f"{session_api} update {user_session} --system-count 2 {default_spec} {deploy_options} {usernode_options}"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "kapi-0,kapi-1" in so
            assert "nodegroup0" in so

            # Delete the user NSR
            cmd = f"{session_api} delete {user_session} {usernode_host_options} --skip-usernode-cleanup"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "Successfully deleted session" in so

            # Retrieve the non-existing user NSR
            cmd = f"{session_api} get {user_session}"
            rv, so, se = run_with_caplog(cmd)
            assert rv != 0, se
            assert "not found" in se

            # User node should not leave access information regarding the deleted user session
            rv, so, se = ctr.exec(
                "sh -c 'jq -r \".clusters[].namespaces[].name\" /opt/cerebras/config_v2'"
            )
            assert rv == 0, se
            assert (
                so.strip() == ""
            ), "expected the user node to be properly cleaned up but failed"

            # The namespace should be deleted in 60 seconds
            start_time = time.time()
            timeout = 60
            namespace_cascade_delete = False
            while time.time() < start_time + timeout:
                exit_code, so, se = ctx.cluster_ctr.exec(
                    f"kubectl get namespace {user_session} --ignore-not-found"
                )
                assert exit_code == 0, se
                ns_deleted = so == ""
                if ns_deleted:
                    namespace_cascade_delete = True
                    break

                time.sleep(1)
            assert namespace_cascade_delete, f"Namespace for {user_session} was not deleted after {timeout} seconds"

            # Due to the last user nsr was deleted, resources were left in system namespace or unassigned namespace
            # Unassign both systems from systemNS so all resources will be in the unassigned namespace
            cmd = f"{session_api} update {system_ns} --system-count 0"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "kapi-0" not in so
            assert "kapi-1" not in so
            assert "nodegroup0" not in so

            # Fail when creating the new session with invalid workload type
            workload_type = "randomstring"
            cmd = f"{session_api} create {user_session} --system-count 1 --workload-type {workload_type} {default_spec} {deploy_options} {usernode_options}"
            rv, so, se = run_with_caplog(cmd)
            assert rv != 0, se
            assert "invalid workload type" in se

            # Recreate the session with the same name with kapi-0 should still work
            workload_type = "inference"
            cmd = f"{session_api} create {user_session} --system-count 1 --workload-type {workload_type} {default_spec} {deploy_options} {usernode_options}"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert user_session in so
            assert "kapi-0" in so
            assert "nodegroup0" not in so # inference workload should not have nodegroup0 assigned

            # Make sure suppress-affinity-errors flag is accepted (no-op here since we are not changing system count)
            cmd = f"{session_api} update {user_session} --system-count 1 --suppress-affinity-errors"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se

            # Delete the user NSR
            cmd = f"{session_api} delete {user_session} {usernode_host_options}"
            rv, so, se = run_with_caplog(cmd)
            assert rv == 0, se
            assert "Successfully deleted session" in so

    # We should see one NSR
    cmd = f"{session_api} list"
    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, se
    assert system_ns in so
    assert user_session not in so

    # Update the systemNS to have two systems (kapi-0, kapi-1) and 1 nodegroup (nodegroup0)
    cmd = f"{session_api} update {system_ns} --system-count 2 {default_spec}"
    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, se
    assert "kapi-0,kapi-1" in so
    assert "nodegroup0" in so

    # systemNS nodegroups should have nodegroup0
    cmd = f"{session_api} list-nodegroups"
    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, se
    assert "nodegroup0" in so

    # Should fail the update given no large memory group
    cmd = f"{session_api} update {system_ns} --system-count 2 {default_spec} --large-memory-rack-count 1"
    rv, so, se = run_with_caplog(cmd)
    assert rv != 0, se
    assert "cluster only has 0 available of 0 total large memory racks but 1 requested" in se


def test_session_scale_down(
    kind_cluster: str,
    tmp_path: pathlib.Path,
    mock_rel_artifact_dir: pathlib.Path,
    system_ns: str = 'job-operator'
):
    """Primarily tests job-operator's session scale down/up feature"""
    ctx = load_cli_ctx(None, kind_cluster)
    session_api = f"--cluster {kind_cluster} session"
    user_session = f"user-{random.randint(0, 10000000)}"
    deploy_options = f"--rel-path {mock_rel_artifact_dir}"

    assert_cluster_server_ready(ctx, system_ns)

    # Delete all user namespaces first
    # This is needed mostly for interactively debugging the e2e tests in an iterative manner
    # Note: we exclude the unassigned resources from the deletion
    cmd = f"csctl session list -ojson | jq -r '.items[] |.name' | grep -v '*unassigned' | xargs -I {{}} csctl session delete {{}}"
    ctx.cluster_ctr.exec(f'bash -c "{cmd}"')

    def must_exec_cli(cmd):
        logger.info(f"exec cli cmd: {cmd}")
        result = CliRunner().invoke(cli, shlex.split(cmd))
        assert result.exit_code == 0, result.output

    # Set up the systemNS not to own any system, usernamespace to have systems and a usernode configured
    must_exec_cli(f"{session_api} update {system_ns} --system-count 0")

    with use_usernode(ctx) as host_port:
        dockername, port = host_port
        with DockerClusterController([dockername]) as ctr:
            # Set up the systemNS to own two systems with deploy. First create the session with a cluster-server
            # then use config-only usernode deploy to deploy real csctl to the session
            usernode_args = f"--usernodes=localhost --usernode-port={port} --usernode-password=''"
            must_exec_cli(
                f"{session_api} create {user_session} --system-count 2 --wsjob-log-preferred-storage-type local --rel-path={mock_rel_artifact_dir} {usernode_args}"
            )
            must_exec_cli(
                f"{session_api} update {user_session} --system-count 2 --usernode-config-only {usernode_args}"
            )

            # validate csctl functions
            _, out, _ = ctr.must_exec("csctl get cluster")
            assert "kapi-0" in out, out

            # scale down the session using a test annotation to force what is regularly scheduled on a cron trigger
            ctx.cluster_ctr.must_exec(f'kubectl annotate nsr {user_session} cerebras/scale-down-now=1')
            must_exec_cli(f"{session_api} update {user_session} --system-count 0")

            # validate csctl still functions
            _, out, _ = ctr.must_exec("csctl get cluster")
            assert "kapi-0" not in out, out

            for _ in assert_eventually(10, 1, "scale down"):
                _, out, _ = ctx.cluster_ctr.must_exec(f"kubectl get deploy -n {user_session} cluster-server -ojsonpath='{{.spec.replicas}}'")
                if out.strip() != "0":
                    logger.info(f"waiting for scale down, got: {out}")
                    continue
                # validate csctl still functions, but output should be empty
                rv, out, _ = ctr.exec("csctl get cluster")
                if rv == 0 and "kapi-0" not in out:
                    break
                logger.info(f"waiting for csctl, got: {rv}/{out}")

            # validate scale up occurs automatically
            must_exec_cli(f"{session_api} update {user_session} --system-count 2")
            for _ in assert_eventually(30, 1, "scale up"):
                _, out, _ = ctx.cluster_ctr.must_exec(
                    f"kubectl get pods -n {user_session} -l app.kubernetes.io/name=cluster-server "
                    "-o jsonpath='{range .items[*]}{.status.conditions[?(@.type==\"Ready\")].status}{end}'"
                )
                if out.strip() != "True":
                    logger.info(f"waiting for Ready, got: {out}")
                    continue

                # validate csctl functions
                rv, out, _ = ctr.exec("csctl get cluster")
                if rv == 0 and "kapi-0" in out:
                    break
                logger.info(f"waiting for csctl, got: {rv}/{out}")

    # cleanup
    must_exec_cli(f"{session_api} update {user_session} --system-count 0")
    must_exec_cli(f"{session_api} update {system_ns} --system-count 2")

