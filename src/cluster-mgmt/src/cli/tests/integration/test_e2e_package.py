import logging
import json
import pathlib
import shlex
import shutil
import subprocess as sp
from typing import Callable

import pytest
from click.testing import Result
from common.cluster import Cmd, DirCopyCmd
from common.context import load_cli_ctx
from common import REGISTRY_URL
from common.context import CliCtx

from tests.integration.conftest import (
    TEST_PULL_IMAGE,
    get_cluster_server_image,
)

logger = logging.getLogger("cs_cluster.test.e2e_package")


@pytest.fixture
def require_pkg_path(kind_cluster: str):
    ctx = load_cli_ctx(None, kind_cluster)
    pkg_dir = ctx.build_dir.joinpath("pkg-test")
    shutil.rmtree(pkg_dir, ignore_errors=True)
    pkg_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ctx.apps_dir / "csadm" / "csadm.sh", pkg_dir)
    yield pkg_dir
    shutil.rmtree(pkg_dir, ignore_errors=True)


def test_package_cbcore_and_cluster_server(
        tmpdir: pathlib.Path,
        kind_cluster: str,
        require_pkg_path: pathlib.Path,
        cs_cluster: Callable[[str], Result],
):
    """ Test ensures that the cbcore image value is set in the cluster-server deployment."""

    server_image = get_cluster_server_image(kind_cluster)
    # stage images in build dir to fake the artifact path
    cmds = [
        f"docker image save -o {tmpdir}/cluster-server-test.tar {server_image}",
        f"gzip -f {tmpdir}/cluster-server-test.tar",
        f"mv {tmpdir}/cluster-server-test.tar.gz {tmpdir}/cluster-server-test.docker",

        f"docker pull {TEST_PULL_IMAGE}",
        f"docker image save -o {tmpdir}/cbcore.tar {TEST_PULL_IMAGE}",
        f"gzip -f {tmpdir}/cbcore.tar",
        f"mv {tmpdir}/cbcore.tar.gz {tmpdir}/cbcore-test.docker",
    ]
    for cmd in cmds:
        logger.info(f"running command on host: {cmd}")
        sp.check_call(shlex.split(cmd))

    result = cs_cluster(
        "package all --components cbcore,cluster-server "
        f"--release-artifact-path {tmpdir} --pkg-path {require_pkg_path}"
    )
    assert result.exit_code == 0, result

    # copy the package to the cluster and run csadm.sh to install it
    ctx = load_cli_ctx(None, kind_cluster)
    remote_pkg_dir = pathlib.Path(ctx.remote_build_dir) / require_pkg_path.name
    ctx.cluster_ctr.exec_cmds([
        Cmd(f"rm -rf {remote_pkg_dir}"),
        Cmd(f"mkdir -p {remote_pkg_dir}"),
        DirCopyCmd(require_pkg_path, remote_pkg_dir),
        Cmd(f"{remote_pkg_dir}/csadm.sh install {remote_pkg_dir}/manifest.json --debug --yes"),
    ])

    # check that the cbcore image is set in the cluster-server deployment
    _, cbcore_image, serr = ctx.cluster_ctr.exec(
        "kubectl get deployment cluster-server -n job-operator "
        "-o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"WSJOB_DEFAULT_IMAGE\")].value}'"
    )
    assert cbcore_image.strip().startswith(f"{REGISTRY_URL}/"), serr

    # check that the csadm assets were included
    _, so, _ = ctx.cluster_ctr.must_exec(f"ls {remote_pkg_dir}/csadm")
    assert "convert_network.py" in so


def test_package_registry_correct_paths_in_manifest(
        tmpdir: pathlib.Path,
        cs_cluster: Callable[[str], Result],
):
    result = cs_cluster(
        f"package all --components=private-registry,log-rotation --pkg-path {tmpdir}"
    )
    assert result.exit_code == 0, result.stderr
    manifest = json.loads((tmpdir / "manifest.json").read_text('utf-8'))
    assert manifest["componentPaths"][0] == "registry/apply-registry.sh"
    assert manifest["componentPaths"][1] == "log-rotation/apply-log-rotation.sh"
