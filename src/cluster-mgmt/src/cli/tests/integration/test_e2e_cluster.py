import logging
import pathlib
import shlex

import pytest
import yaml
from click.testing import CliRunner

from common.cluster import Cmd
from common.cluster import DirCopyCmd
from common.cluster import FileCopyCmd
from common.cluster import MkdirCmd
from common.context import CliCtx

from cs_cluster import cli

logger = logging.getLogger("cs_cluster.test.e2e_cluster")



@pytest.mark.parametrize(
    "cmd",
    [
        "--cluster {cluster} cluster use",
        "cluster use {cluster}",
    ],
)
def test_cluster_use(
        kind_cluster: str,
        cmd: str):
    cfgpath = pathlib.Path(".cs_cluster.conf")
    if cfgpath.exists():
        cfgpath.unlink()

    runner = CliRunner()
    result = runner.invoke(cli, shlex.split(cmd.format(cluster=kind_cluster)))

    assert result.exit_code == 0
    cfg = yaml.safe_load(cfgpath.read_text())
    assert kind_cluster == cfg["cluster"]


def test_ssh_cluster_controller(kind_cluster, ssh_ctx: CliCtx, tmp_path):
    assert ssh_ctx is not None
    ctr = ssh_ctx.cluster_ctr
    ctr.exec_cmds_cluster([Cmd("ls")])
    ctr.exec_cluster("ls")
    ctr.must_exec("ls")

    dir = pathlib.Path(tmp_path).joinpath("d")
    dir.mkdir()
    file = dir.joinpath("f")
    file.write_text("foo")
    ctr.exec_cmds([
        FileCopyCmd(file, "/root/f"),
        MkdirCmd("/root/d"),
        DirCopyCmd(dir, "/root/d"),
    ])
    ctr.must_exec("stat /root/f")
    ctr.must_exec("stat /root/d/f")
