import pathlib
import shlex

from click.testing import CliRunner
from cs_cluster import cli


def test_package_pack_only_fails_when_file_not_exists(tmpdir: pathlib.Path):
    cmd = f"package all --components=private-registry,log-rotation --pkg-path {tmpdir} --pack-only"

    runner = CliRunner()
    result = runner.invoke(cli, ["--cluster", "generic-multibox", "-v", *shlex.split(cmd)])

    assert result.exit_code != 0
    assert "does not exist" in result.output