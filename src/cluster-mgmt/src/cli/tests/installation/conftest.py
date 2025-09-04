import logging
import json
import pathlib
import shlex
import shutil
import socket
import subprocess as sp
import time
import typing
import re
import contextlib
from contextlib import closing
from click.testing import CliRunner
import pytest
import yaml

from common import KIND_NODE_IMAGE
from common.cluster import Cmd
from tests.integration.conftest import parsebool
from tests.integration.conftest import _sp_exec
from tests.integration.conftest import _find_local_ssh_pubkey
from common.cluster import DockerClusterController
from common.context import CliCtx
from common.context import load_cli_ctx
from cs_cluster import cli

logger = logging.getLogger("tests.installation.conftest")

CLI_BASE_DIR = pathlib.Path(__file__).parent.parent.parent
CB_DOCKER_CFG = pathlib.Path('/cb/ecr-token/config.json')
CB_DOCKER_CFG_OLD = pathlib.Path('/cb/data/shared-docker-token/config.json')


def pytest_addoption(parser):
    parser.addoption("--use-cluster", action="store", default="NOT_SET")
    parser.addoption("--keep-cluster", action="store", default=False)
    parser.addoption("--new-cluster-name", action="store", default="")


def docker_secret_path() -> typing.Optional[pathlib.Path]:
    # jenkins / AWS envs still use the old path, but eventually IT will move to CB_DOCKER_CFG
    if CB_DOCKER_CFG.is_file():
        return CB_DOCKER_CFG
    elif CB_DOCKER_CFG_OLD.is_file():
        return CB_DOCKER_CFG_OLD
    return None


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def kinder_cluster(pytestconfig) -> typing.Iterator[str]:
    """Fixture adds the name of a pytest. The logic for creating new clusters is this way so that multiple tests
    can be run in parallel on Jenkins."""
    # --use-cluster="" to use the existing cluster on this machine or make one and keep it
    # --use-cluster="xxx" to use cluster xxx
    # not setting --use-cluster will create a cluster that's cleaned up at the end of the test

    use_cluster = pytestconfig.getoption("use_cluster")
    keep_cluster = parsebool(pytestconfig.getoption("keep_cluster"))
    keep_cluster = keep_cluster or use_cluster == "" or use_cluster != "NOT_SET"
    new_cluster_name = pytestconfig.getoption("new_cluster_name")

    cluster_out = _sp_exec("kind get clusters")
    clusters = re.split(r'\s+', cluster_out) if "No kind clusters" not in cluster_out else []
    if use_cluster == "" and clusters:
        use_cluster = clusters[0]
    need_cluster = use_cluster == 'NOT_SET' or use_cluster == ""

    if need_cluster:
        cluster_name = f"pytest-{int(time.time())}" if not new_cluster_name else new_cluster_name
        cluster_path = CLI_BASE_DIR / "clusters" / cluster_name
        shutil.copytree(f"{CLI_BASE_DIR}/clusters/kind", str(cluster_path), ignore=lambda src, names: names if src == "build" else [])
        _sp_exec(f"{CLI_BASE_DIR}/bin/kinder create cluster --name={cluster_name} "
                 f"--control-plane-nodes 3 --worker-nodes 1 --image={KIND_NODE_IMAGE}")

        # change the node names, add hostnames for ssh tests
        with open(cluster_path / "ha-cluster.yaml", 'r') as f:
            contents = f.read()
        cluster = yaml.safe_load(contents)
        cluster["name"] = cluster_name
        cluster_ips = []
        for i, node in enumerate(cluster["nodes"]):
            name = node["name"].replace("kind", cluster_name)
            addrs = json.loads(_sp_exec(f"docker exec {name} ip -j addr show eth0"))
            addr = addrs[0]["addr_info"][0]["local"]
            cluster_ips.append(addr)
            node["name"] = name
            node["networkInterfaces"] = [{
                "name": "eth0",
                "address": addr,
            }]
        with open(cluster_path / "cluster.yaml", 'w') as f:
            updated_cluster = yaml.safe_dump(cluster, sort_keys=False)
            f.write(updated_cluster)
            logger.info(f"updated cluster config:\n{updated_cluster}")

        pkg_props = yaml.safe_load((CLI_BASE_DIR / "apps" / "common" / "internal-pkg-properties.yaml").read_text())
        pkg_props["properties"]["multiMgmtNodes"] = {}
        pkg_props["properties"]["multiMgmtNodes"]["dataVip"] = cluster_ips[0]
        pkg_props["properties"]["multiMgmtNodes"]["mgmtVip"] = cluster_ips[0]
        override_props_file = (CLI_BASE_DIR / "clusters" / cluster_name / "common" / "internal-pkg-properties.yaml")
        override_props_file.parent.mkdir(parents=True, exist_ok=True)
        override_props_file.write_text(yaml.safe_dump(pkg_props))

        _ensure_kinder_cluster_dependencies(cluster["nodes"])

        runner = CliRunner()
        # deploy k8s installation scripts/cluster yaml
        result = runner.invoke(
            cli, shlex.split(
                f"--cluster {cluster_name} -v deploy k8s --from-k8s-version=1.29.9 --k8s-version=1.31.10"),
        )
        ctx = load_cli_ctx(None, cluster_name)
        _, logs, _ = ctx.cluster_ctr.exec("bash -c 'journalctl -u kubelet -S -1m'")
        assert result.exit_code == 0, f"failed to install k8s: {result.stdout}\n{result.stderr} \n kubelet logs: {logs}"

        rv, so, _ = ctx.cluster_ctr.exec("bash -c 'curl -k https://127.0.0.1:8443/readyz'")
        assert rv == 0, f"failed to check cp node readiness, {so}"
        assert result.exit_code == 0, f"{result.output}"
        yield cluster_name

        if not keep_cluster:
            _sp_exec(f"kind delete cluster --name={cluster_name}")
            shutil.rmtree(str(cluster_path))
            shutil.rmtree(f"/tmp/{cluster_name}", ignore_errors=True)
    else:
        yield use_cluster


@pytest.fixture(scope="session")
def ctx(kind_cluster: str) -> typing.Optional[CliCtx]:
    return load_cli_ctx(None, kind_cluster)


@contextlib.contextmanager
def get_cluster_controlplane_ctr(kind_cluster: str) -> DockerClusterController:
    ctr = DockerClusterController([f"{kind_cluster}-control-plane"])
    try:
        ctr.open()
        yield ctr
    finally:
        ctr.close()


def _ensure_kinder_cluster_dependencies(nodes: dict):
    """
    Starts an ssh server on kind nodes using some public key it finds as ~/.ssh/*.pub.
    """
    logger.info(f"kind version: {str(sp.check_output('kind version'.split()), 'utf-8')}")

    node_names = [node["name"] for node in nodes]
    ctr = DockerClusterController(node_names).open()
    _, containerd_version, _ = ctr.must_exec("containerd --version")
    logger.info(f"containerd version: {containerd_version}")

    # append etc hosts
    cmds = []
    for node in nodes:
        cmd = f"echo {node['networkInterfaces'][0]['address']} {node['name']} >> /etc/hosts"
        cmds.append(Cmd(f"bash -c '{cmd}'"))
    ctr.exec_cmds_cluster(cmds)

    # copy local ssh pub key to kind nodes
    id_pub = _find_local_ssh_pubkey().read_text()
    ctr.exec_cmds_cluster([
        Cmd(f"bash -c 'echo \"root:root\" | chpasswd'"),
        Cmd(f"bash -c 'echo \"{id_pub}\" >> /root/.ssh/authorized_keys'"),
        Cmd(f"bash -c 'test -f {CB_DOCKER_CFG} || "
            f"{{ mkdir -p $(dirname {CB_DOCKER_CFG}) && touch {CB_DOCKER_CFG} ; }}'"),
    ])

    # Allow 'ssh-rsa' as PubkeyAcceptedKeyTypes. Otherwise, paramiko 2.7.1 SSHClient will fail to connect.
    # option PubKeyAcceptedAlgorithms is supported from OpenSSH version 8.5 and above.
    ctr.exec_cmds_cluster([
        Cmd("bash -c 'echo \"PubkeyAcceptedKeyTypes +ssh-rsa\" >> /etc/ssh/sshd_config'"),
        Cmd("bash -c 'systemctl restart sshd'")
    ])
    ctr.close()


# order tests to ensure update test don't run first
def pytest_collection_modifyitems(session, config, items):
    items.sort(key=lambda item: item.name)