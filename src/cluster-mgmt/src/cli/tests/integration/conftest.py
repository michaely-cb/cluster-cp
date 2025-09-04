import logging
import json
import ipaddress
import os
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
from typing import Tuple

import kubernetes
import pytest
import yaml
from click.testing import CliRunner
from click.testing import Result
from distutils.version import StrictVersion
import pprint

from common import BUSYBOX_IMAGE
from common import KIND_NODE_IMAGE
from common import NFS_SHARE_DIR
from common.cluster import Cmd
from common.cluster import FileCopyCmd
from common.cluster import DockerClusterController
from common.cluster import SSHClusterController
from common.context import CliCtx
from common.context import load_cli_ctx
from common.models import Cluster
from cs_cluster import cli

logger = logging.getLogger("tests.integration.conftest")
switch_name = "sc-r1rb1-100gsw32"

TEST_PULL_IMAGE = BUSYBOX_IMAGE
ret = sp.run(shlex.split(f"docker image pull {BUSYBOX_IMAGE}"))
if ret.returncode != 0:
    logger.warning("switch from ECR to docker repo")
    TEST_PULL_IMAGE = "busybox:1.34.1"
CLI_BASE_DIR = pathlib.Path(__file__).parent.parent.parent

CB_DOCKER_CFG = pathlib.Path('/cb/ecr-token/config.json')
CB_DOCKER_CFG_OLD = pathlib.Path('/cb/data/shared-docker-token/config.json')


def parsebool(s: typing.Union[str, bool]) -> bool:
    return s if type(s) == bool else s.strip().lower() in {"t", "true", "1", "yes"}


def pytest_addoption(parser):
    parser.addoption("--use-cluster", action="store", default="NOT_SET")
    parser.addoption("--keep-cluster", action="store", default=False)


@pytest.fixture
def cluster_server_image() -> str:
    return os.environ.get("CLUSTER_SERVER_IMAGE", "")


@pytest.fixture
def job_operator_image() -> str:
    return os.environ.get("JOB_OPERATOR_IMAGE", "")


@pytest.fixture
def cbcore_image() -> str:
    return os.environ.get("CBCORE_IMAGE", TEST_PULL_IMAGE)


def get_spare_root() -> str:
    spare_parent = os.environ.get("CLUSTER_MGMT_SPARE_PARENT_PATH", "/spare")
    return f"{spare_parent}/.cluster_mgmt_spare"


@pytest.fixture(autouse=True)
def log_environment_debug_info(ctx: CliCtx):
    yield
    _log_kind_disk_usage()
    _log_env(ctx)


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


def _kind_config_stdin(cluster_name: str) -> Tuple[str, typing.List[int]]:
    """Create kind cluster configuration with ports mapping for ssh
    Returns: bytes form of kind cluster config, and list of ssh ports for each container
    """
    controlplane_port, worker_port = find_free_port(), find_free_port()

    # Create a folder on the host. Creating this with `mktemp` will create
    # the folder on NFS which has restrictive permissions making it such that
    # `docker cp` will fail when it tries to change the file owner.
    tmp_dir = f"/tmp/{cluster_name}"
    sp.check_call(f"mkdir -p {tmp_dir}".split())
    kind_node_image = os.environ.get("KIND_NODE_IMAGE", KIND_NODE_IMAGE)
    kernel_release = StrictVersion(str(sp.check_output(shlex.split("bash -c 'uname -r | cut -d- -f1'")),
                                       'utf-8').strip())
    # https://docs.cilium.io/en/v1.13/operations/system_requirements/
    # disableDefaultCNI = True if kernel_release >= StrictVersion("4.18.0") else False
    disableDefaultCNI = False
    logger.info(f"kernel_release: {kernel_release}, disableDefaultCNI: {disableDefaultCNI}")

    cfg_mounts = [
        {"hostPath": tmp_dir, "containerPath": NFS_SHARE_DIR, },
        {"hostPath": f"{get_spare_root()}/cluster-mgmt", "containerPath": "/n0/cluster-mgmt", }
    ]
    secret_path = docker_secret_path()
    if secret_path:
        cfg_mounts.append(
            {
                "hostPath": str(secret_path.parent),
                "containerPath": str(CB_DOCKER_CFG.parent),
                "readOnly": True,
            }
        )

    cp_extra_mounts = cfg_mounts.copy()
    cp_extra_mounts.append({"hostPath": f"{get_spare_root()}/containerd-cp", "containerPath": "/var/lib/containerd", })
    wrk_extra_mounts = cfg_mounts.copy()
    wrk_extra_mounts.append({"hostPath": f"{get_spare_root()}/containerd-worker", "containerPath": "/var/lib/containerd", })
    cfg = {
        "kind": "Cluster",
        "apiVersion": "kind.x-k8s.io/v1alpha4",
        "networking": {
            "disableDefaultCNI": disableDefaultCNI,
            "podSubnet": "192.168.128.0/17",
            "serviceSubnet": "192.168.0.0/17",
        },
        "nodes": [
            {
                "role": "control-plane",
                "image": kind_node_image,
                "extraMounts": cp_extra_mounts,
                "extraPortMappings": [
                    {
                        "containerPort": 22,
                        "hostPort": controlplane_port,
                        "protocol": "TCP",
                    },
                ],
            },
            {
                "role": "worker",
                "image": kind_node_image,
                "extraMounts": wrk_extra_mounts,
                "extraPortMappings": [
                    {
                        "containerPort": 22,
                        "hostPort": worker_port,
                        "protocol": "TCP",
                    },
                ],
            },
        ],
    }
    cfg_str = json.dumps(cfg, indent=2)
    logger.info(f"kind config:\n{cfg_str}")
    return cfg_str, [controlplane_port, worker_port]

def generate_node_block(name: str, address: str) -> dict:
    """Helper function to generate a node block."""
    return {
        "interfaces": [
            {
                "address": address,
                "name": "eth0",
                "switch_name": switch_name,
                "switch_port": "Ethernet1/1",
            }
        ],
        "name": name,
    }

def incr_kind_netjson(cluster_name: str, switch_config: dict) -> str:
    """Create network_config.json for incremental nodes
    """

    # To figure out new incremental nodes, the values of various fields in netjson does not matter
    # So, those are hard coded
    prefix = switch_config["subnet"].split("/")[1]
    starting_address = f"{switch_config['virtualStart']}/{prefix}"
    ending_address = f"{switch_config['virtualEnd']}/{prefix}"
    netjson =  {
            "name": f"{cluster_name}",
            "environment": {
                "cluster_prefix": "10.250.248.0/21"
            },
            "management_nodes": [
                generate_node_block(
                    name=f"{cluster_name}-control-plane",
                    address="10.240.0.70"             
                )
            ],
            "worker_nodes": [
                generate_node_block(
                    name=f"{cluster_name}-worker",
                    address="10.240.0.69"
                ),
                generate_node_block(
                    name=f"{cluster_name}-worker2",
                    address="10.240.0.71"
                ),
            ],
            "switches": [
                {
                    "address": "10.240.0.66/26",
                    "name": switch_name,
                    "prefix": f"{switch_config['subnet']}",
                    "virtual_addrs": {
                        "ending_address": ending_address,
                        "starting_address": starting_address
                    }
                }
            ]
        }
    cfg_str = json.dumps(netjson, indent=2)
    logger.debug(f"network_config.json:\n{cfg_str}")
    return cfg_str
       
def _incr_kind_config_stdin(cluster_name: str) -> Tuple[str, typing.List[int]]:
    """Create kind cluster configuration with ports mapping for ssh
    Returns: bytes form of kind cluster config, and list of ssh ports for each container
    """
    controlplane_port, worker_1_port, worker_2_port = find_free_port(), find_free_port(), find_free_port()

    # Create a folder on the host. Creating this with `mktemp` will create
    # the folder on NFS which has restrictive permissions making it such that
    # `docker cp` will fail when it tries to change the file owner.
    tmp_dir = f"/tmp/{cluster_name}"
    rv = sp.check_call(f"mkdir -p {tmp_dir}".split())
    assert rv == 0, f"failed to create tmp dir {tmp_dir}"
    
    kind_node_image = os.environ.get("KIND_NODE_IMAGE", KIND_NODE_IMAGE)
    kernel_release = StrictVersion(str(sp.check_output(shlex.split("bash -c 'uname -r | cut -d- -f1'")),
                                       'utf-8').strip())
    # https://docs.cilium.io/en/v1.13/operations/system_requirements/
    # disableDefaultCNI = True if kernel_release >= StrictVersion("4.18.0") else False
    disableDefaultCNI = False
    logger.info(f"kernel_release: {kernel_release}, disableDefaultCNI: {disableDefaultCNI}")

    cfg_mounts = [
        {"hostPath": tmp_dir, "containerPath": NFS_SHARE_DIR, },
    ]
    secret_path = docker_secret_path()
    if secret_path:
        cfg_mounts.append(
            {
                "hostPath": str(secret_path.parent),
                "containerPath": str(CB_DOCKER_CFG.parent),
                "readOnly": True,
            }
        )

    cfg = {
        "kind": "Cluster",
        "apiVersion": "kind.x-k8s.io/v1alpha4",
        "networking": {
            "disableDefaultCNI": disableDefaultCNI,
            "podSubnet": "192.168.128.0/17",
            "serviceSubnet": "192.168.0.0/17",
        },
        "nodes": [
            {
                "role": "control-plane",
                "image": kind_node_image,
                "extraMounts": cfg_mounts,
                "extraPortMappings": [
                    {
                        "containerPort": 22,
                        "hostPort": controlplane_port,
                        "protocol": "TCP",
                    },
                ],
            },
            {
                "role": "worker",
                "image": kind_node_image,
                "extraMounts": cfg_mounts,
                "extraPortMappings": [
                    {
                        "containerPort": 22,
                        "hostPort": worker_1_port,
                        "protocol": "TCP",
                    },
                ],
            },
            {
                "role": "worker",
                "image": kind_node_image,
                "extraMounts": cfg_mounts,
                "extraPortMappings": [
                    {
                        "containerPort": 22,
                        "hostPort": worker_2_port,
                        "protocol": "TCP",
                    },
                ],
            },
        ],
    }
    cfg_str = json.dumps(cfg, indent=2)
    logger.info(f"kind config:\n{cfg_str}")
    return cfg_str, [controlplane_port, worker_1_port, worker_2_port]

def _find_local_ssh_pubkey() -> pathlib.Path:
    ssh_dir = pathlib.Path.home() / ".ssh"
    if not ssh_dir.is_dir():
        logger.error(f"ssh directory {ssh_dir} does not exist on test host, cannot run ssh-based tests")
        raise Exception(
            "e2e testing requires ~/.ssh/ to be created - please setup the directory on pytest host machine")

    # We had seen in canary test environments where a bad public key was selected by searching for "id_*.pub".
    default_pub_key = ssh_dir / "id_rsa.pub"
    if default_pub_key.is_file():
        return default_pub_key

    # This is a best-effort approach to select a functional public key.
    for f in ssh_dir.iterdir():
        if f.is_file() and f.name.startswith("id_") and f.name.endswith(".pub"):
            logger.info(f"using id_pub={f}")
            return f

    logger.error(f"unable to locate ssh pub key for test host in {ssh_dir}, cannot run ssh-based tests")
    raise Exception("e2e testing requires local ssh public key in ~/.ssh/ - please setup ssh on pytest host machine")


def _ensure_kind_cluster_dependencies(kind_cluster: str):
    """
    Starts an ssh server on kind nodes using some public key it finds as ~/.ssh/*.pub.
    TODO: clean this up or use custom kind node base image
    """
    logger.info(f"kind version: {str(sp.check_output('kind version'.split()), 'utf-8')}")

    ctr = DockerClusterController([f"{kind_cluster}-control-plane", f"{kind_cluster}-worker"]).open()
    _, containerd_version, _ = ctr.must_exec("containerd --version")
    logger.info(f"containerd version: {containerd_version}")

    # copy local ssh pub key to kind nodes
    id_pub = _find_local_ssh_pubkey().read_text()
    ctr.exec_cmds_cluster([
        Cmd(f"bash -c 'echo \"root:root\" | chpasswd'"),
        Cmd(f"bash -c 'echo \"{id_pub}\" >> /root/.ssh/authorized_keys'"),
        Cmd(f"bash -c 'test -f {CB_DOCKER_CFG} || {{ mkdir -p $(dirname {CB_DOCKER_CFG}) && touch {CB_DOCKER_CFG} ; }}'"),
    ])

    # Allow 'ssh-rsa' as PubkeyAcceptedKeyTypes. Otherwise, paramiko 2.7.1 SSHClient will fail to connect.
    # option PubKeyAcceptedAlgorithms is supported from OpenSSH version 8.5 and above.
    ctr.exec_cmds_cluster([
        Cmd("bash -c 'echo \"PubkeyAcceptedKeyTypes +ssh-rsa\" >> /etc/ssh/sshd_config'"),
        Cmd("bash -c 'systemctl restart sshd'")
    ])
    ctr.close()


def get_kind_vip() -> str:
    """Get the second to last IP in the range of the kind network.
    Kind increments upwards from the first IP in the range, so the second to last IP is safe to use
    unless we create absurdly large clusters.
    """
    network_doc = _sp_exec("docker network inspect kind")
    subnet = json.loads(network_doc)[0]["IPAM"]["Config"][0]["Subnet"]
    net = ipaddress.ip_network(subnet)
    return str(net[-2])


def build_switch_config(cluster_ips: typing.List[str]) -> dict:
    """
    Build switch config by inspecting docker kind network

    Note: leaves 8 IPs free before the start of virtual range. Docker default
    IPAM appears to assign IPs using the lowest free IP. 8 should be enough
    since normally only 1 extra container (usernode) used in addition to kind nodes.
    """
    max_additional_docker_containers = 8
    addrs = sorted([ipaddress.ip_address(ip) for ip in cluster_ips])
    network_doc = json.loads(_sp_exec("docker network inspect kind"))
    subnet = network_doc[0]["IPAM"]["Config"][0]["Subnet"]
    gateway = network_doc[0]["IPAM"]["Config"][0]["Gateway"]
    net = ipaddress.ip_network(subnet)
    return {
        "virtualStart": str(addrs[-1] + max_additional_docker_containers),
        "virtualEnd": str(net.broadcast_address - 2),
        "subnet": str(net),
        "parentnet": str(net),
        "gateway": gateway,
    }


def assert_cluster_server_ready(ctx: CliCtx, ns: str):
    selector = "app.kubernetes.io/name=cluster-server"
    # Flakiness had been seen where any of the three checks failed:
    # 1. ingress is ready
    # 2. svc and ep is ready
    # 3. pod is ready
    ctx.cluster_ctr.must_exec(
        f"timeout 2m bash -c 'while ! kubectl get ingress -n{ns} cluster-server -oyaml | grep -w ip; do sleep 5; done'")
    # Make sure there is only one cluster server pod, which indicates stability after deployments
    ctx.cluster_ctr.must_exec(
        f"timeout 1m bash -c 'until [ \"$(kubectl get pods -l{selector} -n{ns} --no-headers | wc -l)\" -eq 1 ]; do sleep 5; done'")
    _, podIp, _ = ctx.cluster_ctr.must_exec(
        f"kubectl get pods -l{selector} -n{ns} -ojsonpath='{{.items[0].status.podIP}}'")
    ctx.cluster_ctr.must_exec(
        f"timeout 1m bash -c 'while ! kubectl get ep -n{ns} cluster-server -oyaml | grep -w {podIp}; do sleep 5; done'")
    ctx.cluster_ctr.must_exec(
        f"""timeout 1m bash -c 'until [ "$(kubectl get pods -l{selector} -n{ns} -ojsonpath="{{.items[0].status.conditions[?(@.type==\\"Ready\\")].status}}")" == "True" ]; do sleep 5; done'""")


@pytest.fixture(scope="session")
def kind_cluster(pytestconfig) -> typing.Iterator[str]:
    """Fixture adds the name of a pytest. The logic for creating new clusters is this way so that multiple tests
    can be run in parallel on Jenkins."""
    # --use-cluster="" to use the existing cluster on this machine or make one and keep it
    # --use-cluster="xxx" to use cluster xxx
    # not setting --use-cluster will create a cluster that's cleaned up at the end of the test

    use_cluster = pytestconfig.getoption("use_cluster")
    keep_cluster = parsebool(pytestconfig.getoption("keep_cluster"))
    keep_cluster = keep_cluster or use_cluster == "" or use_cluster != "NOT_SET"

    cluster_out = _sp_exec("kind get clusters")
    clusters = re.split(r'\s+', cluster_out) if "No kind clusters" not in cluster_out else []
    if use_cluster == "" and clusters:
        use_cluster = clusters[0]
    need_cluster = use_cluster == 'NOT_SET' or use_cluster == ""

    if need_cluster:
        cluster_name = f"pytest-{int(time.time())}"
        cluster_path = CLI_BASE_DIR / "clusters" / cluster_name
        shutil.copytree(f"{CLI_BASE_DIR}/clusters/kind", str(cluster_path), ignore=lambda src, names: names if src == "build" else [])

        kind_cfg, ssh_ports = _kind_config_stdin(cluster_name)
        with open(cluster_path / "kind.json", 'w') as f:
            f.write(kind_cfg)
        _sp_exec(f"kind create cluster --name={cluster_name} --config={cluster_path}/kind.json")

        # change the node names, add hostnames for ssh tests
        with open(cluster_path / "cluster.yaml", 'r') as f:
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
            node["hostname"] = f"0.0.0.0:{ssh_ports[i]}"
            node["networkInterfaces"] = [{
                "name": "eth0",
                "address": addr,
                "gbps": 3600,
            }]
        cluster["groups"][0]["switchConfig"] = build_switch_config(cluster_ips)
        with open(cluster_path / "cluster.yaml", 'w') as f:
            updated_cluster = yaml.safe_dump(cluster, sort_keys=False)
            f.write(updated_cluster)
            logger.info(f"updated cluster config:\n{updated_cluster}")

        # create an internal-pkg-properties.yaml file with a vip in the subnet of the cluster
        pkg_props = yaml.safe_load((CLI_BASE_DIR / "apps" / "common" / "internal-pkg-properties.yaml").read_text())
        pkg_props["properties"]["multiMgmtNodes"] = {}
        pkg_props["properties"]["multiMgmtNodes"]["dataVip"] = get_kind_vip()
        pkg_props["properties"]["kafka"] = {"pvcSize": "1Gi"}
        pkg_props["properties"]["kubewebhook"] = {"disableSystemNamespaceWebhook": "true"}
        pkg_props["properties"]["jobOperator"] = {"namespaceScaleDownCheckInterval": "0"}
        override_props_file = (CLI_BASE_DIR / "clusters" / cluster_name / "common" / "internal-pkg-properties.yaml")
        override_props_file.parent.mkdir(parents=True, exist_ok=True)
        override_props_file.write_text(yaml.safe_dump(pkg_props))

        _ensure_kind_cluster_dependencies(cluster_name)

        _log_kind_disk_usage()

        # deploy all
        runner = CliRunner()
        result = runner.invoke(
            cli, shlex.split(
                f"--cluster {cluster_name} -v deploy --skip-k8s all "
                f"--config-only --enable-multi-coordinator --disable-user-auth"),
        )
        if result.exit_code != 0:
            logger.error(result.output)
        assert result.exit_code == 0, f"{result.output} {result.stderr}"

        _log_kind_disk_usage()

        # docker in CI has very limited disk space - 50GB
        # Remove deployment packages to free up space
        #
        # Post-2.5 pkg size context:
        # root@pytest-1747056846-control-plane:/home/pkg# du -sh /home/pkg/* | sort -h
        # 119M	/home/pkg/nginx
        # 1.3G	/home/pkg/k8s
        # 1.4G	/home/pkg/common-images
        ctx = load_cli_ctx(None, cluster_name)
        ctx.cluster_ctr.must_exec(f"rm -rf /home/pkg/common-images /home/pkg/k8s /home/pkg/nginx")
        # Python images are not used in CLI e2e tests
        ctx.cluster_ctr.must_exec(f"bash -c \"nerdctl -n k8s.io image ls | grep 'registry.local/python' | awk '{{print $3}}' | xargs -r nerdctl -n k8s.io image rm -f\"")
        _log_kind_disk_usage()

        yield cluster_name

        if not keep_cluster:
            _sp_exec(f"kind delete cluster --name={cluster_name}")
            shutil.rmtree(str(cluster_path))
            shutil.rmtree(f"/tmp/{cluster_name}", ignore_errors=True)
    else:
        yield use_cluster


@pytest.fixture(scope="session")
def kind_incremental_cluster() -> typing.Iterator[str]:   
    """Fixture creates a kind cluster with 1 control plane and 2 worker nodes"""

    cluster_name = f"pytest-incr-{int(time.time())}"
    cluster_path = CLI_BASE_DIR / "clusters" / cluster_name
    shutil.copytree(f"{CLI_BASE_DIR}/clusters/kind", str(cluster_path), ignore=lambda src, names: names if src == "build" else [])
    kind_cfg, ssh_ports = _incr_kind_config_stdin(cluster_name)
    with open(cluster_path / "kind.json", 'w') as f:
        f.write(kind_cfg)
    _sp_exec(f"kind create cluster --name={cluster_name} --config={cluster_path}/kind.json")
    # change the node names, add hostnames for ssh tests
    with open(cluster_path / "cluster.yaml", 'r') as f:
        cluster = yaml.safe_load(f)
    cluster["name"] = cluster_name
    cluster_ips = []
    for i, node in enumerate(cluster["nodes"]):
        name = node["name"].replace("kind", cluster_name)
        addrs = json.loads(_sp_exec(f"docker exec {name} ip -j addr show eth0"))
        addr = addrs[0]["addr_info"][0]["local"]
        cluster_ips.append(addr)
        node["name"] = name
        node["hostname"] = f"0.0.0.0:{ssh_ports[i]}"
        node["networkInterfaces"] = [{
            "name": "eth0",
            "address": addr,
            "gbps": 3600,
        }]
    cluster["groups"][0]["switchConfig"] = build_switch_config(cluster_ips)

    with open(cluster_path / "cluster.yaml", 'w') as f:
        yaml.safe_dump(cluster, f, sort_keys=False)
        logger.debug(f"updated cluster config:\n{pprint.pformat(cluster)}")

    # create an internal-pkg-properties.yaml file with a vip in the subnet of the cluster
    pkg_props = yaml.safe_load((CLI_BASE_DIR / "apps" / "common" / "internal-pkg-properties.yaml").read_text())
    pkg_props["properties"]["multiMgmtNodes"] = {}
    pkg_props["properties"]["multiMgmtNodes"]["dataVip"] = get_kind_vip()
    pkg_props["properties"]["kafka"] = {"pvcSize": "1Gi"}
    pkg_props["properties"]["kubewebhook"] = {"disableSystemNamespaceWebhook": "true"}
    pkg_props["properties"]["jobOperator"] = {"namespaceScaleDownCheckInterval": "0"}
    override_props_file = (CLI_BASE_DIR / "clusters" / cluster_name / "common" / "internal-pkg-properties.yaml")
    override_props_file.parent.mkdir(parents=True, exist_ok=True)
    override_props_file.write_text(yaml.safe_dump(pkg_props))
    _ensure_kind_cluster_dependencies(cluster_name)
   
    # Taint the 2nd worker node to avoid deploying any components on it
    new_node_name = f"{cluster_name}-worker2"
    ctx = load_cli_ctx(None, cluster_name)
    rv, stdout, stderr = ctx.cluster_ctr.must_exec(f"kubectl taint nodes {new_node_name} key1=value1:NoSchedule")
    assert rv == 0, f"{stdout}\n{stderr}"

    server_image = get_cluster_server_image(cluster_name)
    server_image_tag = server_image.split(":")[-1]
    cbcore_image = f"cbcore:{server_image_tag}"

    # deploy all
    runner = CliRunner()
    result = runner.invoke(
        cli, shlex.split(
            f"--cluster {cluster_name} -v deploy --skip-k8s all "
            f"--config-only --enable-multi-coordinator --disable-user-auth --cbcore-image {cbcore_image}")
    )
    if result.exit_code != 0:
        logger.error(result.output)
    assert result.exit_code == 0, f"{result.output} {result.stderr}"
    
    yield cluster_name
    _sp_exec(f"kind delete cluster --name={cluster_name}")
    shutil.rmtree(str(cluster_path))
    shutil.rmtree(f"/tmp/{cluster_name}", ignore_errors=True)


def _sp_exec(cmd: str, **kwargs) -> str:
    try:
        so = sp.run(shlex.split(cmd), **kwargs, stdout=sp.PIPE, stderr=sp.STDOUT)
        return str(so.stdout, 'utf-8')
    except sp.CalledProcessError as e:
        logger.warning(f"failed to call {cmd} code={e.returncode}, "
                       f"out={e.stdout}, err={e.stderr}")
        raise


@pytest.fixture(scope="session")
def ssh_ctx(kind_cluster: str) -> typing.Optional[CliCtx]:
    """
    Starts an ssh server on kind nodes using some public key it finds as ~/.ssh/*.pub.
    Returns:
        CliCtx if ssh successfully installs
    """
    ctx = CliCtx(pathlib.Path(__file__).parent.parent.parent.parent.parent, kind_cluster)
    ctx._cluster_ctr = SSHClusterController(
        {n.name: n.hostname for n in ctx.cluster_cfg.nodes},
        deploy_node=ctx.cluster_cfg.nodes[0].name,
        username="root",
        key_filename=str(_find_local_ssh_pubkey())).open()
    return ctx


@pytest.fixture(scope="session")
def ctx(kind_cluster: str) -> typing.Optional[CliCtx]:
    return load_cli_ctx(None, kind_cluster)


@pytest.fixture(scope="session")
def k8s(kind_cluster: str) -> typing.Iterator[kubernetes.client.ApiClient]:
    stdout = sp.check_output(f"kind get kubeconfig --name={kind_cluster}".split())
    cfg_dict = yaml.safe_load(stdout)
    cfg = kubernetes.config.load_kube_config_from_dict(cfg_dict)
    with kubernetes.client.ApiClient(configuration=cfg) as client:
        yield client


@pytest.fixture
def cs_cluster(kind_cluster: str) -> typing.Callable[[str], Result]:
    runner = CliRunner()

    def invoke(cmd) -> Result:
        return runner.invoke(cli, ["--cluster", kind_cluster, "-v", *shlex.split(cmd)])

    return invoke


@contextlib.contextmanager
def use_usernode(ctx: CliCtx) -> typing.Tuple[str, int]:
    # for local interactive debugging, developers can set a non-empty KEEP_USERNODE env var value,
    # so that the usernode container does not get deleted as part of the cleanup.
    # for example: KEEP_USERNODE=* make e2e-test
    keep_usernode = os.environ.get("KEEP_USERNODE")

    # creates an kind container the connects to the kind cluster's network.
    # reuse kind image since we're guaranteed to have it. Installs ssh and adds
    # the key to the authorized keys of the mgmt node.
    kind_node_image = os.environ.get("KIND_NODE_IMAGE", KIND_NODE_IMAGE)
    configure_ssh_cmds = " && ".join([
        "echo \"PubkeyAcceptedKeyTypes +ssh-rsa\" >> /etc/ssh/sshd_config",
        "/etc/init.d/ssh start",
        f'echo "{_find_local_ssh_pubkey().read_text().strip()}" >> /root/.ssh/authorized_keys ',
        'ssh-keygen -t ed25519 -q -f "/root/.ssh/id_ed25519" -N "" -C "root@kind-usernode"',
        "sleep infinity",
    ])

    port = find_free_port()

    name = f"{ctx.cluster_name}-usernode-{int(time.time())}"
    ctx.must_exec("docker run --rm -d -t --network kind --entrypoint bash "
                  f"--name {name} -p {port}:22 {kind_node_image} "
                  f"-c '{configure_ssh_cmds}'")

    try:
        ctx.must_exec(
            f"docker exec {name} timeout 2m bash -c 'while ! [ -f /root/.ssh/id_ed25519.pub ] ; do sleep 1 ; done'")
        so = ctx.must_exec(f"docker exec {name} cat /root/.ssh/id_ed25519.pub")
    except sp.CalledProcessError:
        ctx.must_exec(f"docker kill {name}")
        raise RuntimeError("usernode failed to be ready in 2 minutes")

    ctx.must_exec(f"docker exec {ctx.cluster_name}-control-plane "
                  f"sh -c 'echo {so.strip()} >> /root/.ssh/authorized_keys'")
    try:
        yield [name, port]
    finally:
        if not keep_usernode:
            ctx.must_exec(f"docker rm -f {name}")


def get_cluster_server_image(kind_cluster: str) -> str:
    """Ensure the cluster server image is available in the local docker daemon.
    Returns the image name."""
    list_imgs_cmd = "docker images " \
                    "--filter 'reference=cluster-server' " \
                    "--format '{{.CreatedAt}},{{.Repository}}:{{.Tag}}'"
    image = sorted(str(sp.check_output(list_imgs_cmd, shell=True), "utf-8").strip().split("\n"))[-1]
    if image:
        return image.split(",")[1]
    # src/cli/tests/integration
    cluster_mgmt_src_dir = pathlib.Path(__file__).parent.parent.parent.parent
    ctx = CliCtx(cluster_mgmt_src_dir, kind_cluster)
    build_img_cmd = f"make -C {cluster_mgmt_src_dir.absolute()} cluster-docker-build platform={ctx.build_platform()}"
    sp.check_call(build_img_cmd, shell=True)

    image = str(sp.check_output(list_imgs_cmd, shell=True), "utf-8").strip().split("\n", 1)[0]
    if not image:
        raise RuntimeError("failed to build cluster-server image")
    return image.split(",")[1]


@pytest.fixture(scope="session")
def use_custom_cluster_cm(ctx: CliCtx, tmp_path_factory):
    system_ns = "job-operator"

    @contextlib.contextmanager
    def _use_custom_cluster_cm() -> typing.Tuple[Cluster, typing.Callable[[Cluster], None]]:
        """
        Context manager that returns the current cluster config and a function to replace the Cluster config.
        Reverts the cluster config on context closed.
        """
        # add a switch config for test purposes, then ensure we can't deploy volumes to virtual range
        _, cmdata, _ = ctx.cluster_ctr.must_exec(
            f"kubectl get cm -n{system_ns} cluster -ojsonpath={{'.data.clusterConfiguration\\.yaml'}}"
        )

        def write_cmdata(cluster: Cluster):
            cluster_path = tmp_path_factory.mktemp("cmdata") / "cluster.yaml"
            cluster_path.write_text(cluster.to_yaml())
            ctx.cluster_ctr.exec_cmds(
                [FileCopyCmd(cluster_path, "/opt/cerebras/cluster/cluster.yaml")]
            )
            ctx.cluster_ctr.must_exec(
                f"sh -c 'kubectl create cm -n{system_ns} cluster --from-file=clusterConfiguration.yaml=/opt/cerebras/cluster/cluster.yaml --dry-run=client -oyaml | kubectl -n{system_ns} apply -f-'"
            )

        try:
            yield (Cluster(**yaml.safe_load(cmdata)), write_cmdata,)
        finally:
            write_cmdata(Cluster(**yaml.safe_load(cmdata)))

    return _use_custom_cluster_cm


@pytest.fixture(scope="session")
def use_cilium_ip_range(ctx: CliCtx):
    @contextlib.contextmanager
    def _use_cilium_ip_range(ip_range: str):
        """
        Context manager that creates a fake cilium-config map to test logic relating to cilium IP range validation
        """
        # add a fake cilium config map, then ensure we can't deploy volumes to the cilium range
        ctx.cluster_ctr.exec("yq eval -i 'del .properties.cilium' /opt/cerebras/cluster/pkg-properties.yaml")
        ctx.cluster_ctr.must_exec(
            "sh -c 'kubectl create cm -nkube-system cilium-config "
            f"--from-literal=cluster-pool-ipv4-cidr={ip_range} "
            "--from-literal=cluster-pool-ipv4-mask-size=24 "
            "--dry-run=client -oyaml | kubectl -nkube-system apply -f-'"
        )

        try:
            yield
        finally:
            ctx.cluster_ctr.must_exec("kubectl delete cm -nkube-system cilium-config")
            ctx.cluster_ctr.exec("yq eval -i 'del .properties.cilium' /opt/cerebras/cluster/pkg-properties.yaml")

    return _use_cilium_ip_range


@contextlib.contextmanager
def get_cluster_controlplane_ctr(kind_cluster: str) -> DockerClusterController:
    ctr = DockerClusterController([f"{kind_cluster}-control-plane"])
    try:
        ctr.open()
        yield ctr
    finally:
        ctr.close()


@contextlib.contextmanager
def get_cluster_worker_ctr(kind_cluster: str) -> DockerClusterController:
    ctr = DockerClusterController([f"{kind_cluster}-worker"])
    try:
        ctr.open()
        yield ctr
    finally:
        ctr.close()


def _assert_cbcore_image_set(k8s: kubernetes.client.ApiClient, image, ns="job-operator"):
    # check --cbcore-image set in the debug output which includes the rendered chart
    appsv1 = kubernetes.client.AppsV1Api(k8s)
    deploy = appsv1.read_namespaced_deployment("cluster-server", ns)
    envs = deploy.spec.template.spec.containers[0].env
    matched_at_least_once = False
    for envvar in envs:
        if envvar.name == "WSJOB_DEFAULT_IMAGE":
            assert image == envvar.value
            matched_at_least_once = True
    assert matched_at_least_once


def assert_usernode_configured(dockername, kind_cluster, accessible_namespaces):
    with DockerClusterController([dockername]) as ctr:
        _, so, _ = ctr.must_exec("cat /etc/hosts")
        assert "cluster-server.cerebrassc.local" in so
        assert f"cluster-server.{kind_cluster}.cerebrassc.local" in so
        assert so.count("cluster-server.") == 2

        _, so, _ = ctr.must_exec("cat /etc/security/limits.conf")
        assert "* hard nofile 20000" in so

        _, so, _ = ctr.must_exec(f"cat /opt/cerebras/config_v2")
        try:
            cfg_doc = json.loads(so)
        except Exception as e:
            assert False, f"failed to parse config as json: {so}: {e}"

        try:
            server, vip = cfg_doc["clusters"][0]["server"], f"{get_kind_vip()}:443"
            assert server == vip, f"server should be set to vip {vip}, but got {server}"
            expected_authority = f"cluster-server.{kind_cluster}.cerebrassc.local"
            authority = cfg_doc["clusters"][0]["authority"]
            assert authority == expected_authority, f"authority should be set to {expected_authority}, but got {authority}"

            assert len(cfg_doc["clusters"][0]["namespaces"]) == len(accessible_namespaces), \
                f'accessible namespace count should be {len(accessible_namespaces)}, but got {len(cfg_doc["clusters"][0]["namespaces"])}'
            for ns_cert_auth in cfg_doc["clusters"][0]["namespaces"]:
                assert ns_cert_auth["name"] in accessible_namespaces, \
                    f'config contains a certificate authority for {ns_cert_auth["name"]}, which should not be accessible'

            # ensure reachable - important for e2e wsjob tests which depend on reachability
            ctr.must_exec(f"ping -c 1 -w 1 {vip[:-4]}")
        except Exception as e:
            assert False, f"failed to assert: {e}"

# Despite this being dependent on logging style that may vary by function, it has been moved to conftest due to being used in multiple test files.
@pytest.fixture
def run_with_caplog(caplog: pytest.LogCaptureFixture):
    """
    Fixture to run CLI commands with caplog to capture logs.

    Limitation: Assumes specific logging style
    """

    def _run_with_caplog(cmd, *, level=logging.INFO):
        """
        Run with caplog to capture CLI stderr logs.
        """
        caplog.clear()
        caplog.set_level(level)

        runner = CliRunner()
        result = runner.invoke(cli, shlex.split(cmd))

        messages = [rec.getMessage() for rec in caplog.records]
        return result.exit_code, result.stdout, '\n'.join(messages)
    
    return _run_with_caplog


def _log_kind_disk_usage() -> None:
    # The kind cluster mounts /spare for its containerd storage. Log the space for debugging.
    try:
        fs_usage = str(
            sp.check_output(
                "df -h / /spare /var/lib/docker $GITTOP/build", shell=True
            ),
            'utf-8',
        )
        logging.info(f"filesystem usage: {fs_usage}")
    except sp.CalledProcessError as e:
        logging.warning(f"failed to get filesystem usage: {e}")


def _log_env(ctx: CliCtx) -> None:
    def assert_config_v2_readiness():
        cmd = "ls -al /opt/cerebras/config_v2"
        rv, _, _ = ctx.cluster_ctr.exec(cmd)
        return rv

    attempts = 100
    for i in range(attempts):
        host_cmds = [
            "docker ps -a",
            f"kind get clusters | xargs -I {{}} kind get kubeconfig --name={{}}",
        ]
        joined_cmds = "; ".join(host_cmds)
        output = str(sp.check_output(joined_cmds, shell=True), 'utf-8')
        logging.info(f"{joined_cmds}: {output}")

        cmds = [
            "hostname",
            "ls -al /opt/cerebras",
            "kubectl -n installers get all -o wide",
            "csctl get job",
        ]
        joined_cmds = "; ".join(cmds)
        _, so, _ = ctx.cluster_ctr.exec(f"sh -c '{joined_cmds}'")
        logging.info(f"{joined_cmds}:\n{so}")

        rv = assert_config_v2_readiness()
        if rv == 0:
            logging.info("config_v2 is ready")
            return

        logging.warning("config_v2 is not ready")
        time.sleep(1)

    logging.warning(f"config_v2 is not ready after {attempts} attempts")
