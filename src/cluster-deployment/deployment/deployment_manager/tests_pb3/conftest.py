import contextlib
import ipaddress
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess as sp
import time
import typing

import pytest
import yaml

from deployment_manager.tests_pb3.cluster import Cmd, DirCopyCmd, DockerClusterController, FileCopyCmd, MkdirCmd
from deployment_manager.tests_pb3.context import load_cli_ctx

logger = logging.getLogger("tests.installation.conftest")

CURRENT_DIR = pathlib.Path(__file__).parent
CB_DOCKER_CFG = pathlib.Path('/cb/ecr-token/config.json')
CB_DOCKER_CFG_OLD = pathlib.Path('/cb/data/shared-docker-token/config.json')
ECR_URL = "171496337684.dkr.ecr.us-west-2.amazonaws.com"
KIND_NODE_IMAGE = f"{ECR_URL}/cerebras-kind-node:v1.29.0-5"


def pytest_addoption(parser):
    parser.addoption("--use-cluster", action="store", default="NOT_SET")
    parser.addoption("--keep-cluster", action="store", default=False)
    parser.addoption("--new-cluster-name", action="store", default="")


@pytest.fixture(scope="session")
def kinder_cluster(pytestconfig) -> typing.Iterator[typing.Tuple[str, str]]:
    """Fixture adds the name of a pytest. The logic for creating new clusters is this way so that multiple tests
    can be run in parallel on Jenkins."""
    # --use-cluster="" to use the existing cluster on this machine or make one and keep it
    # --use-cluster="xxx" to use cluster xxx
    # not setting --use-cluster will create a cluster that's cleaned up at the end of the test

    use_cluster = pytestconfig.getoption("use_cluster")
    keep_cluster = parsebool(pytestconfig.getoption("keep_cluster"))
    keep_cluster = keep_cluster or use_cluster == "" or use_cluster != "NOT_SET"
    new_cluster_name = pytestconfig.getoption("new_cluster_name")

    cluster_out = _sp_exec(f"{CURRENT_DIR}/../../bin/kinder get clusters")
    clusters = re.split(r'\s+', cluster_out) if "No kind clusters" not in cluster_out else []
    if use_cluster == "" and clusters:
        use_cluster = clusters[0]
    need_new_cluster = use_cluster == 'NOT_SET' or use_cluster == ""

    if need_new_cluster:
        cluster_name = f"pytest-{int(time.time())}" if not new_cluster_name else new_cluster_name
        cluster_path = CURRENT_DIR / "clusters" / cluster_name
        shutil.copytree(f"{CURRENT_DIR}/clusters/kind", str(cluster_path), ignore=lambda src, names: names if src == "build" else [])
        _sp_exec(
            f"{CURRENT_DIR}/../../bin/kinder create cluster --name={cluster_name} "
            f"--control-plane-nodes 3 --worker-nodes 1 --image={KIND_NODE_IMAGE}"
        )

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
        cluster["groups"][0]["switchConfig"] = build_switch_config(cluster_ips)
        with open(cluster_path / "cluster.yaml", 'w') as f:
            updated_cluster = yaml.safe_dump(cluster, sort_keys=False)
            f.write(updated_cluster)
            logger.info(f"updated cluster config:\n{updated_cluster}")

        network_config = build_net_json(cluster)
        with open(cluster_path / "network_config.json", 'w') as f:
            f.write(network_config)
            logger.info(f"writing network config:\n{network_config}")

        _ensure_kinder_cluster_dependencies(cluster["nodes"])
        ctx = load_cli_ctx(None, cluster_name)
        pb3_pkg = os.getenv("PB3_PKG")
        git_top = os.getenv("GITTOP")
        cli_top = pathlib.Path(git_top) / "src" / "cluster_mgmt" / "src" / "cli"
        ctx.cluster_ctr.exec_cmds(
            [
                MkdirCmd("/opt/cerebras/cluster"),
                MkdirCmd("/home/pkg"),
                FileCopyCmd(cluster_path / "cluster.yaml", "/opt/cerebras/cluster/cluster.yaml"),
                FileCopyCmd(cluster_path / "network_config.json", "/opt/cerebras/cluster/network_config.json"),
                DirCopyCmd(f"{cli_top}/{pb3_pkg}", "/home/pkg")
            ]
        )
        ctx.cluster_ctr.must_exec(
            f"bash -c 'cd /home/pkg; "
            f"K8S_INSTALL_VERSION=1.30.4 ./csadm.sh install manifest.json --yes'"
        )
        rv, so, _ = ctx.cluster_ctr.exec("bash -c 'curl -k https://127.0.0.1:8443/readyz'")
        assert rv == 0, f"failed to check cp node readiness, {so}"
        yield cluster_name, cluster_ips[0]

        if not keep_cluster:
            _sp_exec(f"{CURRENT_DIR}/../../bin/kinder delete cluster --name={cluster_name}")
            shutil.rmtree(str(cluster_path))
            shutil.rmtree(f"/tmp/{cluster_name}", ignore_errors=True)
    else:
        control_plane_node = f"{use_cluster}-control-plane-1"
        addrs = json.loads(_sp_exec(f"docker exec {control_plane_node} ip -j addr show eth0"))
        ip_addr = addrs[0]["addr_info"][0]["local"]
        yield use_cluster, ip_addr


@contextlib.contextmanager
def get_cluster_controlplane_ctr(kind_cluster: str) -> DockerClusterController:
    ctr = DockerClusterController([f"{kind_cluster}-control-plane"])
    try:
        ctr.open()
        yield ctr
    finally:
        ctr.close()


def build_net_json(cluster_yaml: dict) -> str:
    """
    Create network_config.json from cluster YAML and switch config.

    cluster_yaml: dict loaded from cluster.yaml, containing nodes info
    switch_config: dict containing switch details like subnet, virtualStart, virtualEnd, etc.
    """
    switch_config = cluster_yaml["groups"][0]["switchConfig"]
    cluster_name = cluster_yaml.get("name", "unknown-cluster")
    prefix = switch_config["subnet"].split("/")[1]

    # Helper to build interface blocks for a node
    def build_interfaces(interfaces):
        intf_list = []
        for intf in interfaces:
            intf_list.append(
                {
                    "name": intf["name"],
                    "address": f"{intf['address']}/{prefix}" if "/" not in intf["address"] else intf["address"],
                    "gbps": intf.get("gbps", 10),
                    "switch_name": switch_config.get("switchName", "switch0"),
                    "switch_port": intf.get("switch_port", "Eth1/1")  # default port, override if you have mapping
                }
            )
        return intf_list

    management_nodes = []
    worker_nodes = []

    # Assuming cluster_yaml['nodes'] exists and is a list of nodes with required fields
    for node in cluster_yaml.get("nodes", []):
        node_json = {
            "name": node["name"],
            "interfaces": build_interfaces(node.get("networkInterfaces", []))
        }
        if node.get("role") == "management":
            management_nodes.append(node_json)
        elif node.get("role") == "worker":
            worker_nodes.append(node_json)
        else:
            worker_nodes.append(node_json)

    switches = [
        {
            "address": switch_config["virtualStart"] + "/" + prefix,
            "name": switch_config.get("switchName", "switch0"),
            "prefix": switch_config["subnet"],
            "virtual_addrs": {
                "starting_address": switch_config["virtualStart"] + "/" + prefix,
                "ending_address": switch_config["virtualEnd"] + "/" + prefix
            },
            "vendor": "dell",
            "tier": "AW",
            "tier_pos": 0
        }
    ]

    netjson = {
        "name": cluster_name,
        "environment": {
            "cluster_prefix": switch_config.get("parentnet", "")
        },
        "management_nodes": management_nodes,
        "worker_nodes": worker_nodes,
        "switches": switches,
        # Add clusters array for blue-green migration support
        "clusters": [
            {
                "name": cluster_name,
                "data_network": {
                    "vip": "10.250.93.128/32",
                    "virtual_addr_index": 0
                },
                "mgmt_network": {
                    "node_asn": 4286054402,
                    "router_asn": 4286054401,
                    "vip": "172.31.255.1"
                }
            },
            {
                "name": "green-cluster",
                "data_network": {
                    "vip": "10.250.93.133/32",
                    "virtual_addr_index": 1
                },
                "mgmt_network": {
                    "node_asn": 4286054402,
                    "router_asn": 4286054401,
                    "vip": "172.31.255.2"
                }
            }
        ]
    }

    return json.dumps(netjson, indent=2)


def build_switch_config(cluster_ips: typing.List[str]) -> dict:
    """
    Build switch config by inspecting docker kind network

    Note: leaves 8 IPs free before the start of virtual range. Docker default
    IPAM appears to assign IPs using the lowest free IP. 8 should be enough
    since normally only 1 extra container (usernode) used in addition to kind nodes.
    """
    max_additional_docker_containers = 8
    addrs = sorted([ipaddress.ip_address(ip) for ip in cluster_ips])
    network_doc = json.loads(_sp_exec("docker network inspect bridge"))
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


def _ensure_kinder_cluster_dependencies(nodes: dict):
    """
    Starts an ssh server on kind nodes using some public key it finds as ~/.ssh/*.pub.
    """
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
    ctr.exec_cmds_cluster(
        [
            Cmd(f"bash -c 'echo \"root:root\" | chpasswd'"),
            Cmd(f"bash -c 'echo \"{id_pub}\" >> /root/.ssh/authorized_keys'"),
            Cmd(
                f"bash -c 'test -f {CB_DOCKER_CFG} || "
                f"{{ mkdir -p $(dirname {CB_DOCKER_CFG}) && touch {CB_DOCKER_CFG} ; }}'"
            ),
        ]
    )

    # Allow 'ssh-rsa' as PubkeyAcceptedKeyTypes. Otherwise, paramiko 2.7.1 SSHClient will fail to connect.
    # option PubKeyAcceptedAlgorithms is supported from OpenSSH version 8.5 and above.
    ctr.exec_cmds_cluster(
        [
            Cmd("bash -c 'echo \"PubkeyAcceptedKeyTypes +ssh-rsa\" >> /etc/ssh/sshd_config'"),
            Cmd("bash -c 'systemctl restart sshd'")
        ]
    )
    ctr.close()


def _find_local_ssh_pubkey() -> pathlib.Path:
    ssh_dir = pathlib.Path.home() / ".ssh"
    if not ssh_dir.is_dir():
        logger.error(f"ssh directory {ssh_dir} does not exist on test host, cannot run ssh-based tests")
        raise Exception(
            "e2e testing requires ~/.ssh/ to be created - please setup the directory on pytest host machine"
        )

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


def _sp_exec(cmd: str, **kwargs) -> str:
    try:
        so = sp.run(cmd, **kwargs, stdout=sp.PIPE, stderr=sp.STDOUT, shell=True, check=True)
        return str(so.stdout, 'utf-8')
    except sp.CalledProcessError as e:
        logger.warning(
            f"failed to call {cmd} code={e.returncode}, "
            f"out={e.stdout}, err={e.stderr}"
        )
        raise


def parsebool(s: typing.Union[str, bool]) -> bool:
    return s if type(s) == bool else s.strip().lower() in {"t", "true", "1", "yes"}
