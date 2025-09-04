import logging
import os
import pathlib
import shlex
import subprocess as sp
import time
import typing
from asyncio import subprocess
from dataclasses import dataclass
from typing import Any, Dict, List

import yaml
from deployment_manager.tests_pb3.cluster import ClusterController, DockerClusterController, MkdirCmd

# Where the `cluster` configmap is mounted (if running in k8s.)
_CLUSTER_CONFIG_MOUNTPOINT = pathlib.Path(
    "/etc/cluster/clusterConfiguration.yaml",
)

# Environment variable to check to see if this is running in k8s.
_K8S_ENV_VAR = "KUBERNETES"

logger = logging.getLogger("context")

CURRENT_DIR = pathlib.Path(__file__).parent


class CliCtx:
    def __init__(self, src_dir: typing.Optional[pathlib.Path], cluster_name: str):
        if not src_dir:
            # Use this script path to figure out src dir (cluster mgmt source = GITTOP/src/cluster_mgmt/)
            src_dir = CURRENT_DIR.absolute()
        else:
            # Use the absolute path for src_dir. Some commands rely on this path to correctly find the binary,
            # for example, `{src_dir}/src/job-operator/bin/kustomize`.
            src_dir = src_dir.absolute()

        self.cluster_name = cluster_name
        self._cluster_dir = src_dir / "clusters" / cluster_name
        self._src_dir: pathlib.Path = src_dir

        if self.in_kind_cluster:
            self._remote_build_dir = "/home"

        self._k8s_client = None
        self._cluster_ctr = None
        self._created_build_dir = False
        self._created_build_dir_cluster = False
        self._cluster_path = self._cluster_dir.joinpath("cluster.yaml")
        self._initialized_cluster_connection = False
        self._deploy_preflight_checks = False
        self._skip_k8s = False

    def _init_cluster_connection(self):
        """
        Load cluster config from local file or DB and set up cluster controller.
        Loading is lazy so that we don't have to connect to DB when running commands
        that don't need it such as for the `package` commands.
        """
        if not self._initialized_cluster_connection:
            self._init_cluster_ctr()
            self._initialized_cluster_connection = True

    def _init_cluster_ctr(self):
        """ Create the cluster controller. """
        local_cfg_file = self._cluster_dir / "cluster.yaml"
        nodes = parse_and_sort_nodes_from_yaml(local_cfg_file.read_text())

        if self.in_kind_cluster:
            self._cluster_ctr = DockerClusterController([node.name for node in nodes]).open()

        def on_close():  # register close callback so that we forget about a closed controller
            self._cluster_ctr = None

        self._cluster_ctr.close_callback(on_close)

    @property
    def skip_k8s(self) -> bool:
        """ True if the deploy command should run preflight checks. """
        return self._skip_k8s

    @skip_k8s.setter
    def skip_k8s(self, value: bool):
        self._skip_k8s = value

    def build_platform(self) -> str:
        if self.in_kind_cluster:
            if self.must_exec("uname -m").strip() == "arm64":
                return "linux/arm64"
        return "linux/amd64"

    def build_arch(self) -> str:
        return self.build_platform().split("/")[-1]

    def is_macos_build(self) -> bool:
        return self.must_exec("uname -s").strip() == "Darwin"

    def githash_make_param(self) -> str:
        """Set GITHASH for docker image builds to a unique string if there are local modifications. This is needed to deploy
        an image with local modifications since GITHASH is the HEAD commit and therefore the deploy won't know about
        non-committed changed."""
        try:
            for f in self.must_exec("git status --porcelain", cwd=self.src_dir).split("\n"):
                f = f.strip()
                # Add, Delete, Modify. Note that files not added to the index will NOT be detected as changed.
                if f and f[0] in {'A', 'D', 'M'}:
                    existing_hash = self.must_exec("git rev-parse --short=10 HEAD", cwd=self.src_dir).strip()
                    override_hash = f"GITHASH={existing_hash}-{int(time.time())}"
                    logger.debug(f"local repo changes detected, using {override_hash}")
                    return override_hash
        except Exception as e:
            logger.warning(
                f"unable to call git. This may cause issues if you are deploying with local repo changes: {e}"
            )
        return ""

    @property
    def src_dir(self) -> pathlib.Path:
        """Dir containing cluster-mgmt source code."""
        return self._src_dir

    @src_dir.setter
    def src_dir(self, arg=None):
        """Search for source directory if not given, otherwise checks that given dir exists and sets."""
        if not arg:
            raise ValueError("--src-dir cannot be empty")
        if not pathlib.Path(arg).is_dir():
            raise ValueError("--src-dir is not a directory")
        self._src_dir = arg

    @property
    def remote_build_dir(self):
        if not self._created_build_dir:
            logger.info(f"first time mkdir {self._remote_build_dir} on deploy node")
            self.cluster_ctr.exec_cmds([MkdirCmd(self._remote_build_dir)])
            self._created_build_dir = True
        return self._remote_build_dir

    @remote_build_dir.setter
    def remote_build_dir(self, remote_dir: str):
        if self.in_kind_cluster and remote_dir.startswith("/tmp/"):
            raise ValueError(
                f"bad --remote-dir: '{remote_dir}' uses tmp directory in a docker-based deploy. "
                "/tmp can be randomly deleted, failing your deploy in strange ways. Use /home instead"
            )
        self._remote_build_dir = remote_dir

    @property
    def remote_pkg_dir(self):
        return self.remote_build_dir + "/pkg"

    @property
    def remote_build_dir_cluster(self):
        if not self._created_build_dir_cluster:
            logger.debug("first time remote_build_dir_cluster accessed, creating on all nodes")
            self.cluster_ctr.exec_cmds_cluster([MkdirCmd(self._remote_build_dir)])
            self._created_build_dir_cluster = True
        return self._remote_build_dir

    @remote_build_dir.setter
    def remote_build_dir(self, dir):
        self._created_build_dir = False
        self._created_build_dir_cluster = False
        self._remote_build_dir = dir

    def must_exec(self, cmd, cwd: typing.Union[str, pathlib.Path] = None, env=None, shell=False) -> str:
        """
        Throws if command was not successful
        Returns: stdout
        """
        try:
            logger.debug(f"exec '{cmd}'{' in cwd=' + str(cwd) if cwd else ''}")
            env = dict(os.environ) if env is None else env
            if not shell:
                cmd = shlex.split(cmd)
            return sp.check_output(cmd, cwd=cwd, stderr=subprocess.STDOUT, encoding='utf-8', env=env, shell=shell)
        except sp.CalledProcessError as err:
            logger.error(f"failed to exec '{cmd}', exited with {err.returncode}: {err.output}")
            raise

    def exec(self, cmd, cwd=None, shell=False) -> typing.Tuple[int, str, str]:
        """
        Returns
            returncode, stdout, stderr
        """
        try:
            logger.debug(f"exec '{cmd}'{' in cwd=' + cwd if cwd else ''}")
            if not shell:
                cmd = shlex.split(cmd)
            return 0, sp.check_output(cmd, stderr=subprocess.STDOUT, encoding='utf-8', cwd=cwd, shell=shell), ''
        except sp.CalledProcessError as err:
            return err.returncode, err.stdout, err.stderr

    @property
    def in_kind_cluster(self) -> bool:
        return (self.cluster_name == "kind" or self.cluster_name.startswith("pytest-") or
                self.cluster_name.startswith("dmtest-"))

    def close_cluster_ctr(self):
        """
        Closes cluster_ctr if it has been intialized
        """
        if self._cluster_ctr:
            self._cluster_ctr.close()

    @property
    def cluster_ctr(self) -> ClusterController:
        self._init_cluster_connection()
        return self._cluster_ctr


@dataclass
class Node:
    name: str
    role: str
    properties: Dict[str, Any]


def parse_and_sort_nodes_from_yaml(yaml_str: str) -> List[Node]:
    """
    Parses the YAML string and returns a sorted list of Node objects.
    Sort priority:
      1. deployment_node in properties (comes first)
      2. role == "management" (comes before others)
      3. name lexicographically
    """
    data = yaml.safe_load(yaml_str)
    nodes_raw = data.get("nodes", [])

    nodes = [
        Node(
            name=node["name"],
            role=node["role"],
            properties=node.get("properties", {})
        )
        for node in nodes_raw
    ]

    def sort_tuple(n: Node):
        return (
            not n.properties or "deployment_node" not in n.properties,
            n.role != "management",
            n.name,
        )

    return sorted(nodes, key=sort_tuple)


def load_cli_ctx(src_dir: typing.Optional[pathlib.Path] = None, cluster_name: typing.Optional[str] = None) -> CliCtx:
    """
    :param src_dir: Optional path of the src dir of cluster mgmt repo i.e. git top of cluster-mgmt repo.
    :param cluster_name: Optional name of cluster to load. Defaults to cached cluster if exists.
    :return: CLI context. May be invalid but that's intentional - Ctx lazy loads files and reports errors
    to the user if required files are not present.
    """
    return CliCtx(src_dir, cluster_name)
