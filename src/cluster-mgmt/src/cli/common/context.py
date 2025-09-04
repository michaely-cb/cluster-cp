import datetime
import logging
import os
import pathlib
import re
import shlex
import subprocess as sp
import time
import typing
from asyncio import subprocess

import kubernetes.config
import yaml

from common.cluster import ClusterController
from common.cluster import DockerClusterController
from common.cluster import MkdirCmd
from common.cluster import SSHClusterController
from common.devinfra import DevInfraDB
from common.models import Cluster
from common.models import Node
from common.models import System

# Where the `cluster` configmap is mounted (if running in k8s.)
_CLUSTER_CONFIG_MOUNTPOINT = pathlib.Path(
    "/etc/cluster/clusterConfiguration.yaml",
)

# Where the `cluster-auth` secret is mounted (if running in k8s.)
_SSH_CREDENTIAL_MOUNTPOINT = pathlib.Path("/etc/ssh_conf/.ssh.conf")

# Environment variable to check to see if this is running in k8s.
_K8S_ENV_VAR = "KUBERNETES"

# Path on NFS where configs are stored. Previously checked in locally but
# this became too cumbersome to deal with merges for config changes.
REMOTE_CLUSTERS_PATH = pathlib.Path("/cb/system_data/clusters")

logger = logging.getLogger("cs_cluster.context")


def _replace_kubeconf_servername(kubeconf: str, control_plane_host: str) -> dict:
    """replace server with extern hostname of controlplane, otherwise kube client commands won't work on local"""
    try:
        k8s_yaml = yaml.safe_load(kubeconf)
        server = k8s_yaml['clusters'][0]['cluster']['server']
        m = re.match(r"https://([^:]+):.+", server)
        if m:
            replacement_server = server.replace(m.group(1), control_plane_host)
            k8s_yaml['clusters'][0]['cluster']['server'] = replacement_server
            logger.debug(f"replaced clusters[0].cluster.server {server} => {replacement_server}")
            return k8s_yaml
        else:
            logger.warning("didn't find clusters[0].cluster.server in k8s_yaml")
            return k8s_yaml
    except Exception as e:
        logger.error(f"failed to parse .kube yaml: {e}")
        raise


class CliCtx:
    def __init__(self, src_dir: typing.Optional[pathlib.Path], cluster_name: str):
        if not src_dir:
            # Use this script path to figure out src dir (cluster mgmt source = GITTOP/src/cluster_mgmt/)
            src_dir = pathlib.Path(os.path.realpath(os.path.dirname(__file__))).parent.parent.parent.absolute()
        else:
            # Use the absolute path for src_dir. Some commands rely on this path to correctly find the binary,
            # for example, `{src_dir}/src/job-operator/bin/kustomize`.
            src_dir = src_dir.absolute()

        self.cluster_name = cluster_name
        self.apps_dir = src_dir / "src" / "cli" / "apps"
        self._cluster_dir = src_dir / "src" / "cli" / "clusters" / cluster_name
        self._src_dir: pathlib.Path = src_dir

        if self.in_kind_cluster:
            self._remote_build_dir = "/home"
        else:
            # This is the base remote build directory for deployment packages.
            # Cluster-mgmt has a rotation polocy for this directory.
            date = datetime.datetime.today().strftime('%Y-%m-%d')
            user = os.environ.get('USER').lower().replace('_', '-')
            self._remote_build_dir = f"/tmp/deployment-packages/{date}-{user}"

        self._k8s_client = None
        self._cluster_ctr = None
        self._created_build_dir = False
        self._cluster_cfg: typing.Optional[Cluster] = None  # cluster from clusters/CLUSTER/cluster.yaml or devinfraDB
        self._cluster_k8s: typing.Optional[Cluster] = None  # cluster from k8s, possibly out of sync with cluster_cfg
        self._in_kind_cluster = None
        self._cluster_path = self._cluster_dir.joinpath("cluster.yaml")
        self._ssh_path = [
            self._cluster_dir / ".ssh.conf",
            REMOTE_CLUSTERS_PATH / cluster_name / ".ssh.conf",
            REMOTE_CLUSTERS_PATH / ".ssh.conf",
        ]
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
            self._init_cluster_cfg()
            self._init_cluster_ctr()
            self._initialized_cluster_connection = True

    def _init_cluster_cfg(self):
        """
        Load cluster config from local file or DB.
        Local files are used for kind clusters and take precedence over DB.
        DB is used for remote (colovore) clusters.
        """
        local_cfg_file = self._cluster_dir / "cluster.yaml"
        if local_cfg_file.is_file():
            self._cluster_cfg = Cluster(**yaml.safe_load(local_cfg_file.read_text()))
        else:
            self._cluster_cfg = try_load_cluster_from_db(self.cluster_name)

        if not self._cluster_cfg:
            raise ValueError(f"cluster config for '{self.cluster_name}' not found in devinfraDB or local file")

    def _init_cluster_ctr(self):
        """ Create the cluster controller. """
        nodes = self._nodes_ordered(self._cluster_cfg.nodes)

        if self.in_kind_cluster:
            self._cluster_ctr = DockerClusterController([node.name for node in nodes]).open()
        else:
            creds = self.resolve_ssh_creds()
            deploy_node = None
            if nodes and nodes[0].properties and "deployment_node" in nodes[0].properties:
                deploy_node = nodes[0].name
            self._cluster_ctr = SSHClusterController(
                {node.name: node.hostname for node in nodes},
                creds['username'],
                password=creds.get('password'),
                pem=creds.get('pem'),
                key_filename=creds.get('key_filename'),
                deploy_node=deploy_node,
                mgmt_nodes=[node.name for node in nodes if node.role in ["management", "any"]]
            )
            logger.info(f"{self._cluster_cfg.name}: deploy node: {deploy_node}")

        def on_close():  # register close callback so that we forget about a closed controller
            self._cluster_ctr = None

        self._cluster_ctr.close_callback(on_close)

    @property
    def deploy_preflight_checks(self) -> bool:
        """ True if the deploy command should run preflight checks. """
        return self._deploy_preflight_checks

    @deploy_preflight_checks.setter
    def deploy_preflight_checks(self, value: bool):
        self._deploy_preflight_checks = value

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
                f"unable to call git. This may cause issues if you are deploying with local repo changes: {e}")
        return ""

    def resolve_file(self, path) -> pathlib.Path:
        """
        Get file from cluster-specific path, otherwise fallback to app-default or
        app-common. Raises exception if in neither.
        """
        override_path = self._cluster_dir.joinpath(path)
        if override_path.is_file():
            return override_path
        apps_path = self.apps_dir.joinpath(path)
        if apps_path.is_file():
            return apps_path
        apps_common_path = self.apps_dir.joinpath("common").joinpath(pathlib.Path(path).name)
        if apps_common_path.is_file():
            return apps_common_path
        raise Exception(f"file '{path}' did not exist in {override_path.absolute()} or {apps_path.absolute()} "
                        f"or {apps_common_path.absolute()}")

    def resolve_dir(self, path):
        """Get dir from cluster-specific path, otherwise fallback to app-default. Raises exception if in neither."""
        override_path = self._cluster_dir.joinpath(path)
        if override_path.is_dir():
            return override_path
        apps_path = self.apps_dir.joinpath(path)
        if apps_path.is_dir():
            return apps_path
        raise Exception(f"dir '{path}' did not exist in {override_path.absolute()} or {apps_path.absolute()}")

    @property
    def build_dir(self) -> pathlib.Path:
        path = self.src_dir / "src" / "cli" / "build"
        if not path.exists():
            path.mkdir(exist_ok=True)
        return path

    @property
    def pkg_dir(self) -> pathlib.Path:
        """ Directory to store the package. """
        path = self.build_dir.joinpath("pkg")
        if not path.exists():
            path.mkdir()
        return path

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

        self._created_build_dir = False
        if not self.in_kind_cluster:
            cleanup_config = "/n1/cluster-mgmt/cleanup/configs/cleanup-configs.json"
            _, so, _ = self.cluster_ctr.exec(f"jq '.volumes | map(select(.path == \"/tmp/deployment-packages\")) | length' {cleanup_config}")
            can_rotate_deploy_pkg = so.strip() == "1"

            if can_rotate_deploy_pkg:
                self._remote_build_dir += remote_dir
            else:
                self._remote_build_dir = remote_dir

    @property
    def remote_pkg_dir(self):
        return self._remote_build_dir + "/pkg"

    def write_file(self, name: str, contents: str, create_cluster_dir=False) -> str:
        """write a file under the current context's dir. Creates parent dir if required."""
        if not self._cluster_dir.is_dir() and not create_cluster_dir:
            raise Exception(f"the current context directory '{str(self._cluster_dir)}' does not exist."
                            f" Will not write output file {name}. Try using 'cluster use' to resolve.")
        file_path = self._cluster_dir.joinpath(name)
        if str(self._cluster_dir.absolute()).startswith(str(REMOTE_CLUSTERS_PATH.absolute())):
            # remote files require shelling out to OS to set permission bits correctly
            if not file_path.parent.is_dir():
                new_dir_root = file_path.parent
                while not new_dir_root.parent.is_dir():
                    new_dir_root = new_dir_root.parent
                file_path.parent.mkdir(parents=True, exist_ok=True)
                self.must_exec(f"find {new_dir_root} -type d -exec chmod 775 {{}} \;")
            is_new_file = not file_path.is_file()
            file_path.write_text(contents)
            if is_new_file:
                self.must_exec(f"chmod 664 {file_path}")
        else:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(contents)
        return str(file_path)

    def write_build_file(self, name: str, contents: str) -> str:
        """Write a file locally under build dir."""
        file_path = self.build_dir.joinpath(name)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(contents)
        return str(file_path)

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
    def cluster_cfg(self) -> Cluster:
        """ Cluster config from local cluster.yaml or derived from devinfra db. """
        self._init_cluster_connection()
        return self._cluster_cfg

    @property
    def cluster_cfg_k8s(self) -> Cluster:
        """ Cluster config pulled from k8s. Should only use this if you need nodegroup info. """
        self._init_cluster_connection()
        if self._cluster_k8s is not None:
            return self._cluster_k8s

        # TODO: system_namespace should be a property of the cluster
        system_namespace = "job-operator"
        try:
            _, cluster_cm, _ = self._cluster_ctr.must_exec(
                f"kubectl get cm -n {system_namespace} cluster "
                "-ojsonpath='{.data.clusterConfiguration\\.yaml}'"
            )
            self._cluster_k8s = Cluster(**yaml.safe_load(cluster_cm))
        except Exception as e:
            logger.error("failed to get clusterConfiguration from configmap. You must re-create the cluster config.")
            logger.error(e)
            raise ValueError(
                "cluster was not initialized. Did you forget to call 'cs_cluster.py cluster create-config'?"
            )
        return self._cluster_k8s

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

    def resolve_ssh_creds(self) -> dict:
        """Resolve ssh creds from file in context lookup paths, selecting the first matching path."""
        ssh_confs = [f for f in self._ssh_path if f.is_file()]
        if not ssh_confs:
            raise Exception(
                f"Failed to resolve .ssh.conf in any of {self._ssh_path}. Try running 'cluster signin' "
                "to resolve, if running locally. In Kubernetes, verify the SSH auth secret exists and is "
                " mounted at /etc/ssh_conf/."
            )
        with open(ssh_confs[0], 'r') as f:
            creds = yaml.safe_load(f)
        # only support single credential for all hosts at the moment
        if not creds.get('credentials', []):
            raise Exception(
                f"File '{ssh_confs[0]}' does not contain credentials required to ssh into nodes. "
                "Try running 'cluster signin' to resolve."
            )

        return creds['credentials'][0]

    @staticmethod
    def _nodes_ordered(nodes) -> typing.List[Node]:
        """Nodes ordered with the deploy node first. """
        def sort_tuple(n):
            return not n.properties or "deployment_node" not in n.properties, n.role != "management", n.name,
        return sorted(nodes.copy(), key=sort_tuple)


def try_load_cluster_from_db(cluster_name: str, json=None) -> typing.Optional[Cluster]:
    """ Load a partial view of the cluster configuration from the devinfra DB. """
    if not json:
        with DevInfraDB() as db:
            c = db.get_cluster(cluster_name)
            if not c:
                return None
    else:
        c = json
    cluster = Cluster(name=c['name'], nodes=[], systems=[], properties={"source": "devinfra"})

    colo_name = ".cerebrassc.local"
    def clean_name(name):
        return name[:-len(colo_name)] if name.endswith(colo_name) else name

    deployment_node = c.get("deployment_node", "")
    for k, v in c.items():
        if not k.endswith("_hosts"):
            continue
        role = {
            "memoryx": "memory",
            "swarmx": "broadcastreduce",
            "worker": "worker",
            "management": "management",
            "any": "any",
        }.get(k[:-6])
        if not role:
            continue
        cluster.nodes.extend([
            Node(name=clean_name(n), role=role, properties={"deployment_node": "true"} if deployment_node == n else {})
            for n in v
        ])
    for s in c['systems']:
        cluster.systems.append(System(name=s, type="cs2", controlAddress=""))
    return cluster


def load_cli_ctx(src_dir: typing.Optional[pathlib.Path] = None, cluster_name: typing.Optional[str] = None) -> CliCtx:
    """
    :param src_dir: Optional path of the src dir of cluster mgmt repo i.e. git top of cluster-mgmt repo.
    :param cluster_name: Optional name of cluster to load. Defaults to cached cluster if exists.
    :return: CLI context. May be invalid but that's intentional - Ctx lazy loads files and reports errors
    to the user if required files are not present.
    """
    if not cluster_name:
        config_cache: typing.Optional[pathlib.Path] = None
        for location in [pathlib.Path(".cs_cluster.conf"), pathlib.Path("~/.cs_cluster.conf").expanduser()]:
            if location.is_file():
                config_cache = location
                break

        if not config_cache:
            logger.debug("a default cluster was not specified in .cs_cluster.conf, please explicitly specify a "
                         "cluster or use the 'cluster use' command to set a default cluster.")
            return CliCtx(src_dir, "unspecified")

        cfg = yaml.safe_load(config_cache.read_text())
        cluster_name = cfg.get('cluster', 'NOT_SET_IN_CACHE_FILE')

    return CliCtx(src_dir, cluster_name)
