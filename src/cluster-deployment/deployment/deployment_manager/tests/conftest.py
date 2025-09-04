import datetime
import functools
import io
import ipaddress
import logging
import os
import pathlib
import random
import shlex
import shutil
import subprocess as sp
import tarfile
import tempfile
import time
import typing

from threading import Thread
from typing import Dict, Tuple, Optional

import docker
import docker.api
import docker.constants
import docker.errors
import docker.models.containers
import docker.models.networks
import docker.types
import jinja2
import pytest

logging.basicConfig()

logger = logging.getLogger(__name__)

ASSETS_DIR = pathlib.Path(__file__).parent / "assets"

# Debugging helper: don't tear down the containers after test completes
KEEP_CLUSTER = True if os.getenv("KEEP_CLUSTER", False) else False
if "GITTOP" in os.environ:
    GITTOP = pathlib.Path(os.getenv("GITTOP"))
else:
    GITTOP = pathlib.Path(__file__).parent.parent.parent.parent.parent.parent  # tests / dm / d / cd / src / gittop

LOCAL_IMAGE = "rocky-dmgr:8.9-7"
REMOTE_IMAGE = f"171496337684.dkr.ecr.us-west-2.amazonaws.com/{LOCAL_IMAGE}"
DEPLOY_MGR_PATH = "src/cluster_deployment/"
CLITOP = GITTOP.joinpath("src/cluster_mgmt/src/cli")
TESTS_PROJECT_PATH = "deployment/deployment_manager/tests/"
TESTS_ASSETS_ROOT = GITTOP.joinpath(DEPLOY_MGR_PATH + "/" + TESTS_PROJECT_PATH + "assets")
TEST_LABEL = "dmtest"
SQLITE_CMD = "sqlite3 /opt/cerebras/cluster-deployment/dm.db"


def _random_ip_network() -> str:
    octet1 = str(random.randint(0, 31))
    octet2 = str(random.randint(0, 254))
    return f"172.{octet1}.{octet2}.0/24"


def create_network(
        client: docker.client.DockerClient,
        basename: str,
        subnet: Optional[str] = None
) -> docker.models.networks.Network:
    """ Create a network with a pre-specified subnet. Subnet is required since
    containers cannot be pre-assigned an IP without a docker subnet.
    """
    network_name = f"{basename}-network"
    _create = lambda sn: client.networks.create(
        network_name,
        driver="bridge",
        ipam=docker.types.IPAMConfig(pool_configs=[docker.types.IPAMPool(subnet=sn)]),
        labels={"testsuite": TEST_LABEL, },
    )
    if subnet:
        return _create(subnet)
    e = None
    for i in range(5):
        try:
            # retry in case of IP range conflicts
            return _create(_random_ip_network())
        except Exception as ex:
            e = ex
            continue
    raise e


def cleanup_networks(client: docker.client.DockerClient, days=7):
    """ clean up dmtest networks older than 7 days with no containers"""
    for network in client.networks.list():
        labels = network.attrs.get('Labels', {})
        if not network.containers and labels.get("testsuite") == TEST_LABEL and "Created" in network.attrs:
            creation_date = network.attrs['Created'].split("T")[0]
            creation_date = datetime.datetime.strptime(creation_date, "%Y-%m-%d")
            if datetime.datetime.utcnow() - datetime.timedelta(days=days) > creation_date:
                logger.info(f"cleanup stale network {network.id}")
                network.remove()


def create_container(
        client: docker.client.DockerClient,
        name: str,
        hostname: str,
        network: str,
        ip: str,
        mounts: dict,
        env: dict,
        cmd: str,
        image: str,
        extra_hosts: dict = None,
) -> docker.models.containers.Container:
    try:
        client.images.get(image)
    except docker.errors.ImageNotFound:
        client.images.pull(image)

    binds = {host_path: {"bind": container_path, "mode": "ro"} for host_path, container_path in mounts.items()}
    host_config = client.api.create_host_config(binds=binds, extra_hosts=extra_hosts)

    networking_config = client.api.create_networking_config({
        network: client.api.create_endpoint_config(
            ipv4_address=ip,
            aliases=[hostname]
        )
    }) if network else None

    container = client.api.create_container(
        image,
        command=cmd,
        name=name,
        hostname=hostname,
        host_config=host_config,
        networking_config=networking_config,
        environment=env,
    )
    return client.containers.get(container["Id"])


def exec_container(
        container: docker.models.containers.Container, cmd: str, user="root", **kwargs
) -> Tuple[int, str, str]:
    try:
        rv, out = container.exec_run(cmd, user=user, demux=True, **kwargs)
        stdout = out[0].decode() if out[0] else ""
        stderr = out[1].decode() if out[1] else ""
        return rv, stdout, stderr
    except docker.errors.ContainerError as e:
        logger.error(f"Error executing command '{cmd}' in container {container.name}: {e}")
        raise


def must_exec_container(
        container: docker.models.containers.Container, cmd: str, user="root", **kwargs
) -> Tuple[str, str]:
    rv, stdout, stderr = exec_container(container, cmd, user=user, **kwargs)
    if rv != 0:
        logger.error(f"Command '{cmd}' exited with {rv}:\n{stderr}")
        raise Exception(f"Command '{cmd}' exited with {rv}. Out: {stderr}")
    return stdout, stderr


def exec_container_by_name(
        container_name: str, cmd: str, user="root", **kwargs
) -> Tuple[int, str, str]:
    """
    Execute command in container specified by name, return (exit_code, stdout, stderr).

    Args:
        container_name: Name or ID of the container
        cmd: Command to execute
        user: User to execute command as (default: "root")
        **kwargs: Additional keyword arguments to pass to container.exec_run
            privileged: Whether to run with privileged permissions

    Returns:
        Tuple of (exit_code, stdout, stderr)

    Raises:
        docker.errors.NotFound: If container doesn't exist
    """
    client = docker.from_env()
    container = client.containers.get(container_name)
    return exec_container(container, cmd, user=user, **kwargs)


def must_exec_container_by_name(
        container_name: str, cmd: str, user="root", **kwargs
) -> Tuple[str, str]:
    """
    Execute command in container specified by name, raise exception on non-zero exit code.

    Args:
        container_name: Name or ID of the container
        cmd: Command to execute
        user: User to execute command as (default: "root")
        **kwargs: Additional keyword arguments to pass to container.exec_run
            privileged: Whether to run with privileged permissions

    Returns:
        Tuple of (stdout, stderr)

    Raises:
        subprocess.CalledProcessError: If command execution fails or returns non-zero
        docker.errors.NotFound: If container doesn't exist
    """
    client = docker.from_env()
    container = client.containers.get(container_name)
    return must_exec_container(container, cmd, user=user, **kwargs)


def write_container_file(
        container: docker.models.containers.Container, path: pathlib.Path, contents: str,
):
    try:
        with tempfile.NamedTemporaryFile(delete=True) as tmpfile:
            with tarfile.open(mode='w', fileobj=tmpfile) as tar:
                mem_file = io.BytesIO(contents.encode('utf-8'))
                mem_file.seek(0)

                tarinfo = tarfile.TarInfo(name=path.name)
                tarinfo.size = len(mem_file.getvalue())
                tar.addfile(tarinfo=tarinfo, fileobj=mem_file)
            tmpfile.seek(0)

            container.put_archive(path=str(path.parent), data=tmpfile)

    except docker.errors.ContainerError as e:
        logger.error(f"Error writing file {path} in container {container.name}: {e}")
        raise


class ContainerLogStreamer(Thread):
    def __init__(self, container_id: str, container_name: str):
        super().__init__()
        self._client = docker.from_env()
        self._logger = logging.getLogger(f"container_{container_name}")
        self._container_id = container_id

    def run(self) -> None:
        """
        Stream container logs or stop when the given event is triggered.
        Unfortunately there doesn't appear to be a good way to exit this thread
        except by killing the container being streamed from or by signaling the main thread,
        """
        container = self._client.containers.get(self._container_id)
        for line in container.logs(stream=True, follow=True):
            line = line.decode().strip()
            self._logger.info(line)


class MockCluster:
    """
    Mock cluster docker resources context manager.
    It will always create a root server container with the deployment manager project
    mounted at /project and templated configs mounted at /host. Additional containers
    can be added to the cluster before the context has been entered.

    When entered, the most cluster will create this filesystem to be mounted on each container
    at /host on the local file system's workdir
      WORKDIR/config/      # copied/templated from tests/assets/config
      WORKDIR/mocks/       # copied from tests/assets/mocks/TEST_NAME. Used for mock command responses
      WORKDIR/deamon.py    # copied from deamon.py
    """

    def __init__(
            self,
            test_name: str,
            session_id: str = None,
            mocks_dir: Optional[str] = None,
            workdir: Optional[pathlib.Path] = None,
            subnet: Optional[str] = None,
            has_network: bool = True,
            extra_hosts: dict = None,
    ):
        self._test_name = test_name
        self._mocks_dir = mocks_dir if mocks_dir else self._test_name
        self._basename: str = "dmtest"
        self._session_id = f"{session_id}-{str(int(time.time()))}" if session_id else str(int(time.time()))
        self._has_network = has_network
        self._extra_hosts = extra_hosts
        self._subnet: ipaddress.IPv4Network = _random_ip_network() if not subnet else ipaddress.ip_network(subnet)
        self.workdir = workdir
        if not self.workdir:
            if os.getenv("WORKDIR", False):
                self.workdir = pathlib.Path(os.getenv("WORKDIR")) / self._test_name
                self.workdir.mkdir()
            else:
                self.workdir = pathlib.Path(tempfile.mkdtemp(prefix=self._basename + self._session_id))

        self._docker = docker.from_env()
        # xxx: create the network in the constructor so docker provides an unused subnet, avoiding conflicts
        self._network = create_network(self._docker, f"{self._basename}-{self._session_id}", subnet=subnet) if self._has_network else None
        self._subnet = ipaddress.ip_network(self._network.attrs["IPAM"]["Config"][0]["Subnet"]) if self._network else ipaddress.ip_network("192.168.0.0/24")
        self._last_ip = self._subnet.network_address + 1
        self._containers: Dict[str, docker.models.containers.Container] = {}
        self._container_fns: Dict[str, typing.Callable[[], docker.models.containers.Container]] = {}
        self._log_streamers: Dict[str, ContainerLogStreamer] = {}
        self._template_env = {
            "mgmt_subnet": str(self._subnet),
            "mgmt_gateway": str(self._subnet.broadcast_address - 1),
            "servers": [],
        }

        self._add_root_container()

    @functools.cached_property
    def _docker_image(self):
        """ Allow local image builds to take precedence """
        try:
            self._docker.images.get(LOCAL_IMAGE)
            logger.warning(f"found local docker image, using '{LOCAL_IMAGE}'")
            return LOCAL_IMAGE
        except docker.errors.ImageNotFound:
            return REMOTE_IMAGE

    def _add_root_container(self):
        root_server_ip = str(self._next_ip())

        def lazy_create_root():
            return create_container(
                self._docker,
                name=f"{self._basename}-{self._session_id}-rootserver",
                hostname="rootserver",
                ip=root_server_ip,
                network=self._network.name if self._network else None,
                mounts={
                    f"{GITTOP}/{DEPLOY_MGR_PATH}": "/project",
                    f"{self.workdir}/host": "/host",
                    f"{CLITOP}": "/opt/cerebras/packages/upgrade-pkg",
                },
                env={"HOST_MOCK_DIR": f"/host/mocks/rootserver"},
                cmd=f"python3 /host/daemon.py tail_mock_logs",
                image=self._docker_image,
                extra_hosts=self._extra_hosts,
            )

        self._container_fns["rootserver"] = lazy_create_root
        self._template_env["servers"].append({
            "hostname": "rootserver",
            "ip": root_server_ip,
            "role": "MG",
            "device": "SR",
        })
        self._template_env["dns_ip"] = root_server_ip

    def add_mock_server_container(self, hostname: str, role="CS", device="SY", vendor="CS") -> str:
        """ Add a container to be run on context manager enter.
            Use lazy create since network only exists once context is entered.
            Hard coded to assume that the daemon should be run with an HTTP and SSH server started.
        """
        if self._containers:
            raise ValueError(
                "Must not start container after MockCluster has been entered (with MockCluster(...) as cluster: ...)"
            )
        ip = str(self._next_ip())

        def create() -> docker.models.containers.Container:
            return create_container(
                self._docker,
                name=f"{self._basename}-{self._session_id}-{hostname}",
                hostname=hostname,
                ip=ip,
                network=self._network.name if self._network else None,
                mounts={f"{self.workdir}/host": "/host"},
                env={"HOST_MOCK_DIR": f"/host/mocks/{hostname}"},
                cmd="python3 /host/daemon.py run_mock_server 443",
                image=self._docker_image,
                extra_hosts=self._extra_hosts,
            )

        self._container_fns[hostname] = create
        self._template_env["servers"].append({
            "hostname": hostname,
            "ip": ip,
            "role": role,
            "device": device,
            "vendor": vendor,
        })
        return ip

    def _next_ip(self) -> ipaddress.IPv4Address:
        self._last_ip += 1
        return self._last_ip

    def _start_container_and_stream_logs(self, hostname: str, container: docker.models.containers.Container):
        logger.info(f"starting container {hostname}...")
        self._containers[hostname] = container
        container.start()
        self._log_streamers[hostname] = ContainerLogStreamer(container.id, container.name)
        self._log_streamers[hostname].start()

    def __enter__(self) -> 'MockCluster':
        # template/copy test assets into WORKDIR/host/
        host_dir = self.workdir / "host"
        host_dir.mkdir(exist_ok=True)
        (host_dir / "daemon.py").write_text((TESTS_ASSETS_ROOT / "daemon.py").read_text())

        templates_dir = TESTS_ASSETS_ROOT / "config"
        cfg_target_dir = host_dir / "config"
        cfg_target_dir.mkdir()

        # template the config files
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(templates_dir))
        for path in templates_dir.iterdir():
            if not path.is_file():
                continue
            if not path.name.endswith(".j2"):
                (cfg_target_dir / path.name).write_text(path.read_text())
                continue
            template = env.get_template(path.name)
            rendered_content = template.render(self._template_env)
            (cfg_target_dir / path.name[:-3]).write_text(rendered_content)
        logger.info(f"Templates templated into {cfg_target_dir}")

        # copy the mocks tree
        mocks_dir = TESTS_ASSETS_ROOT / "mocks" / self._mocks_dir
        if mocks_dir.is_dir():
            shutil.copytree(str(mocks_dir.absolute()), str((host_dir / "mocks").absolute()))

        # copy extra files
        files_dir = TESTS_ASSETS_ROOT / "files"
        shutil.copytree(str(files_dir.absolute()), str((host_dir / "files").absolute()))

        # start containers
        for hostname, container_fn in self._container_fns.items():
            host_mock_dir = host_dir / "mocks" / hostname
            if not host_mock_dir.exists():
                host_mock_dir.mkdir(parents=True)
            self._start_container_and_stream_logs(hostname, container_fn())

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if KEEP_CLUSTER:
            logger.info("KEEP_CLUSTER set, not deleting test docker resources")
            logger.warning("You MUST interrupt the pytest process (ctrl+C) to exit")
            return
        logger.info("removing test resources")

        for hostname, container in self._containers.items():
            try:
                container.kill()  # the log streamer should die when the container is killed
            except Exception as e:
                logger.warning(f"failed to kill container {container}: {e}")
            try:
                container.remove()
            except Exception as e:
                logger.warning(f"failed to remove container {container}: {e}")
        if self._network:
            self._network.remove()
        cleanup_networks(self._docker)

    def exec(self, hostname: str, cmd: str, **kwargs) -> Tuple[int, str, str]:
        """ exec command on root server """
        return exec_container(self._containers[hostname], cmd, **kwargs)

    def must_exec(self, hostname: str, cmd: str, **kwargs) -> str:
        """ must exec command on root server """
        return must_exec_container(self._containers[hostname], cmd, **kwargs)[0]

    def must_exec_root(self, cmd: str, **kwargs) -> str:
        """ must exec command on root server """
        return must_exec_container(self._containers["rootserver"], cmd, **kwargs)[0]

    def write_file_contents(self, hostname: str, path: pathlib.Path, contents: str):
        return write_container_file(self._containers[hostname], path, contents)

    def __str__(self):
        return f"{self._test_name}[workdir={self.workdir},template_env={self._template_env}]"

    def exec_cscfg(self, command_string: str, **kwargs) -> typing.Any:
      """
      Executes a cscfg command string in the rootserver container
      and returns a CommandResult object with stdout, stderr, and returncode.
      """
      full_cmd = f"cscfg {command_string}"

      exit_code, stdout_str, stderr_str = self.exec("rootserver", full_cmd, **kwargs)

      class CommandResult:
          def __init__(self, stdout, stderr, returncode):
              self.stdout = stdout + "\n" + stderr
              self.stderr = stderr
              self.returncode = returncode

      return CommandResult(stdout_str, stderr_str, exit_code)


@pytest.fixture
def mock_cluster_fixture(request):
    """
    A helper Pytest fixture that provides an initialized MockCluster instance, using it as a context manager.
    We extract a test-specific name to use for the cluster.
    We pre-install cscfg in it, so that the test can simply call `exec_cscfg` as needed.
    """
    test_specific_name = request.node.name.replace("[", "_").replace("]", "").replace("-", "_")
    
    with MockCluster(test_specific_name) as mc:
        try:
           mc.must_exec_root("/project/install.sh")
           logger.info(f"Fixture mock_cluster_fixture: /project/install.sh completed for test {test_specific_name}")

           mc.must_exec_root("cscfg profile create devtest --config_input /host/config/devtest_input.yml")
           logger.info("Cscfg ready to run with devtest profile")
        except Exception as e:
            logger.error(f"Fixture mock_cluster_fixture: /project/install.sh FAILED for test {test_specific_name}: {e}")
            raise
        yield mc
