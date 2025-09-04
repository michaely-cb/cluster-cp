import base64
import io
import logging
import os
import pathlib
import shutil
import socket
import tarfile
import tempfile
import time
import typing
from abc import ABC
from abc import abstractmethod
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from contextlib import AbstractContextManager

import docker
import docker.models.containers as dockercontainer
import paramiko
import subprocess

from commands import ROOT_LOGGER_NAME
from common import DockerImage
from common import REGISTRY_URL

WORKER_THREAD_COUNT = 32


class FakePemFile:
    def __init__(self, b64encpem):
        self.pem = str(base64.b64decode(b64encpem), 'utf-8')

    def readlines(self):
        return self.pem.split("\n")


class Cmd(ABC):
    def __init__(self, cmd):
        self.cmd = cmd

    def __str__(self):
        return f"{self.cmd}"


class MkdirCmd(Cmd):
    def __init__(self, path, ignore_existing=True):
        self.path = path
        self.ignore_existing = ignore_existing

    def __str__(self):
        return f"mkdir{' -p' if self.ignore_existing else ''} {self.path}"


class FileCopyCmd(Cmd):
    """ Copy a file from the local machine to the remote machine. """

    def __init__(self, src, dst):
        self.src = str(src)
        self.dst = str(dst)

    def __str__(self):
        return f"cp {self.src} {self.dst}"


class FileGetCmd(Cmd):
    """ Copy a file from the remote machine to the local machine. """

    def __init__(self, src, dst):
        self.src = pathlib.Path(src)
        self.dst = pathlib.Path(dst)

    def __str__(self):
        return f"cp {self.src} {self.dst}"


class DirCopyCmd(Cmd):
    def __init__(self, src, dst):
        self.src = src
        self.dst = dst

    def __str__(self):
        return f"cp -r {self.src} {self.dst}"


class SudoCmd(Cmd):
    def __init__(self, cmd):
        self.cmd = cmd

    def __str__(self):
        return f"sudo {self.cmd}"


class ClusterController(AbstractContextManager):
    def __init__(self, *args, **kwargs):
        self._close_callback = None

    @abstractmethod
    def exec_cmds(self, cmds: typing.List[Cmd]) -> None:
        """
        Executes a sequence of commands on the first (assumed master) node of the nodes under node controller.
        """
        pass

    @abstractmethod
    def exec_cmds_cluster(self, cmds: typing.List[Cmd]) -> None:
        """
        Executes a command for each node. Execution stops for the first command on a node that fails.
        """
        pass

    @abstractmethod
    def exec(self, cmd: str) -> typing.Tuple[int, str, str]:
        """
        exec the given shell command on the primary node.
        :param cmd:
        :return: Tuple<return_code, std_out, std_err>
        """
        pass

    @abstractmethod
    def exec_background(self, cmd: str) -> None:
        """
        exec the given shell command on the primary node in the background (nohup).
        The command will continue running even after the SSH session is closed.
        :param cmd: Command to run in the background
        """
        pass

    def must_exec(self, cmd: str) -> typing.Tuple[int, str, str]:
        s, so, se = self.exec(cmd)
        if s != 0:
            raise Exception(f"non-zero return code ({s}) from '{cmd}'. stdout: {so}, stderr: {se}")
        return s, so, se

    @abstractmethod
    def exec_cluster(self,
                     cmd: str,
                     node_predicate: typing.Optional[typing.Callable[[str], bool]] = None) -> \
            typing.Mapping[str, typing.Tuple[int, str, str]]:
        """
        Execs the given shell command on all the nodes.
        Parameters:
            cmd: shell command to run over ssh
            node_predicate: optional lambda which may return True for nodes which the cmd is executed on
        Returns:
            node_name -> Tuple<return_code, std_out, std_err>
        """
        pass

    @abstractmethod
    def control_plane_ip(self) -> typing.Optional[str]:
        """returns the ip address of the control plane node"""
        pass

    @abstractmethod
    def control_plane_name(self) -> typing.Optional[str]:
        """returns the name of the control plane node"""
        pass

    def load_image(self, img_path) -> DockerImage:
        """
        Load the given tar'd docker image onto the cluster's container daemon.
        Tags images with path to local registry.
        Parameters:
            img_path: path to image.tar on the remote host
        """
        status, stdout, stderr = self.exec("which ctr")
        if status != 0:
            raise Exception(f"load image requires ctr but ctr is not present: {stderr}")
        ctr_path = stdout.strip()

        status, stdout, stderr = self.exec(f"{ctr_path} -n k8s.io images import {img_path}")

        if status != 0 or not stdout.startswith("unpacking"):
            raise Exception(f"Loading image {img_path}.tar failed: {stdout}. {stderr}")

        out_parts = stdout.split()
        assert len(out_parts) >= 3, f"Should have at least 3 words: {stdout}"
        assert "unpacking" == out_parts[0], f"Should start with unpacking: {stdout}: {out_parts[0]}"
        image_full_name = out_parts[1]

        url = f"{REGISTRY_URL}/{image_full_name.rsplit('/', 1)[1]}"
        self.exec_cmds([
            SudoCmd(f"{ctr_path} -n k8s.io images tag --force {image_full_name} {url}"),
            SudoCmd(f"{ctr_path} -n k8s.io images push --hosts-dir /etc/containerd/certs.d/ {url}")
        ])
        return DockerImage.of(url)

    def open(self) -> 'ClusterController':
        """Opens internally held connections to nodes."""
        return self

    def close(self):
        """Closes internally held connections to nodes"""
        if self._close_callback:
            self._close_callback()

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close_callback(self, callback: typing.Callable):
        self._close_callback = callback


class SSHClusterController(ClusterController):

    def __init__(self, nodes: typing.Dict[str, str],
                 username, password=None, pem=None, key_filename=None,
                 deploy_node=None, mgmt_nodes=None,
                 logger=None, *args, **kwargs):
        """
        :param nodes: dict of node name -> hostnames of the nodes. k/v may be the same so long as the name/hostname is
            reachable for SSH
        :param deploy_node: The deploy node's name. This is the node where deployment commands are run.
        :param pem: base64 encoded pem file
        """
        super().__init__(*args, **kwargs)
        assert len(nodes) > 0
        self._node_hostname = nodes
        self._mgmt_nodes = mgmt_nodes
        self._deploy_node_name = deploy_node
        self._username = username
        self._password = password
        self._pem = pem
        self._key_filename = key_filename
        if not self._password and not self._pem and not self._key_filename:
            raise Exception("must supply 'password' or 'pem' or 'key_filename' parameter")
        if not logger:
            self._logger = logging.getLogger(ROOT_LOGGER_NAME + "." + SSHClusterController.__name__)

    def _client(self, node) -> paramiko.SSHClient:
        """Generate a client to the node. It is the caller's responsibility to close the client on finish"""
        client = paramiko.client.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.MissingHostKeyPolicy)
        host, port = self._node_hostname[node], 22
        try:
            if ":" in host:
                host, port = host.split(":", 1)
            if self._password is not None:
                self._logger.debug(f"connecting to {self._username}@{host}:{port} with password authentication")
                client.connect(hostname=host, port=port, allow_agent=False, look_for_keys=False,
                               username=self._username, password=self._password, timeout=30.0)
            elif self._pem is not None:
                self._logger.debug(f"connecting to {self._username}@{host}:{port} with pem authentication")
                pem_file = paramiko.RSAKey.from_private_key(FakePemFile(self._pem))
                client.connect(hostname=host, port=port, username=self._username, pkey=pem_file, timeout=30.0)
            elif self._key_filename is not None:
                self._logger.debug(f"connecting to {self._username}@{host}:{port} with key file authentication")
                client.connect(hostname=host, port=port,
                               username=self._username, key_filename=self._key_filename, timeout=30.0)
            self._logger.debug(f"connected to {self._username}@{host}:{port}")
        except Exception as e:
            self._logger.error(f"failed to connect {self._username}@{host}: {e}")
            raise
        return client

    def open(self) -> 'SSHClusterController':
        return self

    def close(self):
        ClusterController.close(self)

    @property
    def _deploy_node(self) -> str:
        """
        Returns the node name where cluster deploy commands are run. Uses the first controlplane
        node if 'deploy_node' was not passed in on SSHClusterController init
        """

        if self._deploy_node_name:
            return self._deploy_node_name

        # search the mgmt nodes for a deploy node candidate
        self._logger.debug("SSHClusterController.deploy_node not set, searching for the first controlplane node...")
        candidates = self._mgmt_nodes if self._mgmt_nodes else self._node_hostname.keys()
        for node in candidates:
            with self._client(node) as c:
                _, stdout, stderr = c.exec_command("stat /etc/kubernetes/manifests/kube-apiserver.yaml")
                # read() must be called before receiving exit code or else can hang indefinitely!
                stdout.read()
                stderr.read()
                if stdout.channel.recv_exit_status() == 0:
                    self._deploy_node_name = node
                    return node
        self._logger.warning(f"unable to find control plane node, using first mgmt node as deploy_node: {candidates[0]}")
        self._deploy_node_name = candidates[0]
        return self._deploy_node_name


    def control_plane_ip(self) -> typing.Optional[str]:
        return socket.gethostbyname(self._node_hostname[self._deploy_node])

    def control_plane_name(self) -> str:
        return self._deploy_node

    def exec_cmds(self, cmds: typing.List[Cmd]):
        with SFTPClient(self._client(self._deploy_node), host=self._deploy_node) as c:
            for cmd in cmds:
                self._logger.debug(f"exec cmd {self._deploy_node}: {str(cmd)}")
                self._exec_cmd(c, cmd)

    def exec_cmds_cluster(self, cmds: typing.List[Cmd]):
        def exec_cmds(node, cmds):
            with self._client(node) as c:
                with SFTPClient(c, host=node) as sfptc:
                    for cmd in cmds:
                        self._logger.debug(f"exec cmd cluster {node}: {str(cmd)}")
                        self._exec_cmd(sfptc, cmd)
            return 0

        with ThreadPoolExecutor(max_workers=WORKER_THREAD_COUNT) as executor:
            promises = []
            i = 0
            for n in self._node_hostname.keys():
                i = i + 1
                promises.append(executor.submit(exec_cmds, n, cmds))
                if i % WORKER_THREAD_COUNT == 0 or i >= len(self._node_hostname.keys()):
                    for p in as_completed(promises):
                        p.result()
                    promises = []

    def exec(self, cmd: str, binary_mode: bool = False) -> typing.Tuple[int, typing.Union[str, bytes], str]:
        self._logger.debug(f"exec {self._deploy_node}:'{cmd}'")
        start_time = time.time()
        with self._client(self._deploy_node) as c:
            _, stdout, stderr = c.exec_command(cmd)
            duration = int(time.time() - start_time)
            if duration > 0:
                self._logger.debug(f"took: {duration}s")

            # Read both streams in the same line to help avoid deadlock on large outputs.
            # Sequentially reading one then the other can cause hangs if buffers fill up.
            # This is not a guarantee against deadlocks, for high-volume output on both streams consider reading them in parallel.
            if binary_mode:
                so, se = stdout.read(), stderr.read().decode().strip()
            else:
                so, se = stdout.read().decode().strip(), stderr.read().decode().strip()
            return stdout.channel.recv_exit_status(), so, se

    def exec_background(self, cmd: str) -> None:
        """
        Execute a command in the background using nohup and redirecting output.
        This allows the command to continue running after the SSH session ends.
        """
        self._check_open()
        # Wrap the command with an explicit shell invocation for correct interpretation.
        bg_cmd = f"sh -c 'nohup {cmd} > /dev/null 2>&1 </dev/null & echo $!'"
        self._logger.debug(f"exec background {self._deploy_node}:'{bg_cmd}'")
        node = self._deploy_node
        container = self._client.containers.get(node)
        status, output = container.exec_run(bg_cmd, demux=True)
        
        if status != 0:
            self._logger.warning(f"Failed to start background command '{cmd}', status: {status}")
        else:
            s = lambda x: str(x, 'utf-8') if x else ""
            pid = s(output[0]).strip()
            self._logger.debug(f"Background command started with PID {pid}")
        self._logger.debug(f"exec background {self._deploy_node}:'{bg_cmd}'")
        with self._client(self._deploy_node) as c:
            _, stdout, stderr = c.exec_command(bg_cmd)
            # read() must be called before receiving exit code or else can hang indefinitely!
            pid = stdout.read().decode().strip()
            stderr.read()  # Read stderr to avoid potential hanging
            status = stdout.channel.recv_exit_status()
            if status != 0:
                self._logger.warning(f"Failed to start background command '{cmd}', status: {status}")
            else:
                self._logger.debug(f"Background command started with PID {pid}")

    def exec_cluster(self,
                     cmd: str,
                     node_predicate: typing.Optional[typing.Callable[[str], bool]] = None) -> \
            typing.Mapping[str, typing.Tuple[int, str, str]]:
        if node_predicate is None:
            node_predicate = lambda x: True
        self._logger.debug(f"exec cluster: '{cmd}'")

        rv = {}
        with ThreadPoolExecutor(max_workers=WORKER_THREAD_COUNT) as executor:
            promises = {}
            clients = []
            for i, n in enumerate(self._node_hostname.keys()):
                if node_predicate(n):
                    c = self._client(n)
                    clients.append(c)
                    promises[executor.submit(c.exec_command, cmd)] = n
                if len(promises) % WORKER_THREAD_COUNT == 0 or i == len(self._node_hostname.keys()) - 1:
                    for p in as_completed(promises):
                        _, stdout, stderr = p.result()
                        so, se = stdout.read().decode(), stderr.read().decode()
                        rv[promises[p]] = stdout.channel.recv_exit_status(), so, se
                    promises = {}
                    for c in clients:
                        c.close()
                    clients = []
        return rv

    def _exec_cmd(self, c: 'SFTPClient', cmd: Cmd):
        if isinstance(cmd, MkdirCmd):
            c.mkdir(cmd.path, ignore_existing=cmd.ignore_existing)
        elif isinstance(cmd, FileCopyCmd):
            c.put(cmd.src, cmd.dst)
        elif isinstance(cmd, FileGetCmd):
            c.get(str(cmd.src), str(cmd.dst))
        elif isinstance(cmd, DirCopyCmd):
            c.put_dir(cmd.src, cmd.dst)
        else:
            _, stdout, stderr = c.paramiko_sshclient.exec_command(f"{cmd}")
            if stdout.channel.recv_exit_status() != 0:
                raise Exception(f"{cmd} failed: {stderr.read()}")


class DockerClusterController(ClusterController):
    def __init__(self, nodes: typing.List[str], logger=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert len(nodes) > 0
        self._nodes = nodes
        self._client: typing.Optional[docker.DockerClient] = None
        if not logger:
            self._logger = logging.getLogger(ROOT_LOGGER_NAME + "." + DockerClusterController.__name__)

    def open(self) -> 'DockerClusterController':
        if not self._client:
            self._client = docker.DockerClient()
        return self

    def close(self):
        ClusterController.close(self)
        self._client.close()

    def control_plane_ip(self) -> typing.Optional[str]:
        ctrl = self._client.containers.get(self._nodes[0])
        addr = ctrl.attrs.get("NetworkSettings").get("Networks").get("kind").get("IPAddress")
        self._logger.debug(f"docker container {self._nodes[0]} ip address: {addr}")
        return addr

    def control_plane_name(self) -> str:
        return self._nodes[0]

    def exec_cmds(self, cmds: typing.List[Cmd]):
        self._check_open()
        node = self._nodes[0]
        target = self._client.containers.get(node)
        for cmd in cmds:
            self._logger.debug(f"exec cmd {node}: {str(cmd)}")
            self._exec_cmd(target, cmd)

    def exec_cmds_cluster(self, cmds: typing.List[Cmd]):
        self._check_open()
        for i, node in enumerate(self._nodes):
            target = self._client.containers.get(node)
            for cmd in cmds:
                self._logger.debug(f"exec cmd cluster {node}: {str(cmd)}")
                self._exec_cmd(target, cmd)

    def exec(self, cmd: str) -> typing.Tuple[int, str, str]:
        self._check_open()
        return self._exec(self._nodes[0], cmd)

    def exec_background(self, cmd: str) -> typing.Tuple[int, str, str]:
        """
        Execute a command in the background in a Docker container.
        Returns a tuple of (status, stdout, stderr) where stdout contains the PID.
        """
        self._check_open()
        # Use an explicit shell invocation for correct interpretation of backgrounding.
        bg_cmd = f"sh -c 'nohup {cmd} > /dev/null 2>&1 </dev/null & echo $!'"
        self._logger.debug(f"exec background {self._nodes[0]}: '{bg_cmd}'")
        node = self._nodes[0]
        container = self._client.containers.get(node)

        result = container.exec_run(bg_cmd, demux=True)
        if result is None:
            self._logger.error("exec_run returned None")
            raise RuntimeError("Failed to execute background command.")

        status, output = result
        decode = lambda x: x.decode('utf-8') if x else ""
        stdout = decode(output[0]).strip() if output and len(output) > 0 else ""
        stderr = decode(output[1]).strip() if output and len(output) > 1 else ""
        
        if status != 0:
            self._logger.warning(f"Failed to start background command '{cmd}', status: {status}, stderr: {stderr}")
        else:
            self._logger.debug(f"Background command started with PID {stdout}")

        return status, stdout, stderr

    def exec_cluster(self,
                     cmd: str,
                     node_predicate: typing.Optional[typing.Callable[[str], bool]] = None) -> \
            typing.Mapping[str, typing.Tuple[int, str, str]]:
        if node_predicate is None:
            node_predicate = lambda n: True
        self._check_open()
        self._logger.debug(f"exec_cluster: '{cmd}'")
        rv = {}
        for i, node in enumerate(self._nodes):
            if node_predicate(node):
                rv[self._nodes[i]] = self._exec(node, cmd)
        return rv

    def _exec(self, node, cmd) -> typing.Tuple[int, str, str]:
        self._logger.debug(f"exec {node} {cmd}")
        start_time = time.time()
        status, out = self._client.containers.get(node).exec_run(cmd, demux=True)
        duration = int(time.time() - start_time)
        if duration > 0:
            self._logger.debug(f"took: {duration}s")
        s = lambda x: str(x, 'utf-8') if x else ""
        return status, s(out[0]).strip(), s(out[1]).strip()

    def _check_open(self):
        if not self._client:
            raise Exception("attempted to call an exec command without first opening a connection")

    def _exec_cmd(self, target: dockercontainer.Container, cmd: Cmd):
        if isinstance(cmd, MkdirCmd):
            status, [stdout, stderr] = \
                target.exec_run(f"mkdir {'-p' if cmd.ignore_existing else ''} {cmd.path}", demux=True)
            if status != 0:
                raise Exception(f"failed to mkdir: {stdout}, {stderr}")

        elif isinstance(cmd, FileCopyCmd):
            tardir = tempfile.mkdtemp()
            tmpfile = f"{tardir}/dockerxfr.tar"
            tar = tarfile.open(tmpfile, 'w')
            tar.add(cmd.src, pathlib.Path(cmd.dst).name)
            tar.close()
            with open(tmpfile, 'rb') as f:
                if not target.put_archive(str(pathlib.Path(cmd.dst).parent), f.read()):
                    raise Exception(f"failed to copy file to docker node: {cmd}")
            shutil.rmtree(tardir, ignore_errors=True)

        elif isinstance(cmd, FileGetCmd):
            """ Get a file from a docker container."""
            bits, stat = target.get_archive(cmd.src)
            self._logger.debug(f"docker get_archive {cmd.src}: {stat}")
            tarstream = io.BytesIO(b"".join(bits))
            tar = tarfile.open(fileobj=tarstream)
            tar.extractall(cmd.dst.parent)

        elif isinstance(cmd, DirCopyCmd):
            tardir = tempfile.mkdtemp()
            tarchive = shutil.make_archive(f"{tardir}/dockerxfr", 'tar', cmd.src)
            with open(tarchive, 'rb') as f:
                rawbytes = f.read()
                if not target.put_archive(cmd.dst, rawbytes):
                    raise Exception(f"failed to copy dir to docker node: {cmd}")
            shutil.rmtree(tardir, ignore_errors=True)

        else:
            status, [stdout, stderr] = \
                target.exec_run(cmd.cmd, demux=True, privileged=True)
            if status != 0:
                raise Exception(f"{str(cmd)} failed: {status}, {stdout}, {stderr}")


class SFTPClient:
    """
    A SFTPClient built on top of paramiko sftp client
    """

    def __init__(self, paramiko_sshclient, host=""):
        self.paramiko_sshclient = paramiko_sshclient
        self._logger = logging.getLogger("cs_cluster.SFTPClient")
        self._host = host  # for logging purposes only

    def __enter__(self):
        self.sftp_client: paramiko.SFTPClient = self.paramiko_sshclient.open_sftp()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.sftp_client.close()

    def put(self, source, target):
        """
        Uploads the content of the source file to the target path if target not exists.
        The target directory needs to exist.
        """
        source_size = os.stat(source).st_size
        target_size = -1
        # avoid large image(> 1MB) redundant copy if possible
        # we rely on versioning for most image updates so size check is only for exceptions
        if source_size > 1024 * 1024 and ".tar" in source:
            try:
                target_size = self.sftp_client.stat(target).st_size
            except IOError:
                # if target not exists
                target_size = -1
        if target_size != source_size or self._checksum_diff(source, target):
            self._logger.debug(f"copying {source} to {self._host}:{target}")
            self.sftp_client.put(source, target)
        else:
            self._logger.debug(f"skip copying for {source}")
        self.sftp_client.chmod(f"{target}", os.stat(source).st_mode)

    def _checksum_diff(self, source, target) -> bool:
        try:
            s = subprocess.check_output(f"sha1sum {source} |cut -c 1-10", stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError:
            return False
        s_sum = s.strip()
        _, t, _ = self.paramiko_sshclient.exec_command(f"sha1sum {target} |cut -c 1-10")
        t_sum = t.read().strip()
        self._logger.debug(f"{source} {target} checksum local:{s_sum} remote:{t_sum}")
        return s_sum != t_sum

    def get(self, source, target):
        """
        Downloads the content of the source file to the target path.
        """
        self.sftp_client.get(source, target)

    def put_dir(self, source, target):
        """
        Uploads the contents of the source directory to the target path. The
        target directory needs to exists. All subdirectories in source are
        created under target.
        """
        for item in os.listdir(source):
            if os.path.isfile(os.path.join(source, item)):
                self.put(os.path.join(source, item), f"{target}/{item}")
            else:
                self.mkdir(f"{target}/{item}", ignore_existing=True)
                self.put_dir(os.path.join(source, item), f"{target}/{item}")

    def mkdir(self, path, mode=511, ignore_existing=True):
        """Augments mkdir by adding an option to not fail if the folder exists. Logic handles recursive directory
        creation similar to mkdir -p"""
        try:
            self.sftp_client.mkdir(path, mode)
        except IOError as err:
            if "no such file" in str(err).lower():
                self._logger.debug(f"parent to path {self._host}:{path} did not exist, try create")
                path_so_far = pathlib.Path()
                for part in pathlib.Path(path).parts:
                    path_so_far = path_so_far / part
                    try:
                        self.sftp_client.mkdir(str(path_so_far))
                        self._logger.debug(f"parent to path {self._host}:{path_so_far} created")
                    except IOError as e:
                        self._logger.debug(f"parent to path {self._host}:{path_so_far} exists: {e}")
                    except Exception as e:
                        self._logger.error(f"failed to create parents on {self._host}: {e}")
                        raise
                self._logger.debug(f"mkdir on {self._host}: failed: {err}")
            if not ignore_existing:
                self._logger.debug(f"mkdir on {self._host} already exists")
                raise


class SSHHelper(AbstractContextManager):
    """SSH between multiple hosts that have the same username/credentials configured."""

    # Cache SSH Connections so repeated calls to the same host are fast
    CacheEntry = namedtuple("cacheentry", ["host", "client"])
    CACHESIZE = 8

    def __init__(self, username, password=None, pem=None, port=22):
        self.__cache = []
        self.username = username
        self.password = password
        self.pem = paramiko.RSAKey.from_private_key_file(pem) if pem else None
        self.look_for_keys = False if pem or password else True
        self.port = port
        self._logger = logging.getLogger(ROOT_LOGGER_NAME + "." + SSHHelper.__name__)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        c, self.__cache = self.__cache, []
        for entry in c:
            entry.client.close()

    def _connect(self, host: str, auth_none: bool = False) -> paramiko.SSHClient:
        """ Create a new SSH connection to the host. """
        client = paramiko.client.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.MissingHostKeyPolicy)
        try:
            client.connect(
                hostname=host,
                port=self.port,
                username=self.username,
                password=self.password,
                pkey=self.pem,
                look_for_keys=self.look_for_keys,
                timeout=5,
            )
        except paramiko.SSHException:
            if not auth_none:
                raise
            # hack around the fact that paramiko doesn't accept auth=none which
            # is the auth method for cs machines. Transport is only initialized
            client.get_transport().auth_none("root")
        except socket.gaierror as e:
            raise Exception(f"unable to start ssh session {self.username}@{host}: {e}", e)
        return client

    def _cache_connect(self, host: str) -> paramiko.SSHClient:
        for i in range(len(self.__cache)):
            if self.__cache[i].host == host:
                e = self.__cache.pop(i)
                self.__cache.insert(0, e)
                return e.client
        if len(self.__cache) == SSHHelper.CACHESIZE:
            self.__cache.pop().client.close()
        client = self._connect(host)
        self.__cache.insert(0, SSHHelper.CacheEntry(host, client))
        return client

    def must_exec(self, host: str, cmd: str, close=True, is_system: bool = False) -> str:
        """Execute cmd on host. stdout is returned on success."""

        def _must_exec(client):
            start_time = time.time()
            _, stdout, stderr = client.exec_command(cmd)
            status, stdout = stdout.channel.recv_exit_status(), stdout.read().decode()
            duration = int(time.time() - start_time)
            if duration > 0:
                self._logger.debug(f"took: {duration}s")
            if status != 0:
                stderr = stderr.read().decode()
                self._logger.debug(f"{self.username}@{host} exec({cmd}) => status={status} stderr={stderr}")
                raise RuntimeError(f"non-zero status ({status}) for '{cmd}' on {self.username}@{host}: {stderr}")
            self._logger.debug(f"{self.username}@{host} exec({cmd}) => stdout={stdout}")
            return stdout

        if close:
            with self._connect(host, auth_none=is_system) as client:
                return _must_exec(client)
        return _must_exec(self._cache_connect(host))

    def conn(self, host: str) -> paramiko.SSHClient:
        return self._connect(host)
