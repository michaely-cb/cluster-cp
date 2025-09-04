import io
import logging
import pathlib
import shutil
import tarfile
import tempfile
import time
import typing
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager

import docker
import docker.models.containers as dockercontainer


REGISTRY_URL = "registry.local"


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


class DockerClusterController(ClusterController):
    def __init__(self, nodes: typing.List[str], logger=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert len(nodes) > 0
        self._nodes = nodes
        self._client: typing.Optional[docker.DockerClient] = None
        if not logger:
            self._logger = logging.getLogger(DockerClusterController.__name__)

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
