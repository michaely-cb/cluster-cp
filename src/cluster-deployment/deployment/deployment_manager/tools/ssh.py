import logging
import paramiko
import select
import socket
import sys
import termios
import threading
import time
import tty
import typing
from contextlib import AbstractContextManager
from typing import List, Tuple

logger = logging.getLogger(__name__)


def make_prompt_detector(prompt_chars: str = "]>#$:") -> typing.Callable[[bytes], bool]:

    def is_prompt(output: bytes) -> bool:
        last_line = output.decode().split("\n")[-1]
        if not last_line.strip() or last_line[-1] in ('\n', '\r'):
            return False
        if not last_line.strip():
            return False
        return last_line.rstrip()[-1] in prompt_chars

    return is_prompt


def is_prompt_simple(output: bytes) -> bool:
    s = output.decode()
    return not s.endswith("\n")


class ShellSession:
    def __init__(self, client: paramiko.SSHClient, buffer_size: int, name=""):
        self._client = client
        self._buffer_size = buffer_size
        self._shell: typing.Optional[paramiko.Channel] = None
        self._name = f"session-{time.time()}" if not name else name
        self._prompt_detected = threading.Event()
        self._output_chunks: List[bytes] = []
        self._output_lock = threading.Lock()
        self._stdout_reader: typing.Optional[threading.Thread] = None
        self._exiting = threading.Event()

        self.prompt_detect: typing.Callable[[bytes], bool] = is_prompt_simple

        def on_enter():
            self._shell = self._client.invoke_shell()

            def read_forever(shell: paramiko.Channel, output_chunks: List[bytes], should_exit: threading.Event):
                while not should_exit.is_set():
                    timeout = time.time() + 1
                    while not shell.recv_ready() and time.time() < timeout:
                        time.sleep(0.1)
                    if not shell.recv_ready():
                        continue
                    output = shell.recv(self._buffer_size)
                    if len(output) == 0:
                        logger.warning(f"shell session {self._name} closed unexpectedly")
                        break
                    with self._output_lock:
                        output_chunks.append(output)
                    if self.prompt_detect(output):
                        self._prompt_detected.set()
                self._shell.close()

            self._stdout_reader = threading.Thread(target=read_forever, name="shell-reader",
                                                   args=(self._shell, self._output_chunks, self._exiting))
            self._stdout_reader.start()
            self._shell.send("\n".encode())

        self._on_enter = on_enter

    def __enter__(self) -> 'ShellSession':
        self._on_enter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._exiting.set()
            self._stdout_reader.join(timeout=5)
        except Exception as e:
            logger.warning(f"failed to close shell session {self._name} cleanly: {e}")

    def interactive(self):
        """ Enter an interactive shell session, accepting user input on stdin
        Code taken from: https://github.com/paramiko/paramiko/blob/main/demos/interactive.py
        """
        shell = self._client.invoke_shell()
        oldtty = termios.tcgetattr(sys.stdin)
        try:
            tty.setraw(sys.stdin.fileno())
            tty.setcbreak(sys.stdin.fileno())
            shell.settimeout(0.0)

            while True:
                r, w, e = select.select([shell, sys.stdin], [], [])
                if shell in r:
                    try:
                        x = shell.recv(self._buffer_size).decode()
                        if len(x) == 0:
                            sys.stdout.write("\r\n")
                            break
                        sys.stdout.write(x)
                        sys.stdout.flush()
                    except socket.timeout:
                        pass
                if sys.stdin in r:
                    x = sys.stdin.read(1)
                    if len(x) == 0:
                        break
                    if ord(x) == 27:  # Escape character - for arrow key movement, etc
                        x += sys.stdin.read(2)
                        if x[-1] == '~':  # Sequences like End and Home usually end with '~'
                            x += sys.stdin.read()  # Read the rest to complete the sequence
                    shell.send(x.encode())
        finally:
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)
            shell.close()

    def read(self) -> str:
        out = []
        with self._output_lock:
            while self._output_chunks:
                out.append(self._output_chunks.pop(0))
        return b"".join(out).decode()

    def exec(self, command: typing.Union[str, bytes], timeout: typing.Optional[int] = 60, capture_init=False, disable_logging=False) -> str:
        """
        Exec a command in the shell session, returning the output stripped of the prompt and command if the shell
        returned them.
        Args
            prompt: a prompt string for the shell to act as a marker for termination of the command, e.g. 'sonic# '
            capture_init: whether to return any output that was already in the channel buffer as part of the return value
            disable_logging: whether to log the interaction in the cscfg logs. Disable in cases where sensitive info is passed
        """
        scrubbed_command = "<REDACTED>" if disable_logging else command

        if self._shell.closed:
            raise RuntimeError(f"socket closed unexpectedly prior to sending '{scrubbed_command}'")

        signaled = self._prompt_detected.wait(timeout=10)
        if not signaled:
            logger.warning(f"shell session {self._name} failed to detect initial prompt before command {scrubbed_command}, "
                           "continuing anyways")
        else:
            self._prompt_detected.clear()

        buffer = self.read()
        if not capture_init:
            buffer = ""

        # send command, possibly in parts depending on what the server accepts
        cmd_bytes, cmd_i = (command + '\n').encode() if isinstance(command, str) else command, 0
        while cmd_i < len(cmd_bytes):
            bytes_sent = self._shell.send(cmd_bytes[cmd_i:])
            if bytes_sent == 0:
                raise RuntimeError(f"socket closed unexpectedly while sending '{scrubbed_command}'")
            cmd_i += bytes_sent

        signaled = self._prompt_detected.wait(timeout=timeout)
        if not signaled:
            raise TimeoutError(f"command '{scrubbed_command}' timed out waiting for input prompt for {timeout} seconds, "
                               f"last output: {self.read() if not disable_logging else 'REDACTED'}")

        buffer += self.read()
        if not disable_logging:
            logger.debug(f"{self._name}: {command} -> {buffer}")
        return buffer


class ExecMixin(AbstractContextManager):
    def exec(self, command: str, timeout: int = 30, throw: bool = False) -> Tuple[int, str, str]:
        raise NotImplementedError("exec not implemented")

    def exec_commands(self, commands: List[str], timeout: int = 30) -> typing.Tuple[List[str], str]:
        raise NotImplementedError("exec_commands not implemented")

    def shell_session(self) -> ShellSession:
        raise NotImplementedError("shell_session not implemented")

    def scp_file(self, local_path: str, remote_path: str):
        """ copy a file from local path to remote path """
        raise NotImplementedError("scp_file not implemented")


class SSHConn(ExecMixin):
    """
    SSH Connection that can be entered/exited multiple times
    """
    conn_timeout = 30
    conn_keepalive = int(conn_timeout / 10)
    buffer_size = 1 << 20  # 1 Mi

    def __init__(self, addr: str, username: str, password: typing.Optional[str] = None):
        self.address = addr
        self.username = username
        self.password = password

        self._client = None

    def __enter__(self) -> 'SSHConn':
        if self._client is not None:
            raise RuntimeError("SSHConn already entered")

        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())

        try:
            self._client.connect(self.address,
                                 username=self.username,
                                 password=self.password,
                                 allow_agent=False,
                                 look_for_keys=True if not self.password else False,
                                 timeout=self.conn_timeout)
        except (paramiko.SSHException, socket.error) as ex0:
            logger.warning(f"failed ssh connect to {self.username}@{self.address}: {ex0}")
            # fallback: try passwordless or zero-auth if passwordless was attempted the first time
            try:
                if self.password is None:
                    self._client.get_transport().auth_none(username=self.username)
                else:
                    self._client.connect(self.address,
                                         username=self.username,
                                         allow_agent=False,
                                         look_for_keys=True,
                                         timeout=self.conn_timeout)
            except (paramiko.SSHException, socket.error) as ex1:
                logger.warning(f"failed ssh connect to {self.username}@{self.address}: {ex1}")
                raise

        self._client.get_transport().set_keepalive(self.conn_keepalive)

        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self._client.close()
        self._client = None

    def exec(self, command: str, timeout: typing.Optional[int] = 30, throw: bool = False) -> Tuple[int, str, str]:
        """
        Args:
            command: command string
            timeout: command timeout or None to disable timeouts
            throw: Raise a RuntimeError if true and return code of command !=0

        Returns:
            Tuple[return_code, stdout, stderr]
        """
        try:
            stdin, stdout, stderr = self._client.exec_command(command, timeout=timeout)
            so = stdout.read().decode('utf-8')
            se = stderr.read().decode('utf-8')
            exit_status = stdout.channel.recv_exit_status()
            if logger.isEnabledFor(logging.DEBUG):
                o, e = so.replace("\n", "\\n"), se.replace("\n", "\\n")
                logger.debug(f"{self.username}@{self.address}: {command} -> {exit_status}|{o}|{e}")
            if throw and exit_status != 0:
                raise RuntimeError(f"command {command} exited with {exit_status}, stdout/err {so}, {se}")
            return exit_status, so, se
        except Exception as exc:
            if throw:
                raise exc
            return 1, "", str(exc)

    def exec_commands(self, commands: List[str], timeout: int = 30) -> typing.Tuple[List[str], str]:
        """
        Args:
            commands: List of commands to execute in sequence
            timeout: per-command timeout

        Returns:
            List of collected outputs and a string which if non-empty means the command timed out
        """
        with self.shell_session() as session:
            outputs = []
            for i, command in enumerate(commands):
                try:
                    outputs.append(session.exec(command + '\n', timeout=timeout))
                except TimeoutError:
                    return outputs, f"Command {i} '{command}' timed out after {timeout} seconds"
                logger.debug(f"{self.username}@{self.address}: +{command} -> {outputs[-1]}")
        return outputs, ""

    def shell_session(self) -> ShellSession:
        return ShellSession(self._client, self.buffer_size, name=f"{self.username}@{self.address}")

    def scp_file(self, local_path: str, remote_path: str):
        """
        Copies a file from the local system to the remote system using SCP.

        Args:
            local_path: The path to the local file to copy.
            remote_path: The destination path on the remote server.
        """
        sftp = None
        try:
            logger.debug(f"Copying file from {local_path} to {remote_path} on {self.address}")
            sftp = self._client.open_sftp()
            sftp.put(local_path, remote_path)
        except Exception as e:
            logger.error(f"Failed to copy file from {local_path} to {remote_path}: {e}")
            raise RuntimeError(f"Failed to copy file from {local_path} to {remote_path}: {e}")
        finally:
            if sftp is not None:
                sftp.close()

    def scp_files(self, src_dst_uid_gid_mode_itr: typing.Iterator[Tuple[str, str, int, int, int]]) -> List[str]:
        rv = []
        sftp = None
        try:
            for src, dst, uid, gid, mode in src_dst_uid_gid_mode_itr:
                sftp = self._client.open_sftp()
                sftp.put(src, dst)
                sftp.chown(dst, uid, gid)
                sftp.chmod(dst, mode)
                rv.append(f'{src} -> {dst} {{uid={uid},gid={gid},mode={oct(mode)}}}')
        finally:
            if sftp is not None:
                sftp.close()
        return rv
