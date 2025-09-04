#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

""" SSH based tasks

"""

import functools
import inspect
import io
import logging
import os
import pathlib
import re
import socket
import sys
import threading
import time
import traceback
import types
from abc import ABCMeta, abstractmethod
from contextlib import AbstractContextManager
from typing import Callable, Iterator, Tuple

import paramiko

from .task import _TaskBase
from .utils import clean_buffer
from ..schema import NetworkConfigSchema, ObjContact

logger = logging.getLogger(__name__)


logger = logging.getLogger(__file__)


class SSHLoginConnector(AbstractContextManager):
    """ Manage an SSH connection context
    """
    timeout = 30
    keepalive = timeout // 10

    def __init__(self, obj, username, password):
        self.address = str(ObjContact(obj))
        self.username = str(username)

        if password:
            self.password = str(password)
        else:
            self.password = None

        # To avoid verbose ssh logging when root logger is set to debug
        logging.getLogger("paramiko").setLevel(logging.WARNING)

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())

    def _try_connect(self):
        # try 3 ways: password if provided, passwordless, and with no auth if no password provided
        ssh_args = dict(hostname=self.address, username=self.username, allow_agent=False, timeout=self.timeout)

        last_ex, methods = None, []
        if self.password:
            try:
                self.ssh_client.connect(**ssh_args, password=self.password, look_for_keys=False)
                return None
            except (paramiko.SSHException, socket.error) as ex:
                # root password may have been removed from the device - try passwordless login next
                last_ex = ex
                methods.append("password")

        try:
            self.ssh_client.connect(**ssh_args, look_for_keys=True)
            return self.ssh_client
        except (paramiko.SSHException, socket.error) as ex:
            logger.debug(f"SSH failed to connect to {self.address} using password-less")
            last_ex = ex
            methods.append("passwordless")

        if not self.password:
            # finally, resort to passwordless login
            try:
                self.ssh_client.get_transport().auth_none(username=self.username)
                return None
            except (paramiko.SSHException, socket.error) as ex:
                methods.append("no-auth")

        logger.warning(f"SSH failed to connect to {self.address} using methods: {','.join(methods)}")
        raise last_ex


    def __enter__(self) -> paramiko.SSHClient:
        self._try_connect()
        self.ssh_client.get_transport().set_keepalive(self.keepalive)
        return self.ssh_client

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.ssh_client.close()
        return False


class SSHPrintPost:
    """ Output the results of an ssh call to stdout
    """

    def __init__(self, obj, err_only=False):
        self.name = obj["name"]
        self._err_only = err_only

    def __call__(self, result):
        stdout, stderr, exit_status = result

        if not self._err_only or exit_status:
            logger.info('-----------------------------------------------------------')
            logger.info(f'{self.name}: EXIT STATUS: {exit_status}')

            for out_name, out_src in (("STDERR", stderr), ("STDOUT", stdout)):
                for line in out_src.decode('utf-8').splitlines():
                    logger.info(f'{self.name}: {out_name}: {line.rstrip()}')


class SSHPrintHandler:
    """ Output the exception raised
    """

    def __init__(self, obj):
        self.name = obj["name"]

    def __call__(self, exception):
        logger.info('-----------------------------------------------------------')
        if isinstance(exception, socket.timeout):
            logger.error(f'{self.name}: TIMEOUT')
        elif isinstance(exception,
                        paramiko.ssh_exception.NoValidConnectionsError):
            logger.error(f'{self.name}: REFUSED OR UNREACHABLE')
        else:
            logger.error(f'{self.name}: EXCEPTION:', exc_info=exception)


class SSHExitHandler:
    """ Output the exception raised and re-raise
    """

    def __init__(self, obj):
        self.name = obj["name"]

    def __call__(self, exception):
        traceback.print_exception(type(exception),
                                  value=exception,
                                  tb=exception.__traceback__,
                                  file=sys.stderr)
        raise exception


class _SSHCommandABC(metaclass=ABCMeta):
    """ Abstract base class for ssh commands that can be bundled into a task
    """

    def __call__(self) -> Tuple[str, str, int]:
        if not self.command:
            return '', '', 0,
        with self.connection() as ssh_client:
            _, stdout, stderr = ssh_client.exec_command(self.command)
            exit_status = stdout.channel.recv_exit_status()

            return stdout.read(), stderr.read(), exit_status

    @property
    @abstractmethod
    def command(self):
        """ Provide the command to run
        """

    @abstractmethod
    def connection(self):
        """ Provide the ssh connection context
        """


class SSHCommandBase(_SSHCommandABC):
    """ Base class for commands associated with objects
    """

    @property
    @staticmethod
    def command():
        """ Command to run
        """
        return ''

    def __init__(self, obj, username, password, verbose=False):
        self._obj = obj
        self._connection = SSHLoginConnector(obj, username, password)
        self._verbose = verbose

    def connection(self):
        return self._connection

    def __call__(self):
        if self._verbose:
            logger.info(f"Running {self.command} on {self.connection().address}")
        return super().__call__()


class SSHCommand(SSHCommandBase):
    """ Send an SSH command
    """

    def __init__(self, obj, username, password, command):
        super().__init__(obj, username, password)
        self._command = str(command)

    @property
    def command(self):
        return self._command


class _SSHBatchCommandsABC(metaclass=ABCMeta):
    """ Abstract base class for ssh batch commands that can be
    bundled into a task
    """

    def __call__(self):
        retlist = list()
        with self.connection() as ssh_client:
            for cmd in self.commands:
                _, stdout, stderr = ssh_client.exec_command(cmd)
                exit_status = stdout.channel.recv_exit_status()
                retlist.append((stdout.read(), stderr.read(), exit_status,))

        return retlist

    @property
    @abstractmethod
    def commands(self):
        """ Provide the commands to run
        """

    @abstractmethod
    def connection(self):
        """ Provide the ssh connection context
        """


class SSHBatchCommandsBase(_SSHBatchCommandsABC):
    """ Base class for batch commands associated with objects
    """

    @property
    @staticmethod
    def commands():
        """ Commands to run
        """
        return ()

    def __init__(self, obj, username, password, verbose=False):
        self._obj = obj
        self._connection = SSHLoginConnector(obj, username, password)
        self._verbose = verbose

    def connection(self):
        return self._connection

    def __call__(self):
        if self._verbose:
            for cmd in self.commands:
                logger.info(f"Running {cmd} on {self.connection().address}")
        return super().__call__()


SSHCmdRunner = Callable[[SSHLoginConnector, Iterator[str], int], Tuple[bytes, bytes, int]]


class _SSHBatchShellABC(metaclass=ABCMeta):
    """Abstract base class for batch shell sessions that can be bundled
    into a task.
    """

    read_buf_size = 4096

    def __call__(self):
        return self.get_cmd_runner()(self.connection(), self.input_iter, self.read_buf_size)

    def get_cmd_runner(self) -> SSHCmdRunner:
        return run_ssh_commands

    @property
    @abstractmethod
    def input_iter(self):
        """ Provide an iterator for lines to enter into the shell
        """

    @abstractmethod
    def connection(self) -> SSHLoginConnector:
        """ Provide the ssh connection context
        """


def run_ssh_commands(
        conn: SSHLoginConnector, input_iter: Iterator[str], read_buf_size=4096
) -> Tuple[bytes, bytes, int]:
    """
    Runs SSH commands. Makes no attempt at discovering if the previously submitted command was actually processed
    """
    stdout_buf = io.BytesIO()
    stderr_buf = io.BytesIO()
    exit_status = -1

    def out_reader(read_call, io_buf):
        while True:
            buf = read_call(read_buf_size)
            if len(buf) == 0:
                break
            io_buf.write(buf)

    if input_iter is None:
        return b"", b"", 0

    with conn as ssh_client:
        shell_session = ssh_client.invoke_shell()

        try:
            readers = (
                threading.Thread(target=out_reader, args=(shell_session.recv, stdout_buf)),
                threading.Thread(target=out_reader, args=(shell_session.recv_stderr, stderr_buf))
            )

            for reader in readers:
                reader.start()

            try:
                for input_item in input_iter:
                    sent_total = 0
                    while sent_total < len(input_item):
                        sent = shell_session.send(input_item[sent_total:])
                        if sent == 0:
                            raise RuntimeError("Connection closed prematurely")
                        sent_total += sent

                shell_session.shutdown_write()
                exit_status = shell_session.recv_exit_status()

            finally:
                for reader in readers:
                    reader.join()

        finally:
            shell_session.close()

    return stdout_buf.getvalue(), stderr_buf.getvalue(), exit_status


def run_ssh_commands_await_prompt(
        conn: SSHLoginConnector, input_iter: Iterator[str], read_buf_size=4096
) -> Tuple[bytes, bytes, int]:
    """
    Runs SSH commands, waiting for some clue that the last command has finished processing: the prompt has returned.
    """
    last_cmd = [""]
    out_buffer = [""]

    stdout_buf = io.BytesIO()
    stderr_buf = io.BytesIO()
    exit_status = -1
    prompt_event = threading.Event()
    cmd_lock = threading.Lock()

    def stdout_reader(read_call, io_buf):
        while True:
            buf = read_call(read_buf_size)
            if len(buf) == 0:
                break
            io_buf.write(buf)

            with cmd_lock:
                out = clean_buffer(out_buffer[0] + buf.decode("utf-8"))
                last_line = out.rsplit("\n", 1)[-1]
                # detect the prompt. TODO: set PS1 for consistency
                if re.match(r"^.*[>#$:\]] ?$", last_line):
                    out_buffer[0] = ""
                    prompt_event.set()
                else:
                    out_buffer[0] = last_line

    def stderr_reader(read_call, io_buf):
        while True:
            buf = read_call(read_buf_size)
            if len(buf) == 0:
                break
            io_buf.write(buf)

    if input_iter is None:
        return b"", b"", 0

    with conn as ssh_client:
        shell_session = ssh_client.invoke_shell()

        try:
            readers = (
                threading.Thread(target=stdout_reader, args=(shell_session.recv, stdout_buf)),
                threading.Thread(target=stderr_reader, args=(shell_session.recv_stderr, stderr_buf))
            )

            for reader in readers:
                reader.start()

            try:
                for input_item in input_iter:
                    sent_total = 0

                    # Give the switch a max of 10 seconds to process the command before issuing the next one
                    # The switch should echo back the command issued when it has finished processing.
                    # Special case is the final exit which will not produce a prompt after issuing. To avoid waiting
                    # on the final exit, skip the prompt wait altogether under the assumption that exit returns quickly
                    clear_out_buffer = False
                    if last_cmd[0] != "exit" and not prompt_event.wait(10):
                        dbg = f"Warning {conn.address} prompt did not appear after 10s. "
                        dbg += "Command may be stalled or switch takes too long to process. "
                        dbg += "Future commands may fail!\nDebug info:\n"
                        dbg += f"output_buffer: '{out_buffer[0]}'\n"
                        dbg += f"command: '{last_cmd[0]}'\n"
                        dbg += f"output_buffer_hex: " + (' '.join(f"{ord(c):02x}" for c in out_buffer[0])) + "\n"
                        dbg += f"command_hex: " + (' '.join(f"{ord(c):02x}" for c in last_cmd[0]))
                        logger.info(dbg)
                        clear_out_buffer = True
                    elif last_cmd[0] == "exit":
                        time.sleep(0.1)

                    with cmd_lock:
                        cmd_text = (input_item.decode() if isinstance(input_item, bytes) else input_item).strip()
                        last_cmd[0] = cmd_text.rsplit("\n", 1)[-1]  # newline is a cmd separator, take the last cmd
                        prompt_event.clear()
                        if clear_out_buffer:
                            out_buffer[0] = ""  # prevent buffer from growing too large

                    while sent_total < len(input_item):
                        sent = shell_session.send(input_item[sent_total:])
                        if sent == 0:
                            raise RuntimeError("Connection closed prematurely")
                        sent_total += sent

                shell_session.shutdown_write()
                exit_status = shell_session.recv_exit_status()

            finally:
                for reader in readers:
                    reader.join()

        finally:
            shell_session.close()

    so, se =  stdout_buf.getvalue(), stderr_buf.getvalue()
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(so.decode())
        logger.debug(se.decode())
    return so, se, exit_status

def dry_run_ssh_commands(
        conn: SSHLoginConnector, input_iter: Iterator[str], read_buf_size=4096
) -> Tuple[bytes, bytes, int]:
    lines = [line.encode() for line in input_iter]
    return b"\n".join(lines), b"", 0


class SSHBatchShellBase(_SSHBatchShellABC):
    """ SSH batch shell base class
    """

    @property
    @staticmethod
    def commands():
        """ List of commands to run
        """
        return ()

    def __init__(self, obj, username, password, verbose=False):
        self._obj = obj
        self._connection = SSHLoginConnector(obj, username, password)
        self._verbose = verbose

    def connection(self):
        return self._connection

    @property
    def input_iter(self):
        for command in self.commands:
            if self._verbose:
                logger.info(f"Running {command} on {self._obj['name']}")
            yield command + "\n"


class SSHBatchShellCommands(SSHBatchShellBase):
    """ Send batch shell commands
    """

    def __init__(self, obj, username, password, commands, verbose=False):
        super().__init__(obj, username, password, verbose)

        self._commands = list(commands)

    @property
    def commands(self):
        return self._commands


class SSHBatchShellFile(_SSHBatchShellABC):
    """Send batch shell command from a file like or other line oriented
    iterable object.
    """

    def __init__(self, obj, username, password, input_obj):
        self._obj = obj
        self._connection = SSHLoginConnector(obj, username, password)
        self._input_obj = iter(input_obj)

    def connection(self):
        return self._connection

    @property
    def input_iter(self):
        return self._input_obj


class _SSHUploadABC(metaclass=ABCMeta):
    """ Abstract base class for recursive file uploads that can be bundled
    into a task.
    """

    def __call__(self):
        outbuf = bytes()

        with self.connection() as ssh_client:
            with ssh_client.open_sftp() as sftp_obj:
                for src, dest, owner, group, mode in self.files_iter:
                    sftp_obj.put(src, dest)
                    sftp_obj.chown(dest, owner, group)
                    sftp_obj.chmod(dest, mode)

                    outbuf += (f'{src} -> {dest} ' +
                               f'U({owner}) G({group}) ' +
                               f'M({oct(mode)})\n').encode('ASCII')

        return (outbuf, bytes(), 0)

    @property
    @abstractmethod
    def files_iter(self):
        """ Return an iterator that returns a tuple for each file upload.

        (source file, dest file, file owner, file group, file mode)
        """

    @abstractmethod
    def connection(self):
        """ Provide the ssh connection context
        """


def walk_conf_files(cfg_dir: str) -> Iterator[Tuple[str, str, int, int, int]]:
    real_path = os.path.realpath(cfg_dir)

    for dirpath, _dirnames, filenames in os.walk(real_path):
        dest_dir = dirpath[len(real_path):]
        for filename in filenames:
            if filename.startswith(".") or filename.startswith("#") or \
                    filename.endswith("~"):
                continue

            src = os.path.join(dirpath, filename)
            dest = os.path.join(dest_dir, filename)
            mode = os.stat(src).st_mode & 0o777
            yield src, dest, 0, 0, mode


class SSHUploadBase(_SSHUploadABC):
    """ Base class for uploads from a base directory
    """

    def __init__(self, obj, username, password, base_dir, section, verbose=False):
        self._obj = obj
        self._config_dir = os.path.join(base_dir, section, obj["name"])
        self._section = section
        self._verbose = verbose

        if not os.path.exists(self._config_dir):
            raise RuntimeError(
                f'Configuration dir does not exist: {self._config_dir}')

        self._connection = SSHLoginConnector(obj, username, password)

    def connection(self):
        return self._connection

    @property
    def files_iter(self):
        for src, dest, uid, gid, mode in walk_conf_files(self._config_dir):
            if self._verbose:
                logger.info(f"Copying {src} to {dest} on {self._obj['name']}")
            yield src, dest, uid, gid, mode


class SSHCommandTaskBase(_TaskBase):
    """ Base class for tasks that use ssh commands
    """
    task_alias = None
    compat_sections = ()

    def __init__(self, command, post, handler):
        for callout in command, post, handler:
            if not callable(callout):
                raise ValueError(f'Not callable: {type(callout).__name__}')

        self._command = command
        self._post = post
        self._handler = handler

    def __call__(self, *args, **kwargs):
        return self._command(*args, **kwargs)

    def get_cmd(self):
        cmds = []
        for c in self._command.commands:
            cmds.append(c)
        return cmds

    def post(self, result):
        return self._post(result)

    def handler(self, exception):
        return self._handler(exception)


class MakePrintingSSHTask:
    """Decorator to make a task that prints its results or exception from
    a command.
    """

    def __init__(self, alias, sections, name=None,
                 err_only=False, exc_exit=False):
        self.alias = alias
        self.compat_sections = tuple(sections)
        self.name = name
        self.err_only = err_only
        self.exc_exit = exc_exit

    def __call__(self, cls):
        if not inspect.isclass(cls):
            raise ValueError(
                f'Non-class object provided: {type(cls).__name__}')

        @functools.wraps(getattr(cls, '__init__'))
        def new_init(instance, obj, *args, **kwargs):
            command = cls(obj, *args, **kwargs)
            post = SSHPrintPost(obj, self.err_only)
            if self.exc_exit:
                handler = SSHExitHandler(obj)
            else:
                handler = SSHPrintHandler(obj)

            super(instance.__class__, instance).__init__(
                command, post, handler)

        cls_update = dict(__init__=new_init,
                          task_alias=self.alias,
                          compat_sections=self.compat_sections)

        if self.name is None:
            cls_name = cls.__name__

        else:
            cls_name = self.name

        new_cls = types.new_class(cls_name,
                                  bases=inspect.getmro(SSHCommandTaskBase),
                                  exec_body=lambda x: x.update(cls_update))
        functools.update_wrapper(new_cls, cls, updated=())

        return new_cls


class SSHTaskGeneratorBase:
    """ Base class for generators of ssh tasks to feed to the runner
    """
    sections = NetworkConfigSchema.host_sections

    task_classes = ()

    def __init__(self, network_config, task_alias, username, password=None,
                 task_args=None, task_kwargs=None, filter_fn=None,
                 verbose=False):
        self.network_config = network_config
        self.task_alias = str(task_alias)
        self.username = str(username)

        if password:
            self.password = str(password)
        else:
            self.password = None

        if not task_alias:
            raise ValueError('Task alias must be provided')

        if task_args is None:
            self.task_args = tuple()
        else:
            self.task_args = tuple(task_args)

        if task_kwargs is None:
            self.task_kwargs = dict()
        else:
            self.task_kwargs = dict(task_kwargs)

        if filter_fn is None:
            self.filter_fn = lambda _section, _obj: True
        else:
            self.filter_fn = filter_fn

        if not callable(self.filter_fn):
            raise ValueError('Filter must be callable')

        filter_signature = inspect.signature(self.filter_fn)
        if len(filter_signature.parameters) != 2:
            raise ValueError('Filter function must take two parameters')

        self.verbose = verbose

    def __iter__(self):
        for section in self.sections:
            for obj in self.network_config.get(section, list()):
                if not self.filter_fn(section, obj):
                    continue

                for task_cls in self.task_classes:
                    if self.task_alias != getattr(task_cls, 'task_alias', ''):
                        continue

                    if section not in getattr(task_cls, 'compat_sections', ()):
                        continue

                    task_signature = inspect.signature(
                        getattr(task_cls, '__init__'))

                    call_args = self.task_args
                    call_kwargs = self.task_kwargs

                    call_kwargs['obj'] = obj
                    call_kwargs['username'] = obj.get('username') or self.username
                    call_kwargs['password'] = obj.get('password') or self.password

                    if 'network_config' in task_signature.parameters:
                        call_kwargs['network_config'] = self.network_config

                    if 'section' in task_signature.parameters:
                        call_kwargs['section'] = section

                    yield task_cls(*call_args, **call_kwargs)


class SSHReadFiles:

    def __init__(self, obj, username, password, filenames, delete=False):
        self._obj = obj
        self._connection = SSHLoginConnector(obj, username, password)
        self._filenames = filenames
        self._delete = delete

    def __call__(self) -> dict:
        """ Returns filename -> contents """
        lines = dict()
        with self._connection as ssh_client:
            sftp_client = ssh_client.open_sftp()
            for fname in self._filenames:
                remote_file = sftp_client.open(fname)
                try:
                    lines[fname] = remote_file.readlines()
                except:
                    logger.error(f"Unable to read {fname}")
                finally:
                    remote_file.close()
                    if self._delete:
                        ssh_client.exec_command(f"rm -f {fname}")
        return lines
