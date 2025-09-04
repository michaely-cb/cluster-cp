"""
Network deployment command execution utilities
"""
import argparse
import contextlib
import io
import logging
import subprocess
import sys
import traceback

import paramiko

from typing import List, Dict

from deployment_manager.network_config.schema import NetworkConfigSchema

def exec_cmd(cmd, env=None):
    """ Execute shell command
    """
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            env=env
        )
        stdout, stderr = proc.communicate()
        ret = (
            proc.returncode, stdout.decode('utf-8'), stderr.decode('utf-8')
        )
        return ret
    except Exception: # pylint: disable=broad-except
        return (-1, "", "")

def exec_remote_cmd(cmd, host, username, password, timeout=5):
    """ Execute command on remote shell using ssh
    """
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(
            paramiko.client.AutoAddPolicy())
    try:
        ssh.connect(host, port=22, username=username,
                    password=password, timeout=timeout,
                    allow_agent=False, look_for_keys=False)
        _, stdout, stderr = ssh.exec_command(cmd)
        ret = stdout.channel.recv_exit_status()
        return ret, stdout.read().decode('utf-8'), stderr.read().decode('utf-8')
    except Exception as exc:
        return 1, "", str(exc)
    finally:
        ssh.close()


class ToolBase:
    """ Base class to interact with network_config.tool
    """
    def __init__(self, cmd, args):
        self._cmd = cmd
        self._args = args
        self.retcode = None
        self.stderr = None
        self.retval = None

    def __repr__(self):
        return f"{self._cmd.__name__} - {self._args}"

    @property
    def name(self):
        return self._cmd.__name__

    @property
    def is_successful(self):
        return self.retcode == 0

    def run(self):
        pass


class ToolCommand(ToolBase):
    """ Class to run network_config.tool commands
    """
    def run(self):
        """ Call self._cmd with self._args
        """
        try:
            self.retval = self._cmd()(argparse.Namespace(**self._args))
            self.retcode = 0
        except:
            self.stderr = traceback.format_exc()
            self.retcode = 1


class ToolDeviceAdder(ToolBase):
    """
    Class to add devices to network_config.json via network_config.schema functions.
    This helps with batch addition of devices
    """
    def __init__(self, network_config_file: str, cmd_name: str, device_args: List[Dict]):
        self._nw_schema = NetworkConfigSchema()
        cmd_fn = getattr(self._nw_schema, cmd_name)
        super().__init__(cmd_fn, device_args)
        self._network_config_file = network_config_file

    def run(self):
        nw_config = self._nw_schema.load_json(self._network_config_file)
        try:
            for device in self._args:
                self._cmd(network_config=nw_config, validate=False, **device)
            self.retcode = 0
        except:
            self.stderr = traceback.format_exc()
            self.retcode = 1
        else:
            self._nw_schema.save_json(nw_config, self._network_config_file)
