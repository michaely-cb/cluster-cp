import datetime
import logging
import os
from abc import ABCMeta, abstractmethod
from typing import Tuple

from deployment_manager.db.models import QuerySet
from deployment_manager.tools.config import (
    ConfGen,
)
from deployment_manager.tools.const import (CLUSTER_DEPLOYMENT_BASE)
from deployment_manager.tools.utils import exec_cmd

logger = logging.getLogger(__name__)

DEPLOYMENT_SUCCESSFUL = 0
DEPLOYMENT_SKIP = -1
FATAL_EC = 1
REBOOT_WAIT_TIME = 420


class Deploy(metaclass=ABCMeta):
    """ Base class for deployment push buttons
    """

    cwd: str = CLUSTER_DEPLOYMENT_BASE
    env: dict = None
    _targets: list = None

    def __init__(self, profile, write_to_deploy_log:bool=True):
        self.profile = profile
        self._cg = ConfGen(profile)
        self.profile_config = self._cg.parse_profile()
        self.log_file = f"{self._cg.log_dir}/{type(self).name()}.log"
        self.write_to_deploy_log = write_to_deploy_log

    def write_to_log_ts(self, msg):
        self.write_to_log(f"{datetime.datetime.now()}: {msg}")

    def write_to_log(self, msg):
        if not self.write_to_deploy_log:
            return

        with open(self.log_file, "a") as f:
            f.write(str(msg))
            f.write("\n")

    def exec_cmd(self, cmdstr, cwd=None, env=None, stream=False, logcmd=True) -> Tuple[int, str, str]:
        """
            Execute the shell cmd.

            If cwd is None, define from self.cwd
            If env is None, define from self.env
        """
        if cwd is None:
            cwd = self.cwd
        if env is None:
            env = self.env

        self.write_to_log_ts(cmdstr)
        ret, out, err = exec_cmd(
            cmdstr,
            cwd=cwd,
            env=env,
            stream=stream,
            logcmd=logcmd
        )

        self.write_to_log(f"STDOUT:\n{out}")
        self.write_to_log(f"STDERR:\n{err}")
        return ret, out, err

    def exec_func(self, func, args=None, env=None):
        """
        Execute func with args
        Set env variables to env, self.env if env is None
        """
        self.write_to_log_ts(f"{func.__name__} - {args}")
        environ = env or self.env
        if environ:
            orig_environ = dict()
            for k, v in environ.items():
                orig_environ[k] = os.environ.get(k)
                os.environ[k] = v
        try:
            ret = func(args)
            self.write_to_log(ret)
        finally:
            # restore environ
            if environ:
                for k, v in orig_environ.items():
                    if k not in os.environ:
                        continue
                    if v is None:
                        del os.environ[k]
                    else:
                        os.environ[k] = v
        return ret

    def exec_ansible_playbook(self, cmdstr):
        rtn, out, err = self.exec_cmd(f"ansible-playbook {cmdstr}")
        if rtn:
            logger.error(f"Ansible failed.\nExecute 'show logs --logname {self.name()}' for details")
            logger.info("")
            printline = False
            matches = ["TASK", "fatal:", "failed:"]
            for line in out.split("\n"):
                if "PLAY RECAP" in line:
                    printline = True
                if printline or any(x in line for x in matches):
                    logger.info(f"\t{line}")
            return FATAL_EC
        return DEPLOYMENT_SUCCESSFUL

    @abstractmethod
    def name(cls):
        return "deploy"

    # abstracted execute_get_targets because it is required.  precheck and validate are not.
    @abstractmethod
    def execute_get_targets(self, devices: QuerySet, force: bool = False) -> bool:
        """
        Filter the list of devices that is going to be deployed on.
        Args:
            devices: QuerySet of eligible devices
            force: True to force deployment even if the device doesn't match the step's stage

        Returns: True if there are devices to deploy
        """
        return False

    @abstractmethod
    def precheck(self, args):
        """ Run precheck for this push button step
        """
        return DEPLOYMENT_SUCCESSFUL

    @abstractmethod
    def execute(self, args):
        """ Execute this push button
        """
        return DEPLOYMENT_SUCCESSFUL

    @abstractmethod
    def validate(self, args):
        """ Validate deployment part done by this push button
        """
        return DEPLOYMENT_SUCCESSFUL
