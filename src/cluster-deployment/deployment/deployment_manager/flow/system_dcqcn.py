import concurrent
import tempfile
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List

from deployment_manager.flow.task import Task, FlowContext, FlowStatus
from deployment_manager.network_config.configs import SystemNetworkConfigWriter
from deployment_manager.tools.network_config_tool import NetworkConfigTool
from deployment_manager.tools.system import SystemCtl


class Preflight(Task):
    """
    Ensure the systems which do not require update are skipped and that the minimum image version requirement is met
    """

    def __init__(self, ctx: FlowContext, sysctl: SystemCtl, target_state: bool):
        self._ctx = ctx
        self._target_state = target_state
        self._sysctl = sysctl

    def run(self):
        try:
            if self._target_state == self._sysctl.is_dcqcn_enabled():
                self._ctx.update_state(self._sysctl.hostname, self.name, FlowStatus.Success,
                                       f"skip at preflight because enabled={self._target_state}")
                return
            _, build_id = self._sysctl.get_version()
            if self._target_state:
                earliest_possible_dcqcn_build = "202410071135"  # rough sanity check
                if build_id.split("-")[0] < earliest_possible_dcqcn_build:
                    self._ctx.update_state(
                        self._sysctl.hostname, self.name, FlowStatus.Fail,
                        f"system is running with build id {build_id} which is less "
                        f"than {earliest_possible_dcqcn_build}. Please update the system software to enable")
                else:
                    self._ctx.update_state(self._sysctl.hostname, self.name, FlowStatus.Progressing, "needs enable")
            else:
                self._ctx.update_state(self._sysctl.hostname, self.name, FlowStatus.Progressing, "needs disable")
        except Exception as e:
            self._ctx.update_state(self._sysctl.hostname, self.name, FlowStatus.Fail, f"{e}")

    def __str__(self):
        return f"{self.name}[{self._sysctl.hostname}]"


class ConfigureSystemsTask(Task):
    """ Update the network config on the system """

    def __init__(self, ctx: FlowContext, sysctls: List[SystemCtl], enable: bool):
        self._ctx = ctx
        self._sysctls = {s.hostname: s for s in sysctls}
        self._enable = enable

    def run(self):
        network_cfg = NetworkConfigTool.for_profile(self._ctx.profile).nw_config()
        tmp_dir = tempfile.mkdtemp(prefix="system_nw_configs")
        sys_config_file = {}
        for system_name in self._ctx.get_progressing_devices():
            try:
                files = SystemNetworkConfigWriter(network_cfg, system_name, force_dcqcn=self._enable).write_config(tmp_dir)
                sys_config_file[system_name] = files[0]
            except Exception as e:
                self._ctx.update_state(system_name, self.name, FlowStatus.Fail, f"cfg gen failed: {e}")

        with ThreadPoolExecutor(16) as executor:
            futures = {}
            for system_name, config_file in sys_config_file.items():
                update_config_fn = self._sysctls[system_name].update_network_config
                futures[executor.submit(update_config_fn, config_file)] = system_name

            done, _ = concurrent.futures.wait(list(futures.keys()), return_when=concurrent.futures.ALL_COMPLETED)

            for future, system_name in futures.items():
                try:
                    ok, msg = future.result()
                    if ok:
                        self._ctx.update_state(system_name, self.name, FlowStatus.Progressing)
                    else:
                        self._ctx.update_state(system_name, self.name, FlowStatus.Fail, f"failed to post network-cfg.json: {msg}")
                except Exception as e:
                    self._ctx.update_state(system_name, self.name, FlowStatus.Fail, f"failed to post network-cfg.json: {e}")

    def __str__(self):
        return f"{self.name}"
