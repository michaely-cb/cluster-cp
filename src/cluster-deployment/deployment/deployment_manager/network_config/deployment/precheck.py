"""
Commands to be run as part of 100G deployment precheck
"""
import logging
from typing import List, NamedTuple, Tuple

from ..common.context import NetworkCfgCtx
from ..common.task import generate_tasks_for_entity_type, generate_tasks_for_names, run_tasks
from ..tasks.node import NodeInterfaceCheckNetworkTask, NodeInterfaceDiscoverNetworkTask
from ..tasks.switch import SwitchFirmwareVersionTask, SwitchModelTask

logger = logging.getLogger(__name__)


class PrecheckOutput(NamedTuple):
    device: str
    error: str


class NodeInterfaceStatusCheck:
    """ Check status of 100g interfaces on all nodes
    TODO: this should fail individual devices and also check the expected number of interfaces is up for a given
    device and not just one or all
    """

    def __init__(self, ctx: NetworkCfgCtx):
        self.ctx = ctx
        self._at_least_one_iface = ["management_nodes", "worker_nodes", "user_nodes"]
        self._all_ifaces = ["memoryx_nodes", "swarmx_nodes", "activation_nodes"]


    def run(self) -> Tuple[int, List[PrecheckOutput]]:
        """ Run ethtool command on all nodes and parse output
        """
        exit_code = 0
        output = list()

        if_discover_tasks = generate_tasks_for_entity_type(self.ctx, NodeInterfaceDiscoverNetworkTask)
        if_discover_results = run_tasks(if_discover_tasks)
        ok_devices, err_devices = [], []
        for name, result in if_discover_results.items():
            bin = ok_devices if result.ok else err_devices
            bin.append(name)

        if_check_tasks = generate_tasks_for_names(self.ctx, NodeInterfaceCheckNetworkTask, names=ok_devices)
        if_check_results = run_tasks(if_check_tasks)
        ok_devices = []
        for name, result in if_check_results.items():
            bin = ok_devices if result.ok else err_devices
            bin.append(name)

        nw_cfg = self.ctx.network_config.raw()
        ok_devices = set(ok_devices)

        for name in err_devices:
            output.append(PrecheckOutput(device=name, error="task execution failed"))

        # Mgmt, user and worker nodes need at least one 100G interface to be up.
        # Other nodes need all 100G interfaces to be up
        for ncat in self._at_least_one_iface:
            for n in nw_cfg.get(ncat, []):
                if n["name"] not in ok_devices:
                    continue
                iface_up = False
                for i in n.get("interfaces", []):
                    if i.get("state") != "down":
                        iface_up = True
                        break
                if not iface_up:
                    logger.error(f"All 100G interfaces on node {n['name']} are down")
                    output.append(PrecheckOutput(
                        device=n['name'],
                        error="All 100G interfaces are down"
                    ))
                    exit_code = 1

        for ncat in self._all_ifaces:
            for n in nw_cfg.get(ncat, []):
                if n["name"] not in ok_devices:
                    continue
                for i in n.get("interfaces", []):
                    if i.get("state") == "down":
                        logger.error(f"{i['name']} on node {n['name']} is down")
                        output.append(PrecheckOutput(
                            device=n['name'],
                            error=f"{i['name']} is down"
                        ))
                        exit_code = 1
        return exit_code, output


class SwitchVersionAndModelCheck:
    """ Check if switch OS versions match PoR version
    """
    def __init__(self, ctx: NetworkCfgCtx):
        self._ctx = ctx

    def run(self):
        output = list()
        ok_switches = self._ctx.network_config.get_switch_names()

        for cls in [SwitchModelTask, SwitchFirmwareVersionTask]:
            results = run_tasks(generate_tasks_for_names(self._ctx, cls, names=ok_switches))
            for name, result in results.items():
                if not result.ok:
                    logger.error(f"{cls.__class__} failed: {result.message}")
                    output.append(PrecheckOutput(
                        device=name,
                        error=result.message
                    ))
                    ok_switches.remove(name)

        for switch_name in ok_switches:
            obj = self._ctx.network_config.get_raw_object(switch_name)
            ctl = self._ctx.get_switchctl(switch_name)
            if not ctl.model_support().is_supported_model(obj.get('model')):
                logger.error(f"{switch_name} is an unsupported switch model {obj.get('model')}")
                output.append(PrecheckOutput(
                    device=switch_name,
                    error=(
                        f"Unsupported switch model {obj.get('model')}. "
                        f"Supported models are {','.join(ctl.model_support().supported_models())}"
                    )
                ))
            if not ctl.model_support().is_supported_firmware(obj.get('model'), obj.get('firmware_version')):
                msg = f"Unsupported fw version {obj.get('firmware_version')} for switch {switch_name} model {obj.get('model')}"
                logger.error(msg)
                output.append(PrecheckOutput(
                    device=switch_name,
                    error=(
                        msg +
                        f" Supported firmwares are {','.join(ctl.model_support().firmwares_for_model(obj.get('model')))}. "
                        f"Running ZTP on this switch might help"
                    )
                ))

        return 1 if len(output) > 0 else 0, output
