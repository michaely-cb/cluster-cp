"""
Commands to be run for L3 deployment validation
"""
import ipaddress
import json
import logging
import os
from collections import defaultdict
from typing import List, Optional, Tuple

from deployment_manager.network_config.cli import SwitchTasks
from deployment_manager.network_config.tasks.switch import SwitchPingGenerator
from deployment_manager.network_config.tasks.system import SystemInterfaceCheckTask
from .command import ToolCommand
from ..common.context import NetworkCfgCtx, OBJ_TYPE_SW, AppCtx
from ..common.task import generate_tasks_for_entity_type, generate_tasks_for_names, run_tasks
from ..tasks.node import NodeInterfaceCheckNetworkTask, NodePingTestNetworkTask
from ..tasks.node_ping import NodePingTestGenerator, PingTestKind
from ..tasks.utils import PingTest

logger = logging.getLogger(__name__)

class SwitchPingCheck:
    """ Run network_config.tool ping check for switches
    """

    def __init__(
            self,
            network_config_file: str,
            names: List[str] = None,
            exclude_nodes: List[str] = None,
            app_ctx: Optional[AppCtx] = None,
    ):
        self._fname = network_config_file
        self._password = os.getenv("SWITCH_PASSWORD")
        self._node_password = os.getenv("NODE_PASSWORD")
        self._exclude_nodes = exclude_nodes
        self._app_ctx = app_ctx
        with open(self._fname) as f:
            self._network_config = json.load(f)
        self._names = names if names is not None else [s['name'] for s in self._network_config.get('switches', [])]


    def run(self) -> Tuple[int, List[dict]]:
        """ Execute ping check on switches
        """
        # update interface states for targets that will be pinged
        spg = SwitchPingGenerator(self._network_config)
        system_ping_targets = list()
        node_ping_targets = list()
        for switch_name in self._names:
            system_ping_targets += spg.get_system_targets(switch_name)
            node_ping_targets += spg.get_node_targets(switch_name)

        system_target_names = list(set(t.dst_name for t in system_ping_targets))
        node_target_names = list(set(t.dst_name for t in node_ping_targets))

        with NetworkCfgCtx(self._fname, app_ctx=self._app_ctx) as ctx:
            system_tasks = generate_tasks_for_names(ctx, SystemInterfaceCheckTask, system_target_names)
            system_check_results = run_tasks(system_tasks)

            node_tasks = generate_tasks_for_names(ctx, NodeInterfaceCheckNetworkTask, node_target_names)
            node_check_results = run_tasks(node_tasks)

            gen = NodePingTestGenerator(ctx.network_config, exclude_down_src_interfaces=True)
            vlan_tasks = generate_tasks_for_names(ctx, NodePingTestNetworkTask, names=node_target_names, generator=gen, test_kinds=[PingTestKind.VLAN])
            vlan_ping_results = run_tasks(vlan_tasks)  # node_name -> NetworkTaskResult[List[PingTest]]
            # switch_name -> list of vlans that failed/succeeded
            vlan_ping_success = defaultdict(list)
            vlan_ping_failure = defaultdict(list)
            gw_addr_sw = {}
            for sw_name in ctx.network_config.get_names_by_type(OBJ_TYPE_SW):
                obj = ctx.network_config.get_raw_object(sw_name)
                if "swarmx_vlan_address" in obj:
                    gw_addr_sw[ipaddress.ip_address(obj["swarmx_vlan_address"].split("/")[0])] = sw_name
                if "address" in obj:
                    gw_addr_sw[ipaddress.ip_address(obj["address"].split("/")[0])] = sw_name
            # if there's any failures, find the dst vlan association with a switch
            for node_name, result in vlan_ping_results.items():
                if not isinstance(result.data, list) or not result.data:
                    continue
                if not result.ok:
                    logger.warning(f"failed to execute ping on {node_name}: {result.message}")
                    continue
                for test in result.data:
                    test : PingTest = test
                    vlan = ctx.network_config.get_vlan_for_ip(test.dst_ip)
                    if vlan.gateway in gw_addr_sw:
                        vlan_ids = vlan_ping_success if test.ok else vlan_ping_failure
                        vlan_ids[gw_addr_sw[vlan.gateway]].append(vlan.number)  # switch -> failed vlan ids

            # ignore systems whose interfaces are down or whose tasks failed
            ignore_interfaces = list()
            for t in system_ping_targets:
                system = ctx.network_config.get_raw_object(t.dst_name)
                task_ok = system_check_results[t.dst_name].ok
                for iface in system.get('interfaces', []):
                    if iface['name'] == t.dst_if and (not task_ok or iface.get('state', 'down') == 'down'):
                        ignore_interfaces.append(f"{t.dst_name}:{t.dst_if}")

            # ignore nodes whose interfaces are down or whose tasks failed
            for t in node_ping_targets:
                node = ctx.network_config.get_raw_object(t.dst_name)
                task_ok = node_check_results[t.dst_name].ok
                for iface in node.get('interfaces', []):
                    if iface['name'] == t.dst_if and (not task_ok or iface.get('state', 'down') == 'down'):
                        ignore_interfaces.append(f"{t.dst_name}:{t.dst_if}")

        ret = 0
        switch_failures = list()
        tc = ToolCommand(
            SwitchTasks,
            dict(
                task='ping_check',
                password=self._password,
                names=self._names,
                ignore_interfaces=ignore_interfaces,
                config=self._fname
            )
        )
        logger.info("Running ping check from switches to nodes, systems and other switches")
        tc.run()
        if not tc.is_successful:
            logger.error(
                f"Switch ping check failed with error code {tc.retcode}: \n{tc.stderr}")
            ret = 1
        else:
            ret, results = tc.retval
            if ret:
                for r in results:
                    if r.get('error'):
                        switch_failures.append(dict(
                            switch=r['switch_name'], error=r['error'], failures=r.get('failures', [])
                        ))

            # process vlan ping check failures
            for sw, failed_vlan_ids in vlan_ping_failure.items():
                # tolerate 1 failure if there were some successes
                success_vlan_ids = vlan_ping_success[sw]
                if (success_vlan_ids and len(failed_vlan_ids) > 1) or (not success_vlan_ids and failed_vlan_ids):
                    switch_failures.append(dict(
                        switch=sw, error=f"pings to this switch's vlans {set(failed_vlan_ids)} failed", failures=[])
                    )

        return ret, switch_failures
