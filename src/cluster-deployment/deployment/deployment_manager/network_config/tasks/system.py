import json
import logging
from typing import Dict, List
from deployment_manager.network_config.common.task import NetworkTask, NetworkTaskResult, TargetSystems, ConfigTask
from deployment_manager.network_config.common.context import NetworkCfgDoc
from deployment_manager.network_config.common import SYSTEM_INTERFACE_COUNT
from deployment_manager.network_config.deployment.base import get_obj_name
logger = logging.getLogger(__name__)

def update_system_port_switch(network_config: NetworkCfgDoc, names: List[str] = ()):
    """ Update the switch association of a system's ports by inspecting system_connections """
    def _parse_system_connections(cfg) -> Dict[str, Dict[int, str]]:
        """
        Parses system LLDP information to return a dict of the format
        { system_name: { port_num: switch_name }
        """
        conns = dict()
        for sc in cfg.raw().get('system_connections', []):
            if not conns.get(sc['system_name']):
                conns[sc['system_name']] = dict()
            # system_port is of the format 'Port X' where X is a
            # number from 1 through num_ports. The
            # code which uses _parse_system_connections expects a
            # number from 0 through (max_system_ports - 1).
            # For HPE switches, MAC address is reported instead of
            # 'Port X'. The last nibble of the MAC is the port number
            # in hex
            if "Port" in sc['system_port']:
                port = int(sc['system_port'].split()[-1]) - 1
            else:
                port = int(sc['system_port'][-1], 16) - 1
            conns[sc['system_name']][port] = sc['switch_name']
        return conns

    system_connections = _parse_system_connections(network_config)

    if not system_connections and network_config.raw().get("systems"):
        raise ValueError("No system connection information found. Are systems connected? Has switch LLDP been run?")

    sys = [
        s for s in network_config.raw().get("systems", [])
        if not names or s['name'] in names
    ]

    for system_obj in sys:
        num_ports = network_config.get_system_expected_ports(system_obj['name'])
        for iface_idx in range(num_ports):
            switch_name = system_connections.get(system_obj['name'], {}).get(iface_idx)
            if not switch_name:
                raise ValueError(
                    f"Connection information for port {iface_idx} "
                    f"for system {system_obj['name']} not found."
                )
            if "interfaces" not in system_obj:
                system_obj["interfaces"] = list()

            for iface_base in ("control", "data"):
                iface_name = iface_base + str(iface_idx)

                for iface_obj in system_obj["interfaces"]:
                    if iface_obj["name"] == iface_name:
                        iface_obj["switch_name"] = switch_name
                        break
                else:  # for iface_obj in ...
                    system_obj["interfaces"].append(dict(
                        name=iface_name,
                        switch_name=switch_name
                    ))

class SystemModelTask(TargetSystems, NetworkTask):
    """ Update network model with system model number """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        with self._ctx.get_conn(self.name) as conn:
            _, stdout, _ = conn.exec('system show --output-format json', throw=True)
        return NetworkTaskResult.ok(json.loads(stdout))

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        doc: dict = result.data
        if not isinstance(doc.get("systemType"), str):
            return NetworkTaskResult.error(f"'systemType' field not in system show response: {doc}")
        nw_doc = self._ctx.network_config
        nw_doc.update_raw_object(self.name, {"model": doc["systemType"].lower()})
        return NetworkTaskResult.ok()

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        # check if the system already has a model field defined, and if so, ignore since this never changes
        obj = self._ctx.network_config.get_raw_object(self.name)
        if obj.get("model", ""):
            logger.warning(f"{self.name} failed to fetch model but model is already populated so not exiting")
            return NetworkTaskResult.ok()
        return result

class SystemVersionTask(TargetSystems, NetworkTask):
    """ Update network model with system software version """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        ctl = self._ctx.get_systemctl(self.name)
        vers, _ = ctl.get_version()
        return NetworkTaskResult.ok(vers)

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        vers = result.data
        if not vers:
            return NetworkTaskResult.error(f"Unable to retrive system version")
        nw_doc = self._ctx.network_config
        nw_doc.update_raw_object(self.name, {"version": vers})
        return NetworkTaskResult.ok()

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        # check if the system already has a model field defined, and if so, ignore since this never changes
        obj = self._ctx.network_config.get_raw_object(self.name)
        if obj.get("version", ""):
            logger.warning(f"{self.name} failed to fetch version but version is already populated so not exiting")
            return NetworkTaskResult.ok()
        return result

class SystemPortDiscoverTask(TargetSystems, NetworkTask):
    """ Update network model with system port details """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        ctl = self._ctx.get_systemctl(self.name)
        return NetworkTaskResult.ok(data=ctl.get_network_show())

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        ports = []
        for n in result.data.get("data", []):
            if not n['ifaceName'].startswith('N'):
                continue
            port_num = int(n['ifaceName'].split('N')[1])+1
            ports.append(dict(
                port_num=port_num,
                mac=n['macAddress']
            ))
        self._ctx.network_config.update_raw_object(self.name, {'ports': ports})
        return NetworkTaskResult.ok()

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        obj = self._ctx.network_config.get_raw_object(self.name)
        if obj and len(obj.get("ports", [])) > 0:
            logger.warning(f"{self.name} failed to fetch ports but ports are already populated so not exiting")
            return NetworkTaskResult.ok()
        return result

class SystemInterfaceCheckTask(TargetSystems, NetworkTask):
    """ Update network model with system port status """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        ctl = self._ctx.get_systemctl(self.name)
        return NetworkTaskResult.ok(data=ctl.get_network_show())

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        obj = self._ctx.network_config.get_raw_object(self.name)
        if not obj:
            return NetworkTaskResult.ok()

        interfaces = obj.get('interfaces', [])
        for n in result.data.get("data", []):
            if not n['ifaceName'].startswith('N'):
                continue
            port_num = int(n['ifaceName'].split('N')[1])
            for iface in interfaces:
                if iface['name'] == f'data{port_num}' or iface['name'] == f'control{port_num}':
                    iface['state'] = n['linkState'].lower()

        self._ctx.network_config.update_raw_object(self.name, {'interfaces': interfaces})
        return NetworkTaskResult.ok()

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        logger.warning(f"unable to determine system {self.name} interface state, skip updating: {result.message}")
        return NetworkTaskResult.ok()


class SystemStandbyTask(TargetSystems, NetworkTask):
    """ Standby system """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        ctl = self._ctx.get_systemctl(self.name)
        if not ctl.is_system_standby():
            ctl.standby()
        return NetworkTaskResult.ok()

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        ctl = self._ctx.get_systemctl(self.name)
        # TODO: should avoid making network calls in the recover() method OR make network_cfg updates threadsafe
        if ctl.is_system_standby():
            return NetworkTaskResult.ok()
        return result


class SystemActivateTask(TargetSystems, NetworkTask):
    """ Activate system """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        ctl = self._ctx.get_systemctl(self.name)
        if not ctl.is_system_activated():
            ctl.activate()
        return NetworkTaskResult.ok()

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        ctl = self._ctx.get_systemctl(self.name)
        # TODO: should avoid making network calls in the recover() method OR make network_cfg updates threadsafe
        if ctl.is_system_activated():
            return NetworkTaskResult.ok()
        return result


class SystemNetworkReloadTask(TargetSystems, NetworkTask):
    """ Reload network config on system """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        ctl = self._ctx.get_systemctl(self.name)
        ctl.network_reload()
        return NetworkTaskResult.ok()

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        logger.error(f"Unable to reload network config on system {system.name}")
        return result


class SystemNetworkConfigTask(TargetSystems, NetworkTask):
    """ Update system network config """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        base_dir = self._ctx.get_config_dir(self.name)
        config_file = base_dir / "var/lib/tftpboot/network-cfg.json"
        ctl = self._ctx.get_systemctl(self.name)
        ok, msg = ctl.update_network_config(str(config_file))
        if ok:
            return NetworkTaskResult.ok()
        return NetworkTaskResult.error(msg)

class SystemIfaceBasedFromTierTask(TargetSystems, ConfigTask):
    """ Task to assign system interfaces based on the switch tier."""
    def run(self, *args, **kwargs) -> NetworkTaskResult:
        systems = [get_obj_name(s) for s in self._ctx._deploy_config.systems]
        update_system_port_switch(self._ctx.network_config, systems)
        return NetworkTaskResult.ok()
    
