from collections import defaultdict

import json
import logging
import os
import re
import string
import sys
import time
import traceback
from typing import Dict, List, Tuple, Optional

from .runner import APITask
from .ssh_tasks import (
    SSHCmdRunner,
    _SSHBatchShellABC,
    SSHLoginConnector,
    SSHExitHandler,
    MakePrintingSSHTask,
    run_ssh_commands_await_prompt,
    SSHCommand,
    SSHCommandTaskBase
)
from .utils import PingTest, parse_switch_json_output, parse_edgecore_port_alias, parse_lldp_doc, removeprefix
from ..common.context import DEFAULT_VRF_NAME
from ..connection_map import MemoryxSwitch, SwarmxSwitch
from ..schema import (
    NetworkConfigSchema,
    JSIPInterface,
    JSIPNetwork,
    JSIPAddress,
    ASN4
)
from ..utils import add_or_update_item, items_name_iter

from deployment_manager.network_config.common.task import NetworkTask, NetworkTaskResult, TargetSwitches

logger = logging.getLogger(__name__)

# The command and task classes aren't divided between Arista and other
# switch types. A lot of the commands and behavior will be the same so
# it's expected that new switch types will be conditionals in some
# parts of the code.

HPE_MODEL_256 = "12908"
ARISTA_MODEL_7060 = "7060DX5"
ARISTA_MODEL_7060X6 = "7060X6"
ARISTA_MODEL_7808 = "7808"
CEREBRAS_SYSTEM_OUI = "70:55:f8"
GBPS = 10 ** 9


def try_format_mac(mac: str) -> str:
    s = re.sub(r"[\-.:_]", "", mac.lower())
    if re.match(r"[0-9a-f]{12}", s):
        parts = []
        for i in range(0, 12, 2):
            parts.append(s[i:i + 2])
        return ":".join(parts)
    return mac


class SwitchSessionBase(_SSHBatchShellABC):
    """ Base class for switch session commands
    """

    def __init__(self, obj, username, password):
        self._obj = obj
        self._connection = SSHLoginConnector(obj, username, password)
        self._tlen = 0 if self._obj["vendor"] in ["arista", "dell", "juniper"] else 512

    def connection(self):
        return self._connection

    @property
    @staticmethod
    def commands():
        """ Provide commands to run on the switch
        """
        return ()

    def get_cmd_runner(self) -> SSHCmdRunner:
        return run_ssh_commands_await_prompt

    def _shell_prep_commands(self):
        vendor = self._obj["vendor"]
        model = self._obj.get("model")
        cmds = []
        if vendor == "hpe":
            if model == HPE_MODEL_256:
                cmds = [
                    "screen-length disable",
                    "system-view"
                ]
            else:
                cmds = [
                    f"terminal length {self._tlen}",
                    "terminal width 128",
                    "enable"
                ]
        elif vendor == "arista":
            cmds = [
                f"terminal length {self._tlen}",
                "enable"
            ]
        elif vendor == "dell":
            cmds = [
                "sonic-cli",
                f"terminal length {self._tlen}"
            ]
        elif vendor == "juniper":
            cmds = [
                f"set cli screen-length {self._tlen}"
            ]
        return cmds

    def _shell_exit_commands(self):
        vendor = self._obj["vendor"]
        model = self._obj.get("model")
        cmds = ["", "exit"]
        if model == HPE_MODEL_256 or vendor == "dell":  # exit sonic-cli for dell
            cmds.append("exit")
        return cmds

    @property
    def input_iter(self):
        if not self.commands:
            return None
        else:
            for line in self._shell_prep_commands():
                yield line.rstrip() + "\n"
                time.sleep(1)
            for line in self.commands:
                # To avoid confusion, we add the line end here and just let
                # the iter provide commands.
                yield line.rstrip() + "\n"
                time.sleep(0.1)
            time.sleep(5)
            for line in self._shell_exit_commands():
                yield line.rstrip() + "\n"


class SwitchSession(SwitchSessionBase):
    """ Send commands to a switch
    """

    def __init__(self, obj, username, password, commands):
        super().__init__(obj, username, password)

        self._commands = iter(commands)

    @property
    def commands(self):
        return self._commands


class SwitchPrintingTask(MakePrintingSSHTask):
    """ Switch variant of the printing ssh task decorator
    """

    def __init__(self, alias, name=None, err_only=False, exc_exit=False):
        super().__init__(alias, ('switches',), name, err_only, exc_exit)


@SwitchPrintingTask(alias='version', exc_exit=True)
class SwitchVersionTask(SwitchSessionBase):
    """ Show the version on a switch
    """
    commands = ('show version',)


@SwitchPrintingTask(alias='reload')
class SwitchReloadTask(SwitchSessionBase):
    """ Reload the switch immediately
    """
    commands = ('reload now force',)


@SwitchPrintingTask(alias='uptime')
class SwitchShowUptimeTask(SwitchSessionBase):
    """ Show the switch uptime
    """
    commands = ('show uptime',)


@SwitchPrintingTask(alias='show_running_config')
class SwitchShowRunningConfigTask(SwitchSessionBase):
    """ Show the running configuration
    """
    commands = ('show running-config',)

class SwitchLLDPTask(TargetSwitches, NetworkTask):
    """ Collect LLDP information from a switch and process it"""
    def run(self, *args, **kwargs) -> NetworkTaskResult:
        with self._ctx.get_switchctl(self.name) as ctl:
            return NetworkTaskResult.ok(ctl.list_lldp_neighbors())

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        lldp_neighbors_obj = result.data
        nw_doc = self._ctx.network_config
        lldp_neighbors = []
        vendor = self._ctx.network_config.get_raw_object(self.name).get("vendor") 
        model = self._ctx.network_config.get_raw_object(self.name).get("model")
        for lldp_neighbor in lldp_neighbors_obj:
            if vendor == "hpe" and model != HPE_MODEL_256 and "HundredGig" not in lldp_neighbor.dst_if:
                continue
            lldp_neighbors.append({
                "my_port": lldp_neighbor.src_if,
                "name": lldp_neighbor.dst_name,
                "port": lldp_neighbor.dst_if
            })
        def _post_process_neighbors(nw_doc, lldp_neighbors: List[dict]):
            post = []
            switches = {s["name"]: s for s in (nw_doc.raw().get("switches", []))}
            systems = {s["name"]: s for s in (nw_doc.raw().get("systems", []))}
            
            system_default_hostnames = {s["default_hostname"]: s["name"]
                                        for s in (nw_doc.raw().get("systems", []))
                                        if s.get("default_hostname") is not None}

            for neighbor in lldp_neighbors:
                my_port, name, port = neighbor["my_port"], neighbor["name"], neighbor["port"]

                if name.endswith("-cm"):
                    system_name = name[:-3]
                    if system_name in systems:
                        name = system_name
                    elif system_name in system_default_hostnames:
                        name = system_default_hostnames[system_name]

                # Port IDs for ports connected to systems are represented
                # by their MAC addresses. Those need to be mapped to the
                # port numbers

                # Check if it is system MAC
                if try_format_mac(port).startswith(CEREBRAS_SYSTEM_OUI):
                    name = name.split('-')[0]  # SYSTEM_NAME-cm
                    system_obj = systems.get(name, {})
                    if system_obj:
                        for p in system_obj.get('ports', []):
                            if p['mac'] == port:
                                port = f"Port {p['port_num']}"
                                break

                elif name in switches:
                    if switches[name].get("vendor") == "edgecore":
                        port = parse_edgecore_port_alias(port)

                post.append({
                    "my_port": my_port,
                    "name": name,
                    "port": port,
                })
            return post
        
        lldp_neighbors = _post_process_neighbors(nw_doc, lldp_neighbors)
        switches = dict()
        if nw_doc.raw().get("switches"):
            switches = dict(items_name_iter(nw_doc.raw().get("switches")))
        
        nodes = dict()
        for node_section in NetworkConfigSchema.node_sections:
            if nw_doc.raw().get(node_section):
                nodes.update(items_name_iter(nw_doc.raw().get(node_section)))

        systems = dict()
        if nw_doc.raw().get("systems"):
            for system in nw_doc.raw().get("systems"):
                systems[system["name"]] = system
        
        exterior_switches = dict()
        if nw_doc.raw().get("exterior_switches"):
            exterior_switches = dict(items_name_iter(
                nw_doc.raw().get("exterior_switches")))

        if "connections" not in nw_doc.raw().get("xconnect", {}):
            xconnect = nw_doc.raw().get("xconnect", {})
            xconnect.update({"connections": list()})
            if "xconnect" not in nw_doc.raw():
                nw_doc.raw()["xconnect"] = xconnect

        # Pass 1: Place or update new connections
        current_switch = nw_doc.get_raw_object(self.name)
        for neighbor in lldp_neighbors:
            # Switch to switch connections
            if neighbor['name'] in switches:
                neighbor_obj = switches[neighbor['name']]
                if neighbor_obj["tier"] == current_switch["tier"]:
                    # xconnects must be from one tier to another
                    continue
                
                for connection in nw_doc.raw().get("xconnect", {}).get("connections", list()):
                    links = connection["links"]
                    # Updating existing links may require running the
                    # placer again.

                    if (links[0]["name"] == current_switch["name"]) and \
                       (links[0]["port"] == neighbor["my_port"]):
                        links[1]["name"] = neighbor["name"]
                        if neighbor["port"]:
                            links[1]["port"] = neighbor["port"]
                        break


                    elif (links[1]["name"] == current_switch["name"]) and \
                         (links[1]["port"] == neighbor["my_port"]):

                        links[0]["name"] = neighbor["name"]
                        if neighbor["port"]:
                            links[0]["port"] = neighbor["port"]
                        break

                else: # for connection in ...
                    conn_name = f'L3 {current_switch["name"]} and {neighbor["name"]}'


                    new_connection = dict(
                        name=conn_name,
                        links=list((
                            dict(name=current_switch["name"],
                                 port=neighbor["my_port"]),
                            dict(name=neighbor["name"],
                                 port=neighbor["port"]),)
                        )
                    )
                    xconnect = nw_doc.raw().get("xconnect", {})
                    connections = xconnect.get("connections", [])
                    connections.append(new_connection)
                    nw_doc.raw().update({"xconnect": {"connections": connections}})
                    

            elif neighbor['name'] in nodes:
                # TODO: handle node entries
                pass

            elif neighbor['name'] in systems:
                # System interfaces are handled by a different tool
                # and LLDP shouldn't be overwriting them.
                system_name = systems[neighbor['name']]['name']
                for sc in nw_doc.raw().get("system_connections", []):
                    if sc["system_name"] == system_name and \
                     sc["system_port"] == neighbor['port']:
                        sc["switch_name"] = current_switch["name"]

                        sc["switch_port"] = neighbor["my_port"]
                        break
                else:
                    new_connection = dict(
                        switch_name=current_switch["name"],
                        switch_port=neighbor["my_port"],
                        system_name=systems[neighbor['name']]['name'],
                        system_port=neighbor["port"]
                    )

                    sys_connections = nw_doc.raw().get("system_connections", [])
                    sys_connections.append(new_connection)
                    nw_doc.raw().update({"system_connections": sys_connections})
                    

            elif neighbor['name'] in exterior_switches:
                # These are handled by the exterior connection tool.
                pass

            else:
                logger.info(f'Unknown neighbor: {neighbor["name"]}')

        return NetworkTaskResult.ok()

class SwitchModelTask(TargetSwitches, NetworkTask):
    """ Update network model with switch model number """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        with self._ctx.get_switchctl(self.name) as ctl:
            return NetworkTaskResult.ok(ctl.get_model())

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        model = result.data
        nw_doc = self._ctx.network_config
        if model:
            nw_doc.update_raw_object(self.name, {"model": model})
            return NetworkTaskResult.ok()
        else:
            return NetworkTaskResult.error(f"unable to find model for {self.name}")

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        obj = self._ctx.network_config.get_raw_object(self.name)
        if obj.get("model"):
            logger.warning(f"failed to get model for {self.name} but model is already populated in doc")
            return NetworkTaskResult.ok()
        return result


@SwitchPrintingTask(alias='upload', exc_exit=True)
class SwitchUploadConfigTask(SwitchSessionBase):
    """ Upload the switch configuration
    """

    def __init__(self, obj, username, password, base_dir, config_section=None, ports_for=()):
        super().__init__(obj, username, password)

        self._config_dir = os.path.join(base_dir, "switches", obj["name"])

        self._config_section = config_section
        self._ports_for = []
        self._skip_upload = False
        for p in ports_for:
            link = p.split(":", 1)
            port = link[0]
            if len(link) > 1:
                src, dst = link
                if obj["name"] == src:
                    port = dst
                elif obj["name"] == dst:
                    port = src
                else:
                    continue
            self._ports_for.append(port)
        if ports_for and not self._ports_for:
            # user specified --ports_for but none of this switches' ports matched
            self._skip_upload = True

        self._switch_model = obj.get("model")
        self._switch_vendor = obj.get("vendor")

        if not os.path.exists(self._config_dir):
            raise RuntimeError(
                f'Configuration dir does not exist: {self._config_dir}')

    def _read_json_cfg(self, input_file):
        cfg = json.load(input_file)
        if not self._config_section:
            for section in ["base", "system_vlan", "multi_mgmt_bgp"]:
                for l in cfg.get(section, "").split('\n'):
                    yield l.rstrip()
            for section in ["interfaces", "uplinks", "system_routes"]:
                for interface in cfg.get(section, []):
                    for s in interface["sections"]:
                        for l in s.split('\n'):
                            yield l.rstrip()
            for section in ("ntp", "snmp"):
                for l in cfg.get(section, "").split('\n'):
                    yield l.rstrip()
        elif not self._skip_upload:
            key = ""
            sections = []
            if self._config_section == "nodes":
                key = "node_name"
                sections = ["interfaces"]
            elif self._config_section == "systems":
                key = "system_name"
                sections = ["interfaces", "system_routes"]
            elif self._config_section == "uplinks":
                sections = ["uplinks"]
            elif self._config_section == "switches":
                key = "switch_name"
                sections = ["interfaces"]

            # System VLAN config needs to be applied once per switch
            if self._config_section == "systems":
                for l in cfg.get("system_vlan", "").split('\n'):
                    yield l.rstrip()

            for section in sections:
                for interface in cfg.get(section, []):
                    if self._ports_for and key and interface.get(key) not in self._ports_for:
                        continue
                    for s in interface["sections"]:
                        for l in s.split('\n'):
                            yield l.rstrip()

        yield cfg["final_cmd"]

    @property
    def commands(self):
        if self._switch_vendor == "juniper":
            yield "configure"
            time.sleep(5)
        elif self._switch_model != HPE_MODEL_256 and self._switch_vendor != "edgecore":
            yield "configure terminal"
            time.sleep(5)

        for dirpath, _dirnames, filenames in os.walk(self._config_dir):
            for filename in filenames:
                if filename.startswith(".") or filename.startswith("#"):
                    continue

                if not filename.endswith(".cfg") and not filename.endswith(".json"):
                    continue

                file_fullpath = os.path.join(dirpath, filename)
                with open(file_fullpath) as input_file:
                    # For backward compatibility.
                    # New config will be generated as json
                    if filename.endswith(".cfg"):
                        for input_line in input_file:
                            yield input_line.rstrip()
                    else:
                        for l in self._read_json_cfg(input_file):
                            yield l

        yield ""
        if self._switch_vendor == "edgecore":
            yield "sudo config save -y"
        elif self._switch_vendor != "juniper":
            if self._switch_vendor == "hpe" and self._switch_model == HPE_MODEL_256:
                yield "save flash:/startup.cfg"
                yield "Y"
            else:
                yield "write memory"


class _SwitchPingCheck(SwitchSessionBase):
    def __init__(self, obj, username, password, targets: Dict[str, List[PingTest]], vrf_name=DEFAULT_VRF_NAME):
        super().__init__(obj, username, password)
        self._obj = obj
        self._targets = targets
        self._vrf_name = vrf_name

    @property
    def commands(self):
        if self._obj["vendor"] == "arista":
            t = string.Template(f"ping vrf $vrf $ipaddr repeat 1")
        elif self._obj["vendor"] == "hpe":
            if self._obj.get("model") == HPE_MODEL_256:
                t = string.Template(f"ping -vpn-instance $vrf -c 1 $ipaddr")
            else:
                t = string.Template(f"ping vrf $vrf $ipaddr -c 1")
        elif self._obj["vendor"] == "dell":
            t = string.Template(f"ping vrf Vrf_$vrf $ipaddr -c 1")
        elif self._obj["vendor"] == "juniper":
            t = string.Template(f"ping routing-instance $vrf $ipaddr count 1 wait 2")
        else:  # Edgecore switches
            t = string.Template(f"sudo ip vrf exec Vrf_$vrf ping $ipaddr -c 1")
        for _, targets in self._targets.items():
            for target in targets:
                yield t.substitute({'ipaddr': target.dst_ip, 'vrf': self._vrf_name})

    @staticmethod
    def post(results: Tuple[any, Dict[any, List[PingTest]], str]) -> Dict[str, List[PingTest]]:
        """
        Args
            results: a tuple of the switch name, the targets, and the switch stdout
        Returns
            Input target dict with output fields (ok, details) filled
        """
        switch, target_dict, output = results
        if not target_dict:
            return target_dict

        target_by_ip = {
            t.dst_ip: t for targets in target_dict.values() for t in targets
        }
        dst_index = 0
        dst_ip = None
        packet_loss_percent = "0%"
        lines = output.split('\n')
        for i, l in enumerate(lines):
            words = l.split()
            if switch.get("vendor") == "hpe" and switch.get("model") == HPE_MODEL_256:
                packet_loss_percent = "0.0%"
                if "Ping statistics" in l:
                    dst_ip = words[words.index('Ping') + 3]
                    dst_index = i
            else:
                if "ping statistics" in l:
                    dst_ip = words[words.index('ping') - 1]
                    dst_index = i

            if dst_ip and dst_ip in target_by_ip:
                target = target_by_ip[dst_ip]
                if target.ok:
                    continue
                target.ok = False
                if "packet loss" in l:
                    if words[words.index('packet') - 1] == packet_loss_percent:
                        target.ok = True
                    else:
                        target.ok = False
                        target.details = ' '.join(lines[dst_index:i + 1]).replace('\r', '')

        for _, target in target_by_ip.items():
            if target.ok is None:
                target.ok = False
                target.details = "'ping statistics' is not found in ping output"
        return target_dict

    @staticmethod
    def handler(exception) -> Optional[bool]:
        """ Handler for exceptions in __call__
        Returns true if the exception should be non-fatal
        """
        traceback.print_exception(type(exception),
                                  value=exception,
                                  tb=exception.__traceback__,
                                  file=sys.stderr)
        return False


class SwitchPingGenerator:
    DEST_NODES = "nodes"
    DEST_SYSTEMS = "systems"
    DEST_XCONN = "xconnects"
    DEST_UPLINKS = "uplinks"

    def __init__(self, network_config: dict):
        self._cfg = network_config

    def get_xconnect_targets(self, switch_name: str) -> List[PingTest]:
        """ Ping otherside of directly connect xconnects """
        # links in xconnect['connections'] are one per switch.
        # prefix is the /31 network (two IPs) for both ends of
        # the link. The first ip in this prefix is for the
        # switch in the first link and the second is for the switch
        # in the second link
        targets = []
        for c in self._cfg.get("xconnect", {}).get("connections", []):
            links = c["links"]
            if len(links) != 2:
                logger.error(
                    f"WARNING: cross connect entry {c} did not contain 2 links! Skipping ping for this connection")
                continue
            if 'prefix' not in c:  # placer has not yet been run for new connections
                continue
            link_a_addr = JSIPNetwork(c["prefix"]).network_address
            link_a, link_b, dst_ip = links[0], links[1], None
            if link_a["name"] == switch_name:
                src_ip, dst_ip = str(link_a_addr), str(link_a_addr + 1)
                targets.append(PingTest(
                    src_name=switch_name,
                    src_if=link_a.get("port", ""),
                    src_ip=src_ip,
                    dst_name=link_b.get("name", ""),
                    dst_if=link_b.get("port", ""),
                    dst_ip=dst_ip,
                    kind=SwitchPingGenerator.DEST_XCONN,
                ))
            elif link_b["name"] == switch_name:
                src_ip, dst_ip = str(link_a_addr + 1), str(link_a_addr)
                targets.append(PingTest(
                    src_name=switch_name,
                    src_if=link_b.get("port", ""),
                    src_ip=src_ip,
                    dst_name=link_a.get("name", ""),
                    dst_if=link_a.get("port", ""),
                    dst_ip=dst_ip,
                    kind=SwitchPingGenerator.DEST_XCONN,
                ))
        return targets

    def get_system_targets(self, switch_name: str) -> List[PingTest]:
        """ Ping all directly connected (L2) interfaces on a system """

        def to_port_index(port_n_name: str) -> int:
            m = re.match(r"Port (\d+)", port_n_name)
            if m:
                return int(m.group(1))
            return -1

        sysport_switchport = {
            c["system_name"] + f":{to_port_index(c['system_port'])}": c["switch_port"]
            for c in self._cfg.get("system_connections", [])
        }

        targets = []
        for s in self._cfg.get("systems", []):
            for i in s.get("interfaces", []):
                if i.get("switch_name") == switch_name:
                    # e.g. control11, data0, ...
                    m = re.match(r"\D*(\d+)$", i.get("name", ""))
                    if m:
                        system_port_ordinal = int(m.group(1)) + 1
                    else:
                        system_port_ordinal = -1
                    remote_name = s.get("name", "")
                    switch_port = sysport_switchport.get(f"{remote_name}:{system_port_ordinal}")
                    if not switch_port:
                        logger.debug(f"data inconsistency: system {remote_name} Port {system_port_ordinal} "
                                     "didn't have a system connection, skipping ping")
                        continue
                    targets.append(
                        PingTest(
                            src_name=switch_name,
                            src_if=switch_port,
                            src_ip="",
                            dst_name=remote_name,
                            dst_if=i['name'],
                            dst_ip=str(JSIPInterface(i["address"]).ip),
                            kind=SwitchPingGenerator.DEST_SYSTEMS,
                        )
                    )
        return targets

    def get_node_targets(self, switch_name: str) -> List[PingTest]:
        """ Ping directly connected (L2) interfaces on a switch. """
        targets = []
        for node_type in NetworkConfigSchema.node_sections:
            for node in self._cfg.get(node_type, []):
                for inf in node.get("interfaces", []):
                    if inf["switch_name"] == switch_name:
                        targets.append(PingTest(
                            src_name=switch_name,
                            src_if=inf.get("switch_port"),
                            src_ip="",
                            dst_name=node.get("name", ""),
                            dst_if=inf.get("name", ""),
                            dst_ip=str(JSIPInterface(inf["address"]).ip),
                            kind=SwitchPingGenerator.DEST_NODES,
                        ))
        return targets

    def get_uplink_targets(self, switch_name: str) -> List[PingTest]:
        targets = []
        for ec in self._cfg.get("exterior_connections", []):
            exterior, interior = ec.get("exterior", {}), ec.get("interior", {})
            if interior.get("switch_name") == switch_name:
                targets.append(PingTest(
                    src_name=switch_name,
                    src_if=interior["switch_port"],
                    src_ip=interior["address"].split("/")[0],
                    dst_name=exterior["switch_port"],
                    dst_if=exterior["switch_port"],
                    dst_ip=exterior["address"].split("/")[0],
                    kind=SwitchPingGenerator.DEST_UPLINKS,
                ))
        return targets

    def get_targets(self, switch_name: str, destination_types: List[str],
                    ignore_interfaces: List[str] = ()) -> Dict[str, List[PingTest]]:
        fns = {SwitchPingGenerator.DEST_XCONN: self.get_xconnect_targets,
               SwitchPingGenerator.DEST_NODES: self.get_node_targets,
               SwitchPingGenerator.DEST_SYSTEMS: self.get_system_targets,
               SwitchPingGenerator.DEST_UPLINKS: self.get_uplink_targets,
               }
        addrs = dict()
        for dest_type, fn in fns.items():
            if not destination_types or dest_type in destination_types:
                addrs[dest_type] = [t for t in fn(switch_name) if f"{t.dst_name}:{t.dst_if}" not in ignore_interfaces]
        return addrs


class SwitchPingCheckAPITask(APITask):
    """
    Check if given switch can ping nodes, systems and
    internal switches it should be able to
    """

    def __init__(self, obj, username, password, network_config, *args, **kwargs):
        self._obj = obj
        self._username = username
        self._password = password
        self._cfg = network_config
        self._vrf_name = network_config["environment"].get("vrf_name", DEFAULT_VRF_NAME)

    def object_name(self) -> str:
        return self._obj["name"]

    def __call__(self, destination_types: List[str] = ()) -> Tuple[any, Dict[str, List[PingTest]], str]:
        ipaddrs = SwitchPingGenerator(self._cfg).get_targets(self.object_name(), destination_types)
        out = b""
        if ipaddrs:
            out, err, ret = _SwitchPingCheck(self._obj, self._username, self._password, ipaddrs, self._vrf_name)()
        return self._obj, ipaddrs, out.decode('utf-8')

    def post(self, results: Tuple[any, Dict[str, List[PingTest]], str]) -> List[PingTest]:
        """ Called with what __call__ returns
        """
        return [t for v in _SwitchPingCheck.post(results).values() for t in v]

    def handler(self, exception) -> List[PingTest]:
        return [PingTest(self.object_name(), "*", "*", "*", "*", "*",
                         "*", False, f"exception during handling: {exception}")]


class SwitchPingCheckTask:
    """
    Check if given switch can ping nodes, systems and
    internal switches it should be able to
    """
    task_alias = "ping_check"
    compat_sections = ('switches',)

    def __init__(self, obj, username, password, network_config, destination_types: List[str] = (),
                 ignore_interfaces: List[str] = (), verbose=False):
        self._obj = obj
        self._username = username
        self._password = password
        self._cfg = network_config
        self._verbose = verbose
        self._destination_types = destination_types
        self._ignore_interfaces = ignore_interfaces
        self._vrf = network_config.get("environment", {}).get("vrf_name", DEFAULT_VRF_NAME)

    @property
    def _my_name(self):
        return self._obj["name"]

    def __call__(self) -> Tuple[any, Dict[str, List[PingTest]], str]:
        ipaddrs = SwitchPingGenerator(self._cfg).get_targets(self._my_name, self._destination_types,
                                                             self._ignore_interfaces)
        num_ips = 0
        for _, ips in ipaddrs.items():
            num_ips += len(ips)
        logger.info(f"Pinging {num_ips} addresses from {self._obj['name']}")
        out = b""
        if ipaddrs:
            out, err, ret = _SwitchPingCheck(self._obj, self._username, self._password, ipaddrs)()
        return self._obj, ipaddrs, out.decode('utf-8')

    # pylint: disable=no-self-use
    def post(self, results: Tuple[any, Dict[str, List[PingTest]], str]) -> Tuple[bool, dict]:
        """ Called with what __call__ returns
        """
        category_targets = _SwitchPingCheck.post(results)
        rv, fail_obj, output = True, {}, []
        for category, targets in category_targets.items():
            for target in targets:
                if target.ok:
                    continue
                rv = False
                if category not in fail_obj:
                    fail_obj[category] = []
                fail_obj[category].append(dict(
                    src=target.src_if, dst=f"{target.dst_name}:{target.dst_ip}", reason=target.details,
                ))
                output.append(str(target))
        retval = dict(switch_name=self._my_name, failures=fail_obj)
        if not rv:
            retval['error'] = 'ping failures'
            logger.info("\n".join(output))
        return rv, retval

    # pylint: disable=no-self-use
    def handler(self, exception):
        """ Handler for exceptions in __call__
        """
        _SwitchPingCheck.handler(exception)
        return False, dict(switch_name=self._obj['name'], error='switch unreachable')


class SwitchRoceStatusCommand(SwitchSessionBase):
    """ Check whether roce is enabled on the switch
    """

    def __init__(self, obj, username, password):
        super().__init__(obj, username, password)

        cmds = {
            "arista": ('show run',),
            "hpe": {HPE_MODEL_256: ('show current-configuration',)},
            "dell": ('show run',),
            "edgecore": ('show spanning-tree',),
            "juniper": ('show spanning-tree mstp configuration',)
        }
        cmds["hpe"] = defaultdict(lambda: ('show run',), cmds["hpe"])

        cmd = cmds.get(self._obj["vendor"])
        if cmd and isinstance(cmd, dict):
            cmd = cmd[self._obj["model"]]
        self._commands = cmd

    @property
    def commands(self):
        return self._commands


class SwitchRoceStatusPost:
    """Process the results of the switch roce info and update the network
    configuration.
    """

    def __init__(self, network_config, obj):
        self.network_config = network_config
        self.obj = obj

    @staticmethod
    def _exists_in_output(stdout, search_text):
        return True if search_text in stdout.decode('ASCII') else False

    @staticmethod
    def switch_roce_status_arista(stdout):
        status = "disabled"
        if SwitchRoceStatusPost._exists_in_output(stdout, "qos map traffic-class 5 to priority-group 5"):
            status = "enabled"
        return status

    @staticmethod
    def switch_roce_status_dell(stdout):
        status = "disabled"
        if SwitchRoceStatusPost._exists_in_output(stdout, "roce enable"):
            status = "enabled"
        return status

    @staticmethod
    def switch_roce_status_edgecore(stdout):
        # before enabling ROCE, the spanning tree output is simply "Spanning-tree is not configured"
        out = stdout.decode("ASCII")
        if "Spanning-tree is not configured" in out:
            return "disabled"

        # probably don't need this degree of parsing because of the above comment, but leaving as is
        status = "disabled"
        mode_set = False
        status_enabled = False
        mode_re = re.compile(r'.*Spanning Tree Mode\s+:\s+MSTP')
        status_re = re.compile(r'.*Spanning Tree Enabled/Disabled\s+:\s+Enabled')
        for line in stdout.decode('ASCII').splitlines():
            if not mode_set and mode_re.match(line):
                mode_set = True
                continue
            if not status_enabled and status_re.match(line):
                status_enabled = True
                continue
            if mode_set and status_enabled:
                status = "enabled"
                break
        return status

    @staticmethod
    def switch_roce_status_hpe_fixed(stdout):
        status = "disabled"
        if SwitchRoceStatusPost._exists_in_output(stdout, "traffic pool roce "):
            status = "enabled"
        return status

    @staticmethod
    def switch_roce_status_hpe_modular(stdout):
        status = "disabled"
        if SwitchRoceStatusPost._exists_in_output(stdout, "qos trust dscp"):
            status = "enabled"
        return status

    @staticmethod
    def switch_roce_status_juniper(stdout):
        status = "enabled"
        if SwitchRoceStatusPost._exists_in_output(stdout, "not enabled at global level"):
            status = "disabled"
        return status

    def __call__(self, result):
        stdout, _stderr, return_code = result

        if return_code != 0:
            raise RuntimeError(f'RoCE status retrieval failed: {return_code}')

        fns = {
            'arista': self.switch_roce_status_arista,
            'hpe': {HPE_MODEL_256: self.switch_roce_status_hpe_modular},
            'dell': self.switch_roce_status_dell,
            'edgecore': self.switch_roce_status_edgecore,
            'juniper': self.switch_roce_status_juniper
        }
        fns['hpe'] = defaultdict(lambda: self.switch_roce_status_hpe_fixed, fns['hpe'])

        switch_roce_status = None
        roce_fn = fns.get(self.obj["vendor"])
        if roce_fn is None:
            raise ValueError(f"Unknown switch type {self.obj['vendor']}")
        else:
            if isinstance(roce_fn, dict):
                roce_fn = roce_fn[self.obj.get("model", "")]
            switch_roce_status = roce_fn(stdout)

        self.obj["roce_status"] = switch_roce_status
        add_or_update_item(self.network_config, "switches", self.obj)


class SwitchRoceStatusTask(SSHCommandTaskBase):
    """ Check if roce is enabled on a switch
    """
    task_alias = 'roce_status'
    compat_sections = ('switches',)

    def __init__(self, network_config, obj, username, password):
        super().__init__(
            SwitchRoceStatusCommand(obj, username, password),
            SwitchRoceStatusPost(network_config, obj),
            SSHExitHandler(obj))


class SwitchSystemBgpStatusCommand(SwitchSessionBase):
    """ Check whether bgp is enabled for systems on the switch
    """

    def __init__(self, obj, username, password):
        super().__init__(obj, username, password)

        if self._obj["vendor"] in ["arista", "dell"]:
            self._commands = ('show run',)
        elif self._obj["vendor"] == "edgecore":
            self._commands = ('show run bgp',)
        else:
            if self._obj.get("model") == HPE_MODEL_256:
                self._commands = ('show current-configuration',)
            else:
                self._commands = ('show run',)

    @property
    def commands(self):
        return self._commands


class SwitchSystemBgpStatusPost:
    """ Process the results of the switch system bgp info and update the network
    configuration.
    """

    def __init__(self, network_config, obj):
        self.network_config = network_config
        self.obj = obj
        self._vrf_name = network_config["environment"].get("vrf_name", DEFAULT_VRF_NAME)

    def get_static_routes_hpe_modular(self, stdout):
        routes = list()
        for line in stdout.decode('ASCII').splitlines():
            if f"ip route-static vpn-instance {self._vrf_name} " in line:
                routes.append(line)
        return routes

    def get_static_routes(self, stdout):
        routes = list()
        for line in stdout.decode('ASCII').splitlines():
            if f"ip route vrf {self._vrf_name} " in line:
                routes.append(line)
        return routes

    @staticmethod
    def is_bgp_configured(stdout):
        bgp_configured = False
        for line in stdout.decode('ASCII').splitlines():
            if "ws_cluster_csx" in line:
                bgp_configured = True
                break
        return bgp_configured

    def has_static_routes(self, stdout):
        has_static_route = False
        if self.obj["vendor"] == "hpe" and self.obj.get("model") == HPE_MODEL_256:
            routes = self.get_static_routes_hpe_modular(stdout)
        else:
            routes = self.get_static_routes(stdout)
        for s in self.network_config.get('systems', list()):
            has_static_route = False
            vip = None
            for i in s.get('interfaces', list()):
                if i.get('switch_name') == self.obj['name']:
                    vip = s.get('vip')
                    break
            if vip:
                for r in routes:
                    if f"{vip} " in r or f"{JSIPAddress(vip)} " in r:
                        has_static_route = True
                        break
            if has_static_route:
                break
        return has_static_route

    def __call__(self, result):
        stdout, _stderr, return_code = result
        if return_code != 0:
            raise RuntimeError(f'System BGP status retrieval failed: {return_code}')

        if not self.is_bgp_configured(stdout):
            system_bgp_status = "disabled"
        else:
            if self.has_static_routes(stdout):
                system_bgp_status = "disabled"
            else:
                system_bgp_status = "enabled"
        self.obj["system_bgp_status"] = system_bgp_status
        add_or_update_item(self.network_config, "switches", self.obj)


class SwitchSystemBgpStatusTask(SSHCommandTaskBase):
    """ Check if bgp is enabled for systems on a switch
    """
    task_alias = 'system_bgp_status'
    compat_sections = ('switches',)

    def __init__(self, network_config, obj, username, password):
        super().__init__(
            SwitchSystemBgpStatusCommand(obj, username, password),
            SwitchSystemBgpStatusPost(network_config, obj),
            SSHExitHandler(obj))


class SwitchFirmwareVersionTask(TargetSwitches, NetworkTask):
    """ Update network model with switch firmware version """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        with self._ctx.get_switchctl(self.name) as ctl:
            return NetworkTaskResult.ok(ctl.get_fw_version())

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        fwver = result.data
        nw_doc = self._ctx.network_config
        if fwver:
            nw_doc.update_raw_object(self.name, {"firmware_version": fwver})
            return NetworkTaskResult.ok()
        else:
            return NetworkTaskResult.error(f"unable to find firmware version for {self.name}")

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        obj = self._ctx.network_config.get_raw_object(self.name)
        if obj.get("firmware_version"):
            logger.warning(
                f"failed to get firmware version for {self.name} but firmware_version is already populated in doc")
            return NetworkTaskResult.ok()
        return result


class SwitchASNCommand(SwitchSessionBase):
    """ Command to get switch ASN
    """

    def __init__(self, obj, username, password):
        super().__init__(obj, username, password)

        if self._obj["vendor"] == "arista":
            self._commands = ('show run | include bgp',)
        elif self._obj["vendor"] == "dell":
            self._commands = ('show run | grep bgp',)
        else:
            if self._obj.get("model") == HPE_MODEL_256:
                self._commands = ('show current-configuration | include bgp',)
            else:
                self._commands = ('show run | include bgp',)

    @property
    def commands(self):
        return self._commands


class SwitchASNPost:
    """Process the results of the switch ASN info and update the network
    configuration.
    """

    def __init__(self, network_config, obj):
        self.network_config = network_config
        self.obj = obj

    @staticmethod
    def switch_asn_arista_dell(stdout):
        asn = ""
        regex = re.compile(
            r'.*router bgp (?P<asn>[\d]+)$'
        )
        for line in stdout.decode('ASCII').splitlines():
            m = regex.match(line)
            if m:
                asn = m.group('asn')
                break
        return ASN4(int(asn)) if asn else None

    @staticmethod
    def switch_asn_hpe_fixed(stdout):
        asn = ""
        regex = re.compile(
            r'.*router bgp (?P<asn>[\d]+) vrf .+$'
        )
        for line in stdout.decode('ASCII').splitlines():
            m = regex.match(line)
            if m:
                asn = m.group('asn')
                break
        return ASN4(int(asn)) if asn else None

    @staticmethod
    def switch_asn_hpe_modular(stdout):
        asn = ""
        regex = re.compile(
            r'.*bgp (?P<asn>\d+)$'
        )
        for line in stdout.decode('ASCII').splitlines():
            m = regex.match(line)
            if m:
                asn = m.group('asn')
                break
        return ASN4(int(asn)) if asn else None

    def __call__(self, result):
        stdout, _stderr, return_code = result

        if return_code != 0:
            raise RuntimeError(f'ASN retrieval failed: {return_code}')

        if self.obj["vendor"] in ["arista", "dell"]:
            switch_asn = self.switch_asn_arista_dell(stdout)
        elif self.obj["vendor"] == "hpe":
            if self.obj.get("model") == HPE_MODEL_256:
                switch_asn = self.switch_asn_hpe_modular(stdout)
            else:
                switch_asn = self.switch_asn_hpe_fixed(stdout)
        else:
            raise ValueError(f"Unknown switch type {self.obj['vendor']}")

        if switch_asn:
            self.obj["asn"] = str(switch_asn)
            add_or_update_item(self.network_config, "switches", self.obj)


class SwitchASNTask(SSHCommandTaskBase):
    """ Read switch ASN
    """
    task_alias = 'asn'
    compat_sections = ('switches',)

    def __init__(self, network_config, obj, username, password):
        super().__init__(
            SwitchASNCommand(obj, username, password),
            SwitchASNPost(network_config, obj),
            SSHExitHandler(obj))


class SwitchEnableRoceCommand(SwitchSessionBase):
    """
    Commands to enable roce as global config on switch.
    Dell and Edgecore need a reboot for this config. This command
    is carved out as a separate task for them. For other vendors,
    the upload command is sufficient
    """

    def __init__(self, obj, username, password):
        super().__init__(obj, username, password)

        if self._obj.get("roce_status") == "enabled":
            self._commands = ('',)
        else:
            if self._obj["vendor"] == "edgecore":
                self._commands = (
                    'sudo config spanning-tree enable mstp',
                    'y',
                    'sudo config qos reload',
                    'sudo config save -y',
                    'sudo reboot',
                )
            elif self._obj["vendor"] == "dell":
                self._commands = (
                    'configure',
                    'roce enable force-defaults',
                    'y',
                )
            else:
                self._commands = ('',)

    @property
    def input_iter(self):
        for cmd in super().input_iter:
            yield cmd
        if len(self._commands) > 1:
            # wait for 2 minutes to ensure the reboot has initiated since in edgecore,
            # the switch is still reachable even after reboot is called
            logger.info("waiting for 2 minutes for switch to begin rebooting")
            time.sleep(60 * 2)

    @property
    def commands(self):
        return self._commands


class SwitchEnableRocePost:
    """
    Handle output of RoCE enable commands
    """

    def __init__(self, network_config, obj, username, password):
        self.network_config = network_config
        self.obj = obj
        self._username = username
        self._password = password

    def _wait_for_switch_reboot(self):
        uptime_cmd = SSHCommand(
            self.obj, self._username, self._password, "uptime"
        )
        switch_up = False
        interval = 5  # Check every 5secs if it's up
        timeout = time.time() + 10 * 60  # Wait for max 10 minutes for switch to reboot
        while time.time() < timeout:
            try:
                uptime_cmd()
                switch_up = True
                break
            except:
                logger.info(f"Switch {self.obj['name']} is not up yet. Checking in {interval} secs")
                time.sleep(interval)
        if not switch_up:
            raise ValueError(f"Switch {self.obj['name']} did not come up in 10 mins")

    def __call__(self, result):
        if self.obj["vendor"] not in ["dell", "edgecore"]:
            return
        self._wait_for_switch_reboot()


class SwitchEnableRoceTask(SSHCommandTaskBase):
    """ Enable RoCE on switches
    """
    task_alias = 'enable_roce'
    compat_sections = ('switches',)

    def __init__(self, network_config, obj, username, password):
        super().__init__(
            SwitchEnableRoceCommand(obj, username, password),
            SwitchEnableRocePost(network_config, obj, username, password),
            SSHExitHandler(obj))

