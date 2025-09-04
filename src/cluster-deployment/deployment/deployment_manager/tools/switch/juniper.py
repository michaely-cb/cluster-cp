import json
import logging
import re
from typing import Dict, List, Optional, Callable

from deployment_manager.tools.ssh import make_prompt_detector
from deployment_manager.tools.switch.interface import MacAddrTableEntry, PortInterface, SFlowConfig, SFlowStatus, SwitchCtl, SwitchInterface, SwitchCommands, \
    BreakoutMode, GBPS, LldpNeighbor, SupportedSwitchModels
from deployment_manager.tools.utils import strip_json_dict

logger = logging.getLogger(__name__)

ROUTING_INSTANCE = "cs1data"

class JuniperPortInterface(PortInterface):
    """ Simplified Juniper ethernet interface
    Ignores parsing several interface types:
    - non-fpc=0 cards
    - non-pic=0 cards
    - non-eth interfaces
    - interfaces with logical subchannels
    It's likely that we'll need to handle multiple PIC cards in the future
    """
    _re_mgmt = re.compile(r"^(re0:mgmt(-\d)?)$")
    _re_data = re.compile(r"(^(?P<prefix>et|xe)-(?P<fpc>\d+)/(?P<pic>\d+)/(?P<port>\d+)(:(?P<channel>\d+))?$)")

    def __init__(self, linecard: int, port: int, channel: Optional[int], int_name: Optional[str] = None, prefix: str = "et"):
        if (linecard == -1) and (port == -1):
            # special case for MGMT port
            super().__init__(f"{int_name}", None, 0, None, role="mgmt")
        else:
            name = f"{prefix}-0/{linecard}/{port}" if channel is None else f"{prefix}-0/{linecard}/{port}:{channel}"
            super().__init__(name, linecard, port, channel)

    @classmethod
    def from_str(cls, s) -> Optional['JuniperPortInterface']:
        m_mgmt = cls._re_mgmt.match(s)
        m_data = cls._re_data.match(s)
        if (not m_mgmt) and (not m_data):
            return None
        elif m_mgmt:
            return cls(-1, -1, None, int_name=s)
        else:
            d = m_data.groupdict()

            if d.get("fpc") != "0":
                return None
            
            prefix = d.get("prefix")
            pic = int(d.get("pic"))
            port = int(d.get("port"))
            channel = None if d.get("channel") is None else int(d.get("channel"))
            return cls(pic, port, channel, prefix=prefix)

class JuniperSwitchCommands(SwitchCommands):
    def parse_interface(self, if_name: str) -> Optional[PortInterface]:
        return JuniperPortInterface.from_str(if_name)

    def get_enter_config_mode_cmds(self) -> List[str]:
        return ["configure"]

    def get_exit_config_mode_cmds(self) -> List[str]:
        return ["commit and-quit"]

    def get_sleep_commands(self, seconds: int) -> List[str]:
        return ["start shell", f"sleep {seconds}", "exit"]

    def get_port_breakout_cmds(
            self, port: int, current_mode: BreakoutMode, target_mode: BreakoutMode
    ) -> List[List[str]]:
        if current_mode == target_mode:
            return []

        if target_mode == BreakoutMode.Mode800X1:
            # some transceiver/port combinations require explicitly setting the speed to 800G
            return [
                [f"edit interfaces et-0/0/{port}", f"set speed 800g", "delete number-of-sub-ports", "exit"],
            ]
        elif target_mode == BreakoutMode.Mode400X2:
            return [
                [f"edit interfaces et-0/0/{port}", f"set speed 400g", "set number-of-sub-ports 2", "exit"],
            ]
        elif target_mode == BreakoutMode.Mode100X8:
            return [[f"edit interfaces et-0/0/{port}", f"set speed 100g", "set number-of-sub-ports 8", "exit"]]
        else:
            raise ValueError(f"{self}: unhandled breakout mode {target_mode}")

    def prompt_detector(self) -> Callable[[bytes], bool]:
        return make_prompt_detector(">#$)")

    def get_sflow_config_commands(self, interfaces: List[SwitchInterface], status: SFlowStatus, conf: SFlowConfig) -> List[str]:
        cmds = ["edit protocols sflow"]

        all_interface_names = [i.name for i in interfaces]
        for intf in conf.configure_interfaces if conf.configure_interfaces else []:
            assert (
                intf in all_interface_names
            ), f"Couldn't find specified interface on switch: {intf}"

        # XXX currently, we only configure the egress sampling, and don't touch
        # ingress sampling.
        if conf.enable is not None:
            # By default, select all interfaces.
            selected_interfaces = conf.configure_interfaces or all_interface_names
            set_or_delete = "set" if conf.enable else "delete"
            for intf in selected_interfaces:
                cmds.append(f"{set_or_delete} interfaces {intf}")

        if conf.sampling_rate is not None:
            cmds.append(f"set sample-rate egress {int(conf.sampling_rate)}")

        if conf.poll_interval is not None:
            cmds.append(f"set polling-interval {int(conf.poll_interval)}")

        if conf.agent_ip is not None:
            cmds.append(f"set agent-id {conf.agent_ip}")

        if conf.src_ip is not None:
            cmds.append(f"set source-ip {conf.src_ip}")

        if conf.dsts is not None:
            # Remove all existing dsts.
            for ip, _ in status.dsts:
                cmds.append(f"delete collector {ip}")
            # Add the specified dsts.
            for ip, port in conf.dsts:
                port = port or SFlowStatus.DEFAULT_SFLOW_PORT
                cmds.append(
                    f"set collector {ip} udp-port {port} routing-instance {ROUTING_INSTANCE}")

        # Exit to top level of configuration
        cmds.append("top")

        # Add static routes to collector IPs
        if conf.dsts is not None:
            cmds.append("edit routing-options")
            for ip, _ in conf.dsts:
                cmds.append(
                    f"set static route {ip}/32 next-table {ROUTING_INSTANCE}.inet.0")

        # If --disable and no interfaces are specified, just delete all sFlow config
        if conf.enable == False and not conf.configure_interfaces:
            cmds = ["delete protocols sflow"]

        if len(cmds) == 0: return []
        return (
            self.get_enter_config_mode_cmds() + cmds +
            self.get_exit_config_mode_cmds()
        )

    def get_sflow_clear_commands(self) -> List[str]:
        return (
            ["clear sflow collector statistics"]
        )

class JuniperSupportedSwitchModels(SupportedSwitchModels):

    def __init__(self):
        supported = [
            ("qfx5240-64od", "23.4R2-202405160302.0-EVO"),
            ("qfx5240-64od", "23.4R2-S3.11-EVO"),
            # Note: ptx10002-36qdd is a non-modular PTX series switch that uses Juniper's home-grown
            # silicon which has different characteristics than the broadcom-based QFX series switches.
            ("ptx10002-36qdd", "24.4R1-S2.8-EVO"),
        ]
        super().__init__(supported)

class JuniperSwitchCtl(SwitchCtl):
    def __init__(self, name: str, host: str, username: str, password: str):
        super().__init__(name, host, username, password)

    def vendor_cmds(self) -> SwitchCommands:
        return JuniperSwitchCommands()

    def model_support(self) -> SupportedSwitchModels:
        return JuniperSupportedSwitchModels()

    def get_hostname(self) -> Optional[str]:
        out = self.execute_commands(["show system information | display json | no-more"])
        try:
            doc = strip_json_dict("\n".join(out))
            doc = doc.get("system-information", [{}])[0]
            return doc["host-name"][0]["data"]
        except Exception as e:
            logger.debug(f"unable to parse juniper 'show system information' output: {out} {e}")
            return None

    def get_model(self) -> str:
        _, out, _ = self._exec.exec("show version", throw=True)
        for line in out.split('\n'):
            if "Model:" in line:
                return line.strip().split()[-1]
        return ""

    def get_fw_version(self) -> str:
        _, out, _ = self._exec.exec("show version", throw=True)
        version = ""
        for line in out.split('\n'):
            if "Junos:" in line:
                version = line.strip().split()[-1]
                break
        return version

    def get_max_port_speed_gbps(self) -> int:
        # could use command `show chassis pic pic-slot 0 fpc-slot 0 | display json | no-more` to lookup this info
        # but it is not needed for now since we only support 800G data switches
        return 800
    
    def get_mac_addr_table(self) -> List[MacAddrTableEntry]:
        _re_not_la = re.compile(r"^(?!ae[\d\/_-]*$).+$")
        _, stdout, _ = self._exec.exec("show ethernet-switching table | display json", throw=True)
        infs = json.loads(stdout)
        rv = []
        macdb = infs["l2ng-l2ald-rtb-macdb"][0]
        if len(macdb) == 0:
            return rv
        else:
            entries = macdb["l2ng-l2ald-mac-entry-vlan"][0]["l2ng-mac-entry"]
            for entry in entries:
                interface = entry.get("l2ng-l2-mac-logical-interface", "")[0].get("data", "")
                # skip link-aggregation interfaces
                if _re_not_la.match(interface):
                    mac = entry.get("l2ng-l2-mac-address", "")[0].get("data", "")
                    interface = interface.split(".")[0]  # remove subinterface if present
                    rv.append(MacAddrTableEntry(
                        switch=self._name,
                        name=interface,
                        mac=mac,
                    ))
            return rv

    def list_lldp_neighbors(self) -> List[LldpNeighbor]:

        def _get_param_data(neighbor, param):
            return neighbor[param][0]['data']

        _, stdout, _ = self._exec.exec("show lldp neighbors detail | display json | no-more", throw=True)
        lldp_info = json.loads(stdout)
        rv = []

        if not 'lldp-neighbors-information' in lldp_info:
            return rv

        neighbors = lldp_info['lldp-neighbors-information'][0]['lldp-neighbor-information']
        for nbr in neighbors:
            if 'lldp-remote-system-name' not in nbr:
                continue
            src_if = _get_param_data(nbr, 'lldp-local-interface')
            if 'mgmt' in src_if:
                continue
            dst_name = _get_param_data(nbr, 'lldp-remote-system-name')
            if 'Interface' in _get_param_data(nbr, 'lldp-remote-port-id-subtype'):
                dst_if = _get_param_data(nbr, 'lldp-remote-port-id')
            else:
                dst_if = _get_param_data(nbr,'lldp-remote-port-description')
            dst_desc = _get_param_data(nbr['lldp-system-description'][0], 'lldp-remote-system-description')
            dst_if_mac = ""
            if 'Mac' in _get_param_data(nbr, 'lldp-remote-port-id-subtype'):
                dst_if_mac = _get_param_data(nbr, 'lldp-remote-port-id')
            rv.append(LldpNeighbor(
                src=self._name,
                src_if=src_if,
                dst_name=dst_name,
                dst_desc=dst_desc,
                dst_if=dst_if,
                dst_if_mac=dst_if_mac,
                raw=nbr
            ))
        return rv

    def list_interfaces(self) -> List[SwitchInterface]:
        """ Returns normalized interface status. Only query the first pic/fpc slot """
        with self._exec.shell_session() as shell:
            shell.prompt_detect = self.vendor_cmds().prompt_detector()

            inf_doc = strip_json_dict(shell.exec("show interfaces et-0/0/* | display json | no-more"))
            infs = {}
            for i in inf_doc["interface-information"][0]["physical-interface"]:
                port_inf = JuniperPortInterface.from_str(i["name"][0]["data"])
                if port_inf:
                    infs[port_inf] = i

            chassis_doc = strip_json_dict(shell.exec("show chassis pic pic-slot 0 fpc-slot 0 | display json | no-more"))
            ports = {}
            port_list = safe_get_juniper_data(
                chassis_doc,
                "fpc-information", "fpc", "pic-detail", "port-information", "port",
                default=[]
            )
            for p in port_list:
                port_number = safe_get_juniper_data(p, "port-number", "data", default="")
                if port_number != "":
                    ports[int(port_number)] = p

            desc_doc = strip_json_dict(shell.exec("show interface descriptions et-* | display json | no-more"))
            descs = {}
            physical_interfaces = safe_get_juniper_data(
                desc_doc,
                "interface-information", "physical-interface",
                default=[])
            for i in physical_interfaces:
                name = safe_get_juniper_data(i, "name", "data", default="")
                port_inf = JuniperPortInterface.from_str(name)
                if port_inf:
                    description = safe_get_juniper_data(i, "description", "data", default="")
                    descs[port_inf] = description

        rv = []
        for port_inf, inf_obj in infs.items():
            trans = ""
            if port_inf.port in ports:
                trans = safe_get_juniper_data(ports[port_inf.port], "cable-type", "data", default="")
                inf_obj["port-information"] = ports[port_inf.port]

            oper = safe_get_juniper_data(inf_obj, "oper-status", "data", default="down")
            admin = safe_get_juniper_data(inf_obj, "admin-status", "data", default="down")

            try:
                speed_str = safe_get_juniper_data(inf_obj, "speed", "data", default="")
                if speed_str.endswith("Gbps"):
                    speed = int(speed_str[:-4]) * GBPS
                else:
                    speed = -1
            except ValueError:
                speed = -1
            rv.append(SwitchInterface(
                switch=self._name,
                name=str(port_inf),
                normalized_port=port_inf.port,
                normalized_channel=port_inf.channel,
                alias="",
                desc=descs.get(port_inf, ""),
                up=oper == "up" and admin == "up",
                status=f"oper={oper},admin={admin}",
                speed=speed,
                transceiver=trans,
                raw=inf_obj,
            ))
        return rv

    def sflow_status(self) -> List[SFlowStatus]:
        with self._exec.shell_session() as shell:
            shell.prompt_detect = self.vendor_cmds().prompt_detector()
            stdout = shell.exec("show sflow | display json | no-more")
            show_sflow = strip_json_dict(stdout)
            def get_nested(x, k): return next(iter(x.get(k, [])), {})
            sflow_info = {
                k: next(iter(v), {}).get("data", None) for k, v in
                get_nested(get_nested(show_sflow, "sflow"),
                           "sflow-information").items()
            }

            enabled = sflow_info.get("sflow-status", "") == "Enabled"
            agent_id = sflow_info.get("sflow-agent-id", None)
            src_ip = sflow_info.get("sflow-source-ip", None)
            sampling_rate = int(sflow_info.get(
                "sflow-sample-rate-egress", "0").split(":")[0])
            poll_interval = int(sflow_info.get("sflow-polling-interval", "0"))
            sample_cnt = 0
            collectors = []
            interfaces: Dict[str, bool] = {}

            stdout = shell.exec(
                "show sflow collector | display json | no-more")
            show_collector = strip_json_dict(stdout)
            collector_dicts = [
                {k: next(iter(v), {}).get("data", None)
                    for k, v in c.items() if isinstance(v, list)}
                for c in get_nested(show_collector, "sflow").get("sflow-collector", [])
            ]
            for c in collector_dicts:
                if c.get("remote-collector-interface-address", None):
                    port = c.get("collector-udp-port", None)
                    port = int(port) if port else None
                    collectors.append(
                        (c["remote-collector-interface-address"], port))
                sample_cnt += int(c.get("collector-number-of-samples", 0))

            stdout = shell.exec(
                "show sflow interface | display json | no-more")
            show_intfs = strip_json_dict(stdout)
            intfs_dicts = [
                {k: next(iter(v), {}).get("data", None)
                    for k, v in intf.items() if isinstance(v, list)}
                for intf in get_nested(show_intfs, "sflow").get("sflow-interface", [])
            ]
            intfs_enabled = {
                intf["interface-name"].split(".")[0]: intf.get(
                    "interface-status-egress", "") == "Enabled"
                for intf in intfs_dicts
                if intf.get("interface-name", None)
            }
            all_interfaces = self.list_interfaces()
            all_interface_names = [i.name for i in all_interfaces]
            for ifname in intfs_enabled.keys():
                if ifname in all_interface_names: continue
                logger.warning(f"sFlow enabled interface {ifname} is not present in all interfaces: {all_interface_names}.")
            for ifname in all_interface_names:
                interfaces[ifname] = intfs_enabled.get(ifname, False)

            return [
                SFlowStatus(
                    src=self._name,
                    enabled=enabled,
                    sampling_rate=sampling_rate,
                    poll_interval=poll_interval,
                    agent_ip=agent_id,
                    src_ip=src_ip,
                    dsts=collectors,
                    interfaces=interfaces,
                    sample_cnt=sample_cnt,
                    warnings=[],  # Juniper doesn't show sFlow warnings
                )
            ]


def safe_get_juniper_data(doc, *path, default=None):
    """
    Safely access nested data in Juniper JSON responses.

    Args:
        doc: The JSON data from Juniper command output
        path: List of keys/indices to traverse
        default: Value to return if path doesn't exist

    Returns:
        The data at the specified path or default if path doesn't exist
    """
    current = doc
    try:
        for key in path:
            if isinstance(key, int):
                current = current[key]
            elif isinstance(current, dict):
                current = current.get(key, {})
            elif isinstance(current, list) and current:
                # For lists, we typically want the first element in Juniper responses
                current = current[0].get(key, {}) if isinstance(current[0], dict) else default
                if current == {}:
                    return default
            else:
                return default
        return current if current != {} else default
    except (KeyError, IndexError, TypeError):
        return default
