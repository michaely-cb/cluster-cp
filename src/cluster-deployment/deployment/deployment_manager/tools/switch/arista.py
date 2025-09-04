from collections import defaultdict

import json
import logging
import re
import time
from typing import Dict, List, Optional, Callable

from deployment_manager.tools.ssh import make_prompt_detector
from deployment_manager.tools.switch.interface import PortInterface, SwitchCtl, SwitchInterface, BreakoutMode, \
    SwitchCommands, LldpNeighbor, InterfaceErrStats, SFlowStatus, SFlowConfig, \
    SupportedSwitchModels, MacAddrTableEntry
from deployment_manager.tools.utils import try_parse_mac, strip_json_dict

logger = logging.getLogger(__name__)

VRF_NAME = "cs1data"

class AristaPortInterface(PortInterface):
    _re_mgmt = re.compile(r"^(ma\d)$")
    _re_data = re.compile(r"(^Eth(ernet)?(?P<s0>\d+)(/(?P<s1>\d+))?(/(?P<s2>\d+))?$)")
  
    def __init__(self, linecard: Optional[int], port: int, channel: Optional[int], int_name: Optional[str] = None):
        if (port == -1):
            # special case for MGMT port
            super().__init__(f"{int_name}", None, 0, None, role="mgmt")
        else:
            ordinals = "/".join([str(i + 1) for i in (linecard, port, channel,) if i is not None])
            super().__init__(f"Ethernet{ordinals}", linecard, port, channel)

    @classmethod
    def from_str(cls, s) -> Optional['AristaPortInterface']:
        m_mgmt = cls._re_mgmt.match(s)
        m_data = cls._re_data.match(s)
        if (not m_mgmt) and (not m_data):
            return None
        elif m_mgmt:
            return cls(None, -1, None, int_name=s)
        else:
            d = m_data.groupdict()
            
            s0 = int(d.get("s0")) - 1
            s1 = None if not d.get("s1") else int(d.get("s1")) - 1
            s2 = None if not d.get("s2") else int(d.get("s2")) - 1

            if s1 is not None and s2 is not None:
                return cls(s0, s1, s2)
            if s1 is not None:
                return cls(None, s0, s1)
            return cls(None, s0, None)

class AristaSwitchCommands(SwitchCommands):
    def parse_interface(self, if_name: str) -> Optional[PortInterface]:
        return AristaPortInterface.from_str(if_name)

    def get_enter_config_mode_cmds(self) -> List[str]:
        return ["en",
                "terminal length 0",  # don't paginate
                "terminal dont-ask",  # don't require 'y' prompts
                "config"]

    def get_exit_config_mode_cmds(self) -> List[str]:
        return ["exit", "write memory"]

    def get_sleep_commands(self, seconds: int) -> List[str]:
        return ["en", "bash", f"sleep {seconds}", "exit"]

    def get_port_breakout_cmds(
            self, port: int, current_mode: BreakoutMode, target_mode: BreakoutMode
    ) -> List[List[str]]:
        if current_mode == target_mode:
            return []

        arista_port_idx = port + 1

        if current_mode == BreakoutMode.Mode800X1:
            if target_mode == BreakoutMode.Mode400X2:
                cmd_groups = [
                    [
                        f"interface ethernet {arista_port_idx}/{subport}",
                        "speed 400g-4",  # 400G over 4 lanes
                        "exit",
                    ] for subport in (1, 5)]
            elif target_mode == BreakoutMode.Mode100X8:
                cmd_groups = [
                    [
                        f"interface ethernet {arista_port_idx}/{subport}",
                        "speed 100g-1", # 100G over 1 lane
                        "exit",
                    ] for subport in range(1, 9)]
            else:
                raise ValueError(f"cannot breakout port {arista_port_idx}, unknown target mode for 800g: {target_mode}")

        elif current_mode == BreakoutMode.Mode400X1:
            if target_mode == BreakoutMode.Mode100X4:
                # 4x100g interfaces with copper DACs (txr CR8) should use autonegotiation to configure electrical settings
                # which improve perf. However, ports 63,64 do not work with this setting due to some EOS bug so ignore those
                auto = "auto" if arista_port_idx not in (63, 64) else ""

                cmd_groups = [
                    [
                        f"interface ethernet {arista_port_idx}/{subport}",
                        f"speed {auto} 100g-2",  # 100G over 2 lanes
                        "exit",
                    ] for subport in (1, 3, 5, 7) ]
            elif target_mode == BreakoutMode.Mode100X1:
                cmd_groups = [[
                    f"hardware speed-group {arista_port_idx} serdes 25g",
                    f"interface ethernet {arista_port_idx}/1",
                    "speed 100g-4",
                    "exit",
                ]]
            else:
                raise ValueError(f"cannot breakout port {arista_port_idx}, unhandled target mode {target_mode}")
        else:
            raise ValueError(f"cannot breakout port {arista_port_idx}, port already in breakout mode {current_mode.value}")

        if cmd_groups:
            # prevents subports going into a shared-fate mode with certain optics, see SW-189427
            cmd_groups.append([
                f"interface ethernet {arista_port_idx}/1",
                "transceiver diag simulate removed",
                "exit", "exit", "bash", "sleep 3", "exit", "config",
                f"interface ethernet {arista_port_idx}/1",
                "no transceiver diag simulate removed",
                "exit",
            ])
        return cmd_groups

    def prompt_detector(self) -> Callable[[bytes], bool]:
        return make_prompt_detector(">#$]")

    def get_sflow_config_commands(self, interfaces: List[SwitchInterface], status: SFlowStatus, conf: SFlowConfig) -> List[str]:
        cmds = []

        all_interface_names = [i.name for i in interfaces]
        for intf in conf.configure_interfaces if conf.configure_interfaces else []:
            assert (
                intf in all_interface_names
            ), f"Couldn't find specified interface on switch: {intf}"

        # Arista's default default for per-interface sampling is: ingress
        # enabled, egress disabled. Depending on the desired config, we need to
        # set both the default defaults and per-interface config.
        # XXX currently, we only configure the egress sampling, and don't touch
        # ingress sampling.
        if conf.enable is not None:
            maybe_no = "" if conf.enable else "no "
            cmds.append(f"{maybe_no}sflow run")
            if conf.configure_interfaces:
                # Disable egress sampling for all interfaces by default.
                cmds.append("no sflow interface egress unmodified enable default")
                # Only enable sampling for select interfaces.
                for intf in conf.configure_interfaces:
                    cmds.append(f"interface {intf}")
                    cmds.append(f"{maybe_no}sflow egress unmodified enable")
                    # TODO: configure ingress sampling for this interface.
                    cmds.append("exit") # from interface config
            else:
                # TODO: set default ingress sampling for all interfaces.
                # cmds.append("sflow interface disable default")
                # Set default egress sampling for all interfaces.
                cmds.append(f"{maybe_no}sflow interface egress unmodified enable default")
                # Per-interface config: use the global default that we just set above.
                for intf in all_interface_names:
                    cmds.append(f"interface {intf}")
                    cmds.append("default sflow egress unmodified enable")
                    # TODO: configure ingress sampling for this interface.
                    cmds.append("exit") # from interface config

        if conf.sampling_rate is not None:
            rate = int(conf.sampling_rate)
            cmds.append(f"sflow sample {rate}")

        if conf.poll_interval is not None:
            poll = int(conf.poll_interval)
            if poll == 0: # do not send any counter samples
                cmds.append("no sflow polling-interval")
            else:
                cmds.append(f"sflow polling-interval {poll}")

        if conf.agent_ip is not None:
            cmds.append(f"sflow vrf {VRF_NAME} agent address {conf.agent_ip}")

        if conf.src_ip is not None:
            cmds.append(f"sflow vrf {VRF_NAME} source {conf.src_ip}")

        if conf.dsts is not None:
            # Remove all existing dsts.
            for ip, port in status.dsts:
                cmds.append(f"no sflow vrf {VRF_NAME} destination {ip} {port or ''}")
            # Add the specified dsts.
            for ip, port in conf.dsts:
                cmds.append(f"sflow vrf {VRF_NAME} destination {ip} {port or ''}")

        if len(cmds) == 0: return []
        return (
            self.get_enter_config_mode_cmds() + cmds + self.get_exit_config_mode_cmds()
        )

    def get_sflow_clear_commands(self) -> List[str]:
        return (
            self.get_enter_config_mode_cmds()
            + ["clear sflow counters"]
            + self.get_exit_config_mode_cmds()
        )

class AristaSupportedSwitchModels(SupportedSwitchModels):

    def __init__(self):
        supported = [
            ("7060X6", "4.33.1F"),
            ("7808", "4.33.2F-39661965.cerebraseft1"),
            ("7808", "4.34.1F-41967535.orinocogen5rel"),
            ("7060DX5", "4.33.1F"),
            ("7060DX5", "4.31.2F-34591166.gangesrel"),
            ("7060", "4.23.0F"),
            ("7060", "4.31.3M"),
            ("7260", "4.23.0F"),
        ]
        super().__init__(supported)


class AristaSwitchCtl(SwitchCtl):

    def vendor_cmds(self) -> SwitchCommands:
        return AristaSwitchCommands()

    def model_support(self) -> SupportedSwitchModels:
        return AristaSupportedSwitchModels()

    def get_hostname(self) -> Optional[str]:
        out = self.execute_commands(["en", "show hostname | json"])
        doc = strip_json_dict("\n".join(out))
        return doc.get("hostname")

    def get_model(self) -> str:
        _, out, _ = self._exec.exec("show version", throw=True)
        models = self.model_support().supported_models()
        for line in out.split('\n'):
            if "Arista " in line:
                for m in models:
                    if m in line:
                        return m
        return ""

    def get_fw_version(self) -> str:
        _, out, _ = self._exec.exec("show version", throw=True)
        version = ""
        regex = re.compile(
            r".*Software image version: (?P<vers>[\d+\w+.-]+).*"
        )
        for line in out.split('\n'):
            m = regex.match(line)
            if m:
                version = m.group('vers')
                break
        return version

    def get_mac_addr_table(self) -> List[MacAddrTableEntry]:
        _re_not_la = re.compile(r"^(?!.*port-channel).+$")
        _, stdout, _ = self._exec.exec("show mac address-table | json", throw=True)
        infs = json.loads(stdout)
        rv = []
        entries = infs["unicastTable"]["tableEntries"]
        for entry in entries:
            interface = entry.get("interface", "")
            # skip link-aggregation interfaces
            if _re_not_la.match(interface):
                mac = entry.get("macAddress", "") 
                rv.append(MacAddrTableEntry(
                    switch=self._name,
                    name=interface,
                    mac=mac,
                ))
        return rv
    
    def list_lldp_neighbors(self) -> List[LldpNeighbor]:
        _, stdout, _ = self._exec.exec("show lldp neighbor detail | json", throw=True)
        infs = json.loads(stdout)
        rv = []
        for inf, infos in infs.get("lldpNeighbors", {}).items():

            info_list = infos.get("lldpNeighborInfo", [])
            if isinstance(info_list, dict):
                info_list = [info_list]
            for info in info_list:
                inf_info = info.get("neighborInterfaceInfo", {})
                dst_if, dst_mac = "", ""
                if inf_info.get("interfaceIdType", "") == "interfaceName":
                    dst_if = inf_info.get("interfaceId_v2", inf_info.get("interfaceId", ""))
                elif inf_info.get("interfaceIdType", "") == "macAddress":
                    dst_if = inf_info.get("interfaceDescription", "")
                    dst_mac = try_parse_mac(inf_info.get("interfaceId_v2", inf_info.get("interfaceId", ""))) or ""

                if inf.startswith("Man"):
                    continue

                if not dst_mac:
                    dst_mac = try_parse_mac(info.get("chassisId", ""))

                rv.append(LldpNeighbor(
                    src=self._name,
                    src_if=inf,
                    dst_name=info.get("systemName", infos.get("systemName", "")),
                    dst_desc=info.get("systemDescription", infos.get("systemDescription", "")),
                    dst_if=dst_if,
                    dst_if_mac=dst_mac,
                    raw=info,
                ))
        return rv

    def list_interfaces(self) -> List[SwitchInterface]:
        """ Returns normalized interface status """
        rv = []

        _, stdout, _ = self._exec.exec("show interface status | json", throw=True)
        infs = json.loads(stdout)

        for desc, val in infs["interfaceStatuses"].items():
            port_inf = AristaPortInterface.from_str(desc)
            if not port_inf:
                continue
            line_status = val.get("lineProtocolStatus", "")
            link_status = val.get("linkStatus", "")
            up = line_status == "up" and link_status == "connected"
            transceiver = val.get("interfaceType", "unknown")
            if transceiver == "Not Present":
                transceiver = ""
            rv.append(SwitchInterface(
                switch=self._name,
                name=str(port_inf),
                normalized_port=port_inf.port,
                normalized_channel=port_inf.channel,
                alias="",
                desc=val.get("description", ""),
                up=up,
                status=f"{link_status}/{line_status}",
                speed=val.get("bandwidth", -1),
                transceiver=transceiver,
                raw=val
            ))
        return rv

    def list_interfaces_err_stats(self) -> List[InterfaceErrStats]:
        rv = []
        not_set = 999
        _, stdout, _ = self._exec.exec("show clock | json", throw=True)
        clock = json.loads(stdout)
        switch_time = clock.get("utcTime", time.time())  # seconds in float

        _, stdout, _ = self._exec.exec("show interface | json", throw=True)
        infs = json.loads(stdout)

        _, stdout, _ = self._exec.exec("show interfaces transceiver | json", throw=True)
        port_channel_rx = defaultdict(dict)
        port_channel_tx = defaultdict(dict)
        try:
            # bucket by port/channel: Ethernet5/6 -> [Ethernet5][6]
            # lane for the switches we use is duplex - 2 fibers - aggregated into a channel
            for inf, val in json.loads(stdout)["interfaces"].items():
                if not val or "/" not in inf:
                    continue
                inf_prefix, channel = inf.rsplit("/", 1)
                port_channel_rx[inf_prefix][int(channel)] = val.get("rxPower", not_set)
                port_channel_tx[inf_prefix][int(channel)] = val.get("txPower", not_set)
        except Exception as e:
            logger.debug(f"failed to parse `show interfaces transceiver`: {e}")

        sub_infs = defaultdict(list)
        for if_name in infs["interfaces"].keys():
            inf = AristaPortInterface.from_str(if_name)
            if not inf or inf.channel is None:
                continue
            inf_prefix, subinf = if_name.rsplit("/", 1)
            sub_infs[inf_prefix].append(int(subinf))
        for v in sub_infs.values():
            v.sort()

        _, stdout, _ = self._exec.exec("show interfaces phy diag error-correction histogram | json", throw=True)
        inf_counters = {k: v for k, v in json.loads(stdout).get("interfaces", {}).items()}

        for if_name, val in infs["interfaces"].items():
            port_inf = AristaPortInterface.from_str(if_name)
            if not port_inf or port_inf.channel is None:
                continue
            raw = {"interface": val}
            up = val.get("lineProtocolStatus", "") == "up" and val.get("interfaceStatus", "") == "connected"
            state_duration = int(switch_time - val.get("lastStatusChangeTimestamp", 0.0))

            # associate channels with the subinterface and find the sub interface's min power
            min_rx_power, min_tx_power = not_set, not_set
            inf_prefix, sub_inf = if_name.rsplit("/", 1)
            sub_inf = int(sub_inf)
            if inf_prefix in port_channel_rx and inf_prefix in port_channel_tx and inf_prefix in sub_infs:
                channels_per_port = len(port_channel_rx.get(inf_prefix))
                channels_per_subinf = channels_per_port // len(sub_infs[inf_prefix])
                # assume all subinf have same number of channels
                sub_inf = (sub_infs[inf_prefix].index(sub_inf) * channels_per_subinf) + 1
                for i in range(sub_inf, sub_inf + channels_per_subinf):
                    min_rx_power = min(port_channel_rx[inf_prefix].get(i, min_rx_power), min_rx_power)
                    min_tx_power = min(port_channel_tx[inf_prefix].get(i, min_tx_power), min_tx_power)
            min_rx_power = None if min_rx_power == not_set else min_rx_power
            min_tx_power = None if min_tx_power == not_set else min_tx_power

            # made up statistic to find links with high number of corrected symbols per codeword
            tail_phy_err = 0
            if if_name in inf_counters and "phys" in inf_counters[if_name]:
                try:
                    phy = inf_counters[if_name]["phys"][0]
                    raw["phys"] = phy
                    corr_codewords = phy.get("correctedCodewords", {})
                    corr_codewords_int = {}
                    max_bin = -1
                    for k in corr_codewords.keys():
                        if "-" in k:  # arista can group the first half of bins, seems unpredictable when it does that
                            continue
                        ik = int(k)
                        max_bin = max(max_bin, ik)
                        corr_codewords_int[ik] = corr_codewords[k]
                    for err_bin, v in corr_codewords_int.items():
                        if err_bin >= max_bin // 2:
                            tail_phy_err += v.get("changes", 0)
                except Exception as e:
                    logger.warning(f"failed to parse err counters: {e}")

            rv.append(InterfaceErrStats(
                switch=self._name,
                name=str(port_inf),
                up=up,
                state_duration=state_duration,
                min_rx_power=min_rx_power,
                min_tx_power=min_tx_power,
                tail_phys_errs=tail_phy_err,
                raw=raw,
            ))
        return rv

    def sflow_status(self) -> List[SFlowStatus]:
        _, stdout, _ = self._exec.exec("show sflow | json", throw=True)
        status = json.loads(stdout)

        _, stdout, _ = self._exec.exec("show sflow interfaces | json", throw=True)
        status_intfs = json.loads(stdout)
        running_interfaces = status_intfs.get("interfaces", {})
        interfaces: Dict[str, bool] = {}
        for ifname, ifstate in running_interfaces.items():
            interfaces[ifname] = ifstate.get("sFlows", {}).get(
                "egress", {}).get("status") == "running"

        agent_ip, src_ip, dsts = None, None, []
        for src in status.get("ipv4Sources", []):
            if src.get("vrfName", "") != VRF_NAME:
                continue
            src_ip = src.get("ipv4Address", None)
            break
        for agent in status.get("ipv4Agents", []):
            if agent.get("vrfName", "") != VRF_NAME:
                continue
            agent_ip = agent.get("ipv4Address", None)
            break
        for dst in status.get("ipv4Destinations", []):
            dst_ip4 = dst.get("ipv4Address", None)
            dst_port = dst.get("port", None)
            dsts.append((dst_ip4, dst_port))

        return [
            SFlowStatus(
                src=self._name,
                enabled=status.get("enabled", False),
                sampling_rate=int(status.get("sampleRate", 0)),
                poll_interval=int(status.get("pollingInterval", 0)),
                agent_ip=agent_ip,
                src_ip=src_ip,
                dsts=dsts,
                interfaces=interfaces,
                sample_cnt=status.get("softwareSamples", 0),
                warnings=status.get("warnings", []),
            )
        ]
