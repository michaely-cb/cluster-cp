import logging
import re
import json
from typing import Dict, List, Optional, Callable

from deployment_manager.tools.ssh import make_prompt_detector
from deployment_manager.tools.switch.interface import PortInterface, SFlowConfig, SFlowStatus, SwitchCtl, SwitchInterface, SwitchCommands, \
    BreakoutMode, SupportedSwitchModels, LldpNeighbor
from deployment_manager.tools.switch.utils import parse_table_lines

logger = logging.getLogger(__name__)

def parse_dell_sonic_table(table_string: str) -> List[dict]:
    """
    Parse sonic cli table format. Usually tables look like:
    ```
    ----------
    key0  key1
    ----------
    v0    v1
    v2    v3
    ```
    This function parses them to dicts: [{key0: v0, key1: v1}, {key0: v2, key1: v3}]

    Args:
        table_string: sonic cli table

    Returns: List of dicts
    """
    sep = "----"
    lines = [line.rstrip("\r") for line in table_string.strip().split('\n')]
    cols = []  # (name, start, end)
    if not lines:
        return []
    i = 0
    while i < len(lines):
        if lines[i].startswith(sep) and i + 2 < len(lines) and lines[i + 2].startswith(sep):
            i += 1  # 2 separators: ----\nkey   \n-----
            break
        elif lines[i].startswith(sep) and i > 0:
            i -= 1  # 1 separator: key   \n-----
            break
        i += 1
    else:
        return []

    # col names can have a single whitespace e.g. 'Port Status'
    ws_count = 0
    col = ""
    colw = 0
    for ci, c in enumerate(lines[i].rstrip("\n")):
        if ws_count < 2 and c != " ":
            col = col + c
            colw += 1
            ws_count = 0
        elif c == " ":
            colw += 1
            ws_count += 1
        else:  # non-whitespace - start new column
            cols.append((col, ci - colw, ci,))
            col = c
            colw = 1
            ws_count = 0
    cols.append((col, len(lines[i]) - colw, len(lines[i])))

    i += 1
    while i < len(lines):
        if lines[i].startswith(sep):
            break
        i += 1
    else:
        return []
    i += 1

    return parse_table_lines(cols, lines[i:])


class DellSonicPortInterface(PortInterface):
    """ Dell 400G interface """

    _re = re.compile(r"^Eth(ernet)?(?P<index>\d+)$")

    def __init__(self, index: int):
        super().__init__(f"Ethernet{index}", 0, index // 4, index % 4)
        self.index = index

    @classmethod
    def from_str(cls, s) -> Optional['DellSonicPortInterface']:
        m = cls._re.match(s)
        if not m:
            return None
        return cls(int(m.groupdict()["index"]))


class DellSonicSwitchCommands(SwitchCommands):
    def parse_interface(self, if_name: str) -> Optional[PortInterface]:
        return DellSonicPortInterface.from_str(if_name)

    def get_enter_config_mode_cmds(self) -> List[str]:
        return ["sonic-cli", "configure"]

    def get_exit_config_mode_cmds(self) -> List[str]:
        return ["exit", "write memory", "exit"]

    def get_sleep_commands(self, seconds: int) -> List[str]:
        return [f"sleep {seconds}"]

    def get_sflow_config_commands(self, interfaces: List[SwitchInterface], status: SFlowStatus, conf: SFlowConfig) -> List[str]:
        raise NotImplementedError(f"get_sflow_config_commands not implemented for {self.__class__}")

    def get_sflow_clear_commands(self) -> List[str]:
        raise NotImplementedError(f"get_sflow_clear_commands not implemented for {self.__class__}")

    def get_port_breakout_cmds(
            self, port: int, current_mode: BreakoutMode, target_mode: BreakoutMode
    ) -> List[List[str]]:
        if current_mode == target_mode:
            return []

        if current_mode != BreakoutMode.Mode400X1:
            raise ValueError(f"{self}: cannot breakout a port which is not already in mode {BreakoutMode.Mode400X1}")

        if_index = port * 4
        if target_mode == BreakoutMode.Mode100X4:
            breakout = [f"interface breakout port Eth{port + 1}/1 mode 4x100G"]
            post = []
            for i in range(0, 4):
                post.append(f"interface Ethernet{if_index+i}")
                post.append("no shutdown")
            return [breakout, post]
        elif target_mode == BreakoutMode.Mode100X1:
            breakout = [f"interface breakout port Eth{port + 1}/1 mode 1x100G"]
            post = [f"interface Ethernet{if_index}", "no shutdown", "fec RS", "exit"]
            return [breakout, post]
        else:
            raise ValueError(f"{self}: unhandled breakout mode {target_mode}")

    def prompt_detector(self) -> Callable[[bytes], bool]:
        return make_prompt_detector("#$")

class DellSonicSupportedSwitchModels(SupportedSwitchModels):

    def __init__(self):
        supported = [
            ("Z9664f", "4.2.1"),
        ]
        super().__init__(supported)


class DellSonicSwitchCtl(SwitchCtl):
    def __init__(self, name: str, host: str, username: str, password: str):
        super().__init__(name, host, username, password)

    def vendor_cmds(self) -> SwitchCommands:
        return DellSonicSwitchCommands()

    def model_support(self) -> SupportedSwitchModels:
        return DellSonicSupportedSwitchModels()

    def get_hostname(self) -> str:
        cmd = "hostname"
        out = self.execute_commands([cmd])

        lines = "\n".join(out).splitlines()
        for i, line in enumerate(lines[:-1]):
            if cmd in line:
                return lines[i+1].strip()
        raise RuntimeError(f"cannot find hostname of switch {self._name}")

    def get_model(self) -> str:
        _, out, _ = self._exec.exec("show version")
        for line in out.split('\n'):
            line = line.lower()
            if "z9644f" in line:
                return "Z9644f"
            elif "z9664f" in line:
                return "Z9664f"
        return ""

    def get_fw_version(self) -> str:
        _, out, _ = self._exec.exec("show version")
        version = ""
        regex = re.compile(
            r".*Software Version\s+:\s+(?P<vers>[\d+.]+).*"
        )
        for line in out.split('\n'):
            m = regex.match(line)
            if m:
                version = m.group('vers')
                break
        return version

    def list_interfaces(self) -> List[SwitchInterface]:
        """ Returns normalized interface status """
        with self._exec.shell_session() as shell:
            shell.prompt_detect = self.vendor_cmds().prompt_detector()
            shell.exec("sonic-cli")

            inf_status = shell.exec("show interface status | no-more")
            trans_summary = shell.exec("show interface transceiver summary | no-more")

            inf_table = parse_dell_sonic_table(inf_status)
            trans_table = parse_dell_sonic_table(trans_summary)

        infs: Dict[str, dict] = {i["Name"]: i for i in inf_table}
        trans: Dict[str, dict] = {i["Interface"]: i for i in trans_table}

        rv = []
        for inf, inf_obj in infs.items():
            port_inf = DellSonicPortInterface.from_str(inf)
            if not port_inf:
                continue

            trans_obj = trans.get(inf, {})
            inf_obj["transceiver"] = trans_obj

            oper = inf_obj.get("Oper", "")
            reason = inf_obj.get("Reason", "")
            try:
                speed = int(inf_obj.get("Speed", '')) * 1000000
            except ValueError:
                speed = -1
            rv.append(SwitchInterface(
                switch=self._name,
                name=str(port_inf),
                normalized_port=port_inf.port,
                normalized_channel=port_inf.channel,
                alias=inf_obj.get("AlternateName", ""),
                desc=inf_obj.get("Description", ""),
                up=oper == "up",
                status=f"oper={oper},reason={reason}",
                speed=speed,
                transceiver=trans_obj.get("Name", ""),
                raw=inf_obj,
            ))
        return rv

    def list_lldp_neighbors(self) -> List[LldpNeighbor]:
        _, stdout, _ = self._exec.exec("lldpctl -f json", throw=True)
        infs = json.loads(stdout)
        infs = infs.get("lldp", {}).get("interface", {})

        rv = []

        for inf in infs:
            for src_inf, inf_obj in inf.items():
                chassis_obj = inf_obj.get("chassis", {})
                port_obj = inf_obj.get("port", {})
                dst_if = port_obj.get("id", {}).get("value", "")

                chassis_keys = list(chassis_obj.keys())
                if "id" in chassis_keys:
                    dst_if_mac = chassis_obj.get("id", {}).get("value", "")
                    rv.append(LldpNeighbor(
                        src=self._name,
                        src_if=src_inf,
                        dst_desc=port_obj.get("descr", ""),
                        dst_if=dst_if,
                        dst_if_mac=dst_if_mac,
                        raw=inf_obj,
                    ))
                else:
                    for dst_name, dst_chassis_info in chassis_obj.items():
                        dst_if_mac = dst_chassis_info.get("id", {}).get("value")
                        rv.append(LldpNeighbor(
                                src=self._name,
                                src_if=src_inf,
                                dst_name=dst_name,
                                dst_desc=port_obj.get("descr", ""),
                                dst_if=dst_if,
                                dst_if_mac=dst_if_mac,
                                raw=inf_obj,
                        ))
        return rv