import logging
import re
import json
from typing import Dict, List, Optional, Callable

from deployment_manager.tools.ssh import make_prompt_detector
from deployment_manager.tools.switch.interface import PortInterface, SFlowConfig, SFlowStatus, SwitchCtl, SwitchInterface, SwitchCommands, \
    BreakoutMode, SupportedSwitchModels, LldpNeighbor
from deployment_manager.tools.switch.utils import parse_table_lines, parse_sonic_structured_output

logger = logging.getLogger(__name__)

def parse_edgecore_sonic_table(table_string: str) -> List[dict]:
    """
    Parse sonic cli table format. Usually tables look like:
    ```
    key0    key1
    ----  ------
      v0      v1
      v2      v3
    ```
    This function parses them to dicts: [{key0: v0, key1: v1}, {key0: v2, key1: v3}]

    Args:
        table_string: sonic cli table

    Returns: List of dicts
    """
    sep = "----"
    lines = [line.rstrip("\r") for line in table_string.split('\n')]
    cols = []  # (name, start, end)
    if not lines:
        return []
    i = 0
    while i < len(lines):
        if lines[i].startswith(sep):
            break
        i += 1
    else:
        return []

    colw = 0
    for ci, c in enumerate(lines[i]):
        if c == " " and colw > 0:
            cols.append((lines[i - 1][ci - colw:ci].strip(), ci - colw, ci))
            colw = 0
        else:
            colw += 1
    if colw > 0:
        cols.append((lines[i - 1][len(lines[i]) - colw: len(lines[i])].strip(), len(lines[i]) - colw, len(lines[i])))
    i += 1

    return parse_table_lines(cols, lines[i:])


class EdgecorePortInterface(PortInterface):
    """ Edgecore 400G interface """

    _re = re.compile(r"^Eth(ernet)?(?P<index>\d+)$")

    def __init__(self, index: int):
        super().__init__(f"Ethernet{index}", 0, index // 8, (index % 8) // 2)
        self.index = index

    @classmethod
    def from_str(cls, s) -> Optional['EdgecorePortInterface']:
        m = cls._re.match(s)
        if not m:
            return None
        return cls(int(m.groupdict()["index"]))


class EdgecoreSwitchCommands(SwitchCommands):
    def parse_interface(self, if_name: str) -> Optional[PortInterface]:
        return EdgecorePortInterface.from_str(if_name)

    def get_enter_config_mode_cmds(self) -> List[str]:
        return []

    def get_exit_config_mode_cmds(self) -> List[str]:
        return ["sudo config save -y"]

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

        inf = port * 8
        if target_mode == BreakoutMode.Mode100X4:
            breakout = [f'sudo config interface breakout Ethernet{inf} "4x100G" -y']
            post = []
            for i in range(0, 8, 2):
                post.append(f"sudo config interface startup Ethernet{inf+i}")
            return [breakout, post]
        elif target_mode == BreakoutMode.Mode100X1:
            breakout = [f'sudo config interface breakout Ethernet{inf} "1x100G[40G](4)" -y']
            post = [f'sudo config interface fec Ethernet{inf} rs', f"sudo config interface startup Ethernet{inf}"]
            return [breakout, post]
        else:
            raise ValueError(f"{self}: unhandled breakout mode {target_mode}")

    def prompt_detector(self) -> Callable[[bytes], bool]:
        return make_prompt_detector("#$")

class EdgecoreSupportedSwitchModels(SupportedSwitchModels):

    def __init__(self):
        supported = [
            ("AS9736", "SONiC_20231128_023225_ec202111_hsdk_6.5.23_553"),
        ]
        super().__init__(supported)


class EdgecoreSwitchCtl(SwitchCtl):
    def __init__(self, name: str, host: str, username: str, password: str):
        super().__init__(name, host, username, password)

    def vendor_cmds(self) -> SwitchCommands:
        return EdgecoreSwitchCommands()

    def model_support(self) -> SupportedSwitchModels:
        return EdgecoreSupportedSwitchModels()

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
            if "AS9736" in line:
                return "AS9736"
        return ""

    def get_fw_version(self) -> str:
        _, out, _ = self._exec.exec("show version")
        version = ""
        regex = re.compile(
            r".*SONiC Software Version:\s+SONiC.Edgecore-(?P<vers>[\d+\w+._]+).*"
        )
        for line in out.split('\n'):
            m = regex.match(line)
            if m:
                version = m.group('vers')
                break
        return version

    @staticmethod
    def parse_inf_transceiver(output: str) -> dict:
        """ Parse the semi-structured EEPROM data. Media interface code is the most reliable field but some
        older standard transceivers (SR4) use "Extended Specification Compliance"
        """
        interface = ""
        rv = {}
        for line in output.strip().split("\n"):
            if line.startswith("Ethernet"):
                interface = line.split(":")[0].strip()
            elif ":" in line:
                l, r = [e.strip() for e in line.split(":", 1)]
                if l == "Media Interface Code":
                    rv[interface] = r
                elif l == "Extended Specification Compliance" and interface not in rv:
                    rv[interface] = r
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
    

    def list_interfaces(self) -> List[SwitchInterface]:
        """ Returns normalized interface status """
        with self._exec.shell_session() as shell:
            shell.prompt_detect = self.vendor_cmds().prompt_detector()
            shell.exec("echo 'init'")  # clear banner from shell parser
            inf_output = shell.exec("show interface status")
            trans_output = shell.exec("show interfaces transceiver eeprom")

            inf_objs = parse_edgecore_sonic_table(inf_output)
            trans_objs = parse_sonic_structured_output(trans_output)

        infs: Dict[str, dict] = {i["Interface"]: i for i in inf_objs if "Interface" in i}

        # TODO: also run 'show interface description' to get desc field

        rv = []
        for inf, inf_obj in infs.items():
            port_inf = EdgecorePortInterface.from_str(inf)
            if not port_inf:
                continue

            trans_obj = trans_objs.get(inf, {})
            inf_obj["transceiver"] = trans_obj
            try:
                if trans_obj == "SFP EEPROM Not detected":
                    trans_desc = ""
                else:
                    trans_desc = trans_obj.get("Media Interface Code", "")
                    if not trans_desc and isinstance(trans_obj.get("Application Advertisement"), dict):
                        trans_desc = trans_obj.get("Application Advertisement").get("1", "")
                    if not trans_desc and isinstance(trans_obj.get("Specification compliance"), dict):
                        trans_desc = trans_obj.get("Specification compliance", {}).get("Extended Specification Compliance", "")
            except:
                trans_desc = "unknown"
            admin = inf_obj.get("Admin", "")
            oper = inf_obj.get("Oper", "")
            proto_down = inf_obj.get("ProtoDown", "unknown")

            try:
                speed_g = inf_obj.get("Speed", "")
                if speed_g.endswith("G"):
                    speed = int(speed_g[:-1]) * 1_000_000_000
                else:
                    speed = -1
            except ValueError:
                speed = -1

            rv.append(SwitchInterface(
                switch=self._name,
                name=str(port_inf),
                normalized_port=port_inf.port,
                normalized_channel=port_inf.channel,
                alias=inf_obj.get("Alias", ""),
                desc=inf_obj.get("Description", ""),
                up=admin == "up" and oper == "up" and proto_down == "False",
                status=f"admin={admin},oper={oper},proto_down={proto_down}",
                speed=speed,
                transceiver=trans_desc,
                raw=inf_obj,
            ))
        return rv
