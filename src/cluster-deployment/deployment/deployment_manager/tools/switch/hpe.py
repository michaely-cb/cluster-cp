import logging
import re
from typing import List, Optional, Callable

from deployment_manager.tools.ssh import make_prompt_detector
from deployment_manager.tools.switch.interface import PortInterface, SFlowConfig, SFlowStatus, SwitchCtl, SwitchInterface, SwitchCommands, \
    BreakoutMode, GBPS, LldpNeighbor, SupportedSwitchModels
from deployment_manager.tools.utils import strip_json_dict, strip_json_list
from deployment_manager.tools.utils import try_parse_mac

logger = logging.getLogger(__name__)

HPE_MODEL_256 = "12908" # TODO: remove once we have separate Ctl classes for HPE Onyx and Comware

class HpePortInterface(PortInterface):
    """ Only support first line card of HPE switches, unlikely to need to handle more cases """
    _re = re.compile(r"^Eth1/(\d+)$")

    def __init__(self, port: int):
        super().__init__(f"Eth1/{port + 1}", None, port, None)

    @classmethod
    def from_str(cls, s: str) -> Optional['HpePortInterface']:
        m = cls._re.match(s)
        if m:
            return cls(int(m.group(1)) - 1)
        return None


class HpeSwitchCommands(SwitchCommands):
    def parse_interface(self, if_name: str) -> Optional[PortInterface]:
        return HpePortInterface.from_str(if_name)

    def get_enter_config_mode_cmds(self) -> List[str]:
        return ["configure terminal"]

    def get_exit_config_mode_cmds(self) -> List[str]:
        return ["exit", "write memory"]

    def get_sleep_commands(self, seconds: int) -> List[str]:
        return [f"ping -c1 -W{seconds} 127.0.0.100"]  # hack - ping a not-exist local address for timeout

    def get_sflow_config_commands(self, interfaces: List[SwitchInterface], status: SFlowStatus, conf: SFlowConfig) -> List[str]:
        raise NotImplementedError(f"get_sflow_config_commands not implemented for {self.__class__}")

    def get_sflow_clear_commands(self) -> List[str]:
        raise NotImplementedError(f"get_sflow_clear_commands not implemented for {self.__class__}")

    def get_port_breakout_cmds(
            self, port: int, current_mode: BreakoutMode, target_mode: BreakoutMode
    ) -> List[List[str]]:
        """
        HPE switches in inventory are only 100G. There are some cases where we must force the interface speed to be
        100Gx4 when the switch doesn't recognize certain transceivers but this is not handled today.
        """
        raise ValueError(f"{self}: unhandled breakout mode {target_mode}")

    def prompt_detector(self) -> Callable[[bytes], bool]:
        return make_prompt_detector(">#$]")

class HpeSupportedSwitchModels(SupportedSwitchModels):

    def __init__(self):
        supported = [
            ("12908", "9.1.058"),
            ("3700", "3.10.4006"),
            ("4600", "3.10.4006"),
        ]
        super().__init__(supported)


class HpeSwitchCtl(SwitchCtl):
    def __init__(self, name: str, host: str, username: str, password: str):
        super().__init__(name, host, username, password)

    def vendor_cmds(self) -> SwitchCommands:
        return HpeSwitchCommands()

    def model_support(self) -> SupportedSwitchModels:
        return HpeSupportedSwitchModels()

    def get_hostname(self) -> Optional[str]:
        out = self.execute_commands(["en", "show running-config | include hostname"])
        for line in "\n".join(out).splitlines():
            m = re.match(r"^ *hostname +([a-zA-Z0-9_-]+)$", line)
            if m:
                return m.group(1)
        return None

    def get_model(self) -> str:
        out = self.execute_commands(["en", "show system type"])
        for line in "\n".join(out).splitlines():
            if "SN3700" in line:
                return "3700"
            elif "SN4600" in line:
                return "4600"
            # 12908 is a 256-port HPE switch which has a different
            # command syntax compared to other switches. It's the only
            # one with a different syntax as of now. So we can assume
            # it to be the model if commands go unrecognized.
            elif "Unrecognized command" in line:
                return HPE_MODEL_256
        return ""

    def get_fw_version(self) -> str:
        # TODO: model-specific code is temporary till we add
        # separate Ctl classes for HPE onyx and comware
        out = "\n".join(self.execute_commands(["en"]))
        if "Unrecognized command" in out:
            # HPE comware modular switch
            self.refresh_connection()  # can't open multiple shell sessions on same ssh conn
            cmds = ["screen-length disable", "system-view", "show version"]
            out = "\n".join(self.execute_commands(cmds))
            regex = re.compile(r".*HPE Comware Software, Version (?P<vers>[\d+.]+), Release \d+\w+$")
        else:
            # HPE mellanox switch
            out = "\n".join(self.execute_commands(["en", "show version"]))
            regex = re.compile(r".*Product release:\s+(?P<vers>[\d+.]+)$")

        version = "unknown"
        for line in out.splitlines():
            m = regex.match(line)
            if m:
                version = m.group('vers')
                break
        return version

class HpeComwareSwitchCtl(HpeSwitchCtl):
    """
    HPE Comware switchctl class.
    """
    def __init__(self, name: str, host: str, username: str, password: str):
        super().__init__(name, host, username, password)
    
    def list_interfaces(self) -> List[SwitchInterface]:
        
        rv = []
        with self._exec.shell_session() as shell:
            shell.prompt_detect = self.vendor_cmds().prompt_detector()
            shell.exec("screen-length disable")  # Disable paging
            inf_out = shell.exec("display interface main")
        
        lines = inf_out.split("\n")

        # Split the output into sections by interface
        sections = []
        i = 0
        start = -1
        while i < len(lines):
            if "Current state" in lines[i]:
                start = i
            elif start != -1 and (lines[i].strip() == "" or i == len(lines) - 1):
                sections.append(lines[start-1:i + 1])
                start = -1
            i += 1

        # Extract interface information from each section
        for section in sections:
            port_inf = section[0].strip()
            trans_desc = ""
            desc = ""
            speed = "-1"
            status = "unknown"

            if "Gig" not in port_inf:
                continue
            
            for line in section:
                if "port hardware type" in line:
                    # get this from display transceiver interface?
                    trans_desc = line.split("is")[-1].strip()
                elif line.startswith("Description:"):
                    desc = line.split(":", 1)[1].strip()
                elif line.startswith("Current state:"):
                    status = line.split(":", 1)[1].strip()
                elif line.startswith("Bandwidth:"):
                    speed = line.split()[1].strip()
                
            rv.append(SwitchInterface(
                switch=self._name,
                name=port_inf,
                normalized_port=port_inf.split("/")[-1],
                normalized_channel=0, 
                alias="",
                desc=desc,
                up="UP" in status,
                status=status,
                speed=speed,
                transceiver=trans_desc,
                raw={}
            ))

        return rv

    def list_lldp_neighbors(self) -> List[LldpNeighbor]:
        """ Returns normalized LLDP neighbor list """
        rv = []
        try:
            with self._exec.shell_session() as shell:
                shell.prompt_detect = self.vendor_cmds().prompt_detector()
                shell.exec("screen-length disable")  # Disable paging
                stdout = shell.exec("show lldp neighbor-information verbose")

                # Phase 1: parse the text buffer
                port_re = re.compile(r'^LLDP neighbor-information of port \d+\[([/a-zA-Z0-9_-]+)\]')
                neighbor_re = re.compile(r'^\s+LLDP neighbor index\s+:\s+\d+')
                port_id_re = re.compile(r'^\s+Port ID\s+:\s+([/a-zA-Z0-9_-]+)')
                port_desc_re = re.compile(r'^\s+Port description\s+:\s+([/a-zA-Z0-9_\s-]+)')
                neighbor_desc_re = re.compile(r'^\s+System name\s+:\s+([/a-zA-Z0-9_\s-]+)')

                ### parse the output
                
                lldp_accum = list()
                entry = {}

                for line in stdout.splitlines():
                    match_obj = port_re.match(line)
                    if match_obj:
                        entry = {
                            "my_port": match_obj.group(1),
                            "neighbor_port_id": "",
                            "neighbor_port": "",
                            "neighbor_name": ""
                        }
                        continue

                    match_obj = neighbor_re.match(line)
                    if match_obj:
                        entry["neighbor_port_id"] = ""
                        entry["neighbor_port"] = ""
                        entry["neighbor_name"] = ""
                        continue

                    match_obj = port_id_re.match(line)
                    if match_obj:
                        entry["neighbor_port_id"] = match_obj.group(1)
                        continue

                    match_obj = port_desc_re.match(line)
                    if match_obj:
                        entry["neighbor_port"] = match_obj.group(1)
                        continue

                    match_obj = neighbor_desc_re.match(line)
                    if match_obj and entry["neighbor_port"]:
                        entry["neighbor_name"] = match_obj.group(1)
                        lldp_accum.append(entry)
                

                for la in lldp_accum:
                    if "HundredGig" not in la["my_port"]:
                        continue
                    # system port
                    if la["neighbor_name"].startswith("system") or la["neighbor_name"].startswith("xs"):
                        rv.append(LldpNeighbor(
                            src=self._name,
                            src_if=la["my_port"],
                            dst_name=la["neighbor_name"],
                            dst_if=la["neighbor_port"],
                            dst_if_mac=la["neighbor_port_id"],
                        ))
                        continue
                    # switch port
                    if "eth" in la["neighbor_port_id"].lower():
                        rv.append(LldpNeighbor(
                            src=self._name,
                            src_if=la["my_port"],
                            dst_name=la["neighbor_name"],
                            dst_if=la["neighbor_port_id"],
                        ))
                    else:
                        # node port
                        rv.append(LldpNeighbor(
                            src=self._name,
                            src_if=la["my_port"],
                            dst_name=la["neighbor_name"],
                            dst_if=la["neighbor_port"],
                            dst_if_mac=la["neighbor_port_id"],
                        ))

                return rv
        except Exception as e:
            logger.warning(f"failed to obtain lldp doc from {self._name}: {e}")
            return rv



class HpeOnyxSwitchCtl(HpeSwitchCtl):
    """
    HPE Onyx switchctl class.
    """
    def __init__(self, name: str, host: str, username: str, password: str):
        super().__init__(name, host, username, password)
    
    def list_interfaces(self) -> List[SwitchInterface]:
        """ Returns normalized interface status """
        with self._exec.shell_session() as shell:
            shell.prompt_detect = self.vendor_cmds().prompt_detector()
            shell.exec("en")
            shell.exec("no cli session paging enable")
            inf_dict = strip_json_dict(shell.exec("show interface ethernet description | json").strip())
            trans_doc = strip_json_list(shell.exec("show interface ethernet transceiver | json"))

        port_trans = {}
        for item in trans_doc:
            try:
                header = item["header"]  # "Port 1/PORT state"
                port = int(header.split("/")[1].split(" ")[0])
                port_trans[port] = item
            except Exception as e:
                logger.debug(f"failed to parse HPE transceiver item {item}: {e}")

        rv = []
        for port, inf_obj in inf_dict.items():
            if not port.startswith("Eth"):
                continue
            if isinstance(inf_obj, list):
                inf_obj = inf_obj[0]

            port_inf = HpePortInterface.from_str(port)

            oper = inf_obj.get("Operational state", "unknown")
            admin = inf_obj.get("Admin state", "unknown")
            speed_desc = inf_obj.get("Actual speed", '')
            m = re.match(r"^(\d+)G.*", speed_desc)
            if m:
                speed = int(m.group(1)) * GBPS
            else:
                speed = -1

            trans = port_trans.get(port_inf.port + 1, {})
            trans_desc = "" if not isinstance(trans, dict) else trans.get("ethernet speed and type", "")
            inf_obj["transceiver"] = trans

            rv.append(SwitchInterface(
                switch=self._name,
                name=str(port_inf),
                normalized_port=port_inf.port,
                normalized_channel=port_inf.channel,
                alias="",
                desc=inf_obj.get("Description", ""),
                up=oper == "Up" and admin == "Enabled",
                status=f"oper={oper},admin={admin}",
                speed=speed,
                transceiver=trans_desc,
                raw=inf_obj,
            ))
        return rv
    
    def list_lldp_neighbors(self) -> List[LldpNeighbor]:
        rv = []
        try:
            with self._exec.shell_session() as shell:
                shell.prompt_detect = self.vendor_cmds().prompt_detector()
                shell.exec("en")
                shell.exec("no cli session paging enable")
                output = shell.exec("show lldp remote | json-print")
                doc = strip_json_dict(output)
        except Exception as e:
            logger.warning(f"failed to obtain lldp doc from {self._name}: {e}")
            return rv

        for k, v in doc.items():
            for dst in v:
                mac = try_parse_mac(dst.get("Device ID", ""))
                rv.append(LldpNeighbor(
                    src=self._name,
                    src_if=k,
                    dst_name=dst.get("System Name", ""),
                    dst_if=dst.get("Port ID", ""),
                    dst_if_mac=mac,
                    raw=dst,
                ))
        return rv