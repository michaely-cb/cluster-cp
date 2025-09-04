import base64
import json
import logging
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Callable

from deployment_manager.tools.ssh import make_prompt_detector
from deployment_manager.tools.switch.interface import MacAddrTableEntry, PortInterface, SwitchCtl, \
    SwitchInterface, SwitchCommands, \
    LldpNeighbor
from deployment_manager.tools.utils import try_parse_mac

logger = logging.getLogger(__name__)

def parse_xml_fragments(content: str) -> List[ET.Element]:
    """
    Extracts and parses XML fragments from a string that may have extraneous console output.

    The function splits the content on the XML declaration and attempts to parse each fragment.

    Returns:
        List[ET.Element]: A list of parsed XML Element trees.
    """
    # Split on XML declarations. The regex removes any XML declaration (<?xml ...?>).
    fragments = re.split(r'<\?xml.*?\?>', content)
    elements = []
    for frag in fragments:
        frag = frag.strip()
        if not frag:
            continue
        try:
            element = ET.fromstring(frag)
            elements.append(element)
        except ET.ParseError:
            continue
    return elements


class DellOS10PortInterface(PortInterface):
    """ Dell 1G interface """

    # TODO: linecard, channel handling - OS10 is for mgmt switches at the moment so don't bother initially
    _re_mgmt = re.compile(r"^(Ma\d)$")
    _re_data = re.compile(r"(^[Ee]th(ernet)?1/1/(?P<port>\d+)$)")

    def __init__(self, index: int, int_name: Optional[str] = None):
        if index == -1:
            # special case for MGMT port
            super().__init__(f"{int_name}", None, 0, None, role="mgmt")
        else:
            super().__init__(f"ethernet1/1/{index + 1}", None, index, None)

    @classmethod
    def from_str(cls, s) -> Optional['DellOS10PortInterface']:
        m_mgmt = cls._re_mgmt.match(s)
        m_data = cls._re_data.match(s)
        if (not m_mgmt) and (not m_data):
            return None
        elif m_mgmt:
            return cls(-1, int_name=s)
        else:
            return cls(int(m_data.groupdict()["port"]) - 1)

class DellOS10SwitchCommands(SwitchCommands):
    def parse_interface(self, if_name: str) -> Optional[PortInterface]:
        return DellOS10PortInterface.from_str(if_name)

    def get_enter_config_mode_cmds(self) -> List[str]:
        return ["configure"]

    def get_exit_config_mode_cmds(self) -> List[str]:
        return ["exit", "write memory",]

    def get_sleep_commands(self, seconds: int) -> List[str]:
        return [f'system "sleep {seconds}"']

    def prompt_detector(self) -> Callable[[bytes], bool]:
        return make_prompt_detector("#$")


class DellOS10SwitchCtl(SwitchCtl):

    def __init__(self, name: str, host: str, username: str, password: str):
        super().__init__(name, host, username, password)

    def vendor_cmds(self) -> SwitchCommands:
        return DellOS10SwitchCommands()

    def get_hostname(self) -> str:
        cmd = "system hostname"
        out = self.execute_commands([cmd])
        lines = "\n".join(out).splitlines()
        for i, line in enumerate(lines[:-1]):
            if cmd in line:
                return lines[i+1].strip()
        raise RuntimeError(f"cannot find hostname of switch {self._name}")

    def list_interfaces(self) -> List[SwitchInterface]:
        """ Returns normalized interface status """
        _, out, _ = self._exec.exec("show interface description | display-xml | no-more")

        interfaces: List[SwitchInterface] = []

        for root in parse_xml_fragments(out):
            for intf in root.findall(".//interface"):
                name = (intf.findtext("name") or "").strip()
                inf = DellOS10PortInterface.from_str(name)
                if not inf:
                    continue

                admin_status = (intf.findtext("admin-status") or "").strip().lower()
                oper_status = (intf.findtext("oper-status") or "").strip().lower()

                speed_text = (intf.findtext("speed") or "0").strip()
                try:
                    speed_bits = int(speed_text)
                except ValueError:
                    speed_bits = 0
                speed_gbps = speed_bits // 1000000000
                interface_obj = SwitchInterface(
                    switch=self._name,
                    name=str(inf),
                    alias="",
                    desc="",  # not present
                    up=(admin_status == "up" and oper_status == "up"),
                    status=f"admin={admin_status}/oper={oper_status}",
                    speed=speed_gbps,
                    transceiver="",  # not present, show interface ethernet | display-xml has it but is hard to parse
                    normalized_port=inf.port,
                    raw=""  # since output is xml, just ignore this
                )
                interfaces.append(interface_obj)
        return interfaces
    
    def get_mac_addr_table(self) -> List[MacAddrTableEntry]:
        _re_not_la = re.compile(r"^(?!.*port-channel).+$")
        _, stdout, _ = self._exec.exec("show mac address-table | display-xml | no-more", throw=True)
        rv = []
        for root in parse_xml_fragments(stdout):
            for table in root.findall(".//fwd-table"):
                interface = table.findtext("if-name").strip()
                # skip link-aggregation interfaces
                if _re_not_la.match(interface):
                    mac = table.findtext("mac-addr").strip()
                    rv.append(MacAddrTableEntry(
                        switch=self._name,
                        name=interface,
                        mac=mac,
                    ))
        return rv

    def list_lldp_neighbors(self) -> List[LldpNeighbor]:
        _, out, _ = self._exec.exec("show lldp neighbors | display-xml | no-more")
        neighbors: Dict[str, LldpNeighbor] = {}

        for root in parse_xml_fragments(out):
            # Find each <interface> element that includes LLDP neighbor info.
            for intf in root.findall(".//interface"):
                lldp_info = intf.find("lldp-rem-neighbor-info")
                if lldp_info is None:
                    continue
                info = lldp_info.find("info")
                if info is None:
                    continue

                src_name = (intf.findtext("name") or "").strip()
                src_if = DellOS10PortInterface.from_str(src_name)
                if not src_if or str(src_if) in neighbors:
                    continue

                # Extract neighbor details from <info>
                dst_name = (info.findtext("rem-system-name") or "").strip()
                dst_desc = (info.findtext("rem-system-desc") or "").strip()
                dst_if = (info.findtext("rem-port-desc") or "").strip()
                dst_chassis_id_type = (info.findtext("rem-lldp-chassis-id-subtype") or "").strip()
                dst_mac = ""
                if dst_chassis_id_type == "mac-address":
                    dst_chassis_id = (info.findtext("rem-lldp-chassis-id") or "").strip()
                    mac_bytes = base64.b64decode(dst_chassis_id)
                    dst_mac = try_parse_mac(":".join(f"{b:02X}" for b in mac_bytes)) or ""

                neighbors[str(src_if)] = LldpNeighbor(
                    src=self._name,
                    src_if=str(src_if),
                    dst_name=dst_name,
                    dst_desc=dst_desc,
                    dst_if=dst_if,
                    dst_if_mac=dst_mac,
                    raw="",
                )
        return list(neighbors.values())
