import dataclasses
import json
import re
from typing import Dict, Optional, Union, NamedTuple


@dataclasses.dataclass
class PingTest:
    src_name: str
    src_if: str
    src_ip: str
    dst_name: str
    dst_if: str
    dst_ip: str

    kind: str = ""
    ok: Optional[bool] = None
    details: str = ""

    def __eq__(self, other):
        return self.src_ip == other.src_ip and \
               self.dst_ip == other.dst_ip and \
               self.kind == other.kind

    def is_fatal(self) -> bool:
        """ ping check failed not because the ping failed but because it failed to even start the ping """
        return self.dst_name == "*"

    def __str__(self):
        src = f"{self.src_name}:{self.src_if}" + "" if not self.src_ip else f":{self.src_ip}"
        dst = f"{self.dst_name}:{self.dst_if}:{self.dst_ip}"
        status = ""
        if self.ok is False:
            status = ":: fail"
            if self.details:
                status += f": {self.details}"
        elif self.ok is True:
            status = ":: ok"
        return f"{src} -> {dst}{status}"

    def csv(self) -> str:
        return (
            f"{self.src_name},{self.src_if},{self.src_ip},"
            f"{self.dst_name},{self.dst_if},{self.dst_ip},"
            f"{self.kind}"
        )


def clean_buffer(input_buffer: Union[str, bytes]) -> Union[str, bytes]:
    """ removes non-printing backspace characters """
    stack = []
    for char in input_buffer:
        if char == '\x08':  # Backspace character in hex
            if stack:  # remove the prior character
                stack.pop()
        else:
            stack.append(char)
    return ''.join(stack)


def removeprefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


def removesuffix(s: str, suffix: str) -> str:
    if s.endswith(suffix):
        return s[:-len(suffix)]
    return s


def parse_lldp_doc(doc: dict) -> dict:
    """
    Args:
        doc: parsed output of lldpctl -f json

    Returns: {local_ifname: {name: remote_hostname, port_id: remote_port_id, port_type: remote_port_type }}
    """
    lldp_interfaces = doc.get("lldp", {}).get("interface", {})
    if not isinstance(lldp_interfaces, list):
        lldp_interfaces = [lldp_interfaces]

    inf_remote = {}

    for lldp_obj in lldp_interfaces:
        try:
            for iface_name, iface_obj in lldp_obj.items():
                if "chassis" not in iface_obj or "port" not in iface_obj:
                    continue

                chassis_names = sorted([
                    removesuffix(n, ".cerebras.internal")
                    for n in iface_obj.get("chassis").keys()
                ])

                if len(chassis_names) != 1:
                    print(f"WARNING: expected 1 chassis name per lldp object, got: {chassis_names}")
                    continue
                chassis_name = chassis_names[0]
                port_id_obj = iface_obj.get("port", {}).get("id", {})
                port_type = port_id_obj.get("type", "")
                port_id = port_id_obj.get("value", "")
                inf_remote[iface_name] = {"name": chassis_name, "port_id": port_id, "port_type": port_type}
        except Exception as e:
            print(f"WARNING: failed to parse lldp object: {lldp_obj}, {e}")

    return inf_remote


def parse_edgecore_port_alias(alias: str) -> str:
    """
    Parse edgecore port alias format into a unix style ethernet port format, e.g.
    EthPORT/SUBPORT(PORT)
    Eth39/2(Port39) -> Ethernet306
    Eth8(Port8) -> Ethernet56
    TODO: math is based on observing switch behavior, may not applied to other edgecore switches
    """
    alias = alias.strip()
    m = re.match(r"Eth(\d+)/(\d+)\(Port\d+\)", alias)
    if m:
        # assumes the neighboring switch is 400G so each port occupies 8 slots, each subinterface 2
        # e.g. 2/1 -> port 8, 2/2 -> port 10, ...
        port = int(m.group(1))
        subport = int(m.group(2))
        unix_port = (port - 1) * 8 + (subport - 1) * 2
        return f"Ethernet{unix_port}"
    m = re.match(r"Eth(\d+)\(Port\d+\)", alias)
    if m:
        port = (int(m.group(1)) - 1) * 8
        return f"Ethernet{port}"
    return alias


def parse_switch_json_output(raw_output: bytes) -> Dict:
    """
    Extract the first full JSON object from ssh shell output and return it as a dict.
    For example,

    Last login: Fri Jun 14 06:17:31 2024 from 172.28.38.46

    terminal length 0
    terminal length 0
    sc-r7rb14-400gsw#terminal length 0
    Pagination disabled.
    sc-r7rb14-400gsw#enable
    sc-r7rb14-400gsw#show interface status | json
    {
        "interfaceStatuses": {}
    }

    returns
    {
        "interfaceStatuses": {}
    }

    This works for json output in which the opening and closing braces are on separate lines
    from the rest of the output
    """
    # extract the relevant part of the output
    jstr = ""
    token_pairs = {
        "[": "]",
        "{": "}"
    }
    starting_token = None
    ending_token = None
    num_open = 0
    num_close = 0
    for c in raw_output.decode('ASCII'):
        if c == '\n':
            continue
        if not jstr:
            if c in token_pairs.keys():
                starting_token = c
                ending_token = token_pairs[c]
                num_open += 1
                jstr = c
            continue
        jstr += c
        if c == ending_token:
            num_close += 1
        elif c == starting_token:
            num_open += 1
        if num_open == num_close:
            break

    if jstr:
        return json.loads(jstr)
    else:
        return None
