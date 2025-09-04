import abc
import dataclasses
import functools
import re
import typing
from enum import Enum
from functools import cached_property
from typing import Dict, NamedTuple, Optional, List, Any, Tuple, Iterable, Callable

from deployment_manager.tools.ssh import ExecMixin, SSHConn, is_prompt_simple
from deployment_manager.tools.switch.utils import interface_sort_key
from deployment_manager.tools.utils import ReprFmtMixin, to_duration

GBPS = 10 ** 9

class SwitchOSFamily:
    ARISTA_EOS = "eos"
    DELL_OS10 = "os10"
    DELL_SONIC = "dell_sonic"
    EDGECORE_SONIC = "edgecore_sonic"
    HPE = "hpe"
    HPE_COMWARE = "hpe_comware"
    JUNIPER_JUNOS = "junos"
    NOT_SET = "not_set"

class SwitchInfo(NamedTuple):
    name: str
    host: str
    vendor: str
    username: str
    password: str
    os_family: SwitchOSFamily = SwitchOSFamily.NOT_SET

class SwitchModelFirmware(NamedTuple):
    model: str
    firmware_version: str

@dataclasses.dataclass
class SwitchInterface(ReprFmtMixin):
    switch: str
    name: str  # interface name
    alias: str
    desc: str
    up: bool  # aggregate operational status
    status: str  # status details
    speed: int  # GBPS
    transceiver: str
    raw: Any  # raw output from switch's CLI

    normalized_linecard: int = 0 # line card, zero indexed
    normalized_port: int = 0  # physical port index, zero indexed
    normalized_channel: int = 0  # Zero-indexed channel

    @functools.cached_property
    def sort_key(self) -> Tuple[str, int, int, int]:
        return self.switch, self.normalized_linecard, self.normalized_port, self.normalized_channel

    def to_table_row(self) -> List[str]:
        return [self.switch, self.name, self.desc, self.up, self.status, self.speed, self.transceiver]

    @classmethod
    def table_header(cls) -> List[str]:
        return ["Switch", "Name", "Desc", "Up", "Status", "Speed", "Transceiver"]

    def to_dict(self) -> dict:
        return self.__dict__


@dataclasses.dataclass
class InterfaceErrStats(ReprFmtMixin):
    """ Stats requested by field team for debugging links. Not all switch vendors will support all stats
    """
    switch: str
    name: str  # interface name

    up: bool  # aggregate operational status
    state_duration: int  # seconds since last up/down state transition

    min_rx_power: int  # min rx power reported for the lanes of the interface
    min_tx_power: int  # min tx power reported for the lanes of the interface

    # a measure of the number of corrected symbols in a codeword. Only present in Arista.
    # the sum of the latter half of phy errs in corrected errs histogram
    # see: https://arista.my.site.com/AristaCommunity/s/article/monitoring-link-quality-using-forward-error-correction-fec-data-on-arista-switches
    tail_phys_errs: int

    raw: Any  # raw output from switch's CLI

    @functools.cached_property
    def sort_key(self):
        return self.switch, interface_sort_key(self.name)

    @classmethod
    def table_header(cls) -> List[str]:
        return ["switch", "name", "up", "state_duration", "min_rx_power", "tail_phys_errs"]

    def to_table_row(self) -> List[str]:
        def fmt_power(f):
            return "" if not f else f"{f:.1f}"
        return [self.switch, self.name,
                self.up, to_duration(self.state_duration),
                fmt_power(self.min_rx_power),
                self.tail_phys_errs]

    @classmethod
    def csv_header(cls) -> List[str]:
        return ["switch", "name", "up", "state_duration", "min_rx_power", "min_tx_power", "tail_phys_errs"]

    def to_csv_row(self) -> List[str]:
        return [self.switch, self.name, self.up, self.state_duration, self.min_rx_power, self.min_tx_power, self.tail_phys_errs]

    def to_dict(self) -> dict:
        return self.__dict__

@dataclasses.dataclass
class MacAddrTableEntry(ReprFmtMixin):
    switch: str  # switch name
    name: str  # interface name
    mac: str  # MAC address

    @classmethod
    def table_header(cls) -> List[str]:
        return ["Switch", "Name", "MAC"]
    
    def to_table_row(self) -> List[str]:
        return [self.switch, self.name, self.mac]
    
    def to_dict(self) -> dict:
        return self.__dict__

@dataclasses.dataclass
class LldpNeighbor(ReprFmtMixin):
    src: str  # switch name
    src_if: str  # switch port
    dst_name: str = ""  # chassis name
    dst_desc: str = ""  # chassis desc
    dst_if: str = ""
    dst_if_mac: str = ""
    raw: typing.Any = None

    def to_table_row(self) -> List[str]:
        return [self.src, self.src_if, self.dst_name, self.dst_if, self.dst_if_mac,
                self.dst_desc if len(self.dst_desc) < 30 else self.dst_desc[0:27] + "..."]

    @functools.cached_property
    def sort_key(self) -> Tuple[str,str, str]:
        return self.src, interface_sort_key(self.src_if), self.dst_name,

    @classmethod
    def table_header(cls) -> List[str]:
        return ["Switch", "Interface", "DstName", "DstIf", "DstIfMac", "DstDesc"]

    def to_dict(self) -> dict:
        return self.__dict__

@dataclasses.dataclass
class SFlowStatus(ReprFmtMixin):
    src: str  # switch name
    enabled: bool # whether sampling is enabled
    sampling_rate: int # 1 in every x packet is sampled
    poll_interval: int # poll interface counters every x seconds
    agent_ip: str # agent IPv4 address (in sFlow header)
    src_ip: str # IPv4 source address (in IP header)
    dsts: List[Tuple[str, Optional[int]]] # collector addresses (IPv4 + UDP port)
    interfaces: Dict[str, bool] # enabled interfaces
    sample_cnt: int # total packets sampled
    warnings: List[str] # warnings reported by switch

    DEFAULT_SFLOW_PORT = 6343  # default sFlow collector UDP port

    def _fmt_dsts(self) -> str:
        def fmt_dst(dst: Tuple[str, Optional[int]]):
            addr, p = dst
            p = "" if p in [None, self.DEFAULT_SFLOW_PORT] else f":{p}"
            return f"{addr}{p}"

        return ",".join(map(fmt_dst, self.dsts))

    def _fmt_warnings(self) -> str:
        return ". ".join(self.warnings)

    def _fmt_interfaces(self) -> str:
        all_enabled = all(self.interfaces.values())
        all_disabled = not any(self.interfaces.values())
        if all_enabled:
            return "All enabled"
        elif all_disabled:
            return "All disabled"
        enabled_intfs = [intf for intf, enbld in self.interfaces.items() if enbld]
        all_eth = all(intf.startswith("Ethernet") for intf in enabled_intfs)
        if all_eth:
            just_nums = map(lambda i: i.replace("Ethernet", "").strip(), enabled_intfs)
            return "Eth " + ",".join(just_nums)
        return ",".join(enabled_intfs)

    def to_table_row(self) -> List[str]:
        return list(map(str, [
            self.src,
            self.enabled,
            self.sampling_rate,
            self.poll_interval,
            self.agent_ip,
            self.src_ip,
            self._fmt_dsts(),
            self._fmt_interfaces(),
            self.sample_cnt,
            self._fmt_warnings(),
        ]))

    @functools.cached_property
    def sort_key(self) -> Tuple[str]:
        return self.src,

    @classmethod
    def table_header(cls) -> List[str]:
        return [
            "Switch",
            "Enabled",
            "SamplingRate",
            "PollInterval",
            "Agent",
            "SourceIP",
            "Dsts",
            "EnabledInterfaces",
            "SampleCnt",
            "Warnings",
        ]

    def to_dict(self) -> dict:
        return self.__dict__

@dataclasses.dataclass
class SFlowConfig(ReprFmtMixin):
    enable: bool # whether to enable packet sampling
    sampling_rate: int # 1 in every x packet is sampled
    poll_interval: int # poll interface counters every x seconds
    agent_ip: str # agent IPv4 address (in sFlow header)
    src_ip: str # IPv4 source address (in IP header)
    dsts: List[Tuple[str, Optional[int]]] # collector addresses (IPv4 + UDP port)
    # Only configure these interfaces. If None, configure all interfaces
    configure_interfaces: Optional[List[str]]

    def to_table_row(self) -> List[str]:
        return [
            self.enable,
            self.sampling_rate,
            self.poll_interval,
            self.agent_ip,
            self.src_ip,
            self.dsts,
            self.configure_interfaces,
        ]

    @functools.cached_property
    def sort_key(self) -> Tuple[str]:
        return self.src_ip

    @classmethod
    def table_header(cls) -> List[str]:
        return ["Enable", "SamplingRate", "PollInterval", "Agent", "SourceIP", "Dsts", "Interfaces"]

    def to_dict(self) -> dict:
        return self.__dict__

class PortInterface(abc.ABC):
    """
    Simple generic representation for an interface on a switch. Used to normalize port numberings across vendors.
    port should be zero-indexed, e.g. on Arista, the first interface is Eth1/1 which corresponds to port=0,channel=0.
    Channels may be non-contiguous so on Arista 800g 2x400g interface Eth20/1 would be {port=19, channel=0} and
    Eth20/5 -> {port=19, channel=4}

    When linecard is None, implies the switch that the switch's interface spec doesn't include the linecard in the
        interface name. If the linecard is zero, it might that the switch doesn't support line cards but the linecard
        is still present in the interface spec (e.g. juniper et-0/0/8's could be Arista Eth1/1/9, Eth1/9 or Eth9
        depending on if the Arista model supports linecards and/or port breakouts).
    When channel is None, implies the port cannot be broken out into multiple logical interfaces.

    TODO: support different interface types (e.g. Juniper xe, ge, et, ...)
    """

    def __init__(self, if_name: str, linecard: Optional[int], port: int, channel: Optional[int], role: Optional[str] = None):
        self.if_name = if_name
        self.linecard: Optional[int] = linecard
        self.port: int = port
        self.channel: Optional[int] = channel
        self.role: Optional[str] = role

    @classmethod
    @abc.abstractmethod
    def from_str(cls, s: str) -> Optional['PortInterface']:
        raise NotImplementedError(f"from_str({s}) not implemented for {cls.__name__}")

    def __str__(self):
        return self.if_name

    def __repr__(self):
        return f"{self.__class__.__name__}[{self.if_name},{self.linecard},{self.port},{self.channel}]"

    def __eq__(self, other):
        if isinstance(other, (PortInterface, )):
            return str(other) == str(self)
        return False

    def __hash__(self):
        return str(self).__hash__()

class BreakoutMode(Enum):
    """ Simple generic representation for port breakout modes """
    Mode800X1 = "800x1"
    Mode400X2 = "400x2"
    Mode400X1 = "400x1"
    Mode100X8 = "100x8"
    Mode100X4 = "100x4"
    Mode100X1 = "100x1"

    @cached_property
    def speed(self) -> int:
        return int(self.value.split("x")[0])

    @cached_property
    def subports(self) -> int:
        return int(self.value.split("x")[1])

    @classmethod
    def modes(cls):
        return [cls.Mode800X1,
                cls.Mode400X2,
                cls.Mode400X1,
                cls.Mode100X8,
                cls.Mode100X4,
                cls.Mode100X1,
                ]


@dataclasses.dataclass(frozen=True)
class Transceiver:
    """
    400G-DR4: Fiber links to a 4x100G breakout panel
    CR8: Copper DAC cable directly connecting 100G interfaces
    100GBASE-SR4: Fiber links to IT storage
    """
    kind: str
    breakout_modes: Iterable[BreakoutMode]


_transceiver_matchers = [
    (re.compile(r"100gbase-sr4.*"), Transceiver("100g-sr4", (BreakoutMode.Mode100X1,))),
    (re.compile(r"400gbase-sr4"), Transceiver("400g-sr4", (BreakoutMode.Mode100X4,))),
    (re.compile(r"400gbase-dr4"), Transceiver("400g-dr4", (BreakoutMode.Mode400X1, BreakoutMode.Mode100X4,))),
    (re.compile(r"400gbase-vr4.*"), Transceiver("400g-vr4", (BreakoutMode.Mode400X1, BreakoutMode.Mode100X4,))),
    (re.compile(r"copper cable.*"), Transceiver("100g dac", (BreakoutMode.Mode100X4,))),
    (re.compile(r"400gbase-cr8"), Transceiver("400g-cr8", (BreakoutMode.Mode100X4,))),
    (re.compile(r"2x400g-dr4"), Transceiver("2x400g-dr4", (BreakoutMode.Mode800X1, BreakoutMode.Mode400X2,))),
    (re.compile(r"400gbase-cra8"), Transceiver("400g-cra8", (BreakoutMode.Mode100X4,))),
    (re.compile(r"400gbase-sr8"), Transceiver("400g-cr8", (BreakoutMode.Mode400X1, BreakoutMode.Mode100X4,))),
    (re.compile(r"4x.100gbase-cr8-dac."), Transceiver("4x100g-cr8", (BreakoutMode.Mode100X4,))),
    (re.compile(r"800g-vr8-dual-mpo.*"),
     Transceiver("800g-vr8", (BreakoutMode.Mode800X1, BreakoutMode.Mode400X2, BreakoutMode.Mode100X8))),
    (re.compile(r"800g(base)?-dr8.*"),
     Transceiver("800g-dr8", (BreakoutMode.Mode800X1, BreakoutMode.Mode400X2, BreakoutMode.Mode100X8))),
    (re.compile(r"800g(base)?-2dr4.*"),
     Transceiver("800g-2dr4", (BreakoutMode.Mode400X2,))),
]


def parse_transceiver(desc: str) -> Optional['Transceiver']:
    d = desc.lower()
    for regex, t in _transceiver_matchers:
        if regex.match(d):
            return t
    return None


class SwitchCommands(abc.ABC):
    """ Base class for sequence of vendor specific commands """
    @abc.abstractmethod
    def parse_interface(self, if_name: str) -> Optional[PortInterface]:
        """ Parse an IF name into normalized PortInterface """
        pass

    @abc.abstractmethod
    def get_enter_config_mode_cmds(self) -> List[str]:
        """ Assuming admin user just started a new shell session, enter config mode """
        pass

    @abc.abstractmethod
    def get_exit_config_mode_cmds(self) -> List[str]:
        """ Assuming admin user in state that enter_config_mode entered, exit config mode """
        pass

    @abc.abstractmethod
    def get_sleep_commands(self, seconds: int) -> List[str]:
        """ Assuming admin user in fresh shell session, sleep for SECONDS """
        pass

    def get_sflow_config_commands(self, interfaces: List[SwitchInterface], status: SFlowStatus, conf: SFlowConfig) -> List[str]:
        """ Apply the sflow configuration. """
        raise NotImplementedError(f"{self.__class__.__name__} doesn't implement get_sflow_config_commands()")

    def get_sflow_clear_commands(self) -> List[str]:
        """ Clear sFlow counters. """
        raise NotImplementedError(f"{self.__class__.__name__} doesn't implement get_sflow_clear_commands()")

    def get_port_breakout_cmds(
            self, port: int, current_mode: BreakoutMode, target_mode: BreakoutMode
    ) -> List[List[str]]:
        """ Given a port, the current breakout mode, and the target mode, produce lists of commands where a list of
         commands should be executed, then some time given to wait for the switch to async process the command, and then
         the next list of commands executed. The process repeats until all the commands are issued.
        """
        raise NotImplementedError(f"{self.__class__.__name__} doesn't implement get_port_breakout_cmds()")

    def prompt_detector(self) -> Callable[[bytes], bool]:
        return is_prompt_simple


class _PortSpeedAccum:
    def __init__(self, native_speed_gbps: int):
        self.native_speed_gbps = native_speed_gbps
        self.port_inf_count_current = {}
        self.port_inf_count_target = {}
        self.port_speed_target = {}
        self.port_speed_current = {}
        self.port_speed_modes: Dict[int, Iterable[BreakoutMode]] = {}
        self.ignore_ports = set()

    def set_current_speeds(self, ifs: Iterable[SwitchInterface]) -> List[str]:
        """ accumulate information about current interfaces, return a list of warnings of inconsistencies for current
        ports
        """
        errs = []
        for inf in ifs:
            if inf.normalized_port in self.ignore_ports:
                continue

            trans = parse_transceiver(inf.transceiver)
            if trans:
                self.port_speed_modes[inf.normalized_port] = trans.breakout_modes
            elif inf.transceiver:
                errs.append(f"unknown transceiver for port {inf.normalized_port}: '{inf.transceiver}'. "
                            "Ignoring port!")
                self.ignore_ports.add(inf.normalized_port)

            existing_speed = self.port_speed_current.get(inf.normalized_port, -1)
            speed_gbps = inf.speed // GBPS
            if existing_speed != -1 and existing_speed != speed_gbps:
                errs.append(f"inconsistent state from switch: port {inf.normalized_port} set to multiple speeds "
                            f"which is not supported ({existing_speed}, {speed_gbps})")
                self.ignore_ports.add(inf.normalized_port)
                continue

            self.port_inf_count_current[inf.normalized_port] = self.port_inf_count_current.get(inf.normalized_port, 0) + 1
            self.port_speed_current[inf.normalized_port] = speed_gbps

        return errs

    def set_target_speeds(self, port_speed_target: Dict[PortInterface, int]) -> List[str]:
        """ accumulate target port speed information, return list of warnings for inconsistent or incompatible targets
        """
        errs = []
        for port_if, speed in port_speed_target.items():
            if port_if.port in self.ignore_ports:
                continue
            if speed > self.native_speed_gbps:
                errs.append(f"{port_if} speed target of {speed} is greater than "
                            f"the speed {self.native_speed_gbps} of the switch")
                self.ignore_ports.add(port_if.port)
                continue

            if port_if.port not in self.port_speed_current:
                errs.append(f"port for interface {port_if} was not found on this switch's interface output")
                self.ignore_ports.add(port_if.port)
                continue

            current_speed_target = self.port_speed_target.get(port_if.port, -1)
            if current_speed_target != -1 and current_speed_target != speed:
                errs.append(f"port for interface {port_if} set to multiple speeds "
                            f"which is not supported ({current_speed_target}, {speed})")
                self.ignore_ports.add(port_if.port)
                continue

            target_if_count = self.port_inf_count_target.get(port_if.port, 0)
            if target_if_count + 1 > (self.native_speed_gbps // speed):
                errs.append(f"port for interface {port_if} speed requested more interfaces ({target_if_count + 1}) "
                            f"than could be supported by target speed ({speed})")
                self.ignore_ports.add(port_if.port)
                continue

            self.port_inf_count_target[port_if.port] = target_if_count + 1
            self.port_speed_target[port_if.port] = speed

        return errs

    def determine_breakouts(self) -> Tuple[Dict[int, BreakoutMode], List[str]]:
        breakouts, warnings = {}, []
        for port in sorted(list(self.port_speed_target.keys())):
            target_speed = self.port_speed_target[port]
            if port in self.ignore_ports:
                continue

            if self.native_speed_gbps == 400:
                if target_speed == 400:
                    continue

                port_modes = self.port_speed_modes.get(port, [])
                if not port_modes:
                    warnings.append(f"no transceiver for port {port}. "
                                    "Breakouts will be inferred from link information, possibly incorrect!")

                if self.port_inf_count_target.get(port, 0) > 1:
                    # must be 100x4 since we don't support other breakout modes
                    breakouts[port] = BreakoutMode.Mode100X4
                elif port_modes and BreakoutMode.Mode100X1 not in port_modes:
                    breakouts[port] = BreakoutMode.Mode100X4
                    warnings.append(f"warning: assuming port {port} is mode 100x4 not 100x1 because although there "
                                    "is only 1 interface on this port, the transceiver does not support 1x100 mode")
                else:
                    breakouts[port] = BreakoutMode.Mode100X1

            elif self.native_speed_gbps == 800:
                if target_speed == 800:
                    breakouts[port] = BreakoutMode.Mode800X1
                    continue

                if self.port_inf_count_target.get(port, 0) > 2 or target_speed == 100:
                    breakouts[port] = BreakoutMode.Mode100X8
                elif target_speed == 400:
                    breakouts[port] = BreakoutMode.Mode400X2
                else:
                    warnings.append(f"warning: unknown breakout mode for port {port}")
            else:
                warnings.append(f"warning: breakouts for switches with speed {self.native_speed_gbps} not supported")
                break
        return breakouts, warnings

    def estimate_current_mode(self, port: int) -> BreakoutMode:
        """ Return best guess at what the port breakout is. Defaults to the native mode of the switch """
        native_mode, est_mode = None, None
        subports = self.port_inf_count_current.get(port, 1)
        speed = self.port_speed_current.get(port, 1)
        for mode in BreakoutMode.modes():
            if mode.speed == self.native_speed_gbps and mode.subports == 1:
                native_mode = mode
            if mode.speed == speed and subports == mode.subports:
                est_mode = mode
        if not native_mode:
            assert f"missing native speed mode for speed={self.native_speed_gbps}"
        return est_mode if est_mode else native_mode

class SupportedSwitchModels:

    def __init__(self, model_firmware_list: List[Tuple]):
        self._supported_model_fw = [SwitchModelFirmware(*mfw) for mfw in model_firmware_list]

    def is_supported_model(self, model: str) -> bool:
        return model in self.supported_models()

    def is_supported_firmware(self, model: str, firmware: str) -> bool:
        return SwitchModelFirmware(model, firmware) in self._supported_model_fw

    def firmwares_for_model(self, model: str) -> List[str]:
        return [s.firmware_version for s in self._supported_model_fw if s.model == model]

    def supported_models(self) -> List[str]:
        return [s.model for s in self._supported_model_fw]


class SwitchCtl:

    def __init__(self, name: str, host: str, username: str, password: str):
        self._name = name
        self._host = host
        self._username = username
        self._password = password
        self._exec: Optional[ExecMixin] = None

    def __enter__(self) -> 'SwitchCtl':
        self._exec = SSHConn(self._host, self._username, self._password)
        self._exec.__enter__()
        return self

    def __exit__(self, *args):
        if isinstance(self._exec, SSHConn):
            self._exec.__exit__(*args)
            self._exec = None

    def refresh_connection(self):
        self.__exit__(None, None, None)
        self.__enter__()

    def vendor_cmds(self) -> SwitchCommands:
        raise NotImplementedError(f"commands not implemented for {self.__class__}")

    def model_support(self) -> SupportedSwitchModels:
        raise NotImplementedError(f"supported switch models not specified for {self.__class__}")

    def get_hostname(self) -> str:
        raise NotImplementedError(f"get_hostname not implemented for {self.__class__}")

    def get_model(self) -> str:
        raise NotImplementedError(f"get_model not implemented for {self.__class__}")

    def get_fw_version(self) -> str:
        raise NotImplementedError(f"get_fw_version not implemented for {self.__class__}")

    def get_max_port_speed_gbps(self) -> int:
        raise NotImplementedError(f"get_max_port_speed not implemented for {self.__class__}")

    def list_interfaces(self) -> List[SwitchInterface]:
        raise NotImplementedError(f"list_interfaces not implemented for {self.__class__}")

    def list_interfaces_err_stats(self) -> List[InterfaceErrStats]:
        raise NotImplementedError(f"list_interfaces_err_stats not implemented for {self.__class__}")

    def get_mac_addr_table(self) -> List[MacAddrTableEntry]:
        raise NotImplementedError(f"get_mac_addr_table not implemented for {self.__class__}")
    
    def list_lldp_neighbors(self) -> List[LldpNeighbor]:
        raise NotImplementedError(f"list_lldp_neighbors not implemented for {self.__class__}")

    def sflow_status(self) -> SFlowStatus:
        raise NotImplementedError(f"sflow_status not implemented for {self.__class__}")

    def sflow_configure(self, conf: SFlowConfig) -> bool:
        raise NotImplementedError(f"sflow_configure not implemented for {self.__class__}")
    
    def execute_commands(self, cmds: List[str]) -> List[str]:
        """ Returns: all command output """
        if not cmds:
            return []
        output = []
        with self._exec.shell_session() as sh:
            sh.prompt_detect = self.vendor_cmds().prompt_detector()
            for action in cmds:
                # hack for long sleeps, otherwise 2 minutes. In reality timeouts will need to be tuned per command
                # and generic command execution will not work in all cases
                timeout = 60 * 60 if "sleep " in action else 120
                output.append(sh.exec(action, timeout=timeout))
        return output

    def get_breakout_commands(self, if_speed: Dict[str, int], force=False) -> Tuple[List[str], List[str]]:
        return get_breakout_commands(self, if_speed, force)


def get_breakout_commands(ctl: SwitchCtl, if_speed: Dict[str, int], force=False) -> Tuple[List[str], List[str]]:
    """
    Configures port speeds for 400G interfaces to 4x100G or 1x100G using heuristics and the given if_speed map.

    Transceiver heuristics: transceiver types support particular speed configurations. Only use transceiver in
     breakout decision if there is an ambiguous speed config (e.g. 1 100G interface on a 400G port:
     decide between 1x100G or 4x100G)

    Limitations:
    - does not revert breakouts if going from a lower speed mode to higher speed mode
    - only handles 400G, 800G switches with limited breakout modes

    Args:
        ctl: SwitchCtl object
        if_speed: dict of interface name -> speed (100, 400, 800)
        force: force running breakout commands - ignore current breakout settings and re-apply

    Returns:
        List of breakout commands for ports which could be determined to require a breakout
        List of warnings - errors or assumptions made during breakout command search
    """

    ifs = ctl.list_interfaces()
    try:
        default_port_speed = ctl.get_max_port_speed_gbps()
    except NotImplementedError:
        default_port_speed = max([i.speed for i in ifs]) // GBPS
    port_accum = _PortSpeedAccum(default_port_speed)

    vendor_impl = ctl.vendor_cmds()
    warnings = []

    port_inf_speed_target = {}
    for i, speed in if_speed.items():
        port_inf = vendor_impl.parse_interface(i)
        if not port_inf:
            warnings.append(f"unable to parse input interface name {i}")
            continue
        port_inf_speed_target[port_inf] = speed

    # accumulate current/target states and determine breakouts
    warnings.extend(port_accum.set_current_speeds(ifs))
    warnings.extend(port_accum.set_target_speeds(port_inf_speed_target))
    breakouts, additional_warnings = port_accum.determine_breakouts()
    warnings.extend(additional_warnings)

    default_port_mode = BreakoutMode.Mode800X1 if default_port_speed == 800 else BreakoutMode.Mode400X1
    command_groups = []
    effective_breakout_count = 0
    for port in sorted(list(breakouts.keys())):
        new_mode = breakouts[port]
        try:
            mode = default_port_mode if force else port_accum.estimate_current_mode(port)
            commands_group = vendor_impl.get_port_breakout_cmds(port, mode, new_mode)
            if command_groups:
                effective_breakout_count += 1
            for i, commands in enumerate(commands_group):
                if len(command_groups) < i + 1:
                    command_groups.append(commands)
                else:
                    command_groups[i].extend(commands)
        except ValueError as e:
            warnings.append(str(e))

    # assume 8 seconds per initial breakout as switch takes time to initially reconfigure ports but less time on
    # subsequent breakouts
    initial_wait_time, progressive_wait_time = max(effective_breakout_count * 8, 60), 10
    if command_groups:
        final_commands = []
        for i, command_group in enumerate(command_groups):
            final_commands.extend(vendor_impl.get_enter_config_mode_cmds())
            final_commands.extend(command_group)
            final_commands.extend(vendor_impl.get_exit_config_mode_cmds())
            if i != len(command_groups) - 1:
                wait_time = initial_wait_time if i == 0 else progressive_wait_time
                final_commands.extend(vendor_impl.get_sleep_commands(wait_time))
        return final_commands, warnings
    return [], warnings