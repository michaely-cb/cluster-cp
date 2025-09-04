import abc
import dataclasses
import ipaddress
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

from django.db.models import Count, Q

from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles
from deployment_manager.db.models import Device, HealthState, HealthStatus, QuerySet
from deployment_manager.tools.ssh import SSHConn
from deployment_manager.tools.system import SystemCtl
from deployment_manager.tools.utils import ping_check, tcp_connection_check
from .ipmi import create_ipmi_client
from ..config import ConfGen
from ..dhcp import interface
from ..middleware import lookup_switch_info
from ..switch.switchctl import create_client


@dataclasses.dataclass
class HealthCheckResult:
    name: str
    status: HealthState
    message: str
    detail: str = ""

    def is_ok(self):
        return self.status == HealthState.OK


class DeviceHealthCheck(abc.ABC):
    """
    Device health
    """

    def __init__(self, profile: str, parallelism: int = 10):
        self._parallelism = parallelism
        self._profile = profile

    @classmethod
    def command_name(cls):
        # Camel to snake
        return "".join(
            f"_{c.lower()}" if c.isupper() else c for c in cls.__name__
        ).lstrip("_")

    @classmethod
    def check_items(cls) -> List[str]:
        return [cls.command_name()]

    def get_device(self, device):
        return Device.get_device(device, self._profile)

    def run_check(
        self, targets: List[str]
    ) -> Dict[str, List[HealthCheckResult]]:
        """
        This is the default implementation. Derived classes are required
        to implement _run_check_single_thread.

        For some of classes that do not need multithreading, they need
        to implement their own run_check function

        Args:
            targets (List[str]): target devices that will perform health check

        Returns:
            Dict[str, Tuple]: health check result
        """
        result = {}
        with ThreadPoolExecutor(max_workers=self._parallelism) as executor:
            future_to_device = {
                executor.submit(self._run_check_single_thread, tgt): tgt
                for tgt in targets
            }
            for f, device in future_to_device.items():
                try:
                    result[device] = f.result()
                except Exception as e:
                    result[device] = [
                        HealthCheckResult(
                            self.check_items()[0],
                            HealthState.CRITICAL,
                            f"Err: {e}",
                        )
                    ]
        return result

    def run_check_update(self, targets: List[str]):
        results = self.run_check(targets)
        for tgt, health_statuses in results.items():
            for health_status in health_statuses:
                update_device_health(
                    self._profile,
                    tgt,
                    health_status.name,
                    health_status.status,
                    health_status.message,
                    health_status.detail,
                )

    def _run_check_single_thread(self, target: str) -> List[HealthCheckResult]:
        raise NotImplementedError("_run_check_single_thread not implemented")


class IpmiCheck(DeviceHealthCheck):
    """
    Perform health checks through ipmi client
    """

    CHECK_ITEMS = ["ipmi_access", "server_health", "mgmt_link"]

    def __init__(self, profile: str):
        super().__init__(profile)
        self._mac_to_lease = LeaseCheck.get_mac_to_lease(profile)

    @classmethod
    def check_items(cls) -> List[str]:
        return cls.CHECK_ITEMS

    def _mac_off_by_one(self, ipmi_mac) -> Tuple[bool, List]:
        # check DHCP leases to see if lease has been provided to MAC
        # addresses off by one from the expected ipmi MAC
        possible_pairs = list()
        off_by_one = False
        if ipmi_mac:
            ipmi_mac_int = int(ipmi_mac.replace(':', ''), 16)
            for m in [ipmi_mac_int + 1, ipmi_mac_int - 1]:
                m_str = ':'.join(f"{m:012x}"[i:i+2] for i in range(0, 12, 2))
                lease = self._mac_to_lease.get(m_str)
                if lease is not None:
                    off_by_one = True
                    possible_pairs.append(f"{m_str}:{lease.ip}")
        return off_by_one, possible_pairs

    def _stale_lease(self, ipmi_ip, ipmi_mac) -> Tuple[bool, str]:
        # check DHCP leases to see if expected IP is provided to a
        # different MAC
        stale_mac = None
        stale_lease = False
        for lmac, lease in self._mac_to_lease.items():
            if lease.ip == ipmi_ip:
                if lmac != ipmi_mac:
                    stale_lease = True
                    stale_mac = ipmi_mac
                else:
                    break
        return stale_lease, stale_mac

    def _run_check_single_thread(self, target: str) -> List[HealthCheckResult]:
        device = self.get_device(target)
        ipmi_ip, ipmi_user, ipmi_password, vendor = device.get_ipmi_client_args()
        reachable = ping_check(ipmi_ip)
        if not reachable:
            solution = "check cable or reset IPMI"
            ipmi_mac = device.get_prop(props.prop_ipmi_info_mac)
            stale_lease, stale_mac = self._stale_lease(ipmi_ip, ipmi_mac)
            if stale_lease:
                solution = (
                    f"Expected IP {ipmi_ip} has been provided to {stale_mac}. This can "
                    f"happen if {stale_mac} belonged to a device has since been removed "
                    "and that lease hasn't expired."
                )
            else:
                mac_off_by_one, possible_pairs = self._mac_off_by_one(ipmi_mac)
                if mac_off_by_one:
                    solution = (
                        "IPMI MAC address may be off by one. "
                        "These are the possible allocations - "
                        f"{', '.join(possible_pairs)}"
                    )
            return [
                HealthCheckResult(
                    self.CHECK_ITEMS[0],
                    HealthState.CRITICAL,
                    f"cannot ping IPMI, {solution}",
                )
            ]

        health_statuses = []

        # ipmi access check
        try:
            ipmi = create_ipmi_client(ipmi_ip, ipmi_user, ipmi_password, vendor, timeout=10)
            health_statuses.append(
                HealthCheckResult(self.CHECK_ITEMS[0], HealthState.OK, "")
            )
        except Exception as e:
            return [
                HealthCheckResult(
                    self.CHECK_ITEMS[0],
                    HealthState.CRITICAL,
                    f"cannot login to IPMI: {e.__class__.__name__}",
                    f"log in exception: {e}",
                )
            ]
        # server health check
        is_ok, details = ipmi.get_server_aggregate_health()
        one_psu_ok = ipmi.one_psu_ok()
        if is_ok:
            health_statuses.append(
                HealthCheckResult(
                    self.CHECK_ITEMS[1],
                    HealthState.OK,
                    "",
                    json.dumps(details),
                )
            )
        else:
            # Ignore power critical state as this is due to missing redundancy
            if len(details) == 1 and "Power" in details.keys() and one_psu_ok:
                health_statuses.append(
                    HealthCheckResult(
                        self.CHECK_ITEMS[1],
                        HealthState.WARNING,
                        "check power supply redundancy, login to IPMI for more info",
                        json.dumps(details),
                    )
                )
            else:
                health_statuses.append(
                    HealthCheckResult(
                        self.CHECK_ITEMS[1],
                        HealthState.CRITICAL,
                        "ipmi reports health critical, check details",
                        json.dumps(details),
                    )
                )

        try:
            ipmi.find_mgmt_mac_address()
            health_statuses.append(
                HealthCheckResult(self.CHECK_ITEMS[2], HealthState.OK, "")
            )
        except Exception as e:
            message = "missing mgmt link or server is powered off"
            cause = "check cable"
            power_state = ipmi.get_server_power_state()
            nic_shared = ipmi.check_ipmi_nic_shared()
            if power_state.lower() == "off":
                message = "server is powered off"
                cause = (
                    "server is powered off. Turn on using "
                    f"'cscfg server ipmi power --action on -f name={target}'"
                )
            elif not nic_shared:
                message = "missing mgmt link"
                cause = (
                    "server's mgmt NIC is in DEDICATED mode. Move the IPMI cable to LOM1 port and "
                    "set to SHARED using - "
                    f"'cscfg server ipmi set_nic_mode --mode SHARED -f name={target}'"
                )
            health_statuses.append(
                HealthCheckResult(
                    self.CHECK_ITEMS[2],
                    HealthState.CRITICAL,
                    message,
                    f"{e}, {cause}",
                )
            )

        ipmi.close()
        return health_statuses


class LeaseCheck(DeviceHealthCheck):
    """
    Scrape the lease file to check if DHCP server issue the correct ip
    to the leasee
    """

    def __init__(self, profile: str):
        super().__init__(profile)

    @abc.abstractmethod
    def ip(self, target: Device) -> ipaddress.IPv4Address:
        pass

    @abc.abstractmethod
    def mac(self, target: Device) -> str:
        pass

    @abc.abstractmethod
    def hostnames(self, target: Device) -> List[str]:
        pass

    @staticmethod
    def get_mac_to_lease(profile):
        cg = ConfGen(profile)
        dhcp_provider = interface.get_provider(profile, cg.parse_profile())
        leases = dhcp_provider.get_leases()
        mac_to_info = {}
        for lease_entry in leases:
            mac_to_info[lease_entry.mac] = lease_entry
        return mac_to_info

    def run_check(
        self, targets: List[str]
    ) -> Dict[str, List[HealthCheckResult]]:
        result = {}

        mac_to_info = self.get_mac_to_lease(self._profile)

        if not mac_to_info:
            result = {
                target: [
                    HealthCheckResult(
                        self.command_name(),
                        HealthState.WARNING,
                        "DHCP lease file was empty. Check DHCP status",
                    )
                ]
                for target in targets
            }
            return result

        for target in targets:
            d = self.get_device(target)
            mac = self.mac(d)
            if mac not in mac_to_info:
                result[target] = [
                    HealthCheckResult(
                        self.command_name(),
                        HealthState.WARNING,
                        "dhcp lease not found",
                        "device may have a static IP or is not communicating with the dhcp server",
                    )
                ]
                continue

            ip = self.ip(d)
            entry = mac_to_info[mac]
            if entry.ip != ip:
                result[target] = [
                    HealthCheckResult(
                        self.command_name(),
                        HealthState.WARNING,
                        f"expected ip {ip} but got {entry.ip}",
                    )
                ]
                continue

            expect_hostnames = [name.lower() for name in self.hostnames(d)]
            actual_hostname = entry.host.lower().split(".")[0]
            if actual_hostname not in expect_hostnames:
                result[target] = [
                    HealthCheckResult(
                        self.command_name(),
                        HealthState.WARNING,
                        f"expected hostname {' or '.join(expect_hostnames)} "
                        f"but got {entry.host}",
                    )
                ]
                continue

            result[target] = [
                HealthCheckResult(self.command_name(), HealthState.OK, "")
            ]
        return result


class IpmiLease(LeaseCheck):

    def __init__(self, profile: str):
        super().__init__(profile)

    def ip(self, target: Device) -> ipaddress.IPv4Address:
        return target.get_prop(props.prop_ipmi_info_ip)

    def mac(self, target: Device) -> str:
        return target.get_prop(props.prop_ipmi_info_mac)

    def hostnames(self, target: Device) -> List[str]:
        return list({target.get_prop(props.prop_ipmi_info_name), str(target.name) + "-ipmi"})


class SwitchMgmtPortCheck(DeviceHealthCheck):
    """
    Perform health checks on switch mgmt ports
    """

    CHECK_ITEMS = ["mgmt_access", "hostname"]

    def __init__(self, profile: str):
        super().__init__(profile)

    @classmethod
    def check_items(cls) -> List[str]:
        return cls.CHECK_ITEMS

    def _run_check_single_thread(self, target: str) -> List[HealthCheckResult]:
        d = self.get_device(target)
        mgmt_ip = str(d.get_prop(props.prop_management_info_ip))
        reachable = ping_check(mgmt_ip)

        if not reachable:
            return [
                HealthCheckResult(
                    self.CHECK_ITEMS[0],
                    HealthState.CRITICAL,
                    f"{mgmt_ip} not pingable, check connectivity",
                )
            ]


        switch_info = lookup_switch_info(self._profile, target)
        switch_client = None if not switch_info else create_client(switch_info)
        health_statuses = []

        if switch_client is None:
            # fallback to SSH if the vendor's client is not supported
            ssh_con = SSHConn(mgmt_ip, d.get_prop(props.prop_management_credentials_user), d.get_prop(props.prop_management_credentials_password))
            try:
                with ssh_con as con:
                    health_statuses.append(
                        HealthCheckResult(self.CHECK_ITEMS[0], HealthState.OK, "")
                    )
            except Exception as e:
                health_statuses.append(HealthCheckResult(
                    self.CHECK_ITEMS[0],
                    HealthState.CRITICAL,
                    "can ping but not login to switch mgmt port, check username/password",
                    f"{e}",
                ))
            health_statuses.append(HealthCheckResult(
                self.CHECK_ITEMS[1],
                HealthState.WARNING,
                f"cannot determine hostname: unsupported switch vendor for {target}",
            ))
            return health_statuses
        else:
            try:
                with switch_client as client:
                    # if we opened the client, then switch is reachable -
                    health_statuses.append(
                        HealthCheckResult(self.CHECK_ITEMS[0], HealthState.OK, "")
                    )

                    try:
                        hostname = client.get_hostname()
                        if target != hostname and target != hostname.split(".")[0]:
                            health_statuses.append(
                                HealthCheckResult(
                                    self.CHECK_ITEMS[1],
                                    HealthState.CRITICAL,
                                    f"hostname mismatch (expected={target},actual={hostname}), check if ztp config applied",
                                )
                            )
                        else:
                            health_statuses.append(
                                HealthCheckResult(self.CHECK_ITEMS[1], HealthState.OK, "")
                            )
                    except Exception as e:
                        health_statuses.append(
                            HealthCheckResult(
                                self.CHECK_ITEMS[1],
                                HealthState.CRITICAL,
                                f"unable to determine switch hostname: {e}",
                            )
                        )
            except Exception as e:
                return [HealthCheckResult(
                    self.CHECK_ITEMS[0],
                    HealthState.CRITICAL,
                    f"can ping but not login to switch mgmt port, check username/password: {e}",
                )]

        return health_statuses


class GatewayPing(DeviceHealthCheck):
    """
    Perform ping checks on mgmt switch gateway
    """

    def __init__(self, profile: str):
        super().__init__(profile)

    def _run_check_single_thread(self, target: str) -> List[HealthCheckResult]:
        tgt_switch = self.get_device(target)
        if tgt_switch.device_role != DeploymentDeviceRoles.MANAGEMENT.value:
            return [
                HealthCheckResult(
                    self.command_name(), HealthState.OK, "skip non mgmt switch"
                )
            ]

        gateway_ip = tgt_switch.get_prop(props.prop_subnet_info_gateway)
        if gateway_ip is None:
            return [
                HealthCheckResult(
                    self.command_name(),
                    HealthState.CRITICAL,
                    "gateway ip for mgmt switch is not assigned, please edit device properties",
                )
            ]

        reachable = ping_check(gateway_ip)
        if not reachable:
            return [
                HealthCheckResult(
                    self.command_name(),
                    HealthState.CRITICAL,
                    "gateway ping failed, check uplink config, more info in details",
                    "Possible issues are: BGP peering not setup, switch not configured, "
                    "uplink cable down",
                )
            ]
        else:
            return [HealthCheckResult(self.command_name(), HealthState.OK, "")]


class SwitchLease(LeaseCheck):
    """
    Perform switch lease check

    Scrape the lease file to check if DHCP server issue the correct ip
    to the leasee
    """

    def __init__(self, profile: str):
        super().__init__(profile)

    def ip(self, target: Device) -> ipaddress.IPv4Address:
        return target.get_prop(props.prop_management_info_ip)

    def mac(self, target: Device) -> str:
        return target.get_prop(props.prop_management_info_mac)

    def hostnames(self, target: Device) -> List[str]:
        return [target.name]


class SystemCheck(DeviceHealthCheck):
    """
    Perform health checks on systems
    """

    CHECK_ITEMS = ["system_health", "system_link"]

    def __init__(self, profile: str):
        super().__init__(profile)

    @classmethod
    def check_items(cls) -> List[str]:
        return cls.CHECK_ITEMS

    def _run_check_single_thread(self, target: str) -> List[HealthCheckResult]:
        health_status = []
        d = self.get_device(target)
        mgmt_user = d.get_prop(props.prop_management_credentials_user)
        mgmt_password = d.get_prop(props.prop_management_credentials_password)

        sys_ctl = SystemCtl(d.name, mgmt_user, mgmt_password)

        try:
            status = sys_ctl.get_system_health_status()
            if status in ("DEGRADED", "OK", "STANDBY"):
                health_status.append(
                    HealthCheckResult(self.CHECK_ITEMS[0], HealthState.OK, "")
                )
            else:
                health_status.append(
                    HealthCheckResult(self.CHECK_ITEMS[0], HealthState.CRITICAL,
                                      f"system health is {status}")
                )

            down_links = sys_ctl.get_down_links()
            if down_links:
                if status == "STANDBY":
                    health_status.append(
                        HealthCheckResult(self.CHECK_ITEMS[1], HealthState.WARNING,
                                          f"system in standby, cannot determine link state")
                    )
                else:
                    health_status.append(
                        HealthCheckResult(self.CHECK_ITEMS[1], HealthState.CRITICAL,
                                          f"system links are down: {', '.join(down_links)}")
                    )
            else:
                health_status.append(
                    HealthCheckResult(self.CHECK_ITEMS[1], HealthState.OK, "")
                )

        except Exception as e:
            return [
                HealthCheckResult(
                    self.CHECK_ITEMS[0],
                    HealthState.CRITICAL,
                    f"cannot reach system management service: {e}",
                )
            ]

        return health_status

class PeripheralConnCheck(DeviceHealthCheck):
    """
    Perform ping checks on mgmt switch gateway
    """
    CHECK_ITEMS = ["ping_check", "tcp_check", "ssh_check"]
    
    def __init__(self, profile: str):
        super().__init__(profile)
    
    @classmethod
    def check_items(cls) -> List[str]:
        return cls.CHECK_ITEMS

    def check_items_from_role(self, role: str) -> List[str]:
        """
        Return the check items based on the device role.
        """
        if role == DeploymentDeviceRoles.VALVECTRL.value:
            return [self.CHECK_ITEMS[0], self.CHECK_ITEMS[1]]
        elif role == DeploymentDeviceRoles.MEDIACONV.value:
            return [self.CHECK_ITEMS[0], self.CHECK_ITEMS[1], self.CHECK_ITEMS[2]]
        elif role == DeploymentDeviceRoles.PDU.value:
            return [self.CHECK_ITEMS[0], self.CHECK_ITEMS[2]]
        else:
            raise ValueError(f"Unsupported device role: {role}")

    def _run_check_single_thread(self, target: str) -> List[HealthCheckResult]:
        health_status = []
        tgt_device = self.get_device(target)
        mgmt_ip = tgt_device.get_prop(props.prop_management_info_ip)

        check_items = self.check_items_from_role(tgt_device.device_role)
        # Conn check
        if mgmt_ip is None:
            return [
                HealthCheckResult(
                    self.CHECK_ITEMS[0],
                    HealthState.CRITICAL,
                    "peripheral device does not have an ip address assigned, please edit device properties",
                )
            ]

        if self.CHECK_ITEMS[0] in check_items:
            ping_ok = ping_check(mgmt_ip)
            if not ping_ok:
                health_status.append(
                    HealthCheckResult(
                        self.CHECK_ITEMS[0],
                        HealthState.CRITICAL,
                        "ping failed",
                    )
                )
            else:
                health_status.append(HealthCheckResult(self.CHECK_ITEMS[0], HealthState.OK, ""))
        else:
            health_status.append(HealthCheckResult(self.CHECK_ITEMS[0], HealthState.SKIPPED, ""))

       
        # TCP conn check
        if self.CHECK_ITEMS[1] in check_items:
            tcp_ok = tcp_connection_check(mgmt_ip)
            if not tcp_ok:
                health_status.append(
                    HealthCheckResult(
                        self.CHECK_ITEMS[1],
                        HealthState.CRITICAL,
                        "tcp connection failed",
                    )
                )
            else:
                health_status.append(HealthCheckResult(self.CHECK_ITEMS[1], HealthState.OK, ""))
        else:
            health_status.append(HealthCheckResult(self.CHECK_ITEMS[1], HealthState.SKIPPED, ""))
        
        # SSH conn check
        if self.CHECK_ITEMS[2] in check_items:
            mgmt_user = tgt_device.get_prop(props.prop_management_credentials_user)
            mgmt_password = tgt_device.get_prop(props.prop_management_credentials_password)
            
            if not mgmt_user or not mgmt_password:
                health_status.append(
                    HealthCheckResult(
                        self.CHECK_ITEMS[2],
                        HealthState.CRITICAL,
                        "SSH credentials not configured for peripheral device",
                    )
                )
            else:
                # Use the existing SSHConn class to test connection
                ssh_conn = SSHConn(str(mgmt_ip), mgmt_user, mgmt_password)
                try:
                    with ssh_conn as conn:
                        health_status.append(HealthCheckResult(self.CHECK_ITEMS[2], HealthState.OK, ""))
                        
                except Exception as e:
                    health_status.append(
                        HealthCheckResult(
                            self.CHECK_ITEMS[2],
                            HealthState.CRITICAL,
                            "SSH connection failed",
                            f"Exception: {e}",
                        )
                    )
        else:
            health_status.append(HealthCheckResult(self.CHECK_ITEMS[2], HealthState.SKIPPED, ""))

        return health_status

SERVER_CHECK_CLASSES = [IpmiCheck, IpmiLease]
SERVER_HEALTH_CHECKS = []
for check in SERVER_CHECK_CLASSES:
    SERVER_HEALTH_CHECKS.extend(check.check_items())

SWITCH_CHECK_CLASSES = [SwitchMgmtPortCheck, GatewayPing, SwitchLease]
SWITCH_HEALTH_CHECKS = []
for check in SWITCH_CHECK_CLASSES:
    SWITCH_HEALTH_CHECKS.extend(check.check_items())

SYSTEM_CHECK_CLASSES = [SystemCheck]
SYSTEM_HEALTH_CHECKS = []
for check in SYSTEM_CHECK_CLASSES:
    SYSTEM_HEALTH_CHECKS.extend(check.check_items())

PERIPHERAL_CHECK_CLASSES = [PeripheralConnCheck]
PERIPHERAL_HEALTH_CHECKS = []
for check in PERIPHERAL_CHECK_CLASSES:
    PERIPHERAL_HEALTH_CHECKS.extend(check.check_items())


def get_health_attrs_for_device_type(device_type: str):

    if device_type == "SR":
        return SERVER_HEALTH_CHECKS
    elif device_type == "SW":
        return SWITCH_HEALTH_CHECKS
    elif device_type == "SY":
        return SYSTEM_HEALTH_CHECKS
    elif device_type == "PR":
        return PERIPHERAL_HEALTH_CHECKS
    else:
        assert False, f"Unsupported type {device_type}"


def get_device_health_all(device: Device) -> Dict:

    result = {}
    attrs = get_health_attrs_for_device_type(device.device_type)
    for attr in attrs:
        try:
            health_record = device.health_records.all().get(attr=attr)
            created = health_record.created_at.strftime("%Y-%m-%d%H:%M:%S")
            updated = health_record.updated_at.strftime("%Y-%m-%d%H:%M:%S")
            result[attr] = dict(
                status=health_record.status,
                message=health_record.message,
                detail=health_record.detail,
                created=created,
                updated=updated,
            )
        except:
            result[attr] = dict(status=HealthState.UNKNOWN.value)

    return result


def filter_degraded_devices(query_set: QuerySet, device_type: str):
    """ filter for devices with some non-OK condition """
    attrs_size = len(get_health_attrs_for_device_type(device_type))

    query_1 = (
        query_set
        .annotate(health_count=Count("health_records"))
        .filter(health_count__lt=attrs_size)
    )

    query_2 = (
        query_set
        .annotate(
            not_ok_count=Count(
                "health_records",
                filter=~Q(health_records__status__in=[HealthState.OK.value, HealthState.SKIPPED.value])
            )
        )
        .filter(not_ok_count__gt=0)
    ) 

    return (query_1 | query_2).distinct()


def filter_critical_devices(query_set: QuerySet, device_type: str):
    """ filter for devices with some CRITICAL or UNKNOWN condition """
    attrs_size = len(get_health_attrs_for_device_type(device_type))

    query_1 = (
        query_set
        .annotate(health_count=Count("health_records"))
        .filter(health_count__lt=attrs_size)
    )

    query_2 = (
        query_set
        .annotate(
            critical_count=Count(
                "health_records",
                filter=Q(
                    health_records__status__in=[
                        HealthState.CRITICAL.value,
                        HealthState.UNKNOWN.value,
                    ]
                ),
            )
        )
        .filter(critical_count__gt=0)
    )
    return (query_1 | query_2).distinct()


def filter_devices_with_health_state(
    profile: str, device_type: str, health_attr: str, state: str
):
    hstate = HealthState(state)
    return (
        Device.get_all(profile, device_type=device_type)
        .annotate(
            query_count=Count(
                "health_records",
                filter=(
                    Q(health_records__status=hstate.value)
                    & Q(health_records__attr=health_attr)
                ),
            )
        )
        .filter(query_count=1)
    )


def update_device_health(
    profile: str,
    device: str,
    attr: str,
    status: Optional[HealthState],
    msg: str = "",
    detail: str = "",
):

    d = Device.get_device(device, profile)

    attrs = get_health_attrs_for_device_type(d.device_type)

    assert (
        attr in attrs
    ), f"Device {d.device_type} does not have {attr} check status"

    query_set = HealthStatus.objects.filter(
        device__profile__name=profile
    ).filter(device__name=device)
    try:
        health_status = query_set.get(attr=attr)
    except HealthStatus.DoesNotExist:
        health_status = HealthStatus(
            device=d, attr=attr, status=status.value, message=msg, detail=detail
        )
        health_status.save()
    else:
        if status is None:
            health_status.delete()
        else:
            health_status.status = status.value
            health_status.message = msg
            health_status.detail = detail
            health_status.save()
