from collections import defaultdict
import dataclasses
import ipaddress
import concurrent.futures
import csv
import shutil
import subprocess
import django.db.transaction
import json
import logging
import pathlib
import re
from typing import Dict, Optional, Tuple, List
from django.db.models import Q
from django.db import transaction

from deployment_manager.common.constants import MAX_WORKERS
from deployment_manager.cli.switch.utils import device_to_switchinfo
from deployment_manager.cli.subcommand import SubCommandABC, string_pattern
from deployment_manager.cli.switch.link.filter import add_filter_arg, apply_filters, apply_src_dst_filters
from deployment_manager.cli.switch.link.reprs import LINK_STATE_MATCH, LINK_STATE_MISSING, LINK_STATE_UNRECOGNIZED, LinkRepr, LinkNeighStatusRepr
from deployment_manager.cli.switch.link.reprs import LinkDetectMacsRepr
from deployment_manager.db.models import Device, DeviceProperty, Link
from deployment_manager.db import device_props as props
from deployment_manager.tools.utils import prompt_confirm
from deployment_manager.tools.switch.interface import SwitchCommands
from deployment_manager.tools.middleware import lookup_switch_info
from deployment_manager.tools.switch.switchctl import get_vendor_impl
from deployment_manager.cli.switch.link.lldp import perform_matching
from deployment_manager.tools.switch.switchctl import perform_group_action
from deployment_manager.tools.dhcp import interface as dhcp
from deployment_manager.tools.config import ConfGen
from deployment_manager.cli.switch.interface import _gather_switches
from deployment_manager.db.const import DeploymentDeviceRoles

logger = logging.getLogger(__name__)

ALLOWED_LINK_SPEED = (1600, 800, 400, 100, 25, 10, 1)
switch_cmds_cache = {}

def get_switch_cmds(profile: str, switch_name: str) -> Tuple[Optional[SwitchCommands], Optional[str]]:
    if switch_name in switch_cmds_cache:
        return switch_cmds_cache[switch_name], None
    
    switch_info = lookup_switch_info(profile, switch_name)
    if not switch_info:
        return None, f"failed to lookup switch {switch_name} in profile {profile}"
    
    try:
        impl = get_vendor_impl(switch_info.vendor, switch_info.os_family)
        if not impl:
            return None, f"unsupported vendor {switch_info.vendor} for switch {switch_name}"
        switch_cmds = impl[1]()
        switch_cmds_cache[switch_name] = switch_cmds
        return switch_cmds, None
    except Exception as e:
        return None, f"failed to create switch commands for {switch_name}: {e}"

def set_interface_role(interface, swcmds) -> Optional[str]:
    interface = str(interface).strip()
    management_regex = re.compile(r"^(?P<mgmt>@mgmt(\d)?)$|^(?P<ipmi>@ipmi(\d)?)$|^(?P<ipmi_mgmt>@ipmi_mgmt(\d)?)$")
    match = management_regex.match(interface)
    role = ""
    if match:
        if match.group('mgmt'):
            role = "mgmt"
        elif match.group('ipmi'):
            role = "ipmi"
        elif match.group('ipmi_mgmt'):
            role = "ipmi_mgmt"
        return role
    else:
        if swcmds is not None: # it's a switch
            portint = swcmds.parse_interface(interface)
            if portint is not None:
                if portint.role == "mgmt":
                    role = "mgmt"
                else:
                    role = "data" # data is the default role
                return role
            else:
                return None
        else: # not a switch (might be a server)
            # TODO: we need to define naming conventions for non-switch devices
            return "data" # data is the default role
                    
def create_or_update_link(profile: str, repr: LinkRepr) -> Tuple[Optional[Link], str]:
    """ Updates link associated with the repr, returning updated link or None and an error message if validation failed
    """
    src_dev = Device.get_device(repr.src_name, profile)
    dst_dev = Device.get_device(repr.dst_name, profile)
    src = (src_dev, repr.src_name, repr.src_if)
    dst = (dst_dev, repr.dst_name, repr.dst_if)

    # ensure that src device is a SW and a SP/SX switch if both are switches
    if src[0] is None and dst[0] is None:
        return None, f"src {repr.src_name} and dst {repr.dst_name} were not found in profile {profile}"
    elif src[0] is None:
        src, dst = dst, src
    elif dst[0] and src[0]:
        # swap spine/BR and MA switch to be the SRC in SW/SW links
        if dst[0].device_role in ("SP", "SX", "MA") and dst[0].device_type == "SW" and src[0].device_type == "SW":
            src, dst = dst, src
        # swap src and dst if src is not a switch
        elif dst[0].device_type == "SW" and src[0].device_type != "SW":
            src, dst = dst, src

        # reject LF/LF and MX/MX switch connections
        if src[0].device_role in ("LF", "MX") and dst[0].device_role in ("LF", "MX") and dst[0].device_type == "SW":
            return None, f"src {src[1]} -> dst {dst[1]} creating LF/LF MX/MX switch connections is not allowed"

    if src[0].device_type != "SW":
        if not dst[0]:
            return None, f"src device {src[1]} was not a switch and dst device {dst[1]} was not found"
        else:
            return None, f"neither src device {src[1]} or {dst[1]} is a switch"

    # ensure that the link speed is valid
    try:
        speed = int(repr.speed)
        if speed not in ALLOWED_LINK_SPEED:
            return None, f"invalid link speed: {repr.speed} should be in {','.join([str(l) for l in ALLOWED_LINK_SPEED])}"
    except ValueError:
        return None, f"invalid link speed: '{repr.speed}'"
    
    src_dev, src_name, src_if = src
    dst_dev, dst_name, dst_if = dst
    
    src_cmds, err = get_switch_cmds(profile, src_name)
    if err:
        return None, err 
    
    # define interface roles based on link speed or interface names
    src_if_role = ""
    dst_if_role = ""

    if speed > 1:
        ret = set_interface_role(src_if, src_cmds)
        if ret is not None:
            src_if_role = ret
        else:
            return None, f"'{src_if}' is not in a format that {src_name} recognizes as a valid interface"      

        if dst_dev and dst_dev.is_switch:
            dst_cmds, err = get_switch_cmds(profile, dst_name)
            if err:
                return None, err       
            ret = set_interface_role(dst_if, dst_cmds)
            if ret is not None:
                dst_if_role = ret
            else:
                return None, f"'{dst_if}' is not in a format that {dst_name} recognizes as a valid interface"
        else:
            dst_if_role = set_interface_role(dst_if, None)
    else:
        src_if_role = "mgmt"
        if dst_if.find("@ipmi_mgmt") != -1:
            dst_if_role = "ipmi_mgmt"
        elif dst_if.find("@ipmi") != -1:
            dst_if_role = "ipmi"
        else:
            dst_if_role = "mgmt"
    
    # Lookup an existing link with src_device and src_if as a natural key
    try:
        link = Link.objects.get(src_device=src_dev, src_if=src_if)
        link.src_if_role = src_if_role
        link.dst_device = dst_dev
        link.dst_name = dst_name
        link.dst_if = dst_if
        link.dst_if_role = dst_if_role
        link.speed = repr.speed
        link.origin = repr.origin or link.origin
    except Link.DoesNotExist:
        link = Link(
            src_device=src_dev,
            src_if=src_if,
            src_if_role=src_if_role,
            dst_name=dst_name,
            dst_device=dst_dev,
            dst_if=dst_if,
            dst_if_role=dst_if_role,
            speed=repr.speed,
            origin=repr.origin,
        )

    link.save()
    return link, ""

@django.db.transaction.atomic
def create_or_update_links(profile: str, reprs: List[Optional[LinkRepr]]) -> Tuple[List[LinkRepr], List[str]]:
    """ Returns list of updated links and error messages for any failed link updates """
    oks, errs = [], []
    for i, link in enumerate(reprs):
        if not link:
            continue

        ok, err = create_or_update_link(profile, link)
        if err:
            errs.append(f"link at index {i}: {err}")
        else:
            oks.append(LinkRepr.from_link(ok))
    return oks, errs


def parse_apstra_link_file(doc: dict) -> Tuple[List[Optional[LinkRepr]], List[str]]:
    """
    Parse output of Apstra's link document (Blueprints > BLUEPRINT > Staged > Links > Export Cabling Map)
    Returns List of links and list of error messages from link parsing, if any
    """
    reprs, errs = [], []
    speed_re = re.compile(r"(1|10|25|100|400|800|1600)G")
    for i, link in enumerate(doc["links"]):
        try:
            speed_m = speed_re.match(link.get("speed", "not_set"))
            if not speed_m:
                errs.append(f"link {i} has an unrecognized speed value: {link.get('speed', 'not_set')}")
                continue
            src_name = link["endpoints"][0]["system"]["label"].lower()
            src_if = link["endpoints"][0]["interface"]["if_name"] or ""
            if src_if:
                src_if = src_if.replace(" ", "_")
            dst_name = link["endpoints"][1]["system"]["label"].lower()
            dst_if = link["endpoints"][1]["interface"]["if_name"] or ""
            if dst_if:
                dst_if = dst_if.replace(" ", "_")
            reprs.append(LinkRepr(src_name, src_if, dst_name, dst_if, int(speed_m.group(1))))
        except Exception as e:
            errs.append(f"link {i} failed to parse unexpectedly: {e}")
            reprs.append(None)
    return reprs, errs

def parse_link_file(doc: list) -> Tuple[List[LinkRepr], str]:
    """ Parse link reprs list """
    if not isinstance(doc, list):
        return [], "invalid json doc: must be a list of Links"

    # require json import be formatted 100% correctly
    reprs = []
    for i, item in enumerate(doc):
        try:
            reprs.append(LinkRepr(**item))
        except Exception as e:
            return [], f"failed to parse Link object at index {i}: {e}"
    return reprs, ""

class LinkAdd(SubCommandABC):
    """ Add/update network links between switch and device. Outputs updated link """

    name = "add"

    def construct(self):
        self.parser.add_argument('--src', '-s', help="Source SWITCH:PORT", type=string_pattern(r"^\S+:\S+$"))
        self.parser.add_argument('--dst', '-d', help="Destination SWITCH:PORT?", type=string_pattern(r"^\S+(:\S+)?$"))
        self.parser.add_argument('--speed', help="Speed GBPS", required=True, type=int, choices=[1, 10, 25, 100, 400, 800, 1600])
        self.add_arg_output_format()
        
    def run(self, args):
        src_name, src_port = args.src.split(":", 1)
        dst_name, dst_port = args.dst, ""
        if ":" in args.dst:
            dst_name, dst_port = args.dst.split(":", 1)
        repr = LinkRepr(src_name, src_port, dst_name, dst_port, args.speed, origin="import")
        # store the link to the database
        link, err = create_or_update_link(self.profile, repr)
        if err:
            logger.error(err)
            return 1
        print(LinkRepr.format_reprs([LinkRepr.from_link(link)], args.output))
        return 0


class LinkImport(SubCommandABC):
    """
    Add/update network links between switch and device by importing a file. Outputs updated links and errors.

    FILE argument should be formatted as a list similar to the format of `link show -ojson` or as a json link
    export from apstra. The CSV format should have columns: src_name, src_if, dst_name, dst_if, speed
    """
    name = "import"

    def construct(self):
        self.parser.add_argument("--format",
                                 choices=["json", "csv", "apstra"],
                                 help="Input document format", default="")
        self.parser.add_argument('FILE', help="JSON file of links")
        self.add_arg_output_format()

    def run(self, args):

        try:
            doc_text = pathlib.Path(args.FILE).read_text()
        except Exception as e:
            logger.error(f"failed to read json file {args.FILE}: {e}")
            return 1

        fmt = args.format
        if not fmt:
            if args.FILE.endswith(".csv"):
                fmt = "csv"
            elif args.FILE.endswith(".json"):
                if doc_text.strip().startswith("{"):
                    fmt = "apstra"
                else:
                    fmt = "json"
            else:
                logger.error("unable to determine format, must specify --format")
                return 1
            logger.warning(f"--format not specified, assuming {fmt}")

        if fmt == "json":
            reprs, err = parse_link_file(json.loads(doc_text))
            if err:
                logger.error(f"failed to parse link entries: {err}")
                return 1

        elif fmt == "apstra":
            reprs, errs = parse_apstra_link_file(json.loads(doc_text))
            if errs:
                logger.warning(f"failed to parse {len(errs)} of {len(errs) + len(reprs)} link entries")
                logger.warning('\n - '.join(errs))
                # don't require a perfect file for apstra import
        elif fmt == "csv":
            reprs, errs = [], []
            reader = csv.DictReader(doc_text.splitlines())
            required_fields = {"src_name", "src_if", "dst_name", "dst_if", "speed"}
            fields = reader.fieldnames
            missing_fields = required_fields.difference(set(fields))
            if missing_fields:
                logger.error(f"document missing required fields: {', '.join(missing_fields)}")
                return 1
            extra_fields = set(fields).difference(required_fields)
            if extra_fields:
                logger.warning(f"extra fields will be ignored: {', '.join(extra_fields)}")
            for obj in reader:
                try:
                    d = {k: obj[k] for k in required_fields}
                    reprs.append(LinkRepr(**d))
                except Exception as e:
                    errs.append(f"failed to parse line {reader.line_num}: {e}")
            if errs:
                logger.warning(f"parsed {len(reprs)} of {len(errs) + len(reprs)} lines: \n{' '.join(errs)}")
            if not reprs:
                return 1
        else:
            assert False, "unhandled format"

        for r in reprs:
            r.origin = "import"

        rv = 0
        ok, errs = create_or_update_links(self.profile, reprs)
        print(LinkRepr.format_reprs(ok, args.output))
        if errs:
            logger.error(f"failed to import {len(errs)} of {len(reprs)} link records")
            for err in errs:
                logger.error(err)
            rv = 1

        return rv


class LinkRemove(SubCommandABC):
    """ Remove network links between switches. """
    name = "remove"

    def construct(self):
        add_filter_arg(self.parser)
        self.parser.add_argument('--src', '-s', help="src NAME or NAME:PORT", required=False)
        self.parser.add_argument('--dst', '-d', help="dst NAME or NAME:PORT", required=False)
        self.add_arg_noconfirm()
        self.add_arg_output_format()

    def run(self, args):
        q = Link.objects.filter(src_device__profile__name=self.profile)
        q = apply_src_dst_filters(q, args)
        q = apply_filters(q, args.filter)
        q = q.order_by('src_device__name', 'src_if')
        if len(q) == 0:
            logger.info("no links matched filters")
            return 0

        reprs = [LinkRepr.from_link(l) for l in q]
        print(LinkRepr.format_reprs(reprs, args.output))
        if not args.noconfirm:
            if not prompt_confirm(f"remove {len(reprs)} links? "):
                return 0

        q.delete()

        return 0

class LinkShow(SubCommandABC):
    """ Show network links """
    name = "show"

    def construct(self):
        add_filter_arg(self.parser)
        self.parser.add_argument('--src', '-s', help="src NAME or NAME:PORT", required=False)
        self.parser.add_argument('--dst', '-d', help="dst NAME or NAME:PORT", required=False)
        self.add_arg_output_format()

    def run(self, args):
        q = Link.objects.filter(src_device__profile__name=self.profile)
        q = apply_src_dst_filters(q, args)
        q = apply_filters(q, args.filter)
        q = q.order_by('src_device__name', 'src_if')

        print(LinkRepr.format_reprs([LinkRepr.from_link(link) for link in q], args.output))

def get_ips_from_dhcp_leases(profile) -> List[str]:
    """
    Retrieve IP addresses from DHCP leases.
    Returns:
        List[str]: A list of IP addresses as strings.
    """
    cg = ConfGen(profile)
    cfg = cg.parse_profile()
    if cfg is None:
        raise RuntimeError("unable to parse input.yaml file")
    dhcp_provider = dhcp.get_provider(profile, cfg)
    leases = dhcp_provider.get_leases()
    ips = []
    for lease in leases:
        if lease.ip:
            try:
                ips.append(str(lease.ip))
            except ipaddress.AddressValueError as e:
                logger.error(f"Invalid IP address in lease: {lease.ip} - {e}")
    return ips

def ping_host(host, ping_path):
    """
    Ping a single host.
    Args:
        host (str): Host IP or hostname to ping.
        ping_path (str): Path to the ping command.
    """
    # Construct the ping command.
    command = [ping_path, "-c", "1", "-W", "1", host]
    # Execute the ping command
    try:
        subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        logger.error(f"{command} failed: {e}")

def ping_hosts(hosts: List[str]):
    """
    Ping a list of hosts in parallel using threads.
    Args:
        hosts (List[str]): List of host IPs or hostnames to ping.
    """
    # find the ping command
    ping_path = shutil.which("ping")
    # run the ping command in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks and collect futures
        futures = [executor.submit(ping_host, host, ping_path) for host in hosts]
        # Wait for all the tasks to complete and get results
        concurrent.futures.wait(futures)

def refresh_mac_addr_tables(profile):
    """
    Refresh MAC address tables on all switches in the profile.
    """
    ping_hosts(get_ips_from_dhcp_leases(profile))

def generate_lldp_statuses(profile, args, all_db_links) -> List[LinkNeighStatusRepr]:
    """
    Generate LLDP link statuses.
    """
    # links that support LLDP
    lldp_links = all_db_links.filter(Q(src_if_role__exact='data') & Q(dst_if_role__exact='data'))
    if args:
        if args.filter:    
            lldp_links = apply_filters(lldp_links, args.filter)
    # all the switches that support LLDP
    lldp_switches = Device.get_all(profile).filter(
        device_id__in=
            lldp_links.filter(src_device__device_type="SW").values_list('src_device', flat=True).distinct()
            .union(
            lldp_links.filter(dst_device__device_type="SW").values_list('dst_device', flat=True).distinct()
            )
    ).distinct()
    # switch info of all the switches that support LLDP
    lldp_switches_infos = _gather_switches(profile, lldp_switches)
    # retrieve LLDP neighbors from switches that support LLDP
    lldp_neigh = perform_group_action(list(lldp_switches_infos), "list_lldp_neighbors")
    lldp_ok_switches = set()
    lldp_link_statuses: List[LinkNeighStatusRepr] = []
    for sw, result in lldp_neigh.items():
        if isinstance(result, str):
            logger.error(f"failed to gather lldp results from {sw}: {result}")
        else:
            lldp_ok_switches.add(sw)
    if not lldp_ok_switches:
        logger.warning("no switches matched filter or no LLDP links found")
    else:
        alias_names: Dict[str, str] = dict()
        prop = props.prop_management_info_name
        for p in DeviceProperty.get_all(profile, prop).filter(device__name__in=[l.dst_name for l in lldp_links]):
            alias_names[p.device.name] = prop.cast_value(p.property_value)
        # set LLDP link statuses
        lldp_link_statuses = perform_matching(lldp_neigh, lldp_links, alias_names)
    return lldp_link_statuses

def get_mac_addr_tables(profile, args, all_db_links):
    """
    Retrieve MAC address tables from switches that support ETHERNET links.
    Scan Links in the DB and gather the device names, interface names and roles.
    """
    # links that support only ETHERNET
    eth_links = all_db_links.filter(~Q(src_if_role__exact='data') | ~Q(dst_if_role__exact='data')).select_related('src_device')
    if args:
        if args.filter:    
            eth_links = apply_filters(eth_links, args.filter)
    # scan Links in the DB and gather the device names, interface names and roles
    link_interface_info = {}  # device_name --> role --> (device_interface, src_name, src_interface)
    link_srcs = set()
    for link in eth_links:
        dev_name = link.dst_name
        role = link.dst_if_role
        dev_interface = link.dst_if
        if role == "ipmi_mgmt":
            link_interface_info.setdefault(dev_name, {})["ipmi"] = (dev_interface, link.src_device.name, link.src_if)
            link_interface_info.setdefault(dev_name, {})["mgmt"] = (dev_interface, link.src_device.name, link.src_if)
        else:
            link_interface_info.setdefault(dev_name, {})[role] = (dev_interface, link.src_device.name, link.src_if)
        link_srcs.add(link.src_device.name)
    # SRC switches that have an ethernet link
    eth_switches = Device.get_all(profile).filter(device_type="SW", device_role="MG", name__in=link_srcs).exclude(name__icontains='virtual')
    eth_switch_infos = _gather_switches(profile, eth_switches)
    # for each MG switch, dump the MAC address table
    mac_tab_results = perform_group_action(list(eth_switch_infos), "get_mac_addr_table")
    mac_addr_tables = []
    eth_ok_switches = set()
    for sw, result in mac_tab_results.items():
        if isinstance(result, str):
            logger.error(f"failed to gather MAC address table from {sw}: {result}")
        else:
            mac_addr_tables = mac_addr_tables + result
            eth_ok_switches.add(sw)
    if not eth_ok_switches:
        logger.warning("no switches matched filter or no ETHERNET links found")
    return mac_addr_tables, link_interface_info

def generate_interface_statuses(args, all_db_links):
    """
    Generate interface statuses for all switches in the links.
    """
    # all the switches in the links
    if args:
        if args.filter:
            all_db_links = apply_filters(all_db_links, args.filter)
    switches_in_links = ((all_db_links.values_list('src_device', flat=True).distinct()).union((all_db_links.filter(dst_device__device_type="SW").values_list('dst_device', flat=True).distinct())))
    # switch info of all the switches
    switches_in_links_info = set()
    for sw in switches_in_links:
        try:
            switches_in_links_info.add(device_to_switchinfo(Device.objects.get(device_id=sw)))
        except ValueError as e:
            logger.warning(f"failed to query src device {sw.name}: {e}")
    # retrieve the interface info from all the switches
    all_interface_info = perform_group_action(list(switches_in_links_info), "list_interfaces")
    all_interface_status = {} # switch_name:interface_name --> interface status
    for switch in all_interface_info:
        interfaces = all_interface_info[switch]
        if isinstance(interfaces, List):
            for interface in interfaces:
                all_interface_status[f"{switch}:{interface.name}"] = interface.up     
    return all_interface_status   

def add_int_status(link_statuses: List[LinkNeighStatusRepr], all_interface_status):
    """
    Add interface UP/DOWN status to the link statuses.
    """
    for link_status in link_statuses:
        src_up = ""
        dst_up = ""
        try:
            src_up = all_interface_status[f"{link_status.src}:{link_status.src_if}"]
        except KeyError:
            src_up = ""
        try:
            dst_up = all_interface_status[f"{link_status.dst}:{link_status.dst_if}"]
        except KeyError:
            dst_up = ""
        link_status.src_if_up = src_up
        link_status.dst_if_up = dst_up

def set_int_status(src, src_if, dst, dst_if, all_interface_status) -> tuple[str, str]:
    """
    Return interface UP/DOWN status.
    """
    # SRC
    src_up = ""
    try:
        src_up = all_interface_status[f"{src}:{src_if}"]
    except KeyError:
        src_up = ""
    # DST
    dst_up = ""
    try:
        dst_up = all_interface_status[f"{dst}:{dst_if}"]
    except KeyError:
        dst_up = ""
    # return int statuses
    return (src_up, dst_up)

@dataclasses.dataclass
class _PortRef:
    dev_name: str
    if_name: str
    if_role: str

def get_mgmt_macs(profile) -> Dict[str, _PortRef]:
    """
    scan devices in the DB and gather the MAC addresses of their managment and ipmi interfaces
    """
    all_mgmt_macs : Dict[str, _PortRef] = {} # mac_address --> (dev_name, if_name, if_role)
    all_props = DeviceProperty.get_all(profile).filter(
                                                (Q(property_name__exact='management_info') & Q(property_attribute__exact='mac')) |
                                                (Q(property_name__exact='ipmi_info') & Q(property_attribute__exact='mac'))
                                             ).select_related('device')
    for p in all_props:
        if p.property_name == "management_info":
            all_mgmt_macs.setdefault(p.property_value, _PortRef(dev_name = p.device.name, if_name = "@mgmt", if_role = "mgmt"))
        else:
            all_mgmt_macs.setdefault(p.property_value, _PortRef(dev_name = p.device.name, if_name = "@ipmi", if_role = "ipmi")) 
    return all_mgmt_macs

def mark_missing_links(link_interface_info) -> List[LinkNeighStatusRepr]:   
    """
    mark link status as LINK_STATE_MISSING if the link cannot be matched
    """    
    missing_link_statuses: List[LinkNeighStatusRepr] = []         
    for missing_link in link_interface_info.items():
        for value in missing_link[1].items():
            missing_link_statuses.append(
                LinkNeighStatusRepr(
                    value[1][1], value[1][2], missing_link[0], value[1][0],
                    "ETH", "", "",
                    LINK_STATE_MISSING, "import", "", "", ""
                ) 
            )
    return missing_link_statuses

def generate_eth_statuses(args, mac_addr_tables, mgmt_macs, link_interface_info, all_interface_status) -> List[LinkNeighStatusRepr]:
    """
    Generate LinkNeighStatusReprs by combining the eth information.
    """
    eth_link_statuses: List[LinkNeighStatusRepr] = []
    vlan_links = {} # switch_name:port_name --> [bool, linkneighstatusrepr]
    for entry in mac_addr_tables:
        # try to match MAC address with mgmt/ipmi interface names from DB
        eth_names_macs_entry : _PortRef = mgmt_macs.get(entry.mac)
        # there is a match ==> attach the name of the dst interface and create the link status repr
        if eth_names_macs_entry is not None:
            link_dst_dev = ""
            link_dst_if = ""
            dev_name = eth_names_macs_entry.dev_name
            if_name = eth_names_macs_entry.if_name
            if_role = eth_names_macs_entry.if_role
            try:
                link_dst_dev = dev_name
                link_dst_if = (link_interface_info[dev_name][if_role])[0]
                del link_interface_info[dev_name][if_role]
                if link_interface_info[dev_name] == {}:
                    del link_interface_info[dev_name]
            except KeyError:
                link_dst_dev = ""
                link_dst_if = ""
            # create the link status repr
            src_up, dst_up = set_int_status(entry.switch, entry.name, link_dst_dev, link_dst_if, all_interface_status)
            eth_link_statuses.append(LinkNeighStatusRepr(
                entry.switch, entry.name, link_dst_dev, link_dst_if,
                "ETH", dev_name, if_name,
                LINK_STATE_MATCH, "import", src_up, dst_up, ""
                ))
        # no match ==> check if the entry is part of a VLAN link
        else:
            # search for the link in the VLAN links
            vlan_link_key = f"{entry.switch}:{entry.name}"
            try:
                vlan_link = vlan_links[vlan_link_key]
                # other(s) MAC(s) already on the link
                if vlan_link[0] == False:
                    vlan_links[vlan_link_key][0] = True
                    idx = eth_link_statuses.index(vlan_link[1])
                    eth_link_statuses[idx].dst = ""
                    eth_link_statuses[idx].dst_if = ""
                    eth_link_statuses[idx].dst_matcher_type = "ETH"
                    eth_link_statuses[idx].actual_dst = ""
                    eth_link_statuses[idx].actual_dst_if = "VLAN with many devices"
                    eth_link_statuses[idx].state = LINK_STATE_UNRECOGNIZED
                    eth_link_statuses[idx].link_origin = ""
                    eth_link_statuses[idx].dst_if_up = ""
                    eth_link_statuses[idx].comment = ""
            except:
                # first MAC on the link
                src_up, dst_up = set_int_status(entry.switch, entry.name, "", "", all_interface_status)
                new_link = LinkNeighStatusRepr(
                    entry.switch, entry.name, "", "",
                    "ETH", "???", entry.mac,
                    LINK_STATE_UNRECOGNIZED, "", src_up, dst_up, ""
                    )
                vlan_links[f"{entry.switch}:{entry.name}"] = [False, new_link]
                eth_link_statuses.append(new_link)
    return eth_link_statuses

class LinkStatus(SubCommandABC):
    """
    Gather LLDP and ETHERNET link statuses.
    Check inventory and compare LLDP/MAC status.
    Add interface UP/DOWN status.
    Print table on screen.
    """

    name = "status"

    def construct(self):
        add_filter_arg(self.parser)
        self.add_error_only_flag()
        self.add_arg_output_format()

    def run(self, args):

        # default return value
        rv = 0

        # Refresh MAC address tables on all switches in the profile
        refresh_mac_addr_tables(self.profile)

        # Get all links from the database
        all_db_links = Link.objects.filter(src_device__profile__name=self.profile).exclude(src_device__name__icontains='virtual')
            
        # parallel block
        mgmt_macs : Dict[str, _PortRef] = {}
        all_interface_status = {}
        lldp_link_statuses: List[LinkNeighStatusRepr] = []
        mac_addr_tables = []
        link_interface_info = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            mgmt_macs_f = executor.submit(get_mgmt_macs, self.profile)
            all_interface_status_f = executor.submit(generate_interface_statuses, args, all_db_links)
            lldp_link_statuses_f = executor.submit(generate_lldp_statuses, self.profile, args, all_db_links)
            mac_addr_tables_link_interface_info_f = executor.submit(get_mac_addr_tables, self.profile, args, all_db_links)
            mgmt_macs = mgmt_macs_f.result()
            all_interface_status = all_interface_status_f.result()
            lldp_link_statuses = lldp_link_statuses_f.result()
            mac_addr_tables, link_interface_info = mac_addr_tables_link_interface_info_f.result()

        # Add interface UP/DOWN status to the LLDP link statuses.
        add_int_status(lldp_link_statuses, all_interface_status)

        # generate LinkNeighStatusReprs by combining the eth information
        eth_link_statuses = generate_eth_statuses(args, mac_addr_tables, mgmt_macs, link_interface_info, all_interface_status)

        # mark link status as LINK_STATE_MISSING if the link cannot be matched
        missing_link_statuses: List[LinkNeighStatusRepr] = mark_missing_links(link_interface_info)

        # Combine all statuses
        output_statuses = lldp_link_statuses + eth_link_statuses + missing_link_statuses

        # if asked by user, display only links that didn't match
        if args.error_only:
            output_statuses = [s for s in output_statuses if s.state != LINK_STATE_MATCH]

        # print the table
        print(LinkNeighStatusRepr.format_reprs(output_statuses, args.output))
        
        return rv

def scrape_mac_tables(switch_infos) -> Dict[str, Dict[str, List[str]]]:
    """
    Returns a dictionary of switch names to their MAC address tables
    """  
    logger.debug(f"Scraping MAC table for switches")
    # for each switch, dump the MAC address table
    mac_tab_results = perform_group_action(list(switch_infos), "get_mac_addr_table")
    mac_addr_tables = {} # switch_name --> port_name --> [mac_address]
    ok_switches = set()
    for sw, result in mac_tab_results.items():
        if isinstance(result, str):
            logger.error(f"failed to gather MAC address table from {sw}: {result}")
        else:
            ok_switches.add(sw)
            port_mac = {}
            for entry in result:
                if port_mac.get(entry.name):
                    port_mac[entry.name].append(entry.mac)
                else:
                    port_mac[entry.name] = [entry.mac]
            mac_addr_tables[sw] = port_mac
    if not ok_switches:
        logger.warning("no switches matched filter")
    return mac_addr_tables

def scrape_interface_statuses(switch_infos) -> Dict[str, Dict[str, str]]:
    """
    Returns a dictionary of switch names -> port -> up=true.
    """
    logger.debug(f"Scraping interface status for switches")
    # for each switch, dump the interface statuses
    interface_info = perform_group_action(list(switch_infos), "list_interfaces")
    interface_status = {}
    ok_switches = set()
    for sw, result in interface_info.items():
        if isinstance(result, str):
            logger.error(f"failed to get interface statuses from {sw}: {result}")
        else:
            ok_switches.add(sw)
            link_status = {}
            for entry in result:
                link_status[entry.name] = entry.status
            interface_status[sw] = link_status
    if not ok_switches:
        logger.warning("no switches matched filter")  
    return interface_status


def get_cscfg_device_macs(profile) -> Dict[str, Dict[str, str]]:
    """
    Returns a dictionary of device names to a dict of interface -> MAC addresses from cscfg, e.g. {pdu01: {@mgmt: "MAC"}}
    """
    all_macs = {}
    all_props = DeviceProperty.get_all(profile).filter(
                                                (Q(property_name__exact='management_info') & Q(property_attribute__exact='mac')) |
                                                (Q(property_name__exact='ipmi_info') & Q(property_attribute__exact='mac'))
                                                ).select_related('device')
    for p in all_props:
        mac_value = re.sub(r'[^a-zA-Z0-9:]', '', p.property_value)
        all_macs.setdefault(p.device.name, {})
        if p.property_name == "management_info":
            all_macs[p.device.name].setdefault("@mgmt", mac_value)
        elif p.property_name == "ipmi_info":
            all_macs[p.device.name].setdefault("@ipmi", mac_value)
    return all_macs


@transaction.atomic
def apply_changes(profile, changes: List[LinkDetectMacsRepr], dry_run: bool = False) -> list[str]:
    """ Apply changes to for detected MAC address changes, returning a list of errors if any """
    errors = []
    actions = []
    for cg in changes:
        prop_name = ""
        if cg.dst_if == "@mgmt":
            prop_name = "management_info"
        elif cg.dst_if == "@ipmi":
            prop_name = "ipmi_info"
        
        if not prop_name:
            logger.error(f"unhandled interface type: {cg.dst_if} for device {cg.dst_name}")
            continue

        device = Device.get_device(cg.dst_name, profile)
        if not device:
            errors.append(f"Device {cg.dst_name} had a MAC update but was not found in profile {profile}")
            continue
        
        if not dry_run:
            logger.info(f"Updating {cg.dst_name},{cg.dst_if}: {cg.current_mac} → {cg.actual_macs[0]}")
            device.update_property(prop_name, "mac", cg.actual_macs[0])
        else:
            actions.append(f"cscfg device edit -y -f name={cg.dst_name} -p {prop_name}.mac={cg.actual_macs[0]}")
    
    if dry_run and actions:
        print("\nNo updates applied. Here's the commands you could run to do the updates manually:")
        for action in actions:
            print(action)

    return errors


def sort_devs(expected_sw_port_dev, cscfg_device_macs: Dict[str, Dict[str, str]], mac_table):
    matches : List[LinkDetectMacsRepr] = []
    notfound : List[LinkDetectMacsRepr] = []
    changes : List[LinkDetectMacsRepr] = []
    additions : List[LinkDetectMacsRepr] = []

    # iterate over the expected switches and ports
    for sw_name, ports in expected_sw_port_dev.items():

        # skip switches that are not in the mac_table
        if sw_name not in mac_table:
            continue

        # iterate over the expected ports for the switch
        for port, (expected_dev, expected_if) in ports.items():

            # port not found on the switch --> add to MISSING
            if port not in mac_table[sw_name]:
                logger.warning(f"Port {port} on switch {sw_name} not found in MAC table, expected={expected_dev},{expected_if}")
                notfound.append(
                    LinkDetectMacsRepr(
                        dst_name = expected_dev,
                        dst_if = expected_if,
                        src_name = sw_name,
                        src_if = port,
                        match_state = "MISSING",
                    )
                )
                continue

            # MAC address for the expected device as seen on the switch
            actual_macs = mac_table[sw_name][port]
            if len(actual_macs) > 2:
                logger.error(f"Port {port} on switch {sw_name} has more than 2 MACs!")
                continue

            # device not in the DB --> add to NEW
            if expected_dev not in cscfg_device_macs:
                additions.append(
                    LinkDetectMacsRepr(
                        dst_name = expected_dev,
                        dst_if = expected_if,
                        actual_macs = actual_macs,
                        match_state = "NEW",
                    )
                )
                continue

            # get the MAC address of the device
            ifs = cscfg_device_macs.get(expected_dev)
            if len(actual_macs) == 2:
                if (expected_if != "@ipmi_mgmt"):
                    logger.warning(f"{expected_dev},{expected_if}: {sw_name},{port} has more than one MAC!")
                    continue
                else:
                    for if_name in ifs:
                        cscfg_mac = ifs[if_name]
                        if cscfg_mac in actual_macs:
                            matches.append(
                                    LinkDetectMacsRepr(
                                        dst_name = expected_dev,
                                        dst_if = if_name,
                                        current_mac = cscfg_mac,
                                        actual_macs = [cscfg_mac],
                                        match_state = "MATCH",
                                    )
                                )                           
                            actual_macs.remove(cscfg_mac)
                    if actual_macs:
                        logger.warning(f"MISMATCH MACs found on {expected_dev},{expected_if}: {actual_macs}")
            else:
                if (expected_if == "@ipmi_mgmt"):
                    logger.warning(f"{expected_dev},{expected_if}: {sw_name},{port} has only one MAC!")
                    continue
                else:
                    for if_name in ifs:
                        cscfg_mac = ifs[if_name]
                        if (expected_if != "@ipmi") and (expected_if != "@mgmt"):
                            expected_if = "@mgmt"
                        if expected_if == if_name:     
                            if actual_macs[0] != cscfg_mac:
                                # MAC on the switch != MAC in the DB --> add to CHANGES
                                changes.append(
                                        LinkDetectMacsRepr(
                                            dst_name = expected_dev,
                                            dst_if = if_name,
                                            current_mac = cscfg_mac,
                                            actual_macs = actual_macs,
                                            match_state = "MISMATCH",
                                        )
                                    )   
                            else:
                                # MAC on the switch == MAC in the DB --> add to MATCHES
                                matches.append(
                                        LinkDetectMacsRepr(
                                            dst_name = expected_dev,
                                            dst_if = if_name,
                                            current_mac = cscfg_mac,
                                            actual_macs = [actual_macs[0]],
                                            match_state = "MATCH",
                                        )
                                    )   
    return (matches, notfound, changes, additions)

class LinkDetectMacs(SubCommandABC):
    """ Correlates MAC addresses on switch MAC tables with MAC of devices in DB using link data """

    name = "detect_macs"

    description = """
    Categorizes results into the following match_states:
        MATCH:    Discovered MAC matches the stored MAC
        MISMATCH: Device MAC is different than discovered MAC on device's switch/port
        NEW:      Device MAC was not present and MAC was discovered on device's switch/port
        MISSING:  MAC for device port not discovered on switch MAC table
    
    Permits updating MISMATCH and NEW cases with --update flag.
    """

    def __init__(self, subparsers, cli_instance, profile: str = None, help: str = None, description: str = ""):
        super().__init__(subparsers, cli_instance, profile, help, self.description)

    def construct(self):
        add_filter_arg(self.parser)
        self.add_arg_output_format()
        self.parser.add_argument("--update", action="store_true", help="Run the interactive update flow")

    def run(self, args):

        # default return value
        rv = 0

        # Get all links from the database (exclude LEAF switches)
        db_links = Link.objects.filter(src_device__profile__name=self.profile).exclude(src_device__name__icontains='virtual').exclude(src_device__device_role=DeploymentDeviceRoles.LEAF.value)

        # apply filters
        if args:
            if args.filter:    
                db_links = apply_filters(db_links, args.filter)

        # parse the links and create a dictionary of expected switch:port -> (device_name, device_if)
        expected_sw_port_dev = defaultdict(dict)
        for link in db_links:
            src_name = link.src_device.name
            src_if = link.src_if
            dst_name = link.dst_name
            dst_if = link.dst_if
            expected_sw_port_dev[src_name][src_if] = (dst_name, dst_if)

        # get the managment MAC address of the devices
        cscfg_device_macs = get_cscfg_device_macs(self.profile)

        # ping the devices to refresh the MAC address tables
        refresh_mac_addr_tables(self.profile)

        # gather swith infos
        switches = list(expected_sw_port_dev.keys())
        dev_switches = Device.get_all(self.profile).filter(name__in=switches).exclude(name__icontains='virtual')
        switch_infos = _gather_switches(self.profile, dev_switches)

        # dump the interface statuses from the switches
        port_status = scrape_interface_statuses(switch_infos)

        # dump the MAC address tables from the switches
        mac_table = scrape_mac_tables(switch_infos)

        # sort the devices                            
        matches, notfound, changes, additions = sort_devs(expected_sw_port_dev, cscfg_device_macs, mac_table)
        logger.info(f"Found {len(matches)} matches, {len(changes)} changes, {len(additions)} additions, {len(notfound)} not found")

        # set link_state
        for nf in notfound:
            nf.set_link_state(port_status.get(nf.src_name, {}).get(nf.src_if, 'NOT_SET'))
        
        # print the results
        print(LinkDetectMacsRepr.format_reprs((notfound + additions + changes), args.output))
        updates = changes + additions
        if not updates:
            if args.output == "table":
                print("No mismatched or new MACs discovered")
            return 0

        if args.update:
            # go through the interactive update flow
            while True:
                resp = input(f"\nAccept {len(updates)} updates? Choices: [yes|no] ").lower().strip()
                if resp not in ("yes", "y", "no", "n"):
                    print(f"Invalid choice '{resp}', please try again.")
                    continue
                dry_run = resp in ("no", "n")
                errors = apply_changes(self.profile, updates, dry_run)
                if errors:
                    logger.error(f"Failures during apply MAC updates:\n" + '\n'.join(errors))
                    return 1
                return 0
                
        return rv