from collections import defaultdict

import dataclasses
import ipaddress
import json
import logging
import pathlib
import string
from typing import Dict, List, Optional, Tuple

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.switch.utils import device_to_switchinfo
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles, DeploymentDeviceTypes
from deployment_manager.db.models import Device, get_rack_to_mg_switch
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.dhcp.utils import find_root_server_ip
from deployment_manager.tools.switch.interface import LldpNeighbor
from deployment_manager.tools.switch.switchctl import perform_group_action

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SystemConn:
    system_name: str
    system_port: str  # 1 indexed port, "Port 1"
    switch_name: str
    switch_port: str


def parse_system_port(mac):
    """ Last hex digit of system port MAC corresponds to system port index """
    return f"Port {int(mac[-1], 16)}"


def mc_lldp_to_system_connections(lldps: Dict[str, List[LldpNeighbor]]) -> Tuple[List[SystemConn], List[str]]:
    """
    Args:
        lldps: MC switch name -> LLDP infos from those switches

    Returns: List of system connections and any warnings generated while parsing
    """
    warnings = []
    SYS_PORT_START = 17 # first 16 ports of a 32 port MC are for switch connections
    conns: Dict[str, List[SystemConn]] = defaultdict(lambda: [])
    for mc, neighs in lldps.items():
        sw_conns: List[Optional[SystemConn]] = [None for _ in range(16)]
        sy_conns: List[Optional[SystemConn]] = [None for _ in range(16)]
        for n in neighs:
            src_port = int(n.src_if.split("/")[1])  # HPE media converters use Eth1/PORT convention
            if src_port < SYS_PORT_START:
                sw_conns[src_port - 1] = SystemConn("", "", n.dst_name, n.dst_if)
            else:
                sy_conns[src_port - SYS_PORT_START] = SystemConn(
                    n.dst_name if not n.dst_name.endswith("-cm") else n.dst_name[:-3],
                    parse_system_port(n.dst_if),
                    "", ""
                )
        for i, c in enumerate(sw_conns):
            if not c and sy_conns[i]:
                warnings.append(f"mc {mc} port Eth1/{i+1} should have connected to a LF switch but was empty")
            elif sy_conns[i]:
                sy_conns[i].switch_name = c.switch_name
                sy_conns[i].switch_port = c.switch_port
                conns[sy_conns[i].system_name].append(sy_conns[i])

    complete_conns = []
    for sys, c in conns.items():
        if len(c) != 12:
            warnings.append(f"system {sys} expecting 12 ports connections, got {len(c)}")
        complete_conns.extend(c)
    return complete_conns, warnings


class UpdateSystemConnections(SubCommandABC):
    """
    Scan media converter switches for their system connection information and update the system
    connection document, e.g. the document in input.yaml::cluster_network_config.system_connections_file
    """
    name = "update_system_connections"

    def construct(self):
        self.add_arg_filter(required=False)
        self.parser.add_argument(
            "--merge", default=False, action="store_true",
            help="Merge system connections with existing connections doc based on MC switch source port. "
                 "Do not remove non-existing connections")
        self.parser.add_argument("--output-file", "-O", default="-", help="Output file path")

    def run(self, args):
        query_set = self.filter_devices(args, device_type=DeploymentDeviceTypes.SWITCH.value, query_set=Device.get_all(self.profile))

        # Exclude Credo (CR) because we are not using LLDP or ZTP on them
        query_set = query_set.exclude(
                properties__property_name="vendor", properties__property_attribute="name", properties__property_value="CR")

        query_set = query_set.filter(device_role=DeploymentDeviceRoles.MEDIACONV.value)
        switch_infos = [device_to_switchinfo(d) for d in query_set]
        if not switch_infos:
            logger.warning("no mediaconverter switches found")
            return 0

        merge_file = None
        if args.merge:
            cfg = ConfGen(self.profile).parse_profile()
            merge_file = cfg.get("cluster_network_config", {}).get("system_connections_file", "")
            if not merge_file:
                logger.error("property input.yaml::cluster_network_config.system_connections_file is not set")
                return 1

        # gather switches -> List[LldpInfo]|error
        results = perform_group_action(list(switch_infos), "list_lldp_neighbors")
        errs = [f"{sw} failed to gather lldp: {err}" for sw, err in results.items() if isinstance(err, str)]
        if errs:
            errs = '\n  '.join(errs)
            logger.error(f"encountered {len(errs)} errors obtaining LLDP:\n  {errs}")
            return 1

        new_conns, warnings = mc_lldp_to_system_connections(results)
        if warnings:
            warnings = '\n  '.join(warnings)
            logger.warning(f"encountered {len(warnings)} errors obtaining parsing LLDP:\n  {warnings}")

        if merge_file:
            prev_conns = [SystemConn(**i) for i in json.loads(pathlib.Path(merge_file).read_text())]
            # retain previous entries
            switchport2conn = {f"{c.switch_name}::{c.switch_port}": c for c in new_conns}
            for c in prev_conns:
                k = f"{c.switch_name}::{c.switch_port}"
                if k in switchport2conn:
                    del switchport2conn[k]
            merged_conns = new_conns
            merged_conns.extend(switchport2conn.values())
            new_conns = sorted(merged_conns, key=lambda c: (c.system_name, c.system_port,))
        else:
            new_conns = sorted(new_conns, key=lambda c: (c.system_name, c.system_port,))

        out = json.dumps([c.__dict__ for c in new_conns], indent=2, sort_keys=True)
        if args.output_file == "-":
            logger.info(out)
        else:
            with open(args.output_file, 'w') as f:
                f.write(out)


class GenerateMgmtConfig(SubCommandABC):
    """
    Generate mgmt config for HPE media converter switches and update switch properties with ZTP boot options
    Docs ref: https://www.hpe.com/psnow/doc/a00093990en_us page 36
    """
    name = "generate_mgmt_config"

    STARTUP_CONFIG_TEMPLATE = """
##
## Running-config temporary prefix mode setting
##
no cli default prefix-modes enable

##
## BFD configuration
##
   protocol bfd

##
## Other IPv6 configuration
##
no ipv6 enable


##
## Interface Ethernet buffer configuration
##
   traffic pool roce type lossless
   traffic pool roce memory percent 50.00
   traffic pool roce map switch-priority 5

##
## VLAN configuration
##
   vlan 101-116
   interface ethernet 1/1 switchport access vlan 101
   interface ethernet 1/2 switchport access vlan 102
   interface ethernet 1/3 switchport access vlan 103
   interface ethernet 1/4 switchport access vlan 104
   interface ethernet 1/5 switchport access vlan 105
   interface ethernet 1/6 switchport access vlan 106
   interface ethernet 1/7 switchport access vlan 107
   interface ethernet 1/8 switchport access vlan 108
   interface ethernet 1/9 switchport access vlan 109
   interface ethernet 1/10 switchport access vlan 110
   interface ethernet 1/11 switchport access vlan 111
   interface ethernet 1/12 switchport access vlan 112
   interface ethernet 1/13 switchport access vlan 113
   interface ethernet 1/14 switchport access vlan 114
   interface ethernet 1/15 switchport access vlan 115
   interface ethernet 1/16 switchport access vlan 116
   interface ethernet 1/17 switchport access vlan 101
   interface ethernet 1/18 switchport access vlan 102
   interface ethernet 1/19 switchport access vlan 103
   interface ethernet 1/20 switchport access vlan 104
   interface ethernet 1/21 switchport access vlan 105
   interface ethernet 1/22 switchport access vlan 106
   interface ethernet 1/23 switchport access vlan 107
   interface ethernet 1/24 switchport access vlan 108
   interface ethernet 1/25 switchport access vlan 109
   interface ethernet 1/26 switchport access vlan 110
   interface ethernet 1/27 switchport access vlan 111
   interface ethernet 1/28 switchport access vlan 112
   interface ethernet 1/29 switchport access vlan 113
   interface ethernet 1/30 switchport access vlan 114
   interface ethernet 1/31 switchport access vlan 115
   interface ethernet 1/32 switchport access vlan 116

##
## STP configuration
##
no spanning-tree

##
## L3 configuration
##
   vrf definition mgmt
   interface vlan 101
   interface vlan 102
   interface vlan 103
   interface vlan 104
   interface vlan 105
   interface vlan 106
   interface vlan 107
   interface vlan 108
   interface vlan 109
   interface vlan 110
   interface vlan 111
   interface vlan 112
   interface vlan 113
   interface vlan 114
   interface vlan 115
   interface vlan 116

##
## QoS switch configuration
##
   interface ethernet 1/1-1/32 qos trust L3

##
## Network interface configuration
##
   interface mgmt0 ip address ${mgmt_ip} /${mgmt_prefixlen}

##
## Other IP configuration
##
   hostname ${hostname}
   ip domain-list cerebrassc.local
   ip name-server vrf mgmt 172.28.243.5
   ip route vrf mgmt 0.0.0.0/0 ${mgmt_gateway}

##
## Local user account configuration
##
   username admin password 7 ${admin_pass}
   username monitor password 7 ${monitor_pass}

##
## AAA remote server configuration
##
# ldap bind-password ********
   ldap vrf mgmt enable
   radius-server vrf mgmt enable
# radius-server key ********
   tacacs-server vrf mgmt enable
# tacacs-server key ********

##
## Password restriction configuration
##
no password hardening enable

##
## SNMP configuration
##
   snmp-server community CerebrasColoSC ro
   snmp-server vrf mgmt enable

##
## Network management configuration
##
# web proxy auth basic password ********
no ntp server 172.28.87.100 disable
   ntp server 172.28.87.100 keyID 0
no ntp server 172.28.87.100 trusted-enable
   ntp server 172.28.87.100 version 4
   ntp vrf mgmt enable
   web vrf mgmt enable

##
## Persistent prefix mode setting
##
cli default prefix-modes enable
"""
    ADMIN_PASS = "$6$834bCoeu$smrDL8CVwOh9z0ceDs8Ls4QHJb7Wo6JZCzTHrJtPd3h22Y9oPYf521Fm6zewyco3.WaFH4t0BngrpkvGyIQh51"
    MONITOR_PASS = "$6$eFPoDNie$93wCjNhptioFKWdTNSWbhr5mXRu271rL06sOEUhVpXCtAXqP2vbqg.34Q4rkjm9tukoksz/.0TTud7gpwbVHR/"

    def construct(self):
        self.add_arg_filter(required=False)
        self.parser.add_argument("--output_dir", "-o", default="/var/ftpd", help="Output dir path")

    def run(self, args):
        query_set = self.filter_devices(args, device_type=DeploymentDeviceTypes.SWITCH.value, query_set=Device.get_all(self.profile))

        # Exclude Credo (CR) because we are not using LLDP or ZTP on them
        query_set = query_set.exclude(
                properties__property_name="vendor", properties__property_attribute="name", properties__property_value="CR")

        query_set = query_set.filter(device_role=DeploymentDeviceRoles.MEDIACONV.value)
        if not query_set:
            logger.warning("no mediaconverter switches found")
            return 0

        output_dir = pathlib.Path(args.output_dir)
        if not output_dir.is_dir():
            logger.error(f"output dir {output_dir.absolute()} is not a directory")
            return 1

        cg = ConfGen(self.profile)
        cfg = cg.parse_profile()
        root_server, root_server_ip = find_root_server_ip(self.profile, cfg)

        template = string.Template(self.STARTUP_CONFIG_TEMPLATE)

        rack_mg_sw = get_rack_to_mg_switch(self.profile)

        updated = 0
        for d in query_set:
            # find corresponding mgmt switch
            rack = d.get_prop(props.prop_location_rack)
            if not rack:
                logger.warning(f"skipping {d.name}: no location.rack property set, cannot find mgmt switch")
                continue
            mg_sw = rack_mg_sw.get(rack)
            if not mg_sw:
                logger.warning(f"skipping {d.name}: no mg switch corresponding to location.rack={rack}, "
                                      f"please update {props.prop_switch_info_connected_racks} on an mg switch "
                                      f"connecting this mc switch or {props.prop_location_rack} on this mc switch")
                continue
            subnet: Optional[ipaddress.IPv4Network] = mg_sw.get_prop(props.prop_subnet_info_subnet)
            if not subnet:
                continue

            output_path = output_dir / f"{d.name}.startup_config"
            output_path.write_text(template.substitute(
                mgmt_ip=str(d.get_prop(props.prop_management_info_ip)),
                mgmt_prefixlen=str(subnet.prefixlen),
                hostname=d.name,
                mgmt_gateway=str(mg_sw.get_prop(props.prop_subnet_info_gateway)),
                admin_pass=self.ADMIN_PASS,
                monitor_pass=self.MONITOR_PASS,
            ))
            d.set_prop(props.prop_network_config_dhcp_option_66, f",tftp://{root_server_ip}/,")
            d.set_prop(props.prop_network_config_dhcp_option_67, f",{output_path.name},")
            updated += 1
        logger.info(f"updated {updated} of {len(query_set)} media converter switches")
        return 0 if updated == len(query_set) else 1


class MediaConverterCmd(SubCommandABC):
    """ Media converter switch related actions """

    SUB_CMDS = [UpdateSystemConnections, GenerateMgmtConfig]

    name = "mediaconverter"

    def construct(self):
        subparsers = self.parser.add_subparsers(dest="action", required=True)
        for ssc in self.SUB_CMDS:
            m = ssc(
                subparsers, profile=self.profile, cli_instance=self.cli_instance
            )
            m.build()

    def run(self, args):
        if hasattr(args, "func"):
            args.func(args)
