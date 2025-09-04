#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Misc CLI classes and methods
"""
import ipaddress
import math
import os
import re
from typing import List, Optional

from .base import \
    SubCommandBase, \
    UsesNetworkConfig, \
    add_network_config_arg
from .. import schema, placer, configs, templates, tasks
from ..common.context import SX_VLAN, SYS_VLAN, DEFAULT_VLAN, NetworkCfgDoc
from ..placer.switch import place_xconnect_prefixes
from ..schema import NetworkConfigSchema

from ..placer import switch as switch_placer
from ..placer import vip as vip_placer

class NewConfig(SubCommandBase):
    """ Build a new network configuration
    """
    sub_command = 'new_config'

    @staticmethod
    def build_parser(parser):
        add_network_config_arg(parser)

        parser.add_argument('-n', '--name', required=True,
                            help='Name of the configuration')

        parser.add_argument('-d', '--description',
                            help='Description of the configuration')

    def __call__(self, args):
        network_schema = schema.NetworkConfigSchema()

        network_config = network_schema.new_config(
            name=args.name, description=args.description)

        network_schema.save_json(network_config, args.config)

@UsesNetworkConfig(save=True)
class AddRootServer(SubCommandBase):
    """ Add a root server info to the network configuration
    """
    sub_command = 'add_root_server'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-n', '--name', required=True,
                            help='Name of the root server')

        parser.add_argument('-i', '--ip-address',
                            help='root server address')

    def __call__(self, args):
        args.network_schema.update_root_server(
            args.network_config,
            name=args.name,
            ip_address=args.ip_address)


def set_network_config_environment(
        network_config: dict,
        cluster_prefix: str,
        exterior_prefixes,
        exterior_nonpolicy_prefixes,
        service_prefix,
        overlay_prefixes,
        asn_extents,
        subnet_pools: Optional[List[dict]] = None,
        system_enable_dcqcn: bool = False
):
    """Update network configuration with environment settings

    Args:
        network_config: The network configuration doc to update
        cluster_prefix: Network containing the cluster
        exterior_prefixes: List of exterior prefixes to add
        exterior_nonpolicy_prefixes: List of exterior non-policy prefixes
        service_prefix: k8s service subnets
        overlay_prefixes: k8s pod subnets
        asn_extents: List of ASN extent strings in format "starting_asn,count"
        subnet_pools: Optional list of subnet pools allowed to allocate from the cluster prefix
        system_enable_dcqcn: Flag to enable DCQCN on systems
    """
    env = network_config.get("environment", {})

    parentnet = ipaddress.ip_network(cluster_prefix)

    for entry, value in [
        ("cluster_prefix", cluster_prefix),
        ("exterior_prefixes", exterior_prefixes),
        ("exterior_nonpolicy_prefixes", exterior_nonpolicy_prefixes),
        ("service_prefix", service_prefix),
    ]:
        if value:
            env[entry] = value

    if not subnet_pools:
        # default pool to entire cluster prefix
        env["subnet_pools"] = [
            {
                "subnets": [ cluster_prefix ],
                "description": "cluster prefix",
            }
        ]
    else:
        for subnet_pool in subnet_pools:
            # Check if the subnet pool is a valid prefix and a subnet of the cluster prefix
            for subnet in subnet_pool["subnets"]:
                try:
                    subnet = ipaddress.ip_network(subnet)
                    if not subnet.subnet_of(parentnet):
                        raise ValueError(
                            f"Subnet {subnet} from pool {subnet_pool} is not a subnet of the cluster prefix {cluster_prefix}"
                        )
                except ValueError:
                    raise ValueError(f"Invalid subnet pool: {subnet}")
        env["subnet_pools"] = subnet_pools

    if overlay_prefixes is not None:
        env["overlay_prefixes"] = overlay_prefixes

    if asn_extents is not None:
        cluster_asns = list()
        for asn_extent in asn_extents:
            starting_asn, count = asn_extent.split(',')
            cluster_asns.append(dict(starting_asn=starting_asn, count=int(count)))

        env["cluster_asns"] = cluster_asns

    # Number of cluster data VIPs. We don't expect it to be more than one.
    # It's being saved here since other config modules will have to refer to it.
    env["cluster_data_vip_count"] = 1

    env["system_enable_dcqcn"] = system_enable_dcqcn
    network_config["environment"] = env


@UsesNetworkConfig(save=True)
class RunPlacer(SubCommandBase):
    """ Run the network resource placer
    """
    sub_command = 'placer'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('--no-switches', action='store_true',
                            help="Disable placing switches")

        parser.add_argument('--no-vips', action='store_true',
                            help="Disable placing vips")

        parser.add_argument('--no-xconnect', action='store_true',
                            help="Disable placing cross connections")

        parser.add_argument('--no-interfaces', action='store_true',
                            help="Disable placing interfaces")

    def __call__(self, args):
        # Place switches first
        if not args.no_switches:
            nw_doc = NetworkCfgDoc(args.network_config)
            placer.SwitchASNPlacer(nw_doc)()
            switch_placer.place_switch_prefixes(nw_doc, DEFAULT_VLAN)
            switch_placer.place_switch_prefixes(nw_doc, SYS_VLAN)
            switch_placer.place_switch_prefixes(nw_doc, SX_VLAN)
            placer.SwitchVirtualAddrPlacer(nw_doc)()
            placer.SwitchVLANPlacer(nw_doc)()
        # Then place VIPs and cross connects
        if not args.no_vips:
            vip_placer.place_cluster_vips(nw_doc)
            vip_placer.place_system_vips(nw_doc)

        if not args.no_xconnect:
            place_xconnect_prefixes(nw_doc)

        # Finally place all interfaces
        if not args.no_interfaces:
            placer.InterfacePlacer(nw_doc)()
            placer.SystemVLANInterfacePlacer(nw_doc)()
            placer.SwarmxVLANInterfacePlacer(nw_doc)()


@UsesNetworkConfig(save=True)
class RemoveMgtAddrs(SubCommandBase):
    """ Remove all management addresses from the network configuration
    """
    sub_command = 'remove_mgt_addrs'

    @staticmethod
    def build_parser(_parser):
        pass

    def __call__(self, args):
        for section in args.network_schema.host_sections:
            for obj in args.network_config.get(section, list()):
                if "management_address" in obj:
                    del obj["management_address"]


@UsesNetworkConfig(save=True)
class UpdateOverlayPrefixSize(SubCommandBase):
    """
    Calculate overlay prefix size based on provided overlay
    prefixes and nodes in the configuration
    """
    sub_command = 'update_overlay_prefix_size'

    @staticmethod
    def build_parser(_parser):
        pass

    def __call__(self, args):
        orig_size = args.network_config["environment"].get("overlay_prefix_mask_size")
        if orig_size is not None:
            return  # cluster-mgmt can't handle this variable changing after cluster has been deployed

        overlay_prefix_size = 0
        for op in args.network_config.get("environment", {}).get("overlay_prefixes", []):
            overlay_prefix_size += schema.JSIPNetwork(op).num_addresses
        if not overlay_prefix_size:
            return

        num_nodes = 0
        for section in NetworkConfigSchema.node_sections:
            if section == "user_nodes":
                continue
            num_nodes += len(args.network_config.get(section, []))
        if not num_nodes:
            return

        mask_size = 32 - math.floor(math.log((overlay_prefix_size / num_nodes), 2))
        if mask_size > 27:
            raise ValueError(
                f"calculated environment.overlay_prefix_mask_size was /{mask_size} "
                f"but require at most /{27} (32 addresses)"
            )
        # shouldn't need more than 128 pods per node
        mask_size = max(mask_size, 25)

        args.network_config["environment"]["overlay_prefix_mask_size"] = mask_size


@UsesNetworkConfig(save=False)
class BuildConfigs(SubCommandBase):
    """ Build configuration files
    """
    sub_command = 'build_configs'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-t', '--config_type', default='all',
                        help='Config type to build',
                        choices=('all', 'switches', 'systems', 'nodes'))

        parser.add_argument('-o', '--output_dir', required=True,
                            help="Output directory name")

    def __call__(self, args):
        if args.config_type == 'all':
            writer_cls = configs.AllConfigsWriter

        elif args.config_type == 'switches':
            writer_cls = configs.AllSwitchesConfigWriter

        elif args.config_type == 'systems':
            writer_cls = configs.AllSystemsConfigWriter

        elif args.config_type == 'nodes':
            writer_cls = configs.AllNodesConfigWriter

        writer_cls(args.network_config).write_config(args.output_dir)


@UsesNetworkConfig(save=False)
class BuildSwitchSystemBgpConfigs(SubCommandBase):
    """ Build system vip BGP configuration files
    """
    sub_command = 'build_system_vip_bgp_configs'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-o', '--output_dir', required=True,
                            help="Output directory name")

        parser.add_argument('--system_names', action='append',
                            help=(
                                "Systems for which bgp needs to "
                                "be enabled on switches"
                            ))

    def __call__(self, args):
        writer_cls = configs.EnableSystemBgpConfigWriter
        writer_cls(args.network_config).write_config(args.output_dir)


@UsesNetworkConfig(save=False)
class BuildSwitchSystemStaticConfigs(SubCommandBase):
    """ Build system vip static route configuration files
    """
    sub_command = 'build_system_vip_static_configs'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-o', '--output_dir', required=True,
                            help="Output directory name")

        parser.add_argument('--system_names', action='append',
                            help=(
                                "Systems for which bgp needs to "
                                "be disabled on switches"
                            ))

    def __call__(self, args):
        writer_cls = configs.DisableSystemBgpConfigWriter
        writer_cls(args.network_config).write_config(args.output_dir)


@UsesNetworkConfig(save=True)
class RemoveSystem(SubCommandBase):
    """ Remove one or more CS2s from the network config
    """
    sub_command = 'remove_system'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-n', '--name', action="append",
                            help="Name of the system")

    def __call__(self, args):
        # Filter out systems with names that are in args.name
        args.network_config["systems"] = [
            s for s in args.network_config.get("systems", [])
            if s['name'] not in args.name
        ]

        # Filter out system connections with system names that are in args.name
        args.network_config["system_connections"] = [
            sc for sc in args.network_config.get("system_connections", [])
            if sc['system_name'] not in args.name
        ]


@UsesNetworkConfig(save=True)
class AddSystem(SubCommandBase):
    """ Add a CS to the network configuration
    """
    sub_command = 'add_system'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-n', '--name', required=True,
                            help='Name of the system')

        parser.add_argument('-m', '--management_addr',
                            help='Management address')

        parser.add_argument('--nodegroup',
                            help='Node group of the system (for cluster mgmt)')

        parser.add_argument('--rack',
                            help='Rack name or number')

        parser.add_argument('--rack_unit',
                            help='Position in the rack')

        parser.add_argument('--stamp',
                            help='Stamp name')

        parser.add_argument('--system_vlan', action='store_true',
                            help='Flag to indicate if this system should be included in system vlan')

        parser.add_argument('--default_hostname', default="", required=False,
                            help='Hostname reported by LLDP. Not required if name field is default_hostname')


    def __call__(self, args):
        args.network_schema.add_system(
            args.network_config,
            name=args.name,
            mgt_addr=args.management_addr,
            nodegroup=args.nodegroup,
            rack=args.rack,
            rack_unit=args.rack_unit,
            stamp=args.stamp,
            system_vlan=args.system_vlan,
            default_hostname=args.default_hostname,
        )


@UsesNetworkConfig(save=True)
class SwapSystem(SubCommandBase):
    """ Swap one system for another in the network config. If names are the same, only update the mgmt IP and remove
    the system connections. """
    sub_command = 'swap_system'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('--old-name', required=True,
                            help="Name of the system to swap out")
        parser.add_argument('--new-name', required=True,
                            help="Name of the system to swap in")
        parser.add_argument('--mgmt-ip', required=False,
                            help="New mgmt IP")
        parser.add_argument('--default-hostname', required=False, default="",
                            help="New default hostname")

    def __call__(self, args):
        args.network_schema.update_system_name(
            args.network_config, args.old_name, args.new_name
        )
        if args.mgmt_ip:
            for sys in args.network_config.get("systems", []):
                if sys["name"] == args.new_name:
                    sys["management_address"] = args.mgmt_ip
                    if args.default_hostname != "":
                        sys["default_hostname"] = args.default_hostname
                    break
        args.network_config["system_connections"] = [
            sc for sc in args.network_config.get("system_connections", [])
            if sc['system_name'] != args.old_name
        ]


@UsesNetworkConfig(save=True)
class RemoveNode(SubCommandBase):
    """ Remove one or more nodes from the network config
    """
    sub_command = 'remove_node'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-n', '--name', action="append",
                            help="Name of the node")

    def __call__(self, args):
        for section in NetworkConfigSchema.node_sections:
            args.network_config[section] = [
                n for n in args.network_config.get(section, [])
                if n['name'] not in args.name
            ]


@UsesNetworkConfig(save=True)
class AddNode(SubCommandBase):
    """ Add a node to the network configuration
    """
    sub_command = 'add_node'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-n', '--name', required=True,
                            help='Name of the node')

        parser.add_argument('-m', '--management_addr',
                            help='Management address')

        parser.add_argument('-s', '--section', required=True,
                            help='Node section',
                            choices=schema.NetworkConfigSchema.node_sections)

        parser.add_argument('--nodegroup',
                            help='Node group number (for cluster mgmt)')

        parser.add_argument('--username',
                            help='Log in username')

        parser.add_argument('--password',
                            help='Log in password')

        parser.add_argument('--rack',
                            help='Rack name or number')

        parser.add_argument('--rack_unit',
                            help='Position in the rack')

        parser.add_argument('--stamp',
                            help='Stamp name')

    def __call__(self, args):
        args.network_schema.add_node(
            args.network_config,
            node_section=args.section,
            name=args.name,
            mgt_addr=args.management_addr,
            nodegroup=args.nodegroup,
            username=args.username,
            password=args.password,
            rack=args.rack,
            rack_unit=args.rack_unit,
            stamp=args.stamp)


@UsesNetworkConfig(save=True)
class SwapNode(SubCommandBase):
    """ Swap one node for another in the network config
    """
    sub_command = 'swap_node'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('--old-name', required=True,
                            help="Name of the node to swap out")
        parser.add_argument('--new-name', required=True,
                            help="Name of the node to swap in")

    def __call__(self, args):
        section = ""
        for n in args.network_config.get("management_nodes", []):
            if n['name'] == args.name:
                section = "management_nodes"

        for n in args.network_config.get("memoryx_nodes", []):
            if n['name'] == args.name:
                section = "memoryx_nodes"

        for n in args.network_config.get("activation_nodes", []):
            if n['name'] == args.name:
                section = "activation_nodes"

        for n in args.network_config.get("worker_nodes", []):
            if n['name'] == args.name:
                section = "worker_nodes"

        for n in args.network_config.get("swarmx_nodes", []):
            if n['name'] == args.name:
                section = "swarmx_nodes"

        for n in args.network_config.get("user_nodes", []):
            if n['name'] == args.name:
                section = "user_nodes"

        args.network_schema.update_node_name(
            args.network_config, section, args.old_name, args.new_name
        )


@UsesNetworkConfig(save=True)
class RemoveSwitch(SubCommandBase):
    """ Remove one or more switches from the network config
    """
    sub_command = 'remove_switch'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-n', '--name', action="append",
                            help="Name of the switch")

    def __call__(self, args):
        switches = [
            s for s in args.network_config.get("switches", [])
            if s["name"] not in args.name
        ]
        args.network_config["switches"] = switches

        # remove xconnects associated with these switches
        if "connections" not in args.network_config.get("xconnect", {}):
            return

        args.network_config["xconnect"]["connections"] = [
            x for x in args.network_config["xconnect"]["connections"]
            if len(x.get("links")) == 2 and (
                    x["links"][0]["name"] not in args.name and
                    x["links"][1]["name"] not in args.name
            )
        ]


@UsesNetworkConfig(save=True)
class AddSwitch(SubCommandBase):
    """ Add a switch to the network configuration
    """
    sub_command = 'add_switch'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-n', '--name', required=True,
                            help='Name of the switch')

        parser.add_argument('-m', '--management_addr',
                            help='Management address')

        parser.add_argument('-v', '--vendor', required=True,
                            help='Switch vendor')

        parser.add_argument('--model',
                            help='Name of the switch model')

        parser.add_argument('-t', '--tier', required=True,
                            help='Switch tier')

        parser.add_argument('-p', '--position', required=True, type=int,
                            help='Switch tier position')

        parser.add_argument('--username',
                            help='Log in username')

        parser.add_argument('--password',
                            help='Log in password')

        parser.add_argument('--rack',
                            help='Rack number or name')

        parser.add_argument('--rack_unit',
                            help='Position in the rack')

        parser.add_argument('--stamp',
                            help='Stamp name')

        parser.add_argument('--prefix',
                            help='Block of IPs to use for devices connected to this switch')

        parser.add_argument('--system_prefix',
                            help='Block of IPs to use for systems connected to this switch')

        parser.add_argument('--swarmx_prefix',
                            help='Block of IPs to use for swarmx nodes connected to this switch, if it is a leaf switch')

    def __call__(self, args):
        args.network_schema.add_switch(
            args.network_config,
            name=args.name,
            mgt_addr=args.management_addr,
            vendor=args.vendor,
            model=args.model,
            tier=args.tier,
            tier_pos=args.position,
            username=args.username,
            password=args.password,
            rack=args.rack,
            rack_unit=args.rack_unit,
            stamp=args.stamp,
            prefix=args.prefix,
            system_prefix=args.system_prefix,
            swarmx_prefix=args.swarmx_prefix)


@UsesNetworkConfig(save=True)
class AddExteriorSwitch(SubCommandBase):
    """ Add a switch to the network configuration
    """
    sub_command = 'add_exterior_switch'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-n', '--name', required=True,
                            help='Name of the switch')

        parser.add_argument('-m', '--management_addr',
                            help='Management address')

        parser.add_argument('-v', '--vendor', default='unknown',
                            help='Switch vendor')

        parser.add_argument('-M', '--mtu', type=int, required=True,
                            help='MTU (1500-9000)')

        parser.add_argument('-a', '--asn', type=schema.ASN4,
                            help='ASN')


    def __call__(self, args):
        if (args.mtu < 1500) or (args.mtu > 9000):
            raise ValueError('mtu out of bounds')

        if "exterior_switches" not in args.network_config:
            args.network_config["exterior_switches"] = list()


        switch_obj = dict(name=str(args.name),
                          vendor=str(args.vendor),
                          mtu=int(args.mtu))

        if args.asn is not None:
            switch_obj["asn"] = str(args.asn)

        if hasattr(args, 'management_addr') and args.management_addr is not None:
            switch_obj["management_addr"] = str(args.management_addr)

        for es in args.network_config["exterior_switches"]:
            if es["name"] == str(args.name):
                es.update(switch_obj)
                break
        else:
            args.network_config["exterior_switches"].append(switch_obj)


@UsesNetworkConfig(save=True)
class AddExteriorConnections(SubCommandBase):
    """Add exterior connections to the network configuration from an
    input file.
    """
    sub_command='add_exterior_conn'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-i', '--input_file', required=True,
                            help='Input file of space separated values')

    def __call__(self, args):
        with open(args.input_file) as input_fd:
            for line in input_fd:
                fields = line.split()
                entry = dict(
                    interior=dict(
                        switch_name=fields[0],
                        switch_port=fields[1],
                        address=fields[2]
                    ),
                    exterior=dict(
                        switch_name=fields[3],
                        switch_port=fields[4],
                        address=fields[5]
                    ),
                    name=f'L3 {fields[0]} and {fields[3]}')

                if "exterior_connections" not in args.network_config:
                    args.network_config["exterior_connections"]=list()

                if entry not in args.network_config["exterior_connections"]:
                    args.network_config["exterior_connections"].append(entry)


@UsesNetworkConfig(save=False)
class ExteriorSwitchConfig(SubCommandBase):
    """Generate a configuration snippet for the exterior switch side of
    the uplink.
    """
    sub_command = 'exterior_switch_cfg'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-n', '--name', required=True,
                            help="name of the switch to generate")

    def __call__(self, args):
        ext_switches = dict()
        for switch_obj in args.network_config.get("exterior_switches", list()):
            ext_switches[switch_obj["name"]] = switch_obj

        int_switches = dict()
        for switch_obj in args.network_config.get("switches", list()):
            int_switches[switch_obj["name"]] = switch_obj

        if args.name not in ext_switches:
            raise RuntimeError(f'Unknown switch: {args.name}')

        for conn in args.network_config.get("exterior_connections", list()):
            if conn["interior"]["switch_name"] not in int_switches:
                continue
            int_switch_obj = int_switches[conn["interior"]["switch_name"]]

            if conn["exterior"]["switch_name"] != args.name:
                continue
            ext_switch_obj = ext_switches[conn["exterior"]["switch_name"]]

            ext_switch_vendor = ext_switch_obj.get("vendor", "unknown")

            outlines = \
                templates.switch.L3PeerIface(ext_switch_vendor).substitute(
                    name=str(conn["exterior"]["switch_name"]),
                    neighbor=str(conn["interior"]["switch_name"]),
                    my_port=str(conn["exterior"]["switch_port"]),
                    my_address=str(conn["exterior"]["address"]),
                    mtu=str(9000)).splitlines()

            if ext_switch_obj.get("asn", ""):
                outlines += templates.switch.L3PeerBgp(
                    ext_switch_vendor).substitute(
                        name=str(conn["exterior"]["switch_name"]),
                        neighbor=str(conn["interior"]["switch_name"]),
                        asn=str(ext_switch_obj["asn"]),
                        neighbor_address=str(
                            schema.JSIPInterface(
                                conn["interior"]["address"]).ip),
                        neighbor_asn=str(int_switch_obj["asn"]),
                        peer_group=str("__INVALID__")
                    ).splitlines()


            for line in outlines:
                if "__INVALID__" not in line:
                    print(line)


@UsesNetworkConfig(save=False)
class AllHosts100G(SubCommandBase):
    """ Generate a table of host to 100G IP address based on the name
    """
    sub_command = 'all_hosts_100G'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-S', '--separator', default=",",
                            help='Separator between fields')

        parser.add_argument('-m', '--management', action='store_true',
                            help="Include management addresses")

    @staticmethod
    def _print_addr_entry(args, name, obj, address_field, iface_suffix):
        if obj.get(address_field, ""):
            addr_value = str(schema.JSIPInterface(obj[address_field]).ip)
            addr_entry = f'{addr_value}{args.separator}'
            addr_entry += f'{name}{iface_suffix}'
            print(addr_entry)

    def __call__(self, args):
        for switch_obj in args.network_config.get('switches', list()):
            if args.management:
                self._print_addr_entry(args, switch_obj['name'], switch_obj,
                                       'management_address', '')

            self._print_addr_entry(args, switch_obj['name'], switch_obj,
                                   'address', f'-v1')

        for system_obj in args.network_config.get('systems', list()):
            if args.management:
                self._print_addr_entry(args, system_obj['name'], system_obj,
                                       'management_address', '')

            self._print_addr_entry(args, system_obj['name'], system_obj,
                                   'vip', f'-vip')

            for iface_obj in system_obj.get("interfaces", list()):
                self._print_addr_entry(args, system_obj["name"], iface_obj,
                                       "address",
                                       f'-{iface_obj["name"]}')

        iface_ids = dict()
        for ifname, index in configs.node.NODE_INTERFACE_INDEXES:
            iface_ids[ifname] = index

        for node_section in schema.NetworkConfigSchema.node_sections:
            for node_obj in args.network_config.get(node_section, list()):
                if args.management:
                    self._print_addr_entry(args, node_obj['name'], node_obj,
                                           'management_address', '')

                node_ifaces = node_obj.get("interfaces", list())
                sorted_ifaces = sorted(node_ifaces,
                                       key=lambda x: iface_ids[x["name"]])
                idx=0
                for iface_obj in sorted_ifaces:
                    self._print_addr_entry(args, node_obj["name"],
                                           iface_obj, "address",
                                           f'-p{str(idx)}')
                    idx += 1


@UsesNetworkConfig(save=True)
class ImportMgmtHosts(SubCommandBase):
    """ Import management hosts file in standard "/etc/hosts".
    """
    sub_command = 'import_mgmt_hosts'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-i', '--input', required=True,
                            help='Input hosts file')

    def __call__(self, args):
        host_ip = dict()

        matcher = re.compile(r'^(\S+)\s+(\S+).*$')

        with open(args.input) as input_file:
            for input_line in input_file.readlines():
                match_obj = matcher.match(input_line)
                if match_obj is not None:
                    host_ip[match_obj.group(2)] = match_obj.group(1)

        for section in schema.NetworkConfigSchema.host_sections:
            for obj in args.network_config.get(section, list()):
                if obj["name"] in host_ip:
                    obj["management_address"] = host_ip[obj["name"]]


@UsesNetworkConfig(save=False)
class ConnectionList(SubCommandBase):
    """ List connections
    """
    sub_command = 'connection_list'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-S', '--separator', default=",",
                            help='Separator between fields')

    def __call__(self, args):
        print("SWITCH CROSS CONNECTS: ---------------------------------------")
        connections = args.network_config.get(
            'xconnect', dict()).get('connections', list())

        for xconn_obj in connections:
            if "links" in xconn_obj:
                print(xconn_obj["links"][0]["name"],
                      xconn_obj["links"][0]["port"],
                      xconn_obj["links"][1]["name"],
                      xconn_obj["links"][1]["port"],
                      sep=args.separator)

        print("SYSTEMS: ---------------------------------------")
        for system_obj in args.network_config.get('systems', list()):
            iface_links = dict()
            for iface_obj in system_obj.get("interfaces", list()):
                link=dict()
                link["switch_name"] = iface_obj.get("switch_name", "MISSING")
                link["switch_port"] = iface_obj.get("switch_port", "")

                if iface_obj["name"].startswith("data"):
                    tag="data"

                elif iface_obj["name"].startswith("control"):
                    tag="control"

                else:
                    continue

                iface_links[int(iface_obj["name"][len(tag):])] = link

            for iface_num in sorted(iface_links.keys()):
                print(system_obj["name"],
                      str(iface_num),
                      iface_links[iface_num]["switch_name"],
                      iface_links[iface_num]["switch_port"],
                      sep=args.separator)

        print("NODES: ---------------------------------------")
        for node_section in schema.NetworkConfigSchema.node_sections:
            for node_obj in args.network_config.get(node_section, list()):
                for iface_obj in node_obj.get("interfaces", list()):
                    print(node_obj["name"],
                          iface_obj["name"],
                          iface_obj.get("switch_name", "MISSING"),
                          iface_obj.get("switch_port", ""),
                          sep=args.separator)


@UsesNetworkConfig(save=True)
class ImportSwitchLLDP(SubCommandBase):
    """ Import switch LLDP data from a file instead of via the task runner.
    """
    sub_command = 'import_switch_lldp'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-f', '--filename', required=True,
                            help='Input file with LLDP data')

        parser.add_argument('-n', '--name', required=True,
                            help='Switch name')

        parser.add_argument('--clear_xconnects', action='store_true',
                            help='Remove cross connections for this switch')

    def __call__(self, args):
        if args.clear_xconnects:
            to_clear = list()
            for connection in args.network_config.get(
                    "xconnect", dict()).get("connections", list()):
                if "links" in connection:
                    if (connection["links"][0]["name"] == args.name) or \
                       (connection["links"][0]["name"] == args.name):
                        to_clear.append(connection)

            for connection in to_clear:
                args.network_config["xconnect"][
                    "connections"].remove(connection)

        with open(args.filename, 'rb') as input_file:
            lldp_data = input_file.read()

        for switch_obj in args.network_config.get("switches", list()):
            if switch_obj["name"] == args.name:
                parser = tasks.SwitchLLDPPost(args.network_config, switch_obj)
                parser((lldp_data, b'', 0))
                break

        else: # for switch_obj in ...
            raise ValueError(f'Unknown switch name: {args.name}')


@UsesNetworkConfig(save=True)
class BatchImportNodeLLDP(SubCommandBase):
    """ Import node LLDP data from a directory with files instead of running the probe.
    """
    sub_command = 'batch_import_node_lldp'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-d', '--directory', required=True,
                            help="Directory name to descend looking for files")

    def __call__(self, args):
        for section in args.network_schema.node_sections:
            for node_obj in args.network_config.get(section, list()):
                lldp_path = os.path.join(args.directory,
                                         section, node_obj["name"],
                                         'lldp.json')
                if not os.path.isfile(lldp_path):
                    print(f'Missing: {lldp_path}')
                    continue

                parser = tasks.NodeLLDPPost(args.network_config, node_obj)

                with open(lldp_path, 'rb') as lldp_file:
                    lldp_data = lldp_file.read()
                    parser((lldp_data, b'', 0))


@UsesNetworkConfig(save=False)
class BuildSwitchDcqcnConfigs(SubCommandBase):
    """ Build configuration files to enable/disable DCQCN on switches
    """
    sub_command = 'build_switch_dcqcn_configs'

    @staticmethod
    def build_parser(parser):
        parser.add_argument('-o', '--output_dir', required=True,
                            help="Output directory name")

        parser.add_argument('--action', required=True,
                            choices=("enable", "disable"),
                            help=(
                                "Choose whether generated configuration should"
                                " 'enable' or 'disable' DCQCN on switches"
                            ))

        parser.add_argument('--switch_category', required=True,
                            choices=("leaf", "spine"),
                            help=(
                                "Category of switches for which configuration"
                                " is to be built [leaf|spine]"
                            ))

        parser.add_argument('--ecn_min_threshold', type=int,
                            help="Minimum ECN threshold in kbytes")

        parser.add_argument('--ecn_max_threshold', type=int,
                            help="Maximum ECN threshold in kbytes")

        parser.add_argument('--ecn_probability', type=int,
                            help="ECN probability (1-100)")

        parser.add_argument('--ecn_delay', type=int,
                            help="ECN delay in milliseconds")

        parser.add_argument('--interface_type', type=str,
                            choices=("100g", "400g", "800g"),
                            help=("Type of interface to apply DCQCN config to. "
                                "Will be applied to all interfaces by default"))

    def __call__(self, args):
        if args.ecn_min_threshold and args.ecn_min_threshold < 0:
            raise ValueError("Invalid ecn_min_threshold. Must be a positive integer")
        if args.ecn_max_threshold and args.ecn_max_threshold < 0:
            raise ValueError("Invalid ecn_max_threshold. Must be a positive integer")
        if args.ecn_probability and (args.ecn_probability < 1 or args.ecn_probability > 100):
            raise ValueError("Invalid ecn_probability. Must be integer between 1 and 100")
        if args.ecn_delay and args.ecn_delay < 0:
            raise ValueError("Invalid ecn_delay. Must be a positive integer")

        switch_category = "LF" if args.switch_category.lower() == "leaf" else "SP"
        configs.DcqcnConfigWriter(
            args.network_config, (args.action.lower()=="enable"), switch_category,
            args.ecn_min_threshold, args.ecn_max_threshold, args.ecn_probability, args.ecn_delay,
            args.interface_type
        ).write_config(args.output_dir)
