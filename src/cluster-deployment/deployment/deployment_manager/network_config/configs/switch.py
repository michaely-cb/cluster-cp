#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

"""
Configuration builders for switches
"""
import json
import logging
from collections import deque

from .base import (
    _ConfigFileWriterBase,
    _ConfigAggregateBase,
    MTU_DEFAULT,
    MTU_MAX_WITH_OVERHEAD
)
from ..common.context import DEFAULT_VRF_NAME
from ..schema import (
    ASN4,
    JSIPAddress,
    JSIPInterface,
    JSIPNetwork, NetworkConfigSchema
)
from ..templates.switch import (
    L3Base,
    L3SystemBgpBase,
    L3BaseLeaf,
    L3BaseSpine,
    L3PeerIface,
    L3PeerBgp,
    L2Iface,
    L2SystemIface,
    L2SystemVlan,
    L2SwarmxVlan,
    L3UplinkIface,
    L3ClearStaticRoute,
    L3StaticRoute,
    L3MultiMgmtBgp,
    Ntp,
    Snmp
)
from ..templates.switch.arista import (
    DcqcnEnableBase,
    DcqcnEnableIface,
    DcqcnDisableBase,
    DcqcnDisableIface,
    MmuProfileIface
)
from ..utils import items_name_iter

logger = logging.getLogger(__name__)

GBPS = 10**9
SWITCHES_WITH_MMU_SUPPORT = [
    ("arista", "7060DX5"),
    ("arista", "7060X6"),
]
LF_SPEED_MMU_PROFILE = {
    100 * GBPS: "mmu-profile-100g",
    400 * GBPS: "mmu-profile-400g",
    800 * GBPS: "mmu-profile-800g"
}
LF_SP_SPEED_MMU_PROFILE_OVERRIDES = {
    ("arista", "7060X6", "arista", "7808", 800 * GBPS): "mmu-profile-800g-jericho",
}
# TODO: MMU settings should probably be pushed into the network model
LF_SP_MMU_HEADROOM_POOL_SIZES = {
    # LF/SP pairs overrides
    ("arista", "7060X6", "arista", "7808"): 24000,

    # LF defaults
    ("arista", "7060X6"): 9600,
    ("arista", "7060DX5"): 9600,
}

def default_switch_mtu(vendor):
    """
    Juniper includes ethernet frame encapsulation overheads + payload
    while other vendors don't. MTU set to 9000 on the server side does
    not include these overheads. So an L3 MTU of 9000 is encapsulated
    by ~18 bytes at L2, thus exceeding the Juniper MTU
    """
    return MTU_MAX_WITH_OVERHEAD if vendor == "juniper" else MTU_DEFAULT

def parse_ethernet_port_number(port_desc: str) -> str:
    if port_desc.startswith("Ethernet"):
        return port_desc[len("Ethernet"):]
    if port_desc.startswith("Eth"):
        return port_desc[len("Eth"):]
    return port_desc


# pylint: disable=too-many-instance-attributes
class SwitchConfigWriter(_ConfigFileWriterBase):
    """ Write switch configuration files
    """
    config_class_name = "switches"
    config_file_mode = 0o644

    @property
    def config_item_name(self):
        return self.name

    def __init__(self, network_config, switch_name):
        self._network_config = network_config
        self._switch_name = switch_name
        switches = dict(items_name_iter(network_config["switches"]))
        self._switch_vendor_model_map = {name: (s.get("vendor", ""), s.get("model", "")) for name, s in switches.items()}

        switch_obj = switches[switch_name]

        self.name = switch_name
        self._vrf = self._network_config.get("environment", {}).get("vrf_name", DEFAULT_VRF_NAME)

        self.root_server = network_config.get("root_server", {})
        self.is_memx = (switch_obj.get("tier", "AW") == "AW")
        self.is_spine = (switch_obj.get("tier", "AW") == "SP")
        self.is_leaf = (switch_obj.get("tier", "AW") == "LF")

        self.mtu = int(switch_obj.get("mtu", default_switch_mtu(switch_obj["vendor"])))
        self.asn = ASN4(switch_obj["asn"])
        self.address = ""
        if "address" in switch_obj:
            self.address = JSIPInterface(switch_obj["address"])
        self.system_address = ""
        if "system_vlan_address" in switch_obj:
            self.system_address = JSIPInterface(switch_obj["system_vlan_address"])
        self.swarmx_address = ""
        if "swarmx_vlan_address" in switch_obj:
            self.swarmx_address = JSIPInterface(switch_obj["swarmx_vlan_address"])
        if "router_id" in switch_obj:
            self.router_id = JSIPAddress(switch_obj["router_id"])
        else:
            self.router_id = ""
            if self.address:
                self.router_id = JSIPInterface(switch_obj["address"]).ip
        self.prefix = ""
        if "prefix" in switch_obj:
            self.prefix = JSIPNetwork(switch_obj["prefix"])
        self.system_prefix = ""
        if "system_prefix" in switch_obj:
            self.system_prefix = JSIPNetwork(switch_obj["system_prefix"])
        self.swarmx_prefix = ""
        if "swarmx_prefix" in switch_obj:
            self.swarmx_prefix = JSIPNetwork(switch_obj["swarmx_prefix"])
        self.cluster_prefix = JSIPNetwork(network_config["environment"]["cluster_prefix"])
        self.vip_prefix = JSIPNetwork(network_config["vips"]["prefix"])
        self.switch_vendor = switch_obj["vendor"]
        self.switch_model = switch_obj.get("model")

        self.vlan = switch_obj["vlan"]
        self.system_vlan = switch_obj.get("system_vlan")
        self.swarmx_vlan = switch_obj.get("swarmx_vlan")
        self.nodes = []
        for node_section in NetworkConfigSchema.node_sections:
            self.nodes.extend(network_config.get(node_section, []))
        self.swarmx_nodes = network_config.get("swarmx_nodes", [])
        self.systems = {s['name']: s for s in network_config.get('systems', [])}

        self.switch_ports = dict()
        for p in switch_obj.get("ports", []):
            self.switch_ports[p['name']] = p

        ecn_min_threshold = "444 kbytes"
        ecn_max_threshold = "4 mbytes"
        if self.is_leaf:
            ecn_min_threshold = "890 kbytes"
            ecn_max_threshold = "8 mbytes"
        ecn_probability = 100

        self.links = list()
        self.uplinks = list()
        self._peer_spine_models = set()

        for connection in network_config.get("xconnect", {}).get("connections", []):
            prefix = JSIPNetwork(connection["prefix"])
            my_idx = None

            if connection["links"][0]["name"] == switch_name:
                my_idx = 0

            elif connection["links"][1]["name"] == switch_name:
                my_idx = 1

            if my_idx is None:
                continue

            neighbor_idx = (my_idx + 1) % 2
            neighbor_switch_obj = switches[connection["links"][neighbor_idx]["name"]]

            neighbor_mtu = int(neighbor_switch_obj.get("mtu", default_switch_mtu(neighbor_switch_obj["vendor"])))
            link_mtu = min(self.mtu, neighbor_mtu)

            if self.is_leaf:
                self._peer_spine_models.add((neighbor_switch_obj["vendor"], neighbor_switch_obj["model"],))

            link = dict(
                name=connection["name"],
                my_address=JSIPInterface(str(prefix[my_idx]) + '/' + str(prefix.prefixlen)),
                my_port=connection["links"][my_idx]["port"],
                neighbor=neighbor_switch_obj["name"],
                neighbor_port=connection["links"][neighbor_idx]["port"],
                neighbor_address=JSIPAddress(str(prefix[neighbor_idx])),
                neighbor_asn=ASN4(neighbor_switch_obj["asn"]),
                mtu=link_mtu,
                ecn_min_threshold=ecn_min_threshold,
                ecn_max_threshold=ecn_max_threshold,
                ecn_probability=ecn_probability,
                peer_group="ws_cluster_switches"
            )

            self.links.append(link)

        exterior_switches = dict(items_name_iter(
            network_config.get("exterior_switches", "")))

        mgmt_node_data_address = ""
        for m in network_config.get("management_nodes", []):
            for i in m.get("interfaces", []):
                if i["switch_name"] == switch_name:
                    mgmt_node_data_address = i["address"]
                    break
            if mgmt_node_data_address:
                break

        for uplink in network_config.get("exterior_connections", list()):
            if uplink["interior"]["switch_name"] != switch_name:
                continue

            if uplink["exterior"]["switch_name"] not in exterior_switches:
                continue

            exterior_obj = exterior_switches[uplink["exterior"]["switch_name"]]

            if mgmt_node_data_address:
                mgmt_node_data_address = JSIPInterface(mgmt_node_data_address).ip
            link = dict(
                name=uplink["name"],
                my_address=JSIPInterface(uplink["interior"]["address"]),
                my_port=uplink["interior"]["switch_port"],
                neighbor=uplink["exterior"]["switch_name"],
                neighbor_address=JSIPInterface(uplink["exterior"]["address"]).ip,
                mtu=int(exterior_obj.get("mtu", default_switch_mtu(exterior_obj.get("vendor")))),
                mgmt_node_data_address=mgmt_node_data_address,
            )

            if exterior_obj.get("asn", ""):
                link["peer_group"] = "exterior_switches"
                link["neighbor_asn"] = ASN4(exterior_obj["asn"])

            self.uplinks.append(link)

        self.ports = list()
        self.system_ports = list()

        for node in self.nodes:
            for i in node.get("interfaces", []):
                if i["switch_name"] != self.name:
                    continue
                vlan = self.vlan
                if self.swarmx_vlan and node in self.swarmx_nodes:
                    vlan = self.swarmx_vlan
                self.ports.append(
                    dict(
                        my_port=i["switch_port"],
                        neighbor=f'{node["name"]}/{i["name"]}',
                        switch_vlan=vlan,
                        mtu=self.mtu,
                        ecn_min_threshold=ecn_min_threshold,
                        ecn_max_threshold=ecn_max_threshold,
                        ecn_probability=ecn_probability,
                        node_name=node["name"]
                    )
                )
        for sc in network_config.get("system_connections", []):
            if sc["switch_name"] != self.name:
                continue
            if sc["system_name"] not in self.systems:
                logger.warning(
                    f"system '{sc['system_name']}' found in .system_connections[] but not in .system[]."
                    " This indicates either stale data in network_config.json after a system was removed or "
                    " a bug in the LLDP / system_connections import"
                )
                continue
            vlan = self.vlan
            if self.systems[sc["system_name"]].get("system_vlan", "false") == "true" or self.is_leaf:
                vlan = self.system_vlan
            self.system_ports.append(
                dict(
                    my_port=sc["switch_port"],
                    neighbor=f'{sc["system_name"]}/{sc["system_port"].replace(" ", "_")}',
                    switch_vlan=vlan,
                    mtu=self.mtu,
                    ecn_min_threshold=ecn_min_threshold,
                    ecn_max_threshold=ecn_max_threshold,
                    ecn_probability=ecn_probability,
                    system_name=sc["system_name"]
                )
            )

        self.clear_vip_routes = list()
        self.vip_routes = list()
        cleared_routes = list()
        if self.is_leaf:
            for system_obj in network_config.get("systems", list()):
                for iface_obj in system_obj.get("interfaces", list()):
                    if not iface_obj["name"].startswith("control"):
                        continue
                    if iface_obj.get("switch_name") == switch_name:
                        dest = iface_obj.get("address")
                        if not dest:
                            continue
                        self.vip_routes.append(dict(
                            neighbor=system_obj["name"],
                            neighbor_asn=ASN4("65010.16"),
                            neighbor_address=dest.split('/')[0],
                            peer_group="ws_cluster_csx"
                        ))
        else:
            for system_obj in network_config.get("systems", list()):
                dest = system_obj.get("vip", "")
                if not dest:
                    continue

                for iface_obj in system_obj.get("interfaces", list()):
                    if not iface_obj["name"].startswith("control"):
                        continue

                    via = iface_obj.get("address", "")
                    if via and (iface_obj.get("switch_name", "") == switch_name):
                        self.vip_routes.append(
                            dict(dest=JSIPNetwork(dest),
                                 via=JSIPInterface(via).ip,
                                 system_name=system_obj["name"]))
                        if dest not in cleared_routes:
                            self.clear_vip_routes.append(dict(
                                dest=JSIPNetwork(dest),
                                system_name=system_obj["name"]
                            ))
                            cleared_routes.append(dest)


    def output_config(self):
        return ""


class SwitchConfigJSONWriter(SwitchConfigWriter):

    config_file_name = "l3_config.json"

    def output_config(self):
        doc = self.generate_doc()
        return json.dumps(doc, indent=2)

    def generate_doc(self) -> dict:
        output = dict()
        # L3 Base template
        cls = L3Base
        if self.is_leaf:
            cls = L3BaseLeaf
        elif self.is_spine:
            cls = L3BaseSpine

        # apply the correct MMU headroom cell values depending on who the neighbor is
        mmu_headroom_pool_limit_cells = "not-applicable"
        mmu_key = (self.switch_vendor, self.switch_model,)
        if self.is_leaf and mmu_key in SWITCHES_WITH_MMU_SUPPORT:
            mmu_headroom_keys = deque()
            mmu_headroom_keys.append(mmu_key)
            if len(self._peer_spine_models) == 1:
                peer_vendor_model = list(self._peer_spine_models)[0]
                mmu_headroom_keys.appendleft((self.switch_vendor, self.switch_model, peer_vendor_model[0], peer_vendor_model[1],))
            elif len(self._peer_spine_models) > 1:
                logger.error(f"more than one spine vendor/model detected, MMU profile settings may be incorrect! {self._peer_spine_models}")

            for k in mmu_headroom_keys:
                if k in LF_SP_MMU_HEADROOM_POOL_SIZES:
                    mmu_headroom_pool_limit_cells = LF_SP_MMU_HEADROOM_POOL_SIZES[k]
                    break
            else:
                raise AssertionError(f"leaf {self.name} ({mmu_key}) supports MMU settings but did not define an MMU headroom pool size!")

        output["base"] = cls(self.switch_vendor, model_name=self.switch_model).substitute(
            name=str(self.name),
            asn=str(self.asn),
            asn_num=int(self.asn),
            asn_colon=str(self.asn).replace('.', ':'),
            switch_address=str(self.address),
            switch_address_netmask=str(self.address.as_netmask) if self.address else "",
            router_id=str(self.router_id),
            switch_prefix=str(self.prefix),
            switch_prefix_sep=str(self.prefix).replace('/', ' /'),
            switch_prefix_addr=str(self.prefix.network_address) if self.prefix else "",
            switch_prefix_netmask=str(self.prefix.netmask) if self.prefix else "",
            switch_bits=str(self.prefix.prefixlen) if self.prefix else "",
            base_prefix=str(self.cluster_prefix),
            base_prefix_sep=str(self.cluster_prefix).replace('/', ' /'),
            base_prefix_addr=str(self.cluster_prefix.network_address),
            base_bits=str(self.cluster_prefix.prefixlen),
            switch_vlan=self.vlan,
            vip_prefix=str(self.vip_prefix),
            vip_prefix_sep=str(self.vip_prefix).replace('/', ' /'),
            vip_prefix_addr=str(self.vip_prefix.network_address),
            vip_bits=str(self.vip_prefix.prefixlen),
            vrf_name=self._vrf,
            mtu=str(self.mtu),
            mmu_headroom_pool_limit_cells=mmu_headroom_pool_limit_cells,
        )
        if self.system_address:
            output["system_vlan"] = L2SystemVlan(self.switch_vendor,
                        model_name=self.switch_model).substitute(
                            system_vlan=self.system_vlan,
                            mtu=str(self.mtu),
                            router_id=str(self.router_id),
                            system_switch_address=str(self.system_address),
                            system_switch_address_netmask=str(self.system_address.as_netmask),
                            system_switch_bits=str(self.system_prefix.prefixlen),
                            asn=str(self.asn),
                            asn_num=int(self.asn),
                            system_switch_prefix=str(self.system_prefix),
                            system_switch_prefix_sep=str(self.system_prefix).replace('/', ' /'),
                            system_switch_prefix_addr=str(self.system_prefix.network_address),
                            system_switch_prefix_netmask=str(self.system_prefix.netmask),
                            vrf_name=self._vrf,
                        )
        if self.swarmx_address:
            output["base"] += L2SwarmxVlan(self.switch_vendor,
                        model_name=self.switch_model).substitute(
                            swarmx_vlan=self.swarmx_vlan,
                            mtu=str(self.mtu),
                            router_id=str(self.router_id),
                            swarmx_switch_address=str(self.swarmx_address),
                            swarmx_switch_address_netmask=str(self.swarmx_address.as_netmask),
                            swarmx_switch_bits=str(self.swarmx_prefix.prefixlen),
                            asn=str(self.asn),
                            asn_num=int(self.asn),
                            swarmx_switch_prefix=str(self.swarmx_prefix),
                            swarmx_switch_prefix_sep=str(self.swarmx_prefix).replace('/', ' /'),
                            swarmx_switch_prefix_addr=str(self.swarmx_prefix.network_address),
                            swarmx_switch_prefix_netmask=str(self.swarmx_prefix.netmask),
                            vrf_name=self._vrf,
                        )
        # L3 multi mgmt template
        if self.is_memx or self.is_leaf:
            output["multi_mgmt_bgp"] = L3MultiMgmtBgp(self.switch_vendor).substitute(
                name=str(self.name),
                asn=str(self.asn),
                asn_num=int(self.asn),
                prefix=str(self.prefix),
                prefix_sep=str(self.prefix).replace('/', ' /'),
                vrf_name=self._vrf,
            )

        output["interfaces"] = []

        # L2 Interface templates
        for port in self.ports:
            my_port_num = parse_ethernet_port_number(port["my_port"])
            sections=[
                L2Iface(self.switch_vendor, model_name=self.switch_model).substitute(
                    my_port=port["my_port"],
                    my_port_num=my_port_num,
                    mtu=str(port["mtu"]),
                    neighbor=port["neighbor"],
                    switch_vlan=port["switch_vlan"],
                    ecn_min_threshold=port["ecn_min_threshold"],
                    ecn_max_threshold=port["ecn_max_threshold"],
                    ecn_probability=port["ecn_probability"],
                    vrf_name=self._vrf,
                )
            ]
            # add mmu profile for leaf switches
            if self.is_leaf and (self.switch_vendor, self.switch_model) in SWITCHES_WITH_MMU_SUPPORT:
                port_speed = self.switch_ports.get(port['my_port'], {}).get('speed')
                if port_speed:
                    mmu_profile = LF_SPEED_MMU_PROFILE.get(port_speed, "mmu-profile-100g")
                    sections.append(
                        MmuProfileIface(self.switch_model).substitute(
                        my_port=port["my_port"],
                        mmu_profile=mmu_profile)
                    )
            output["interfaces"].append(dict(
                name=port["my_port"],
                node_name=port.get("node_name", ""),
                sections=sections
            ))

        for port in self.system_ports:
            my_port_num = parse_ethernet_port_number(port["my_port"])
            sections=[
                L2SystemIface(self.switch_vendor, model_name=self.switch_model).substitute(
                    my_port=port["my_port"],
                    my_port_num=my_port_num,
                    mtu=str(port["mtu"]),
                    neighbor=port["neighbor"],
                    switch_vlan=port["switch_vlan"],
                    ecn_min_threshold=port["ecn_min_threshold"],
                    ecn_max_threshold=port["ecn_max_threshold"],
                    ecn_probability=port["ecn_probability"],
                )
            ]
            # add mmu profile for leaf switches
            if self.is_leaf and (self.switch_vendor, self.switch_model) in SWITCHES_WITH_MMU_SUPPORT:
                port_speed = self.switch_ports.get(port['my_port'], {}).get('speed')
                if port_speed:
                    mmu_profile = LF_SPEED_MMU_PROFILE.get(port_speed, "mmu-profile-100g")
                    sections.append(
                        MmuProfileIface(self.switch_model).substitute(
                        my_port=port["my_port"],
                        mmu_profile=mmu_profile)
                    )
            output["interfaces"].append(dict(
                name=port["my_port"],
                system_name=port.get("system_name"),
                sections=sections
            ))

        # L3 Peer templates
        for link in self.links:
            if self.switch_vendor == "arista":
                my_port_num = ""
            else:
                my_port_num = parse_ethernet_port_number(link["my_port"])
            sections = [
                L3PeerIface(self.switch_vendor, model_name=self.switch_model).substitute(
                    name=str(self.name),
                    neighbor=str(link["neighbor"]),
                    neighbor_port=link["neighbor_port"],
                    my_port=str(link["my_port"]),
                    my_port_num=my_port_num,
                    my_address=str(link["my_address"]),
                    my_address_netmask=str(link["my_address"].as_netmask),
                    mtu=str(link["mtu"]),
                    ecn_min_threshold=link["ecn_min_threshold"],
                    ecn_max_threshold=link["ecn_max_threshold"],
                    ecn_probability=link["ecn_probability"],
                    vrf_name=self._vrf,
                )
            ]

            if link.get("neighbor_asn", ""):
                sections.append(
                    L3PeerBgp(self.switch_vendor, model_name=self.switch_model).substitute(
                        name=str(self.name),
                        neighbor=str(link["neighbor"]),
                        asn=str(self.asn),
                        asn_num=int(self.asn),
                        neighbor_asn=str(link["neighbor_asn"]),
                        neighbor_asn_num=int(link["neighbor_asn"]),
                        neighbor_address=str(link["neighbor_address"]),
                        peer_group=str(link["peer_group"]),
                        vrf_name=self._vrf,
                    )
                )
            # add mmu profile for leaf switches
            if self.is_leaf and (self.switch_vendor, self.switch_model) in SWITCHES_WITH_MMU_SUPPORT:
                port_speed = self.switch_ports.get(link['my_port'], {}).get('speed')
                if port_speed:
                    neighbor_vendor, neighbor_model = self._switch_vendor_model_map.get(link['neighbor'], ("", "",))
                    mmu_profile = LF_SP_SPEED_MMU_PROFILE_OVERRIDES.get((
                        self.switch_vendor, self.switch_model,
                        neighbor_vendor, neighbor_model,
                        port_speed,
                    ), None)
                    if not mmu_profile:
                        mmu_profile = LF_SPEED_MMU_PROFILE.get(port_speed, "mmu-profile-100g")
                    sections.append(
                        MmuProfileIface(self.switch_model).substitute(
                            my_port=link["my_port"],
                            mmu_profile=mmu_profile,
                        )
                    )
            output["interfaces"].append(dict(
                name=str(link["my_port"]),
                switch_name=str(link["neighbor"]),
                sections=sections
            ))

        output["uplinks"] = []
        for link in self.uplinks:
            my_port_num = parse_ethernet_port_number(link["my_port"])
            sections = [
                L3UplinkIface(self.switch_vendor).substitute(
                    name=str(self.name),
                    neighbor=str(link["neighbor"]),
                    my_port=str(link["my_port"]),
                    my_port_num=my_port_num,
                    my_address=str(link["my_address"]),
                    my_address_netmask=str(link["my_address"].as_netmask),
                    mtu=str(link["mtu"]),
                    fw_address=str(link["my_address"].ip),
                    mgmt_node_data_address=str(link["mgmt_node_data_address"]),
                    vrf_name=self._vrf,
                )
            ]

            if link.get("neighbor_asn", ""):
                sections.append(
                    L3PeerBgp(self.switch_vendor).substitute(
                        name=str(self.name),
                        neighbor=str(link["neighbor"]),
                        asn=str(self.asn),
                        asn_num=int(self.asn),
                        neighbor_asn=str(link["neighbor_asn"]),
                        neighbor_asn_num=int(link["neighbor_asn"]),
                        neighbor_address=str(link["neighbor_address"]),
                        peer_group=str(link["peer_group"]),
                        vrf_name=self._vrf,
                    )
                )
            output["uplinks"].append(dict(
                name=str(link["my_port"]),
                sections=sections
            ))

        output["system_routes"] = []
        if self.is_leaf:
            for vip_route in self.vip_routes:
                output["system_routes"].append(dict(
                    name=str(vip_route["neighbor_address"]),
                    system_name=vip_route["neighbor"],
                    sections=[
                        L3PeerBgp(self.switch_vendor).substitute(
                            name=str(self.name),
                            neighbor=str(vip_route["neighbor"]),
                            asn=str(self.asn),
                            asn_num=int(self.asn),
                            neighbor_asn=str(vip_route["neighbor_asn"]),
                            neighbor_asn_num=int(vip_route["neighbor_asn"]),
                            neighbor_address=str(vip_route["neighbor_address"]),
                            peer_group=str(vip_route["peer_group"]),
                            vrf_name=self._vrf,
                        )
                    ]
                ))
        else:
            for vip_route in self.clear_vip_routes:
                output["system_routes"].append(dict(
                    name=str(vip_route["dest"]),
                    system_name=vip_route["system_name"],
                    sections=[
                        L3ClearStaticRoute(self.switch_vendor,model_name=self.switch_model).substitute(
                            dest=str(vip_route["dest"]),
                            dest_sep=(" ".join(str(vip_route["dest"]).split('/'))),
                            vrf_name=self._vrf,
                        )
                    ]
                ))
            for vip_route in self.vip_routes:
                output["system_routes"].append(dict(
                    name=str(vip_route["dest"]),
                    system_name=vip_route["system_name"],
                    sections=[
                        L3StaticRoute(self.switch_vendor,model_name=self.switch_model).substitute(
                            dest=str(vip_route["dest"]),
                            dest_sep=(" ".join(str(vip_route["dest"]).split('/'))),
                            via=str(vip_route["via"]),
                            vrf_name=self._vrf,
                        )
                    ]
                ))

        # NTP template
        if self.root_server:
            output["ntp"] = Ntp(self.switch_vendor).substitute(
                    ntp_server=self.root_server["ip_address"]
                )

        # SNMP template
        output["snmp"] = Snmp(self.switch_vendor).substitute()

        if self.switch_vendor == "arista":
            output["final_cmd"] = "!\nend\n"
        elif self.switch_vendor == "edgecore":
            output["final_cmd"] = "\n"
        elif self.switch_vendor == "juniper":
            output["final_cmd"] = "#\ncommit\nexit\n"
        else:
            output["final_cmd"] = "#\nexit\n"

        return output


class SwitchConfigCommandWriter(SwitchConfigWriter):

    config_file_name = "l3_config.raw"

    def output_config(self):
        json_writer = SwitchConfigJSONWriter(self._network_config, self._switch_name)
        cfg = json_writer.generate_doc()

        output = []

        for section in ["base", "system_vlan", "multi_mgmt_bgp"]:
            output.extend([l.rstrip() for l in cfg.get(section, "").split('\n')])

        for section in ["interfaces", "uplinks", "system_routes"]:
            for i in cfg.get(section, []):
                for s in i["sections"]:
                    output.extend([l.rstrip() for l in s.split('\n')])

        for section in ["ntp", "snmp"]:
            output.extend([l.rstrip() for l in cfg.get(section, "").split('\n')])

        output.append(cfg["final_cmd"])

        return "\n".join(output)


class AllSwitchesConfigWriter(_ConfigAggregateBase):
    """ Write configuration files for all switches
    """
    def __init__(self, network_config):
        self._config_instances = [
            SwitchConfigJSONWriter(network_config, switch_obj["name"])
            for switch_obj in network_config.get("switches", list())
        ]
        self._config_instances += [
            SwitchConfigCommandWriter(network_config, switch_obj["name"])
            for switch_obj in network_config.get("switches", list())
        ]

    def config_instances(self):
        return self._config_instances


class SwitchEnableSystemBgpConfigWriter(_ConfigFileWriterBase):

    config_file_name = "enable_bgp.cfg"

    config_class_name = "switches"
    config_file_mode = 0o644

    @property
    def config_item_name(self):
        return self.name

    def __init__(self, network_config, switch_name, system_names=None):
        switches = dict(items_name_iter(network_config["switches"]))

        switch_obj = switches[switch_name]

        if "asn" not in switch_obj:
            raise ValueError(f"No ASN found for {switch_name}")

        self.name = switch_name
        self.switch_vendor = switch_obj["vendor"]
        self.switch_model = switch_obj.get("model")
        self.asn = ASN4(switch_obj["asn"])

        self.clear_vip_routes = list()
        self.vip_routes = list()
        cleared_routes = list()

        if system_names:
            systems = [s for s in network_config.get('systems', list()) if s['name'] in system_names]
        else:
            systems = network_config.get('systems', list())

        for system_obj in systems:
            vip_dest = system_obj.get("vip")
            if not vip_dest:
                continue

            for iface_obj in system_obj.get("interfaces", list()):
                if not iface_obj["name"].startswith("control"):
                    continue
                if iface_obj.get("switch_name") == switch_name:
                    dest = iface_obj.get("address")
                    if not dest:
                        continue
                    self.vip_routes.append(dict(
                        neighbor=system_obj["name"],
                        neighbor_asn=ASN4("65010.16"),
                        neighbor_address=dest.split('/')[0],
                        peer_group="ws_cluster_csx"
                    ))
                    if dest not in cleared_routes:
                        self.clear_vip_routes.append(dict(
                            dest=JSIPNetwork(vip_dest),
                            system_name=system_obj["name"]
                        ))
                        cleared_routes.append(vip_dest)

    def output_config(self):
        output = str()

        output += L3SystemBgpBase(self.switch_vendor, model_name=self.switch_model).substitute(
                        asn=str(self.asn),
                        asn_num=int(self.asn)
                    )
        for vip_route in self.clear_vip_routes:
            output += L3ClearStaticRoute(self.switch_vendor, model_name=self.switch_model).substitute(
                            dest=str(vip_route['dest']),
                            dest_sep=(" ".join(str(vip_route["dest"]).split('/'))),
                        )
            output += '\n'
        for vip_route in self.vip_routes:
            output += L3PeerBgp(self.switch_vendor, model_name=self.switch_model).substitute(
                        name=str(self.name),
                        neighbor=str(vip_route["neighbor"]),
                        asn=str(self.asn),
                        asn_num=int(self.asn),
                        neighbor_asn=str(vip_route["neighbor_asn"]),
                        neighbor_asn_num=int(vip_route["neighbor_asn"]),
                        neighbor_address=str(vip_route["neighbor_address"]),
                        peer_group=str(vip_route["peer_group"])
                    )
            output += '\n'

        if self.switch_vendor == "arista":
            output += "!\nend\n"
        else:
            output += "#\nexit\n"

        return output


class EnableSystemBgpConfigWriter(_ConfigAggregateBase):
    """ Enable BGP for system VIPs on all switches
    """
    def __init__(self, network_config, system_names=None):
        self._config_instances = [
            SwitchEnableSystemBgpConfigWriter(network_config, switch_obj["name"], system_names)
            for switch_obj in network_config.get("switches", list())
        ]

    def config_instances(self):
        return self._config_instances


class SwitchDisableSystemBgpConfigWriter(_ConfigFileWriterBase):

    config_file_name = "disable_bgp.cfg"

    config_class_name = "switches"
    config_file_mode = 0o644

    @property
    def config_item_name(self):
        return self.name

    def __init__(self, network_config, switch_name, system_names=None):
        switches = dict(items_name_iter(network_config["switches"]))

        switch_obj = switches[switch_name]

        self.name = switch_name
        self.switch_vendor = switch_obj["vendor"]
        self.switch_model = switch_obj.get("model")

        if system_names:
            systems = [s for s in network_config.get('systems', list()) if s['name'] in system_names]
        else:
            systems = network_config.get('systems', list())

        self.vip_routes = list()
        for system_obj in systems:
            dest = system_obj.get("vip", "")
            if not dest:
                continue

            for iface_obj in system_obj.get("interfaces", list()):
                if not iface_obj["name"].startswith("control"):
                    continue
                via = iface_obj.get("address", "")
                if via and (iface_obj.get("switch_name") == switch_name):
                    self.vip_routes.append(
                        dict(dest=JSIPNetwork(dest),
                             via=JSIPInterface(via).ip,
                             system_name=system_obj["name"]))

    def output_config(self):
        output = str()

        for vip_route in self.vip_routes:
            output += L3StaticRoute(self.switch_vendor, model_name=self.switch_model).substitute(
                        dest=str(vip_route["dest"]),
                        dest_sep=(" ".join(str(vip_route["dest"]).split('/'))),
                        via=str(vip_route["via"])
                    )
            output += '\n'

        if self.switch_vendor == "arista":
            output += "!\nend\n"
        else:
            output += "#\nexit\n"

        return output


class DisableSystemBgpConfigWriter(_ConfigAggregateBase):
    """ Disable BGP for system VIPs on all switches
    """
    def __init__(self, network_config, system_names=None):
        self._config_instances = [
            SwitchDisableSystemBgpConfigWriter(network_config, switch_obj["name"], system_names)
            for switch_obj in network_config.get("switches", list())
        ]

    def config_instances(self):
        return self._config_instances


class SwitchDcqcnConfigWriter(_ConfigFileWriterBase):

    config_file_name = "dcqcn.cfg"

    config_class_name = "switches"
    config_file_mode = 0o644

    @property
    def config_item_name(self):
        return self.name

    def __init__(self, network_config, switch_name,
                enable=True, ecn_min_threshold=None, ecn_max_threshold=None,
                ecn_probability=None, ecn_delay=None, interface_type=None):
        switches = dict(items_name_iter(network_config["switches"]))

        switch_obj = switches[switch_name]

        self.name = switch_name
        self.switch_vendor = switch_obj["vendor"]
        self.switch_model = switch_obj.get("model")
        self.is_leaf = (switch_obj.get("tier", "AW") == "LF")
        self.enable = enable
        self.nodes = network_config.get("management_nodes", []) + \
                    network_config.get("memoryx_nodes", []) + \
                    network_config.get("swarmx_nodes", []) + \
                    network_config.get("worker_nodes", []) + \
                    network_config.get("user_nodes", []) + \
                    network_config.get("activation_nodes", [])
        self.interface_speed = None
        if interface_type == "100g":
            self.interface_speed = 100*GBPS
        elif interface_type == "400g":
            self.interface_speed = 400*GBPS

        if self.switch_vendor != "arista":
            raise ValueError(
                "DCQCN support is only available for Arista switches"
            )

        self.switch_ports = dict()
        for p in switch_obj.get("ports", []):
            self.switch_ports[p['name']] = p

        def is_th5():
            return self.switch_model in ["7060X6"]

        default_ecn_thresholds = {
            100*GBPS: {
                "min": 676,
                "max": 6084
            },
            400*GBPS: {
                "min": 676,
                "max": 6084
            },
            800*GBPS: {
                "min": 676,
                "max": 6084
            }
        }
        if self.is_leaf:
            if is_th5():
                default_ecn_thresholds[100*GBPS]["min"] = 328
                default_ecn_thresholds[100*GBPS]["max"] = 2952
                default_ecn_thresholds[400*GBPS]["min"] = 1312
                default_ecn_thresholds[400*GBPS]["max"] = 11808
                default_ecn_thresholds[800*GBPS]["min"] = 1312
                default_ecn_thresholds[800*GBPS]["max"] = 11808
            else:
                default_ecn_thresholds[100*GBPS]["min"] = 222
                default_ecn_thresholds[100*GBPS]["max"] = 2000
                default_ecn_thresholds[400*GBPS]["min"] = 890
                default_ecn_thresholds[400*GBPS]["max"] = 8000
                default_ecn_thresholds[800*GBPS]["min"] = 890
                default_ecn_thresholds[800*GBPS]["max"] = 8000
        default_ecn_probability = 1

        def get_ecn_thresholds(port_speed):
            if port_speed not in default_ecn_thresholds:
                port_speed = 100*GBPS
            min_t = ecn_min_threshold or default_ecn_thresholds[port_speed]["min"]
            max_t = ecn_max_threshold or default_ecn_thresholds[port_speed]["max"]
            if min_t > max_t:
                raise ValueError(
                    "ecn_min_threshold must be lower than ecn_max_threshold"
                )
            return min_t, max_t

        if ecn_probability is None:
            ecn_probability = default_ecn_probability
        if ecn_delay is None:
            ecn_delay = 3

        self.links = list()
        self.ports = list()
        self.system_ports = list()

        for connection in network_config.get(
                                "xconnect", {}).get(
                                "connections", []):
            my_idx = None
            if connection["links"][0]["name"] == switch_name:
                my_idx = 0
            elif connection["links"][1]["name"] == switch_name:
                my_idx = 1
            if my_idx is None:
                continue
            my_port = connection['links'][my_idx]['port']
            port_speed = self.switch_ports.get(my_port, {}).get('speed')
            if self.interface_speed and port_speed != self.interface_speed:
                continue
            min_threshold, max_threshold = get_ecn_thresholds(port_speed)
            link = dict(
                my_port=my_port,
                ecn_min_threshold=min_threshold,
                ecn_max_threshold=max_threshold,
                ecn_probability=ecn_probability,
                ecn_delay=ecn_delay
            )
            self.links.append(link)

        for node in self.nodes:
            for i in node.get("interfaces", []):
                if i["switch_name"] != self.name:
                    continue
                my_port = i["switch_port"]
                port_speed = self.switch_ports.get(my_port, {}).get('speed')
                if self.interface_speed and port_speed != self.interface_speed:
                    continue
                min_threshold, max_threshold = get_ecn_thresholds(port_speed)
                self.ports.append(
                    dict(
                        my_port=my_port,
                        ecn_min_threshold=min_threshold,
                        ecn_max_threshold=max_threshold,
                        ecn_probability=ecn_probability,
                        ecn_delay=ecn_delay
                    )
                )
        for sc in network_config.get("system_connections", []):
            if sc["switch_name"] != self.name:
                continue
            my_port = sc["switch_port"]
            port_speed = self.switch_ports.get(my_port, {}).get('speed')
            if self.interface_speed and port_speed != self.interface_speed:
                continue
            min_threshold, max_threshold = get_ecn_thresholds(port_speed)
            self.system_ports.append(
                dict(
                    my_port=sc["switch_port"],
                    ecn_min_threshold=min_threshold,
                    ecn_max_threshold=max_threshold,
                    ecn_probability=ecn_probability,
                    ecn_delay=ecn_delay
                )
            )

    def output_config(self):
        output = str()

        if self.enable:
            output += DcqcnEnableBase(model_name=self.switch_model).substitute()
            output += '\n'
            for port in self.ports + self.system_ports + self.links:
                output += DcqcnEnableIface(model_name=self.switch_model).substitute(
                            my_port=port["my_port"],
                            ecn_min_threshold=f'{port["ecn_min_threshold"]} kbytes',
                            ecn_max_threshold=f'{port["ecn_max_threshold"]} kbytes',
                            ecn_probability=port["ecn_probability"],
                            ecn_delay=port["ecn_delay"]
                        )
                output += '\n'
        else:
            if self.interface_speed is None:
                output += DcqcnDisableBase(model_name=self.switch_model).substitute()
                output += '\n'
            for port in self.ports + self.system_ports + self.links:
                output += DcqcnDisableIface(model_name=self.switch_model).substitute(
                            my_port=port["my_port"],
                        )
                output += '\n'

        if self.switch_vendor == "arista":
            output += "!\nend\n"
        else:
            output += "#\nexit\n"

        return output


class DcqcnConfigWriter(_ConfigAggregateBase):
    """ Enable/disable DCQCN on all switches
    """
    def __init__(self, network_config, enable=True, switch_category="LF",
            ecn_min_threshold=None, ecn_max_threshold=None,
            ecn_probability=None, ecn_delay=None, interface_type=None):
        self._config_instances = [
            SwitchDcqcnConfigWriter(network_config, s["name"], enable,
            ecn_min_threshold, ecn_max_threshold, ecn_probability, ecn_delay, interface_type)
            for s in network_config.get("switches", list()) if s["tier"] == switch_category
        ]

    def config_instances(self):
        return self._config_instances

