import argparse
import json
import pathlib
from collections import defaultdict
from typing import List, Optional

from deployment_manager.network_config.cli import BuildTierAllocations, RunPlacer, RemoveSwitch, RemoveNode, \
    RemoveSystem
from deployment_manager.network_config.common import NUM_EXPECTED_CS_PORTS, SystemModels
from deployment_manager.network_config.configs import AllConfigsWriter, AllSwitchesConfigWriter
from deployment_manager.network_config.schema import NetworkConfigSchema

__switch = {"username": "admin", "tier_pos": 0}

# Note: the interface names will be eos style (EthernetX/Y) instead of junos/ec/dell
# style (et-0/0/1:0 or EthernetX), but that doesn't matter for simply executing the templater
ARISTA_SWITCH_BASE = {"vendor": "arista", "model": "7060DX5", **__switch, }
EC_SWITCH_BASE = {"vendor": "edgecore", "model": "", **__switch, }
DELL_SWITCH_BASE = {"vendor": "dell", "model": "", **__switch, }
HP_SWITCH_BASE = {"vendor": "hpe", "model": "12908", **__switch, }
JUNIPER_SWITCH_BASE = {"vendor": "juniper", "model": "qfx5240-64od", **__switch}

_node_base = {
    "nodegroup": 0,
}


class LeafSpineNetworkBuilder:
    """
    Builds a leaf-spine network for placement testing. Adds nodes/switches/systems and their switch connections as
    well as spine connections for leaf switches.

    Doesn't attempt to place racks or ports realistically but this shouldn't matter for placement. It will always
    use unique ports on switches so no connection to a switch should appear in the same interface twice.
    """

    def __init__(self,
                 spine_count: int,
                 xconnect_per_spine=1,
                 sx_interface_count=6,
                 switch_base: Optional[dict] = None,
    ):
        self._rack_counter = 0
        self._rack_counter = 1
        self._port_counter = {}
        self._rack_unit_counter = {}
        self._system_mac_counter = int("7055f8000000", 16)
        self._switch_base = switch_base if switch_base else ARISTA_SWITCH_BASE
        self.network = {
            "name": "test",
            "clusters": [
                {
                    "name": "test",
                    "mgmt_network": {
                        "vip": "172.254.254.0",
                        "router_asn": "4259906900",
                        "node_asn": "4259906901",
                    },
                    "data_network": {
                        "virtual_addr_index": 0,
                    },
                }
            ],
            "switches": [],
            "exterior_connections": [],
            "exterior_switches": [],
            "tiers": [],
            "xconnect": {"connections": []},
            "systems": [],
            "system_connections": [],
            "environment": {
                "cluster_prefix": "10.250.0.0/16",
                "cluster_asns": [{"count": 2048, "starting_asn": "4259906944"}],
                "overlay_prefix_mask_size": 25,
                "overlay_prefixes": ["100.64.0.0/15"],
                "service_prefix": "100.66.0.0/15",
            }
        }
        for obj_type in NetworkConfigSchema.node_sections:
            self.network[obj_type] = []
        self._xconnect_per_spine = xconnect_per_spine
        self._sx_interface_count = sx_interface_count
        self.add_spines(spine_count)

    def _next_rack(self, prefix="net") -> str:
        rv = int(self._rack_counter)
        self._rack_counter += 1
        return f"{prefix}{rv:03}"

    def _next_rack_unit(self, rack: str) -> int:
        rv = self._rack_unit_counter.get(rack, 0)
        self._rack_unit_counter[rack] = rv + 1
        return rv

    def _next_port(self, switch_name: str) -> int:
        rv = self._port_counter.get(switch_name, 0)
        self._port_counter[switch_name] = rv + 1
        return rv

    def _next_system_mac(self) -> str:
        rv = self._system_mac_counter
        self._system_mac_counter += 1
        rv = hex(rv)[2:]
        return ":".join([rv[i:i + 2] for i in range(0, len(rv), 2)])

    def add_cluster(self):
        assert len(self.network.get("clusters", [])) < 2, "does not support > 2 clusters"
        self.network.get("clusters", []).append({
            "name": "test-green",
            "mgmt_network": {
                "vip": "172.254.254.1",
                "router_asn": "4259906900",
                "node_asn": "4259906901",
            },
            "data_network": {
                "virtual_addr_index": 1,
            },
        })

    @property
    def spines(self) -> List[dict]:
        return [sw for sw in self.network.get("switches", []) if sw["tier"] == "SP"]

    def add_spines(self, count):
        spines_per_rack = 6
        for i in range(count):
            rack = f"net{self._rack_counter + int(i / spines_per_rack):03}"
            self.network["switches"].append({
                "name": f"sw-spine-{rack}-{i}",
                "tier": "SP",
                "rack": rack,
                "rack_unit": self._next_rack_unit(rack),
                **self._switch_base,
            })
        self._rack_counter += int(count / spines_per_rack)

    def add_xconnects(self, leaf_switch: dict):
        conns = self.network["xconnect"]["connections"]
        for sp in self.spines:
            spine_name = sp["name"]
            for i in range(self._xconnect_per_spine):
                conns.append({
                    "name": f"L3 {spine_name} and {leaf_switch['name']}",
                    "links": [{
                        "name": spine_name,
                        "port": f"Ethernet{self._next_port(spine_name)}/1"
                    }, {
                        "name": leaf_switch["name"],
                        "port": f"Ethernet{self._next_port(leaf_switch['name'])}/1",
                    }, ]
                })

    def _add_sx(self, leaf_switch: dict, index: int):
        """ Add a swarmx - 2 400G ports, 4 100G ports """
        rack = leaf_switch['rack']
        node = {
            "name": f"sx-{rack}-{index}",
            "rack": rack,
            "rack_unit": self._next_rack_unit(leaf_switch["rack"]),
            "interfaces": [],
            **_node_base
        }
        sw_name = leaf_switch['name']
        for i in range(2):
            node["interfaces"].append({
                "name": f"eth400g{i}",
                "gbps": 400,
                "switch_name": sw_name,
                "switch_port": f"Ethernet{self._next_port(sw_name)}/1",
            })
        sx_100g_ports = self._sx_interface_count - 2
        if sx_100g_ports > 0:
            sw_port = self._next_port(sw_name)
            for i in range(sx_100g_ports):
                node["interfaces"].append({
                    "name": f"eth100g{i + 2}",
                    "gbps": 100,
                    "switch_name": sw_name,
                    "switch_port": f"Ethernet{sw_port}/{i * 2 + 1}",  # 1, 3, 4, 7
                })
        self.network["swarmx_nodes"].append(node)

    def _add_ax(self, leaf_switch_0: dict, leaf_switch_1: dict, index: int):
        """ Add ax - 2 400G ports connected to 2 potentially different switches """
        rack = leaf_switch_0["rack"]
        node = {
            "name": f"ax-{rack}-{index}",
            "rack": rack,
            "rack_unit": self._next_rack_unit(rack),
            "interfaces": [],
            **_node_base
        }
        for i, sw in enumerate([leaf_switch_0, leaf_switch_1]):
            sw_name = sw["name"]
            node["interfaces"].append({
                "name": f"eth400g{i}",
                "gbps": 400,
                "switch_name": sw_name,
                "switch_port": f"Ethernet{self._next_port(sw_name)}/1",
            })
        self.network["activation_nodes"].append(node)

    def add_nodes(self, leaf: dict, role: str, count: int):
        """ Add a node. Assumes 2 100G interfaces each """
        if count == 0:
            return
        assert f"{role}_nodes" in self.network, "invalid node role"
        name_prefix = f"{role}-{leaf['rack']}-"
        starting_offset = len([n for n in self.network[f"{role}_nodes"] if n["name"].startswith(name_prefix)])
        last_port, last_inf = -1, 7
        for i in range(count):
            node = {
                "name": f"{name_prefix}{starting_offset + i}",
                "rack": leaf["rack"],
                "rack_unit": self._next_rack_unit(leaf["rack"]),
                "interfaces": [],
                **_node_base,
            }
            for j in range(2):
                if last_inf >= 7:
                    port = self._next_port(leaf['name'])
                    inf = 1
                else:
                    port = last_port
                    inf = last_inf + 2
                last_port, last_inf = port, inf
                node["interfaces"].append({"name": f"eth100g{j}",
                                           "switch_name": leaf['name'],
                                           "switch_port": f"Ethernet{port}/{inf}"})
            self.network[f"{role}_nodes"].append(node)

    def _add_system(self, leafs: List[dict], model: str = SystemModels.CS3.value):
        rack = self._next_rack("sys")
        num_ports_per_cs = NUM_EXPECTED_CS_PORTS[model]
        system = {
            "name": f"xs{len(self.network['systems']):05}",
            "model": model,
            "rack": rack,
            "rack_unit": self._next_rack_unit(rack),
            "interfaces": [],
            "ports": [{"mac": self._next_system_mac(), "port_num": i} for i in range(1, num_ports_per_cs+1)],
        }
        ports_per = int(num_ports_per_cs / len(leafs))
        assert num_ports_per_cs % len(leafs) == 0, f"invalid leaf count, must divide {num_ports_per_cs} evenly"
        conns = []
        last_switch, last_port, last_inf = None, -1, -1
        for i in range(num_ports_per_cs):
            sw = leafs[int(i / ports_per)]
            sw_name = sw["name"]
            if last_switch != sw_name or last_inf >= 7:
                port = self._next_port(sw_name)
                inf = 1
            else:
                port = last_port
                inf = last_inf + 2
            sw_port = f"Ethernet{port}/{inf}"

            system["interfaces"].extend([{
                "name": f"control{i}",
                "switch_name": sw_name,
                "switch_port": sw_port,
            }, {
                "name": f"data{i}",
                "switch_name": sw_name,
                "switch_port": sw_port,
            }])
            last_switch, last_port, last_inf = sw_name, port, inf
            conns.append({"switch_name": sw_name, "switch_port": sw_port,
                          "system_name": system['name'], "system_port": f"Port {i + 1}"})
        self.network["systems"].append(system)
        self.network["system_connections"].extend(conns)

    def add_stamp(
            self, system_count: int = 30, sleaf_count: int = 12,
            sx_per_leaf: int = 6, mg_per_rack: int = 0, id_per_leaf=0,
            round_robin_node_links: bool = True, leaf_per_rack=None,
            system_model: str = SystemModels.CS3.value
    ) -> 'LeafSpineNetworkBuilder':
        """
        Add a stamp. Generally stamps will have 30 systems/ax, 12 sleafs, 6 swarmx per sleaf
        """
        if 12 % sleaf_count != 0 or sleaf_count > 12:
            raise ValueError("sleaf count must be 1, 2, 3, 4, 6 or 12")

        if leaf_per_rack is None:
            leaf_per_rack = {1: 1, 2: 2, 3: 3, 4: 2, 6: 3, 12: 2}[sleaf_count]

        # add leafs and sx
        leafs, last_rack, rack_index = [], "", 0
        for i in range(sleaf_count):
            rack = f"net{(int(int(i / leaf_per_rack) + self._rack_counter)):03}"
            leaf = {
                "name": f"sw-sleaf-{rack}-{i}",
                "tier": "LF",
                "rack_unit": self._next_rack_unit(rack),
                "rack": rack,
                **self._switch_base,
            }
            self.add_xconnects(leaf)
            if last_rack != rack:
                last_rack = rack
                rack_index = 0
            for j in range(sx_per_leaf):
                self._add_sx(leaf, rack_index + j)
            leafs.append(leaf)
            rack_index += sx_per_leaf
        self.network["switches"].extend(leafs)
        self._rack_counter += int(sleaf_count / leaf_per_rack)

        # add ax
        if round_robin_node_links:
            # ax were originally cabled to switch (1, 2) then (2, 3) then (3, 1) in a rack of 3 switches
            # we stopped this practice since it wasn't really needed and was hard to cable
            rack_groups = sleaf_count // leaf_per_rack
            ax_per_rack_group = round(system_count / rack_groups)
            if system_count / rack_groups != ax_per_rack_group:
                ax_per_rack_group += 1  # should have same number of ax per rack so round up
            for rack_i in range(rack_groups):
                ax_rack_group_count = ax_per_rack_group
                for ax_i in range(ax_rack_group_count):
                    self._add_ax(
                        leafs[(rack_i * leaf_per_rack) + (ax_i % leaf_per_rack)],
                        leafs[(rack_i * leaf_per_rack) + ((ax_i + 1) % leaf_per_rack)],
                        ax_i,
                    )
        else:
            ax_indices = defaultdict(int)
            ax_per_lf, rem = divmod(system_count, sleaf_count)
            for i, sw in enumerate(leafs):
                ax_count = ax_per_lf if i < len(leafs) - 1 else ax_per_lf + rem
                for j in range(ax_count):
                    ax_indices[sw["rack"]] += 1
                    self._add_ax(sw, sw, ax_indices[sw["rack"]])

        # add id per leaf
        if id_per_leaf > 0:
            for leaf in leafs:
                for j in range(id_per_leaf):
                    # this will add 100g interfaces but that doesn't really matter for placement testing
                    self.add_nodes(leaf, "inferencedriver", id_per_leaf)

        # add systems
        for i in range(system_count):
            self._add_system(leafs, system_model)

        # add mg (possible in a 1 leaf stamp, e.g. the "engineering cluster")
        if mg_per_rack > 0:
            last_rack = -1
            for sleaf in leafs:
                if sleaf['rack'] != last_rack:
                    self.add_nodes(sleaf, "management", mg_per_rack)
                    last_rack = sleaf['rack']
        return self

    def add_mleaf(self, mx: int = 0, wk: int = 0, mg: int = 0, us: int = 0, append=False, **kwargs) -> 'LeafSpineNetworkBuilder':
        if not append:
            rack = self._next_rack()
            leaf = {
                "name": f"sw-mleaf-{rack}",
                "tier": "LF",
                "rack_unit": self._next_rack_unit(rack),
                "rack": rack,
                **self._switch_base,
                **kwargs,
            }
            self.add_xconnects(leaf)
            self.network["switches"].append(leaf)
        else:
            leaf = self.network["switches"][-1]
        self.add_nodes(leaf, "memoryx", mx)
        self.add_nodes(leaf, "worker", wk)
        self.add_nodes(leaf, "management", mg)
        self.add_nodes(leaf, "user", us)
        return self

    def call_allocate_tiers(self, tmp_dir: pathlib.Path, **overrides):
        """ args for the cli command allocate_tiers """
        path = tmp_dir / "network.json"
        path.write_text(json.dumps(self.network))

        # TODO: these values, especially the "count" values should come with overhead baked-in to
        # accommodate expansion
        leaf_count = len(self.network["switches"]) - len(self.spines)
        args = dict(
            cluster_prefix=self.network["environment"]["cluster_prefix"],
            topology="leaf_spine",
            aw_prefix=None,
            aw_size=512,
            aw_count=leaf_count,
            br_prefix=None,
            br_size=512,
            br_count=leaf_count,
            aw_physical=0,
            br_physical=0,
            leaf_count=leaf_count,
            vip_prefix=None,
            vip_count=256,
            xconn_prefix=None,
            xconn_count=leaf_count * len(self.spines) * self._xconnect_per_spine,
            system_prefix=None,
            system_prefix_size=256,
            config=str(path),
        )
        args.update(overrides)
        BuildTierAllocations()(argparse.Namespace(**args))
        self.network = json.loads(path.read_text())

    def call_placer(self, tmp_dir: pathlib.Path):
        """ args for the cli command placer """
        path = tmp_dir / "network.json"
        path.write_text(json.dumps(self.network))

        RunPlacer()(argparse.Namespace(
            no_switches=False,
            no_vips=False,
            no_xconnect=False,
            no_interfaces=False,
            config=str(path),
        ))
        self.network = json.loads(path.read_text())

    def call_generate_cfg(self, tmp_dir: pathlib.Path) -> List[str]:
        """
        Generate switch and node configs
        """
        path = tmp_dir / "network.json"
        path.write_text(json.dumps(self.network))

        writer = AllConfigsWriter(self.network)
        return writer.write_config(str(tmp_dir / "config"))

    def generate_switch_cfg(self, tmp_dir: pathlib.Path):
        """
        Generate switch config templates
        """
        path = tmp_dir / "network.json"
        path.write_text(json.dumps(self.network))

        writer = AllSwitchesConfigWriter(self.network)
        writer.write_config(str(tmp_dir / "config"))

    def call_remove_resources(self, tmp_dir: pathlib.Path,
                              switch_names: List[str] = (),
                              node_names: List[str] = (),
                              system_names: List[str] = ()):
        path = tmp_dir / "network.json"
        path.write_text(json.dumps(self.network))

        if switch_names:
            RemoveSwitch()(argparse.Namespace(name=switch_names, config=str(path)))
        if node_names:
            RemoveNode()(argparse.Namespace(name=node_names, config=str(path)))
        if system_names:
            RemoveSystem()(argparse.Namespace(name=system_names, config=str(path)))
        self.network = json.loads(path.read_text())


def nano_stamp_builder(**kwargs) -> LeafSpineNetworkBuilder:
    """ build a 4 cs 1 mini stamp cluster """
    builder = LeafSpineNetworkBuilder(1, **kwargs)
    builder.add_stamp(system_count=4, sleaf_count=1, sx_per_leaf=9)
    builder.add_mleaf(mx=12, wk=2, mg=4, us=1)
    return builder


def cs4_nano_stamp_builder(**kwargs) -> LeafSpineNetworkBuilder:
    """ build a 4 cs4 1 mini stamp cluster """
    builder = LeafSpineNetworkBuilder(1, **kwargs)
    builder.add_stamp(system_count=4, sleaf_count=1, sx_per_leaf=9, system_model=SystemModels.CS4.value)
    builder.add_mleaf(mx=12, wk=2, mg=4, us=1)
    return builder


def cg3_builder() -> LeafSpineNetworkBuilder:
    # build a cg3-like (68 systems) cluster
    builder = LeafSpineNetworkBuilder(24)

    # stamp 1, 2 and mleafs
    builder.add_stamp().add_stamp()
    builder.add_mleaf(mx=48, wk=17, mg=3).add_mleaf(mx=48, wk=17, mg=3).add_mleaf(mx=24, wk=30, mg=2)
    # engineering_cluster
    builder.add_stamp(4, 1, 6, 2).add_mleaf(mx=48, wk=4, mg=4)
    # infra
    builder.add_mleaf(us=16).add_mleaf(us=16)
    return builder


def cg345_builder() -> LeafSpineNetworkBuilder:
    # build a cg3,4,5-like (64*3 + 4 systems) cluster
    builder = cg3_builder()

    # stamp 3,4,5,6
    builder.add_stamp().add_stamp()
    builder.add_mleaf(mx=48, wk=17, mg=3).add_mleaf(mx=48, wk=17, mg=3).add_mleaf(mx=24, wk=30, mg=2)
    builder.add_stamp().add_stamp()
    builder.add_mleaf(mx=48, wk=17, mg=3).add_mleaf(mx=48, wk=17, mg=3).add_mleaf(mx=24, wk=30, mg=2)

    return builder
