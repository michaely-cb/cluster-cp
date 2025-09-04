#!/usr/bin/env python

"""
Adds a network resource mapper which lists out network
resources (leaf memx_switches, leaf swarmx switches) used by a wsjob.
Additionally, it also supports visualization of the full network of a cluster.
Prerequisites:
    - Certain cerebras utils need to be present in the python path.
    - Easiest way is add <monolith>/py_root to PYTHONPATH, eg: export PYTHONPATH=~/ws/monolith/py_root
    - Example Commands:
        - python3 -m network_config.tool network_resource_mapper -c <network_config.json> --cluster-details <cluster_details.json>
        - python3 -m network_config.tool network_resource_mapper -c <network_config.json> --cluster-details <cluster_details.json> --viz-network

"""

from dataclasses import dataclass
from .base import SubCommandBase, UsesNetworkConfig


@dataclass(frozen=True)
class SwitchPort:
    switch: str
    port: str

    def __repr__(self):
        return f"SwitchPort('{self.switch}', '{self.port}')"

    def __str__(self):
        return f"{self.switch}_{self.port}"


@dataclass(frozen=True)
class HostPort:
    host: str
    port: str

    def __repr__(self):
        return f"HostPort('{self.host}', '{self.port}')"

    def __str__(self):
        return f"{self.host}_{self.port}"


@dataclass(frozen=True)
class SystemPort:
    system: str
    port: str

    def __repr__(self):
        return f"SystemPort('{self.system}', '{self.port}')"

    def __str__(self):
        return f"{self.system}_{self.port}"


@dataclass(frozen=True)
class Link:
    port1: object
    port2: object

    def __repr__(self):
        return f"Link({repr(self.port1)}, {repr(self.port2)})"

    def __str__(self):
        return f"{str(self.port1)} <-> {str(self.port2)}"


class NetworkDetails:
    def __init__(self, network_config: str):
        self.network_config = network_config
        self.memoryx_ports = set()
        self.switch_ports = set()
        self.swarmx_ports = set()
        self.system_ports = set()
        self.connections = set()

        for node_obj in network_config.get("memoryx_nodes", []) + \
                        network_config.get("activation_nodes", []):
            for ifc in node_obj["interfaces"]:
                host_port = HostPort(node_obj["name"], ifc["name"])
                switch_port = SwitchPort(ifc["switch_name"], ifc["switch_port"])
                self.memoryx_ports.add(host_port)
                self.switch_ports.add(switch_port)
                self.connections.add(Link(host_port, switch_port))

        for node_obj in network_config.get("swarmx_nodes", []):
            for ifc in node_obj["interfaces"]:
                host_port = HostPort(node_obj["name"], ifc["name"])
                switch_port = SwitchPort(ifc["switch_name"], ifc["switch_port"])
                self.swarmx_ports.add(host_port)
                self.switch_ports.add(switch_port)
                self.connections.add(Link(host_port, switch_port))

        for node_obj in network_config.get("system_connections", []):
            switch_port = SwitchPort(
                node_obj["switch_name"], node_obj["switch_port"]
            )
            self.switch_ports.add(switch_port)
            system_port = node_obj["system_port"].replace(":", ".")
            system_port = SystemPort(node_obj["system_name"], system_port)
            self.system_ports.add(system_port)
            self.connections.add(Link(switch_port, system_port))

        for node_obj in network_config.get("xconnect", {}).get(
            "connections", []
        ):
            switch_port_left = SwitchPort(
                node_obj["links"][0]["name"], node_obj["links"][0]["port"]
            )
            switch_port_right = SwitchPort(
                node_obj["links"][1]["name"], node_obj["links"][1]["port"]
            )
            self.switch_ports.add(switch_port_left)
            self.switch_ports.add(switch_port_right)
            self.connections.add(Link(switch_port_left, switch_port_right))

    def plot(self, filename="network.dot"):
        try:
            from graphviz import Digraph
        except ImportError as e:
            raise ImportError("graphviz package couldn't be imported. "
                              "To use network_resource_mapper: "
                              "Please ensure that this is being run inside the monolith environment or within a cbcore") from e


        switches = [switch_port.switch for switch_port in self.switch_ports]
        hosts = [
            host_port.host
            for host_port in self.memoryx_ports.union(self.swarmx_ports)
        ]
        systems = [system_port.system for system_port in self.system_ports]

        dot = Digraph(comment="Multibox Network Diagram")
        dot.attr(rankdir="LR")
        dot.attr(nodesep="1", ranksep="1")
        dot.attr("edge", dir="both")
        dot.attr(splines="ortho")

        for switch in switches:
            with dot.subgraph(name=f"cluster_{switch}") as switch_subgraph:
                switch_subgraph.attr(color="blue")
                switch_subgraph.attr(label=switch)
                for switch_port in self.switch_ports:
                    if switch_port.switch == switch:
                        switch_subgraph.node(
                            str(switch_port),
                            label=switch_port.port,
                            shape="box",
                        )

        for host in hosts:
            with dot.subgraph(name=f"cluster_{host}") as host_subgraph:
                host_subgraph.attr(color="yellow")
                host_subgraph.attr(label=host)
                for host_port in self.memoryx_ports.union(self.swarmx_ports):
                    if host_port.host == host:
                        host_subgraph.node(
                            str(host_port), label=host_port.port, shape="box"
                        )

        for system in systems:
            with dot.subgraph(name=f"cluster_{system}") as system_subgraph:
                system_subgraph.attr(color="green")
                system_subgraph.attr(label=system)
                for system_port in self.system_ports:
                    if system_port.system == system:
                        system_subgraph.node(
                            str(system_port),
                            label=system_port.port,
                            shape="box",
                        )

        for connection in self.connections:
            dot.edge(str(connection.port1), str(connection.port2))

        print(f"Saving dot file: {filename}")
        dot.save(filename)


class RunMapper:
    def __init__(self, network_details):
        self.network_details = network_details
        self.systems = set()
        self.memoryx_nodes = set()
        self.swarmx_nodes = set()
        self.run_memoryx_ports = set()
        self.run_swarmx_ports = set()
        self.run_system_ports = set()
        self.run_connections = set()

    def _setup_resources(self, cluster_details_json):
        try:
            from cerebras.common.protobuf import proto_msg_from_jsonfile
            from cerebras.pb.workflow.appliance.common.common_config_pb2 import (
                ClusterDetails,
            )
        except ImportError as e:
            raise ImportError("Cerebras package couldn't be imported. "
                              "To use network_resource_mapper: "
                              "Please ensure that this is being run inside the monolith environment or within a cbcore") from e



        cluster_details = proto_msg_from_jsonfile(
            ClusterDetails, cluster_details_json
        )

        for task in cluster_details.tasks:
            for address_details in task.task_map:
                if task.task_type == ClusterDetails.TaskInfo.TaskType.WSE:
                    system = address_details.address_book.task_node_name
                    self.systems.add(system)
                elif (
                    task.task_type == ClusterDetails.TaskInfo.TaskType.WGT
                    or task.task_type == ClusterDetails.TaskInfo.TaskType.ACT
                ):
                    self.memoryx_nodes.add(
                        address_details.address_book.task_node_name
                    )
                elif task.task_type == ClusterDetails.TaskInfo.TaskType.BR:
                    self.swarmx_nodes.add(
                        address_details.address_book.task_node_name
                    )

        self.run_memoryx_ports = set(
            [
                port
                for port in self.network_details.memoryx_ports
                if port.host in self.memoryx_nodes
            ]
        )
        self.run_swarmx_ports = set(
            [
                port
                for port in self.network_details.swarmx_ports
                if port.host in self.swarmx_nodes
            ]
        )
        self.run_system_ports = set(
            [
                port
                for port in self.network_details.system_ports
                if port.system in self.systems
            ]
        )
        self.run_connections = set(
            connection
            for connection in self.network_details.connections
            if connection.port1 in self.run_memoryx_ports
        )
        self.run_connections = self.run_connections.union(
            set(
                connection
                for connection in self.network_details.connections
                if connection.port1 in self.run_swarmx_ports
            )
        )
        self.system_connections = set(
            connection
            for connection in self.network_details.connections
            if connection.port2 in self.run_system_ports
        )
        self.memx_switches = set(
            run_connection.port2.switch
            for run_connection in self.run_connections
            if run_connection.port1 in self.run_memoryx_ports
        )
        self.swarmx_switches = set(
            run_connection.port2.switch
            for run_connection in self.run_connections
            if run_connection.port1 in self.run_swarmx_ports
        )

    def run(self, cluster_details_json):
        self._setup_resources(cluster_details_json)
        print("--- Network Resources ---")
        print("\nLeaf MemoryX Switches:")
        for memx_switch in sorted(self.memx_switches):
            print(memx_switch)
        print("\nLeaf SwarmX Switches:")
        for swarmx_switch in sorted(self.swarmx_switches):
            print(swarmx_switch)
        print("\nMemoryX Nodes:")
        for memx_host in sorted(self.memoryx_nodes):
            print(memx_host)
        print("\nSwarmX Nodes:")
        for swarmx_host in sorted(self.swarmx_nodes):
            print(swarmx_host)
        print("\nSystems:")
        for system in sorted(self.systems):
            print(system)


@UsesNetworkConfig(save=False)
class NetworkResourceMapper(SubCommandBase):
    """Obtain the network resources for a given wsjob"""

    sub_command = "network_resource_mapper"

    @staticmethod
    def build_parser(parser):
        parser.add_argument(
            "--cluster-details",
            required=True,
            help="Path to the cluster details for the job whose network resources are to be mapped",
        )
        parser.add_argument(
            "--viz-network",
            action="store_true",
            help="Outputs a dot file for the entire network of the cluster",
        )

    @staticmethod
    def map(args):
        network_details = NetworkDetails(args.network_config)
        if args.viz_network:
            print(f"NOTE: For too big a network, dot may not be able to convert it to svg")
            network_details.plot()
        run_mapper = RunMapper(network_details)
        if args.cluster_details:
            run_mapper.run(args.cluster_details)

    def __call__(self, args):
        self.map(args)
