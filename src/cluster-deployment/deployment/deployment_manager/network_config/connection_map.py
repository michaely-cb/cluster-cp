# Copyright 2023, Cerebras Systems, Inc. All rights reserved.

"""
Classes to generate connection map in the absence of LLDP
"""
import json

from abc import ABCMeta, abstractmethod
from operator import itemgetter
from tabulate import tabulate

class ConnectionMap(metaclass=ABCMeta):
    """ Base class for switch connection map
    """
    def __init__(self, obj, network_config):
        self.obj = obj
        self.network_config = network_config
        self.rack = int(self.obj["rack"])

    @abstractmethod
    def get_port_map(self):
        """ Return port layout
        """
        pass

    @abstractmethod
    def get_ports(self):
        """ Return all ports of the switch as a list
        """
        pass

    @abstractmethod
    def get_connection_template(self):
        """ Return per-port connection template
        """
        pass

    def get_port(self, device_physical_name):
        """ Return port to which device_physical_name is connected
        """
        pmap = self.get_port_map()
        for i, row in enumerate(self.get_connection_template()):
            for j, device in enumerate(row):
                if device == device_physical_name:
                    return pmap[i][j]
        return ""

    def get_device(self, port_num):
        """ Return physical name of device connected to port_num
        """
        conn_template = self.get_connection_template()
        for i, row in enumerate(self.get_port_map()):
            for j, port in enumerate(row):
                if port == port_num:
                    return conn_template[i][j]
        return ""


class HPE3700(ConnectionMap):

    def get_port_map(self):
        pmap = [[], []]
        for i in range(32):
            pmap[i%2].append(f"Eth1/{i+1}")
        return pmap

    def get_ports(self):
        ports = []
        for r in self.get_port_map():
            ports += r
        return ports

    def get_connection_template(self):
        return [
            [f"WSE{self.rack:03d}-MX-SR01-P001",f"WSE{self.rack:03d}-MX-SR02-P001",f"WSE{self.rack:03d}-MX-SR03-P001",f"WSE{self.rack:03d}-MX-SR04-P001","SWX001-SX-SW01", "SWX003-SX-SW01", "SWX005-SX-SW01", "SWX007-SX-SW01", "SWX009-SX-SW01", "SWX011-SX-SW01",'','',f"WSE{self.rack:03d}-WK-SR01-P001",f"WSE{self.rack:03d}-WK-SR02-P001",'',''],
            [f"WSE{self.rack:03d}-MX-SR01-P002",f"WSE{self.rack:03d}-MX-SR02-P002",f"WSE{self.rack:03d}-MX-SR03-P002",f"WSE{self.rack:03d}-MX-SR04-P002","SWX002-SX-SW01", "SWX004-SX-SW01", "SWX006-SX-SW01", "SWX008-SX-SW01", "SWX010-SX-SW01", "SWX012-SX-SW01",'','',f"WSE{self.rack:03d}-WK-SR01-P002",f"WSE{self.rack:03d}-WK-SR02-P002",'','']
        ]


class HPE4600(ConnectionMap):

    def get_port_map(self):
        pmap = [[], [], [], []]
        for i in range(64):
            pmap[i%4].append(f"Eth1/{i+1}")
        return pmap

    def get_ports(self):
        ports = []
        for r in self.get_port_map():
            ports += r
        return ports

    def get_connection_template(self):
        return [
            [f"WSE{self.rack:03d}-MX-SR01-P001", f"WSE{self.rack:03d}-MX-SR02-P001", f"WSE{self.rack:03d}-MX-SR03-P001", f"WSE{self.rack:03d}-MX-SR04-P001", f"WSE{self.rack:03d}-MX-SR05-P001", f"WSE{self.rack:03d}-MX-SR06-P001", f"WSE{self.rack:03d}-MX-SR07-P001", f"WSE{self.rack:03d}-MX-SR08-P001", f"WSE{self.rack:03d}-MX-SR09-P001", f"WSE{self.rack:03d}-MX-SR10-P001", f"WSE{self.rack:03d}-MX-SR11-P001", f"WSE{self.rack:03d}-MX-SR12-P001", f"WSE{self.rack:03d}-WK-SR01-P001", f"WSE{self.rack:03d}-WK-SR02-P001", f"WSE{self.rack:03d}-WK-SR03-P001", f"WSE{self.rack:03d}-WK-SR04-P001"],
            [f"WSE{self.rack:03d}-MX-SR01-P002", f"WSE{self.rack:03d}-MX-SR02-P002", f"WSE{self.rack:03d}-MX-SR03-P002", f"WSE{self.rack:03d}-MX-SR04-P002", f"WSE{self.rack:03d}-MX-SR05-P002", f"WSE{self.rack:03d}-MX-SR06-P002", f"WSE{self.rack:03d}-MX-SR07-P002", f"WSE{self.rack:03d}-MX-SR08-P002", f"WSE{self.rack:03d}-MX-SR09-P002", f"WSE{self.rack:03d}-MX-SR10-P002", f"WSE{self.rack:03d}-MX-SR11-P002", f"WSE{self.rack:03d}-MX-SR12-P002", f"WSE{self.rack:03d}-WK-SR01-P002", f"WSE{self.rack:03d}-WK-SR02-P002", f"WSE{self.rack:03d}-WK-SR03-P002", f"WSE{self.rack:03d}-WK-SR04-P002"],
            ["SWX001-SX-SW01", "SWX003-SX-SW01", "SWX005-SX-SW01", "SWX007-SX-SW01", "SWX009-SX-SW01", "SWX011-SX-SW01",'','','','','', f"WSE{self.rack:03d}-MG-SR01-P001", f"WSE{self.rack:03d}-WK-SR05-P001", f"WSE{self.rack:03d}-WK-SR06-P001", f"WSE{self.rack:03d}-WK-SR07-P001", f"WSE{self.rack:03d}-WK-SR08-P001"],
            ["SWX002-SX-SW01", "SWX004-SX-SW01", "SWX006-SX-SW01", "SWX008-SX-SW01", "SWX010-SX-SW01", "SWX012-SX-SW01",'','','','','', f"WSE{self.rack:03d}-MG-SR01-P002", f"WSE{self.rack:03d}-WK-SR05-P002", f"WSE{self.rack:03d}-WK-SR06-P002", f"WSE{self.rack:03d}-WK-SR07-P002", f"WSE{self.rack:03d}-WK-SR08-P002"]
        ]


class HPE12908(ConnectionMap):

    def get_port_map(self):
        pmap = [[], [], [], [], [], []]
        for i in range(6):
            for j in range(48):
                pmap[i].append(f"HundredGigE{i+2}/0/{j+1}")
        return pmap

    def get_ports(self):
        ports = []
        for r in self.get_port_map():
            ports += r
        return ports

    def get_connection_template(self):
        return [
            [f"SWX{self.rack:03d}-SX-SR01-P001", f"SWX{self.rack:03d}-SX-SR01-P002", f"SWX{self.rack:03d}-SX-SR01-P003", f"SWX{self.rack:03d}-SX-SR01-P004", f"SWX{self.rack:03d}-SX-SR01-P005", f"SWX{self.rack:03d}-SX-SR01-P006", f"SWX{self.rack:03d}-SX-SR02-P001", f"SWX{self.rack:03d}-SX-SR02-P002", f"SWX{self.rack:03d}-SX-SR02-P003", f"SWX{self.rack:03d}-SX-SR02-P004", f"SWX{self.rack:03d}-SX-SR02-P005", f"SWX{self.rack:03d}-SX-SR02-P006", f"SWX{self.rack:03d}-SX-SR03-P001", f"SWX{self.rack:03d}-SX-SR03-P002", f"SWX{self.rack:03d}-SX-SR03-P003", f"SWX{self.rack:03d}-SX-SR03-P004", f"SWX{self.rack:03d}-SX-SR03-P005", f"SWX{self.rack:03d}-SX-SR03-P006", f"SWX{self.rack:03d}-SX-SR04-P001", f"SWX{self.rack:03d}-SX-SR04-P002", f"SWX{self.rack:03d}-SX-SR04-P003", f"SWX{self.rack:03d}-SX-SR04-P004", f"SWX{self.rack:03d}-SX-SR04-P005", f"SWX{self.rack:03d}-SX-SR04-P006", "WSE001-MX-SW01", "WSE002-MX-SW01", "WSE003-MX-SW01", "WSE004-MX-SW01", "WSE005-MX-SW01", "WSE006-MX-SW01", "WSE007-MX-SW01", "WSE008-MX-SW01", "WSE009-MX-SW01", "WSE010-MX-SW01", "WSE011-MX-SW01", "WSE012-MX-SW01", f"CS001-P{self.rack:03d}", f"CS002-P{self.rack:03d}", f"CS003-P{self.rack:03d}", f"CS004-P{self.rack:03d}", f"CS005-P{self.rack:03d}", f"CS006-P{self.rack:03d}", f"CS007-P{self.rack:03d}", f"CS008-P{self.rack:03d}", f"CS009-P{self.rack:03d}", f"CS010-P{self.rack:03d}", f"CS011-P{self.rack:03d}", f"CS012-P{self.rack:03d}"],
            [f"SWX{self.rack:03d}-SX-SR05-P001", f"SWX{self.rack:03d}-SX-SR05-P002", f"SWX{self.rack:03d}-SX-SR05-P003", f"SWX{self.rack:03d}-SX-SR05-P004", f"SWX{self.rack:03d}-SX-SR05-P005", f"SWX{self.rack:03d}-SX-SR05-P006", f"SWX{self.rack:03d}-SX-SR06-P001", f"SWX{self.rack:03d}-SX-SR06-P002", f"SWX{self.rack:03d}-SX-SR06-P003", f"SWX{self.rack:03d}-SX-SR06-P004", f"SWX{self.rack:03d}-SX-SR06-P005", f"SWX{self.rack:03d}-SX-SR06-P006", f"SWX{self.rack:03d}-SX-SR07-P001", f"SWX{self.rack:03d}-SX-SR07-P002", f"SWX{self.rack:03d}-SX-SR07-P003", f"SWX{self.rack:03d}-SX-SR07-P004", f"SWX{self.rack:03d}-SX-SR07-P005", f"SWX{self.rack:03d}-SX-SR07-P006", f"SWX{self.rack:03d}-SX-SR08-P001", f"SWX{self.rack:03d}-SX-SR08-P002", f"SWX{self.rack:03d}-SX-SR08-P003", f"SWX{self.rack:03d}-SX-SR08-P004", f"SWX{self.rack:03d}-SX-SR08-P005", f"SWX{self.rack:03d}-SX-SR08-P006", "WSE013-MX-SW01", "WSE014-MX-SW01", "WSE015-MX-SW01", "WSE016-MX-SW01", "WSE017-MX-SW01", "WSE018-MX-SW01", "WSE019-MX-SW01", "WSE020-MX-SW01", "WSE021-MX-SW01", "WSE022-MX-SW01", "WSE023-MX-SW01", "WSE024-MX-SW01", f"CS013-P{self.rack:03d}", f"CS014-P{self.rack:03d}", f"CS015-P{self.rack:03d}", f"CS016-P{self.rack:03d}", f"CS017-P{self.rack:03d}", f"CS018-P{self.rack:03d}", f"CS019-P{self.rack:03d}", f"CS020-P{self.rack:03d}", f"CS021-P{self.rack:03d}", f"CS022-P{self.rack:03d}", f"CS023-P{self.rack:03d}", f"CS024-P{self.rack:03d}"],
            [f"SWX{self.rack:03d}-SX-SR09-P001", f"SWX{self.rack:03d}-SX-SR09-P002", f"SWX{self.rack:03d}-SX-SR09-P003", f"SWX{self.rack:03d}-SX-SR09-P004", f"SWX{self.rack:03d}-SX-SR09-P005", f"SWX{self.rack:03d}-SX-SR09-P006", f"SWX{self.rack:03d}-SX-SR10-P001", f"SWX{self.rack:03d}-SX-SR10-P002", f"SWX{self.rack:03d}-SX-SR10-P003", f"SWX{self.rack:03d}-SX-SR10-P004", f"SWX{self.rack:03d}-SX-SR10-P005", f"SWX{self.rack:03d}-SX-SR10-P006", f"SWX{self.rack:03d}-SX-SR11-P001", f"SWX{self.rack:03d}-SX-SR11-P002", f"SWX{self.rack:03d}-SX-SR11-P003", f"SWX{self.rack:03d}-SX-SR11-P004", f"SWX{self.rack:03d}-SX-SR11-P005", f"SWX{self.rack:03d}-SX-SR11-P006", f"SWX{self.rack:03d}-SX-SR12-P001", f"SWX{self.rack:03d}-SX-SR12-P002", f"SWX{self.rack:03d}-SX-SR12-P003", f"SWX{self.rack:03d}-SX-SR12-P004", f"SWX{self.rack:03d}-SX-SR12-P005", f"SWX{self.rack:03d}-SX-SR12-P006", "WSE025-MX-SW01", "WSE026-MX-SW01", "WSE027-MX-SW01", "WSE028-MX-SW01", "WSE029-MX-SW01", "WSE030-MX-SW01", "WSE031-MX-SW01", "WSE032-MX-SW01", "WSE033-MX-SW01", "WSE034-MX-SW01", "WSE035-MX-SW01", "WSE036-MX-SW01", f"CS025-P{self.rack:03d}", f"CS026-P{self.rack:03d}", f"CS027-P{self.rack:03d}", f"CS028-P{self.rack:03d}", f"CS029-P{self.rack:03d}", f"CS030-P{self.rack:03d}", f"CS031-P{self.rack:03d}", f"CS032-P{self.rack:03d}", f"CS033-P{self.rack:03d}", f"CS034-P{self.rack:03d}", f"CS035-P{self.rack:03d}", f"CS036-P{self.rack:03d}"],
            [f"SWX{self.rack:03d}-SX-SR13-P001", f"SWX{self.rack:03d}-SX-SR13-P002", f"SWX{self.rack:03d}-SX-SR13-P003", f"SWX{self.rack:03d}-SX-SR13-P004", f"SWX{self.rack:03d}-SX-SR13-P005", f"SWX{self.rack:03d}-SX-SR13-P006", f"SWX{self.rack:03d}-SX-SR14-P001", f"SWX{self.rack:03d}-SX-SR14-P002", f"SWX{self.rack:03d}-SX-SR14-P003", f"SWX{self.rack:03d}-SX-SR14-P004", f"SWX{self.rack:03d}-SX-SR14-P005", f"SWX{self.rack:03d}-SX-SR14-P006", f"SWX{self.rack:03d}-SX-SR15-P001", f"SWX{self.rack:03d}-SX-SR15-P002", f"SWX{self.rack:03d}-SX-SR15-P003", f"SWX{self.rack:03d}-SX-SR15-P004", f"SWX{self.rack:03d}-SX-SR15-P005", f"SWX{self.rack:03d}-SX-SR15-P006", f"SWX{self.rack:03d}-SX-SR16-P001", f"SWX{self.rack:03d}-SX-SR16-P002", f"SWX{self.rack:03d}-SX-SR16-P003", f"SWX{self.rack:03d}-SX-SR16-P004", f"SWX{self.rack:03d}-SX-SR16-P005", f"SWX{self.rack:03d}-SX-SR16-P006", "WSE037-MX-SW01", "WSE038-MX-SW01", "WSE039-MX-SW01", "WSE040-MX-SW01", "WSE041-MX-SW01", "WSE042-MX-SW01", "WSE043-MX-SW01", "WSE044-MX-SW01", "WSE045-MX-SW01", "WSE046-MX-SW01", "WSE047-MX-SW01", "WSE048-MX-SW01", f"CS037-P{self.rack:03d}", f"CS038-P{self.rack:03d}", f"CS039-P{self.rack:03d}", f"CS040-P{self.rack:03d}", f"CS041-P{self.rack:03d}", f"CS042-P{self.rack:03d}", f"CS043-P{self.rack:03d}", f"CS044-P{self.rack:03d}", f"CS045-P{self.rack:03d}", f"CS046-P{self.rack:03d}", f"CS047-P{self.rack:03d}", f"CS048-P{self.rack:03d}"],
            [f"SWX{self.rack:03d}-SX-SR17-P001", f"SWX{self.rack:03d}-SX-SR17-P002", f"SWX{self.rack:03d}-SX-SR17-P003", f"SWX{self.rack:03d}-SX-SR17-P004", f"SWX{self.rack:03d}-SX-SR17-P005", f"SWX{self.rack:03d}-SX-SR17-P006", f"SWX{self.rack:03d}-SX-SR18-P001", f"SWX{self.rack:03d}-SX-SR18-P002", f"SWX{self.rack:03d}-SX-SR18-P003", f"SWX{self.rack:03d}-SX-SR18-P004", f"SWX{self.rack:03d}-SX-SR18-P005", f"SWX{self.rack:03d}-SX-SR18-P006", f"SWX{self.rack:03d}-SX-SR19-P001", f"SWX{self.rack:03d}-SX-SR19-P002", f"SWX{self.rack:03d}-SX-SR19-P003", f"SWX{self.rack:03d}-SX-SR19-P004", f"SWX{self.rack:03d}-SX-SR19-P005", f"SWX{self.rack:03d}-SX-SR19-P006", f"SWX{self.rack:03d}-SX-SR20-P001", f"SWX{self.rack:03d}-SX-SR20-P002", f"SWX{self.rack:03d}-SX-SR20-P003", f"SWX{self.rack:03d}-SX-SR20-P004", f"SWX{self.rack:03d}-SX-SR20-P005", f"SWX{self.rack:03d}-SX-SR20-P006", "WSE049-MX-SW01", "WSE050-MX-SW01", "WSE051-MX-SW01", "WSE052-MX-SW01", "WSE053-MX-SW01", "WSE054-MX-SW01", "WSE055-MX-SW01", "WSE056-MX-SW01", "WSE057-MX-SW01", "WSE058-MX-SW01", "WSE059-MX-SW01", "WSE060-MX-SW01", f"CS049-P{self.rack:03d}", f"CS050-P{self.rack:03d}", f"CS051-P{self.rack:03d}", f"CS052-P{self.rack:03d}", f"CS053-P{self.rack:03d}", f"CS054-P{self.rack:03d}", f"CS055-P{self.rack:03d}", f"CS056-P{self.rack:03d}", f"CS057-P{self.rack:03d}", f"CS058-P{self.rack:03d}", f"CS059-P{self.rack:03d}", f"CS060-P{self.rack:03d}"],
            [f"SWX{self.rack:03d}-SX-SR21-P001", f"SWX{self.rack:03d}-SX-SR21-P002", f"SWX{self.rack:03d}-SX-SR21-P003", f"SWX{self.rack:03d}-SX-SR21-P004", f"SWX{self.rack:03d}-SX-SR21-P005", f"SWX{self.rack:03d}-SX-SR21-P006", f"SWX{self.rack:03d}-SX-SR22-P001", f"SWX{self.rack:03d}-SX-SR22-P002", f"SWX{self.rack:03d}-SX-SR22-P003", f"SWX{self.rack:03d}-SX-SR22-P004", f"SWX{self.rack:03d}-SX-SR22-P005", f"SWX{self.rack:03d}-SX-SR22-P006", f"SWX{self.rack:03d}-SX-SR23-P001", f"SWX{self.rack:03d}-SX-SR23-P002", f"SWX{self.rack:03d}-SX-SR23-P003", f"SWX{self.rack:03d}-SX-SR23-P004", f"SWX{self.rack:03d}-SX-SR23-P005", f"SWX{self.rack:03d}-SX-SR23-P006", f"SWX{self.rack:03d}-SX-SR24-P001", f"SWX{self.rack:03d}-SX-SR24-P002", f"SWX{self.rack:03d}-SX-SR24-P003", f"SWX{self.rack:03d}-SX-SR24-P004", f"SWX{self.rack:03d}-SX-SR24-P005", f"SWX{self.rack:03d}-SX-SR24-P006", "WSE061-MX-SW01", "WSE062-MX-SW01", "WSE063-MX-SW01", "WSE064-MX-SW01", "WSE065-MX-SW01", "WSE066-MX-SW01", "WSE067-MX-SW01", "WSE068-MX-SW01", "WSE069-MX-SW01", "WSE070-MX-SW01", "WSE071-MX-SW01", "WSE072-MX-SW01", f"CS061-P{self.rack:03d}", f"CS062-P{self.rack:03d}", f"CS063-P{self.rack:03d}", f"CS064-P{self.rack:03d}", f"CS065-P{self.rack:03d}", f"CS066-P{self.rack:03d}", f"CS067-P{self.rack:03d}", f"CS068-P{self.rack:03d}", f"CS069-P{self.rack:03d}", f"CS070-P{self.rack:03d}", f"CS071-P{self.rack:03d}", f"CS072-P{self.rack:03d}"]
        ]


class PhysicalDevice:

    def __init__(self, device_name):
        self._elems = device_name.split('-')
        if self.is_system:
            self.name = self._elems[0]
        else:
            self.name = "-".join(self._elems[:3])

    @property
    def is_switch(self):
        return (not self.is_system) and self._elems[2].startswith("SW")

    @property
    def is_node(self):
        return (not self.is_system) and self._elems[2].startswith("SR")

    @property
    def is_system(self):
        return self._elems[0].startswith("CS")

    def get_node_port(self):
        p = int(self._elems[-1][1:])
        return f"eth100g{p}"

    def get_system_port(self):
        p = int(self._elems[-1][1:])
        return f"Port {p}"


class Cluster:

    def __init__(self, network_config):
        self.network_config = network_config

    def get_nodes(self):
        nodes = []
        cmap = {
            "management_nodes": CMManagementNode,
            "memoryx_nodes": CMMemoryxNode,
            "worker_nodes": CMWorkerNode,
            "swarmx_nodes": CMSwarmxNode
        }
        for c, nc in cmap.items():
            for n in self.network_config.get(c, []):
                nodes.append(nc(n, self.network_config))
        return nodes

    def get_switches(self):
        switches = []
        for s in self.network_config.get("switches", []):
            if s['tier'] == 'AW':
                switches.append(MemoryxSwitch(s, self.network_config))
            else:
                switches.append(SwarmxSwitch(s, self.network_config))
        return switches

    def get_systems(self):
        systems = []
        for s in self.network_config.get("systems", []):
            systems.append(System(s, self.network_config))
        return systems

class Rack:

    def __init__(self, network_config, rack_num):
        self.network_config = network_config
        self.rack = int(rack_num)
        self._memx_prefix = f"WSE{self.rack:03d}"
        self._swarmx_prefix = f"SWX{self.rack:03d}"


    def _get_switch(self, switch_category):
        for s in self.network_config.get("switches", []):
            if s["tier"] == switch_category and int(s["rack"]) == self.rack:
                return s
        else:
            return None

    def get_memoryx_switch(self):
        return self._get_switch("AW")

    def get_swarmx_switch(self):
        return self._get_switch("BR")

    def get_memoryx_switch_name(self):
        return f"{self._memx_prefix}-MX-SW01"

    def get_swarmx_switch_name(self):
        return f"{self._swarmx_prefix}-SX-SW01"

    def get_memoryx_node_name(self, rack_pos):
        return f"{self._memx_prefix}-MX-SR{rack_pos:02d}"

    def get_worker_node_name(self, rack_pos):
        return f"{self._memx_prefix}-WK-SR{rack_pos:02d}"

    def get_management_node_name(self, rack_pos):
        return f"{self._memx_prefix}-MG-SR{rack_pos:02d}"

    def get_swarmx_node_name(self, rack_pos):
        return f"{self._swarmx_prefix}-SX-SR{rack_pos:02d}"


class System:

    def __init__(self, obj, network_config):
        self.obj = obj
        self.name = obj['name']
        self.rack = int(obj['rack'])
        self.network_config = network_config

    def get_device_name(self):
        return f"CS{self.rack:03d}"


class Switch:
    MEMX_CONN_MAP = {
        "hpe:3700": HPE3700,
        "hpe:4600": HPE4600
    }
    SWARMX_CONN_MAP = {
        "hpe:12908": HPE12908
    }

    def __init__(self, obj, network_config):
        self.obj = obj
        self.name = obj['name']
        self.rack = int(obj['rack'])
        self.network_config = network_config
        self._set_conn_map()

    def _set_conn_map(self):
        m = self.MEMX_CONN_MAP if self.obj["tier"] == "AW" else self.SWARMX_CONN_MAP
        self.conn_map = m[f"{self.obj['vendor']}:{self.obj['model']}"](
            self.obj, self.network_config
        )

    def get_port(self, device_physical_name):
        """ Return port to which given device is connected
        """
        return self.conn_map.get_port(device_physical_name)

    def get_device(self, port_num):
        """ Return physical name of device connected to port_num
        """
        return self.conn_map.get_device(port_num)

    def get_ports(self):
        """ Return list of ports for the switch
        """
        return self.conn_map.get_ports()



class MemoryxSwitch(Switch):

    def get_device_name(self):
        rack = Rack(self.network_config, self.rack)
        return rack.get_memoryx_switch_name()

    def get_connections(self):
        conns = list()
        nodes = Cluster(self.network_config).get_nodes()
        switches = Cluster(self.network_config).get_switches()
        for p in self.get_ports():
            device = PhysicalDevice(self.get_device(p))
            if not device.name:
                continue
            if device.is_node:
                node = None
                for n in nodes:
                    if n.get_device_name() == device.name:
                        node = n
                        break
                if node is None:
                    continue
                conns.append(dict(
                    my_port=p,
                    name=node.name,
                    port=device.get_node_port()
                ))
            elif device.is_switch:
                # Get Switch() from device name
                switch = None
                for s in switches:
                    if s.get_device_name() == device.name:
                        switch = s
                        break
                if switch is None:
                    continue
                # get port_num using switch.get_port(self.get_physical_name)
                conns.append(dict(
                    my_port=p,
                    name=switch.name,
                    port=switch.get_port(self.get_device_name())
                ))
        return conns


class SwarmxSwitch(Switch):

    def get_device_name(self):
        rack = Rack(self.network_config, self.rack-100)
        return rack.get_swarmx_switch_name()

    def get_connections(self):
        conns = list()
        self.conn_map.rack -= 100
        nodes = Cluster(self.network_config).get_nodes()
        switches = Cluster(self.network_config).get_switches()
        systems = Cluster(self.network_config).get_systems()
        for p in self.get_ports():
            device = PhysicalDevice(self.get_device(p))
            if not device.name:
                continue
            if device.is_node:
                node = None
                for n in nodes:
                    if n.get_device_name() == device.name:
                        node = n
                        break
                if node is None:
                    continue
                conns.append(dict(
                    my_port=p,
                    name=node.name,
                    port=device.get_node_port()
                ))
            elif device.is_switch:
                # Get Switch() from device name
                switch = None
                for s in switches:
                    if s.get_device_name() == device.name:
                        switch = s
                        break
                if switch is None:
                    continue
                # get port_num using switch.get_port(self.get_physical_name)
                conns.append(dict(
                    my_port=p,
                    name=switch.name,
                    port=switch.get_port(self.get_device_name())
                ))
            elif device.is_system:
                system = None
                for s in systems:
                    if s.get_device_name() == device.name:
                        system = s
                        break
                if system is None:
                    continue
                conns.append(dict(
                    my_port=p,
                    name=f"{system.name}-cm",
                    port=device.get_system_port()
                ))

        return conns

class Node:

    def __init__(self, obj, network_config):
        self.obj = obj
        self.network_config = network_config
        self.name = obj['name']
        self.rack = int(obj['rack'])
        self.rack_pos = self._get_rack_pos()

    @property
    @staticmethod
    def node_category():
        """ Category key for this node in network config
        """
        return ''

    def _sort_nodes(self):
        rack_objects = [
            o for o in self.network_config.get(self.node_category, []) if int(o["rack"]) == self.rack
        ]
        return sorted(rack_objects, key=itemgetter("rack_unit"))

    def _get_rack_pos(self):
        sorted_nodes = self._sort_nodes()
        for i, s in enumerate(self._sort_nodes()):
            if s['name'] == self.obj['name']:
                return (i+1)
        else:
            return None

    def get_connections(self):
        """ Returns a list of dicts specifying port, switch-name and switch-port
        """
        return list()

    def get_device_name(self):
        return ""


class CMMemoryxNode(Node):

    node_category = "memoryx_nodes"

    def get_connections(self):
        rack = Rack(self.network_config, self.rack)
        switch = Switch(rack.get_memoryx_switch(), self.network_config)
        conns = list()
        for i in range(2):
            port = f"{(i+1):03d}"
            conns.append(dict(
                iface_name=f"eth100g{i}",
                switch_name=switch.name,
                switch_port=switch.get_port(
                    f"{rack.get_memoryx_node_name(self.rack_pos)}-P{port}"
                )
            ))
        return conns

    def get_device_name(self):
        rack = Rack(self.network_config, self.rack)
        return rack.get_memoryx_node_name(self.rack_pos)


class CMWorkerNode(Node):

    node_category = "worker_nodes"

    def get_connections(self):
        rack = Rack(self.network_config, self.rack)
        switch = Switch(rack.get_memoryx_switch(), self.network_config)
        conns = list()
        for i in range(2):
            port = f"{(i+1):03d}"
            conns.append(dict(
                iface_name=f"eth100g{i}",
                switch_name=switch.name,
                switch_port=switch.get_port(
                    f"{rack.get_worker_node_name(self.rack_pos)}-P{port}"
                )
            ))
        return conns

    def get_device_name(self):
        rack = Rack(self.network_config, self.rack)
        return rack.get_worker_node_name(self.rack_pos)


class CMManagementNode(Node):

    node_category = "management_nodes"

    def get_connections(self):
        rack = Rack(self.network_config, self.rack)
        switch = Switch(rack.get_memoryx_switch(), self.network_config)
        conns = list()
        for i in range(2):
            port = f"{(i+1):03d}"
            conns.append(dict(
                iface_name=f"eth100g{i}",
                switch_name=switch.name,
                switch_port=switch.get_port(
                    f"{rack.get_management_node_name(self.rack_pos)}-P{port}"
                )
            ))
        return conns

    def get_device_name(self):
        rack = Rack(self.network_config, self.rack)
        return rack.get_management_node_name(self.rack_pos)


class CMSwarmxNode(Node):

    node_category = "swarmx_nodes"

    def get_connections(self):
        rack = Rack(self.network_config, self.rack)
        switch = Switch(rack.get_swarmx_switch(), self.network_config)
        conns = list()
        for i in range(6):
            port = f"{(i+1):03d}"
            conns.append(dict(
                iface_name=f"eth100g{i}",
                switch_name=switch.name,
                switch_port=switch.get_port(
                    f"{rack.get_swarmx_node_name(self.rack_pos)}-P{port}"
                )
            ))
        return conns

    def get_device_name(self):
        rack = Rack(self.network_config, self.rack)
        return rack.get_swarmx_node_name(self.rack_pos)


class NetworkLayout:

    def __init__(self, network_config):
        self.network_config = network_config

    def _get_node_layout(self, node, node_category):
        layout = list()
        for i in node.get("interfaces", []):
            switch = None
            for s in self.network_config.get('switches', []):
                if s['name'] == i['switch_name']:
                    if node_category == "swarmx_nodes":
                        switch = SwarmxSwitch(s, self.network_config)
                    else:
                        switch = MemoryxSwitch(s, self.network_config)
                    break
            if switch is None:
                continue
            layout.append(dict(
                port=i['name'],
                switch=switch.get_device_name(),
                switch_port=i['switch_port']
            ))
        return layout

    def _node_layout(self):
        cmap = {
            "management_nodes": CMManagementNode,
            "memoryx_nodes": CMMemoryxNode,
            "worker_nodes": CMWorkerNode,
            "swarmx_nodes": CMSwarmxNode
        }
        layout = dict()
        for category, cls in cmap.items():
            for n in self.network_config.get(category, []):
                cmn = cls(n, self.network_config)
                layout[cmn.get_device_name()] = self._get_node_layout(n, category)
        return layout

    def _system_layout(self):
        layout = dict()
        system_map = dict()
        for s in self.network_config.get("systems", []):
            system = System(s, self.network_config)
            system_map[s['name']] = system.get_device_name()
            layout[system.get_device_name()] = list()

        for c in self.network_config.get('system_connections', []):
            switch = None
            for s in self.network_config.get('switches', []):
                if s['name'] == c['switch_name']:
                    switch = SwarmxSwitch(s, self.network_config)
                    break
            if switch is None:
                continue
            layout[system_map[c['system_name']]].append(dict(
                port=c['system_port'],
                switch=switch.get_device_name(),
                switch_port=c['switch_port']
            ))
        return layout

    def _xconnect_layout(self):
        switch_map = dict()
        layout = dict()

        for s in self.network_config.get('switches', []):
            if s['tier'] == 'AW':
                switch = MemoryxSwitch(s, self.network_config)
            else:
                switch = SwarmxSwitch(s, self.network_config)
            layout[switch.get_device_name()] = list()
            switch_map[s['name']] = switch.get_device_name()

        for c in self.network_config.get('xconnect', {}).get(
            'connections', []):
            for i, l in enumerate(c['links']):
                if i == 0:
                    switch = switch_map[c['links'][1]['name']]
                    switch_port = c['links'][1]['port']
                else:
                    switch = switch_map[c['links'][0]['name']]
                    switch_port = c['links'][0]['port']
                layout[switch_map[l['name']]].append(dict(
                    port=l['port'],
                    switch=switch,
                    switch_port=switch_port
                ))
        return layout

    def get_layout(self):
        return dict(
            nodes=self._node_layout(),
            systems=self._system_layout(),
            xconnects=self._xconnect_layout()
        )

def compare_network_layouts(network_config_file_src, network_config_file_tgt):
    with open(network_config_file_src, 'r') as f:
        network_config_src = json.load(f)
    with open(network_config_file_tgt, 'r') as f:
        network_config_tgt = json.load(f)

    layout_src = NetworkLayout(network_config_src).get_layout()
    layout_tgt = NetworkLayout(network_config_tgt).get_layout()

    diff = dict(
        nodes=list(),
        systems=list(),
        xconnects=list()
    )

    headers = ["Node", "Port", "Source switch", "Source switch port", "Target switch", "Target switch port"]
    rows = []
    for node, layout in layout_src['nodes'].items():
        if node not in layout_tgt['nodes']:
            continue
        l_tgt = layout_tgt['nodes'][node]
        for i, l in enumerate(layout):
            if i < len(l_tgt):
                lt = l_tgt[i]
                """
                if l['switch'] != lt['switch']:
                    print(
                        f"{l['port']} of {node} is connected to {l['switch']} in "
                        f"source config and {lt['switch']} in target config"
                    )
                    continue
                if l['switch_port'] != lt['switch_port']:
                    print(
                        f"{l['port']} of {node} is connected to {l['switch_port']} of {l['switch']} in "
                        f"source config and {lt['switch_port']} in target config"
                    )
                """
                if l['switch'] != lt['switch'] or l['switch_port'] != lt['switch_port']:
                    rows.append([
                        node, l['port'], l['switch'], l['switch_port'],
                        lt['switch'], lt['switch_port']
                    ])
    if rows:
        print("Nodes")
        print(tabulate(rows, headers))

    headers = ["System", "Port", "Source switch", "Source switch port", "Target switch", "Target switch port"]
    rows = []
    for system, layout in layout_src['systems'].items():
        if system not in layout_tgt['systems']:
            continue
        l_tgt = dict()
        for l in layout_tgt['systems'][system]:
            l_tgt[l['port']] = dict(
                switch=l['switch'],
                switch_port=l['switch_port']
            )
        for l in layout:
            """
            if l['switch'] != l_tgt[l['port']]['switch']:
                print(
                    f"{l['port']} of system {system} is connected to {l['switch']} in "
                    f"source config and {l_tgt[l['port']]['switch']} in target config"
                )
                continue
            if l['switch_port'] != l_tgt[l['port']]['switch_port']:
                print(
                    f"{l['port']} of system {system} is connected to {l['switch_port']} of {l['switch']} in "
                    f"source config and {l_tgt[l['port']]['switch_port']} in target config"
                )
            """
            if l['switch'] != l_tgt[l['port']]['switch'] or l['switch_port'] != l_tgt[l['port']]['switch_port']:
                rows.append([
                    system, l['port'], l['switch'], l['switch_port'],
                    l_tgt[l['port']]['switch'], l_tgt[l['port']]['switch_port']
                ])
    if rows:
        print("\nSystems")
        print(tabulate(rows, headers))

    headers = ["Switch", "Port", "Source switch", "Source switch port", "Target switch", "Target switch port"]
    rows = []
    for switch, layout in layout_src['xconnects'].items():
        if switch not in layout_tgt['xconnects']:
            continue
        for l in layout_tgt['xconnects'][switch]:
            l_tgt[l['port']] = dict(
                switch=l['switch'],
                switch_port=l['switch_port']
            )

        for l in layout:
            """
            if l['switch'] != l_tgt[l['port']]['switch']:
                print(
                    f"{l['port']} of switch {switch} is connected to {l['switch']} in "
                    f"source config and {l_tgt[l['port']]['switch']} in target config"
                )
                continue
            if l['switch_port'] != l_tgt[l['port']]['switch_port']:
                print(
                    f"{l['port']} of switch {switch} is connected to {l['switch_port']} of {l['switch']} in "
                    f"source config and {l_tgt[l['port']]['switch_port']} in target config"
                )
            """
            if l['switch'] != l_tgt[l['port']]['switch'] or l['switch_port'] != l_tgt[l['port']]['switch_port']:
                rows.append([
                    switch, l['port'], l['switch'], l['switch_port'],
                    l_tgt[l['port']]['switch'], l_tgt[l['port']]['switch_port']
                ])

    if rows:
        print("\nXconnects")
        print(tabulate(rows, headers))


def dump_layout(network_config_file):
    with open(network_config_file, 'r') as f:
        network_config = json.load(f)

    layout = NetworkLayout(network_config).get_layout()

    print("Nodes\n")
    headers = ["Node", "Port", "Switch", "Switch Port"]
    rows = []
    for node, nl in layout['nodes'].items():
        for l in nl:
            rows.append([
                node, l['port'],
                l['switch'], l['switch_port']
            ])
    print(tabulate(rows, headers))

    print("\nSystems\n")
    headers = ["System", "Port", "Switch", "Switch Port"]
    rows = []
    for system, sl in layout['systems'].items():
        for l in sl:
            rows.append([
                system, l['port'],
                l['switch'], l['switch_port']
            ])
    print(tabulate(rows, headers))

    print("\nXconnects\n")
    headers = ["Switch", "Port", "Peer switch", "Peer switch port"]
    rows = []
    for switch, xl in layout['xconnects'].items():
        for l in xl:
            rows.append([
                switch, l['port'],
                l['switch'], l['switch_port']
            ])
    print(tabulate(rows, headers))
