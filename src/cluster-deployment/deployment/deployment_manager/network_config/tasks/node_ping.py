import dataclasses
import enum
import logging
import random
import threading
from collections import defaultdict
from functools import cached_property
from itertools import product
from typing import List, Dict

from deployment_manager.network_config.common.context import NetworkCfgDoc, OBJ_TYPE_SR, OBJ_TYPE_SY
from deployment_manager.network_config.tasks.utils import PingTest

logger = logging.getLogger(__name__)

class PingTestKind(enum.Enum):
    GATEWAY = "gateway"
    VLAN = "vlan"
    NODE = "node"
    SYSTEM = "system"


def wrapped_range(lo, hi, start_idx):
    for i in range(start_idx, hi):
        yield i
    for i in range(lo, start_idx):
        yield i


class NodePingTestGenerator:
    """
    Methods for generating ping tests for nodes. The idea is that this class is called by NetworkTasks executing on
    a particular node to avoid materializing the entire set of ping tests at once. Requires threadsafe handling of
    shared datastructures.
    """

    def __init__(self,
                 cfg: NetworkCfgDoc,
                 include_src: List[str] = None,
                 include_dst: List[str] = None,
                 exclude: List[str] = None,
                 tests_per_dst_node_if: int = 1,
                 tests_per_dst_system_if: int = 1,
                 seed: int = 0,
                 exclude_down_src_interfaces = False,
                 ):
        """
        Args
            cfg: Network config
            include_src: optional list of names to include as sources. None = include all servers
            include_dst: optional list of names to include as destinations. None = include all servers and systems
            exclude: optional list of names to exclude
            tests_per_node_if: number of times a particular node interface appears as a ping target assuming that
                all the included nodes request a ping test
            tests_per_system_if: number of times a particular system interface appears as a ping target assuming that
                there are at least 'test_per_system_if' ax/sx/mx/mg nodes in the included nodes
        """
        self._cfg = cfg
        self._exclude_set = set(exclude) if exclude else set()
        self._include_src = set(include_src) if include_src else None
        self._include_dst = set(include_dst) if include_dst else None
        self._tests_per_dst_node_if = tests_per_dst_node_if
        self._tests_per_dst_system_if = tests_per_dst_system_if
        self._exclude_down_src_interfaces = exclude_down_src_interfaces
        self._random = random.Random(seed)
        self._lock = threading.Lock()

        self._src_partial_map = {}
        self._init_partial_src_node_tests()

        self._dst_node_partial_init = False
        self._dst_node_partial_list = []
        self._dst_node_partial_count = {}
        self._node_tests_per_src_est = -1

        self._dst_sys_partial_init = False
        self._dst_sys_partial_list = []
        self._dst_sys_partial_count = {}
        self._sys_tests_per_src_est = -1

    def _init_partial_src_node_tests(self):
        if self._include_src is not None:
            node_targets = [n for n in self._include_src if n not in self._exclude_set and self._cfg.get_entity(n) and self._cfg.get_entity(n).type == OBJ_TYPE_SR]
        else:
            node_targets = [n for n in self._cfg.get_names_by_type(OBJ_TYPE_SR) if n not in self._exclude_set]

        for name in node_targets:
            ent = self._cfg.get_entity(name)
            self._src_partial_map[name] = []
            for inf in ent.obj.get("interfaces", []):
                addr = inf.get("address")
                if not addr:
                    continue
                test = PingTest(
                    src_name=name,
                    src_if=inf["name"],
                    src_ip=addr.split("/")[0],
                    kind="",
                    dst_name=inf["switch_name"],
                    dst_if=inf["switch_port"],
                    dst_ip="",
                )
                if self._exclude_down_src_interfaces and inf.get("state", "up") != "up":
                    continue
                self._src_partial_map[name].append(test)

    def _init_partial_dst_node_tests(self):
        if self._dst_node_partial_init:
            return
        with self._lock:
            if self._dst_node_partial_init:
                return

            if self._include_dst is not None:
                node_targets = [n for n in self._include_dst if n not in self._exclude_set and self._cfg.get_entity(n) and self._cfg.get_entity(n).type == OBJ_TYPE_SR]
            else:
                node_targets = [n for n in self._cfg.get_names_by_type(OBJ_TYPE_SR) if n not in self._exclude_set]

            for name in node_targets:
                ent = self._cfg.get_entity(name)
                for inf in ent.obj.get("interfaces", []):
                    addr = inf.get("address")
                    if not addr:
                        continue
                    test = PingTest(
                        src_name=name,
                        src_if=inf["name"],
                        src_ip=addr.split("/")[0],
                        kind="",
                        dst_name=inf["switch_name"],
                        dst_if=inf["switch_port"],
                        dst_ip="",
                    )
                    self._dst_node_partial_list.append(test)
                    self._dst_node_partial_count[test.src_ip] = self._tests_per_dst_node_if
            self._random.shuffle(self._dst_node_partial_list)

            src_count = sum([len(v) for v in self._src_partial_map.values()])
            dst_count = len(self._dst_node_partial_list)
            if src_count > 0:
                c = dst_count * self._tests_per_dst_node_if / src_count
                if c % 1 != 0:
                    c = int(c) + 1
                else:
                    c = int(c)
                self._node_tests_per_src_est = c
            else:
                self._node_tests_per_src_est = 0

            self._dst_node_partial_init = True

    def _init_partial_system_dst_tests(self):
        if self._dst_sys_partial_init:
            return

        with self._lock:
            if self._dst_sys_partial_init:
                return

            if self._include_dst is not None:
                sys_targets = [n for n in self._include_dst if n not in self._exclude_set and self._cfg.get_entity(n) and self._cfg.get_entity(n).type == OBJ_TYPE_SY]
            else:
                sys_targets = [n for n in self._cfg.get_names_by_type(OBJ_TYPE_SY) if n not in self._exclude_set]

            for name in sys_targets:
                ent = self._cfg.get_entity(name)
                all_down = True
                for inf in ent.obj.get("interfaces", []):
                    addr = inf.get("address")
                    if not addr:
                        continue
                    test = PingTest(src_name="", src_if="", src_ip="", kind="system",
                        dst_name=name,
                        dst_if=inf.get("name", ""),
                        dst_ip=addr.split("/")[0],
                    )
                    if self._exclude_down_src_interfaces and inf.get('state', 'up') != "up":
                        continue
                    else:
                        all_down = False
                    self._dst_sys_partial_list.append(test)
                    self._dst_sys_partial_count[test.dst_ip] = self._tests_per_dst_system_if

                vip = ent.obj.get("vip")
                if vip:
                    test = PingTest(src_name="", src_if="", src_ip="", kind="system",
                                    dst_name=name,
                                    dst_if="vip",
                                    dst_ip=vip.split("/")[0],
                                    )
                    if not self._exclude_down_src_interfaces or not all_down:
                        self._dst_sys_partial_list.append(test)
                        self._dst_sys_partial_count[test.dst_ip] = self._tests_per_dst_system_if

            self._random.shuffle(self._dst_sys_partial_list)

            src_count = sum([len(v) for v in self._src_partial_map.values()])
            if src_count > 0:
                c = (len(self._dst_sys_partial_list) * self._tests_per_dst_system_if) / src_count
                if c % 1 != 0:
                    self._sys_tests_per_src_est = int(c) + 1
                else:
                    self._sys_tests_per_src_est = int(c)
            else:
                self._sys_tests_per_src_est = 0

            self._dst_sys_partial_init = True

    @cached_property
    def node2gateway_tests(self) -> Dict[str, List[PingTest]]:
        """
        Return ping tests for nodes to ping their gateways. Only considers src interfaces

        Returns
            dict of node names to their ping tests
        """

        self._init_partial_src_node_tests()

        rv = defaultdict(list)
        for partial_tests in self._src_partial_map.values():
            for partial_test in partial_tests:
                vlan = self._cfg.get_vlan_for_ip(partial_test.src_ip)
                if vlan:
                    rv[partial_test.src_name].append(dataclasses.replace(partial_test,
                        dst_if=f"vlan{vlan.number}",
                        dst_ip=str(vlan.gateway),
                        kind="gateway",
                    ))
        return rv

    @cached_property
    def vlan2vlan_tests(self) -> Dict[str, List[PingTest]]:
        """
        Create a test from every vlan to every other vlan using a representative node from each vlan.

        To keep things simple, target the intersection of the src and dst sets

        Returns
            list of node names to their ping tests
        """
        if self._include_src is not None and self._include_dst is not None:
            targets = self._include_src.intersection(self._include_dst)
        elif self._include_src is None and self._include_dst is None:
            targets = None
        elif self._include_src:
            targets = self._include_src
        else:
            targets = self._include_dst

        sw_vlan_test = {}  # (sw, vlanid) -> test for lowest order name for that vlan
        for node, partial_tests in self._src_partial_map.items():
            if targets is not None and node not in targets:
                continue
            for partial_test in partial_tests:
                vlan = self._cfg.get_vlan_for_ip(partial_test.src_ip)
                key = (partial_test.dst_name, vlan.number)
                if key not in sw_vlan_test or sw_vlan_test[key].src_name > partial_test.src_name:
                    sw_vlan_test[key] = PingTest(
                        src_name=partial_test.src_name,
                        src_if=f'{partial_test.src_if}:(vlan{vlan.number})',
                        src_ip=partial_test.src_ip,
                        dst_name="",
                        dst_if="",
                        dst_ip="",
                        kind="vlan",
                    )
        rv = defaultdict(list)
        for src, dst in product(sw_vlan_test.values(), sw_vlan_test.values()):
            if src.src_ip == dst.src_ip:
                continue
            rv[src.src_name].append(dataclasses.replace(src, dst_name=dst.src_name, dst_if=dst.src_if, dst_ip=dst.src_ip))
        return rv

    def _generate_node_tests(self, name: str) -> List[PingTest]:
        """
        Generate tests for this source node. Each interface on the node will ping some number of node interfaces
        such that the target is not on the same node, and the target is not pinged more than once by the same interface
        on this node.
        """
        if self._include_src is not None and name not in self._include_src:
            return []

        self._init_partial_dst_node_tests()

        # for each interface on the node, generate N number of tests with shuffle
        # prefer pinging nodes which are on different switches first to exercise spine connections
        with self._lock:
            rv = []
            for src_test in self._src_partial_map.get(name, []):
                if not self._dst_node_partial_list:
                    return rv
                tests_for_src = []
                start_idx = self._random.randint(0, len(self._dst_node_partial_list) - 1)
                rebuild = False
                for i in wrapped_range(0, len(self._dst_node_partial_list), start_idx):
                    partial_test = self._dst_node_partial_list[i]
                    if partial_test.src_name == name:
                        continue
                    tests_for_src.append(dataclasses.replace(src_test, dst_name=partial_test.src_name, dst_if=partial_test.src_if, dst_ip=partial_test.src_ip, kind="node"))
                    self._dst_node_partial_count[partial_test.src_ip] -= 1
                    if self._dst_node_partial_count[partial_test.src_ip] <= 0:
                        rebuild = True
                    if len(tests_for_src) >= self._node_tests_per_src_est:
                        break
                if rebuild:
                    self._dst_node_partial_list = [t for t in self._dst_node_partial_list if self._dst_node_partial_count[t.src_ip] > 0]
                rv.extend(tests_for_src)
            return rv

    def _generate_system_tests(self, name: str) -> List[PingTest]:
        """
        Get system tests for given node name. The node will ping some number of system interfaces and will never ping
        the same system interface more than once per node interface.
        """
        self._init_partial_system_dst_tests()

        with self._lock:
            rv = []
            for src_test in self._src_partial_map.get(name, []):
                if not src_test or not self._dst_sys_partial_list:
                    return rv
                tests_for_src = []
                start_idx = self._random.randint(0, len(self._dst_sys_partial_list) - 1)
                rebuild = False
                for i in wrapped_range(0, len(self._dst_sys_partial_list), start_idx):
                    dst_partial_test = self._dst_sys_partial_list[i]
                    tests_for_src.append(dataclasses.replace(dst_partial_test, src_name=src_test.src_name, src_if=src_test.src_if, src_ip=src_test.src_ip, kind="system"))
                    self._dst_sys_partial_count[dst_partial_test.dst_ip] -= 1
                    if self._dst_sys_partial_count[dst_partial_test.dst_ip] <= 0:
                        rebuild = True
                    if len(tests_for_src) >= self._sys_tests_per_src_est:
                        break
                if rebuild:
                    self._dst_sys_partial_list = [t for t in self._dst_sys_partial_list if self._dst_sys_partial_count[t.dst_ip] > 0]
                rv.extend(tests_for_src)
            return rv

    def get_tests(self, name: str, kinds: List[PingTestKind]) -> List[PingTest]:
        tests = []
        for kind in kinds:
            if kind == PingTestKind.VLAN:
                tests.extend(self.vlan2vlan_tests.get(name, []))
            elif kind == PingTestKind.GATEWAY:
                tests.extend(self.node2gateway_tests.get(name, []))
            elif kind == PingTestKind.NODE:
                tests.extend(self._generate_node_tests(name))
            elif kind == PingTestKind.SYSTEM:
                tests.extend(self._generate_system_tests(name))
            else:
                raise AssertionError(f"unknown ping test kind '{kind}'")
        return tests
