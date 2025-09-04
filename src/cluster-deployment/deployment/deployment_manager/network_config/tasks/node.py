#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

""" SSH based node commands
"""

import json
import logging
import time
from functools import cached_property
from typing import List

import paramiko.ssh_exception

from .node_ping import NodePingTestGenerator, PingTestKind
from .ssh_tasks import MakePrintingSSHTask, walk_conf_files
from .utils import parse_edgecore_port_alias, parse_lldp_doc, PingTest
from ..common.context import OBJ_TYPE_SW, NetworkCfgCtx
from ..common.task import NetworkTask, NetworkTaskResult, TargetNodes
from ..configs import NODE_INTERFACE_INDEXES
from ..schema import NetworkConfigSchema

logger = logging.getLogger(__name__)

_WELL_KNOWN_NODE_INTERFACES = set(n[0] for n in NODE_INTERFACE_INDEXES)


class NodePrintingTask(MakePrintingSSHTask):
    """ Node variant of the printing ssh task decorator
    """
    def __init__(self, alias, name=None, err_only=False):
        super().__init__(alias, NetworkConfigSchema.node_sections, name, err_only)


class NodeRebootTask(TargetNodes, NetworkTask):
    """ Issue reboot command """
    def run(self, *args, **kwargs) -> NetworkTaskResult:
        conn_established = False
        try:
            with self._ctx.get_conn(self.name) as conn:
                conn_established = True
                conn.exec("/sbin/shutdown -r now")
        except Exception as e:
            if not conn_established:
                raise
        return NetworkTaskResult.ok()


class NodeAwaitRebootTask(TargetNodes, NetworkTask):
    """ Wait until node has rebooted, checking 'uptime' command """
    reboot_timeout = 60 * 15  # 15-minute timeout
    check_interval = 30

    def run(self, *args, **kwargs) -> "NetworkTaskResult":
        logger.debug(f"checking reboot status on node {self.name}")
        deadline = time.time() + self.reboot_timeout
        ok = False

        while not ok and time.time() < deadline:
            # ensure the wait runs before ever checking to prevent situations where the node is reachable while shutting down
            time.sleep(self.check_interval)
            try:
                with self._ctx.get_conn(self.name) as conn:
                    conn.exec("uptime")
                ok = True
            except Exception as e:
                remainder = int(deadline - time.time() - self.check_interval)
                if remainder <= 0:
                    break
                logger.debug(
                    f"node {self.name} failed to respond after reboot; will timeout in {remainder} seconds. "
                    f"waiting {self.check_interval}s before retrying: {e}"
                )

        logger.info(f"node {self.name} reboot {'succeeded' if ok else 'failed; check node status'}")
        return NetworkTaskResult.ok() if ok else NetworkTaskResult.error(f"{self.name} failed to come online after {self.reboot_timeout} seconds")


class NodeSysctlReloadTask(TargetNodes, NetworkTask):
    """Reload sysctl configuration on the node."""

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        with self._ctx.get_conn(self.name) as conn:
            conn.exec("/usr/bin/systemctl restart systemd-sysctl.service")
        return NetworkTaskResult.ok()


class NodeNetworkManagerRestartTask(TargetNodes, NetworkTask):
    """Reload NetworkManager connections on the node."""

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        with self._ctx.get_conn(self.name) as conn:
            conn.exec("/usr/bin/nmcli con reload")
        return NetworkTaskResult.ok()


class NodeIfaceReloadTask(TargetNodes, NetworkTask):
    """Reload interfaces on the node by bringing them down and up."""

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        obj = self._ctx.network_config.get_raw_object(self.name)
        interfaces = obj.get("interfaces", [])
        if not interfaces:
            return NetworkTaskResult.ok()

        with self._ctx.get_conn(self.name) as conn:
            with conn.shell_session() as sh:
                for iface in interfaces:
                    # TODO: this should probably go through network manager
                    sh.exec(f"/usr/sbin/ifdown {iface['name']}")
                    sh.exec(f"/usr/sbin/ifup {iface['name']}")
        return NetworkTaskResult.ok()


class NodeNMConnectionCleanupNetworkTask(TargetNodes, NetworkTask):
    """ Cleanup duplicate NetworkManager connections for 100g interfaces """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        obj = self._ctx.network_config.get_raw_object(self.name)
        infs = obj.get("interfaces", [])
        if not infs:
            return NetworkTaskResult.ok()

        with self._ctx.get_conn(self.name) as conn:
            with conn.shell_session() as sh:
                for inf in infs:
                    # TODO: err checking
                    sh.exec(f"nmcli c show | grep {inf['name']} | grep System | awk '{{print $3}}' | xargs nmcli con delete")
        return NetworkTaskResult.ok()


class NodeLLDPNetworkTask(TargetNodes, NetworkTask):

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        with self._ctx.get_conn(self.name) as conn:
            rv, stdout, stderr = conn.exec("/usr/sbin/lldpctl -f json")
        if rv != 0:
            return NetworkTaskResult.error(f"failed to execute lldpctl: {rv} {stdout} {stderr}")
        return NetworkTaskResult.ok(data=json.loads(stdout))

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        obj = self._ctx.network_config.get_raw_object(self.name)
        obj_interfaces = obj.get("interfaces", [])

        lldp_neighbors = []
        inf_remote = parse_lldp_doc(result.data)
        for inf, remote in inf_remote.items():
            # If the interface name is not recognized, ignore it
            # because we will raise an exception for it in the
            # build_configs step anyway
            if inf not in _WELL_KNOWN_NODE_INTERFACES:
                logger.debug(f"unrecognized interface name {inf} for node {self.name}")
                continue
            remote_name, port, port_type = remote.get("name"), remote.get("port_id"), remote.get("port_type")

            sw = self._ctx.network_config.get_entity(remote_name)
            if not sw or sw.type != OBJ_TYPE_SW:
                continue

            if sw.obj.get("vendor") == "edgecore" and port_type == "local":
                port = parse_edgecore_port_alias(port)

            lldp_neighbors.append(dict(
                iface_name=inf,
                switch_name=remote_name,
                switch_port=port,
            ))

        for n in lldp_neighbors:
            # Adding an empty interfaces this deep in the loop in
            # order to avoid changing the object if there's
            # nothing to do.
            for tp_iface_obj in obj_interfaces:
                if tp_iface_obj["name"] == n["iface_name"]:
                    break
            else: # new interface
                tp_iface_obj = dict(name=n['iface_name'])
                obj_interfaces.append(tp_iface_obj)

            tp_iface_obj["switch_name"] = n["switch_name"]
            tp_iface_obj["switch_port"] = n["switch_port"]

        self._ctx.network_config.update_raw_object(self.name, dict(interfaces=obj_interfaces))
        return NetworkTaskResult.ok()

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        """
        If interfaces already have a switch name / port, then return true indicating that this exception can be ignored.
        Useful in cases where previously deployed nodes are down and we don't want to fail the entire generate process.
        """
        obj = self._ctx.network_config.get_raw_object(self.name)
        for inf in obj.get("interfaces", []):
            if "switch_name" not in inf or "switch_port" not in inf:
                return NetworkTaskResult.error("failed to call lldp on node and some interfaces are missing "
                                               f"'switch_name' or 'switch_port', original error: {result.message}")
        return NetworkTaskResult.ok()


class NodeUploadFilesNetworkTask(TargetNodes, NetworkTask):

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        cfg_dir = self._ctx.get_config_dir(self.name)

        if not cfg_dir.is_dir():
            return NetworkTaskResult.error(f"config directory '{cfg_dir}' does not exist")

        summary = []
        with self._ctx.get_conn(self.name) as conn:
            conn.scp_files(walk_conf_files(str(cfg_dir.absolute())))
        return NetworkTaskResult.ok(summary)


ICMP_HEADER_SIZE = 8
IP_HEADER_SIZE = 20
DEFAULT_MTU = -1
DEFAULT_NODE_MTU = 9000
DEFAULT_SYSTEM_MTU = 8150


class NodePingTestNetworkTask(TargetNodes, NetworkTask):
    def __init__(self, ctx: NetworkCfgCtx, name: str, generator: NodePingTestGenerator, test_kinds: List[PingTestKind], outfile=None, mtu=DEFAULT_MTU, *args, **kwargs):
        super().__init__(ctx, name)
        self._gen = generator
        self._kinds = test_kinds
        self._mtu = int(mtu) if mtu is not None else 0

        if outfile:
            self._outfile = outfile
        else:
            self._outfile = f"/tmp/ping_check_{int(time.time())}.txt"
        self._outfile_ignored = f"{self._outfile}.ignored"

    def get_mtu_opts(self, target_kind: str) -> str:
        if self._mtu == 0:
            return ""
        if self._mtu == DEFAULT_MTU:
            mtu = DEFAULT_NODE_MTU if target_kind != "system" else DEFAULT_SYSTEM_MTU
        else:
            mtu = self._mtu
        packet_size = mtu - ICMP_HEADER_SIZE - IP_HEADER_SIZE
        return f"-M do -s {packet_size}"

    @staticmethod
    def _parse_output(targets: List[PingTest], output: str):
        failures_src_dst_ips = set()
        for line in output.splitlines():
            if "->" not in line:
                continue
            src, dst = line.split("->")
            failures_src_dst_ips.add((src.strip(), dst.strip(),))
        for tgt in targets:
            if (tgt.src_ip, tgt.dst_ip,) in failures_src_dst_ips:
                tgt.ok = False
                tgt.details = "ping failed"
            else:
                tgt.ok = True

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        targets = self._gen.get_tests(self.name, self._kinds)
        if not targets:
            return NetworkTaskResult.ok([])
        try:
            with self._ctx.get_conn(self.name) as conn:
                with conn.shell_session() as sh:
                    sh.exec(f"touch {self._outfile}")
                    for tgt in targets:
                        packet_opts = self.get_mtu_opts(tgt.kind)
                        ip = tgt.dst_ip
                        local_ip = tgt.src_ip
                        src = f'-I {local_ip}' if local_ip else ''
                        ping_cmd = f'ping -c 1 -W 2 {packet_opts} {src} {ip} 2>&1>/dev/null'
                        sh.exec(
                            f"if ! {{ {ping_cmd} || {{ sleep 0.05 && {ping_cmd} ; }} ; }} ; "
                            f"then echo '{local_ip} -> {ip}' >> {self._outfile}; fi"
                        )
                    output = sh.exec(f"cat {self._outfile}")
                    sh.exec(f"rm -f {self._outfile}")
            self._parse_output(targets, output)
            return NetworkTaskResult.ok(targets)
        except paramiko.ssh_exception.SSHException as e:
            for tgt in targets:
                tgt.ok = False
                tgt.details = f"ssh to {self.name} failed: {e}"
            return NetworkTaskResult.ok(targets)


class NodeInterfaceDiscoverNetworkTask(TargetNodes, NetworkTask):
    """
    Get mellanox interfaces on a node using ethtool
    """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        """
        Print output like:
        IFNAME DRIVER
        """
        if_filter = "ip link show | egrep -v 'link|lo:|lxc|cilium' | awk {'print $2'} | sed 's/://'"
        cmd = f'for i in $({if_filter}); do l=`ethtool -i $i|grep "driver"`; echo $i $l; done'
        with self._ctx.get_conn(self.name) as conn:
            rv, stdout, stderr = conn.exec(cmd)
        if rv == 0:
            return NetworkTaskResult.ok(stdout)
        return NetworkTaskResult.error(f"returncode={rv}, stderr={stderr}")

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        """
        Add an interface entry to the node object if the interface is a mlx interface and isn't already present
        """
        obj = self._ctx.network_config.get_raw_object(self.name)
        interfaces = obj.get("interfaces", [])
        current_if_names = [i['name'] for i in interfaces]
        for l in result.data.split('\n'):
            words = l.split()
            if not words:
                continue
            if "mlx" in l and words[0] not in current_if_names and words[0] in _WELL_KNOWN_NODE_INTERFACES:
                interfaces.append({"name": words[0]})
        self._ctx.network_config.update_raw_object(self.name, {'interfaces': interfaces})
        return NetworkTaskResult.ok()


class NodeInterfaceCheckNetworkTask(TargetNodes, NetworkTask):
    def run(self, *args, **kwargs) -> NetworkTaskResult:
        """
        Print output like:
        IFNAME LINK_STATE ADDR?
        """
        obj = self._ctx.network_config.get_raw_object(self.name)
        interfaces = obj.get("interfaces", [])
        if_string = " ".join([i["name"] for i in interfaces])
        cmd = str(
            f"for i in {if_string};do "
            "l=$(ethtool $i|grep 'Link detected'|awk '{print $NF}'); "
            "a=$(ip -j -f inet addr show $i | jq -r '.[0].addr_info[0].local'); "
            "echo $i $l $a; "
            'done'
        )
        with self._ctx.get_conn(self.name) as conn:
            rv, stdout, stderr = conn.exec(cmd)
        if rv == 0:
            return NetworkTaskResult.ok(stdout)
        return NetworkTaskResult.error(f"returncode={rv}, stderr={stderr}")

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        """
        Update the node's interfaces 'state' field as up or down depending on if the link shows as up in ethtool and
        if it has the IP address specified in the network config
        """
        obj = self._ctx.network_config.get_raw_object(self.name)
        interfaces = obj.get("interfaces", [])
        if_health = dict()
        for l in result.data.splitlines():
            words = l.split()
            if not words:
                continue
            if len(words) == 3:
                iface, state, addr = words
            else:
                addr = None
                iface, state = words
            if state.strip().lower() != "yes":
                if_health[iface] = "down"
            else:
                for i in interfaces:
                    if i['name'] == iface:
                        expected_address = i.get("address")
                        if expected_address:
                            expected_address = expected_address.split("/")[0]
                            if_health[iface] = "up" if expected_address == addr else "down"
                        else:
                            if addr and addr != "null":
                                logger.warning(f"discovered an IP address on {self.name}:{iface} ({addr}) "
                                               "but network config did not have this recorded")
                            if_health[iface] = "up"
        for i in interfaces:
            if i['name'] in if_health:
                i['state'] = if_health[i['name']]
        self._ctx.network_config.update_raw_object(self.name, {'interfaces': interfaces})
        return NetworkTaskResult.ok()

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        """ if we can't determine the state, assume it's down """
        obj = self._ctx.network_config.get_raw_object(self.name)
        interfaces = obj.get("interfaces", [])
        for i in interfaces:
            i['state'] = "down"
        self._ctx.network_config.update_raw_object(self.name, {'interfaces': interfaces})
        return NetworkTaskResult.error("unable to reach node, assuming all interfaces are down")


class NodeInterfaceSpeedNetworkTask(TargetNodes, NetworkTask):
    """
    Read speed of all data interfaces on the node and update network config
    """

    def run(self, *args, **kwargs) -> NetworkTaskResult:
        # skip if not a swarmx or ax node, though we could do all the nodes, it's not super important to care
        entity = self._ctx.network_config.get_entity(self.name)
        if entity.role not in ("activation", "swarmx", "inferencedriver"):
            return NetworkTaskResult.ok({})

        obj = self._ctx.network_config.get_raw_object(self.name)
        interfaces = obj.get("interfaces", [])
        if_speed = {}
        with self._ctx.get_conn(self.name) as conn:
            for intf in interfaces:
                speed = -1
                rv, stdout, stderr = conn.exec(f"cat /sys/class/net/{intf['name']}/speed")
                if rv == 0:
                    try:
                        speed = int(int(stdout.strip()) / 1000)  # mbps -> gbps
                    except ValueError:
                        pass
                if speed == -1:
                    return NetworkTaskResult.error(f"unable to determine speed for interface {intf['name']}: stdout={stdout}, stderr={stderr}")
                if_speed[intf['name']] = speed
        return NetworkTaskResult.ok(if_speed)

    def process(self, result: NetworkTaskResult) -> NetworkTaskResult:
        obj = self._ctx.network_config.get_raw_object(self.name)
        interfaces = obj.get("interfaces", [])
        for if_name, speed in result.data.items():
            for i in interfaces:
                if i['name'] == if_name:
                    i['gbps'] = speed
                    break
        return NetworkTaskResult.ok()

    def recover(self, result: NetworkTaskResult) -> NetworkTaskResult:
        obj = self._ctx.network_config.get_raw_object(self.name)
        for inf in obj.get("interfaces", []):
            if "gbps" not in inf:
                return result
        return NetworkTaskResult.ok()