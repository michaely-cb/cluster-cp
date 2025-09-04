"""
Classes and functions to enable interaction with the network_config tool
"""
import ipaddress
import json
import logging
import os
import tempfile
from typing import Dict, List, Optional, Tuple

from deployment_manager.common.lock import LOCK_NETWORK, with_lock
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceTypes
from deployment_manager.db.models import ClusterDevice, Device
from deployment_manager.network_config.schema import NetworkConfigSchema
from deployment_manager.network_config.tasks import TaskRunner
from deployment_manager.network_config.tasks.switch import SwitchPingCheckAPITask
from deployment_manager.network_config.tasks.utils import PingTest
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.const import CLUSTER_DEPLOYMENT_BASE, CLUSTER_DEPLOYMENT_PYTHON
from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node
from deployment_manager.tools.utils import exec_cmd

logger = logging.getLogger(__name__)


class NetworkConfigTool:
    """
    Class which abstracts network_config tool functionality
    """

    base_cmd = f"{CLUSTER_DEPLOYMENT_PYTHON} -m network_config.tool"
    cwd = f"{CLUSTER_DEPLOYMENT_BASE}/deployment/deployment_manager"

    def __init__(self, network_config_file: str, env: Optional[dict] = None, profile: Optional[str] = None):
        self._cfg_file = network_config_file
        self.env = env if env else {"PYTHONPATH": self.cwd}
        self.profile = profile

    @classmethod
    def for_profile(cls, profile: str) -> 'NetworkConfigTool':
        cg = ConfGen(profile)
        profile_cfg = cg.parse_profile(False)
        env = os.environ.copy()
        env["PYTHONPATH"] = env.get("PYTHONPATH", "") + f":{CLUSTER_DEPLOYMENT_BASE}/deployment/deployment_manager"
        env_passwords = profile_cfg.get("passwords", {})
        if "node_password" in env_passwords:
            env["NODE_PASSWORD"] = env_passwords["node_password"]
        if "switch_password" in env_passwords:
            env["SWITCH_PASSWORD"] = env_passwords["switch_password"]
        return cls(cg.network_config_file, env, profile)

    def _run_cmd(self, cmd, throw=False) -> Tuple[int, str, str]:
        return exec_cmd(
            f"{self.base_cmd} {cmd}",
            cwd=self.cwd,
            env=self.env,
            throw=throw,
        )

    def nw_config(self) -> dict:
        try:
            with open(self._cfg_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"{self._cfg_file} does not exist")
            return dict()

    def must_run_cmd(self, cmd) -> Tuple[int, str, str]:
        return self._run_cmd(cmd, throw=True)

    def get_leaf_switches(self) -> List:
        return [s for s in self.nw_config().get('switches', []) if s['tier'] == 'LF']

    def get_spine_switches(self) -> List:
        return [s for s in self.nw_config().get('switches', []) if s['tier'] == 'SP']

    def init_network_config(self, cluster_name):
        ret, _, err = self._run_cmd(f"new_config -c {self._cfg_file} -n {cluster_name}")
        if ret:
            logger.error(f"Network config init failed with {ret}: {err}")
        return ret

    def run_sanity_check(self) -> dict:
        """ Return parsed network sanity json output or raise an exception if failed to run """
        ret, out, err = self._run_cmd(f"sanity -c {self._cfg_file}")
        ex = None
        if out:
            try:
                return json.loads(out)
            except Exception as e:
                ex = e
        logger.error(f"network sanity failed: out={out}, err={err}")
        if ex:
            raise RuntimeError(f"failed to parse sanity output, check logs: {ex}")
        raise RuntimeError(f"network sanity failed to produce output, stderr={err}")

    def _add_node(self, node_name, node_type):
        ret, _, err = self._run_cmd(
            f"add_node -c {self._cfg_file} -s {node_type} -n {node_name}"
        )
        if ret:
            logger.error(f"Failed to add node, {ret}: {err}")
        return ret

    def add_management_node(self, node_name):
        return self._add_node(node_name, 'management_nodes')

    def add_memoryx_node(self, node_name):
        return self._add_node(node_name, 'memoryx_nodes')

    def add_worker_node(self, node_name):
        return self._add_node(node_name, 'worker_nodes')

    def add_swarmx_node(self, node_name):
        return self._add_node(node_name, 'swarmx_nodes')

    def add_activation_node(self, node_name):
        return self._add_node(node_name, 'activation_nodes')

    def add_system(self, system_name):
        ret, _, err = self._run_cmd(
            f"add_system -c {self._cfg_file} -n {system_name}"
        )
        return ret

    def discover_switches(self):
        ret, _, err = self._run_cmd(
            f"node_tasks -c {self._cfg_file} discover_switches"
        )
        if ret:
            logger.error(f"Failed to discover switches, {ret}: {err}")
        return ret

    def read_switch_models(self):
        ret, _, err = self._run_cmd(
            f"switch_tasks -c {self._cfg_file} -p \"$SWITCH_PASSWORD\" model"
        )

    def discover_connections(self):
        self._run_cmd(
            f"system_tasks -c {self._cfg_file} -u admin -p admin port_discover"
        )
        self._run_cmd(
            f"node_tasks -c {self._cfg_file} lldp"
        )
        self._run_cmd(
            f"node_tasks -c {self._cfg_file} -u root -p \"$NODE_PASSWORD\" lldp"
        )
        self._run_cmd(
            f"switch_tasks -c {self._cfg_file} -u admin -p \"$SWITCH_PASSWORD\" lldp"
        )
        self._run_cmd(
            f"system_iface_from_tier -c {self._cfg_file}"
        )
        self._run_cmd(
            f"system_tasks -c {self._cfg_file} -u admin -p admin ip_discover"
        )

    def get_switch_firmware_versions(self):
        ret, _, err = self._run_cmd(
            f"switch_tasks -c {self._cfg_file} -p \"$SWITCH_PASSWORD\" firmware_version"
        )
        fw_versions = dict()
        with open(self._cfg_file, 'r') as f:
            cfg = json.load(f)
            for s in cfg.get("switches", []):
                fw_versions[s["name"]] = s.get("firmware_version", "")
        return fw_versions

    def get_switch_roce_status(self):
        ret, _, err = self._run_cmd(
            f"switch_tasks -c {self._cfg_file} -p \"$SWITCH_PASSWORD\" roce_status"
        )
        roce_status = dict()
        with open(self._cfg_file, 'r') as f:
            cfg = json.load(f)
            for s in cfg.get("switches", []):
                rs = s.get("roce_status")
                if rs == "enabled":
                    rs = True
                elif rs == "disabled":
                    rs = False
                else:
                    rs = None
                roce_status[s['name']] = rs
        return roce_status

    def get_switch_vendors(self):
        vendors = dict()
        with open(self._cfg_file, 'r') as f:
            cfg = json.load(f)
            for s in cfg.get("switches", []):
                vendors[s["name"]] = s.get("vendor", "")
        return vendors

    def get_switch_models(self):
        models = dict()
        with open(self._cfg_file, 'r') as f:
            cfg = json.load(f)
            for s in cfg.get("switches", []):
                models[s["name"]] = s.get("model", "")
        return models

    def enable_system_switch_bgp(self):
        output_dir = "/tmp/bgp_config"
        ret, _, err = self._run_cmd(
            f"build_system_vip_bgp_configs -c {self._cfg_file} "
            f"-o {output_dir}"
        )
        if ret:
            logger.error(f"Failed to generate system BGP config {ret} - {err}")
        else:
            ret, _, err = self._run_cmd(
                f"switch_tasks -c {self._cfg_file} -b {output_dir} upload"
            )
            if ret:
                logger.error(f"Failed to upload system BGP config to switches {ret} - {err}")

        exec_cmd(f"rm -rf {output_dir}")
        return ret

    def disable_system_switch_bgp(self):
        output_dir = "/tmp/bgp_config"
        ret, _, err = self._run_cmd(
            f"build_system_vip_static_configs -c {self._cfg_file} "
            f"-o {output_dir}"
        )
        if ret:
            logger.error(f"Failed to generate system static config {ret} - {err}")
        else:
            ret, _, err = self._run_cmd(
                f"switch_tasks -c {self._cfg_file} -b {output_dir} upload"
            )
            if ret:
                logger.error(f"Failed to upload system static config to switches {ret} - {err}")

        exec_cmd(f"rm -rf {output_dir}")
        return ret

    def _set_switch_dcqcn(self, enable, switch_category,
                          ecn_min_threshold=None, ecn_max_threshold=None,
                          ecn_probability=None, ecn_delay=None, interface_type=None):
        output_dir = "/tmp/dcqcn_config"
        action = "enable" if enable else "disable"
        cmd = (
            f"build_switch_dcqcn_configs -c {self._cfg_file} "
            f"-o {output_dir} --action {action}"
        )
        cmd = f"{cmd} --switch_category {switch_category}"
        if enable:
            if ecn_min_threshold is not None:
                cmd = f"{cmd} --ecn_min_threshold {ecn_min_threshold}"
            if ecn_max_threshold is not None:
                cmd = f"{cmd} --ecn_max_threshold {ecn_max_threshold}"
            if ecn_probability is not None:
                cmd = f"{cmd} --ecn_probability {ecn_probability}"
            if ecn_delay is not None:
                cmd = f"{cmd} --ecn_delay {ecn_delay}"
        if interface_type is not None:
            cmd = f"{cmd} --interface_type {interface_type}"
        ret, _, err = self._run_cmd(cmd)
        if ret:
            logger.error(f"Failed to generate switch DCQCN config {ret} - {err}")
        else:
            cmd = f"switch_tasks -c {self._cfg_file} -b {output_dir}"
            switches = self.get_leaf_switches() if switch_category == 'leaf' else self.get_spine_switches()
            cmd = f"{cmd} -n {' -n '.join([s['name'] for s in switches])} upload"
            ret, _, err = self._run_cmd(cmd)
            if ret:
                logger.error(f"Failed to upload DCQCN config to switches {ret} - {err}")

        exec_cmd(f"rm -rf {output_dir}")
        return ret

    def enable_switch_dcqcn(self, switch_category,
                            ecn_min_threshold, ecn_max_threshold,
                            ecn_probability, ecn_delay, interface_type):
        return self._set_switch_dcqcn(
            True, switch_category, ecn_min_threshold,
            ecn_max_threshold, ecn_probability, ecn_delay, interface_type
        )

    def disable_switch_dcqcn(self, switch_category, interface_type):
        return self._set_switch_dcqcn(False, switch_category, interface_type=interface_type)

    @with_lock(LOCK_NETWORK)
    def network_device_removal(self, remove_devices: List[Device]):
        """
        Delete devices from network_config.json and Device database
        """
        server_names = ""
        switch_names = ""
        system_names = ""
        for d in remove_devices:
            if d.device_type == DeploymentDeviceTypes.SERVER.value:
                server_names += f" -n {d.name} "
            elif d.device_type == DeploymentDeviceTypes.SWITCH.value:
                switch_names += f" -n {d.name} "
            elif d.device_type == DeploymentDeviceTypes.SYSTEM.value:
                system_names += f" -n {d.name} "
        # servers
        if server_names:
            self._run_cmd(
                f"remove_node -c {self._cfg_file} {server_names}"
            )
        # switches
        if switch_names:
            self._run_cmd(
                f"remove_switch -c {self._cfg_file} {switch_names}"
            )
        # systems
        if system_names:
            self._run_cmd(
                f"remove_system -c {self._cfg_file} {system_names}"
            )
        # remove from db
        for d in remove_devices:
            d.delete()

    def switch_ping_check(self, switches: List[Device], destination_types=("xconnects",)) -> Dict[str, List[PingTest]]:
        """
        Generate and run ping checks for a list of switches to specified destination types.
        Only switches that are present in the network document are included in the returned dictionary otherwise no
        entry will be present for that switch.

        Parameters:
            switches: A list of switch devices for which to generate ping checks.
            destination_types: A tuple of destination types to include in the ping checks. For options, see
                destination_types in SwitchPingCheck

        Returns:
            Dict[str, List[PingTest]]: SwitchName -> list of completed `PingTest` instances
        """
        tasks = []
        nw = self.nw_config()
        nw_switches = {sw['name']: sw for sw in nw.get("switches", [])}
        for sw in switches:
            if sw.name not in nw_switches:
                continue
            u = sw.get_prop(props.prop_management_credentials_user)
            p = sw.get_prop(props.prop_management_credentials_password)
            tasks.append(
                SwitchPingCheckAPITask(
                    nw_switches[sw.name], u, p, nw,
                )
            )
        runner = TaskRunner(tasks)
        return runner.call_api_tasks(*destination_types)

    @with_lock(LOCK_NETWORK)
    def push_links(self, config_dir, switch_pairs: List[str]):
        """ Push links for xconnects. Should be specified as SwitchA:SwitchB """
        ports_for = " ".join([f"--ports_for={p}" for p in switch_pairs])
        self._run_cmd(
            f"switch_tasks -c {self._cfg_file} {ports_for} --config_section switches -b {config_dir} upload",
            throw=True
        )

    def generate_nw_config_for_k8s(
        self,
        cluster_name: str = None,
    ) -> str:
        """
        Generate a network_config.json without nodes and systems that haven't gone through pb2 validation.
        Devices for whom the prop kubernetes.ignore==True are ignored as well.
        If the node is in target k8s cluster but hasn't gone through pb2 validation,
        it will still be included in the generated network_config.json to avoid accidental removal from k8s during pb3.

        cluster_name is for filtering required devices (for BG upgrade) and determines
        which cluster configuration to overlay onto the top-level fields.

        Returns
            name of the file where the network_config is dumped
        """

        def _ignore_for_k8s(device):
            return device.get_prop(props.prop_kubernetes_ignore) is True

        nw = self.nw_config()
        dnode = get_k8s_lead_mgmt_node(self.profile, cluster_name)
        if not dnode:
            raise RuntimeError(
                "Kubernetes deployment node not found. Please ensure the deployment node is set up correctly."
            )
        user = dnode.get_prop(props.prop_management_credentials_user)
        password = dnode.get_prop(props.prop_management_credentials_password)
        with KubernetesCtl(self.profile, dnode.name, user, password) as kctl:
            already_in_k8s_names = kctl.get_cluster_device_names()

        # Get devices in the cluster (filtered by cluster_name if provided)
        query = ClusterDevice.objects.filter(device__profile__name=self.profile)
        if cluster_name is not None:
            query = query.filter(cluster__name=cluster_name)
        # Get network-pushed devices that aren't marked to ignore
        all_devices = ([cd.device for cd in query] +
                       list(
                           Device.objects.filter(
                               profile__name=self.profile,
                               device_type=DeploymentDeviceTypes.SWITCH.value
                           )
                       ))
        all_cluster_device_names = {d.name for d in all_devices}
        valid_device_names = {
            device.name for device in all_devices
            if device.network_pushed() and not _ignore_for_k8s(device)
        }

        def filter_devices(doc: dict, key: str):
            if key not in doc:
                return
            filtered_objects = []
            for obj in doc.get(key, []):
                obj_name = obj['name']
                # Include if already in target k8s cluster
                if obj_name in already_in_k8s_names and obj_name in all_cluster_device_names:
                    filtered_objects.append(obj)
                    continue
                # Include if valid device with all valid interfaces
                # allow excluding a switch and any devices connected to that switch
                if obj_name in valid_device_names:
                    valid_device = True
                    for interface in obj.get("interfaces", []):
                        switch_name = interface.get("switch_name")
                        if (switch_name and
                            switch_name not in valid_device_names and
                            switch_name not in already_in_k8s_names):
                            valid_device = False
                            break
                    if valid_device:
                        filtered_objects.append(obj)
            doc[key] = filtered_objects

        # Apply filtering to all relevant sections
        for section in [*NetworkConfigSchema.node_sections, "systems", "switches"]:
            filter_devices(nw, section)

        # Overlay cluster-specific configuration to top-level fields
        clusters = {c["name"]: c for c in nw.get("clusters", [])}
        assert len(clusters) < 3, "Network config must not contain more than 2 clusters"

        # Split virtual address ranges if multiple clusters or specific cluster found
        logger.debug(
            f"generate_nw_config_for_k8s CALLED: cluster_name={cluster_name}, "
            f"clusters={list(clusters.keys())}"
        )

        if len(clusters) > 1:
            if not cluster_name in clusters:
                raise ValueError(f"cluster '{cluster_name}' was not present in network_config.json but must be. "
                                 "This is likely a manual misconfiguration")
            cluster = clusters[cluster_name]
            v_addr_idx = cluster.get("data_network", {}).get("virtual_addr_index")
            logger.debug(f"Splitting virtual addresses with virtual_addr_index={v_addr_idx}")
            nw["name"] = cluster_name

            # Overlay the multus virtual address range
            nw["switches"] = [_split_vaddr_range(s, v_addr_idx) for s in nw.get("switches", [])]

            # Overlay cluster_data_vip from target cluster's data_network
            if "data_network" in cluster:
                data_network = cluster["data_network"]
                if "vip" in data_network:
                    nw["cluster_data_vip"] = {"vip": data_network["vip"]}

            # Overlay cluster_mgmt_vip from target cluster's mgmt_network
            if "mgmt_network" in cluster:
                mgmt_network = cluster["mgmt_network"]
                if "cluster_mgmt_vip" not in nw:
                    nw["cluster_mgmt_vip"] = {}
                for key in ["vip", "node_asn", "router_asn"]:
                    if key in mgmt_network:
                        nw["cluster_mgmt_vip"][key] = mgmt_network[key]

        else:
            logger.debug(
                f"NOT splitting virtual address range: len(clusters)={len(clusters)}, "
                f"has_switches={'switches' in nw}, {cluster_name}"
            )

        outfile = tempfile.mktemp('network_config')
        with open(outfile, 'w') as f:
            json.dump(nw, f, indent=4)
        return outfile


def _split_vaddr_range(switch: dict, target_cluster_index: int) -> dict:
    assert target_cluster_index in (0, 1), "Missing required field virtual_addr_index"

    vaddr = switch.get("virtual_addrs")
    if not vaddr or "starting_address" not in vaddr or "ending_address" not in vaddr:
        return switch

    start, prefixlen = vaddr["starting_address"].split("/")
    start = ipaddress.ip_address(start)
    end = ipaddress.ip_address(vaddr["ending_address"].split("/")[0])
    first_half_end = start + ((int(end) - int(start)) // 2)
    if target_cluster_index == 0:
        vaddr["ending_address"] = str(first_half_end) + "/" + prefixlen
    else:
        vaddr["starting_address"] = str(first_half_end + 1) + "/" + prefixlen
    logger.debug(f"{switch['name']} virtual address split: {start}/{prefixlen}-{end}/{prefixlen} -> "
                 f"{vaddr['starting_address']}-{vaddr['ending_address']}")
    return switch