"""
Base classes needed by the deployment modules
"""
import json
import logging
import os
import time

from abc import ABCMeta, abstractmethod
from typing import Optional, Any, Tuple, List

from yaml import safe_load

logger = logging.getLogger(__name__)

def get_obj_name(obj):
    """
    Return the network-reachable name of an object in the deployment config
    """
    return obj.get("mgmt_host") or obj["name"]

def get_obj_credentials(obj):
    creds = obj.get('login', {})
    if 'rootLogin' in obj:
        creds = obj['rootLogin']
    return dict(
        username=creds.get('username'),
        password=creds.get('password')
    )

def get_obj_nodegroup(obj):
    return obj.get("nodeGroup", 0)

def get_obj_rack(obj):
    rack_info = obj.get("rackUnit", {})
    return rack_info.get("rackNum"), rack_info.get("unitNum")

def get_obj_stamp(obj):
    return obj.get("stamp")


class DeploymentConfig:
    """ Interface to the deployment config file for the other modules
    """
    ARTIFACTS_DIR = "artifacts"
    DEFAULT_MTU = 9000
    DEFAULT_SWITCH_VENDOR = "arista"

    def __init__(self, fname, output_file=None, artifacts_dir=None):
        self._output_file = output_file
        self._artifacts_dir = artifacts_dir
        self._provision_artifacts_dir()
        self._fname = fname
        self.config = self._load_config()

        self.cluster_name = self.config["name"]
        self.network = self.get_section("network")
        self.inventory = dict(
            nodes=self.get_section("servers"),
            switches=self.get_section("switches"),
            systems=self.get_section("systems")
        )

        self.node_password = os.getenv("NODE_PASSWORD",
                            default="")
        self.switch_password = os.getenv("SWITCH_PASSWORD",
                            default="")

    @property
    def artifacts_dir(self):
        """ Directory in which output files of the L3 cfg tool will be saved
        """
        if self._artifacts_dir:
            return self._artifacts_dir
        else:
            path = os.path.dirname(os.path.realpath(__file__))
            return f"{path}/{self.ARTIFACTS_DIR}"

    def _provision_artifacts_dir(self):
        os.makedirs(self.artifacts_dir, exist_ok=True)

    @property
    def root_server(self):
        """ Server from which deployment is running
        """
        rs = self.config.get("misc", {}).get(
                "rootServer")
        if rs is None:
            rs = self.management_nodes[0]["name"]
        return rs

    @property
    def root_server_ip(self):
        """ Server ip from which deployment is running
        """
        rs = self.config.get("misc", {}).get("rootServerIp")
        if rs is None and self.management_nodes:
            logger.warning("Root server IP not set, using first management node IP")
            return self.management_nodes[0]["address"]
        return rs

    @property
    def mgmt_vip(self) -> Optional[dict]:
        """ Cluster mgmt vip config """
        return self.config["network"].get("managementNetwork",{}).get("mgmtVip")

    @property
    def network_topology(self):
        """ 100G connection topology
        """
        return self.config.get("misc", {}).get(
                "topology")

    @property
    def cluster_prefix(self):
        """ IP subnet for the cluster
        """
        if self.network is None:
            return None
        return self.network.get(
                "clusterNetwork", {}).get(
                "ipConfig", {}).get("ipSuperCidr")

    @property
    def overlay_prefixes(self):
        """ CIDRs for the overlay network
        """
        if self.network is None:
            return None
        return self.network.get(
            "managementNetwork", {}).get(
            "podCidrs")

    @property
    def service_prefix(self):
        """ CIDR for the service network
        """
        if self.network is None:
            return None
        return self.network.get(
            "managementNetwork", {}).get(
            "serviceCidr")

    @property
    def exterior_prefixes(self):
        """ List of exterior CIDRs
        """
        if self.network is None:
            return []
        return self.network.get(
            "exteriorNetwork", {}).get(
            "prefixes", [])

    @property
    def cluster_asns(self):
        """ List of ASN ranges in the cluster
        """
        if self.network is None:
            return []
        return self.network.get(
            "clusterNetwork", {}).get(
            "clusterAsns", [])

    @property
    def vip_prefix(self):
        """ IP subnet for system VIPs
        """
        if self.network is None:
            return None
        return self.network.get(
                "clusterNetwork", {}).get(
                "ipConfig", {}).get("ipVipCidr")

    @property
    def system_prefix(self):
        """ IP subnet for system IPs
        """
        if self.network is None:
            return None
        return self.network.get(
                "clusterNetwork", {}).get(
                "ipConfig", {}).get("ipSystemCidr")

    @property
    def aw_prefix(self):
        """ IP subnet for memx
        """
        if self.network is None:
            return None
        return self.network.get(
                "clusterNetwork", {}).get(
                "ipConfig", {}).get("ipMemxCidr")

    @property
    def br_prefix(self):
        """ IP subnet for swarmx
        """
        if self.network is None:
            return None
        return self.network.get(
                "clusterNetwork", {}).get(
                "ipConfig", {}).get("ipSwarmxCidr")

    @property
    def xconn_prefix(self):
        """ IP subnet for cross-connect IPs
        """
        if self.network is None:
            return None
        return self.network.get(
                "clusterNetwork", {}).get(
                "ipConfig", {}).get("ipCrossConnectCidr")

    def get_switch_ip_block(self, rack):
        """ Return IP block provided for switch in rack, if given
        """
        if self.network is None or rack is None:
            return None
        for block in self.network.get(
                    "clusterNetwork", {}).get(
                    "clusterIpBlocks", []):
            if block["rack"] == int(rack):
                return block["ipBlock"]
        else:
            return None

    def get_system_switch_ip_block(self, rack):
        """ Return IP block provided for systems on the switch in rack, if given
        """
        if self.network is None or rack is None:
            return None
        for block in self.network.get(
                    "clusterNetwork", {}).get(
                    "clusterSystemIpBlocks", []):
            if block["rack"] == int(rack):
                return block["ipBlock"]
        else:
            return None

    def get_swarmx_switch_ip_block(self, rack):
        """ Return IP block provided for swarmx on the switch in rack, if given
        """
        if self.network is None or rack is None:
            return None
        for block in self.network.get(
                    "clusterNetwork", {}).get(
                    "clusterSwarmxIpBlocks", []):
            if block["rack"] == int(rack):
                return block["ipBlock"]
        else:
            return None

    def _load_config(self):
        with open(self._fname, "r") as f:
            return safe_load(f)

    def get_section(self, section, strict=False):
        """ Returns given section from the deployment config
        """
        sect = self.config.get(section)
        if strict and sect is None:
            raise RuntimeError(f"{section} does not exist in the deployment config")
        return sect

    @property
    def network_config_filename(self):
        """ Configuration file generated by network_config.tool
        """
        if self._output_file:
            return self._output_file
        else:
            return (
                f"{self.artifacts_dir}/{self.cluster_name}_network_config.json"
            )

    def create_tmp_config(self) -> dict:
        """ Returns a minimal network config with node and switch entries """
        tmp_cfg = {
            "name": "tmp_cfg",
            "management_nodes": [],
            "memoryx_nodes": [],
            "worker_nodes": [],
            "swarmx_nodes": [],
            "user_nodes": [],
            "activation_nodes": [],
            "switches": [],
        }
        for ncat, nodes in {
            "management_nodes": self.management_nodes,
            "worker_nodes": self.worker_nodes,
            "user_nodes": self.user_nodes,
            "memoryx_nodes": self.memoryx_nodes,
            "swarmx_nodes": self.swarmx_nodes,
            "activation_nodes": self.activation_nodes
        }.items():
            for n in nodes:
                creds = get_obj_credentials(n)
                node_obj = {"name": get_obj_name(n)}
                if creds.get("username") and creds.get("password"):
                    node_obj["username"] = creds["username"]
                    node_obj["password"] = creds["password"]
                tmp_cfg[ncat].append(node_obj)

        for s in self.aw_switches + self.br_switches + self.leaf_switches + self.spine_switches:
            creds = get_obj_credentials(s)
            tmp_cfg["switches"].append({
                "name": get_obj_name(s),
                "vendor": DeploymentConfig.get_switch_vendor(s),
                "tier": "AW",
                "tier_pos": 0,
                "username": creds["username"],
                "password": creds["password"]
            })
        return tmp_cfg

    @property
    def exterior_connection_filename(self):
        """
        File containing uplink/downlink info.
        This is generated based on deployment config.
        """
        return f"{self.artifacts_dir}/{self.cluster_name}_exterior.txt"

    @property
    def output_directory(self):
        """ Directory where config files to be uploaded will be placed
        """
        return f"{self.artifacts_dir}/{self.cluster_name}_out"

    @property
    def uplinks(self):
        """ Uplink information from deployment config
        """
        u = []
        if self.network is not None:
            for ul in self.network.get("clusterNetwork", {}).get("uplinks", []):
                u.append({
                    "aw_switch": ul["memxSwitch"],
                    "aw_switch_port": ul["memxSwitchPort"],
                    "uplink_ip": ul["uplinkIp"],
                    "downlink": {
                        "switch_name": ul["extSwitchName"],
                        "switch_port": ul["extSwitchPort"],
                        "downlink_ip": ul["extDownlinkIp"]
                    }
                })

        return u

    @property
    def switches(self):
        """ Memx and swarmx switches specified in the inventory
        """
        s = None
        if self.inventory is not None:
            s = self.inventory.get("switches")
        return s

    @property
    def exterior_switches(self):
        """ Exterior switches specified in the inventory
        """
        e = []
        e_names = []
        for u in self.network.get("clusterNetwork", {}).get("uplinks", []):
            if (u["extSwitchName"], u.get("extSwitchAsn", "")) not in e_names:
                e_names.append((u["extSwitchName"], u.get("extSwitchAsn", "")))
        for en, ea in e_names:
            e.append(dict(
                name=en,
                asn=ea,
                mtu=self.DEFAULT_MTU,
                vendor=self.DEFAULT_SWITCH_VENDOR
            ))
        return e

    @classmethod
    def get_switch_vendor(cls, switch):
        v = switch.get("vendor")
        if "AR" in v:
            return "arista"
        elif "HP" in v:
            return "hpe"
        elif "DL" in v:
            return "dell"
        elif "EC" in v:
            return "edgecore"
        elif "JU" in v:
            return "juniper"
        elif "CR" in v:
            return "credo"
        else:
            return cls.DEFAULT_SWITCH_VENDOR

    @classmethod
    def get_switch_management_addr(cls, switch):
        return switch.get("ip")

    @classmethod
    def get_switch_model(cls, switch):
        v = switch.get("vendor")
        if ':' in v:
            return v.split(':')[1]
        else:
            return ''

    def _get_switches(self, switch_type):
        sw = []
        for s in self.switches or []:
            if s["role"] == switch_type:
                sw.append(s)
        return sw

    @property
    def tiers(self):
        """ Switch prefix and connectivity information
        """
        t = dict()
        t["aw_size"] = self.network.get(
                    "clusterNetwork", {}).get(
                    "clusterMemxSubnetSize")
        t["aw_physical"] = self.network.get(
                    "clusterNetwork", {}).get(
                    "clusterMemxInterfaces", 0)
        t["br_physical"] = self.network.get(
                    "clusterNetwork", {}).get(
                    "clusterSwarmxInterfaces", 0)
        t["br_size"] = self.network.get(
                    "clusterNetwork", {}).get(
                    "clusterSwarmxSubnetSize")
        t["system_size"] = self.network.get(
                    "clusterNetwork", {}).get(
                    "clusterSystemSubnetSize")
        t["xconn_count"] = self.network.get(
                    "clusterNetwork", {}).get(
                    "crossConnects")
        t["vip_count"] = self.network.get(
                    "clusterNetwork", {}).get(
                    "vipCount", 1)
        return t

    @property
    def aw_switches(self):
        """ Memx switches
        """
        return self._get_switches("MX")

    @property
    def br_switches(self):
        """ Swarmx switches
        """
        return self._get_switches("SX")

    @property
    def leaf_switches(self):
        """ Leaf switches
        """
        return self._get_switches("LF")

    @property
    def spine_switches(self):
        """ Spine switches
        """
        return self._get_switches("SP")

    @property
    def systems(self):
        """ CS systems specified in the inventory
        """
        s = None
        if self.inventory is not None:
            s = self.inventory.get("systems")
        return s

    def get_system_mgmt_addr(self, system):
        mgmt_addr = None
        for p in system.get("ports", []):
            if p.get("name") == "MGMT":
                mgmt_addr = p.get("address")
                break
        return mgmt_addr

    @property
    def nodes(self):
        """ Nodes specified in the inventory
        """
        n = None
        if self.inventory is not None:
            n = self.inventory.get("nodes")
        return n

    def _get_nodes(self, node_type):
        ns = []
        for n in self.nodes or []:
            if n["role"] == node_type:
                ns.append(n)
        return ns

    @property
    def management_nodes(self):
        """ Management nodes from the inventory
        """
        return self._get_nodes("MG")

    @property
    def user_nodes(self):
        """ User nodes from the inventory
        """
        return self._get_nodes("US")

    @property
    def worker_nodes(self):
        """ Worker nodes from the inventory
        """
        return self._get_nodes("WK")

    @property
    def swarmx_nodes(self):
        """ Swarmx nodes from the inventory
        """
        return self._get_nodes("SX")

    @property
    def memoryx_nodes(self):
        """ Memx nodes from the inventory
        """
        return self._get_nodes("MX")

    @property
    def activation_nodes(self):
        """ Activation nodes from the inventory
        """
        return self._get_nodes("AX")

    def is_user_node(self, node):
        """ Check if a node is a user node
        """
        return node["role"] == "US"

    @property
    def num_aw_switches(self):
        """ Count of memx switches
        """
        return len(self.aw_switches) if self.aw_switches else 0

    @property
    def num_br_switches(self):
        """ Count of swarmx switches
        """
        return len(self.br_switches) if self.br_switches else 0

    @property
    def num_leaf_switches(self):
        """ Count of leaf switches
        """
        return len(self.leaf_switches) if self.leaf_switches else 0

    @property
    def num_systems(self):
        """ Number of systems specified in the inventory
        """
        return len(self.systems) if self.systems else 0

    @property
    def system_connections_file(self) -> Optional[str]:
        return self.config["network"].get("clusterNetwork", {}).get("systemConnectionsFile")


class NetworkConfig:
    """ Interface to the network_config file
    """
    def __init__(self, fname: str):
        self._cfg = dict()
        try:
            with open(fname, 'r') as f:
                self._cfg = json.load(f)
        except:
            logger.exception(f"Unable to load {fname}")

        # nodes
        self.nodes = self._cfg.get("management_nodes", []) + \
                    self._cfg.get("memoryx_nodes", []) + \
                    self._cfg.get("worker_nodes", []) + \
                    self._cfg.get("swarmx_nodes", []) + \
                    self._cfg.get("user_nodes", []) + \
                    self._cfg.get("activation_nodes", [])
        self.node_names = [n['name'] for n in self.nodes]
        # systems
        self.systems = self._cfg.get("systems", [])
        self.system_names = [s['name'] for s in self.systems]
        # switches
        self.switches = self._cfg.get("switches", [])
        self.switch_names = [s['name'] for s in self.switches]
        self.exterior_switches = self._cfg.get("exterior_switches", [])
        self.exterior_switch_names = [s['name'] for s in self.exterior_switches]


class DeployCmd(metaclass=ABCMeta):
    """ Base class for all deployment commands """

    def __init__(self, deployment_config: DeploymentConfig, **kwargs):
        self.cfg = deployment_config
        self.network_config = NetworkConfig(self.cfg.network_config_filename)

    @property
    def name(self):
        """ Name of the deployment command
        """
        return ''

    @abstractmethod
    def run(self, dry_run=False) -> Tuple[int, Any, str]:
        """
        Execute the deployment command.
        Returns exit code, return value (arbitrary, depends on context) and a contextual message.
        """
        pass


class Deploy(DeployCmd, metaclass=ABCMeta):
    """ Base class for all commands the call ToolCommand """

    @abstractmethod
    def get_command(self):
        """ Returns an instance of ToolCommand
        """
        pass

    def run(self, dry_run=False) -> Tuple[int, Any, str]:
        """ Run network_config.tool command
        """
        exit_code = 0
        retval = None
        msg = None
        tc = self.get_command()
        if tc is not None:
            if not dry_run:
                start_time = time.time()
                logger.info(self.name)
                tc.run()
                retval = tc.retval
                duration = int(time.time() - start_time)
                if not tc.is_successful:
                    exit_code = 1
                    msg = (
                        f"Command {tc} returned {tc.retcode} ({duration}s)\n"
                        f"RETVAL: {tc.retval}\n"
                        f"STDERR: {tc.stderr}\n"
                    )
                    logger.error(msg)
                else:
                    msg = (
                        f"{tc} successful ({duration}s)\n"
                        f"RETVAL: {tc.retval}\n"
                        f"STDERR: {tc.stderr}\n"
                    )
                    logger.info(msg)
            else:
                logger.info(f"{self.name}: {tc}\n")
        return exit_code, retval, msg


class DeployIterator(DeployCmd, metaclass=ABCMeta):
    """ Base class to run ToolCommands in a loop
    """

    @abstractmethod
    def get_commands(self):
        """ Returns a list of ToolCommand instances
        """
        pass

    def run(self, dry_run=False) -> Tuple[int, List[Any], str]:
        """ Run network_config.tool commands in a loop
        """
        exit_code = 0
        retvals = list()
        msg = None

        for tc in self.get_commands():
            if tc is not None:
                if not dry_run:
                    start_time = time.time()
                    logger.info(tc.name)
                    tc.run()
                    retvals.append(tc.retval)
                    duration = int(time.time() - start_time)
                    if not tc.is_successful:
                        exit_code = 1
                        msg = (
                            f"Command {tc} returned {tc.retcode}\n"
                            f"RETVAL: {tc.retval}\n"
                            f"STDERR: {tc.stderr}\n"
                        )
                        logger.error(msg)
                        return exit_code, retvals, msg
                    else:
                        logger.info(
                            f"{tc} successful ({duration}s)\n"
                            f"RETVAL: {tc.retval}\n"
                            f"STDERR: {tc.stderr}\n"
                        )
                else:
                    logger.info(f"{self.name}: {tc}\n")

        return exit_code, retvals, msg
