import ipaddress
import itertools
import json
import logging
import pathlib
import socket
import yaml
from typing import List, Optional, Tuple, Type

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.models import Device
from deployment_manager.db import device_props as props
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.kubernetes.interface import KubernetesCtl
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node
from deployment_manager.tools.network_config_tool import (
    NetworkConfigTool
)
from deployment_manager.tools.switch.interface import SFlowStatus, SFlowConfig
from deployment_manager.tools.switch.switchctl import perform_group_action, create_client
from deployment_manager.tools.utils import prompt_confirm, ReprFmtMixin
from deployment_manager.cli.switch.interface import _GenericSwitchCmd, _gather_switches

logger = logging.getLogger(__name__)

def mgmt_node_run(profile: str, cmd: str) -> Tuple[int, str, str]:
    """
    Run a command on the lead management node of the k8s cluster for the given profile.
    Returns a tuple of (return code, stdout, stderr).
    """
    lead_mgmt_node = get_k8s_lead_mgmt_node(profile)
    if not lead_mgmt_node:
        raise ValueError(f"No lead management node found for profile {profile}")

    user = lead_mgmt_node.get_prop(props.prop_management_credentials_user)
    password = lead_mgmt_node.get_prop(props.prop_management_credentials_password)

    with KubernetesCtl(profile, lead_mgmt_node.name, user, password) as kctl:
        return kctl.run(cmd)  # type: ignore[return-value]

class Status(_GenericSwitchCmd):
    """Show sFlow status normalized across vendors."""
    name = "status"

    def class_params(self) -> Tuple[Type[ReprFmtMixin], str]:
        return SFlowStatus, "sflow_status"

    def post_process(self, results):
        return results


class Clear(SubCommandABC):
    """Clear all sFlow counters (e.g., packet samples counter)."""

    name = "clear"

    def construct(self):
        self.add_arg_filter(False)

    def run(self, args):
        switches = Device.get_all(self.profile).filter(device_type="SW", device_role__in=("SP", "SX", "MX", "LF"))
        switches = self.filter_devices(args, query_set=switches)

        infos = _gather_switches(self.profile, switches)
        if not infos:
            logger.info("nothing to configure")
            return 1

        # TODO: parallelize
        sw_cmds = {}
        for switch_info in infos:
            with create_client(switch_info) as client:
                sw_cmds[switch_info.name] = client.vendor_cmds().get_sflow_clear_commands()

        doc = {}
        for name, cmds in sw_cmds.items():
            doc[name] = {"commands": "; ".join(cmds)}
        logger.info(yaml.safe_dump(doc, default_flow_style=False))

        rv = 0
        args = [((sw_cmds[i.name],),) for i in infos]
        result = perform_group_action(infos, "execute_commands", switch_args=args)
        for sw, res in result.items():
            if isinstance(res, str):
                logger.error(f"{sw} failed: {res}")
                rv = 1

        return rv


class Configure(SubCommandABC):
    """Enable/disable sFlow and configure sFlow parameters."""

    name = "configure"

    def construct(self):
        self.add_arg_filter(False)
        self.add_arg_noconfirm()
        self.add_arg_dryrun(help="Do not execute. Only print or write commands to --output-dir")
        self.parser.add_argument("--output-dir", default="", help="Location to write commands, else print to stdout")
        self.parser.add_argument("--enable", default=None, help="Run sFlow egress sampling", action="store_true")
        self.parser.add_argument("--disable", default=None, help="Do not run sFlow", action="store_true")
        self.parser.add_argument("--interfaces", default=None, help="Comma-separated list of interfaces to en/disable (e.g., Ethernet10/1,Ethernet21/3). If not specificied, by default all interfaces are configured")
        self.parser.add_argument("--sampling-rate", default=None, help="Packet sampling rate, e.g., 65536 will sample 1 in 65536 packets")
        self.parser.add_argument("--poll-interval", default=None, help="Interface counter poll interval (s). Set to 0 to disable")
        self.parser.add_argument("--src-ip", default=None, help="sFlow packet IPv4 source address")
        self.parser.add_argument("--agent-ip", default=None, help="sFlow packet agent address")
        self.parser.add_argument("--dsts", default=None, help="Comma-separated list of sFlow collectors, e.g., 10.0.0.1,10.0.0.2:6444. If this argument is not specified, the mgmt VIP will be used as the collector address by default")
        self.parser.add_argument("--auto", default=False, help="Automatically configure the agent IP and collector with values from network_config.json. If there are multiple collectors, they will be round-robin assigned to each switch.", action="store_true")

    def _auto_detect_collector_ips(self) -> List[str]:
        """
        Automatically detect the collector IPs from the management nodes running sflow-collector pods.
        Returns a list of IPs.
        """
        # Get the management nodes running an sflow-collector pod
        kctl_get_nodes = "kubectl get pods -nprometheus -lapp=cluster-mgmt-sflow-exporter -ojsonpath='{.items..spec.nodeName}'"
        exit_code, stdout, stderr = mgmt_node_run(self.profile, kctl_get_nodes)
        if exit_code != 0:
            logger.error(f"Failed to get management nodes running sflow-collector pods. Command: `{kctl_get_nodes}`. STDERR: {stderr}")
            return []
        mgmt_nodes = stdout.strip().split()
        if len(mgmt_nodes) == 0:
            logger.error("No management nodes found running sflow-collector pods. Please ensure the sflow-collector is deployed in the cluster.")
            return []
        # Get the first interface with an address for each management node
        ip_for_node = {}
        nwt = NetworkConfigTool.for_profile(self.profile) # load network_config.json
        for node in nwt.nw_config().get("management_nodes", []):
            if node["name"] not in mgmt_nodes:
                continue
            for iface in node.get("interfaces", []):
                ip = iface.get("address", None)
                if not ip: continue
                assert node["name"] not in ip_for_node, f"Duplicate management node name {node['name']} found in network_config.json"
                ip_for_node[node["name"]] = ip.split("/")[0]  # Get the IP without the CIDR
                break
        if len(ip_for_node) == 0:
            logger.error(f"No management node IPs found for {mgmt_nodes}. Please ensure the management nodes have interfaces with IP addresses.")
            return []
        for node in mgmt_nodes:
            if node not in ip_for_node:
                logger.warning(
                    f"Management node {node} does not have an IP address configured in network_config.json. Skipping. Will not be used as a sFlow collector.")
        mgmt_node_ips = ip_for_node.values()
        return mgmt_node_ips

    def run(self, args):
        # Automatically determine the collector IPs if --auto is specified
        round_robin_collector = None
        if args.auto and args.enable and not args.dsts:
            mgmt_node_ips = self._auto_detect_collector_ips()
            if len(mgmt_node_ips) == 0:
                logger.error("No management node IPs found to use as sFlow collectors. Please specify --dsts or ensure management nodes have IPs configured in network_config.json.")
                return 1
            round_robin_collector = itertools.cycle(mgmt_node_ips)

        switches = Device.get_all(self.profile).filter(device_type="SW", device_role__in=("SP", "SX", "MX", "LF"))
        switches = self.filter_devices(args, query_set=switches)

        infos = _gather_switches(self.profile, switches)
        if not infos:
            logger.info("nothing to configure")
            return 1

        # Collect the current sFLow configs
        result = perform_group_action(infos, "sflow_status")
        statuses, err = {}, []
        for sw_name, v in result.items():
            if isinstance(v, str):
                err.append(f"{sw_name}: {v}")
            else:
                # result value is a singleton [SFlowStatus]
                statuses[sw_name] = v[0]
        if err:
            logger.error(f"Failed to sflow_status on {len(err)} switches:")
            for e in err:
                logger.error(e)
            return 1

        # Collect the interfaces
        result = perform_group_action(infos, "list_interfaces")
        interfaces, err = {}, []
        for sw_name, v in result.items():
            if isinstance(v, str):
                err.append(f"{sw_name}: {v}")
            else:
                interfaces[sw_name] = v
        if err:
            logger.error(f"Failed to list_interfaces on {len(err)} switches:")
            for e in err:
                logger.error(e)
            return 1

        def parse_dsts(dsts: Optional[str]) -> List[Tuple[str, Optional[int]]]:
            if dsts is None:
                return None
            def parse_dst(d: str) -> Tuple[str, Optional[int]]:
                if not d: return None
                ip_port = [s.strip() for s in d.split(":")]
                port = int(ip_port[1]) if len(ip_port) == 2 else None
                return (ip_port[0], port)
            return list(filter(None, map(parse_dst, dsts.split(","))))

        def is_ipv4_address(host: str) -> bool:
            try:
                ipaddress.IPv4Address(host)  # Ensure it's an IPv4 address
                return True
            except ValueError:
                return False

        enable = not args.disable if args.disable is not None else args.enable
        specified_dsts = parse_dsts(args.dsts) if args.dsts else None

        configure_interfaces = [s.strip() for s in args.interfaces.split(",")] if args.interfaces else None

        num_switches = len(statuses)
        if configure_interfaces and num_switches > 1:
            logger.warning(
                f"You selected {num_switches} switches and passed the `--interfaces` argument. This will configure the interfaces {configure_interfaces} on all {num_switches} of the switches. Is this what you intended?"
            )

        sflow_confs = {}
        for switch_info in infos:
            agent_ip = args.agent_ip
            if enable and not agent_ip and args.auto:
                # Automatically set the agent IP to the switch's management IP
                agent_ip = switch_info.host if is_ipv4_address(
                    switch_info.host) else socket.gethostbyname(switch_info.host)
                logger.info(
                    f"{switch_info.name}: --agent-ip not provided. Using the switch's management IP as the agent IP: {agent_ip}"
                )
            dsts = specified_dsts
            if round_robin_collector:
                dsts = [(next(round_robin_collector), None)]
                logger.info(
                    f"{switch_info.name}: --dsts not provided. Round-robin assigning a collector from the pool of managment nodes: {dsts[0][0]}"
                )
            sflow_confs[switch_info.name] = SFlowConfig(
                enable=enable,
                sampling_rate=args.sampling_rate,
                poll_interval=args.poll_interval,
                agent_ip=agent_ip,
                src_ip=args.src_ip,
                dsts=dsts,
                configure_interfaces=configure_interfaces,
            )

        sw_cmds = {}
        for switch_info in infos:
            assert switch_info.name in statuses
            assert switch_info.name in interfaces
            switch_ctl = create_client(switch_info)
            sw_cmds[
                switch_info.name
            ] = switch_ctl.vendor_cmds().get_sflow_config_commands(
                interfaces.get(switch_info.name),
                statuses.get(switch_info.name),
                sflow_confs.get(switch_info.name),
            )

        if args.output_dir:
            output_path = pathlib.Path(args.output_dir)
            output_path.mkdir(exist_ok=True)
            for name, cmds in sw_cmds.items():
                (output_path / name).write_text("\n".join(cmds))
        else:
            doc = {}
            for name, cmds in sw_cmds.items():
                doc[name] = {"commands": "; ".join(cmds)}
            logger.info(yaml.safe_dump(doc, default_flow_style=False))

        if args.dryrun:
            return 0

        print(f"modifying {len(infos)} switches:")
        print("  " + "\n  ".join([d.name for d in infos]))
        if not args.noconfirm and not prompt_confirm("continue?"):
            return 0

        rv = 0
        args = [((sw_cmds[i.name],),) for i in infos]
        result = perform_group_action(infos, "execute_commands", switch_args=args)
        for sw, res in result.items():
            if isinstance(res, str):
                logger.error(f"{sw} failed: {res}")
                rv = 1

        return rv


class SFlowCmd(SubCommandABC):
    """Switch sFlow related actions."""

    SUB_CMDS = [Status, Clear, Configure]

    name = "sflow"

    def construct(self):
        subparsers = self.parser.add_subparsers(dest="action", required=True)
        for ssc in self.SUB_CMDS:
            m = ssc(subparsers, profile=self.profile, cli_instance=self.cli_instance)
            m.build()

    def run(self, args):
        if hasattr(args, "func"):
            args.func(args)
