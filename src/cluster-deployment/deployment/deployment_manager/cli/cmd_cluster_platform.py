import json
import logging
import os
import paramiko
import socket
import tempfile
import time
from dataclasses import asdict
from tabulate import tabulate
from threading import Thread
from typing import List

from deployment_manager.cli.cluster_platform.gen_platform_version import gen_platform_versions
from deployment_manager.cli.cluster_platform.update_servers import update_servers
from deployment_manager.cli.cluster_platform.update_systems import update_systems
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles
from deployment_manager.db.models import Cluster, Device, DeviceProperty
import deployment_manager.platform.manifests.cluster_manifest as cluster_manifest
import deployment_manager.platform.manifests.cluster_status as cluster_status
from deployment_manager.platform.manifests.interface import Manifest
from deployment_manager.platform.utils import (
    get_bios_config,
    get_bios_vendor,
    get_bios_version,
    get_package_versions,
    get_server_roce_status,
    get_system_roce_status,
)
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node
from deployment_manager.tools.network_config_tool import NetworkConfigTool
from deployment_manager.tools.system import SystemCtl
from deployment_manager.tools.ssh import SSHConn
from deployment_manager.tools.utils import (
    TaskRunner,
    exec_cmd,
    exec_remote_cmd,
    get_version,
    ping_check,
)

logger = logging.getLogger(__name__)

class ClusterPlatformCheckSoftware(SubCommandABC):
    """ Compare platform software versions against manifest
    """
    name = "check_software"

    def construct(self):
        manifest_names = [m.name for m in Manifest.get_manifests()]
        self.parser.add_argument("--profile",
            help="Name of a saved cluster profile")
        self.parser.add_argument("-m", "--manifest",
            help="Name of manifest to compare against",
            choices=manifest_names,
            default=manifest_names[0]
        )
        self.parser.add_argument("--config",
            help="Flag to indicate if configuration should be checked",
            action="store_true"
        )
        self.parser.add_argument("--refresh",
            help="Flag to indicate if package information should be refreshed",
            action="store_true"
        )
        self.parser.add_argument("-o", "--output_format",
            help="Format of output of the check",
            choices=["table", "json"],
            default="table"
        )
        self.parser.add_argument("--show_all",
            help="Show mismatches of all severities. Only critical will be shown by default.",
            action="store_true"
        )
        self.parser.add_argument("--release",
            help="Version of manifest to check against",
            choices=["3.0.0", "2.5.0", "2.4.0"]
        )

    def _get_package_versions(self, manifest, servers):
        tr = TaskRunner(get_package_versions)
        arg_iter = [(manifest, s.name,) for s in servers]
        results = tr.run(arg_iter)

        version_dict = dict()
        for i, result in enumerate(results):
            ret, versions = result
            if ret != True:
                version_dict[servers[i]] = versions
        return version_dict

    def _get_bios_vendors(self, servers):
        tr = TaskRunner(get_bios_vendor)
        arg_iter = [(s.name,) for s in servers]
        results = tr.run(arg_iter)

        vendor_dict = dict()
        for i, result in enumerate(results):
            ret, vendor = result
            if not ret:
                vendor_dict[servers[i]] = vendor
        return vendor_dict

    def _get_bios_versions(self, servers):
        tr = TaskRunner(get_bios_version)
        arg_iter = [(s.name,) for s in servers]
        results = tr.run(arg_iter)

        version_dict = dict()
        for i, result in enumerate(results):
            ret, version = result
            if not ret:
                version_dict[servers[i]] = version
        return version_dict

    def _get_roce_status(self, devices, fn):
        tr = TaskRunner(fn)
        arg_iter = [(s.name,) for s in devices]
        results = tr.run(arg_iter)

        status_dict = dict()
        for i, result in enumerate(results):
            ret, status = result
            if not ret:
                status_dict[devices[i]] = status
        return status_dict

    def _get_server_roce_status(self, servers):
        return self._get_roce_status(servers, get_server_roce_status)

    def _get_system_roce_status(self, systems):
        return self._get_roce_status(systems, get_system_roce_status)

    def _get_bios_config(self, servers):
        tr = TaskRunner(get_bios_config)
        arg_iter = [(s.name,
                     s.get_prop(props.prop_bios_vendor),
                     s.get_prop(props.prop_ipmi_credentials_user),
                     s.get_prop(props.prop_ipmi_credentials_password),
                     )
                    for s in servers]
        results = tr.run(arg_iter)

        config_dict = dict()
        for i, result in enumerate(results):
            ret, config = result
            if not ret:
                config_dict[servers[i]] = config
        return config_dict

    def _get_switch_firmware_versions(self, network_config_tool, switches):
        fw_dict = dict()
        for switch_name, fwver in network_config_tool.get_switch_firmware_versions().items():
            for s in switches:
                if s.name == switch_name:
                    fw_dict[s] = fwver
        return fw_dict

    def _get_switch_vendors(self, network_config_tool, switches):
        vendor_dict = dict()
        for s in switches:
            vendor_dict[s] = s.get_prop(props.prop_vendor_name)
        return vendor_dict

    def _get_switch_models(self, network_config_tool, switches):
        model_dict = dict()
        for switch_name, model in network_config_tool.get_switch_models().items():
            for s in switches:
                if s.name == switch_name:
                    model_dict[s] = model
        return model_dict

    def _get_switch_roce_status(self, network_config_tool, switches):
        rs_dict = dict()
        for switch_name, rs in network_config_tool.get_switch_roce_status().items():
            for s in switches:
                if s.name == switch_name:
                    rs_dict[s] = rs
        return rs_dict

    def _collect_node_status(self, node, vendor, role, manifest_path):
        """Collect the cluster status of a node"""
        if not ping_check(node):
            logger.error(f"Node {node} not reachable")
            return -1, ""

        tmp_dir = "/tmp/node_status"
        ret, _, _ = exec_remote_cmd(f"mkdir -p {tmp_dir}", node, "root")
        if ret:
            logger.error(f"Failed to create temp directory {tmp_dir} on {node}")
            return -1, ""

        # Script to run remotely on the node to collect the status
        script = "collect_node_status.py"
        script_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "platform", "manifests", script)
        scp_cmd = f"scp {script_path} {manifest_path} {node}:{tmp_dir}"
        ret, _, _ = exec_cmd(scp_cmd, logcmd=False)
        if ret:
            logger.error(f"Failed to copy files to {node}")
            return -1, ""

        cmd = (
            f"python3 {tmp_dir}/{script} --node {node} --role {role} --vendor {vendor}"
            + f" --manifest {os.path.join(tmp_dir, os.path.basename(manifest_path))}"
        )
        ret_value, out, err = exec_remote_cmd(cmd, node, "root")

        ret, _, _ = exec_remote_cmd(f"rm -rf {tmp_dir}", node, "root")
        if ret:
            logger.error(f"Failed to delete temp directory {tmp_dir} on {server}")

        return ret_value, out


    def _populate_node_status(
        self,
        nodes,
        manifest_file,
        status: cluster_status.ClusterStatus
    ):
        """Populate all nodes status in parallel"""
        tr = TaskRunner(self._collect_node_status)
        arg_iter = [(n.name,
                     n.get_prop(props.prop_vendor_name),
                     n.device_role,
                     manifest_file
                    ) for n in nodes]
        results = tr.run(arg_iter)

        status.summary.nodes.num_nodes = len(nodes)
        for i, n in enumerate(nodes):
            ret, out = results[i]

            if not out:
                # Node is not reachable
                status.summary.nodes.num_discrepant_nodes += 1
                node_info = cluster_manifest.NodeInfo(
                    vendor=n.get_prop(props.prop_vendor_name),
                    role=n.device_role
                )
                feature=cluster_status.Feature(name="Not reachable")
                status.discrepancies.nodes.append(
                    cluster_status.ClusterNodeFeatures(
                        name=n.name,
                        info=node_info,
                        features=[feature]
                    )
                )
                status.details.nodes.append(
                    cluster_status.ClusterNodeFeatures(
                        name=n.name,
                        info=node_info,
                        features=[feature]
                    )
                )
                continue

            data = json.loads(out)
            node_status = cluster_status.from_dict(cluster_status.ClusterStatus, data)

            status.summary.nodes.num_discrepant_nodes += node_status.summary.nodes.num_discrepant_nodes
            status.discrepancies.nodes.extend(node_status.discrepancies.nodes)
            status.details.nodes.extend(node_status.details.nodes)


    def _populate_switch_status(self, switches, manifest: cluster_manifest.ClusterManifest, status: cluster_status.ClusterStatus):
        """Populate switch status"""
        status.summary.switches.num_switches = len(switches)
        status.summary.switches.num_discrepant_switches = 0

        # TODO: Get switch status from database serially for now,
        # need to parallelize and get live data from switch.
        for s in switches:
            vendor = s.get_prop(props.prop_vendor_name)
            model = s.get_prop(props.prop_switch_info_model)
            role = s.device_role
            switch_info = cluster_status.SwitchInfo(vendor, model, role)

            discrepancies = cluster_status.ClusterSwitchFeatures()
            discrepancies.name = s.name
            discrepancies.info = switch_info

            details = cluster_status.ClusterSwitchFeatures()
            details.name = s.name
            details.info = switch_info

            for m in manifest.switch_manifest:
                if m.name == "firmware":
                    firmware = s.get_prop(props.prop_switch_info_firmware_version)
                    expected = cluster_manifest.get_expected(m, switch_info)

                    feature = cluster_status.Feature()
                    feature.name = m.name
                    feature.value = firmware
                    if expected != "N/A":
                        feature.expected = expected

                    details.features.append(feature)
                    if expected != "N/A" and feature.value != feature.expected:
                        discrepancies.features.append(feature)
                else:
                    logger.error(f"Unsupported switch feature {m.name}")

            status.details.switches.append(details)
            if len(discrepancies.features) > 0:
                status.summary.switches.num_discrepant_switches += 1
                status.discrepancies.switches.append(discrepancies)

    def _collect_system_status(self, system, system_manifest):
        """Collect the cluster status of a node"""
        if not ping_check(system.name):
            logger.error(f"System {system.name} not reachable")
            return -1, ""

        system_ctl = SystemCtl(
            system.name,
            system.get_prop(props.prop_management_credentials_user, include_default=False) or "admin",
            system.get_prop(props.prop_management_credentials_password, include_default=False) or ""
        )

        cmd = "dashboard show --output-format json"
        ret, out, err = system_ctl._exec_remote_cmd(cmd)
        if ret != 0:
            logger.error(f"dashboard show command failed on {system.name}: stdout: {out}, stderr: {err}")
            return -1, ""

        dashboard = json.loads(out)

        status = cluster_status.ClusterStatus()
        status.summary.systems.num_systems = 1

        system_info = cluster_manifest.SystemInfo(model=dashboard["product"]["name"])
        discrepancies = cluster_status.ClusterSystemFeatures(
            name = system.name,
            info = system_info
        )
        details = cluster_status.ClusterSystemFeatures(
            name = system.name,
            info = system_info
        )

        for m in system_manifest:
            if m.name == "software":
                software = dashboard["product"]["version"]
                feature = cluster_status.Feature(name=m.name, value=software)
                details.features.append(feature)
            elif m.name == "build-id":
                buildid = dashboard["product"]["buildid"]
                feature = cluster_status.Feature(name=m.name, value=buildid)
                details.features.append(feature)
            else:
                logger.error(f"Unsupported system feature {m.name}")

        status.details.systems.append(details)
        if len(discrepancies.features) > 0:
            status.summary.systems.num_discrepant_nodes = 1
            status.discrepancies.systems.append(discrepancies)

        return 0, json.dumps(asdict(status))


    def _populate_system_status(
        self,
        systems,
        manifest: cluster_manifest.ClusterManifest,
        status: cluster_status.ClusterStatus,
    ):
        """Populate all systems status in parallel"""
        tr = TaskRunner(self._collect_system_status)
        arg_iter = [(s,
                     manifest.system_manifest
                    ) for s in systems]
        results = tr.run(arg_iter)

        status.summary.systems.num_systems = len(systems)
        for i, s in enumerate(systems):
            ret, out = results[i]

            if not out:
                # System is not reachable
                status.summary.systems.num_discrepant_systems += 1
                system_info = cluster_manifest.SystemInfo()
                feature=cluster_status.Feature(name="Not reachable")
                status.discrepancies.systems.append(
                    cluster_status.ClusterSystemFeatures(
                        name=s.name,
                        info=system_info,
                        features=[feature]
                    )
                )
                status.details.systems.append(
                    cluster_status.ClusterSystemFeatures(
                        name=s.name,
                        info=system_info,
                        features=[feature]
                    )
                )
                continue

            data = json.loads(out)
            system_status = cluster_status.from_dict(cluster_status.ClusterStatus, data)

            status.summary.systems.num_discrepant_systems += system_status.summary.systems.num_discrepant_systems
            status.discrepancies.systems.extend(system_status.discrepancies.systems)
            status.details.systems.extend(system_status.details.systems)

    def _concat_cluster_features(
        self,
        cluster_features : cluster_status.ClusterFeatures
    ) -> List[cluster_status.Feature]:
        features = []
        for n in cluster_features.nodes:
            for d in n.features:
                features.append([
                    "Node",
                    n.name,
                    n.info.vendor,
                    n.info.model,
                    n.info.role,
                    d.name,
                    d.value,
                    d.expected
                ])
        for sw in cluster_features.switches:
            for d in sw.features:
                features.append([
                    "Switch",
                    sw.name,
                    sw.info.vendor,
                    sw.info.model,
                    sw.info.role,
                    d.name,
                    d.value,
                    d.expected
                ])
        for sy in cluster_features.systems:
            for d in sy.features:
                features.append([
                    "System",
                    sy.name,
                    "",
                    sy.info.model,
                    "",
                    d.name,
                    d.value,
                    d.expected
                ])

        return features


    def _display(self, status: cluster_status.ClusterStatus, args):
        if args.output_format == "json":
            print(json.dumps(asdict(status), indent=4))
        elif args.output_format == "table":
            summary_header = ["Device Type", "Total", "Discrepancies"]
            summary = []
            summary.append(
                [
                    "Node",
                    status.summary.nodes.num_nodes,
                    status.summary.nodes.num_discrepant_nodes,
                ]
            )
            summary.append(
                [
                    "Switch",
                    status.summary.switches.num_switches,
                    status.summary.switches.num_discrepant_switches,
                ]
            )
            summary.append(
                [
                    "System",
                    status.summary.systems.num_systems,
                    status.summary.systems.num_discrepant_systems,
                ]
            )

            print("Cluster status summary")
            print(tabulate(summary, headers=summary_header))

            if (
                status.summary.nodes.num_discrepant_nodes
                or status.summary.switches.num_discrepant_switches
                or status.summary.systems.num_discrepant_systems
            ):
                discrepancy_header = [
                    "Type",
                    "Name",
                    "Vendor",
                    "Model",
                    "Role",
                    "Feature",
                    "Installed",
                    "Expected",
                ]
                discrepancies = self._concat_cluster_features(status.discrepancies)
                print("\nCluster status discrepancies")
                print(tabulate(discrepancies, headers=discrepancy_header))

            if args.show_all:
                details_header = [
                    "Type",
                    "Name",
                    "Vendor",
                    "Model",
                    "Role",
                    "Feature",
                    "Installed",
                    "Expected",
                ]
                details = self._concat_cluster_features(status.details)
                print("\nCluster status details")
                print(tabulate(details, headers=details_header))


    def report_cluster_status(self, args):
        manifest_file = f"cluster_manifest_rel-{args.release}.json"
        manifest_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "..",
            "platform",
            "manifests",
            "data"
        )
        manifest_path = os.path.join(manifest_dir, manifest_file)
        manifest = cluster_manifest.read_from_json(manifest_path)

        profilename = self.profile
        if args.profile:
            profilename = args.profile
        servers = Device.get_servers(profilename)
        # Ignore all infrastructure nodes
        servers =  [(s) for s in servers if s.device_role != DeploymentDeviceRoles.INFRA.value]

        switches = Device.get_switches(profilename)
        systems = Device.get_systems(profilename)

        status = cluster_status.ClusterStatus()

        start_time = time.time()

        self._populate_node_status(servers, manifest_path, status)
        node_time = time.time()
        node_elapsed_time = node_time - start_time

        self._populate_switch_status(switches, manifest, status)
        switch_time = time.time()
        switch_elapsed_time = switch_time - node_time

        self._populate_system_status(systems, manifest, status)
        system_time = time.time()
        system_elapsed_time = system_time - switch_time

        logger.info(f"Populated node status in {node_elapsed_time}s, switch status in {switch_elapsed_time}s, and system status in {system_elapsed_time}s")

        self._display(status, args)


    def run(self, args):
        if args.release:
            self.report_cluster_status(args)
            return

        manifest = Manifest.get_manifest(args.manifest)
        if manifest is None:
            logger.info(f"Manifest {args.manifest} not found")
            return
        profilename = self.profile
        if args.profile:
            profilename = args.profile
        nwt = NetworkConfigTool.for_profile(profilename)
        if args.output_format != "json":
            logger.info(
                f"Running platform software version check for {get_version()}"
            )
        servers = Device.get_servers(profilename)
        # Ignore all infrastructure nodes
        servers =  [(s) for s in servers if s.device_role != DeploymentDeviceRoles.INFRA.value]

        systems = Device.get_systems(profilename)
        switches = Device.get_switches(profilename)
        # Refresh info if specified
        if args.refresh:
            version_dict = self._get_package_versions(manifest, servers)
            for server, versions in version_dict.items():
                server.update_package_versions(versions)
            vendor_dict = self._get_bios_vendors(servers)
            for server, vendor in vendor_dict.items():
                server.set_prop(props.prop_bios_vendor, vendor)
            bios_version_dict = self._get_bios_versions(servers)
            for server, version in bios_version_dict.items():
                server.set_prop(props.prop_bios_version, version)
            switch_vendor_dict = self._get_switch_vendors(nwt, switches)
            for switch, vendor in switch_vendor_dict.items():
                # TODO: this should be mapped to the well-known vendor names, e.g. arista -> AR, juniper -> JU, ...
                switch.set_prop(props.prop_vendor_name, vendor)
            switch_model_dict = self._get_switch_models(nwt, switches)
            for switch, model in switch_model_dict.items():
                switch.set_prop(props.prop_switch_info_model, model)
            switch_fw_dict = self._get_switch_firmware_versions(nwt, switches)
            for switch, version in switch_fw_dict.items():
                switch.set_prop(props.prop_switch_info_firmware_version, version)
            if args.config:
                roce_dict = self._get_server_roce_status(servers)
                roce_dict.update(self._get_system_roce_status(systems))
                roce_dict.update(self._get_switch_roce_status(nwt, switches))
                for device, roce_status in roce_dict.items():
                    device.set_prop(props.prop_network_config_roce, roce_status)
                bios_config_dict = self._get_bios_config(servers)
                for server, bc in bios_config_dict.items():
                    server.update_bios_config(bc)
        # Server check
        pkg_mismatches = []
        bios_mismatches = []
        roce_mismatches = []
        bios_config_mismatches = []
        switch_fw_mismatches = []
        for s in servers:
            # packages
            for mp in manifest.get_packages():
                if not args.show_all and not manifest.is_package_critical(mp):
                    continue
                server_roles = manifest.get_package_server_roles(mp)
                if server_roles is not None and s.device_role not in server_roles:
                    continue
                mv = manifest.get_package_version(mp)
                pv = DeviceProperty.get_package_version(s, mp)
                if pv is None:
                    pkg_mismatches.append([s.name, mp, mv, "Unknown"])
                elif pv != mv:
                    if pv == "":
                        pkg_mismatches.append([s.name, mp, mv, "Not installed"])
                    elif manifest.needs_exact_match(mp):
                        pkg_mismatches.append([s.name, mp, mv, pv])
            # bios
            vendor = s.get_prop(props.prop_bios_vendor)
            if vendor is not None:
                mb = manifest.get_bios_version(vendor)
                pb = s.get_prop(props.prop_bios_version)
                if pb is not None:
                    if pb != mb:
                        bios_mismatches.append([s.name, vendor, mb, pb])
                else:
                    bios_mismatches.append([s.name, vendor, mb, "Unknown"])
            else:
                bios_mismatches.append([s.name, "Unknown", "-", "-"])
            # server config
            if args.config:
                # roce
                if args.show_all or manifest.is_server_roce_critical(s.role):
                    roce_status = s.get_prop(props.prop_network_config_roce)
                    mr = manifest.get_server_roce_status(s.role)
                    if roce_status is not None:
                        roce_status = (roce_status == "enabled")
                        if mr != roce_status:
                            roce_mismatches.append([
                                s.name,
                                "Enabled" if mr else "Disabled",
                                "Enabled" if roce_status else "Disabled"
                            ])
                    else:
                        roce_mismatches.append([
                            s.name,
                            "Enabled" if mr else "Disabled",
                            "Unknown"
                        ])
                # bios
                if args.show_all or manifest.is_bios_config_critical():
                    bios_config = s.get_bios_config()
                    mbc = Manifest.get_bios_config(s.device_role)
                    for attr, val in mbc.items():
                        bca = bios_config.get(attr, "Unknown")
                        if bca != val:
                            bios_config_mismatches.append([
                                s.name,
                                attr,
                                val,
                                bca
                            ])
        # system config
        if args.config:
            if args.show_all or manifest.is_system_roce_critical():
                for s in systems:
                    roce_status = s.get_prop(props.prop_network_config_roce)
                    mr = manifest.get_system_roce_status()
                    if roce_status is not None:
                        roce_status = (roce_status == "enabled")
                        if mr != roce_status:
                            roce_mismatches.append([
                                s.name,
                                "Enabled" if mr else "Disabled",
                                "Enabled" if roce_status else "Disabled"
                            ])
                    else:
                        roce_mismatches.append([
                            s.name,
                            "Enabled" if mr else "Disabled",
                            "Unknown"
                        ])
        # switches
        for s in switches:
            # firmware version
            vendor = s.get_prop(props.prop_vendor_name)
            model = s.get_prop(props.prop_switch_info_model)
            if vendor and model:
                mv = manifest.get_switch_firmware_version(vendor, model)
                sv = s.get_prop(props.prop_switch_info_firmware_version)
                if sv is not None and sv != "":
                    if sv != mv:
                        switch_fw_mismatches.append([s.name, vendor, model, mv, sv])
                else:
                    switch_fw_mismatches.append([s.name, vendor, model, mv, "Unknown"])
            else:
                switch_fw_mismatches.append([s.name, "Unknown", "Unknown", "-", "-"])


        if args.output_format == "table":
            pkg_headers = [
                "Node", "Package name", "Expected version", "Installed version"
            ]
            bios_headers = [
                "Node", "Vendor", "Expected version", "Installed version"
            ]
            switch_fw_headers = [
                "Switch", "Vendor", "Model", "Expected version", "Installed version"
            ]
            if pkg_mismatches:
                logger.info("Package check result")
                logger.info("\n" + tabulate(pkg_mismatches, headers=pkg_headers))
            if bios_mismatches:
                logger.info("BIOS check result")
                logger.info("\n" + tabulate(bios_mismatches, headers=bios_headers))
            if switch_fw_mismatches:
                logger.info("Switch firmware check result")
                logger.info("\n" + tabulate(switch_fw_mismatches, headers=switch_fw_headers))
            if args.config:
                if bios_config_mismatches:
                    bios_config_headers = [
                        "Device", "Attribute",
                        "Expected value", "Configured value"
                    ]
                    logger.info("BIOS config check result")
                    logger.info("\n" + tabulate(bios_config_mismatches,
                            headers=bios_config_headers))
                if roce_mismatches:
                    roce_headers = [
                        "Device", "Expected status", "Configured status"
                    ]
                    logger.info("RoCE check result")
                    logger.info("\n" + tabulate(roce_mismatches, headers=roce_headers))
        else:
            output = dict()
            if pkg_mismatches:
                output['packages'] = list()
            for node, pkg_name, exp_v, inst_v in pkg_mismatches:
                output['packages'].append(dict(
                    node=node,
                    package=pkg_name,
                    expected_version=exp_v,
                    installed_version=inst_v
                ))
            if bios_mismatches:
                output['bios'] = list()
            for node, vendor, exp_v, inst_v in bios_mismatches:
                output['bios'].append(dict(
                    node=node,
                    vendor=vendor,
                    expected_version=exp_v,
                    installed_version=inst_v
                ))
            if switch_fw_mismatches:
                output['switch_firmware'] = list()
            for switch, vendor, model, exp_v, inst_v in switch_fw_mismatches:
                output['switch_firmware'].append(dict(
                    switch=switch,
                    vendor=vendor,
                    model=model,
                    expected_version=exp_v,
                    installed_version=inst_v
                ))
            if args.config:
                output['config'] = dict()
                if roce_mismatches:
                    output['config']['network'] = dict(roce=list())
                    for device, exp_s, cfg_s in roce_mismatches:
                        if cfg_s == "Unknown":
                            continue
                        output['config']['network']['roce'].append(dict(
                            device=device,
                            expected_status=(True if exp_s == "Enabled" else False),
                            configured_status=(True if cfg_s == "Enabled" else False)
                        ))
                if bios_config_mismatches:
                    output['config']['bios'] = list()
                    for device, attr, exp_v, cfg_v in bios_config_mismatches:
                        if cfg_v == "Unknown":
                            continue
                        output['config']['bios'].append(dict(
                            device=device,
                            attribute=attr,
                            expected_value=exp_v,
                            configured_value=cfg_v
                        ))

            logger.info(json.dumps(output, indent=4))


class ClusterPlatformGenPlatformVersion(SubCommandABC):
    """Check cbcore compatibility with the servers and generate platform-version.json file on each server"""

    name = "gen_platform_version"

    def construct(self):
        self.parser.add_argument("--profile",
            help="Name of a saved cluster profile")

    def run(self, args):
        profilename = self.profile
        if args.profile:
            profilename = args.profile

        servers = Device.get_servers(profilename)
        cluster_servers = [
            (s)
            for s in servers
            if s.device_role != DeploymentDeviceRoles.USER.value
            and s.device_role != DeploymentDeviceRoles.INFRA.value
        ]

        return gen_platform_versions(cluster_servers)


class ClusterPlatformUpdateServers(SubCommandABC):
    """Update server software"""

    name = "update_servers"

    # TODO: BIOS updates in the future

    def construct(self):
        self.parser.add_argument(
            "--all-patches",
            help="Install all patches and Mellanox driver",
            action="store_true",
        )

        server_group = self.parser.add_mutually_exclusive_group(required=True)
        server_group.add_argument(
            "--servers",
            help="Install patches on the specified servers. Use 'ALL' to update all servers.",
            type=str,
            nargs="+",
        )
        server_group.add_argument(
            "--server-file",
            help="Only install patches on the servers listed in the file in line-by-line form",
            type=str,
        )
        server_group.add_argument(
            "--session",
            help="Only install patches for specific session assigned/related nodes.",
            type=str,
        )
        self.parser.add_argument(
            "--skip-sx",
            help="Used with the --session option, skip picking SX nodes as part of session nodes selection",
            action="store_true",
        )
        self.parser.add_argument(
            "--force",
            help="Force install the Mellanox driver even when it is already up-to-date.",
            action="store_true",
        )
        self.parser.add_argument(
            "--dry-run",
            help="Only pickup servers without actual updating.",
            action="store_true",
        )
        self.parser.add_argument(
            "--no-confirm",
            help="Do not prompt user before updating and rebooting servers",
            action="store_true",
        )
        self.parser.add_argument(
            "--verbose",
            help="Enable verbose output",
            action="store_true",
        )
        self.parser.epilog = (
            "Examples:\n"
            "  cluster_platform update_servers --all-patches --servers ALL\n"
            "  cluster_platform update_servers --all-patches --servers server1 server2 server3\n"
            "  cluster_platform update_servers --all-patches --session session1\n"
            "  cluster_platform update_servers --all-patches --server-file ./server-file.txt\n"
        )

    def _get_control_plane_node(self):
        """Get a control plane node, which can be different from the deploy node that cscfg is running on"""
        dnode = get_k8s_lead_mgmt_node(self.profile)
        return dnode.name if dnode else ""

    def _get_latest_package_version(self):
        """Return the latest package version in the manifest file, assuming the first one in json file is the latest"""
        dir_path = os.path.dirname(os.path.realpath(__file__))
        package_manifest = os.path.join(
            dir_path, "../platform/manifests/data/package_manifest.json"
        )

        with open(package_manifest, "r") as f:
            package_json = json.load(f)

        versions = package_json.get("package_versions", [])
        return versions[0].get("name")

    def _collect_servers(self, args, control_plane_node) -> List[str]:
        """Collect and validate the list of servers to update"""

        # cluster servers defined in the inventory
        cluster_servers = {
            s.name: (s.device_role, s.get_prop(props.prop_vendor_name))
            for s in Device.get_servers(self.profile)
            if s.device_role != DeploymentDeviceRoles.INFRA.value
        }

        # Precondition: 
        #   Exactly one of the options servers, server_file, or session must be
        #   provided. This is already enforced in argparse.
        servers = []
        if args.servers:
            if len(args.servers) == 1 and args.servers[0] == "ALL":
                servers = list(cluster_servers.keys())
            else:
                for s in args.servers:
                    if s in cluster_servers:
                        servers.append(s)
                    else:
                        logger.error(
                            f"Server {s} does not belong to the cluster. Ignored"
                        )
        elif args.server_file:
            with open(args.server_file, "r") as f:
                for s in f:
                    s = s.strip()
                    if s in cluster_servers:
                        servers.append(s)
                    else:
                        logger.error(
                            f"Server {s} does not belong to the cluster. Ignored"
                        )
        elif args.session:
            if control_plane_node:
                remote_server_file = f"/tmp/remote-upgrade-nodes-{args.session}"
                target_version = self._get_latest_package_version()
                cmd = (
                    f"select_nodes_fp={remote_server_file} "
                    f"csadm.sh select-upgrade-nodes {target_version} {args.session} {'y' if args.skip_sx else ''}"
                )
                ret, out, err = exec_remote_cmd(
                    cmd, control_plane_node, "root", timeout=180
                )
                if ret != 0:
                    msg = (
                        f"Failed to select servers for upgrade. The failing command is \n  {cmd}\n"
                        f"with logs:\n {out} {err}"
                        "Once resolved, please rerun the update_servers command."
                    )
                    logger.error(f"{msg}")
                    return []

                local_server_file = f"/tmp/upgrade-nodes-{args.session}"
                cmd = f"scp root@{control_plane_node}:{remote_server_file} {local_server_file}"
                ret, _, _ = exec_cmd(cmd, logcmd=False)

                if ret != 0:
                    logger.error(
                        f"Failed to copy {remote_server_file} from control plane node {control_plane_node}"
                    )
                    return []

                with open(local_server_file, "r") as f:
                    for s in f:
                        # All session servers should be in cluster_servers,
                        # but just in case.
                        if s in cluster_servers:
                            servers.append(s)
                        else:
                            logger.error(
                                f"Session server {s} does not belong to the cluster. Ignored"
                            )
            else:
                logger.error(
                    f"Cannot determine the servers of session {args.session} as there is no control plane node"
                )

        return servers

    def run(self, args):
        if not args.all_patches and not args.dry_run:
            logger.info("--all-patches option not selected")
            return

        if Cluster.objects.filter(profile__name=self.profile).count() > 1:
            logger.error("This is a blue-green cluster. Please use 'cscfg cluster upgrade ...' to update the servers.")
            return

        # Find a control plane node, which can be different from the deploy node.
        control_plane_node = self._get_control_plane_node()

        servers = self._collect_servers(args, control_plane_node)
        if len(servers) == 0:
            logger.info("No servers to update")
            return

        if args.dry_run:
            logger.info(f"Selected {len(servers)} servers for update. Stopped due to dry run")
            return

        _, _ = update_servers(
            self.profile,
            control_plane_node,
            servers,
            "/opt/cerebras/packages",
            args.no_confirm,
            args.force,
            args.verbose,
        )


class ClusterPlatformUpdateSystems(SubCommandABC):
    """Update CS system software"""

    name = "update_systems"

    def construct(self):
        """Construct itself"""
        self.parser.add_argument(
            "--image", help="CS system image to install", type=str, required=True
        )
        self.parser.add_argument(
            "--systems",
            help="List of CS systems to update. Use 'ALL' to update all systems.",
            type=str,
            nargs="+",
            required=False,
        )
        self.parser.add_argument(
            "--session",
            help="Update CS systems of a session",
            type=str,
            required=False,
        )
        self.parser.add_argument(
            "--no_confirm",
            help="Do not prompt user before updating systems",
            action="store_true",
        )
        self.parser.add_argument(
            "--force",
            help="Force update",
            action="store_true",
            dest="force",
            default=False,
        )
        self.parser.add_argument(
            "--skip_activate",
            help="Skip system activation after OS update",
            action="store_true",
            dest="skip_activate",
            default=False,
        )
        self.parser.add_argument(
            "--username",
            help="Custom CS system username",
            type=str,
            required=False,
        )
        self.parser.add_argument(
            "--password",
            help="Custom CS system password",
            type=str,
            required=False,
        )
        self.parser.epilog = (
            "Examples:\n"
            "  cluster_platform update_systems --image ./CS1-0.0.0.tar.gz --systems ALL\n"
            "  cluster_platform update_systems --image ./CS1-0.0.0.tar.gz --systems system1 system2 system3\n"
            "  cluster_platform update_systems --image ./CS1-0.0.0.tar.gz --session session1\n"
            "  cluster_platform update_systems --image ./CS1-0.0.0.tar.gz --systems ALL --filter name=~'xs104'\n"
        )

        self.add_arg_filter()

    def _get_control_plane_node(self):
        """Get a control plane node, which can be different from the deploy node that cscfg is running on"""
        dnode = get_k8s_lead_mgmt_node(self.profile)
        return dnode.name if dnode else ""

    # Status for check the system credential
    SYS_OK = 0
    SYS_UNREACHABLE = 1
    SYS_BAD_CREDENTIAL = 2

    def _check_credential(self, system, username, password):
        """Check if the system is reachable, and if the credential works"""

        if username is None:
            username = (
                system.get_prop(
                    props.prop_management_credentials_user, include_default=False
                )
                or "admin"
            )
        if password is None:
            password = (
                system.get_prop(
                    props.prop_management_credentials_password, include_default=False
                )
                or ""
            )

        # Try a simple command and see if works
        try:
            with SSHConn(system.name, username, password) as conn:
                _, _, _ = conn.exec("system date", throw=True)
        except paramiko.AuthenticationException:
            return ClusterPlatformUpdateSystems.SYS_BAD_CREDENTIAL
        except Exception:
            return ClusterPlatformUpdateSystems.SYS_UNREACHABLE

        return ClusterPlatformUpdateSystems.SYS_OK

    def _collect_systems(self, args):
        """Collect the systems and validate the credential if provided"""
        # 1. Make sure either --systems or --namespace option is provided
        if args.systems is None and args.session is None:
            logger.error("Either --systems or --session option must be provided")
            return [], -1

        if args.systems and args.session:
            logger.error("Either --systems or --session option can be provided, not both")
            return [], -1

        # 2. Collect the systems, make sure there are at least one system to update.
        if args.systems:
            if len(args.systems) == 1 and args.systems[0] == "ALL":
                filter_name = []
            else:
                filter_name = args.systems
            systems = self.filter_devices(
                args, Device.get_systems(self.profile), name=filter_name
            )
            if len(systems) == 0:
                logger.error("No systems are selected. Please check the --systems option.")
                return [], -1
        else:
            # collect session systems
            cmd = f"set -o pipefail; csctl session get {args.session} -ojson | jq -r '.state.resources.systems[]'"
            control_plane_node = self._get_control_plane_node()
            if not control_plane_node:
                logger.error(f"There are no control plane nodes, hence no sessions in the cluster.")
                return [], -1
            ret, out, _ = exec_remote_cmd(cmd, control_plane_node, "root", timeout=15)
            if ret:
                logger.error(f"Error in extracting the CS systems in session {args.session}")
                return [], -1
            systems = self.filter_devices(
                args, Device.get_systems(self.profile), name=out.split()
            )
            if len(systems) == 0:
                logger.error("No systems are selected. Please check if the session is a valid one.")
                return [], -1

        # 3. Check if the systems are reachable, and if username and password
        #    are correct.  Filter out bad ones for update.
        if (args.username is None) ^ (args.password is None) == 1:
            # Error if only one of them is provided.
            logger.error("Both username and password need to be provided.")
            return [], 1

        tr = TaskRunner(self._check_credential)
        arg_iter = [
            (
                s,
                args.username,
                args.password,
            )
            for s in systems
        ]
        results = tr.run(arg_iter)

        unreachables = []
        bad_credentials = []

        for i, status in enumerate(results):
            system = systems[i]
            if status == ClusterPlatformUpdateSystems.SYS_UNREACHABLE:
                unreachables.append(system.name)
            elif status == ClusterPlatformUpdateSystems.SYS_BAD_CREDENTIAL:
                bad_credentials.append(system.name)

        unreachables.sort()
        bad_credentials.sort()

        if len(unreachables):
            unreachables_out = ", ".join(unreachables)
            logger.error(f"{len(unreachables)} system(s) are unreachable. They will not be updated: {unreachables_out}")
            systems = [s for s in systems if s.name not in unreachables]

        if len(bad_credentials):
            bad_credentials_out = ", ".join(bad_credentials)
            if args.username and args.password:
                logger.error(f"Username and/or password invalid for the {len(bad_credentials)} systems. They will not be updated: {bad_credentials_out}")
            else:
                logger.error(f"Default username and/or password invalid for the {len(bad_credentials)} systems. They will not be updated: {bad_credentials_out}")
            systems = [s for s in systems if s.name not in bad_credentials]

        if len(systems) == 0:
            logger.error("No systems available for update.")
            return [], -1

        return systems, 0

    def run(self, args):
        systems, ret = self._collect_systems(args)
        if ret != 0:
            return ret

        ret, _ = update_systems(
            args.image,
            systems,
            args.no_confirm,
            args.force,
            args.skip_activate,
            args.username,
            args.password,
        )
        return ret


class ClusterPlatformSystemBgp(SubCommandABC):
    """ Configure switches to enable/disable BGP for systems
    """
    name = "system_bgp"

    def construct(self):
        self.parser.add_argument("action",
            help="Specify action [enable|disable]",
            choices=["enable", "disable"])
        self.parser.add_argument("--profile",
            help="Name of a saved cluster profile")

    def run(self, args):
        profilename = self.profile
        if args.profile:
            profilename = args.profile
        nwt = NetworkConfigTool.for_profile(profilename)

        if args.action == "enable":
            ret = nwt.enable_system_switch_bgp()
        elif args.action == "disable":
            ret = nwt.disable_system_switch_bgp()
        else:
            print(
                f"Invalid action {args.action}. "
                "Valid options are 'enable' or 'disable'"
            )
            return

        if ret:
            print(f"Failed to {args.action} switch config for system BGP")
        else:
            print(f"Switch config for system BGP {args.action}d successfully")


CMDS = [
    ClusterPlatformCheckSoftware,
    ClusterPlatformGenPlatformVersion,
    ClusterPlatformUpdateServers,
    ClusterPlatformUpdateSystems,
    ClusterPlatformSystemBgp,
]
