from collections import defaultdict

import abc
import json
import logging
import os
import pathlib
import shutil
import yaml
from tabulate import tabulate
from typing import Dict, List, Optional, Tuple

import deployment_manager.db.device_props as props
from deployment_manager.common.lock import with_lock, LOCK_ANSIBLE
from deployment_manager.db.const import DeploymentDeviceRoles
from deployment_manager.db.models import Device
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.dhcp import interface as dhcp
from deployment_manager.tools.utils import exec_cmd
from .device_status import DeviceStatus, Status
from .device_task import DeviceTask

logger = logging.getLogger(__name__)

PART_TO_FW = {
    "MCX653106A-ECA_HPE_Ax": "fw-ConnectX6-rel-20_37_1700-MCX653106A-ECA_HPE_Ax-UEFI-14.30.13-FlexBoot-3.7.102.tar.gz",
    "MCX653105A-ECA_HPE_Ax": "fw-ConnectX6-rel-20_42_1000-MCX653105A-ECA_HPE_Ax-UEFI-14.35.15-FlexBoot-3.7.500.tar.gz",
    "MCX653436A-HDA_HPE_Ax": "fw-ConnectX6-rel-20_37_1700-MCX653436A-HDA_HPE_Ax-UEFI-14.30.13-FlexBoot-3.7.102.tar.gz",
    "MCX516A-CDA_Ax_Bx": "fw-ConnectX5-rel-16_35_1012-MCX516A-CDA_Ax_Bx-UEFI-14.28.15-FlexBoot-3.6.804.bin.zip",
    "MCX555A-ECA_Ax": "fw-ConnectX5-rel-16_35_1012-MCX555A-ECA_Ax_Bx-UEFI-14.28.15-FlexBoot-3.6.804.bin.zip",
    "MCX555A-ECA_Ax_Bx": "fw-ConnectX5-rel-16_35_1012-MCX555A-ECA_Ax_Bx-UEFI-14.28.15-FlexBoot-3.6.804.bin.zip",
    "MCX623106AC-CDA_Ax": "fw-ConnectX6Dx-rel-22_38_1900-MCX623106AC-CDA_Ax-UEFI-14.31.20-FlexBoot-3.7.201.signed.bin.zip",
    "0VC496_06FKDT_Ax": "fw-ConnectX5-rel-16_32_2004-0VC496_06FKDT_Ax-FlexBoot-3.6.502.bin.zip",
    "MCX623436MS-CDA_Ax": "fw-ConnectX6Dx-rel-22_39_3004-MCX623436MS-CDA_Ax-UEFI-14.32.17-FlexBoot-3.7.300.signed.bin.zip",
    "MCX75310AAS-NEA_Ax": "fw-ConnectX7-rel-28_39_3004-MCX75310AAS-NEA_Ax-UEFI-14.32.17-FlexBoot-3.7.300.signed.bin.zip",
    "0F6FXM_08P2T2_Ax": "fw-ConnectX6Dx-rel-22_36_1010-0F6FXM_08P2T2_Ax-UEFI-14.29.14-FlexBoot-3.6.901.signed.bin.zip",
    "0FD63G_Ax": "fw-ConnectX6Dx-rel-22_40_1000-0FD63G_Ax-UEFI-14.33.10-FlexBoot-3.7.300.signed.bin.zip",
    "Super_Micro_AOC-A100G-m2C_Ax": "",  # no firmware image available at the moment
}

# Add a few IP addresses used by pool.ntp.org, in case there is no DNS on the management node.
DEFAULT_NTP_SERVERS = ["pool.ntp.org", "82.113.53.41", "142.202.190.19"]



def _get_nmcli_conn(mac: str) -> str:
    """ Find the nmcli conn name associated with a mac address. Usually 'Wired connection 1' """
    ifname = "unknown"
    try:
        _, ifname, _ = exec_cmd(
            f"ip -j a | jq -r '.[] | select(.address == \"{mac}\") | .ifname'",
            logcmd=False, throw=True
        )
        _, conn, _ = exec_cmd(
            f"nmcli --field GENERAL.CONNECTION d show {ifname.strip()} | cut -d: -f2",
            logcmd=False, throw=True
        )
        conn = conn.strip()
        if "\n" in conn:  # should be a single line
            raise ValueError(f"discovered connection looks invalid ({conn})")
        return conn
    except Exception as e:
        msg = f"failed to get nmcli connection name for mac={mac}, ifname={ifname} ({e})"
        logger.error(msg)
        raise RuntimeError(msg)


def generate_full_inventory(
    profile: str, targets: Optional[List[str]] = None
) -> Dict:
    """
    Args:
        profile (str): profile name
        targets (List[str], optional): generate inventory for devices in
                targets only. When targets is None, it will generate full
                inventory for the given profile

                Defaults to None.
    Returns:
        str: full inventory file in yaml string
    """

    inventory_roles = [
        DeploymentDeviceRoles.ACTIVATION.value,
        DeploymentDeviceRoles.MANAGEMENT.value,
        DeploymentDeviceRoles.MEMORYX.value,
        DeploymentDeviceRoles.WORKER.value,
        DeploymentDeviceRoles.SWARMX.value,
        DeploymentDeviceRoles.USER.value,
        DeploymentDeviceRoles.INFRA.value,
        DeploymentDeviceRoles.INFERENCEDRIVER.value,
    ]

    # scan all nodes and put them into different categories
    children_info = {
        'nodes': {'children': {role: {"hosts": {}} for role in inventory_roles}}
    }

    # root server group
    root_server_group = dict(hosts=dict())

    cg = ConfGen(profile)
    cfg = cg.parse_profile()
    root_server = cfg["basic"]["root_server"]

    all_servers = Device.get_all(profile, device_type="SR")
    if targets:
        all_servers = all_servers.filter(name__in=targets)

    for server in all_servers:
        name = server.name
        ip = str(server.get_prop(props.prop_management_info_ip))
        user = server.get_prop(props.prop_management_credentials_user)
        password = server.get_prop(props.prop_management_credentials_password)
        role = server.device_role

        if role not in inventory_roles:
            logger.warning(f"skip add {name} to inventory, unknown role={role}")
            continue

        if name == root_server:
            root_server_group["hosts"][name] = dict(
                ansible_host=ip,
                ansible_user=user,
                ansible_password=password,
            )
            continue

        children_info["nodes"]["children"][role]["hosts"][name] = dict(
            ansible_host=ip,
            ansible_user=user,
            ansible_password=password,
        )
    children_info["root-server"] = root_server_group

    inventory_obj = {'all': {'children': children_info}}
    return inventory_obj


def get_failure_context(doc: dict) -> Dict[str, List[dict]]:
    """ Parse ansible json output to a dict of hostname -> list of failures. Empty list = success """
    failure_context = defaultdict(list)
    for play in doc.get("plays", []):
        for task in play.get("tasks", []):
            task_name = task.get("task", {}).get("name", "")
            for host, status in task.get("hosts", {}).items():
                if status.get("failed", False):
                    task_failure = {"task_name": task_name}
                    # record some context fields - this is roughly chosen based on some previously failed scripts
                    for k in ("msg", "stderr", "stdout", "rc", "action", "cmd"):
                        if k in status:
                            v = status[k]
                            if isinstance(v, list):
                                v = "\n".join(v)
                            task_failure[k] = v
                    failure_context[host].append(task_failure)
    for host, summary in doc["stats"].items():
        if summary["failures"] == 0 and summary["unreachable"] == 0:
            failure_context[host] = []
        elif summary["unreachable"] > 0:
            failure_context[host].append({"task": "host_reachability", "status": "unreachable"})
        else:
            failure_context[host].append({"task": "summary", "status": f"{summary['failures']} failed tasks"})
    return failure_context


class AnsibleTask(DeviceTask):
    """
    Run ansible task
    """

    ANSIBLE_DIRECTORY = pathlib.Path(
        "/opt/cerebras/cluster-deployment/os-provision"
    )

    def __init__(self, profile: str):
        super().__init__(profile)
        self._cg = ConfGen(profile)
        self._cfg = self._cg.parse_profile()
        self._root_server = self._cfg["basic"]["root_server"]

    @with_lock(LOCK_ANSIBLE)  # TODO: we shouldn't lock, better to use a tempdir or ansible inventory provider
    def run(self, targets) -> Dict[str, DeviceStatus]:
        if not targets:
            return {}

        inventory_file = self.ANSIBLE_DIRECTORY / "inventory" / "hosts.yml"
        out, err = "", ""
        try:
            self._clean_up()
            self._generate_ansible_variable()
            inventory_dict = self._generate_inventory(targets)
            inventory_file.write_text(
                yaml.dump(inventory_dict, indent=4, default_flow_style=False)
            )
            logger.info(
                f"Running ansible task {self.name()} with devices: {' '.join(targets)}"
            )
            out, err = self._ansible_playbook()
            result = self._parse_ansible_result(out)[0]
            # add ansible JSON output to log file
            ansible_log = self._parse_ansible_result(out)[1]
            root = logging.getLogger()
            ansible_log_file = root.handlers[1].get_name()
            with open(ansible_log_file, 'a') as file:
                file.write('\n--------------------------- ANSIBLE LOG BEGIN ---------------------------------\n')
                file.write(str(ansible_log))
                file.write('\n---------------------------  ANSIBLE LOG END  ---------------------------------\n')
            return result
        except Exception as e:
            logger.error(f"ansible task {self.name()} raised an exception: {e}, stderr: {err}")
            result = {
                target: DeviceStatus(Status.FAILED, f"{e}")
                for target in targets
            }
            return result

    @abc.abstractmethod
    def _ansible_playbook(self) -> Tuple[str, str]:
        pass

    @abc.abstractmethod
    def _generate_ansible_variable(self):
        pass

    @abc.abstractmethod
    def _generate_inventory(self, targets) -> Dict:
        pass

    def _clean_up(self):
        # remove log file
        ansible_log = self.ANSIBLE_DIRECTORY / "ansible.log"
        if ansible_log.is_file():
            os.remove(str(ansible_log.absolute()))

        # clean up all previously generated ansible vars
        host_var = self.ANSIBLE_DIRECTORY / "host_vars"
        group_var = self.ANSIBLE_DIRECTORY / "group_vars"
        for dir in [host_var, group_var]:
            if dir.is_dir():
                shutil.rmtree(str(dir.absolute()))
            dir.mkdir()

        inventory_dir = self.ANSIBLE_DIRECTORY / "inventory"
        if inventory_dir.is_dir():
            shutil.rmtree(str(inventory_dir.absolute()))
        inventory_dir.mkdir()

    def _parse_ansible_result(self, output):
        result = {}

        try:
            doc = json.loads(output[output.find("{"):])  # strip leading warning messages
        except Exception as e:
            first_line = output if "\n" not in output else output[:output.find("\n")]
            raise RuntimeError("unable to parse ansible json output, check ansible log, usually "
                               f"/opt/cerebras/cluster-deployment/os-provision/ansible.log First line: '{first_line}'")

        failure_context = get_failure_context(doc)
        for target, failures in failure_context.items():
            if not failures:
                result[target] = DeviceStatus(Status.OK, "")
            else:
                result[target] = DeviceStatus(Status.FAILED, yaml.safe_dump(failures, default_flow_style=False))

        # log ansible summary
        header = ["changed", "failures", "ok", "unreachable"]
        rows = []
        for name, stats in doc["stats"].items():
            row = [name]
            row.extend([stats[key] for key in header])
            rows.append(row)
        header.insert(0, "device")

        # log table to stderr and file
        logger.info("\n" + tabulate(rows, header))
        # if stderr_handler level is higher than INFO, also print() the table
        root = logging.getLogger()
        stderr_handler_level = root.handlers[0].level
        if stderr_handler_level > logging.INFO:
            print(tabulate(rows, header))

        return (result, doc)

    def _write_dhcp_confs(self, configs: Dict[str, List[Tuple[pathlib.Path, str]]]):
        """ Write host_vars/root-server.yaml with dhcp_config_dir """
        host_vars_dir = self.ANSIBLE_DIRECTORY / "host_vars"
        host_vars_dir.mkdir(exist_ok=True)

        config_dir = self._cg.dhcp_config_dir
        for server_name, server_configs in configs.items():
            write_names = [server_name]
            if server_name == self._root_server:  # some flows refer to the root-server as "root-server" otherwise hostname
                write_names.append("root-server")
            for name in write_names:
                host_config_dir = config_dir / name
                host_config_dir.mkdir(exist_ok=True)
                host_var_file = (host_vars_dir / (name + ".yaml"))
                host_var_file.write_text(yaml.safe_dump({"dhcp_config_dir": str(host_config_dir.absolute())}))
                for fp, contents in server_configs:
                    (host_config_dir / fp.name).write_text(contents)


class ConfigureRootServer(AnsibleTask):
    """
    Run deploy main dhcp server. There are 2 deployment models.

    1. For smaller cluster, main dhcp server and root server will be on the same
       physical node.
    2. For large cluster, like CG series cluster, the dhcp server exists only
       on deployment node.

    based on config input, we can determine if we are using model 1 or model 2.
    """

    def __init__(self, profile: str):
        super().__init__(profile)

    def _ansible_playbook(self) -> Tuple[str, str]:
        cmd = "ansible-playbook configure-root-server.yml"
        ret, output, err = exec_cmd(
            cmd, cwd=str(self.ANSIBLE_DIRECTORY.absolute()), logcmd=True
        )
        return output, err

    def _generate_inventory(self, targets: List[str] = ()) -> Dict:
        assert len(targets) == 1, f"expected a single target for root server configuration, got: {targets}"
        inv = generate_full_inventory(self._profile, targets)
        root_server_group = inv["all"]["children"]["root-server"]
        for host in root_server_group["hosts"].values():
            host["ansible_connection"] = "local"
        return {
            "root-server": root_server_group
        }

    def _generate_ansible_variable(self):
        group_vars_file = self.ANSIBLE_DIRECTORY / "group_vars" / "root-server"

        cg = ConfGen(self._profile)
        cfg = cg.parse_profile()
        root_server_name = cfg["basic"].get("root_server", None)

        root_server = Device.get_device(root_server_name, self._profile)
        if root_server is None:
            raise ValueError(
                f"Please add root server {root_server_name} into inventory"
            )
        assert root_server.device_role in {
            DeploymentDeviceRoles.INFRA.value,
            DeploymentDeviceRoles.MANAGEMENT.value,
        }, f"unsupported root server {root_server.name} role {root_server.device_role}"

        # generate dhcp config files
        dhcp_provider = dhcp.get_provider(self._profile, self._cfg)
        configs = dhcp_provider.generate_configs()
        self._write_dhcp_confs(configs)

        # if root server is infrastructure node, then root server is on
        # deployment node.
        use_deployment_node = (
            root_server.device_role == DeploymentDeviceRoles.INFRA.value
        )

        ntp_servers = cfg["basic"].get("ntp_servers", [])
        ntp_servers = list(filter(None, ntp_servers))
        if not ntp_servers:
            ntp_servers = DEFAULT_NTP_SERVERS

        dns_servers = cfg["mgmt_network_int_config"].get("mgmt_dns", [])

        if use_deployment_node:
            dhcp_main_ip = cfg["basic"]["root_server_ip"]
        else:
            dhcp_main_ip = str(root_server.get_prop(props.prop_management_info_ip))
            config_root_server_ip = cfg["basic"]["root_server_ip"]
            if config_root_server_ip:
                assert dhcp_main_ip == config_root_server_ip, (
                    f"inconsistent root server ip detected. config input:"
                    f"{config_root_server_ip}, inventory: {dhcp_main_ip}"
                )

        cluster_name = cfg["basic"]["name"]
        is_docker_env = (
            cluster_name == "testmock"
        )  # TODO: better mock integration

        fw_manifest = "/opt/mlnx_fw/part_to_fw.json"
        fw_manifest_dir = pathlib.Path(fw_manifest).parent
        fw_manifest_dir.mkdir(parents=True, exist_ok=True)
        with open(fw_manifest, "w") as fw_file:
            fw_file.write(json.dumps(PART_TO_FW, indent=2))

        group_vars_dict = dict(
            dhcp_ip=dhcp_main_ip,
            dhcp_provider=dhcp_provider.name,
            ntp_servers=ntp_servers,
            nic_fw_files=list(PART_TO_FW.values()) + ["part_to_fw.json"],
            is_docker_env=is_docker_env,
            setup_network=False,
        )

        # setup mgmt network for root server if not using deployment node
        if not use_deployment_node:
            root_ip = str(root_server.get_prop(props.prop_management_info_ip))
            root_mac = root_server.get_prop(props.prop_management_info_mac)
            root_gateway = cfg["mgmt_network_int_config"]["mgmt_gateway_overwrite"]
            root_subnet = cfg["mgmt_network_int_config"]["mgmt_netmask_per_rack"].lstrip("/")
            conn = _get_nmcli_conn(root_mac)
            network_vars = dict(
                connection_name=conn,
                root_subnet=root_subnet,
                root_ip=root_ip,
                root_gateway=root_gateway,
                dns_servers=dns_servers,
                setup_network=True,
            )
            group_vars_dict.update(network_vars)

        group_vars_file.write_text(
            yaml.dump(group_vars_dict, default_flow_style=False)
        )

class ConfigureNics(AnsibleTask):

    def __init__(self, profile: str):
        super().__init__(profile)
        self._profile = profile

    def _generate_inventory(self, targets: Optional[List[str]] = None) -> Dict:
        return generate_full_inventory(self._profile, targets)

    def _generate_ansible_variable(self):
        fw_manifest = "/opt/mlnx_fw/part_to_fw.json"
        fw_manifest_dir = pathlib.Path(fw_manifest).parent
        fw_manifest_dir.mkdir(parents=True, exist_ok=True)
        with open(fw_manifest, "w") as fw_file:
            fw_file.write(json.dumps(PART_TO_FW, indent=2))

        group_vars_file = self.ANSIBLE_DIRECTORY / "group_vars" / "all"
        cg = ConfGen(self._profile)
        cfg = cg.parse_profile()
        group_vars_dict = dict(
            nic_fw_files=list(PART_TO_FW.values()) + ["part_to_fw.json"],
            enable_dcqcn=cfg.get('cluster_network_config', {}).get('enable_dcqcn', False)
        )
        group_vars_file.write_text(
            yaml.dump(group_vars_dict, default_flow_style=False)
        )

    def _ansible_playbook(self) -> Tuple[str, str]:
        cmd = "ansible-playbook configure-nic.yml"
        ret, output, err = exec_cmd(
            cmd, cwd=str(self.ANSIBLE_DIRECTORY.absolute()), logcmd=True
        )
        return output, err


class ConfigureDcqcn(AnsibleTask):

    def __init__(self, profile: str, action: str = "enable"):
        super().__init__(profile)
        self._action = action

    def _generate_inventory(self, targets: Optional[List[str]] = None) -> Dict:
        return generate_full_inventory(self._profile, targets)

    def _generate_ansible_variable(self):
        group_vars_file = self.ANSIBLE_DIRECTORY / "group_vars" / "all"
        group_vars_file.write_text(
            yaml.dump({"action": self._action}, default_flow_style=False)
        )

    def _ansible_playbook(self) -> Tuple[str, str]:
        cmd = "ansible-playbook configure-dcqcn.yml"
        ret, output, err = exec_cmd(
            cmd, cwd=str(self.ANSIBLE_DIRECTORY.absolute()), logcmd=True
        )
        return output, err


class DeployServers(AnsibleTask):

    def __init__(self, profile: str):
        super().__init__(profile)

    def _generate_inventory(self, targets: Optional[List[str]] = None):
        return generate_full_inventory(self._profile, targets)

    def _generate_ansible_variable(self):

        group_vars_file = self.ANSIBLE_DIRECTORY / "group_vars" / "all"

        cg = ConfGen(self._profile)
        cfg = cg.parse_profile()

        # If only 1 MG node in the inventory, RAID disk will be setup
        single_mgmt = (
            Device.get_all(
                self._profile,
                DeploymentDeviceRoles.MANAGEMENT.value,
                "SR",
            ).count()
            == 1
        )

        cluster_name = cfg["basic"]["name"]
        is_docker_env = (
            cluster_name == "testmock"
        )  # TODO: better mock integration

        group_vars_dict = dict(
            single_mgmt=single_mgmt,
            is_docker_env=is_docker_env,
            management_node=cfg["basic"]["root_server"],
            configure_log_storage=cfg["basic"].get("configure_log_storage", False),
        )

        group_vars_file.write_text(
            yaml.dump(group_vars_dict, default_flow_style=False)
        )

    def _ansible_playbook(self) -> Tuple[str, str]:
        cmd = "ansible-playbook configure-common.yml"
        ret, output, err = exec_cmd(
            cmd, cwd=str(self.ANSIBLE_DIRECTORY.absolute()), logcmd=True
        )
        return output, err

class UpdateDhcpMirrorServer(AnsibleTask):
    def __init__(self, profile: str):
        super().__init__(profile)

    def _generate_inventory(self, targets: List[str] = ()) -> Dict:
        cg = ConfGen(self._profile)
        cfg = cg.parse_profile()
        mirror_servers_names = cfg["basic"].get("mirror_dhcp_servers", [])
        mirror_servers = Device.get_all(self._profile, device_type="SR").filter(
            name__in=mirror_servers_names
        )

        assert mirror_servers.count() == len(mirror_servers_names), (
            f"Missing mirror server in the inventory, "
            f"expected: {' '.join(mirror_servers_names)}, "
            f"found: {' '.join([d.name for d in mirror_servers])}"
        )

        dhcp_mirror_hosts = {}
        for mirror_server in mirror_servers:
            name = mirror_server.name
            dhcp_mirror_hosts[name] = dict(
                ansible_host=str(mirror_server.get_prop(props.prop_management_info_ip)),
                ansible_user=mirror_server.get_prop(props.prop_management_credentials_user),
                ansible_password=mirror_server.get_prop(props.prop_management_credentials_password),
            )

        return {'dhcp-mirror-servers': {'hosts': dhcp_mirror_hosts}}

    def _generate_ansible_variable(self):
        dhcp_provider = dhcp.get_provider(self._profile, self._cfg)
        configs = dhcp_provider.generate_configs()
        self._write_dhcp_confs(configs)

        # generate group variables
        group_vars_file = self.ANSIBLE_DIRECTORY / "group_vars" / "dhcp-mirror-servers"

        ntp_servers = [s for s in self._cfg["basic"].get("ntp_servers", []) if s]
        if not ntp_servers:
            # Add a few IP addresses used by pool.ntp.org, in case there is no
            # DNS on the management node.
            logger.warning(f"input.yaml::basic.ntp_servers not set, using default {','.join(DEFAULT_NTP_SERVERS)}")
            ntp_servers = DEFAULT_NTP_SERVERS

        group_vars_dict = dict(
            ntp_servers=ntp_servers,
        )

        group_vars_file.write_text(
            yaml.dump(group_vars_dict, default_flow_style=False)
        )

    def _ansible_playbook(self) -> Tuple[str, str]:
        cmd = "ansible-playbook configure-dhcp-mirror-node.yml"
        ret, output, err = exec_cmd(
            cmd, cwd=str(self.ANSIBLE_DIRECTORY.absolute()), logcmd=True
        )
        return output, err


class UpdateDhcpServer(AnsibleTask):
    def __init__(self,
                 profile: str,
                 provider: Optional["DhcpProvider"] = None,
                 configs: Dict[str, List[Tuple[pathlib.Path, str]]] = None):
        """
        Args:
            provider: Optional DHCP provider
            configs: Optional generated DHCP configs. These can take a while to generate so optimize by passing in
        """
        super().__init__(profile)
        self._dhcp_provider = provider
        self._dhcp_configs = configs

    def _generate_inventory(self, targets: List[str] = ()):
        return {
            "all": {
                "hosts":
                    {"root-server": {"ansible_connection": "local"}}
            }
        }

    def _generate_ansible_variable(self):
        if not self._dhcp_provider:
            self._dhcp_provider = dhcp.get_provider(self._profile, self._cfg)
        if not self._dhcp_configs:
            self._dhcp_configs = self._dhcp_provider.generate_configs()

        self._write_dhcp_confs(self._dhcp_configs)
        group_vars_file = self.ANSIBLE_DIRECTORY / "group_vars" / "all"
        group_vars_file.write_text(
            yaml.dump({"dhcp_provider": self._dhcp_provider.name}, default_flow_style=False)
        )

    def _ansible_playbook(self) -> Tuple[str, str]:
        cmd = "ansible-playbook configure-dhcp-master.yml"
        rv, output, err = exec_cmd(
            cmd, cwd=str(self.ANSIBLE_DIRECTORY.absolute()), logcmd=True
        )
        return output, err

    def run(self, targets) -> Dict:
        rv = super().run(targets)

        # return early if the DHCP update failed
        for status in rv.values():
            if status.status != Status.OK:
                return rv

        # kind of a hack
        self._dhcp_provider.update_dns()
        return rv


class ConfigureFreeIpa(AnsibleTask):

    def __init__(self, profile: str):
        super().__init__(profile)
        cg = ConfGen(self._profile)
        self.config_input = cg.parse_profile()

    def _ansible_playbook(self) -> Tuple[str, str]:
        cmd = "ansible-playbook configure-freeipa.yml --extra-vars master_password=$MASTER_PASSWORD"

        freeipa_config = self.config_input.get("freeipa")
        ret, output, err = exec_cmd(
            cmd,
            cwd=str(self.ANSIBLE_DIRECTORY.absolute()),
            logcmd=True,
            env={"MASTER_PASSWORD": freeipa_config["master_password"]},
        )
        return output, err

    def _generate_ansible_variable(self):
        group_vars_file = self.ANSIBLE_DIRECTORY / "group_vars" / "all"
        group_vars_dict = dict(domain=self.config_input["basic"]["domain"])
        freeipa_config = self.config_input.get("freeipa")
        for k in [
            "master_username",
            "ipa_master_nis_domain",
            "ipa_master_server",
            "ipa_master_domain",
        ]:
            group_vars_dict[k] = freeipa_config[k]
        group_vars_file.write_text(
            yaml.dump(group_vars_dict, default_flow_style=False)
        )

    def _generate_inventory(self, targets: Optional[List[str]] = None) -> Dict:
        return generate_full_inventory(self._profile, targets)


class ConfigureNfsMount(AnsibleTask):

    def __init__(self, profile: str):
        super().__init__(profile)
        cg = ConfGen(self._profile)
        self.config_input = cg.parse_profile()

    def _ansible_playbook(self) -> Tuple[str, str]:
        cmd = (
            f"ansible-playbook nfs-mount.yml"
        )

        ret, output, err = exec_cmd(
            cmd,
            cwd=str(self.ANSIBLE_DIRECTORY.absolute()),
            logcmd=True,
        )
        return output, err

    def _generate_ansible_variable(self):
        group_vars_file = self.ANSIBLE_DIRECTORY / "group_vars" / "all"
        group_vars_dict = dict()
        mount_config = self.config_input.get("nfs_mount")
        for k in [
            "u_names",
            "u_ids",
            "u_passwords",
            "g_names",
            "g_ids",
            "server_paths",
            "mount_paths",
        ]:
            group_vars_dict[k] = mount_config[k]
        group_vars_file.write_text(
            yaml.dump(group_vars_dict, default_flow_style=False)
        )

    def _generate_inventory(self, targets: Optional[List[str]] = None) -> Dict:
        return generate_full_inventory(self._profile, targets)


class LockdownNodes(AnsibleTask):

    def __init__(self, profile: str):
        super().__init__(profile)
        cg = ConfGen(self._profile)
        self.config_input = cg.parse_profile()

    def _ansible_playbook(self) -> Tuple[str, str]:
        ret, output, err = exec_cmd(
            "ansible-playbook configure-root-access.yml",
            cwd=str(self.ANSIBLE_DIRECTORY.absolute()),
            logcmd=True,
        )
        return output, err

    def _generate_ansible_variable(self):
        pass

    def _generate_inventory(self, targets: Optional[List[str]] = None) -> Dict:
        return generate_full_inventory(self._profile, targets)
