import datetime
import json
import logging
import os
import pathlib
import shutil
import traceback
from typing import Dict, List, Tuple, Optional

import django.db.transaction
import yaml
from yaml import safe_load

from deployment_manager.common.models import SecretsProviderConfig, ClusterDeploymentConfig
from deployment_manager.conftool.confgen import write_master_config
from deployment_manager.db.const import (
    DeploymentDeviceTypes,
    DeploymentDeviceRoles
)
from deployment_manager.db.device_props import (
    device_type_has_property,
    prop_management_info_ip,
    INVENTORY_DEVICE_PROPS
)
from deployment_manager.db.models import (
    DeploymentProfile,
    DeploymentCtx,
    Device, Cluster,
)
from deployment_manager.network_config.schema.utils import DEFAULT_NODE_ROLE_IF_COUNT
from deployment_manager.tools.const import (
    CONFIG_BASEPATH,
    DEPLOYMENT_LOGS,
)
from deployment_manager.tools.utils import (
    exec_cmd,
    edit_file,
    make_dir,
    rm_dir,
    show_file
)

logger = logging.getLogger(__name__)

class ConfGen:

    def __init__(self, profile: str, config_dir=None):
        self._profile = profile
        if config_dir is None:
            self.config_dir = f"{CONFIG_BASEPATH}/{self._profile}"
        else:
            self.config_dir = f"{config_dir}"

        self.input_filename = "input.yml"
        self.inventory_filename = "inventory.csv"

    @property
    def profile(self):
        return self._profile

    @property
    def log_dir(self):
        return f"{DEPLOYMENT_LOGS}/{self._profile}"

    @property
    def network_config_file(self):
        return f"{self.config_dir}/network_config.json"

    @property
    def network_config_templates_dir(self):
        return f"{self.config_dir}/config/{self.profile}_out/"

    @property
    def dhcp_config_dir(self) -> pathlib.Path:
        conf_dir = f"{self.config_dir}/config/dhcp"
        conf_dir = pathlib.Path(conf_dir)
        conf_dir.mkdir(parents=True, exist_ok=True)
        return conf_dir

    @property
    def input_file(self):
        return f"{self.config_dir}/{self.input_filename}"

    @property
    def tmp_input_file(self):
        return f"{self.input_file}.tmp"

    @property
    def tmp_input_file_timestamp(self):
        now = datetime.datetime.now()
        return f"{self.input_file}-{now:%Y%m%d-%H%M%S}.tmp"

    @property
    def inventory_file(self):
        return f"{self.config_dir}/{self.inventory_filename}"

    @property
    def tmp_inventory_file(self):
        return f"{self.inventory_file}.tmp"

    @property
    def tmp_inventory_file_timestamp(self):
        now = datetime.datetime.now()
        return f"{self.inventory_file}-{now:%Y%m%d-%H%M%s}.tmp"

    @property
    def master_file(self):
        return f"{self.config_dir}/master_config.yml"

    @property
    def tmp_master_file(self):
        return f"{self.master_file}.tmp"

    @property
    def tmp_master_file_timestamp(self):
        now = datetime.datetime.now()
        return f"{self.master_file}-{now:%Y%m%d-%H%M%s}.tmp"

    def is_exists(self):
        """
            Check if the profile exists.
            If the directory path exits, then profile is considered to exists.
        """
        return os.path.isdir(self.config_dir)

    def create_deployment_dirs(self):
        profile_name = self._profile
        ret = make_dir(f"{self.config_dir}")
        if ret:
            logger.error(f"Failed to create config dir for {profile_name}")
        else:
            ret = make_dir(f"{self.log_dir}")
            if ret:
                logger.error(f"Failed to create log dir for {profile_name}")
        return ret

    def load_config(self) -> ClusterDeploymentConfig:
        """ Parse the input.yml file and return a ClusterDeploymentConfig object """
        f = pathlib.Path(self.input_file)
        if not f.is_file():
            raise ValueError(f"file {self.input_file} does not exist")
        doc = yaml.safe_load(f.read_text())
        return ClusterDeploymentConfig.model_validate(doc)

    def parse_profile(self, backup: bool = False) -> Optional[dict]:
        """
        Parse the input.yml file
        """
        fname = self.input_file if not backup else self.tmp_input_file
        if not os.path.isfile(fname):
            logger.error(f"Profile {self._profile} not found. Returning default values.")
            profile_config = dict()
        else:
            with open(fname, "r") as f:
                profile_config = safe_load(f)

        # Default values
        if "basic" not in profile_config:
            profile_config["basic"] = {}
        if "name" not in profile_config["basic"]:
            profile_config["basic"]["name"] = self._profile
        if "passwords" not in profile_config:
            profile_config["passwords"] = {}
        if "node_password" not in profile_config["passwords"]:
            profile_config["passwords"]["node_password"] = "5cerebras"
        if "switch_password" not in profile_config["passwords"]:
            profile_config["passwords"]["switch_password"] = "CerebrasColo"
        if "freeipa" not in profile_config:
            profile_config["freeipa"] = {"enabled": False}
        if "secrets_provider" not in profile_config:
            profile_config["secrets_provider"] = SecretsProviderConfig().model_dump()

        return profile_config

    def get_expected_role_nic_counts(self) -> Dict[str, int]:
        config = self.parse_profile()
        nic_count_override = config.get("basic", {}).get("hardware_expectations", {}).get("node_data_nic_count", {})
        return {
            role: nic_count_override.get(role.lower(), default_nic_count)
            for role, default_nic_count in DEFAULT_NODE_ROLE_IF_COUNT.items()
        }

    def _add_device_to_dict(self, device_dict, inv_dev, inv_col, prop, attr):
        # Skip special case
        if not device_type_has_property(inv_dev["Device"], prop):
            return

        if prop not in device_dict:
            device_dict[prop] = {}
        device_dict[prop][attr] = inv_dev[inv_col]

    def inventory_file_to_list(self, filename: Optional[str] = None, filetype: str = "csv") -> List[dict]:
        """
            Parses the inventory file and return a list of dicts that matches
            the Device databse
        """
        devices = self.inventory_file_to_dict(filename=filename, filetype=filetype)
        device_list = []
        for name, dev in devices.items():
            dev["name"] = name
            device_list.append(dev)
        return device_list

    def inventory_file_to_dict(self, filename:Optional[str]=None, filetype:str="csv") -> dict:
        """
            Parses the inventory file and return a dict that matches
            the Device databse
        """
        devices = {}
        if filetype == "csv":
            inventory, _ = self.parse_inventory_csv(csv_file=filename)
            for dev in inventory:
                d = {
                    "device_type": dev["Device"],
                    "device_role": dev["Role"]
                }

                for item in INVENTORY_DEVICE_PROPS.values():
                    self._add_device_to_dict(device_dict=d, inv_dev=dev, inv_col=item.title, prop=item.name, attr=item.attr)

                devices[dev["Name"]] = d
        elif filetype == "json":
            with open(filename, "r") as fp:
                devices = json.load(fp)
        elif filetype == "yaml":
            with open(filename, "r") as fp:
                devices = safe_load(fp)
        return devices

    def parse_inventory_csv(self, backup:bool=False, csv_file:Optional[str]=None) -> Tuple[List[Dict[str, str]], List[str]]:
        """ Parse inventory csv, return row entries and header """
        if csv_file:
            fname = csv_file
            if not os.path.exists(fname):
                logger.error(f"File {fname} does not exists")
                return [], []
        else:
            fname = self.inventory_file if not backup else self.tmp_inventory_file

        if not os.path.isfile(fname):
            logger.error(f"Inventory for {self._profile} not found")
            return [], []

        rows = []
        header = []
        with open(fname, "r") as f:
            for l in f:
                # skip empty lines
                if l.strip() == "":
                    continue
                # Skip lines that starts with '#'
                if l.startswith("#"):
                    continue
                if not header:
                    header = [e.strip() for e in l.split(',')]
                else:
                    entry = {}
                    for j, e in enumerate(l.split(',')):
                        entry[header[j]] = e.strip()
                    rows.append(entry)
        return rows, header

    def write_inventory(self, inventory_rows: List[Dict[str, str]], header: List[str]):
        """ Overwrite inventory csv file """
        if not os.path.isfile(self.inventory_file):
            raise RuntimeError(f"Inventory for {self._profile} not found")

        with open(self.inventory_file, "w") as f:
            f.write(",".join(header) + "\n")
            for row in inventory_rows:
                f.write(",".join([row[k] for k in header]) + "\n")

    def update_inventory_from_file(self, filename:Optional[str]=None, filetype:str="csv"):
        """
            Update the Device database from inventory file
            New Devices are added, but existing ones will have properties updated.
            If new Devices, device data must include device_type and device_role.
        """
        if filename is None:
            filename = self.inventory_file
            filetype = "csv"

        # Get devices from the inventory file
        try:
            devices = self.inventory_file_to_dict(filename=filename, filetype=filetype)
        except Exception as exc:
            logger.error(exc)
            return 1

        profile = DeploymentProfile.get_profile(self._profile)
        for name, dev in devices.items():
            d:Device = Device.get_device(name=name, profile_name=self._profile)
            if not d:
                d = Device.add(
                    name=name,
                    device_type=dev["device_type"],
                    device_role=dev["device_role"],
                    profile=profile)
            else:
                if "device_type" in dev:
                    d.device_type = dev["device_type"]
                if "device_role" in dev:
                    d.device_role = dev["device_role"]
            d.batch_update_properties(dev)

    def parse_master_file(self, backup:bool=False):
        fname = self.master_file if not backup else self.tmp_master_file
        if not os.path.isfile(fname):
            return None
        with open(fname, "r") as f:
            return safe_load(f)

    def get_root_server(self):
        cfg = self.parse_profile()
        return cfg.get("basic", {}).get("root_server") if cfg else None

    def get_root_server_ip(self):
        rs_ip = None
        rs = self.get_root_server()
        if rs is not None:
            rs_dev = Device.get_device(rs, self._profile)
            if rs_dev is not None:
                rs_ip = rs_dev.get_prop(prop_management_info_ip)
        return rs_ip

    def get_devices(self, return_type:str="list"):
        """
            Parse the master config for devices
            return_type specifies either return a list or a dict key by [device type][device name]
        """
        if return_type == "list":
            devices = []
        else:
            devices = {}

        mcfg = self.parse_master_file()
        if mcfg is not None:
            keys = [
                ('servers', DeploymentDeviceTypes.SERVER.value),
                ('switches', DeploymentDeviceTypes.SWITCH.value),
                ('systems', DeploymentDeviceTypes.SYSTEM.value)
            ]
            for k, t in keys:
                if return_type == "dict":
                    devices[k] = {}
                for s in mcfg.get(k, []):
                    d = {
                        'name': s['name'],
                        'device_type': t,
                        'device_role': s['role'],
                        'login_user': s.get('login', {}).get('username', None),
                        'login_password': s.get('login', {}).get('password', None)
                    }
                    if k == 'servers':
                        if s.get('ipmiLogin'):
                            d['ipmi_user'] = s['ipmiLogin']['username']
                            d['ipmi_password'] = s['ipmiLogin']['password']
                            d['login_user'] = s['rootLogin']['username']
                            d['login_password'] = s['rootLogin']['password']

                        ipmi_dns_name = None
                        ipmi_ip_address = None
                        ports = s.get('ports', [])
                        for port in ports:
                            if port['name'] == 'ILO':
                                ipmi_dns_name = port['dnsName']
                                ipmi_ip_address = port['address']
                                break

                        if not ipmi_dns_name:
                            raise ValueError(f"Cannot find ipmi DNS for {s['name']}")
                        if not ipmi_ip_address:
                            raise ValueError(f"Cannot find ipmi ip for {s['name']}")

                        d['ipmi_dns_name'] = ipmi_dns_name
                        d['ipmi_ip'] = ipmi_ip_address

                    if return_type == "list":
                        devices.append(d)
                    else:
                        devices[k][s['name']] = d
        return devices

    def delete_network_config(self):
        exec_cmd(f"rm -f {self.network_config_file}")

    def generate_inventory_csv(self,
                               filepath: Optional[str] = None,
                               empty_ok=False):
        """
            Generate the inventory.csv file from database

            The default filepath is '<cluster-deployment root>/meta/<profile name>/inventory.csv'
        """
        devices = Device.get_all(profile_name=self._profile)
        if len(devices) == 0 and not empty_ok:
            logger.info("Inventory not created, no devices found in the database")
            return

        # If no path
        if filepath is None:
            filepath = self.inventory_file

        # remove existing inventory file
        if os.path.exists(filepath):
            os.unlink(filepath)

        # generate the header
        header = ["Name", "Role", "Device"]
        keys = [k.title for k in INVENTORY_DEVICE_PROPS.values()]
        header.extend(keys)

        # Retrieve inventory data from the database
        inventory = self.get_inventory_dict()

        with open(filepath, "w") as fp:
            # Write comments
            fp.write(f"# Do not edit.  This file is generated by CSCFG {self._profile}\n")
            fp.write(f"# Created on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            fp.write(",".join(header) + "\n")
            for dev_name, dev in inventory.items():
                line = [dev_name, dev["device_role"], dev["device_type"]]
                for dp in INVENTORY_DEVICE_PROPS.values():
                    value = dev.get(dp.name, {}).get(dp.attr, "")
                    line.append(value if value is not None else "")
                fp.write(",".join(line) + "\n")

    def get_inventory_dict(self) -> dict:
        """
        Retrieve the inventory data from the database
        """
        inventory = {}
        for dev in Device.get_all(profile_name=self._profile):
            inventory[dev.name] = {
                "device_obj": dev,
                "device_type": dev.device_type,
                "device_role": dev.device_role
            }

            properties = dev.get_device_inventory_dict()
            inventory[dev.name].update(properties)
        return inventory

    def edit_profile(self):
        """ Edit the config input file """
        edit_file(self.input_file)

    def edit_inventory(self):
        edit_file(self.inventory_file)

    def show_profile(self):
        show_file(self.input_file)

    def show_network_config(self):
        if os.path.isfile(self.network_config_file):
            show_file(self.network_config_file)
        else:
            logger.error(
                f"Network configuration has not been generated for {self._cluster_name}"
            )

    def backup_input_file(self):
        if os.path.isfile(self.input_file):
            shutil.copy(self.input_file, self.tmp_input_file)
            shutil.copy(self.input_file, self.tmp_input_file_timestamp)

    def restore_input_file(self):
        if os.path.isfile(self.tmp_input_file):
            shutil.copy(self.tmp_input_file, self.input_file)

    def backup_inventory_file(self):
        if os.path.isfile(self.inventory_file):
            shutil.copy(self.inventory_file, self.tmp_inventory_file)
            shutil.copy(self.inventory_file, self.tmp_inventory_file_timestamp)

    def restore_inventory_file(self):
        if os.path.isfile(self.tmp_inventory_file):
            shutil.copy(self.tmp_inventory_file, self.inventory_file)

    def backup_master_config(self):
        if os.path.isfile(self.master_file):
            shutil.copy(self.master_file, self.tmp_master_file)
            shutil.copy(self.master_file, self.tmp_master_file_timestamp)

    def restore_master_config(self):
        if os.path.isfile(self.tmp_master_file):
            shutil.copy(self.tmp_master_file, self.master_file)

    def write_master_config(self) -> bool:
        """ Create the master_config.yml file """
        self.backup_master_config()
        try:
            write_master_config(self.input_file, Device.get_all(self.profile), self.master_file)
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Failed to write master config: {e}")
            self.restore_master_config()
            return False
        return True

    class K8sConfig:
        def __init__(self, cluster_yaml_file):
            with open(cluster_yaml_file, 'r') as f:
                self._cfg = safe_load(f)
            self.cluster_name = self._cfg["name"]
            self.devices = self._get_devices()

        def _get_devices(self):
            device_types = [
                ('nodes', DeploymentDeviceTypes.SERVER.value),
                ('systems', DeploymentDeviceTypes.SYSTEM.value)
            ]
            node_roles = {
                'activation': DeploymentDeviceRoles.ACTIVATION.value,
                'management': DeploymentDeviceRoles.MANAGEMENT.value,
                'worker': DeploymentDeviceRoles.WORKER.value,
                'memory': DeploymentDeviceRoles.MEMORYX.value,
                'broadcastreduce': DeploymentDeviceRoles.SWARMX.value
            }
            devices = list()
            for device_type, device_type_enum in device_types:
                for entry in self._cfg.get(device_type, []):
                    if device_type == "systems":
                        dm_role = DeploymentDeviceRoles.SYSTEM.value
                    else:  # nodes
                        dm_role = node_roles.get(entry.get('role', ""))
                        if not dm_role:
                            logger.warning(f"device {entry['name']} did not have a recognizable role, was {entry}, skipping")
                            continue

                    devices.append({
                        'name': entry['name'],
                        'device_type': device_type_enum,
                        'device_role': dm_role,
                    })
            return devices

    def parse_k8s_config(self):
        fname = "/tmp/cluster.yaml"
        cfg = None
        retcode, _, err = exec_cmd(
            "kubectl get cm cluster -n job-operator "
            " -ojsonpath='{.data.clusterConfiguration\.yaml}' >"
            f" {fname}"
        )
        if retcode:
            logger.error(f"Failed to get k8s cluster config: {err}")
        else:
            cfg = self.K8sConfig(fname)
        exec_cmd(f"rm {fname}")
        return retcode, cfg

    class NetworkConfig:
        def __init__(self, network_json_file):
            with open(network_json_file, 'r') as f:
                self._cfg = json.load(f)
            self.switches = self._get_switch_devices()

        def _get_switch_devices(self):
            keys = [
                ('switches', DeploymentDeviceTypes.SWITCH.value)
            ]
            devices = list()
            for k, t in keys:
                for s in self._cfg.get(k, []):
                    devices.append({
                        'name': s['name'],
                        'device_type': t
                    })
            return devices

    def parse_network_config(self, network_json_file):
        return self.NetworkConfig(network_json_file)


class ClusterProfile:
    @staticmethod
    def create(profile: str, input_file: str, inventory_file: Optional[str]):
        """
        Create the profile.
        A directory in the meta directory will be created.
        The input and inventory file will be copied to the new directory.
        The profile will be initiated in the database with the inventory data if given
        """
        cg = ConfGen(profile)

        if os.path.exists(cg.config_dir):
            # rename directory to save it
            backupdir = cg.config_dir + "." + str(int(datetime.datetime.now().timestamp()))
            logger.info(f"Meta directory already exists.  Backing it up to {backupdir}")
            shutil.move(cg.config_dir, backupdir)

        ret = cg.create_deployment_dirs()
        if ret:
            logger.error(
                f"Failed to setup environment for {profile}: {ret}"
            )
            return ret

        # If files provided by cli, copy them
        shutil.copy(input_file, cg.input_file)

        # get cluster name
        cfg = cg.parse_profile()
        cluster_name = cfg["basic"]["name"]
        mgmt_vip = cfg.get("mgmt_network_int_config", {}).get("mgmt_vip", {}).get("vip", "")

        # Get devices from the inventory file if given
        devices = []
        if inventory_file:
            shutil.copy(inventory_file, cg.inventory_file)
            try:
                devices = cg.inventory_file_to_list()
            except Exception as exc:
                logger.error(exc)
                return 1

        # Create database entries
        ClusterProfile.initialize_profile_database(profile, cluster_name, devices, mgmt_vip=mgmt_vip)

        # Temporary, when we obsolete master-config, replace this with a
        # inventory validation to ensure required fields are present
        if not cg.write_master_config():
            logger.error(f"Failed to create master-config for profile {profile}")
            dp = DeploymentProfile.get_profile(profile)
            dp.mark_not_deployable()
            return 1

        return 0

    @staticmethod
    @django.db.transaction.atomic
    def initialize_profile_database(profile:str, cluster_name:str, devices:List[Dict], deployable:bool=True, **kwargs):
        # create profile record in db
        profile_record = DeploymentProfile.create(
            profile, cluster_name, deployable=deployable
        )

        # create the Cluster associated with the profile

        # add devices to db
        for d in devices:
            dev = Device.add(d['name'], d['device_type'], d.get('device_role', ""), profile_record)
            dev.batch_update_properties(d)

        # create deployment ctx record in db
        DeploymentCtx.create(profile_record)

        Cluster(
            name=cluster_name,
            dns_name="",
            is_primary=True,
            management_vip=kwargs.get("mgmt_vip", ""),
            profile=profile_record
        ).save()

    @staticmethod
    def get(name:str):
        cg = ConfGen(name)
        if os.path.isfile(cg.input_file):
            return cg.input_file
        else:
            return None

    @staticmethod
    def edit_config(profile:str, input_file:Optional[str]=None) -> bool:
        # Returns True if there are changes
        cg = ConfGen(profile)
        old_cfg = cg.parse_profile()
        cg.backup_input_file()
        if input_file:
            shutil.copy(input_file, cg.input_file)
        cg.edit_profile()
        new_cfg = cg.parse_profile()

        if old_cfg != new_cfg:
            # TODO: add finer-grained checks to decide which
            # part of deployment needs
            # to re-run. For now, re-run network init onwards.
            ClusterProfile.force_network_init(profile)

            # Update the cluster_name if it changes
            p = DeploymentProfile.get_profile(profile)
            if p is not None and p.cluster_name != new_cfg["basic"]["name"]:
                p.cluster_name = new_cfg["basic"]["name"]
                p.save()

            return True

        return False

    @staticmethod
    def edit_inventory(profile:str, inventory_file:Optional[str]=None):
        """
            Read inventory file as a backup before allowing user to edit.
            Return the edited version as the new_inventory.
            If an inventory_file is provided, that file is edited instead of the
            profiled version
        """
        cg = ConfGen(profile)
        old_inventory, _ = cg.parse_inventory_csv()
        cg.backup_inventory_file()
        if inventory_file:
            shutil.copy(inventory_file, cg.inventory_file)
        cg.edit_inventory()
        new_inventory, _ = cg.parse_inventory_csv()
        return old_inventory, new_inventory

    @staticmethod
    def show_config(name):
        cg = ConfGen(name)
        cg.show_profile()

    @staticmethod
    def show_network_config(name):
        cg = ConfGen(name)
        cg.show_network_config()

    @staticmethod
    def delete(name):
        cg = ConfGen(name)
        rm_dir(cg.log_dir)
        rm_dir(cg.config_dir)

    @staticmethod
    def force_network_init(profile):
        cg = ConfGen(profile)
        for d in Device.get_all(profile):
            d.rewind_to_network()
        cg.delete_network_config()


def init_profile_deployability():
    """
    Profiles that have been created before the 'deployable' flag
    was updated correctly as part of create/import need to have
    the flag set appropriately for them.
    Profiles that don't have an associated input yaml file
    will have their deployable flag set to False
    """
    for p in DeploymentProfile.list():
        cg = ConfGen(p.name)
        if not os.path.isfile(cg.input_file):
            p.mark_not_deployable()
