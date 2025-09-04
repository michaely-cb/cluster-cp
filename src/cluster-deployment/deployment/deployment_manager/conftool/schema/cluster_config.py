import json
import jsonschema
import yaml
from pkg_resources import resource_string
from typing import Union, List

from deployment_manager.common import models
from deployment_manager.conftool.schema.cluster_types import FUNCTION_TYPES, DEVICE_TYPES, TYPE_TO_LONG
from deployment_manager.tools.utils import snake_to_camel


class SingletonMeta(type):
    """
    The Singleton class
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class ClusterSchemaValidator:
    """
    Cluster schema validator
    """

    schema_file_path = "./cluster_config_schema.json"

    def __init__(self):
        schema_buf = resource_string(
            __name__, ClusterSchemaValidator.schema_file_path
        )
        self._schema = json.loads(schema_buf)

        # init format checker
        format_checker = jsonschema.FormatChecker()
        self.validator = jsonschema.validators.Draft202012Validator(
            schema=self._schema, format_checker=format_checker
        )

    def load_json(self, data_filename):
        with open(data_filename) as input_file:
            data_obj = json.load(input_file)

        self.validate(data_obj)
        return data_obj

    def save_json(self, data_obj, data_filename):
        self.validate(data_obj)

        with open(data_filename, "w") as output_file:
            json.dump(data_obj, output_file, sort_keys=True, indent=4)
            output_file.flush()

    def save_yaml(self, data_obj, data_filename):
        self.validate(data_obj)

        with open(data_filename, "w") as output_file:
            yaml.dump(data_obj, output_file, indent=4, default_flow_style=False)

    def is_valid(self, data_obj):
        return self.validator.is_valid(data_obj)

    def validate(self, data_obj):
        self.validator.validate(data_obj)


class ClusterConfigData(metaclass=SingletonMeta):
    """
    Cluster configuration data
    """

    def __init__(self):
        self._data_obj = dict(
            name="",
            network={},
            clusters=[], systems=[], servers=[], switches=[], misc={}, meta={}
        )
        self._validator = ClusterSchemaValidator()
        self._device_cache = {}

        # device cache
        for device_type in DEVICE_TYPES:
            self._device_cache[device_type] = {}

    def validate(self):
        res = self._validator.is_valid(self._data_obj)
        self._validator.validate(self._data_obj)
        assert res, "config is not valid"

    def is_valid(self):
        return self._validator.is_valid(self._data_obj)

    def find_device_list(self, device_type) -> List[dict]:
        assert device_type in DEVICE_TYPES, f'unsupported device {device_type}'

        key = device_type + "es" if device_type == "switch" else device_type + "s"
        if key not in self._data_obj:
            self._data_obj[key] = []
        return self._data_obj[key]

    def find_device(self, device_type, name):
        return self._device_cache.get(device_type, {}).get(name, None)

    def check_update_root_server_info(self, server_name, server_ip):
        """ check the rootServer and determine if rootServerIp needs to be set
        """
        if server_name != self._data_obj["misc"]["rootServer"]:
            return
        # If rootServerIp is already defined, skip
        if "rootServerIp" in self._data_obj["misc"]:
            return
        self._data_obj["misc"]["rootServerIp"] = server_ip
        self.validate()

    def add_meta(self, key: str, value):
        self._data_obj["meta"][key] = value
        self.validate()

    def update_from_clusterconfig(self, cluster_config: models.ClusterDeploymentConfig) -> "ClusterConfigData":
        """ Update the configuration data from a ClusterDeploymentConfig """
        # header info
        basic_config = cluster_config.basic
        self._data_obj["name"] = basic_config.name
        self._data_obj["misc"] = {
            "rootServer": basic_config.root_server,
            "rootServerIp": basic_config.root_server_ip,
            "topology": basic_config.topology or "memx_swarmx",
        }

        # mgmt network
        src = cluster_config.mgmt_network_int_config
        dst = {
            "podCidrs": src.mgmt_pod_cidr,
            "serviceCidr": src.mgmt_service_cidr,
            "disableCsxIpAssignment": src.disable_csx_ip_assignment
        }
        if src.mgmt_vip is not None:
            dst["mgmtVip"] = {
                "routerAsn": src.mgmt_vip.router_asn,
                "nodeAsn": src.mgmt_vip.node_asn,
                "vip": src.mgmt_vip.vip,
            }
        self._data_obj["network"]["managementNetwork"] = dst

        # data network
        dnc: models.DataNetworkConfig = cluster_config.cluster_network_config
        dst_cfg = {
            "ipConfig": {
                "ipSuperCidr": dnc.cluster_ip_block,
                "subnetPools": [
                    sp.model_dump() for sp in (dnc.subnet_pools or [])
                ],
                "clusterNetmaskPerRack": dnc.cluster_netmask_per_rack,
            },
            "clusterMemxSubnetSize": dnc.cluster_memx_subnet_size,
            "clusterSwarmxSubnetSize": dnc.cluster_swarmx_subnet_size,
            "clusterSystemSubnetSize": dnc.cluster_system_subnet_size or 128,
            "crossConnects": dnc.cluster_xconn_count,
            "vipCount": dnc.cluster_vip_count,
            "clusterAsns": [d.model_dump() for d in dnc.cluster_asns or []],
            "clusterMemxInterfaces": dnc.cluster_memx_interfaces or 48,
            "systemEnableDcqcn": dnc.system_enable_dcqcn,
            "systemsWithVlan": dnc.systems_with_vlan or [],
            "clusterIpBlocks": [{"rack": b.rack, "ipBlock": b.ip_block} for b in dnc.cluster_ip_blocks or []],
            "clusterSystemIpBlocks": [{"rack": b.rack, "ipBlock": b.ip_block} for b in dnc.system_ip_blocks or []],
            "systemConnectionsFile": dnc.system_connections_file or "",
        }

        if dnc.cluster_cs_vip_prefix:
            dst_cfg["ipConfig"]["ipVipCidr"] = dnc.cluster_cs_vip_prefix

        uplinks = []
        for ul in dnc.uplinks:
            uplink = {
                "extDownlinkIp": ul.ext_downlink_ip,
                "extSwitchName": ul.ext_switch_name,
                "extSwitchPort": ul.ext_switch_port,
                "extSwitchAsn": ul.ext_switch_asn,
                "uplinkIp": ul.uplink_ip,
                "memxSwitch": ul.memx_switch,
                "memxSwitchPort": ul.memx_switch_port
            }
            uplinks.append(uplink)
        dst_cfg["uplinks"] = uplinks
        self._data_obj["network"]["clusterNetwork"] = dst_cfg

        # exterior network
        ext_cfg: models.ExteriorDataNetworkConfig = cluster_config.exterior_network_config
        self._data_obj["network"]["exteriorNetwork"] = {"prefixes": ext_cfg.prefixes}

        self.validate()
        return self

    def config_name(self):
        return self._data_obj["name"]

    def network_config(self):
        return self._data_obj["network"]

    def data(self):
        return self._data_obj

    def save_yaml(self, outname=None):
        if not outname:
            file_name = self.config_name()
        else:
            file_name = outname
        self._validator.save_yaml(self._data_obj, file_name)

    def add_or_update_device(
            self, device_type, new_item, validate=False
    ):
        assert device_type in DEVICE_TYPES, f'unsupported device {device_type}'
        assert "name" in new_item.keys(), f"device name is required: {new_item}"

        target_device = self.find_device(
            device_type, new_item["name"]
        )

        # update an existing device
        if target_device:
            target_device.update(new_item)
            self.validate()
            return

        device_list = self.find_device_list(device_type)
        device_list.append(new_item)

        # update device cache
        cache_needs_update = self._device_cache[device_type]
        assert new_item["name"] not in cache_needs_update
        cache_needs_update[new_item["name"]] = new_item

        if validate:
            self.validate()
