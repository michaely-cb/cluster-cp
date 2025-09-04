import abc
import dataclasses
import ipaddress
import json
import logging
import pathlib
import typing
from dataclasses import dataclass

from . import NUM_EXPECTED_CS_PORTS

from deployment_manager.common.models import MgmtNetworkConfig, Cluster
from deployment_manager.network_config.schema.schema import SCHEMA
from deployment_manager.tools.ssh import ExecMixin, SSHConn
from deployment_manager.tools.switch.interface import SwitchCtl
from deployment_manager.tools.switch.interface import SwitchInfo
from deployment_manager.tools.switch.switchctl import create_client as create_switchctl
from deployment_manager.tools.system import SystemCtl

logger = logging.getLogger(__name__)

OBJ_TYPE_SR = "SR"
OBJ_TYPE_SY = "SY"
OBJ_TYPE_SW = "SW"

_OBJ_TYPE_CONFIG_DIR = {
    OBJ_TYPE_SR: "_nodes",
    OBJ_TYPE_SY: "systems",
    OBJ_TYPE_SW: "switches",
}

@dataclass
class ObjCred:
    host: str
    username: str
    password: str

ObjCredProviderFn = typing.Callable[[str], typing.Optional[ObjCred]]

@dataclass
class Entity:
    type: str  # SY|SW|SR
    role: str  # activation|management|...
    obj: dict

    def __repr__(self):
        return f"{self.type}:{self.role}:{self.obj['name']}"


DEFAULT_VRF_NAME = "cs1data"


@dataclass
class Vlan:
    network_addr: ipaddress.IPv4Address
    prefixlen: int
    gateway: ipaddress.IPv4Address
    number: int

    @classmethod
    def from_gw_prefix(cls, prefix: str, number: int) -> 'Vlan':
        parts = prefix.split("/")
        if len(parts) != 2:
            raise ValueError(f"expected format IP/PREFIX, got: '{prefix}'")
        nw = ipaddress.ip_network(prefix, strict=False)
        gw = ipaddress.ip_address(parts[0])
        return cls(nw.network_address, nw.prefixlen, gw, number)


K_POOL_CLASS_MX = "mx_vlan"
K_POOL_CLASS_SX = "sx_vlan"
K_POOL_CLASS_SY = "sy_vlan"
K_POOL_CLASS_XC = "xconnect"
K_POOL_CLASS_VIP = "vip"
K_POOL_CLASS_NONE = ""

@dataclasses.dataclass(frozen=True)
class _VlanConsts:
    provided_prefix_key: str
    prefix_key: str
    gateway_key: str
    tier_prefix_key: str
    tier_class_map: dict


DEFAULT_VLAN = _VlanConsts(
    provided_prefix_key="provided_prefix",
    prefix_key="prefix",
    gateway_key="address",
    tier_prefix_key="starting_prefix",
    tier_class_map={
        "AW": K_POOL_CLASS_MX,
        "BR": K_POOL_CLASS_SX,
        "LF": K_POOL_CLASS_MX,
    },
)

SX_VLAN = _VlanConsts(
    provided_prefix_key="provided_swarmx_prefix",
    prefix_key="swarmx_prefix",
    gateway_key="swarmx_vlan_address",
    tier_prefix_key="swarmx_starting_prefix",
    tier_class_map={
        "LF": K_POOL_CLASS_SX,
    }
)

SYS_VLAN = _VlanConsts(
    provided_prefix_key="provided_system_prefix",
    prefix_key="system_prefix",
    gateway_key="system_vlan_address",
    tier_prefix_key="system_starting_prefix",
    tier_class_map={
        "BR": K_POOL_CLASS_SY,
        "LF": K_POOL_CLASS_SY,
    }
)


def lookup_vendor(vendor: str) -> typing.Optional[str]:
    """ Convert network doc vendor strings to cscfg vendor strings """
    vendor_dict = {
        "arista": "AR",
        "hpe": "HP",
        "juniper": "JU",
        "dell": "DL",
        "edgecore": "EC"
    }
    return vendor_dict.get(vendor)


class NetworkCfgDoc:
    """ Wrapper around the network config. Not threadsafe. """

    def __init__(self, data: dict):
        self._data = data
        self._cache: typing.Optional[typing.Dict[str, Entity]] = None
        self._vlan_cache_data: typing.Optional[typing.Dict[ipaddress.IPv4Address, Vlan]] = None  # nw addr -> vlan
        self._config_cache_data: typing.Optional[typing.Dict[str, ConfigSection]] = None

    @classmethod
    def from_file(cls, path: str) -> 'NetworkCfgDoc':
        with open(path, 'r') as f:
            doc = json.loads(f.read())
        return cls(doc)

    @property
    def _entity_cache(self) -> typing.Dict[str, Entity]:
        if self._cache is not None:
            return self._cache
        cache, _type, _role = {}, "", ""
        for key, value in self._data.items():
            if not isinstance(value, list):
                continue
            if key == "systems":
                _type, _role = OBJ_TYPE_SY, OBJ_TYPE_SY
            elif key == "switches":
                _type, _role = OBJ_TYPE_SW, OBJ_TYPE_SW  # 'tier' could make sense as a role
            elif key.endswith("_nodes"):
                _type, _role = OBJ_TYPE_SR, key[:-len("_nodes")]
            else:
                continue
            for obj in value:
                cache[obj["name"]] = Entity(_type, _role, obj)
        self._cache = cache
        return self._cache

    @property
    def _vlan_cache(self) -> typing.Dict[ipaddress.IPv4Address, Vlan]:
        if self._vlan_cache_data is not None:
            return self._vlan_cache_data
        vlans = {}
        for sw in self._data.get("switches", []):
            for vlan_type in (DEFAULT_VLAN, SYS_VLAN, SX_VLAN):
                gw_key, vlan_key = vlan_type.gateway_key, vlan_type.prefix_key
                if gw_key in sw and vlan_key in sw:
                    vlan = Vlan.from_gw_prefix(sw[gw_key], sw[vlan_key])
                    vlans[vlan.network_addr] = vlan
        self._vlan_cache_data = vlans
        return self._vlan_cache_data


    def topology(self) -> str:
        return self._data.get("environment", {}).get("topology", "memx_swarmx")

    def is_inference_cluster(self) -> bool:
        # heuristic: inference clusters don't have SX, MX or WK
        for role in ("worker", "memoryx", "swarmx"):
            if len(self._data.get(f"{role}_nodes", [])) > 0:
                return False
        return True

    def gateway_index(self) -> int:
        return int(self._data.get("environment", {}).get("gateway_index", -1))

    def get_vlan_for_ip(self, addr: str) -> typing.Optional[Vlan]:
        if "/" in addr:
            nw = ipaddress.ip_network(addr, strict=False)
            if nw.network_address in self._vlan_cache:
                return self._vlan_cache[nw.network_address]
            return None
        # if we were using python3.11 reliably, then we'd store a list of sorted vlans and use bisect to find
        # the vlan by network address.
        nw = ipaddress.ip_network(f"{addr}/32", strict=False)
        while nw.prefixlen > 0:
            vlan = self._vlan_cache.get(nw.network_address)
            if vlan is not None:
                return vlan
            nw = nw.supernet(prefixlen_diff=1)
        return None

    def raw(self) -> dict:
        return self._data

    def get_names_by_type(self, obj_type: str) -> typing.List[str]:
        return [name for name, e in self._entity_cache.items() if e.type == obj_type]  # could be optimized...

    def get_entity(self, name: str) -> typing.Optional[Entity]:
        return self._entity_cache.get(name)
    
    def get_raw_object(self, name: str) -> typing.Optional[dict]:
        e = self._entity_cache.get(name)
        if e:
            return e.obj
        return None

    def update_raw_object(self, name: str, fields: dict) -> typing.Optional[dict]:
        # TODO: handle name update
        obj = self.get_raw_object(name)
        if obj is not None:
            obj.update(fields)
        return obj

    def get_objcreds(self, name: str) -> typing.Optional[ObjCred]:
        """
        Note that we shouldn't store credentials in the network config doc. This method should be replaced with a
        Provider from a secure source.
        Returns
            credentials from the network config if present
        """
        obj = self.get_raw_object(name)
        if not obj:
            return None
        host = obj.get('management_address', obj.get('name', ""))
        user, pw = obj.get("username"), obj.get("password")
        if bool(host) and bool(user):  # password optional - could be passwordless login
            return ObjCred(host=host, username=user, password=pw)
        return None

    def get_switch_names(self) -> typing.List[str]:
        return self.get_names_by_type(OBJ_TYPE_SW)

    def get_system_version(self, name: str) -> typing.Optional[str]:
        system = self.get_raw_object(name)
        if not system:
            return None
        return system.get("version")

    def get_system_model(self, name: str) -> typing.Optional[str]:
        system = self.get_raw_object(name)
        if not system:
            return None
        return system.get("model")

    def get_system_expected_ports(self, name: str) -> int:
        model = self.get_system_model(name)
        return NUM_EXPECTED_CS_PORTS[model]


class AppCtx(abc.ABC):
    @abc.abstractmethod
    def get_mgmt_network_config(self) -> MgmtNetworkConfig:
        """ Global management network configuration. """
        pass

    @abc.abstractmethod
    def get_clusters(self) -> typing.List[Cluster]:
        """ Get the list of k8 clusters in the deployment context. """
        pass

    @abc.abstractmethod
    def get_obj_creds(self, name: str) -> typing.Optional[ObjCred]:
        """ Get device credentials for a given name."""
        pass


class NetworkCfgCtx:
    def __init__(
            self,
            network_config_path: str,
            app_ctx: typing.Optional[AppCtx] = None,
            deploy_config: typing.Optional['DeploymentConfig'] = None,
            config_base_dir: str = "",
            persist_config_updates=False,
    ):
        self._network_config_path = network_config_path
        self._app_ctx = app_ctx
        self._cred_providers = [app_ctx.get_obj_creds] if app_ctx else []

        self._deploy_config = deploy_config
        self._config_base_dir = config_base_dir
        if self._deploy_config and not config_base_dir:
            self._config_base_dir = self._deploy_config.output_directory

        self.network_config: typing.Optional[NetworkCfgDoc] = None
        self.persist_config_updates = persist_config_updates

    def __enter__(self) -> 'NetworkCfgCtx':
        self.network_config = NetworkCfgDoc.from_file(self._network_config_path)
        SCHEMA.validate(self.network_config.raw())

        # network config is default fallback credential provider
        self._cred_providers.append(self.network_config.get_objcreds)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # TODO: optimize to only validate / save on change
        if self.persist_config_updates:
            SCHEMA.save_json(self.network_config.raw(), self._network_config_path)

    def get_app_ctx(self) -> typing.Optional[AppCtx]:
        return self._app_ctx

    def get_config_dir(self, target: str) -> pathlib.Path:
        """
        Returns:
             basedir for configuration artifacts for given target
        """
        e = self.network_config.get_entity(target)
        if not e:
            raise ValueError(f"entity {e} was not found in network_config")

        if not self._config_base_dir:
            raise AssertionError("config_base_dir not provided, pass 'deploy_config' or 'config_base_dir' to NetworkConfigCtx.__init__")

        class_dir = _OBJ_TYPE_CONFIG_DIR[e.type]
        if e.type == OBJ_TYPE_SR:
            class_dir = f"{e.role}{class_dir}"
        return pathlib.Path(self._config_base_dir) / class_dir / e.obj['name']

    def _must_get_creds(self, name: str) -> ObjCred:
        for provider in self._cred_providers:
            creds = provider(name)
            if creds is not None:
                return creds
        raise ValueError(f"invalid state: no credentials provider found for {name}")

    def get_conn(self, name: str) -> ExecMixin:
        """ Returns unopened SSHConn """
        creds = self._must_get_creds(name)
        return SSHConn(creds.host, creds.username, creds.password)

    def get_systemctl(self, name: str) -> SystemCtl:
        """ Returns SystemCtl instance """
        creds = self._must_get_creds(name)
        return SystemCtl(creds.host, creds.username, creds.password)

    def get_switchctl(self, name: str) -> SwitchCtl:
        """ Returns SwitchCtl instance """
        obj = self.network_config.get_raw_object(name)
        creds = self._must_get_creds(name)
        info = SwitchInfo(name, creds.host, lookup_vendor(obj.get("vendor")), creds.username, creds.password)
        ctl = create_switchctl(info)
        if ctl is None:
            raise ValueError(f"unable to create SwitchCtl for {name}, {obj.get('vendor')}")
        else:
            return ctl
