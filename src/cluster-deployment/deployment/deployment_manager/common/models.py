from typing import Dict, Literal, Union
from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic import ConfigDict

K_SYSTEM_ADMIN_PASSWORD = "default_system_admin_password"
K_SYSTEM_ROOT_PASSWORD = "default_system_root_password"
K_AWS_CREDENTIAL_KEY = "aws_credential_key"
K_AWS_CREDENTIAL_SECRET = "aws_credential_secret"
K_AWS_ACCOUNT_ID = "aws_account_id"
K_AWS_REGION = "aws_region"


def well_known_secrets() -> Dict[str, str]:
    return {
        K_SYSTEM_ADMIN_PASSWORD: "admin",
        K_SYSTEM_ROOT_PASSWORD: "",
        K_AWS_CREDENTIAL_KEY: "",
        K_AWS_CREDENTIAL_SECRET: "",
        K_AWS_ACCOUNT_ID: "171496337684",
        K_AWS_REGION: "us-west-2",
    }


class SecretsProviderConfig(BaseModel):
    """ Secrets provider configuration """

    provider: str = Field(
        default="embedded",
        pattern=r"^embedded$",
        description="Secrets provider. Embedded supports secrets directly embedded in the config file while in "
                    "the 'data' field while in the future the 'data' field may contain parameters for accessing an "
                    "external vault",
    )
    data: Dict[str, str] = Field(
        default_factory=well_known_secrets,
        description="Secrets data for the embedded secrets provider"
    )

    model_config = ConfigDict(extra="ignore")


## "base" models for cluster configuration


DEFAULT_METRICS_USERNAME = "metrics"
DEFAULT_METRICS_PASSWORD = "Metrics123"  # passes HPE/SM requirements: 8+ chars, uppercase, digits


class GlobalSystemConfigs(BaseModel):
    min_psu_required: int = 6

    class Config:
        extra = "forbid"


class NodeNicCount(BaseModel):
    sx: int = 6
    mx: int = 2
    wk: int = 2
    mg: int = 2
    ax: int = 2
    ix: int = 2
    us: int = 1


class HardwareExpectations(BaseModel):
    node_data_nic_count: Optional[NodeNicCount] = Field(
        default_factory=NodeNicCount,
        description="Minimum number of 100g or 400g NICs per node role",
    )

    class Config:
        extra = "forbid"


class ClusterMeta(BaseModel):
    name: str = "cerebras-cluster"
    ntp_servers: List[str] = Field(default_factory=list)
    ilo_license: str = ""
    mirror_dhcp_servers: List[str] = Field(default_factory=list)
    root_server: str = ""
    root_server_ip: str = ""
    switch_ztp_ip: Optional[str] = Field(
        default=None,
        description=(
            "IP address used for management switch deployment. "
            "Hosts the HTTP server for management switch ZTP configuration"
        ),
    )
    topology: str = "leaf_spine" # or "memx_swarmx"
    domain: str = "cerebras.internal"
    hostname_prefix: str = Field(
        default="",
        description=(
            "Prefix applied to generated device names on import. "
            "E.g. 'cg3-' results in names like cg3-wse001-mx-sr01"
        ),
    )
    configure_log_storage: bool = Field(
        False,
        description="Set to true to configure dedicated log storage for weight streaming jobs",
    )
    global_system_configs: Optional[GlobalSystemConfigs] = Field(
        default=None,
        description="Optional site-specific configs applied to all systems during PB1",
    )
    ipmi_metrics_username: str = Field(
        DEFAULT_METRICS_USERNAME,
        description="Username for readonly metrics user created on IPMI during PB1",
    )
    ipmi_metrics_password: str = Field(
        DEFAULT_METRICS_PASSWORD,
        description=(
            "Password for readonly metrics user created on IPMI during PB1 "
            "(8+ characters recommended)"
        ),
    )
    hardware_expectations: Optional[HardwareExpectations] = Field(
        default=None,
        description=(
            "Optional site-specific hardware expectations used in PB validation steps"
        ),
    )

    class Config:
        extra = "forbid"


## Management network configuration models


class DHCPConfig(BaseModel):
    provider: Literal["dnsmasq", "kea"] = Field(
        "dnsmasq",
        description=(
            "DHCP provider. dnsmasq also serves as DNS and TFTP; "
            "kea expects those services separately."
        ),
    )
    kea_tsig_name: Optional[str] = Field(
        default=None,
        description="Optional TSIG key name for DDNS",
    )
    kea_tsig_key: Optional[str] = Field(
        default=None,
        description="Optional TSIG key secret for DDNS",
    )
    powerdns_enable: Optional[bool] = Field(
        default=None,
        description="Enable creation of a PowerDNS updater (requires powerdns_api_key)",
    )
    powerdns_api_url: Optional[str] = Field(
        default=None,
        description="https://host:port of PowerDNS or PowerDNS-Admin server",
    )
    powerdns_api_key: Optional[str] = Field(
        default=None,
        description="API key for PowerDNS or PowerDNS-Admin (global key not recommended)",
    )

    class Config:
        extra = "forbid"


class MgmtVip(BaseModel):
    router_asn: Union[str, int] = Field(
        4294967294,
        description="ASN of the mgmt routing fabric (aggregate switches)",
    )
    node_asn: Union[str, int] = Field(
        4294967293,
        description="ASN of the Kubernetes control-plane nodes",
    )
    vip: str = Field(
        "",
        description="VIP address; take from a reserved subnet near the start of parentnet",
    )

    class Config:
        extra = "forbid"


class NetworkExtent(BaseModel):
    name: str
    subnets: List[str] = Field(
        ...,
        description="List of networks comprising the extent; must be children of parentnet",
    )

    class Config:
        extra = "forbid"


class IPAllocationConfig(BaseModel):
    parentnet: str = Field(
        "172.16.0.0/20",
        description=(
            "1 G parent subnet reserved for all cluster IP allocations; "
            "divided into network_extents"
        ),
    )
    network_extents: Optional[List[NetworkExtent]] = Field(
        default=None,
        description="Reserved network ranges",
    )
    asn_start: Union[str, int] = Field(
        4259840000,
        description="1 G ASN start, inclusive",
    )
    asn_count: int = Field(
        128,
        description="Number of ASNs reserved for allocation after asn_start",
    )
    allocation_strategy: Literal["v1", "v2"] = Field(
        "v2",
        description=(
            "IP allocation strategy for host subnets. v1 is legacy; "
            "v2 uses mgmt_netmask_per_rack per switch subnet."
        ),
    )

    class Config:
        extra = "forbid"


class MgmtNetworkConfig(BaseModel):
    dhcp: Optional[DHCPConfig] = None
    mgmt_gateway_overwrite: str = ""
    mgmt_ip_blocks: List[str] = Field(
        default_factory=list,
        description=(
            "List of management IP blocks. Each block is a CIDR subnet "
            "assigned to a rack or a group of racks. Prefer using ip_allocation instead for new clusters."
        ),
    )
    mgmt_vip: Optional[MgmtVip] = Field(
        default_factory=MgmtVip,
        description=(
            "Management VIP configuration. Used for Kubernetes control-plane "
            "and other management services. Should be in the mgmt_ip_blocks."
        ),
    )
    ip_allocation: Optional[IPAllocationConfig] = Field(
        default_factory=IPAllocationConfig,
        description=(
            "IP allocation configuration for management network. "
            "Defines parentnet, network_extents, and ASNs for allocation."
        ),
    )
    asn_agg_0: Optional[Union[str, int]] = Field(
        default=None, description="ASN of first aggregate switch"
    )
    asn_agg_1: Optional[Union[str, int]] = Field(
        default=None, description="ASN of second aggregate switch"
    )

    mgmt_netmask_per_rack: str = "/24"
    mgmt_pod_cidr: List[str] = Field(
        default_factory=lambda: ["100.64.0.0/12"],
        description="CIDR blocks for Kubernetes pods in the management network"
    )
    mgmt_service_cidr: str = Field(
        default = "100.80.0.0/16",
        description="CIDR block for Kubernetes services in the management network",
    )
    mgmt_dns: List[str] = Field(default_factory=list)
    mgmt_routes: Optional[List[str]] = None
    mgmt_gateway_nth_address_per_rack: int = -1
    disable_csx_ip_assignment: bool = Field(
        False,
        description="Disable DHCP-based IP assignment for CSX devices",
    )
    default_lease_time: str = Field(
        "1h",
        description="Default lease time for dnsmasq (overridable per switch)",
    )

    class Config:
        extra = "forbid"


## Exterior Management network configuration models

class ExteriorMgmtNetworkConfig(BaseModel):
    ext_ip: str = Field(
        "",
        description="IP address assigned to the external NIC",
    )
    ext_default_gateway: str = Field(
        "",
        description="Default gateway for the external NIC",
    )
    ext_route: Optional[List[str]] = Field(
        default=None,
        description="Static routes to add on the external NIC",
    )
    ext_dns: Optional[List[str]] = Field(
        default=None,
        description="DNS servers reachable via the external NIC",
    )

    class Config:
        extra = "forbid"


## 100G Network configuration models

class SubnetPool(BaseModel):
    description: Optional[str] = None
    subnets: List[str] = Field(
        ...,
        description="Subnets in the pool",
    )
    permit_classes: Optional[List[str]] = Field(
        default=None,
        description="Allowed VLAN classes to use this pool",
    )

    class Config:
        extra = "forbid"


class ClusterAsn(BaseModel):
    start: str
    count: int

    class Config:
        extra = "forbid"


class RackIpBlock(BaseModel):
    rack: int
    ip_block: str

    class Config:
        extra = "forbid"


class Uplink(BaseModel):
    memx_switch: str
    memx_switch_port: str
    uplink_ip: str
    ext_switch_name: str
    ext_switch_port: str
    ext_downlink_ip: str
    ext_switch_asn: str

    class Config:
        extra = "forbid"


class DataNetworkConfig(BaseModel):
    cluster_ip_block: str = "10.254.0.0/16"
    subnet_pools: Optional[List[SubnetPool]] = None

    cluster_netmask_per_rack: str = "/24"

    cluster_memx_interfaces: int = 48
    cluster_memx_subnet_size: int = 256
    cluster_swarmx_subnet_size: int = 256
    cluster_system_subnet_size: int = 256

    cluster_vip_count: int = 2
    cluster_cs_vip_prefix: Optional[str] = None
    cluster_xconn_count: int = 24

    cluster_asns: List[ClusterAsn] = Field(
        default_factory=lambda: [ClusterAsn(start="65001", count=128)]
    )

    cluster_ip_blocks: List[RackIpBlock] = Field(default_factory=list)
    enable_dcqcn: bool = Field(
        False,
        description="Enable DCQCN on servers",
    )
    system_ip_blocks: List[RackIpBlock] = Field(default_factory=list)
    systems_with_vlan: List[str] = Field(default_factory=list)

    system_enable_dcqcn: bool = Field(
        False,
        description="Enable DCQCN on systems. Only applies if this flag is enabled on a v2 network cluster",
    )

    uplinks: List[Uplink] = Field(
        default_factory=lambda: [
            Uplink(
                memx_switch="<memx switch name>",
                memx_switch_port="<memx switch port>",
                uplink_ip="<ip>",
                ext_switch_name="<external switch name>",
                ext_switch_port="<external switch port>",
                ext_downlink_ip="<external downlink ip>",
                ext_switch_asn="<external switch asn>",
            )
        ]
    )

    system_connections_file: Optional[str] = Field(
        default=None,
        description=(
            "Optional file path. File contains a JSON array of objects with "
            "form {switch_(name,port), system_(name,port)} which amend system "
            "connections discovered by LLDP in network configuration."
        ),
    )

    class Config:
        extra = "forbid"


## Exterior 100G network configuration models

class ExteriorDataNetworkConfig(BaseModel):
    prefixes: List[str] = Field(
        default_factory=lambda: ["10.0.0.0/8"],
        description="CIDR blocks reachable through the exterior network",
    )

    class Config:
        extra = "forbid"


## FreeIPA configuration models

class FreeIPAConfig(BaseModel):
    enabled: bool = Field(
        False,
        description="Enable FreeIPA integration",
    )
    ipa_master_nis_domain: str = Field(
        "<nis_domain>",
        description="NIS domain of the IPA master",
    )
    ipa_master_server: str = Field(
        "<master_server_host>",
        description="Hostname or IP of the IPA master",
    )
    ipa_master_domain: str = Field(
        "<domain>",
        description="Kerberos domain managed by the IPA master",
    )
    master_username: str = Field(
        "<username>",
        description="Admin username for the IPA master",
    )
    master_password: str = Field(
        "<password>",
        description="Admin password for the IPA master",
    )

    class Config:
        extra = "forbid"


## NFS mount configuration models


class NFSMountConfig(BaseModel):
    enabled: bool = Field(
        False,
        description="Enable NFS / shared-storage configuration",
    )

    server_paths: List[str] = Field(
        default_factory=lambda: [
            "cerebras-storage:/cb",
            "cerebras-storage:/home",
            "cerebras-storage:/tests",
            "cerebras-storage:/ml",
        ],
        description="NFS server exports to mount",
    )

    mount_paths: List[str] = Field(
        default_factory=lambda: ["/cb", "/cb/home", "/cb/tests", "/cb/ml"],
        description="Corresponding local mount points",
    )

    u_names: List[str] = Field(
        default_factory=lambda: ["lab"],
        description="User names to create on client systems",
    )
    u_ids: List[int] = Field(
        default_factory=lambda: [1001],
        description="UIDs that map to the user names",
    )
    u_passwords: List[str] = Field(
        default_factory=lambda: [""],
        description=(
            "Optional hashed passwords for users (see Ansible mkpasswd). "
            "Use SHA-512 hashes, e.g. $6$saltsalt$hash"
        ),
    )

    g_names: List[str] = Field(
        default_factory=lambda: ["lab"],
        description="Supplementary group names",
    )
    g_ids: List[int] = Field(
        default_factory=lambda: [1001],
        description="Supplementary GIDs",
    )

    class Config:
        extra = "forbid"


class ClusterDeploymentConfig(BaseModel):
    basic: ClusterMeta = Field(default_factory=ClusterMeta)
    mgmt_network_int_config: MgmtNetworkConfig = Field(default_factory=MgmtNetworkConfig)
    mgmt_network_ext_config: ExteriorMgmtNetworkConfig = Field(default_factory=ExteriorMgmtNetworkConfig)
    cluster_network_config: DataNetworkConfig = Field(default_factory=DataNetworkConfig)
    exterior_network_config: ExteriorDataNetworkConfig = Field(default_factory=ExteriorDataNetworkConfig)
    freeipa: FreeIPAConfig = Field(default_factory=FreeIPAConfig)
    nfs_mount: NFSMountConfig = Field(default_factory=NFSMountConfig)
    secrets_provider: SecretsProviderConfig = Field(default_factory=SecretsProviderConfig)

    class Config:
        extra = "forbid"

class Cluster(BaseModel):
    name: str
    mgmt_vip: str
