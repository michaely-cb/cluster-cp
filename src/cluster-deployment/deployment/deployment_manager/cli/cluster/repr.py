import dataclasses
from typing import Any, Dict, List, Optional

from deployment_manager.db.models import ClusterUpgrade, ClusterUpgradeBatch, ClusterUpgradeBatchDevice
from deployment_manager.tools.utils import ReprFmtMixin


@dataclasses.dataclass
class ClusterStatusRepr:
    is_primary: bool = False
    controlplane: List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ClusterRepr(ReprFmtMixin):
    name: str
    dns_name: str  # e.g. msp1.cerebrascloud.com
    management_vip: str = ""
    status: Optional[ClusterStatusRepr] = dataclasses.field(default_factory=ClusterStatusRepr)

    @classmethod
    def table_header(cls) -> list:
        return ["name", "dns_name", "management_vip", "status.is_primary", "status.controlplane"]

    def to_table_row(self) -> list:
        return [
            self.name,
            self.dns_name,
            self.management_vip,
            str(self.status.is_primary),
            " ".join(self.status.controlplane),
        ]

    def to_dict(self) -> Dict[str, Any]:
        result: dict[str, Any] = {}
        for attr, value in vars(self).items():
            if dataclasses.is_dataclass(value):
                result[attr] = dataclasses.asdict(value)
            else:
                result[attr] = value
        return result

    @classmethod
    def from_cluster(cls, c) -> "ClusterRepr":
        return cls(
            name=c.name,
            dns_name=c.dns_name,
            management_vip=c.management_vip,
            status=ClusterStatusRepr(
                is_primary=c.is_primary,
            )
        )

    def update_attrs(self, properties: List[str]) -> "ClusterRepr":
        """ props_list is a "k=v" string specification list from cmd line """
        kwargs = {"name": None, "dns_name": "", "management_vip": ""}
        for prop in properties:
            kv = prop.split("=", 1)
            if len(kv) != 2:
                raise ValueError(f"invalid property spec, should be '{'|'.join(kwargs.keys())}=VALUE'")
            k, v = kv
            if k not in kwargs:
                raise ValueError(f"property key '{k}' not allowed, should be one of '{', '.join(kwargs.keys())}'")
            setattr(self, k, v)
        return self


@dataclasses.dataclass
class ClusterDeviceRepr(ReprFmtMixin):
    cluster_name: str
    name: str
    type: str
    role: str
    controlplane: bool

    @property
    def sort_key(self):
        return self.cluster_name, not self.controlplane, self.type, self.role, self.name,

    @classmethod
    def table_header(cls) -> list:
        return ["cluster_name", "name", "type", "role", "controlplane", ]

    def to_table_row(self) -> list:
        return [self.cluster_name, self.name, self.type, self.role, str(self.controlplane), ]

    @classmethod
    def from_clusterdevice(cls, cd) -> "ClusterDeviceRepr":
        return cls(
            cluster_name=cd.cluster.name,
            name=cd.device.name,
            type=cd.device.device_type,
            role=cd.device.device_role,
            controlplane=cd.controlplane,
        )

@dataclasses.dataclass
class ClusterUpgradeRepr(ReprFmtMixin):
    upgrade_id: int
    source_cluster: str
    dest_cluster: str
    upgrade_pkg_path: str
    status: str
    current_batch_id: Optional[int]
    data_preparation_status: str
    created_at: str
    updated_at: str

    @classmethod
    def table_header(cls) -> List[str]:
        return [
            "UpgradeID",
            "Source Cluster",
            "Dest Cluster",
            "Package Path",
            "Status",
            "Current BatchID",
            "Data Prep Status",
            "Created At",
            "Updated At",
        ]

    def to_table_row(self) -> List[str]:
        return [
            str(self.upgrade_id),
            self.source_cluster,
            self.dest_cluster,
            self.upgrade_pkg_path,
            self.status,
            str(self.current_batch_id) if self.current_batch_id is not None else "N/A",
            self.data_preparation_status,
            self.created_at,
            self.updated_at,
        ]

    @classmethod
    def from_orm(cls, upgrade: ClusterUpgrade) -> "ClusterUpgradeRepr":
        # Get the latest batch for this upgrade
        latest_batch = ClusterUpgradeBatch.objects.filter(upgrade_process=upgrade).order_by('-created_at').first()
        current_batch_id = latest_batch.id if latest_batch else None

        return cls(
            upgrade_id=upgrade.id,
            source_cluster=upgrade.source_cluster.name,
            dest_cluster=upgrade.dest_cluster.name,
            upgrade_pkg_path=upgrade.upgrade_pkg_path,
            status=upgrade.status,
            current_batch_id=current_batch_id,
            data_preparation_status=upgrade.data_preparation_status,
            created_at=upgrade.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            updated_at=upgrade.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
        )


@dataclasses.dataclass
class ClusterUpgradeBatchRepr(ReprFmtMixin):
    batch_id: int
    upgrade_id: int
    status: str
    created_at: str
    updated_at: str

    @classmethod
    def table_header(cls) -> List[str]:
        return [
            "BatchID",
            "UpgradeID",
            "Status",
            "Created At",
            "Updated At",
        ]

    def to_table_row(self) -> List[str]:
        return [
            str(self.batch_id),
            str(self.upgrade_id),
            self.status,
            self.created_at,
            self.updated_at,
        ]

    @classmethod
    def from_orm(cls, batch: ClusterUpgradeBatch) -> "ClusterUpgradeBatchRepr":
        return cls(
            batch_id=batch.id,
            upgrade_id=batch.upgrade_process.id,
            status=batch.status,
            created_at=batch.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            updated_at=batch.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
        )

@dataclasses.dataclass
class ClusterUpgradeBatchDeviceRepr(ReprFmtMixin):
    device_name: str
    device_type: str
    device_role: str
    status: str
    current_step: str
    message: str

    @property
    def sort_key(self):
        return self.device_type, self.device_role, self.device_name,

    @classmethod
    def table_header(cls) -> List[str]:
        return [
            "Device Name",
            "Type",
            "Role",
            "Status",
            "Current Step",
            "Message",
        ]

    def to_table_row(self) -> List[str]:
        return [
            self.device_name,
            self.device_type,
            self.device_role,
            self.status,
            self.current_step,
            self.message,
        ]

    @classmethod
    def from_orm(cls, batch_device: ClusterUpgradeBatchDevice) -> "ClusterUpgradeBatchDeviceRepr":
        return cls(
            device_name=batch_device.device.name,
            device_type=batch_device.device.device_type,
            device_role=batch_device.device.device_role,
            status=batch_device.status,
            current_step=batch_device.current_step or "",
            message=batch_device.message or "",
        )
