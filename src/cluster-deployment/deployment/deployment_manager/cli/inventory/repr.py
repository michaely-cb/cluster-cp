from typing import List, Optional, Dict, Any
from pydantic import Field, BaseModel, conint
from deployment_manager.common.representation import PydanticBaseModel
import tabulate
from deployment_manager.db.models import DeploymentDeviceTypes, DeploymentDeviceRoles
from deployment_manager.db.device_props import ALL_DEVICE_PROP_DICT


class DeviceSpec(BaseModel):
    model_config = {
        "use_enum_values": True
    }
    type: DeploymentDeviceTypes
    role: DeploymentDeviceRoles
    count: conint(gt=0)
    name_format: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class Elevation(PydanticBaseModel):
    name: str
    device_specs: List[DeviceSpec] = Field(min_length=1)

    def to_table_row(self) -> List[str]:
        pass

    def to_table_rows(self) -> List[List[str]]:
        rows = []

        if self.device_specs is None or len(self.device_specs) == 0:
            rows.append([self.name, "", "", ""])
            return rows

        for row_num, device_spec in enumerate(self.device_specs):
            rows.append([
                self.name if row_num == 0 else "",
                device_spec.type,
                device_spec.role,
                device_spec.count,
            ])

        return rows

    @classmethod
    def format_reprs(cls, reprs, fmt: str = "table") -> str:
        if fmt in {"yaml", "json", "csv"}:
            return super().format_reprs(reprs, fmt)

        rows = []
        for r in reprs:
            rows.extend(r.to_table_rows())

        return tabulate.tabulate(rows, headers=cls.table_header())

    @classmethod
    def table_header(cls) -> List[str]:
        return ["elevation name", "type", "role", "count"]
    

class Location(BaseModel):
    position: Optional[str] = Field(..., description="Required field, but value can be null or empty string")
    stamp: Optional[str] = Field(..., description="Required field, but value can be null or empty string")

    model_config = {
        "extra": "forbid"
    }

class Properties(BaseModel):
    location: Location

    model_config = {
        "extra": "forbid"
    }

class Rack(PydanticBaseModel):
    name: str
    elevation_name: str
    part_number: str
    properties: Properties

    def to_table_row(self) -> List[str]:
        return [
            self.name,
            self.elevation_name,
            self.properties.location.position,
            self.properties.location.stamp,
        ]

    @classmethod
    def table_header(cls) -> List[str]:
        return ["name", "elevation_name", "location.position", "location.stamp"]