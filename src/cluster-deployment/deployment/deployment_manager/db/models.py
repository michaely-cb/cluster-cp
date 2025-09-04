import datetime
import logging
import typing
import pytz
from django.core.exceptions import ObjectDoesNotExist, FieldDoesNotExist
from django.db import models
from enum import Enum
from . import device_props as props
from .const import (DeploymentDeviceRoles, DeploymentDeviceTypes, DeploymentSections, DeploymentStageStatus,
                    DeviceDeploymentStages)
from .device_props import (ALL_DEVICE_PROP_DICT, AttrDict, PropAttrDict, get_propattr_dict_for_device_type,
                           must_get_prop_attr_default_value,
                           get_property_attrs, DeviceProp, INVENTORY_DEVICE_PROPS_DICT)
from typing import Dict, List, Optional, TYPE_CHECKING, Tuple, Union
import django.db.transaction
import pytz
from django.core.exceptions import FieldDoesNotExist, ObjectDoesNotExist
from django.db.models.signals import post_save
from django.dispatch import receiver


from deployment_manager.common import logger as cl
from . import device_props as props
from .const import (
    DeploymentDeviceRoles, DeploymentDeviceTypes, DeploymentSections, DeploymentStageStatus,
    DeviceDeploymentStages,
)
from .device_props import (
    ALL_DEVICE_PROP_DICT, AttrDict, DeviceProp, INVENTORY_DEVICE_PROPS_DICT, PropAttrDict,
    get_propattr_dict_for_device_type, get_property_attrs, must_get_prop_attr_default_value,
)

if TYPE_CHECKING:
    from django.db.models import QuerySet
else:
    QuerySet = List

logger = logging.getLogger(__name__)

class MissingPropVals(Enum):
    EXCLUDE = 0  # don't include missing props
    DEFAULTS = 1  # include missing props with default vals
    NULLS = 2  # include missing props with None values


class UpgradeProcessStatus(Enum):
    NOT_STARTED = "NOT_STARTED"
    DEST_CLUSTER_UPGRADED = "DEST_CLUSTER_UPGRADED"
    MIGRATING_BATCHES = "MIGRATING_BATCHES"
    SOURCE_CLUSTER_UPGRADED = "SOURCE_CLUSTER_UPGRADED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class UpgradeBatchStatus(Enum):
    NOT_STARTED = "NOT_STARTED"
    TAINT_COMPLETED = "TAINT_COMPLETED"
    QUIESCE_COMPLETED = "QUIESCE_COMPLETED"
    MIGRATE_STARTED = "MIGRATE_STARTED"
    UPDATE_DEVICE_ASSOCIATION_IN_PROGRESS = "UPDATE_DEVICE_ASSOCIATION_IN_PROGRESS"
    UPDATE_DEVICE_ASSOCIATION_COMPLETED = "UPDATE_DEVICE_ASSOCIATION_COMPLETED"
    UPDATE_DEVICE_ASSOCIATION_FAILED = "UPDATE_DEVICE_ASSOCIATION_FAILED"
    MIGRATE_OUT_IN_PROGRESS = "MIGRATE_OUT_IN_PROGRESS"
    MIGRATE_OUT_COMPLETED = "MIGRATE_OUT_COMPLETED"
    MIGRATE_OUT_FAILED = "MIGRATE_OUT_FAILED"
    SECURITY_PATCH_SERVERS_IN_PROGRESS = "SECURITY_PATCH_SERVERS_IN_PROGRESS"
    SECURITY_PATCH_SERVERS_COMPLETED = "SECURITY_PATCH_SERVERS_COMPLETED"
    SECURITY_PATCH_SERVERS_FAILED = "SECURITY_PATCH_SERVERS_FAILED"
    UPGRADE_SYSTEMS_IN_PROGRESS = "UPGRADE_SYSTEMS_IN_PROGRESS"
    UPGRADE_SYSTEMS_COMPLETED = "UPGRADE_SYSTEMS_COMPLETED"
    UPGRADE_SYSTEMS_FAILED = "UPGRADE_SYSTEMS_FAILED"
    MIGRATE_IN_IN_PROGRESS = "MIGRATE_IN_IN_PROGRESS"
    MIGRATE_IN_COMPLETED = "MIGRATE_IN_COMPLETED"
    MIGRATE_IN_FAILED = "MIGRATE_IN_FAILED"
    COMPLETE_BATCH_MIGRATION_FAILED = "COMPLETE_BATCH_MIGRATION_FAILED"
    MIGRATE_COMPLETED = "MIGRATE_COMPLETED"
    FAILED = "FAILED"


class UpgradeDeviceStatus(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class DataPreparationStatus(Enum):
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"  # Has completed at least once
    FAILED = "FAILED"        # Last attempt failed, but may have succeeded before


class DeploymentProfile(models.Model):
    profile_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=128)
    cluster_name = models.CharField(max_length=128)
    # This flag indicates whether 'deploy' commands can be run
    # for this profile. Essentially this indicates inventory and
    # input files are available for this profile. The only case
    # where this will be false is when profile is imported from
    # k8s cluster definition.
    deployable = models.BooleanField(default=True)

    @classmethod
    def create(cls, name: str, cluster_name: str, deployable: bool = True) -> Optional['DeploymentProfile']:
        if cls.objects.filter(name=name).count() > 0:
            cl.force_log(logger, logging.ERROR, f"Profile {name} already exists")
            return None
        cn = cluster_name or name
        p = cls(name=name, cluster_name=cn, deployable=deployable)
        p.save()
        return p

    @classmethod
    def list(cls) -> List['DeploymentProfile']:
        return cls.objects.all()

    @classmethod
    def get_profile(cls, name: str) -> Optional['DeploymentProfile']:
        try:
            return cls.objects.get(name=name)
        except:
            return None

    def servers(self) -> List['Device']:
        return list(self.devices.filter(device_type=DeploymentDeviceTypes.SERVER.value))

    def systems(self) -> List['Device']:
        return list(self.devices.filter(device_type=DeploymentDeviceTypes.SYSTEM.value))

    def switches(self) -> List['Device']:
        return list(self.devices.filter(device_type=DeploymentDeviceTypes.SWITCH.value))

    def mark_not_deployable(self):
        self.deployable = False
        self.save()

    def mark_deployable(self):
        self.deployable = True
        self.save()


class Device(models.Model):
    """
        Device database model
    """
    device_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=128)
    device_type = models.CharField(max_length=2)
    device_role = models.CharField(max_length=2, default="")
    deployment_stage = models.IntegerField(
        null=True, default=None
    )
    deployment_status = models.IntegerField(
        null=True, default=0
    )
    deployment_msg = models.CharField(
        max_length=1024, default=""
    )
    deployment_update_time = models.DateTimeField(
        null=True,
    )
    profile = models.ForeignKey(
        DeploymentProfile, on_delete=models.CASCADE,
        related_name='devices'
    )

    if TYPE_CHECKING:
        # IDE support for FK `properties` field
        properties: 'QuerySet[DeviceProperty]'

    @property
    def is_switch(self) -> bool:
        return self.device_type == DeploymentDeviceTypes.SWITCH.value

    @property
    def is_server(self) -> bool:
        return self.device_type == DeploymentDeviceTypes.SERVER.value

    @property
    def is_system(self) -> bool:
        return self.device_type == DeploymentDeviceTypes.SYSTEM.value

    @property
    def is_mg_switch(self) -> bool:
        return self.device_type == DeploymentDeviceTypes.SWITCH.value and \
            self.device_role == DeploymentDeviceRoles.MANAGEMENT.value

    @property
    def is_leaf_switch(self) -> bool:
        return self.device_type == DeploymentDeviceTypes.SWITCH.value and \
            self.device_role == DeploymentDeviceRoles.LEAF.value

    @property
    def is_spine_switch(self) -> bool:
        return self.device_type == DeploymentDeviceTypes.SWITCH.value and \
            self.device_role == DeploymentDeviceRoles.SPINE.value

    @property
    def is_aw_switch(self) -> bool:
        return self.device_type == DeploymentDeviceTypes.SWITCH.value and \
            self.device_role == DeploymentDeviceRoles.MEMORYX.value

    @property
    def is_sx_switch(self) -> bool:
        return self.device_type == DeploymentDeviceTypes.SWITCH.value and \
            self.device_role == DeploymentDeviceRoles.SWARMX.value

    @property
    def device_type_str(self):
        try:
            return DeploymentDeviceTypes(self.device_type).name
        except:
            return ""

    @property
    def device_role_str(self):
        try:
            return DeploymentDeviceRoles(self.device_role).name
        except:
            return ""

    @property
    def deployment_stage_str(self):
        try:
            return DeviceDeploymentStages(self.deployment_stage).name
        except:
            return ""

    @property
    def deployment_status_str(self):
        try:
            return DeploymentStageStatus(self.deployment_status).name
        except:
            return ""

    @classmethod
    def add(cls,
            name: str,
            device_type: typing.Union[str, DeploymentDeviceTypes],
            device_role: typing.Union[str, DeploymentDeviceRoles],
            profile: Union[DeploymentProfile, str]) -> 'Device':
        """
        Add a device with type/role to the DB for a given profile. Raises ValueError if name is not unique against the profile
        Returns: created device
        """
        if isinstance(profile, str):
            profile = DeploymentProfile.get_profile(profile)
            if not profile:
                raise ValueError(f"attempted to add device to profile '{profile}' but this profile does not exist")

        dt = device_type.value if isinstance(device_type, DeploymentDeviceTypes) else device_type
        dr = DeploymentDeviceRoles.to_str(device_role)
        try:
            existing: Device = cls.objects.filter(profile__name=profile.name, name=name).get()
            if existing.device_role == dr and existing.device_type == dt:
                return existing
            raise ValueError(f"attempted to add duplicate entries for device {name}")
        except ObjectDoesNotExist:
            c = cls(name=name, device_type=dt, device_role=dr, profile=profile)
            c.save()
            return c

    @classmethod
    def get_all(cls, profile_name: str,
                device_role: Optional[str] = None,
                device_type: Optional[str] = None) -> QuerySet['Device']:
        devices = cls.objects.filter(
            profile__name=profile_name,
        )
        if device_role:
            devices = devices.filter(device_role=device_role)
        if device_type:
            devices = devices.filter(device_type=device_type)
        return devices

    @classmethod
    def get_device(cls, name: str, profile_name: str) -> Optional['Device']:
        try:
            return cls.objects.get(profile__name=profile_name, name=name)
        except cls.DoesNotExist:
            return None
        except Exception as e:
            cl.force_log(logger, logging.ERROR, f"unexpected error while fetching device {name}, {e}")
            raise

    @classmethod
    def get_servers(cls,
                    profile_name: str,
                    device_role: Optional[typing.Union[DeploymentDeviceRoles, str, list]] = None,
                    server_name: Optional[typing.Union[str, list]] = None) -> QuerySet['Device']:
        devices = cls.objects.filter(
            profile__name=profile_name,
            device_type=DeploymentDeviceTypes.SERVER.value
        )
        if device_role:
            if isinstance(device_role, list):
                devices = devices.filter(device_role__in=[DeploymentDeviceRoles.to_str(r) for r in device_role])
            else:
                devices = devices.filter(device_role=DeploymentDeviceRoles.to_str(device_role))
        if server_name:
            if isinstance(server_name, list):
                devices = devices.filter(name__in=server_name)
            else:
                devices = devices.filter(name=server_name)
        return devices

    @classmethod
    def get_console_servers(cls, profile_name: str) -> QuerySet['Device']:
        return cls.objects.filter(
            profile__name=profile_name,
            device_type="CN"
        )

    @classmethod
    def get_switches(cls, profile_name: str) -> QuerySet['Device']:
        return cls.objects.filter(
            profile__name=profile_name,
            device_type=DeploymentDeviceTypes.SWITCH.value
        )

    @classmethod
    def get_data_switches(cls, profile_name: str) -> QuerySet['Device']:
        return cls.objects.filter(
            profile__name=profile_name,
            device_type=DeploymentDeviceTypes.SWITCH.value,
        ).exclude(device_role=DeploymentDeviceRoles.MANAGEMENT.value)

    @classmethod
    def get_systems(cls, profile_name: str) -> QuerySet['Device']:
        return cls.objects.filter(
            profile__name=profile_name,
            device_type=DeploymentDeviceTypes.SYSTEM.value
        )

    @classmethod
    def delete_device(cls, name: str, profile_name: str):
        device = cls.get_device(name, profile_name)
        if device:
            device.delete()


    @classmethod
    def field_exists(cls, field: str):
        try:
            cls._meta.get_field(field)
            return True
        except FieldDoesNotExist:
            return False

    def update_stage(self, stage: int, status: int, msg: str = "", force: bool = False):
        # Set force to True if stage has to go backwards
        if self.deployment_stage is None or force or self.deployment_stage <= stage:
            self.deployment_stage = stage
            self.deployment_status = status
            self.deployment_msg = msg
            self.deployment_update_time = datetime.datetime.now(tz=pytz.UTC)
            self.save()

    @classmethod
    def get_deployed_after(cls, profile_name: str, update_time: datetime.datetime) -> QuerySet:
        return cls.objects.filter(
            profile__name=profile_name,
            deployment_update_time__gte=update_time
        )

    def rewind_to_network(self):
        # return when deployment_stage is None
        if not self.deployment_stage:
            return
        # Set deployment to start of NETWORK_INIT,
        # if current_stage is not before it
        if self.deployment_stage >= DeviceDeploymentStages.NETWORK_INIT.value:
            self.update_stage(
                DeviceDeploymentStages.NETWORK_INIT.value,
                DeploymentStageStatus.NOT_STARTED.value,
                force=True
            )

    def deployment_not_started(self):
        return self.deployment_stage is None

    def check_stage_completed(self, stage: int) -> bool:
        if self.deployment_stage is None:
            return False
        return self.deployment_stage > stage or (
            self.deployment_stage == stage and
            self.deployment_status == DeploymentStageStatus.COMPLETED.value
        )

    def _check_stage_failed(self, stage: int) -> bool:
        return self.deployment_stage == stage and \
            self.deployment_status == DeploymentStageStatus.FAILED.value

    def _check_stage_needs_update(self, stage: int) -> bool:
        return self.deployment_stage == stage and \
            self.deployment_status == DeploymentStageStatus.NEEDS_UPDATE.value

    def ipmi_configured(self) -> bool:
        return self.check_stage_completed(
            DeviceDeploymentStages.IPMI_CONFIGURATION.value,
        )

    def os_installed(self) -> bool:
        return self.check_stage_completed(
            DeviceDeploymentStages.OS_INSTALLATION.value,
        )

    def provisioned(self) -> bool:
        return self.check_stage_completed(
            DeviceDeploymentStages.PROVISIONING.value
        )

    def nics_collected(self) -> bool:
        return self.check_stage_completed(
            DeviceDeploymentStages.NIC_COLLECTION.value
        )

    def network_initialized(self) -> bool:
        return self.check_stage_completed(
            DeviceDeploymentStages.NETWORK_INIT.value
        )

    def network_generate(self) -> bool:
        return self.check_stage_completed(
            DeviceDeploymentStages.NETWORK_GENERATE.value
        )

    def network_pushed(self) -> bool:
        return self.check_stage_completed(
            DeviceDeploymentStages.NETWORK_PUSH.value
        )

    def freeipa_installed(self) -> bool:
        return self.check_stage_completed(
            DeviceDeploymentStages.FREEIPA.value
        )

    def nfs_mounted(self) -> bool:
        return self.check_stage_completed(
            DeviceDeploymentStages.NFS_MOUNT.value
        )

    def get_prop_attr_dict(self, include_mode: MissingPropVals = MissingPropVals.DEFAULTS) -> PropAttrDict:
        """
        Get all properties.attribute values and return it as a dict

        Args:
            include_mode: DEFAULTS - set default values for device type's props, or if NULLS, None values
        """
        rtn = {}
        for prop, attrs in get_propattr_dict_for_device_type(str(self.device_type)).items():
            prop_attrs = self.get_attr_dict(name=prop, attrs=attrs, include_mode=include_mode)
            if prop_attrs:
                rtn[prop] = prop_attrs
        return rtn

    def get_attr_dict(
        self,
        name: str,
        attrs: Optional[dict] = None,
        include_mode: MissingPropVals = MissingPropVals.DEFAULTS
    ) -> AttrDict:
        """
        Get the all attribute values for the given properties and return it as a dict

        Args:
            name: property name
            attrs: optional dictionary of attributes to include. Presence in the dict means include the attr. None
                means include all attrs
            include_mode: DEFAULTS - set default values for device type's props, or if NULLS, None values
        """

        def include_attr(attr: str) -> bool:
            if attrs is None:
                return True
            return attr in attrs

        rtn = {}
        for dp in self.properties.filter(property_name=name):
            if include_attr(dp.property_attribute):
                rtn[dp.property_attribute] = dp.property_value

        if include_mode in (MissingPropVals.NULLS, MissingPropVals.DEFAULTS):
            default_attrs = get_property_attrs(name)
            for attr, default_value in default_attrs.items():
                if include_attr(attr) and attr not in rtn:
                    rtn[attr] = default_value if include_mode == MissingPropVals.DEFAULTS else None

        return rtn

    def get_property(self, name: str, attribute: str, include_default=True) -> Optional[str]:
        """
        Get the DeviceProperty value for the give name and attribute

        Returns
            None by default. If include_default is true, the default value
        """
        value = None
        default_value = must_get_prop_attr_default_value(name, attribute)

        if include_default:
            value = default_value
        try:
            p = self.properties.get(
                property_name=name,
                property_attribute=attribute
            )
            value = p.property_value
        except:
            pass
        return value

    def get_prop(self, prop: DeviceProp, include_default=True) -> Optional[typing.Any]:
        """
        Get the DeviceProperty value for the give name and attribute

        Returns
            The default value by default. If include_default is False, returns None
        """
        try:
            p = self.properties.get(property_name=prop.name, property_attribute=prop.attr)
            return prop.cast_value(p.property_value)
        except DeviceProperty.DoesNotExist:
            return prop.default_value if include_default else None

    def must_get_prop(self, prop: DeviceProp) -> typing.Any:
        """
        Get the DeviceProperty value for the give name and attribute or raise a ValueError if not present
        """
        try:
            p = self.properties.get(property_name=prop.name, property_attribute=prop.attr)
            v = prop.cast_value(p.property_value)
            if v is None:
                raise ValueError(
                    f"required property {prop} from device {self.name} "
                    f"was present but was not in required format {prop.value_type}"
                )
            return v
        except DeviceProperty.DoesNotExist:
            raise ValueError(f"required property {prop} from device {self.name} was not present")

    def set_prop(self, prop: DeviceProp, value: Optional[str]):
        self.update_property(prop.name, prop.attr, value, default_if_none=False)

    def update_property(self, name: str, attribute: str, value: Optional[str], default_if_none: bool = False):
        """
        Update the DeviceProperty for the given property name and attribute with value.

        If default_if_none is true and value is None set the value to the default value defined in device_properties.
        If value is None, the property will be deleted.
        """
        default_value = must_get_prop_attr_default_value(name, attribute)
        if default_if_none and value is None:
            value = default_value

        prop = props.ALL_DEVICE_PROPS.get(f"{name}.{attribute}")
        log_val = "*****" if prop and prop.sensitive else value
        cl.force_log(logger, logging.DEBUG, f"update prop {self.name}.{name}.{attribute} -> {log_val}")

        try:
            p = self.properties.get(
                property_name=name,
                property_attribute=attribute,
            )
        except DeviceProperty.DoesNotExist:
            if value is not None:
                p = DeviceProperty(
                    device=self,
                    property_name=name,
                    property_attribute=attribute,
                    property_value=value,
                )
                p.save()
        else:
            if value is None:
                p.delete()
            else:
                p.property_value = value
                p.save()

    def batch_update_properties(self, updates: PropAttrDict):
        """
        Batch update the DeviceProperty
        This is an update operation, only values in updates will be updated.
        If the value is None, the default is set.

        The parameter updates should be in a similar k/v structure as
        _property_attr
        """
        for prop_name, attrs in ALL_DEVICE_PROP_DICT.items():
            if prop_name not in updates:
                continue

            for attr in attrs:
                if attr not in updates[prop_name]:
                    continue

                self.update_property(
                    name=prop_name,
                    attribute=attr,
                    value=updates[prop_name][attr],
                    default_if_none=True
                )

    def get_device_inventory_dict(self) -> dict:
        """
        Get the Device properties values as a dict that only contains prop.attr from INVENTORY_DEVICE_PROPS_DICT
        """
        prop_dict = self.get_prop_attr_dict(include_mode=MissingPropVals.DEFAULTS)
        # Remove non-inventory properties.attributes
        for p in list(prop_dict.keys()):
            if p not in INVENTORY_DEVICE_PROPS_DICT:
                prop_dict.pop(p)
                continue
            for a in list(prop_dict[p].keys()):
                if a not in INVENTORY_DEVICE_PROPS_DICT[p]:
                    prop_dict[p].pop(a)
        return prop_dict

    def update_package_versions(self, versions: dict):
        for pkg_name, version in versions.items():
            self.update_property(
                name=pkg_name,
                attribute="version",
                value=version
            )

    def update_bios_config(self, bios_config: dict):
        for attr, val in bios_config.items():
            self.update_property(
                name="bios_config",
                attribute=attr,
                value=val
            )

    def get_bios_config(self):
        return self.get_attr_dict(name="bios_config", include_mode=MissingPropVals.EXCLUDE)

    def get_ipmi_client_args(self) -> Tuple[str, str, str, str]:
        """ ipmi ip, user, password, vendor """
        return (
            str(self.get_prop(props.prop_ipmi_info_ip)),
            self.get_prop(props.prop_ipmi_credentials_user),
            self.get_prop(props.prop_ipmi_credentials_password),
            self.get_prop(props.prop_vendor_name),
        )

    @property
    def has_health_records(self) -> bool:
        """ True if the device has any health records """
        return self.health_records.exists()

    @property
    def has_critical_health(self) -> bool:
        """ True if the device has any critical or unknown health records """
        return self.health_records.filter(
            status__in=[
                HealthState.CRITICAL.value,
                HealthState.UNKNOWN.value,
            ]
        ).exists()


class ProfileDeploymentStage(models.Model):
    stage_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=32)
    status = models.IntegerField(
        default=DeploymentStageStatus.NOT_STARTED.value
    )
    section = models.IntegerField(
        default=DeploymentSections.PRECHECK.value
    )

    @property
    def status_str(self):
        try:
            return DeploymentStageStatus(self.status).name
        except:
            return ""

    @property
    def section_str(self):
        try:
            return DeploymentSections(self.section).name
        except:
            return ""


class DeploymentCtx(models.Model):
    ctx_id = models.AutoField(primary_key=True)
    profile = models.ForeignKey(DeploymentProfile, on_delete=models.CASCADE)
    current_stage = models.ForeignKey(
        ProfileDeploymentStage, default=None, null=True,
        on_delete=models.CASCADE
    )
    status = models.IntegerField(
        null=True,
        default=None
    )

    @property
    def status_str(self):
        try:
            return DeploymentStageStatus(self.status).name
        except:
            return ""

    @classmethod
    def create(cls, profile: DeploymentProfile) -> Optional['DeploymentCtx']:
        if cls.objects.filter(profile=profile).count() > 0:
            cl.force_log(logger, logging.ERROR, f"Context has already been created for {profile.name}")
            return None
        c = cls(
            profile=profile,
            status=DeploymentStageStatus.NOT_STARTED.value
        )
        c.save()
        return c

    @classmethod
    def get_ctx(cls, profile_name: str) -> Optional['DeploymentCtx']:
        try:
            return cls.objects.get(profile__name=profile_name)
        except:
            return None

    def _update_status(self, status: str):
        self.status = status
        self.save()

    def mark_started(self):
        self._update_status(DeploymentStageStatus.STARTED.value)

    def mark_completed(self):
        self._update_status(DeploymentStageStatus.COMPLETED.value)

    def mark_failed(self):
        self._update_status(DeploymentStageStatus.FAILED.value)

    def get_stage_name(self):
        return None if self.current_stage is None else self.current_stage.name

    def update_stage(self, stage_name: str, section: str, status: str):
        if self.current_stage is None:
            s = ProfileDeploymentStage(
                name=stage_name, section=section, status=status
            )
            s.save()
            self.current_stage = s
            self.save()
        else:
            self.current_stage.name = stage_name
            self.current_stage.section = section
            self.current_stage.status = status
            self.current_stage.save()
            self.save()

    def update_status(self, status: str):
        self.current_stage.status = status
        self.current_stage.save()
        self.save()


class DeviceProperty(models.Model):
    """
    Model for Device properties

    For the map of properties and attributes, refer to Device._property_attr
    """
    prop_id = models.AutoField(primary_key=True)
    property_name = models.CharField(max_length=256)
    property_attribute = models.CharField(max_length=64)
    property_value = models.CharField(max_length=128)
    device = models.ForeignKey(Device, on_delete=models.CASCADE, related_name='properties')

    class Meta:
        unique_together = ('device', 'property_name', 'property_attribute')

    @classmethod
    def get_all(cls, profile_name: str, prop: Optional[DeviceProp] = None) -> QuerySet['DeviceProperty']:
        props = cls.objects.filter(device__profile__name=profile_name)
        if prop is not None:
            props = props.filter(
                property_name=prop.name,
                property_attribute=prop.attr
            )
        return props

    @classmethod
    def get_package_version(cls, device: str, pkg_name: str):
        try:
            prop = cls.objects.filter(device=device).filter(property_name=pkg_name).get(property_attribute="version")
            version = prop.property_value
        except:
            version = None
        return version


def get_props_values(profile: str, prop: DeviceProp) -> List[typing.Any]:
    """ Get all unique values for a device prop within a profile. Values are cast to the expected type """
    vals = set(
        prop.cast_value(p.property_value)
        for p in DeviceProperty.objects.filter(
            property_name=prop.name,
            property_attribute=prop.attr,
            device__profile__name=profile,
        )  # .distinct('property_value') distinct on fields is not allowed in sqlite
    )
    return [v for v in vals if v is not None]


def get_devices_with_prop(profile: str, prop: DeviceProp, value: str) -> QuerySet[Device]:
    """ Get devices with the given prop/value """
    return Device.objects.filter(
        profile__name=profile,
        properties__property_name=prop.name,
        properties__property_attribute=prop.attr,
        properties__property_value=value,
    )


def get_devices_without_prop(profile: str, prop: DeviceProp, **additional_filters) -> QuerySet[Device]:
    """ Get devices without the given prop """
    return Device.objects.filter(
        profile__name=profile,
        **additional_filters,
    ).exclude(
        models.Q(
            properties__property_name=prop.name,
            properties__property_attribute=prop.attr
        )
    )


def get_rack_to_mg_switch(profile: str) -> Dict[str, Device]:
    """ Returns rack -> MG switch connecting that rack """
    mg_switches = Device.get_all(profile, device_type="SW", device_role="MG")
    rack_device = {}
    for sw in mg_switches:
        sn = sw.get_prop(props.prop_subnet_info_subnet)
        if not sn:
            continue
        rack = sw.get_prop(props.prop_location_rack)
        rack_device[rack] = sw
        additional_racks = sw.get_prop(props.prop_switch_info_connected_racks)
        if additional_racks:
            for rack in additional_racks.split(","):
                rack_device[rack] = sw
    return rack_device


_SEVERITY = {
    "SKIPPED": 0,
    "OK": 1,
    "UNKNOWN": 2,
    "WARNING": 3,
    "CRITICAL": 4,
}


class HealthState(Enum):
    SKIPPED = "SKIPPED"
    UNKNOWN = "UNKNOWN"
    OK = "OK"
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"


    @property
    def short(self) -> str:
        """ Return the first letter of the value (short form) """
        return self.value[0]

    def __lt__(self, other):
        global _SEVERITY
        if not isinstance(other, HealthState):
            return NotImplemented
        return _SEVERITY[self.value] < _SEVERITY[other.value]


class HealthStatus(models.Model):
    """
    Model for device health
    """

    id = models.AutoField(primary_key=True)
    attr = models.CharField(max_length=256)
    status = models.CharField(max_length=16)
    message = models.CharField(max_length=256, default="")
    detail = models.TextField(default="")
    device = models.ForeignKey(
        Device,
        on_delete=models.CASCADE,
        related_name='health_records'
    )

    # time stamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class Link(models.Model):
    """ Data network link - src device must be a switch and should be spine/swarmx if a sw/sw link """
    id = models.AutoField(primary_key=True)
    src_device = models.ForeignKey(Device, on_delete=models.CASCADE, related_name='src_links')
    src_if = models.CharField(max_length=255)
    src_if_role = models.CharField(max_length=255, default="") # managment, ipmi, data, etc...
    dst_device = models.ForeignKey(Device, on_delete=models.CASCADE, related_name='dst_links', null=True, blank=True)
    dst_name = models.CharField(max_length=255)
    dst_if = models.CharField(max_length=255, default="")
    dst_if_role = models.CharField(max_length=255, default="") # managment, ipmi, data, etc...
    speed = models.IntegerField()
    origin = models.CharField(max_length=64, default="import")  # import | lldp

    class Meta:
        unique_together = ('src_device', 'src_if')

    def __str__(self):
        return f'{self.src_device.name}::{self.src_if} -> {self.dst_name}::{self.dst_if} @ {self.speed}'

class Cluster(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(null=False, max_length=128)
    dns_name = models.CharField(max_length=128, blank=True, null=True)
    is_primary = models.BooleanField(default=False)
    management_vip = models.CharField(max_length=128, blank=True, null=True)

    profile = models.ForeignKey(
        DeploymentProfile, on_delete=models.CASCADE,
        related_name='clusters'
    )

    class Meta:
        constraints = [
            django.db.models.UniqueConstraint(
                fields=["profile"],
                condition=django.db.models.Q(is_primary=True),
                name="db_cluster_primary_unique",
            )
        ]

    @staticmethod
    def must_get(profile: str, name: Optional[str]) -> 'Cluster':
        cluster_name = None if name is None or name == "*" else name

        if cluster_name:
            cluster = Cluster.objects.filter(profile__name=profile, name=cluster_name).first()
        else:
            cluster = Cluster.objects.filter(profile__name=profile, is_primary=True).first()

        if not cluster:
            raise ValueError(f"cluster {profile}/{cluster_name} was not found")

        return cluster

    def __str__(self):
        return f"{self.name}{' (primary)' if self.is_primary else ''}"


class ClusterDevice(models.Model):
    id = models.AutoField(primary_key=True)
    cluster = models.ForeignKey(Cluster, on_delete=models.CASCADE, related_name='cluster_devices')
    # device can have 0 or 1 cluster. 0 cluster implies affiliation with primary cluster
    device = models.OneToOneField(Device, on_delete=models.CASCADE, related_name='cluster_device')
    controlplane = models.BooleanField(default=False)

    def __str__(self):
        return f'ClusterDevice({self.cluster.name}/{self.device.name}{" controlplane=True" if self.controlplane else ""})'


@receiver(post_save, sender=Device)
def sync_cluster_device(sender, instance, created, **kwargs):
    """
    Auto associate device with primary cluster if not already associated.
    Works for both new and updated devices.
    Only associates server and system devices, excluding infra and usernodes.
    """
    # Check if there's already an association for this device
    if not ClusterDevice.objects.filter(device=instance).exists():
        # Only associate servers (SR) and systems (SY)
        # Exclude infra (IN) and usernodes (US)
        if instance.device_type not in ("SR", "SY") or instance.device_role in ("IN", "US"):
            return

        try:
            # Get the primary cluster
            primary_cluster = Cluster.objects.get(profile=instance.profile, is_primary=True)
            # Check if kubernetes.controlplane property is set to true using prop
            # Note: this is not reliable as property may not be saved yet (not in transaction, should be?)
            # Can always fallback to use "cluster device edit" to set controlplane
            is_controlplane = instance.get_prop(props.prop_kubernetes_controlplane) is True
            # Create the association
            ClusterDevice.objects.create(
                device=instance,
                cluster=primary_cluster,
                controlplane=is_controlplane
            )
        except Cluster.DoesNotExist:
            # No primary cluster exists
            logger.warning(f"no primary cluster exists for {instance}, skip updating ClusterDevice")
            pass


@django.db.transaction.atomic
def update_clusterdevices(
    cluster: Cluster, devices: List[Device], controlplane: Optional[bool] = None, save=True
) -> List[ClusterDevice]:
    """
    Updates device cluster association and control-plane prop
    """
    planned = []

    for device in devices:
        update = None
        cluster_device = ClusterDevice.objects.filter(device=device).first()
        cp = True if controlplane is True else False

        if not cluster_device:
            logger.warning(f"unexpected state: ClusterDevice record found for '{device.name}' not found, creating new one")
            cd = ClusterDevice(cluster=cluster, device=device, controlplane=cp)
            planned.append(cd)
            continue

        if cluster_device.cluster != cluster or cluster_device.controlplane != cp:
            cluster_device.cluster = cluster
            cluster_device.controlplane = cp
            update = cluster_device

        if update:
            planned.append(update)

    if save:
        for update in planned:
            logger.debug(f"updating record {update}")
            update.save()

    return planned


def list_cluster_controlplanes(profile: str, cluster_name: Optional[str] = None) -> QuerySet[Device]:
    """
    Arguments
        profile: profile_name
        cluster_name: Optional cluster_name, primary cluster if not provided
    Return the controlplane servers for given cluster
    """
    if cluster_name:
        cluster = Cluster.objects.filter(profile__name=profile, name=cluster_name).first()
        if not cluster:
            raise ValueError(f"cluster {cluster_name} was not found")
    else:
        cluster = Cluster.objects.get(profile__name=profile, is_primary=True)

    return Device.objects.filter(
        profile__name=profile,
        cluster_device__cluster__name=cluster.name,
        cluster_device__controlplane=True,
    ).order_by("name")

def list_cluster_nodes(profile: str, cluster_name: Optional[str] = None) -> QuerySet[Device]:
    """
    Arguments
        profile: profile_name
        cluster_name: Optional cluster_name, primary cluster if not provided
    Return the c management nodes for a given cluster
    """
    if cluster_name:
        cluster = Cluster.objects.filter(profile__name=profile, name=cluster_name).first()
        if not cluster:
            raise ValueError(f"cluster {cluster_name} was not found")
    else:
        cluster = Cluster.objects.get(profile__name=profile, is_primary=True)

    return Device.objects.filter(
        profile__name=profile,
        cluster_device__cluster__name=cluster.name
    ).order_by("name")


class ClusterUpgrade(models.Model):
    """
    Model for tracking the overall cluster upgrade process
    """
    id = models.AutoField(primary_key=True)
    source_cluster = models.ForeignKey(
        Cluster,
        on_delete=models.CASCADE,
        related_name='upgrade_sources'
    )
    dest_cluster = models.ForeignKey(
        Cluster,
        on_delete=models.CASCADE,
        related_name='upgrade_dests'
    )
    upgrade_pkg_path = models.CharField(
        max_length=255,
        help_text="Path to the directory or file containing upgrade packages and manifests"
    )
    ## TODO(agam): Should we use `IntegerField` here instead, with enums ?
    ##             New Django versions also have `IntegerChoices`
    status = models.CharField(
        max_length=64,
        choices=[(status.value, status.value) for status in UpgradeProcessStatus],
        default=UpgradeProcessStatus.NOT_STARTED.value,
        help_text="Current status/stage of the overall upgrade process"
    )
    data_preparation_status = models.CharField(
        max_length=32,
        choices=[(status.value, status.value) for status in DataPreparationStatus],
        default=DataPreparationStatus.NOT_STARTED.value,
        help_text="Status of data preparation (CephFS, registry, K8s state sync)"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Upgrade {self.id}: {self.source_cluster.name} -> {self.dest_cluster.name} ({self.status})"


class ClusterUpgradeBatch(models.Model):
    """
    Model for tracking a batch of devices being upgraded together
    """
    id = models.AutoField(primary_key=True)
    upgrade_process = models.ForeignKey(
        ClusterUpgrade,
        on_delete=models.CASCADE,
        related_name='batches'
    )
    status = models.CharField(
        max_length=64,
        choices=[(status.value, status.value) for status in UpgradeBatchStatus],
        default=UpgradeBatchStatus.NOT_STARTED.value,
        help_text="Status of this batch"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Batch {self.id} for Upgrade {self.upgrade_process.id} ({self.status})"


class ClusterUpgradeBatchDevice(models.Model):
    """
    Model for tracking individual device status within an upgrade batch
    """
    id = models.AutoField(primary_key=True)
    upgrade_batch = models.ForeignKey(
        ClusterUpgradeBatch,
        on_delete=models.CASCADE,
        related_name='batch_devices'
    )
    device = models.ForeignKey(
        Device,
        ## TODO(agam): if we want to preserve history after a device is deleted,
        ##             this should be model.SET_NULL instead of CASCADE
        on_delete=models.CASCADE,
        related_name='upgrade_entries_in_batches'
    )
    status = models.CharField(
        max_length=32,
        choices=[(status.value, status.value) for status in UpgradeDeviceStatus],
        default=UpgradeDeviceStatus.PENDING.value,
        help_text="Status of this device's processing within the batch"
    )
    current_step = models.CharField(
        max_length=64,
        blank=True,
        null=True,
        help_text="The specific sub-step the device is in, e.g., 'removed_from_src_k8s', 'os_patched', 'added_to_dst_k8s'"
    )
    message = models.TextField(
        blank=True,
        null=True,
        help_text="Error message or other status details for this device's step"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('upgrade_batch', 'device')

    def __str__(self):
        return f"Device {self.device.name} in Batch {self.upgrade_batch.id} ({self.status})"


def get_device_count_by_cluster(profile: str, cluster_name: Optional[str] = None, device_type: str = "SR") -> int:
    """
    Get the count of devices or systems in a cluster.
    If cluster_name is None, returns the count for the primary cluster.

    Args:
        profile: The profile name
        cluster_name: Optional cluster name, uses primary cluster if None
        device_type: Device type to count - "SR" for servers/devices, "SY" for systems

    Returns:
        Count of devices of the specified type in the cluster

    Raises:
        ValueError: If cluster is not found or device_type is invalid
    """
    if cluster_name:
        cluster = Cluster.objects.filter(profile__name=profile, name=cluster_name).first()
        if not cluster:
            raise ValueError(f"cluster {cluster_name} was not found")
    else:
        cluster = Cluster.objects.get(profile__name=profile, is_primary=True)

    # Validate device_type parameter
    if device_type not in [DeploymentDeviceTypes.SERVER.value, DeploymentDeviceTypes.SYSTEM.value]:
        raise ValueError(f"device_type must be '{DeploymentDeviceTypes.SERVER.value}' or '{DeploymentDeviceTypes.SYSTEM.value}'")

    return ClusterDevice.objects.filter(cluster=cluster, device__device_type=device_type).count()


class ElevationEntity(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=128)
    profile = models.ForeignKey(
        DeploymentProfile, on_delete=models.CASCADE,
        related_name='elevations'
    )

    class Meta:
        unique_together = ('name', 'profile')


class DeviceSpecEntity(models.Model):
    id = models.AutoField(primary_key=True)
    elevation_entity = models.ForeignKey(
        ElevationEntity,
        related_name="device_specs",
        on_delete=models.CASCADE,
    )
    type = models.CharField(max_length=2)
    role = models.CharField(max_length=2, default="")
    count = models.PositiveIntegerField()
    name_format = models.CharField(max_length=100, null=True, blank=True)
    properties = models.JSONField(null=True, blank=True)


class RackEntity(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=128)
    elevation_entity = models.ForeignKey(
        ElevationEntity,
        on_delete=models.CASCADE,
        related_name='racks',
    )
    profile = models.ForeignKey(
        DeploymentProfile, 
        on_delete=models.CASCADE,
        related_name='racks',
    )
    part_number = models.CharField(max_length=128)
    properties = models.JSONField()

    class Meta:
        unique_together = ('name', 'profile')

    @property
    def elevation_name(self) -> str:
        return self.elevation_entity.name