import typing
from enum import Enum


class EnumBase(Enum):
    @classmethod
    def byname(cls, name:str):
        return getattr(cls, name.upper())

    @classmethod
    def values(cls):
        return [e.value for e in cls]


class DeploymentDeviceTypes(EnumBase):
    SERVER = "SR"
    SWITCH = "SW"
    SYSTEM = "SY"
    CONSOLE = "CN"
    PERIPHERAL = "PR"

    def is_valid_role(self, role: typing.Union['DeploymentDeviceRoles', str]) -> bool:
        """
        Check if the device role is valid for the current type

        if <role> is a str, it can be DeploymentDeviceRoles name or value
        """
        check_role = role
        if isinstance(role, str):
            if hasattr(DeploymentDeviceRoles, role):
                check_role = DeploymentDeviceRoles.byname(role)
            else:
                check_role = DeploymentDeviceRoles(role)

        return check_role in VALID_DEVICE_TYPE_ROLE[self]


class DeploymentStageStatus(EnumBase):
    NOT_STARTED = 0
    STARTED = 1
    COMPLETED = 2
    NEEDS_UPDATE = 3
    FAILED = -1
    INCOMPLETE = -2

class DeviceDeploymentStages(EnumBase):
    IPMI_CONFIGURATION = 1
    OS_INSTALLATION = 2
    PROVISIONING = 3
    NIC_COLLECTION = 4
    NETWORK_INIT = 5
    NETWORK_GENERATE = 6
    NETWORK_PUSH = 7
    NFS_MOUNT = 8
    FREEIPA = 9
    K8S = 10
    LOCKDOWN = 11


class DeploymentSections(EnumBase):
    PRECHECK = 0
    EXECUTE = 1
    VALIDATE = 2


class DeploymentDeviceRoles(EnumBase):
    ACTIVATION = "AX"
    MANAGEMENT = "MG"  # MG node or switch
    MEMORYX = "MX"  # MX node or switch
    SWARMX = "SX"  # SX node or switch
    WORKER = "WK"
    USER = "US"
    SYSTEM = "CS"
    LEAF = "LF"  # 100G leaf switch
    SPINE = "SP"  # 100G spine switch
    INFRA = "IN"  # infrastructure node
    MEDIACONV = "MC"  # Media converter switch
    FRONTEND = "FE"  # Frontend switch
    MG_AGGREGATE = "MA" # Management Aggregate
    VALVECTRL = "VC" # Valve controller
    INFERENCEDRIVER = "IX"  # Inference driver "inferencex" node
    PDU = "PU"  # Power Distribution Unit

    @staticmethod
    def to_str(r: typing.Union['DeploymentDeviceRoles', str]) -> str:
        if isinstance(r, DeploymentDeviceRoles):
            return r.value
        return r

    def is_valid_type(self, device_type: typing.Union['DeploymentDeviceTypes', str]) -> bool:
        """
        Check if the device type is valid for the current role

        if <type> is a str, it can be DeploymentDeviceTypes name or value
        """
        check_type = device_type
        if isinstance(device_type, str):
            if hasattr(DeploymentDeviceTypes, device_type):
                check_type = DeploymentDeviceTypes.byname(device_type)
            else:
                check_type = DeploymentDeviceTypes(device_type)

        return self in VALID_DEVICE_TYPE_ROLE[check_type]


class DeploymentDeviceVendors(EnumBase):
    ARISTA = "AR"
    HPE = "HP"
    DELL = "DL"
    JUNIPER = "JU"
    EDGECORE = "EC"
    SUPERMICRO = "SM"
    CREDO = "CR"
    LENOVO = "LN"
    CEREBRAS = "CS"
    OPENGEAR = "OG"
    ENLOGIC = "EN"
    VERTIV = "VE"
    SERVERTECH = "ST"
    APC = "AP"
    BELIMO = "BL"


VALID_DEVICE_TYPE_ROLE = {
    DeploymentDeviceTypes.SERVER: [
        DeploymentDeviceRoles.ACTIVATION,
        DeploymentDeviceRoles.MANAGEMENT,
        DeploymentDeviceRoles.MEMORYX,
        DeploymentDeviceRoles.SWARMX,
        DeploymentDeviceRoles.USER,
        DeploymentDeviceRoles.WORKER,
        DeploymentDeviceRoles.INFRA,
        DeploymentDeviceRoles.INFERENCEDRIVER,
    ],
    DeploymentDeviceTypes.SWITCH: [
        DeploymentDeviceRoles.MANAGEMENT,
        DeploymentDeviceRoles.MEMORYX,
        DeploymentDeviceRoles.SWARMX,
        DeploymentDeviceRoles.LEAF,
        DeploymentDeviceRoles.SPINE,
        DeploymentDeviceRoles.MEDIACONV,
        DeploymentDeviceRoles.FRONTEND,
        DeploymentDeviceRoles.MG_AGGREGATE,
    ],
    DeploymentDeviceTypes.SYSTEM: [DeploymentDeviceRoles.SYSTEM],
    DeploymentDeviceTypes.CONSOLE: [DeploymentDeviceRoles.INFRA],
    DeploymentDeviceTypes.PERIPHERAL: [
        DeploymentDeviceRoles.MEDIACONV,
        DeploymentDeviceRoles.VALVECTRL,
        DeploymentDeviceRoles.PDU,

    ],
}
