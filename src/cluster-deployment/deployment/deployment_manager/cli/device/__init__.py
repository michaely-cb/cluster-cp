from deployment_manager.cli.device.add import DeviceAdd
from deployment_manager.cli.device.assign_ip import AssignIPs
from deployment_manager.cli.device.edit import DeviceEdit
from deployment_manager.cli.device.import_inventory import ImportInventory
from deployment_manager.cli.device.remove import DeviceRemove
from deployment_manager.cli.device.show import DeviceShow
from deployment_manager.cli.device.update import DeviceUpdate
from deployment_manager.cli.device.config_sanity import ConfigSanity

CMDS = [
    AssignIPs,
    DeviceAdd,
    DeviceEdit,
    DeviceRemove,
    DeviceShow,
    DeviceUpdate,
    ImportInventory,
    ConfigSanity,
]
