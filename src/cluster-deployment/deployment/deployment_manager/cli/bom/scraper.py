import time
import logging
import concurrent.futures
from typing import Dict, List, Optional, Callable, Any, Type
from enum import Enum
from dataclasses import dataclass
import paramiko
import socket

from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device
from deployment_manager.tools.utils import to_duration
from deployment_manager.common.constants import MAX_WORKERS

# Import all device-specific modules
from deployment_manager.cli.bom.console_server import ConsoleServerCtl, ConsoleServerInfo

logger = logging.getLogger(__name__)

class DeviceType(Enum):
    """Enum representing different device types"""
    SERVER = "server"
    SYSTEM = "system"
    CONSOLE_SERVER = "console_server"
    MEDIA_CONVERTER = "media_converter"

@dataclass
class DeviceTypeConfig:
    """Configuration for a device type"""
    info_class: Type
    client_class: Type
    get_devices_func: Callable
    friendly_name: str

# Device type configuration mapping
DEVICE_TYPE_CONFIG = {
    DeviceType.CONSOLE_SERVER: DeviceTypeConfig(
        info_class=ConsoleServerInfo,
        client_class=ConsoleServerCtl,
        get_devices_func=Device.get_console_servers,
        friendly_name="console servers"
    )
}

def _get_device_args(device: Device, device_type: DeviceType) -> Any:
    """Get connection information for a device based on its type"""
    name = device.name
    
    # Common properties for all devices
    vendor = device.get_prop(props.prop_vendor_name) or "Cerebras"
    addr = str(device.get_prop(props.prop_management_info_ip))
    user = device.get_prop(props.prop_management_credentials_user, include_default=False) or "admin"
    pw = device.get_prop(props.prop_management_credentials_password, include_default=False) or "admin"
    
    if not addr or not vendor:
        raise ValueError(f"{name} missing one of {props.prop_vendor_name}, {props.prop_management_info_ip}")
    
    # Handle device-specific info objects
    if device_type == DeviceType.CONSOLE_SERVER:
        return ConsoleServerInfo(name, addr, vendor, user, pw)
    else:
        raise ValueError(f"Unknown device type: {device_type}")

def _gather_devices(devices: List[Device], device_type: DeviceType) -> List[Any]:
    """Gather connection information for multiple devices of the same type"""
    device_infos = []
    for device in devices:
        try:
            device_infos.append(_get_device_args(device, device_type))
        except ValueError as e:
            logger.error(str(e))
    return device_infos

def _collect_single_device_info(
    device_info: Any, 
    device_type: DeviceType,
    extra_args: Dict = None
) -> Optional[Any]:
    """Collect BOM information from a single device"""
    config = DEVICE_TYPE_CONFIG[device_type]
    start_time = time.time()
    device_name = device_info.name
    
    try:
        # Create the appropriate client class with the device info
        client_kwargs = {f"{device_type.value}_info": device_info}
        with config.client_class(**client_kwargs) as client:
            # Call get_bom_info with any extra arguments
            if extra_args and device_type == DeviceType.SERVER and "get_software_packages" in extra_args:
                bom_info = client.get_bom_info(extra_args["get_software_packages"])
            else:
                bom_info = client.get_bom_info()
            
            elapsed_time = time.time() - start_time
            logger.debug(f"Collected info for {device_name}, time taken: {elapsed_time:.2f} seconds")
            
            return bom_info
            
    except paramiko.AuthenticationException:
        logger.error(f"Authentication failed for {device_name}")
    except (socket.timeout, TimeoutError):
        logger.error(f"Connection timed out for {device_name}")
    except paramiko.ssh_exception.NoValidConnectionsError:
        logger.error(f"Connection failed for {device_name}")
    except paramiko.SSHException as e:
        logger.error(f"SSH connection failed for {device_name}: {str(e)}")
    except Exception as e:
        logger.error(f"Error collecting info from {device_name}: {str(e)}")
    
    elapsed_time = time.time() - start_time
    logger.debug(f"Failed collecting info for {device_name}, time taken: {elapsed_time:.2f} seconds")
    return None

def get_multi_devices_bom(
    profile: str, 
    device_type: DeviceType,
    extra_args: Dict = None
) -> Dict[str, Dict]:
    """Get BOM information for all devices of a specific type in a profile"""
    config = DEVICE_TYPE_CONFIG[device_type]
    
    # Get devices of the specified type
    devices = config.get_devices_func(profile)
    
    if not devices:
        logger.info(f"No {config.friendly_name} found in the database")
        return {}
    
    # Gather connection information for all devices
    device_infos = _gather_devices(devices, device_type)
    if not device_infos:
        logger.info(f"No valid {config.friendly_name} found to collect BOM information")
        return {}
    
    # Use thread pool to collect information from all devices in parallel
    total_devices = len(device_infos)
    completed_devices = 0
    device_boms = {}
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        
        # Submit all tasks to the executor
        for device_info in device_infos:
            future = executor.submit(
                _collect_single_device_info,
                device_info,
                device_type,
                extra_args
            )
            futures[future] = device_info.name
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            device_name = futures[future]
            completed_devices += 1
            
            try:
                bom_info = future.result()
                if bom_info:
                    device_boms[device_name] = bom_info.to_dict()
                logger.info(f"Collected info for {device_name} ({completed_devices}/{total_devices})")
            except Exception as e:
                logger.error(f"Failed to collect info for {device_name}: {str(e)}")
    
    elapsed_time = time.time() - start_time
    logger.info(f"{config.friendly_name.capitalize()} BOM collection completed in {to_duration(elapsed_time)}")
    return device_boms