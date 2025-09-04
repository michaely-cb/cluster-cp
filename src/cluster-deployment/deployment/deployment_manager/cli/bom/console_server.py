import re
import logging
from typing import Dict, Optional
from dataclasses import dataclass, asdict
from deployment_manager.tools.ssh import SSHConn

logger = logging.getLogger(__name__)

@dataclass
class ConsoleServerBomInfo:
    """BOM information for a console server"""
    name: str
    vendor: str
    model_name: str
    serial_number: str
    software_version: str
    role: str
    ip: str
    
    def to_dict(self) -> dict:
        return asdict(self)

@dataclass
class ConsoleServerInfo:
    """Information needed to connect to a console server"""
    name: str
    addr: str
    vendor: str
    user: str
    password: str

class ConsoleServerCtl:
    """Control class for interacting with console servers"""
    
    def __init__(self, console_server_info: ConsoleServerInfo, logger: Optional[logging.Logger] = None):
        self._name = console_server_info.name
        self._host = console_server_info.addr
        self._username = console_server_info.user
        self._password = console_server_info.password
        self._vendor = console_server_info.vendor
        self._exec: Optional[SSHConn] = None
        self.logger = logger or logging.getLogger(__name__)
        
    def __enter__(self) -> 'ConsoleServerCtl':
        # Simplified version that doesn't handle specific exceptions
        self._exec = SSHConn(self._host, self._username, self._password)
        self._exec.__enter__()
        return self
    
    def __exit__(self, *args):
        if isinstance(self._exec, SSHConn):
            self._exec.__exit__(*args)
    
    def get_bom_info_cmds(self) -> Dict[str, str]:
        """Return commands needed to gather BOM information"""
        return {
            "GET_SERIAL_NUMBER": "showserial",
            "GET_FIRMWARE_VERSION": "cat /etc/version"
        }
    
    def execute_command(self, cmd: str) -> str:
        """Execute a single command and return its output"""
        _, stdout, _ = self._exec.exec(cmd, throw=True)
        return stdout.strip()
    
    def get_bom_info(self) -> ConsoleServerBomInfo:
        """Get BOM information from the console server"""
        role = ""  # Will be filled with the device role from the database
        
        try:
            cmds = self.get_bom_info_cmds()
            
            # Get serial number
            serial_number = self.execute_command(cmds["GET_SERIAL_NUMBER"])
            
            # Get firmware version and model name
            firmware_output = self.execute_command(cmds["GET_FIRMWARE_VERSION"])
            
            firmware_match = re.search(r'Version\s+(\S+)', firmware_output)
            firmware_version = firmware_match.group(1) if firmware_match else ""
                
            model_match = re.search(r'OpenGear/(\S+)', firmware_output)
            model_name = model_match.group(1) if model_match else ""
            
            # Create BOM info
            return ConsoleServerBomInfo(
                name=self._name,
                vendor=self._vendor,
                model_name=model_name,
                serial_number=serial_number,
                software_version=firmware_version,
                role=role,
                ip=self._host
            )
        except Exception as e:
            # Just propagate the error to higher levels instead of handling it here
            self.logger.debug(f"Error executing commands on {self._name}: {str(e)}")
            raise