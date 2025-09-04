from concurrent.futures import ThreadPoolExecutor

import logging
from typing import Dict, List, Optional, Any, Tuple, Type, Union

from deployment_manager.tools.switch.arista import AristaSwitchCtl, AristaSwitchCommands
from deployment_manager.tools.switch.dell_os10 import DellOS10SwitchCtl, DellOS10SwitchCommands
from deployment_manager.tools.switch.dell_sonic import DellSonicSwitchCtl, DellSonicSwitchCommands
from deployment_manager.tools.switch.edgecore import EdgecoreSwitchCtl, EdgecoreSwitchCommands
from deployment_manager.tools.switch.hpe import HpeSwitchCommands, HpeComwareSwitchCtl, HpeOnyxSwitchCtl
from deployment_manager.tools.switch.interface import SwitchCtl, SwitchInfo, SwitchCommands, SwitchOSFamily
from deployment_manager.tools.switch.juniper import JuniperSwitchCtl, JuniperSwitchCommands
from deployment_manager.common.constants import MAX_WORKERS

logger = logging.getLogger(__name__)

def get_vendor_impl(vendor: str, os_family = SwitchOSFamily.NOT_SET) -> Optional[Tuple[Type[SwitchCtl], Type[SwitchCommands]]]:
    if vendor == "AR":
        return AristaSwitchCtl, AristaSwitchCommands
    elif vendor == "DL":
        if os_family == SwitchOSFamily.DELL_OS10:
            return DellOS10SwitchCtl, DellOS10SwitchCommands
        return DellSonicSwitchCtl, DellSonicSwitchCommands
    elif vendor == "EC":
        return EdgecoreSwitchCtl, EdgecoreSwitchCommands
    elif vendor == "JU":
        return JuniperSwitchCtl, JuniperSwitchCommands
    elif vendor == "HP":
        if os_family == SwitchOSFamily.HPE_COMWARE:
            return HpeComwareSwitchCtl, HpeSwitchCommands
        return HpeOnyxSwitchCtl, HpeSwitchCommands
    return None


def create_client(switch: SwitchInfo) -> Optional[SwitchCtl]:
    name, host, vendor, username, password, os_family = switch
    impl = get_vendor_impl(vendor, os_family)
    if not impl:
        return None

    return impl[0](name, host, username, password)


def perform_group_action(
        switches: List[SwitchInfo],
        method: str,
        switch_args: Optional[List[Union[Tuple[Tuple, Dict], Tuple[Tuple]]]] = None,
        *args, **kwargs
) -> Dict[str, Any]:
    """
    Executes the given method name with given args against all switches in the switch info list.
    Args
        method: name of switchctl method, e.g. list_interfaces
        switch_args: optional list of (args) or (args,kwargs) where switch_args[0] is passed to switch[0].method(..)
        args, kwargs: optional args/kwargs to be passed to switch[*].method(..) if switch_args is not set
    Returns: Dict of switchName -> result or error_message
    """
    # Ensure switch_args matches the number of switches, or defaults to repeated (args, kwargs)
    if switch_args is None:
        switch_args = [(args, kwargs)] * len(switches)
    else:
        assert len(switch_args) == len(switches), \
            "Programmer error: an (args, kwargs) tuple must be passed for each switch info object."

    def execute_method(switch_info: SwitchInfo, method: str, switch_args: Tuple[Tuple, Dict]) -> Union[str, Any]:
        ctl = create_client(switch_info)
        if ctl is None:
            return f"{method} is not implemented for vendor {switch_info.vendor}"

        if not hasattr(ctl, method):
            return f"Programmer error: {method} is not present on switch interface"

        with ctl:
            function = getattr(ctl, method)
            if len(switch_args) == 1:
                logger.debug(f"calling {switch_info.name}::{method}(args={switch_args[0]})")
                return function(*switch_args[0])
            else:
                logger.debug(f"calling {switch_info.name}::{method}(args={switch_args[0]}, kwargs={switch_args[1]})")
                return function(*switch_args[0], **switch_args[1])

    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_switch = {
            switch.name: executor.submit(execute_method, switch, method, switch_args[i])
            for i, switch in enumerate(switches)
        }

        for switch_name, future in future_to_switch.items():
            try:
                results[switch_name] = future.result(timeout=900)  # 15 minute timeout
            except Exception as e:
                results[switch_name] = str(e)

    return results
