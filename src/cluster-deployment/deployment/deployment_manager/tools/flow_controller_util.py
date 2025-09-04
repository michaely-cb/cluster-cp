
from deployment_manager.client.flow_controller import BelimoFlowController, GetFlowRateRepr
from deployment_manager.db.models import (
    Device, QuerySet)
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List, Dict, Union
from deployment_manager.db import device_props as props
from deployment_manager.tools.pb1.device_status import DeviceStatus, Status

MAX_WORKERS = 128
TIMEOUT = 60.0

def create_client(device: Device) -> BelimoFlowController:
    """Create a BelimoFlowController client for the given device."""
    return BelimoFlowController(
        host=device.name,
        username=device.get_prop(props.prop_management_credentials_user) or "admin",
        password=device.get_prop(props.prop_management_credentials_password) or "tlnsg",
        ip=device.get_prop(props.prop_management_info_ip),
    )


def get_fc_config_updates(clients: List[BelimoFlowController]) -> Dict[str, Union[list, str]]:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_client = {executor.submit(client.get_config_updates): client for client in clients}
        updates = {}
        try:
            for future in as_completed(future_to_client, timeout=TIMEOUT):
                client = future_to_client[future]
                try:
                    client_updates = future.result()
                    updates[client.host] = client_updates
                except Exception as exc:
                    updates[client.host] = f"Unexpected {exc.__class__.__name__}: {exc}"
                finally:
                    future_to_client.pop(future)
        except TimeoutError as exc:
            for client in future_to_client.values():
                updates[client.host] = f"Operation timed out: {exc}"
    return updates


def put_fc_config_updates(clients: List[BelimoFlowController], updates: Dict[str, list]) -> Dict[str, DeviceStatus]:
    clients = {client.host: client for client in clients if client.host in updates}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_client = {executor.submit(clients[name].put_config, arg): name for name, arg in updates.items() if name in clients}
        results = {}
        try:
            for future in as_completed(future_to_client, timeout=TIMEOUT):
                name = future_to_client[future]
                try:
                    future.result()
                    results[name] = DeviceStatus(Status.OK, f"Configuration updated for {name}")
                except Exception as exc:
                    results[name] = DeviceStatus(Status.FAILED, f"Unexpected {exc.__class__.__name__}: {exc}")
                finally:
                    future_to_client.pop(future)
        except TimeoutError as exc:
            for name in future_to_client.values():
                results[name] = DeviceStatus(Status.FAILED, f"Operation timed out: {exc}")
    return results


def check_fc_flow_rate(clients: List[BelimoFlowController]) -> Dict[str, GetFlowRateRepr]:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_client = {executor.submit(client.get_flow_rate): client for client in clients}
        results = {}
        try:
            for future in as_completed(future_to_client, timeout=TIMEOUT):
                client = future_to_client[future]
                try:
                    results[client.host] = future.result()
                except Exception as exc:
                    results[client.host] = GetFlowRateRepr(
                        client.host,
                        error=f"Unexpected {exc.__class__.__name__}: {exc}"
                    )
                finally:
                    future_to_client.pop(future)
        except TimeoutError as exc:
            for client in future_to_client.values():
                results[client.host] = GetFlowRateRepr(
                    client.host,
                    error=f"Operation timed out: {exc}"
                )
    return results