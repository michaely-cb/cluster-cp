# For API reference:
# https://cerebras.atlassian.net/wiki/spaces/DEVINFRA/pages/2715516959/SystemDB+GUI+RestApi
import logging

import requests
from typing import Any, Tuple

from deployment_manager.db.const import DeploymentDeviceRoles
from deployment_manager.db.models import Device, DeploymentProfile
from deployment_manager.tools.config import ConfGen

logger = logging.getLogger(__name__)

HOST = "systems-db.devinfra.cerebras.aws"
URL = f"http://{HOST}"
# Default users with admin
# This is necessary since the executing user is most likely root
API_USER = "cluster_deployment"
TABLE_NAME = "multiboxes"


def _check_response(response: requests.Response) -> dict:
    if response.status_code > 299 or response.status_code < 200:
        logger.error(f"request failed ({response.status_code}), "
                     f"request.body: {response.request.body}, response.body: {response.text}")
        response.raise_for_status()
    return response.json()


def add_entry(data: dict) -> Tuple[int, Any]:
    url = f"{URL}/api/v1/insert/{TABLE_NAME}"
    if "api_user" not in data:
        data["api_user"] = API_USER

    response = requests.request("POST", url, json=data)
    reply = _check_response(response)
    return response.status_code, reply.get("message", "")


def get_entry(cluster_name: str) -> Tuple[int, dict]:
    """
        Get entries that matches the filter

        reply is a dictionary with these keys:
            count, data, status, table_name
    """
    params = {"_id": cluster_name}
    response = requests.request("GET", f"{URL}/api/v1/search/{TABLE_NAME}", params=params)
    reply = _check_response(response)
    return response.status_code, reply


def delete_entry(name: str, api_user: str = API_USER):
    payload = {
        "_id": name,
        "api_user": api_user
    }

    response = requests.request("POST", f"{URL}/api/v1/delete/{TABLE_NAME}", json=payload)
    reply = _check_response(response)
    return response.status_code, reply.get("message", "")


def update_entry(data: dict):
    if "api_user" not in data:
        data["api_user"] = API_USER

    response = requests.request("POST", f"{URL}/api/v1/update/{TABLE_NAME}", json=data)
    reply = _check_response(response)
    return response.status_code, reply.get("message", "")


def generate_systemdb_multibox_entry(profile: str,
                                     location: str = None,
                                     purpose: str = None,
                                     system_only=False) -> dict:
    """ Generate the systemdb database entry from the device db """
    cluster_name = DeploymentProfile.get_profile(profile).cluster_name
    input_cfg = ConfGen(profile).parse_profile()

    domain = input_cfg.get("basic", {}).get("domain", "")
    if domain and not domain.startswith("."):
        domain = f".{domain}"

    if not cluster_name:
        raise ValueError(f"profile '{profile}' cluster_name is not defined")
    # Map the hosts/servers
    sr_role_key = {
        DeploymentDeviceRoles.ACTIVATION.value: "activation_hosts",
        DeploymentDeviceRoles.MANAGEMENT.value: "management_hosts",
        DeploymentDeviceRoles.MEMORYX.value: "memoryx_hosts",
        DeploymentDeviceRoles.SWARMX.value: "swarmx_hosts",
        DeploymentDeviceRoles.WORKER.value: "worker_hosts",
        DeploymentDeviceRoles.USER.value: "user_hosts",
    }
    sw_role_key = {
        DeploymentDeviceRoles.MEMORYX.value: "aw_switches",
        DeploymentDeviceRoles.SWARMX.value: "br_switches",
        # set to "sleaf" as workaround of "bad request" till system db schema has "leaf" field
        DeploymentDeviceRoles.LEAF.value: "sleaf_switches",
        DeploymentDeviceRoles.SPINE.value: "spine_switches",
    }
    doc = {
        "_id": cluster_name,
        "name": cluster_name,
        "allow_access": True,
        "type": "Multibox",
        "systems": [],
    }
    if location is not None:
        doc["location"] = location
    if purpose is not None:
        doc["purpose"] = purpose

    if not system_only:
        doc.update({v: [] for v in sr_role_key.values()})
        doc.update({v: [] for v in sw_role_key.values()})
        devices = Device.get_all(profile_name=profile)
    else:
        devices = Device.get_systems(profile)

    for d in devices:
        if not d.network_pushed():
            logger.warning(f"{d.name} with role {d.device_role_str} was not added to the devinfra DB"
                           "because its was not network_pushed")
            continue

        role = d.device_role
        if d.is_switch and role in sw_role_key:
            doc[sw_role_key[role]].append(d.name + domain)
        elif d.is_server:
            if role in sr_role_key:
                doc[sr_role_key[role]].append(d.name + domain)
            else:
                logger.warning(f"server {d.name} with role {d.device_role_str} was not added to the devinfra DB"
                               "because its role was not recognized")
        elif d.is_system:
            doc["systems"].append(d.name + domain)
    return {k: sorted(v) if isinstance(v, list) else v for k, v in doc.items()}


def update_systemdb(update_body: dict, replace: bool = False, merge_update=False) -> Tuple[int, Any]:
    """ Update the systemdb database with data
    Args
        update_body: JSON object to POST
        replace: True deletes the existing entry and associated update history
        merge_updates: setting to True will replace only fields specified in update_body otherwise entire update_body
            is POSTed
    Returns
        http status code: status_code == 200 means it is successful, error otherwise
        response: body
    """
    # Get existing entry if exists
    cluster_name = update_body["name"]
    status, reply = get_entry(cluster_name)
    if status == 200 and reply["count"] > 0:
        is_new = False
        current_body = reply["data"][cluster_name]
    else:
        is_new = True
        current_body = {}

    # use existing values from database if not specified in the update or the update's string value fields are empty
    if merge_update and current_body:
        for k, v in current_body.items():
            if k not in update_body or (isinstance(v, str) and not update_body[k]):
                update_body[k] = v

    if replace:
        is_new = True
        delete_entry(name=cluster_name)

    if is_new:
        return add_entry(data=update_body)
    return update_entry(data=update_body)
