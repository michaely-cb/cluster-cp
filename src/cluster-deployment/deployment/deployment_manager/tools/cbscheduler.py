from typing import Any, Tuple

import requests

URL = "http://cbs.devinfra.cerebras.aws"

# Default users with admin
# This is necessary since the executing user is most likely root
API_USER = "cluster_deployment"


def add_resource(resource_name: str, data: dict = None) -> Tuple[int, Any, str]:
    """
        Add a new resource or enable a disabled resource
    """
    url = f"{URL}/api/v1/manage/resource/{resource_name}"

    payload = {"api_user": API_USER, "cmd": "new"}
    if data:
        payload["data"] = data
    response = requests.request("POST", url, json=payload)

    try:
        reply = response.json()
        output = reply.get("message", "")
    except:
        output = response.text

    return response.status_code, output, response.reason


def get_resource(filters: dict = None, constraints: dict = None) -> Tuple[int, Any, str]:
    """
        Retrieve the resource info
    """
    url = f"{URL}/api/v1/get_resource"
    filters = filters or {}
    constraints = constraints or {}
    response = requests.request("GET", url, params={**filters, **constraints})
    try:
        output = response.json()
    except:
        output = response.text

    return response.status_code, output, response.reason
