import logging
import pathlib

from typing import Dict, Tuple, List

from deployment_manager.tools.pb1.ansible_task import (UpdateDhcpMirrorServer, UpdateDhcpServer)
from deployment_manager.tools.pb1.device_status import Status

logger = logging.getLogger(__name__)


def run_dhcp_updates(
        profile: str,
        provider: "DhcpProvider",
        configs: Dict[str, List[Tuple[pathlib.Path, str]]] = None,
        skip_root=False,
        mirror_servers=None,
) -> str:
    """ Returns a non-empty error string if the update failed """
    if not skip_root:
        update_dhcp = UpdateDhcpServer(profile, provider, configs)
        result = update_dhcp.run(["root-server"])
        if result["root-server"].status != Status.OK:
            return "dhcp service update failed, check ansible log for details"
        logger.info("dhcp service on root server restarted and updated")

    if mirror_servers:
        update_dhcp_mirror = UpdateDhcpMirrorServer(profile)
        results = update_dhcp_mirror.run(mirror_servers)
        errors = []
        for server, status in results.items():
            if status.status == Status.OK:
                logger.info(f"dhcp service on mirror servers {server} restarted and updated")
            else:
                errors.append(server)
        if errors:
            return f"dhcp mirror servers {', '.join(errors)} failed to update"

    return ""
