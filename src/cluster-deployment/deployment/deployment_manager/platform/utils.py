import json
import logging
from deployment_manager.tools.utils import exec_cmd, exec_remote_cmd

logger = logging.getLogger(__name__)

VENDOR_HPE = "hpe"
VENDOR_DELL = "dell"

def get_package_versions(manifest, node):
    """ Returns a dict of the format {'pkg_name': 'version'}
    """
    versions = dict()
    cmds = []
    for p in manifest.get_packages():
        cmd = manifest.get_package_command(p)
        ret, version_output, _ = exec_remote_cmd(cmd, node, 'root')
        if ret:
            logger.error(f"Failed to get {p} version on {node}")
            versions[p] = ""
        else:
            versions[p] = version_output.strip()
    return ret, versions

def get_bios_vendor(node):
    vendor = None
    cmd = 'dmidecode -s bios-vendor'
    ret, out, _ = exec_remote_cmd(cmd, node, 'root')
    if ret:
        logger.error(f"Failed to get BIOS vendor for {node}")
    else:
        vendor = out.strip().strip('\n').lower()
    return ret, vendor

def get_bios_version(node):
    version = None
    cmd = 'dmidecode -s bios-version'
    ret, out, _ = exec_remote_cmd(cmd, node, 'root')
    if ret:
        logger.error(f"Failed to get BIOS version for {node}")
    else:
        version = out.strip().strip('\n')
    return ret, version

def get_server_roce_status(server):
    # Get all 100G network interface name(s)
    cmd = "ifconfig -a | grep 'mtu 9000' | awk -F':' '{print $1}'"
    ret, out, _ = exec_remote_cmd(cmd, server, 'root')
    if ret:
        logger.error(f"Failed to get 100G interface name(s) for {server}")
        return ret, False

    # Check if all interfaces have RoCE enabled
    interfaces = out.splitlines()
    for i in interfaces:
        cmd = f"mlnx_qos -i {i}" + " | grep 'Priority trust state' | awk '{print $NF}'"
        ret, out, _ = exec_remote_cmd(cmd, server, 'root')
        if ret:
            logger.error(f"Failed to get RoCE for {server} network interface {i}")
            return ret, False
        if out.strip() != "dscp":
            return ret, False

    # All interfaces have RoCE enabled
    return 0, True

def get_system_roce_status(system):
    status = None
    cmd = 'cs config roce show --output-format json'
    ret, out, _ = exec_remote_cmd(cmd, system, 'root')
    if ret:
        if 'Error: Invalid subcommand "roce"' in out or 'Error: unknown flag: --output-format' in out:
            # roce command removed
            return 0, True
        logger.error(f"Failed to get RoCE status for {system}")
    else:
        sd = json.loads(out)
        if 'ENABLED' in sd['defaultStateDesc'] and sd['state'] == 'DEFAULT':
            status = True
        elif sd['state'] == 'ENABLED':
            status = True
        else:
            status = False
    return ret, status

def get_ipmi_host(node):
    ipaddr = None
    ret, out, _ = exec_remote_cmd(
                    "ipmitool lan print | grep 'IP Address\s*:'",
                    node,
                    'root'
                  )
    if not ret:
        elems = out.split()
        if len(elems) == 4:
            ipaddr = elems[-1].strip()
    return ipaddr

def get_bios_config(node_name, node_vendor,
            ipmi_user, ipmi_password):
    ret = True
    config = None
    ipmi_host = get_ipmi_host(node_name)
    if ipmi_host is not None and node_vendor == VENDOR_HPE:
        cmd = f"curl -s -k -u {ipmi_user}:{ipmi_password} https://{ipmi_host}/redfish/v1/systems/1/bios"
        ret, out, _ = exec_cmd(cmd, logcmd=False)
        if not ret:
            try:
                config = json.loads(out).get("Attributes")
            except:
                ret = True
    return ret, config
