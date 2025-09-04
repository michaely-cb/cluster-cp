import json
import logging
import os

from deployment_manager.db.const import DeploymentDeviceRoles
import deployment_manager.platform.manifests.const as const

logger = logging.getLogger(__name__)

DATA_DIR = f"{os.getenv('CLUSTER_DEPLOYMENT_BASE', '')}/deployment/deployment_manager/platform/manifests/data/"
BIOS_DATA_DIR = f"{os.getenv('CLUSTER_DEPLOYMENT_BASE', '')}/os-provision/tools/ostools/tools/golden/"

class Manifest:
    def __init__(self, manifest_name):
        self.name = manifest_name
        fname = os.path.join(DATA_DIR, f"{manifest_name}.json")
        with open(fname, 'r') as f:
            self._cfg = json.load(f)
        self._packages = self._read_packages()

    @classmethod
    def get_manifests(cls):
        manifests = list()
        for dirpath, _, filenames in os.walk(DATA_DIR):
            for fname in filenames:
                if not fname.startswith('manifest'):
                    continue
                if not fname.endswith('.json'):
                    continue
                manifests.append(Manifest(fname.strip('.json')))
        return manifests

    @classmethod
    def get_manifest(cls, name):
        try:
            return cls(name)
        except:
            return None

    @staticmethod
    def get_bios_config(node_role):
        config = None
        if node_role == DeploymentDeviceRoles.SWARMX.value:
            fname = os.path.join(BIOS_DATA_DIR, "sx_golden.json")
        else:
            fname = os.path.join(BIOS_DATA_DIR, "mx_golden.json")
        if os.path.exists(fname):
            with open(fname, 'r') as f:
                config = json.load(f)
        return config

    def _read_packages(self):
        pkgs = dict()
        for p in self._cfg.get('packages', []):
            pkgs[p['name']] = p
        return pkgs

    def get_packages(self):
        return self._packages.keys()

    def get_package_version(self, pkg_name):
        """ Return the version of package with pkg_name in manifest
        """
        version = None
        pkg = self._packages.get(pkg_name)
        if pkg is not None:
            version = pkg['version']
        return version

    def get_package_command(self, pkg_name):
        """ Return the command used to check version of package 'pkg_name'
        """
        cmd = None
        pkg = self._packages.get(pkg_name)
        if pkg is not None:
            cmd = pkg['command']
        return cmd

    def needs_exact_match(self, pkg_name):
        """ Return True if exact version match is required for 'pkg_name'
        """
        ret = False
        pkg = self._packages.get(pkg_name)
        if pkg is not None:
            ret = pkg.get('exact_match', False)
        return ret

    def get_package_server_roles(self, pkg_name):
        roles = None
        pkg = self._packages.get(pkg_name)
        if pkg is not None:
            roles = pkg.get('server_roles')
        return roles

    def is_package_critical(self, pkg_name):
        ret = False
        pkg = self._packages.get(pkg_name)
        if pkg is not None:
            ret = (pkg.get("severity",
                const.SEVERITY_INFO) == const.SEVERITY_CRITICAL)
        return ret

    def get_bios_version(self, vendor):
        version = None
        for b in self._cfg.get("bios", []):
            if b["vendor"] == vendor:
                version = b["version"]
                break
        return version

    def get_switch_firmware_version(self, vendor, model):
        version = None
        for v in self._cfg.get("switch_firmware", []):
            if v["vendor"] == vendor and v["model"] == model:
                version = v["version"]
                break
        return version

    def get_server_roce_status(self, role):
        roce = self._cfg.get(
            "config", {}).get(
            "server", {}).get(
            "roce", {})
        if roce:
            roles = roce.get("roles", [])
            if len(roles) == 0 or role in roles:
                return roce.get("status", False)
        return False

    def get_switch_roce_status(self):
        return self._cfg.get(
            "config", {}).get(
            "switch", {}).get(
            "roce", {}).get(
            "status", False)

    def get_system_roce_status(self):
        return self._cfg.get(
            "config", {}).get(
            "system", {}).get(
            "roce", {}).get(
            "status", False)

    def is_server_roce_critical(self, role):
        return self._cfg.get(
            "config", {}).get(
            "server", {}).get(
            "roce", {}).get(
            "severity", const.SEVERITY_INFO) == const.SEVERITY_CRITICAL

    def is_switch_roce_critical(self):
        return self._cfg.get(
            "config", {}).get(
            "switch", {}).get(
            "roce", {}).get(
            "severity", const.SEVERITY_INFO) == const.SEVERITY_CRITICAL

    def is_system_roce_critical(self):
        return self._cfg.get(
            "config", {}).get(
            "system", {}).get(
            "roce", {}).get(
            "severity", const.SEVERITY_INFO) == const.SEVERITY_CRITICAL

    def is_bios_config_critical(self):
        return self._cfg.get(
            "config", {}).get(
            "bios", {}).get(
            "severity", const.SEVERITY_INFO) == const.SEVERITY_CRITICAL
