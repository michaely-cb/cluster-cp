import argparse
import json
import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def has_package(package, role, vendor):
    """Check if the host contains the required package"""
    roles = package.get("roles", [])
    if len(roles) and not role in roles:
        # package check not applicable
        return True, "NA"

    vendors = package.get("vendor", [])
    if len(vendors) and not vendor in vendors:
        # package check not applicable
        return True, "NA"

    cmd = package.get("command")
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    if result.returncode:
        # Error in getting the package version
        return False, "unknown"

    out = result.stdout.decode("utf-8").strip()
    return out == package.get("version"), out


def has_config(config, role, vendor):
    """Check if the host has the required config"""
    roles = config.get("roles", [])
    if len(roles) and not role in roles:
        # config check not applicable
        return True, "NA"

    vendors = config.get("vendor", [])
    if len(vendors) and not vendor in vendors:
        # config check not applicable
        return True, "NA"

    cmd = config.get("get_command")
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    if result.returncode:
        # Error in getting the config
        return False, "unknown"

    out = result.stdout.decode("utf-8").strip()
    return out == config.get("output"), out


def get_package_version(package_manifest, role, vendor):
    """Determine the package version of the current host"""
    with open(package_manifest, "r") as f:
        package_json = json.load(f)

    versions = package_json.get("package_versions", [])

    # Loop over each version, return the first one matched
    # Record any mismatched packages for the first version
    mismatched = []
    first_version = True

    for version in versions:
        version_matched = True
        for p in version.get("packages", []):
            status, out = has_package(p, role, vendor)
            if not status:
                version_matched = False
                if first_version:
                    mismatch = dict()
                    mismatch["name"] = p.get("name")
                    mismatch["expected"] = p.get("version")
                    mismatch["actual"] = out
                    mismatched.append(mismatch)
                else:
                    break

        first_version = False
        if version_matched:
            return version.get("name"), []

    # No version matched
    return "unknown", mismatched


def get_config_version(config_manifest, role, vendor):
    """Determine the config version of the current host"""
    with open(config_manifest, "r") as f:
        config_json = json.load(f)

    versions = config_json.get("config_versions", [])

    # Loop over each version, return the first one matched
    # Record any mismatched configs for the first version
    mismatched = []
    first_version = True

    for version in versions:
        version_matched = True
        for c in version.get("configs", []):
            status, out = has_config(c, role, vendor)
            if not status:
                version_matched = False
                if first_version:
                    mismatch = dict()
                    mismatch["name"] = c.get("name")
                    mismatch["expected"] = c.get("output")
                    mismatch["actual"] = out
                    mismatched.append(mismatch)
                else:
                    break

        first_version = False
        if version_matched:
            return version.get("name"), []

    # No version matched
    return "unknown", mismatched


def get_compatible_cbcore(cbcore_compat, package_version, config_version):
    """Determine the list of compatible cbcores"""
    with open(cbcore_compat, "r") as f:
        cbcore_json = json.load(f)

    cbcores = []
    cbcore_versions = cbcore_json.get("cbcore", [])
    for cbcore in cbcore_versions:
        for compat_version in cbcore.get("compatibility", []):
            if package_version == compat_version.get(
                "package"
            ) and config_version == compat_version.get("config"):
                cbcores.append(cbcore.get("name"))
                break

    return cbcores


def gen_platform_version(
    package_manifest, config_manifest, cbcore_compat, platform_output, role, vendor
):
    """Generate the platform-version.json file of the current host"""
    # Return 0 if output file is generated, 1 otherwise

    # Remove existing output file
    if os.path.isfile(platform_output):
        os.remove(platform_output)

    pkg_version, mismatched_pkgs = get_package_version(package_manifest, role, vendor)
    config_version, mismatched_configs = get_config_version(
        config_manifest, role, vendor
    )

    cbcores = get_compatible_cbcore(cbcore_compat, pkg_version, config_version)

    platform = dict()
    platform["package"] = pkg_version
    platform["config"] = config_version
    platform["cbcore"] = cbcores

    if len(mismatched_pkgs) > 0:
        platform["mismatched_packages"] = mismatched_pkgs

    if len(mismatched_configs) > 0:
        platform["mismatched_configs"] = config_version

    with open(platform_output, "w") as outfile:
        json.dump(platform, outfile, indent=4)

    return not os.path.isfile(platform_output)


def main() -> int:
    """Main function to generate platform-version.json file of the current host"""

    parser = argparse.ArgumentParser(
        description="Generate platform-version.json of this server host"
    )
    parser.add_argument(
        "--package", required=True, help="package-manifest.json input file"
    )
    parser.add_argument(
        "--config", required=True, help="config-manifest.json input file"
    )
    parser.add_argument(
        "--cbcore", required=True, help="cbcore-compatibility.json input file"
    )
    parser.add_argument(
        "--output", required=True, help="platform-version.json output file"
    )
    parser.add_argument("--role", required=True, help="server role")
    parser.add_argument("--vendor", required=True, help="server vendor")
    args = parser.parse_args()

    return gen_platform_version(
        args.package, args.config, args.cbcore, args.output, args.role, args.vendor
    )


if __name__ == "__main__":
    import sys
    sys.exit(main())
