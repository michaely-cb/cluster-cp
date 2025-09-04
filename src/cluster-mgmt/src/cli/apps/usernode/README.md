# Usernode Installer

This directory contains artifacts for usernode installation. To install usernode
components, you must cd into the directory containing `install.sh` and then
run the `./install.sh` script as root user or sudo.

Do not run the installer downloaded from clusterA whilst intending to target
clusterB! The usernode installer has some customization per the cluster that
the installer was downloaded from. This cluster-specific config is located in
`install.json` and is used by the script.

The installer script roughly does the following
- places wheels in `/opt/cerebras/wheels`
- extracts cluster-specific certs to `/opt/cerebras/certs`
- places the cluster-specific config at `/opt/cerebras/config_v2`
- installs `csctl`
- modifies some host settings
  - increases max open file limit to accomodate pytorch
  - modifies `/etc/hosts` with an entry for the cluster's frontdoor API
