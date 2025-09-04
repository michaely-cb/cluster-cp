#!/bin/bash
#
# Running from the mgmt node, this script deploys the user nodes by running:
# for each user node {
#     copy user-pkg tar ball to the user node's /opt/cerebras/package directory
#     untar the tar ball
#     cd to user-pkg directory
#     run install.sh
# }
#
# Currently we assume that the user nodes are accessible by the mgmt node, and
# they share the same root password.

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"
source "../pkg-common.sh"

# 1. Extract list of user nodes from master-config.yaml
master_config="/opt/cerebras/cluster/master-config.yaml"

if [ ! -f "$master_config" ]; then
  echo "File $master_config not found.  Skipping user node deployment"
  exit 0
fi

user_nodes=($(yq eval '.servers[] | select(.role == "US") | .name' $master_config))
root_usernames=($(yq eval '.servers[] | select(.role == "US") | .rootLogin.username' $master_config))
root_passwords=($(yq eval '.servers[] | select(.role == "US") | .rootLogin.password' $master_config))

skip_msg="Skipping user node deployment. You must manually deploy the user nodes. See /opt/cerebras/packages/user-pkg-readme. To automate the usernode deploy, add usernode root username/passwords to vendor-info.csv."

if [ ${#user_nodes[@]} -eq 0 ]; then
   echo "No user nodes found. ${skip_msg}"
   exit 0
fi

if [ ${#user_nodes[@]} -ne ${#root_usernames[@]} ]; then
   echo "Mismatch user node root usernames. ${skip_msg}"
   exit 0
fi

if [ ${#user_nodes[@]} -ne ${#root_passwords[@]} ]; then
   echo "Mismatch user node root passwords. ${skip_msg}"
   exit 0
fi

echo "User node(s): ${user_nodes}"

# 2. Determine the user-pkg tar ball
# Use the latest user-pkg file in mgmt node's /opt/cerebras/packages directory
pkgFileTarGz=($(ls -1t ${PKG_DIR}/user-pkg-*.tar.gz))

if [ -z "$pkgFileTarGz" ] ; then
    echo "User package not found in ${PKG_DIR} directory. Skipping user node deployment" >&2
    exit 0
fi

echo "Using user package file ${pkgFileTarGz}"

# 3. Deploy each user node sequentially
pkgBaseFileTarGz=$(basename "$pkgFileTarGz")
dirname=$(basename "$pkgFileTarGz" | sed -e 's/\.tar\.gz$//')

# Exit immediately if user node deployment fails.
set -e

cmd="cd /opt/cerebras/packages && tar xzf ${pkgBaseFileTarGz} && cd ${dirname} && ./install.sh"

for i in "${!user_nodes[@]}";
do
    user_node=${user_nodes[$i]}
    username=${root_usernames[$i]}
    password=${root_passwords[$i]}

    echo "Deploying user node ${user_node}"
    sshpass -p "${password}" ssh -o StrictHostKeyChecking=no "${username}@${user_node}" "mkdir -p /opt/cerebras/packages"
    sshpass -p "${password}" scp -o StrictHostKeyChecking=no -r "${pkgFileTarGz}" "${username}@${user_node}:/opt/cerebras/packages"
    sshpass -p "${password}" ssh -o StrictHostKeyChecking=no "${username}@${user_node}" "$cmd"
done
