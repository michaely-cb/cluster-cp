#!/bin/bash

# Retrieve the system name in the cluster from system DB.
#
# Since we have systems on multiple domains, we need the name to include the domain
# information. This script will retrieve the name from the system DB.
#
# EXAMPLE:
#    get-system-name.sh mb-wsperfdrop1
#

cd "$(dirname "$0")"
source common.sh

get_devinfra_db_env "$1" 2>&1 > /dev/null
echo "$SYSTEM"