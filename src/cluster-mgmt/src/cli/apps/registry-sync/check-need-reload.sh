#!/usr/bin/env bash

SCRIPT_PATH=$(dirname "$0")
cd "$SCRIPT_PATH"
source "../pkg-common.sh"

# Only reload images for multiple management nodes.
if has_multiple_mgmt_nodes; then
  exit 1
else
  exit 0
fi
