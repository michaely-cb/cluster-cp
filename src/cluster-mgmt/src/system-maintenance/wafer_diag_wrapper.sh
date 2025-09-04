#!/bin/bash
set -e

export PYTHONUNBUFFERED=1
export PYTHONIOENCODING=utf-8

LOG_NAME="wafer_diag_$(date +%Y%m%d_%H%M%S)"
WORKDIR="wafer-diag-logs"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

echo "=== Wafer-diag Container Setup ==="
echo "Log files will be: ${LOG_NAME}.out, ${LOG_NAME}.err"

echo "Installing Python dependencies from pre-built wheels..."
if [ ! -d "/system-maintenance-framework/wheels" ]; then
    echo "ERROR: Wheels directory not found. System-client image may be outdated."
    exit 1
fi

pip install --no-index --find-links /system-maintenance-framework/wheels \
    -r /system-maintenance-framework/requirements.txt

echo "Dependencies installation complete. Starting wafer-diag..."
echo "Command: $@"

# Execute final wafer-diag command with logging
exec "$@" > >(tee -a "${LOG_NAME}.out") 2> >(tee -a "${LOG_NAME}.err" >&2)

