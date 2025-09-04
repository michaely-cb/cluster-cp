#!/bin/bash
set -e

echo "Copying system-maintenance framework..."
cp -r /system-maintenance-framework/. /shared-framework/

echo "Framework copied successfully"
echo "Verifying wheels directory..."
ls -la /shared-framework/wheels/

echo "Wheels available:"
find /shared-framework/wheels -name "*.whl" | wc -l

echo "Init container setup complete"
