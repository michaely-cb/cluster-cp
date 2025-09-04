import asyncio
import os
import sys
import time

from system_maintenance_helper import (
    collect_system_status,
    assert_system_maintenance_status,
    assert_system_health_status,
    assert_system_maintenance_purpose_if_enabled,
    get_system_credentials,
    set_vddc_offset,
    unset_vddc_offset,
    SystemMaintenanceExecutionFailedError,
    DRYRUN_SLEEP_SECONDS,
)

from system_client.system_exporter_logger import logger
from system_client.tunnel_system_client import TunneledSystemClient

async def execute_vddc():
    args = sys.argv[1:]
    system_name = args[0]
    system_addr = args[1]
    vddc_set = True if args[2] == "set" else False # set or unset
    expect_maintenance_enabled = not vddc_set
    vddc_offset = int(args[3]) # offset value
    allow_error = True if args[4] == "allow-error" else False # allow-error or not-allow-error

    username, password = get_system_credentials()
    tunnel_client = TunneledSystemClient(system_name, system_addr, username, password)

    try:
        await tunnel_client.connect()
        system_grpc_client = tunnel_client.system_client

        if not system_grpc_client:
            raise SystemMaintenanceExecutionFailedError("Unable to retrieve system_grpc_client")

        # pre-check
        logger.info("Pre checking...")
        system_status = await collect_system_status(system_name, system_grpc_client)
        if system_status is None:
            raise SystemMaintenanceExecutionFailedError("System pre-check failed due to unable to retrieve system status summary")
        logger.info(f"System status summary from pre-check:\n{system_status}")

        # when maintenance mode is enabled, the purpose should already say "vddc" and "set" for both set/unset operations
        assert_system_maintenance_purpose_if_enabled(system_status, "vddc", "set")

        if not allow_error:
            assert_system_health_status(system_status)
            # user should be able to adjust vddc even if maintenance status is enabled
            if not vddc_set:
                assert_system_maintenance_status(system_status, expect_maintenance_enabled)

        if vddc_set:
            result = await set_vddc_offset(system_name, system_grpc_client, vddc_offset)
        else:
            result = await unset_vddc_offset(system_name, system_grpc_client)

        if result is None or result.status != "COMPLETED":
            raise SystemMaintenanceExecutionFailedError(f"{'Setting' if vddc_set else 'Unsetting'} of vddc voltage has failed")

        # post-check
        logger.info("Post checking...")
        system_status = await collect_system_status(system_name, system_grpc_client)
        if system_status is None:
            raise SystemMaintenanceExecutionFailedError("System post-check failed due to unable to retrieve system status summary")
        logger.info(f"System status summary from post-check:\n{system_status}")

        if not allow_error:
            assert_system_health_status(system_status)
            assert_system_maintenance_status(system_status, not expect_maintenance_enabled)

    except Exception as e:
        logger.error(f"Error executing vddc: {e}")
        raise SystemMaintenanceExecutionFailedError(f"Execution failed: {e}") from e
    finally:
        await tunnel_client.close()

if __name__ == "__main__":
    # Check if dryrun mode is enabled
    if os.getenv("DRYRUN_MODE") == "true":
        logger.info("Dry run mode enabled. Skipping actual execution.")
        sleep_duration = int(os.getenv("DRYRUN_SLEEP_SECONDS", DRYRUN_SLEEP_SECONDS))
        time.sleep(sleep_duration)
        sys.exit(0)

    asyncio.run(execute_vddc())