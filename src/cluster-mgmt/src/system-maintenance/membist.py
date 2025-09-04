import asyncio
import os
import sys
import time

from system_maintenance_helper import (
    collect_system_status,
    assert_system_maintenance_status,
    assert_system_health_status,
    get_system_credentials,
    poll_membist_status,
    start_membist,
    SystemMaintenanceExecutionFailedError,
    DRYRUN_SLEEP_SECONDS,
)

from system_client.system_exporter_logger import logger
from system_client.tunnel_system_client import TunneledSystemClient

async def execute_membist():
    args = sys.argv[1:]
    system_name = args[0]
    system_addr = args[1]
    skip_failed_ops = True if args[2] == "skip-failed-ops" else False # Skip failed ops
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

        if not allow_error:
            assert_system_health_status(system_status)
            assert_system_maintenance_status(system_status, expect_maintenance_enabled=False)

        # Start membist
        logger.info("Starting membist...")
        test_id = await start_membist(system_name, system_grpc_client, vddc_offset, skip_failed_ops)

        # Polling for the result
        logger.info(f"Polling membist status for test id {test_id}")
        await poll_membist_status(system_name, system_grpc_client, test_id)

        # post-check
        logger.info("Post checking...")
        system_status = await collect_system_status(system_name, system_grpc_client)
        if system_status is None:
            raise SystemMaintenanceExecutionFailedError("System post-check failed due to unable to retrieve system status summary")
        logger.info(f"System status summary from post-check:\n{system_status}")

        if not allow_error:
            assert_system_health_status(system_status)
            assert_system_maintenance_status(system_status, expect_maintenance_enabled=False)

    except Exception as e:
        logger.error(f"Error executing membist: {e}")
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

    asyncio.run(execute_membist())
