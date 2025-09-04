import asyncio
import base64
import collections
import os
import time

from kubernetes import client, config

from system_client.system_client import SystemClient, SystemClientGrpcError
from system_client.system_exporter_logger import logger

from pb.cli.diagnostics import CliDiagnostics_pb2
from pb.cli.system import CliSystem_cs2_pb2, status_cs2_pb2
from pb.schema import TestResult_pb2

class SystemMaintenanceExecutionFailedError(Exception):
    pass

DRYRUN_SLEEP_SECONDS = 3
SYSTEM_CREDENTIAL_NAME = "system-credential"
SYSTEM_CREDENTIAL_NAMESPACE = "prometheus"

def get_system_credentials():
    secrets = collections.defaultdict(str)

    try:
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        secret = v1.read_namespaced_secret(SYSTEM_CREDENTIAL_NAME, SYSTEM_CREDENTIAL_NAMESPACE)
        username_b64 = secret.data.get("user", "")
        password_b64 = secret.data.get("password", "")

        # Decode base64 values
        secrets["user"] = base64.b64decode(username_b64).decode("utf-8") if username_b64 else os.getenv("SYSTEM_LOGIN_USER")
        secrets["password"] = base64.b64decode(password_b64).decode("utf-8") if password_b64 else os.getenv("SYSTEM_LOGIN_PASSWORD")
    except Exception as e:
        logger.warning(f"Error loading secrets: {e}")
        secrets["user"] = os.getenv("SYSTEM_LOGIN_USER")
        secrets["password"] = os.getenv("SYSTEM_LOGIN_PASSWORD")
    return secrets["user"], secrets["password"]

async def collect_system_status(
    system_name: str,
    system_grpc_client: SystemClient,
) -> CliSystem_cs2_pb2.ShowResponse:
    """Retrieve the current system status via gRPC

    Args:
        system_name: Name of the target system
        system_grpc_client: gRPC client to interact with the system

    Returns:
        A ShowResponse object containing system details, or None in case of failure
    """
    logger.info(f"{system_name}: Collecting system status")
    try:
        response = await system_grpc_client.system_show()
        return response
    except SystemClientGrpcError as e:
        logger.error(f"SystemClient gRPC call failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while fetching system status: {e}")
        return None


async def set_vddc_offset(
    system_name: str,
    system_grpc_client: SystemClient,
    vddc_offset: int
) -> TestResult_pb2.TestStatus:
    """Apply on offset on VDDC voltage for the system

    Args:
        system_name: Name of the target system
        system_grpc_client: gRPC client to interact with the system
        vddc_offset: The voltage offset to be applied (in mV)

    Returns:
        A TestStatus object indicating the operation result, or None in case of failure
    """
    if vddc_offset not in range(-50, 51): #range from -50 to 50
        raise SystemMaintenanceExecutionFailedError(f"vddc offset value {vddc_offset} is out of range (+/-50mV)")

    logger.info(f"{system_name}: Setting VDDC offset to {vddc_offset} mV")
    try:
        response = await system_grpc_client.vddc_set(vddc_offset=vddc_offset)
        logger.info(f"vddc set status: \n{response}")
        return response.vddcSetStatus
    except SystemClientGrpcError as e:
        logger.error(f"SystemClient gRPC call failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while setting VDDC offset: {e}")
        return None

async def unset_vddc_offset(
    system_name: str,
    system_grpc_client: SystemClient
) -> TestResult_pb2.TestStatus:
    """Reset the VDDC voltage to its default value

    Args:
        system_name: Name of the target system
        system_grpc_client: gRPC client to interact with the system

    Returns:
        A TestStatus object indicating the operation result, or None in case of failure
    """
    logger.info(f"{system_name}: Unsetting VDDC offset")
    try:
        response = await system_grpc_client.vddc_unset()
        logger.info(f"vddc unset status: \n{response}")
        return response.vddcUnsetStatus
    except SystemClientGrpcError as e:
        logger.error(f"SystemClient gRPC call failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while unsetting VDDC offset: {e}")
        return None

async def start_membist(
    system_name: str,
    system_grpc_client: SystemClient,
    vddc_offset: int,
    skip_failed_ops: bool = False
) -> str:
    """Start HL Memory BIST

    Args:
        system_name: Name of the target system
        system_grpc_client: gRPC client to interact with the system
        vddc_offset: VDDC offset to be applied during the test (in mV)
        skip_failed_ops: If True, continues without failing on any failed operations

    Returns:
        The test id associated with the membist test run
    """
    if vddc_offset not in range(-50, 51): #range from -50 to 50
        raise SystemMaintenanceExecutionFailedError(f"vddc offset value {vddc_offset} is out of range (+/-50mV)")

    request = CliDiagnostics_pb2.HlMemBistStartRequest(
        noConfirm=True, # no confirm needed as the request comes from csctl
        vddcOffset=vddc_offset,
        skipFailedOps=skip_failed_ops,
    )
    logger.info(f"{system_name}: Starting membist with vddc offset at: {vddc_offset} mV")

    response = await system_grpc_client.hl_mem_bist_start(request=request)
    if response.testId is None:
        raise SystemMaintenanceExecutionFailedError("Starting membist failed, unable to receive a test id")
    return response.testId


async def poll_membist_status(
    system_name: str,
    system_grpc_client: SystemClient,
    test_id: str
):
    """Poll for HL Memory BIST status, with maximum timeout of 30 minutes

    Args:
        system_name: Name of the target system
        system_grpc_client: gRPC client to interact with the system
        test_id: The test id associated with the membist test run

    Throws:
        SystemMaintenanceExecutionFailedError when membist fails
    """

    max_backoff = 120
    cur_backoff = 2
    max_timeout = 1800  # 30 minutes
    start_time = time.time()

    try:
        while True:
            # Check if timeout has been reached
            elapsed_time = time.time() - start_time
            if elapsed_time > max_timeout:
                logger.error(f"Membist polling timed out after {max_timeout} seconds for test_id: {test_id}")
                raise SystemMaintenanceExecutionFailedError(f"Membist polling timed out for test_id: {test_id}")

            result = await system_grpc_client.hl_mem_bist_show(test_id)

            # One of: STARTED, PASSED or FAILED
            test_status = result.testStatus.status

            if test_status == 'PASSED':
                logger.info(f"Membist completed successfully for test_id: {test_id}")
                return
            elif test_status == 'FAILED':
                # TODO: better relaying of the error to user (jira: SW-185559)
                logger.error(f"Logging the full test result as membist failed: {result}")
                raise SystemMaintenanceExecutionFailedError(f"Membist failed for test_id: {test_id}, check log at: {result.testLogsDirPath}")
            elif test_status == 'STARTED':
                logger.info(f"Membist ongoing for test_id: {test_id}, polling again...")
            else:
                logger.error(f"Unknown status: {test_status} for test_id: {test_id}")
                raise SystemMaintenanceExecutionFailedError(f"Unknown test status: {test_status}, failing execution")

            # Exponential backoff logic
            await asyncio.sleep(cur_backoff)
            cur_backoff = min(cur_backoff * 2, max_backoff)  # Exponential backoff starting at 2 seconds, max 2 minutes
    except Exception as e:
        logger.error(f"Unexpected error while polling for membist execution status: {e}")
        raise e

def assert_system_health_status(status_summary: CliSystem_cs2_pb2.ShowResponse):
    if (
        status_summary.system_health is None
        or status_summary.system_health not in (status_cs2_pb2.Status.OK, status_cs2_pb2.Status.DEGRADED)
    ):
        raise SystemMaintenanceExecutionFailedError("System check failed due to system not in 'OK' or 'DEGRADED' status")

def assert_system_maintenance_status(status_summary: CliSystem_cs2_pb2.ShowResponse, expect_maintenance_enabled: bool):
    maintenance_mode_enabled = "no" if not status_summary.HasField("maintenance_mode") else status_summary.maintenance_mode.enabled
    if maintenance_mode_enabled.lower() == ("no" if expect_maintenance_enabled else "yes"):
        raise SystemMaintenanceExecutionFailedError(f"System check failed, expected maintenance mode enabled = { expect_maintenance_enabled }")

def assert_system_maintenance_purpose_if_enabled(status_summary: CliSystem_cs2_pb2.ShowResponse, *purposes):
    if (
        not status_summary.HasField("maintenance_mode")
        or status_summary.maintenance_mode.purpose is None
        or status_summary.maintenance_mode.enabled.lower() == "no"
    ):
        return

    # Check if all purposes are in the 'purpose' string
    if not all(s.lower() in status_summary.maintenance_mode.purpose.lower() for s in purposes):
        raise SystemMaintenanceExecutionFailedError(
            f"System check failed, maintenance mode purpose does not match: {status_summary.maintenance_mode.purpose}"
        )