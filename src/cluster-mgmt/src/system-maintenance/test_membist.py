import json
import pytest
import sys

from unittest.mock import patch, MagicMock, AsyncMock, call
from membist import execute_membist

from pb.cli.system import CliSystem_cs2_pb2, status_cs2_pb2
from pb.cli.diagnostics import CliDiagnostics_pb2
from pb.schema import TestResult_pb2
from system_maintenance_helper import SystemMaintenanceExecutionFailedError

@pytest.mark.asyncio
async def test_membist_execute_happy_path():
    test_args = ['membist', 'localhost', 'localhost', 'not_skip_failed_ops', '50', 'not_allow_error']

    show_response_without_maintenance = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy"
    )

    membist_start_response = CliDiagnostics_pb2.HlMemBistStartResponse(
        testId = "testId"
    )

    membist_show_response_in_progress = CliDiagnostics_pb2.HlMemBistShowResponse(
        testId="testId",
        testStatus=TestResult_pb2.TestStatus(
            status="STARTED"
        )
    )

    membist_show_response_completed = CliDiagnostics_pb2.HlMemBistShowResponse(
        testId="testId",
        testStatus=TestResult_pb2.TestStatus(
            status="PASSED"
        )
    )

    with patch.object(sys, 'argv', test_args), \
        patch('membist.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.hl_mem_bist_start = AsyncMock(return_value=membist_start_response)

        mock_system_grpc_client.hl_mem_bist_show = AsyncMock(
            side_effect=[membist_show_response_in_progress, membist_show_response_completed]
        )

        mock_system_grpc_client.system_show = AsyncMock(
            side_effect=[show_response_without_maintenance, show_response_without_maintenance]
        )

        await execute_membist()

        assert mock_system_grpc_client.system_show.call_count == 2, "system_show should be called twice"
        assert mock_system_grpc_client.hl_mem_bist_start.call_count == 1, "hl_mem_bist_start should be called exactly once"
        assert mock_system_grpc_client.hl_mem_bist_show.call_count == 2, "hl_mem_bist_show should be called twice"

        # Verify `hl_mem_bist_start` was called with expected arguments
        mock_system_grpc_client.hl_mem_bist_start.assert_called_once_with(request=CliDiagnostics_pb2.HlMemBistStartRequest(
            noConfirm=True,
            vddcOffset=50,
            skipFailedOps=False
        ))
        mock_system_grpc_client.assert_has_calls([
            call.hl_mem_bist_show("testId"),
            call.hl_mem_bist_show("testId")
        ])

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()

@pytest.mark.asyncio
async def test_membist_execute_failure():
    test_args = ['membist', 'localhost', 'localhost', 'not_skip_failed_ops', '-50', 'not_allow_error']

    show_response_without_maintenance = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy"
    )

    membist_start_response = CliDiagnostics_pb2.HlMemBistStartResponse(
        testId = "testId"
    )

    membist_show_response_failed = CliDiagnostics_pb2.HlMemBistShowResponse(
        testId="testId",
        testStatus=TestResult_pb2.TestStatus(
            status="FAILED"
        ),
        testLogsDirPath="test-log-dir"
    )

    with patch.object(sys, 'argv', test_args), \
        patch('membist.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.hl_mem_bist_start = AsyncMock(return_value=membist_start_response)

        mock_system_grpc_client.hl_mem_bist_show = AsyncMock(
            side_effect=[membist_show_response_failed]
        )

        mock_system_grpc_client.system_show = AsyncMock(
            side_effect=[show_response_without_maintenance]
        )

        with pytest.raises(SystemMaintenanceExecutionFailedError, match="Execution failed: Membist failed for test_id: testId, check log at: test-log-dir"):
            await execute_membist()

        assert mock_system_grpc_client.system_show.call_count == 1, "system_show should be called excatly once"
        assert mock_system_grpc_client.hl_mem_bist_start.call_count == 1, "hl_mem_bist_start should be called exactly once"
        assert mock_system_grpc_client.hl_mem_bist_show.call_count == 1, "hl_mem_bist_show should be called exactly once"

        # Verify `hl_mem_bist_start` was called with expected arguments
        mock_system_grpc_client.hl_mem_bist_start.assert_called_once_with(request=CliDiagnostics_pb2.HlMemBistStartRequest(
            noConfirm=True,
            vddcOffset=-50,
            skipFailedOps=False
        ))
        mock_system_grpc_client.hl_mem_bist_show.assert_called_once_with("testId")

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()

@pytest.mark.asyncio
async def test_membist_execute_unknown_status():
    test_args = ['membist', 'localhost', 'localhost', 'not_skip_failed_ops', '-50', 'not_allow_error']

    show_response_without_maintenance = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy"
    )

    membist_start_response = CliDiagnostics_pb2.HlMemBistStartResponse(
        testId = "testId"
    )

    membist_show_response_failed = CliDiagnostics_pb2.HlMemBistShowResponse(
        testId="testId",
        testStatus=TestResult_pb2.TestStatus(
            status="UNKNOWN"
        ),
        testLogsDirPath="test-log-dir"
    )

    with patch.object(sys, 'argv', test_args), \
        patch('membist.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.hl_mem_bist_start = AsyncMock(return_value=membist_start_response)

        mock_system_grpc_client.hl_mem_bist_show = AsyncMock(
            side_effect=[membist_show_response_failed]
        )

        mock_system_grpc_client.system_show = AsyncMock(
            side_effect=[show_response_without_maintenance]
        )

        with pytest.raises(SystemMaintenanceExecutionFailedError, match="Execution failed: Unknown test status: UNKNOWN, failing execution"):
            await execute_membist()

        assert mock_system_grpc_client.system_show.call_count == 1, "system_show should be called excatly once"
        assert mock_system_grpc_client.hl_mem_bist_start.call_count == 1, "hl_mem_bist_start should be called exactly once"
        assert mock_system_grpc_client.hl_mem_bist_show.call_count == 1, "hl_mem_bist_show should be called exactly once"

        # Verify `hl_mem_bist_start` was called with expected arguments
        mock_system_grpc_client.hl_mem_bist_start.assert_called_once_with(request=CliDiagnostics_pb2.HlMemBistStartRequest(
            noConfirm=True,
            vddcOffset=-50,
            skipFailedOps=False
        ))
        mock_system_grpc_client.hl_mem_bist_show.assert_called_once_with("testId")

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()