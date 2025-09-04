from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest

from pb.cli.system import CliSystem_cs2_pb2, status_cs2_pb2
from pb.cli.diagnostics import CliDiagnostics_pb2
from pb.schema import TestResult_pb2
from system_maintenance_client import SystemClient

@pytest.mark.asyncio
async def test_system_show_success():
    mock_channel = MagicMock()
    mock_stub = MagicMock()

    maintenance_mode = status_cs2_pb2.MaintenanceMode(
        enabled="YES",
        purpose="System maintenance"
    )

    response_data = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy",
        maintenance_mode = maintenance_mode
    )

    async def mock_show(request, timeout):
        return response_data

    mock_stub.show = mock.AsyncMock(side_effect=mock_show)

    with mock.patch("pb.cli.system.CliSystem_cs2_pb2_grpc.SystemStub", return_value=mock_stub):
        client = SystemClient(channel=mock_channel, timeout_sec=5)
        response = await client.system_show(detailed=True)

        assert response.system_type == "TestSystem"
        assert response.system_health == 1
        assert response.system_health_desc == "Healthy"
        assert response.maintenance_mode.enabled == "YES"
        assert response.maintenance_mode.purpose == "System maintenance"

        mock_stub.show.assert_called_once_with(
            CliSystem_cs2_pb2.ShowRequest(detailed=True), timeout=5
        )

@pytest.mark.asyncio
async def test_vddc_set_success():
    mock_channel = MagicMock()
    mock_stub = MagicMock()

    response_data = CliDiagnostics_pb2.VddcShowResponse(
        testId="testId",
        vddcSetOffset="-50"
    )

    async def mock_vddc_set(request, timeout):
        return response_data

    mock_stub.vddcSet = AsyncMock(side_effect=mock_vddc_set)

    with mock.patch("pb.cli.diagnostics.CliDiagnostics_pb2_grpc.DiagnosticsStub", return_value=mock_stub):
        client = SystemClient(channel=mock_channel, timeout_sec=5)
        response = await client.vddc_set(vddc_offset=-50, no_confirm=True, skip_failed_ops=False)

        assert response.testId == "testId"
        assert response.vddcSetOffset == "-50"

        mock_stub.vddcSet.assert_called_once_with(
            CliDiagnostics_pb2.VddcSetRequest(
                noConfirm=True,
                vddcOffset=-50,
                skipFailedOps=False
            ),
            timeout=5
        )

@pytest.mark.asyncio
async def test_vddc_unset_success():
    mock_channel = MagicMock()
    mock_stub = MagicMock()

    response_data = CliDiagnostics_pb2.VddcShowResponse(
        testId="testId",
        vddcSetOffset="0"
    )

    async def mock_vddc_unset(request, timeout):
        return response_data

    mock_stub.vddcUnset = AsyncMock(side_effect=mock_vddc_unset)

    with mock.patch("pb.cli.diagnostics.CliDiagnostics_pb2_grpc.DiagnosticsStub", return_value=mock_stub):
        client = SystemClient(channel=mock_channel, timeout_sec=5)
        response = await client.vddc_unset(no_confirm=True, skip_set_check=False, skip_failed_ops=True)

        assert response.testId == "testId"
        assert response.vddcSetOffset == "0"

        mock_stub.vddcUnset.assert_called_once_with(
            CliDiagnostics_pb2.VddcUnsetRequest(
                noConfirm=True,
                skipSetCheck=False,
                skipFailedOps=True
            ),
            timeout=5
        )

@pytest.mark.asyncio
async def test_hl_mem_bist_start_success():
    mock_channel = MagicMock()
    mock_stub = MagicMock()

    request_data = CliDiagnostics_pb2.HlMemBistStartRequest(
        noConfirm=True,
        vddcOffset=10,
        skipFailedOps=True,
    )

    response_data = CliDiagnostics_pb2.HlMemBistStartResponse(
        testId="test_id"
    )

    async def mock_mem_bist_start(request, timeout):
        return response_data

    mock_stub.hlMemBistStart = AsyncMock(side_effect=mock_mem_bist_start)

    with mock.patch("pb.cli.diagnostics.CliDiagnostics_pb2_grpc.DiagnosticsStub", return_value=mock_stub):
        client = SystemClient(channel=mock_channel, timeout_sec=5)
        response = await client.hl_mem_bist_start(request=request_data)

        assert response.testId == "test_id"

        mock_stub.hlMemBistStart.assert_called_once_with(
            CliDiagnostics_pb2.HlMemBistStartRequest(
                noConfirm=True,
                vddcOffset=10,
                skipFailedOps=True,
            ),
            timeout=5
        )

@pytest.mark.asyncio
async def test_hl_mem_bist_show_success():
    mock_channel = MagicMock()
    mock_stub = MagicMock()

    response_data = CliDiagnostics_pb2.HlMemBistShowResponse(
        testId="test_id",
        testLogsDirPath="path_to_log",
        testStatus=TestResult_pb2.TestStatus(
            status = "PASSED"
        )
    )

    async def mock_mem_bist_show(request, timeout):
        return response_data

    mock_stub.hlMemBistShow = AsyncMock(side_effect=mock_mem_bist_show)

    with mock.patch("pb.cli.diagnostics.CliDiagnostics_pb2_grpc.DiagnosticsStub", return_value=mock_stub):
        client = SystemClient(channel=mock_channel, timeout_sec=5)
        response = await client.hl_mem_bist_show(test_id="test_id")

        assert response.testId == "test_id"
        assert response.testLogsDirPath == "path_to_log"
        assert response.testStatus.status == "PASSED"

        mock_stub.hlMemBistShow.assert_called_once_with(
            CliDiagnostics_pb2.HlMemBistShowRequest(
                testId="test_id"
            ),
            timeout=5
        )
