import pytest
import sys

from unittest.mock import patch, MagicMock, AsyncMock
from vddc import execute_vddc

from pb.cli.system import CliSystem_cs2_pb2, status_cs2_pb2
from pb.cli.diagnostics import CliDiagnostics_pb2
from pb.schema import TestResult_pb2
from system_maintenance_helper import SystemMaintenanceExecutionFailedError

@pytest.mark.asyncio
async def test_vddc_set_happy_path():
    test_args = ['vddc', 'localhost', 'localhost', 'set', '50', 'not_allow_error']

    show_response_with_maintenance = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy",
        maintenance_mode=status_cs2_pb2.MaintenanceMode(
            enabled="YES",
            purpose="vddc set"
        )
    )

    show_response_without_maintenance = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy"
    )

    vddc_set_response = CliDiagnostics_pb2.VddcShowResponse(
        testId="test id",
        vddcSetOffset="50",
        vddcSetStatus=TestResult_pb2.TestStatus(
            status="COMPLETED"
        ),
        vddcUnsetStatus=TestResult_pb2.TestStatus(
            status="PENDING"
        )
    )

    with patch.object(sys, 'argv', test_args), \
        patch('vddc.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.vddc_set = AsyncMock(return_value=vddc_set_response)

        mock_system_grpc_client.system_show = AsyncMock(
            side_effect=[show_response_without_maintenance, show_response_with_maintenance]
        )

        await execute_vddc()

        assert mock_system_grpc_client.system_show.call_count == 2, "system_show should be called twice"
        assert mock_system_grpc_client.vddc_set.call_count == 1, "vddc_set should be called exactly once"

        # Verify `vddc_set` was called with expected arguments
        mock_system_grpc_client.vddc_set.assert_called_once_with(vddc_offset=50)

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()

@pytest.mark.asyncio
async def test_allow_vddc_set_when_maintenance_enabled():
    test_args = ['vddc', 'localhost', 'localhost', 'set', '50', 'not_allow_error']

    show_response_with_maintenance = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy",
        maintenance_mode=status_cs2_pb2.MaintenanceMode(
            enabled="YES",
            purpose="vddc SET"
        )
    )

    vddc_set_response = CliDiagnostics_pb2.VddcShowResponse(
        testId="test id",
        vddcSetOffset="50",
        vddcSetStatus=TestResult_pb2.TestStatus(
            status="COMPLETED"
        ),
        vddcUnsetStatus=TestResult_pb2.TestStatus(
            status="PENDING"
        )
    )

    with patch.object(sys, 'argv', test_args), \
        patch('vddc.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.vddc_set = AsyncMock(return_value=vddc_set_response)

        mock_system_grpc_client.system_show = AsyncMock(
            side_effect=[show_response_with_maintenance, show_response_with_maintenance]
        )

        await execute_vddc()

        assert mock_system_grpc_client.system_show.call_count == 2, "system_show should be called twice"
        assert mock_system_grpc_client.vddc_set.call_count == 1, "vddc_set should be called exactly once"

        # Verify `vddc_set` was called with expected arguments
        mock_system_grpc_client.vddc_set.assert_called_once_with(vddc_offset=50)

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()

@pytest.mark.asyncio
async def test_vddc_unset_happy_path():
    test_args = ['vddc', 'localhost', 'localhost', 'unset', '0', 'not_allow_error']

    show_response_with_maintenance = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy",
        maintenance_mode=status_cs2_pb2.MaintenanceMode(
            enabled="YES",
            purpose="vddc set"
        )
    )

    show_response_without_maintenance = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy",
        maintenance_mode=status_cs2_pb2.MaintenanceMode(
            enabled="NO",
            purpose="VDDC_SET"
        )
    )

    vddc_unset_response = CliDiagnostics_pb2.VddcShowResponse(
        testId="test id",
        vddcSetOffset="0",
        vddcSetStatus=TestResult_pb2.TestStatus(
            status="PENDING"
        ),
        vddcUnsetStatus=TestResult_pb2.TestStatus(
            status="COMPLETED"
        )
    )

    with patch.object(sys, 'argv', test_args), \
        patch('vddc.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.vddc_unset = AsyncMock(return_value=vddc_unset_response)

        mock_system_grpc_client.system_show = AsyncMock(
            side_effect=[show_response_with_maintenance, show_response_without_maintenance]
        )

        await execute_vddc()

        assert mock_system_grpc_client.system_show.call_count == 2, "system_show should be called twice"
        assert mock_system_grpc_client.vddc_unset.call_count == 1, "vddc_unset should be called exactly once"

        # Verify `vddc_set` was called without arguments
        mock_system_grpc_client.vddc_unset.assert_called_once_with()

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()

@pytest.mark.asyncio
async def test_system_health_validation_fails():
    test_args = ['vddc', 'localhost', 'localhost', 'set', '30', 'not_allow_error']

    show_response_unhealthy = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=4,
        system_health_desc="Critical Failure"
    )

    with patch.object(sys, 'argv', test_args), \
        patch('vddc.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.system_show = AsyncMock(return_value=show_response_unhealthy)

        with pytest.raises(SystemMaintenanceExecutionFailedError, match="Execution failed: System check failed due to system not in 'OK' or 'DEGRADED' status"):
            await execute_vddc()

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()

@pytest.mark.asyncio
async def test_system_maintenance_purpose_validation_fails():
    test_args = ['vddc', 'localhost', 'localhost', 'set', '30', 'not_allow_error']

    show_response_with_maintenance = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy",
        maintenance_mode=status_cs2_pb2.MaintenanceMode(
            enabled="YES",
            purpose="some other maintenance types"
        )
    )

    with patch.object(sys, 'argv', test_args), \
        patch('vddc.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.system_show = AsyncMock(return_value=show_response_with_maintenance)

        with pytest.raises(SystemMaintenanceExecutionFailedError, match="Execution failed: System check failed, maintenance mode purpose does not match: some other maintenance types"):
            await execute_vddc()

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()

@pytest.mark.asyncio
async def test_vddc_offset_value_out_of_boundary():
    test_args = ['vddc', 'localhost', 'localhost', 'set', '-100', 'not_allow_error']

    show_response = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="HEALTHY"
    )

    with patch.object(sys, 'argv', test_args), \
        patch('vddc.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.system_show = AsyncMock(return_value=show_response)

        with pytest.raises(SystemMaintenanceExecutionFailedError, match=r"Execution failed: vddc offset value -100 is out of range (\(\+/-50mV\))"):
            await execute_vddc()

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_vddc_set_allow_error():
    test_args = ['vddc', 'localhost', 'localhost', 'set', '-50', 'allow_error']

    show_response_system_unhealthy = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=4,
        system_health_desc="Critical Failure"
    )

    vddc_set_response = CliDiagnostics_pb2.VddcShowResponse(
        testId="test id",
        vddcSetOffset="-50",
        vddcSetStatus=TestResult_pb2.TestStatus(
            status="COMPLETED"
        ),
        vddcUnsetStatus=TestResult_pb2.TestStatus(
            status="PENDING"
        )
    )

    with patch.object(sys, 'argv', test_args), \
        patch('vddc.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.system_show = AsyncMock(return_value=show_response_system_unhealthy)
        mock_system_grpc_client.vddc_set = AsyncMock(return_value=vddc_set_response)

        await execute_vddc()

        assert mock_system_grpc_client.vddc_set.call_count == 1, "vddc_set should be called exactly once"
        assert mock_system_grpc_client.system_show.call_count == 2, "system_show should be called twice"

        # Verify `vddc_set` was called with expected arguments
        mock_system_grpc_client.vddc_set.assert_called_once_with(vddc_offset=-50)

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()

@pytest.mark.asyncio
async def test_vddc_set_result_status_not_completed():
    test_args = ['vddc', 'localhost', 'localhost', 'set', '-30', 'allow_error']

    show_response_without_maintenance = CliSystem_cs2_pb2.ShowResponse(
        system_type="TestSystem",
        system_health=1,
        system_health_desc="Healthy"
    )

    vddc_set_response = CliDiagnostics_pb2.VddcShowResponse(
        testId="test id",
        vddcSetOffset="-50",
        vddcSetStatus=TestResult_pb2.TestStatus(
            status="PENDING"
        ),
        vddcUnsetStatus=TestResult_pb2.TestStatus(
            status="PENDING"
        )
    )

    with patch.object(sys, 'argv', test_args), \
        patch('vddc.TunneledSystemClient', autospec=True) as MockTunneledSystemClient:

        mock_tunnel_client = MockTunneledSystemClient.return_value
        mock_tunnel_client.connect = AsyncMock(return_value=None)
        mock_tunnel_client.close = AsyncMock(return_value=None)

        mock_system_grpc_client = MagicMock()
        mock_tunnel_client.system_client = mock_system_grpc_client

        mock_system_grpc_client.system_show = AsyncMock(return_value=show_response_without_maintenance)
        mock_system_grpc_client.vddc_set = AsyncMock(return_value=vddc_set_response)

        with pytest.raises(SystemMaintenanceExecutionFailedError, match="Execution failed: Setting of vddc voltage has failed"):
            await execute_vddc()

        assert mock_system_grpc_client.vddc_set.call_count == 1, "vddc_set should be called exactly once"
        assert mock_system_grpc_client.system_show.call_count == 1, "system_show should be called once"

        # Verify `vddc_set` was called with expected arguments
        mock_system_grpc_client.vddc_set.assert_called_once_with(vddc_offset=-30)

        # Verify tunnel_client connect/close
        mock_tunnel_client.connect.assert_called_once_with()
        mock_tunnel_client.close.assert_called_once_with()