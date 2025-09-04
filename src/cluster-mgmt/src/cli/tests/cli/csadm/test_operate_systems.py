from unittest.mock import AsyncMock, MagicMock, patch

import asyncssh
import pytest
from apps.csadm.csadm import csadm_operate_systems


@pytest.mark.asyncio
@patch("asyncssh.connect")
async def test_run_command_success(mock_connect):
    mock_conn = AsyncMock()
    mock_result = MagicMock()
    mock_result.exit_status = 0
    mock_result.stdout = "Some output"
    mock_conn.run.return_value = mock_result
    mock_connect.return_value.__aenter__.return_value = mock_conn

    result = await csadm_operate_systems._run_command("systemf123", "cmd", None)

    assert result.system_addr == "systemf123"
    assert result.output == "Some output"
    assert result.return_code == 0
    assert result.error is None


@pytest.mark.asyncio
@patch("asyncssh.connect")
async def test_run_command_failure(mock_connect):
    mock_connect.side_effect = asyncssh.Error(code=1, reason="Connection failed")

    result = await csadm_operate_systems._run_command("systemf123", "cmd", None)

    assert result.system_addr == "systemf123"
    assert result.output is None
    assert result.return_code == 1
    assert result.error == "Connection failed"


@patch("apps.csadm.csadm.csadm_operate_systems._run_command")
def test_run_parallel_commands(mock_run_command):
    async def mock_run_command_async(system_addr, command, cred):
        return csadm_operate_systems.RemoteExecuteResult(
            system_addr=system_addr, output=command, return_code=0, error=None
        )

    mock_run_command.side_effect = mock_run_command_async

    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"
    commands = ["cmd1", "cmd2"]

    results = csadm_operate_systems._run_parallel_commands(
        system_list, system_cred, commands
    )

    assert len(results) == 2
    for result in results:
        assert result.system_addr in system_list
        if result.system_addr == "systemf123":
            assert result.output == "cmd1"
        elif result.system_addr == "systemf456":
            assert result.output == "cmd2"
        assert result.return_code == 0
        assert result.error is None


@patch("apps.csadm.csadm.csadm_operate_systems._run_parallel_commands")
def test_system_system_show_handler(mock_run_parallel_commands):
    mock_run_parallel_commands.return_value = [
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf123",
            output='{"systemHealth": "OK"}',
            return_code=0,
            error=None,
        ),
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf456",
            output='{"systemHealth": "Degraded"}',
            return_code=0,
            error=None,
        ),
    ]

    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"
    result = csadm_operate_systems.system_system_show_handler(
        system_list, system_cred, []
    )

    assert result.success is True
    assert result.payload == {"systemf123": "OK", "systemf456": "Degraded"}
    assert result.message == "2 out of 2 systems were successful"
    assert result.error == ""


@patch("apps.csadm.csadm.csadm_operate_systems._run_parallel_commands")
def test_system_system_show_handler_failure(mock_run_parallel_commands):
    mock_run_parallel_commands.return_value = [
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf123",
            output='{"systemHealth": "OK"}',
            return_code=0,
            error=None,
        ),
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf456",
            output=None,
            return_code=1,
            error="Failed to connect",
        ),
    ]

    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"
    result = csadm_operate_systems.system_system_show_handler(
        system_list, system_cred, []
    )

    assert result.success is False
    assert result.payload == {
        "systemf123": "OK",
        "systemf456": "Error: Failed to get system status",
    }
    assert result.message == "1 out of 2 systems were successful"
    assert result.error == "Failed to get system status from 1 systems"


@patch("apps.csadm.csadm.csadm_operate_systems._run_parallel_commands")
def test_system_software_image_show_handler(mock_run_parallel_commands):
    mock_run_parallel_commands.return_value = [
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf123",
            output='{"softwareImages": [{"imageName": "image1", "imageType": "WSE_DATA"}]}',
            return_code=0,
            error=None,
        ),
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf456",
            output='{"softwareImages": [{"imageName": "image2", "imageType": "WSE_DATA"}]}',
            return_code=0,
            error=None,
        ),
    ]

    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"
    result = csadm_operate_systems.system_software_image_show_handler(
        system_list, system_cred, []
    )

    assert result.success is True
    assert result.payload == {"systemf123": ["image1"], "systemf456": ["image2"]}
    assert result.message == "2 out of 2 systems were successful"
    assert result.error == ""


@patch("apps.csadm.csadm.csadm_operate_systems._run_parallel_commands")
def test_system_software_image_show_handler_failure(mock_run_parallel_commands):
    mock_run_parallel_commands.return_value = [
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf123",
            output='{"softwareImages": [{"imageName": "image1", "imageType": "WSE_DATA"}]}',
            return_code=0,
            error=None,
        ),
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf456",
            output="",
            return_code=1,
            error="Failed to get image info",
        ),
    ]

    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"
    result = csadm_operate_systems.system_software_image_show_handler(
        system_list, system_cred, []
    )

    assert result.success is False
    assert result.payload == {"systemf123": ["image1"]}
    assert result.message == "1 out of 2 systems were successful"
    assert result.error == "Failed to get WSE_DATA image from 1 systems"


@patch("apps.csadm.csadm.csadm_operate_systems._check_event_issue_time")
@patch("apps.csadm.csadm.csadm_operate_systems._run_parallel_commands")
def test_system_standby_handler(
    mock_run_parallel_commands, mock_check_event_issue_time
):
    mock_run_parallel_commands.return_value = [
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf123",
            output="",
            return_code=0,
            error=None,
        ),
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf456",
            output="",
            return_code=0,
            error=None,
        ),
    ]

    mock_check_event_issue_time.return_value = {
        "systemf123": "2024-09-26T12:00:00",
        "systemf456": "2024-09-26T12:05:00",
    }

    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"
    result = csadm_operate_systems.system_standby_handler(system_list, system_cred, [])

    assert result.success is True
    assert result.payload == {
        "success_systems": ["systemf123", "systemf456"],
        "success_systems_time": ["2024-09-26T12:00:00", "2024-09-26T12:05:00"],
        "failed_systems": [],
    }
    assert result.message == "2 out of 2 systems were successful"
    assert result.error == ""


@patch("apps.csadm.csadm.csadm_operate_systems._check_event_issue_time")
@patch("apps.csadm.csadm.csadm_operate_systems._run_parallel_commands")
def test_system_activate_handler(
    mock_run_parallel_commands, mock_check_event_issue_time
):
    mock_run_parallel_commands.return_value = [
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf123",
            output="",
            return_code=0,
            error=None,
        ),
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf456",
            output="",
            return_code=0,
            error=None,
        ),
    ]

    mock_check_event_issue_time.return_value = {
        "systemf123": "2024-09-26T12:00:00",
        "systemf456": "2024-09-26T12:05:00",
    }

    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"
    result = csadm_operate_systems.system_activate_handler(system_list, system_cred, [])

    assert result.success is True
    assert result.payload == {
        "success_systems": ["systemf123", "systemf456"],
        "success_systems_time": ["2024-09-26T12:00:00", "2024-09-26T12:05:00"],
        "failed_systems": [],
    }
    assert result.message == "2 out of 2 systems were successful"
    assert result.error == ""


@patch("apps.csadm.csadm.csadm_operate_systems._run_parallel_commands")
def test_system_check_for_event_handler(mock_run_parallel_commands):
    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"
    args = ["EventFilter", "2024-09-26T12:00:00,2024-09-26T12:05:00"]

    mock_run_parallel_commands.return_value = [
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf123",
            output='{"events": [{"type": "EventTypeCompleted"}]}',
            return_code=0,
            error=None,
        ),
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf456", output='{"events": []}', return_code=0, error=None
        ),
    ]

    result = csadm_operate_systems.system_check_for_event_handler(
        system_list, system_cred, args
    )

    assert result.success is False
    assert result.payload == {
        "finished_systems": {"systemf123": "EventTypeCompleted"},
        "failed_to_check_systems": {},
        "pending_systems": ["systemf456"],
        "pending_systems_time": ["2024-09-26T12:05:00"],
    }
    assert result.message == "1 out of 2 systems has finished"
    assert result.error == ""


@patch("apps.csadm.csadm.csadm_operate_systems._get_system_wse_config_info")
def test_system_wse_config_show_handler(mock_get_system_wse_config_info):
    mock_get_system_wse_config_info.return_value = (
        {"systemf123": "configA", "systemf456": "configB"},
        {"systemf123": ["configA", "configC"], "systemf456": ["configB", "configD"]},
    )

    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"

    result = csadm_operate_systems.system_wse_config_show_handler(
        system_list, system_cred, []
    )

    assert result.success is True
    assert result.payload == {
        "systemf123": {
            "active_config": "configA",
            "available_configs": ["configA", "configC"],
        },
        "systemf456": {
            "active_config": "configB",
            "available_configs": ["configB", "configD"],
        },
    }
    assert result.message == "2 out of 2 systems were successful"
    assert result.error == ""


@patch("apps.csadm.csadm.csadm_operate_systems._get_system_wse_config_info")
def test_system_wse_config_set_handler(mock_get_system_wse_config_info):
    mock_get_system_wse_config_info.return_value = (
        {},
        {"systemf123": ["configA", "configB"], "systemf456": ["configA", "configC"]},
    )

    mock_run_parallel_commands = patch(
        "apps.csadm.csadm.csadm_operate_systems._run_parallel_commands"
    ).start()
    mock_run_parallel_commands.return_value = [
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf123",
            output="WSE config set to configA",
            return_code=0,
            error=None,
        ),
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf456",
            output="WSE config set to configA",
            return_code=0,
            error=None,
        ),
    ]

    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"
    args = ["configA"]

    result = csadm_operate_systems.system_wse_config_set_handler(
        system_list, system_cred, args
    )

    assert result.success is True
    assert result.payload == {
        "systemf123": {
            "returnCode": 0,
            "output": "WSE config set to configA",
            "error": None,
        },
        "systemf456": {
            "returnCode": 0,
            "output": "WSE config set to configA",
            "error": None,
        },
    }
    assert result.message == "2 out of 2 systems were successful"
    assert result.error == ""

    mock_run_parallel_commands.stop()


@patch("apps.csadm.csadm.csadm_operate_systems._show_system_software_image")
def test_system_wse_data_update_handler(mock_show_system_software_image):
    mock_show_system_software_image.return_value = {
        "systemf123": ["image1", "image2"],
        "systemf456": ["image3", "image4"],
    }

    mock_run_parallel_commands = patch(
        "apps.csadm.csadm.csadm_operate_systems._run_parallel_commands"
    ).start()
    mock_run_parallel_commands.return_value = [
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf123",
            output="WSE_DATA updated to image1",
            return_code=0,
            error=None,
        ),
        csadm_operate_systems.RemoteExecuteResult(
            system_addr="systemf456",
            output="WSE_DATA updated to image3",
            return_code=0,
            error=None,
        ),
    ]

    system_list = ["systemf123", "systemf456"]
    system_cred = "user:password"
    args = ["image1,image3"]

    result = csadm_operate_systems.system_wse_data_update_handler(
        system_list, system_cred, args
    )

    assert result.success is True
    assert result.payload == {
        "systemf123": {
            "returnCode": 0,
            "output": "WSE_DATA updated to image1",
            "error": None,
        },
        "systemf456": {
            "returnCode": 0,
            "output": "WSE_DATA updated to image3",
            "error": None,
        },
    }
    assert result.message == "2 out of 2 systems were successful"
    assert result.error == "0 systems failed to update WSE_DATA image"

    mock_run_parallel_commands.stop()
