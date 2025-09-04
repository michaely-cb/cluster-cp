import argparse
import asyncio
import collections
import datetime
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import asyncssh

REMOTE_EXECUTION_TIMEOUT_SEC = 60


class CommandExecutionResult:
    def __init__(
        self, success: bool, message: str, payload: Dict[str, Any], error: str
    ):
        self.success = success
        self.message = message
        self.payload = payload
        self.error = error

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "message": self.message,
            "payload": self.payload,
            "error": self.error,
        }


class RemoteExecuteResult:
    def __init__(
        self,
        system_addr: str,
        output: Optional[str] = None,
        return_code: Optional[int] = None,
        error: Optional[str] = None,
    ):
        self.system_addr = system_addr
        self.output = output
        self.return_code = return_code
        self.error = error


async def _run_command(
    system_addr: str, command: str, cred: Optional[str]
) -> RemoteExecuteResult:
    result = RemoteExecuteResult(system_addr)
    kwargs = {}

    if cred:
        try:
            user, password = cred.split(":")
            if user and password:
                kwargs["username"] = user
                kwargs["password"] = password
        except ValueError:
            result.error = "Invalid credentials format"
            result.return_code = 1
            return result

    try:
        async with asyncssh.connect(system_addr, **kwargs) as conn:
            ssh_result = await conn.run(command)
            result.return_code = ssh_result.exit_status
            result.output = (
                str(ssh_result.stdout).strip() if ssh_result.stdout else None
            )
    except (OSError, asyncssh.Error) as exc:
        result.error = str(exc)
        if hasattr(exc, "stderr") and exc.stderr:
            result.error += f" | Stderr: {exc.stderr.strip()}"
        result.return_code = 1
    return result


async def _run_commands_on_systems(
    system_list: List[str],
    system_cred: Optional[str],
    commands: List[str],
    timeout_sec: Optional[int] = REMOTE_EXECUTION_TIMEOUT_SEC,
) -> List[RemoteExecuteResult]:
    if len(system_list) != len(commands):
        raise ValueError("system_list and commands must have the same length")

    tasks = [
        asyncio.wait_for(
            _run_command(system, command, system_cred), timeout=timeout_sec
        )
        for system, command in zip(system_list, commands)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    final_results: List[RemoteExecuteResult] = []
    for system, result in zip(system_list, results):
        if isinstance(result, Exception):
            final_results.append(
                RemoteExecuteResult(
                    system_addr=system, error=str(result), return_code=1
                )
            )
        else:
            assert isinstance(
                result, RemoteExecuteResult
            ), f"Unexpected result type: {type(result)}"
            final_results.append(result)
    return final_results


def _run_parallel_commands(
    system_list: List[str],
    system_cred: Optional[str],
    commands: List[str],
    timeout_sec: Optional[int] = None,
) -> List[RemoteExecuteResult]:
    try:
        results = asyncio.run(
            _run_commands_on_systems(system_list, system_cred, commands, timeout_sec)
        )
    except Exception as e:
        results = [
            RemoteExecuteResult(system_addr=system, error=str(e), return_code=1)
            for system in system_list
        ]
    return results


def _show_system_software_image(
    system_list: List[str], system_cred: Optional[str]
) -> Dict[str, List[str]]:
    list_command = (
        "entersupportmode --noConfirm; software image show --output-format json"
    )
    commands = [list_command] * len(system_list)
    results = _run_parallel_commands(system_list, system_cred, commands)

    system_images = collections.defaultdict(list)
    for result in results:
        if result.return_code != 0:
            logging.debug(
                f"Failed to get image info from {result.system_addr}: "
                f"{result.error}\n"
                f"Output: {result.output}"
            )
            continue
        if not result.output:
            logging.debug(f"No output from system {result.system_addr}")
            continue
        try:
            data = json.loads(result.output)
            wse_data_images = [
                image["imageName"]
                for image in data["softwareImages"]
                if image["imageType"] == "WSE_DATA"
            ]
            system_images[result.system_addr] = wse_data_images
        except json.JSONDecodeError as e:
            logging.debug(f"Error decoding JSON from system {result.system_addr}: {e}")
            system_images[result.system_addr] = []
    return system_images


def system_software_image_show_handler(
    system_list: List[str], system_cred: Optional[str], unused_args: List[str]
) -> CommandExecutionResult:
    system_images = _show_system_software_image(system_list, system_cred)
    success_count = 0
    for system, images in system_images.items():
        if len(images) > 0:
            success_count += 1

    success = success_count == len(system_list)
    error = ""
    if not success:
        error = (
            "Failed to get WSE_DATA image from "
            f"{len(system_list) - success_count} systems"
        )

    return CommandExecutionResult(
        success=success,
        message=f"{success_count} out of {len(system_list)} systems were successful",
        payload=system_images,
        error=error,
    )


def system_check_for_event_handler(
    system_list: List[str], system_cred: Optional[str], args: List[str]
) -> CommandExecutionResult:
    if len(args) != 2:
        return CommandExecutionResult(
            success=False,
            message="Invalid arguments, expect event filter and wait args",
            payload={},
            error="Invalid arguments",
        )

    event_filter = args[0]
    wait_args = args[1].split(",")
    system_time = dict(zip(system_list, wait_args))

    payload: Dict[str, Any] = {
        "finished_systems": {},
        "failed_to_check_systems": {},
        "pending_systems": [],
        "pending_systems_time": [],
    }

    commands = []
    for system in system_list:
        events_command = (
            f"events show --output-format json --event-type {event_filter} "
            f"--from-time {system_time[system]}"
        )
        commands.append(events_command)

    results = _run_parallel_commands(system_list, system_cred, commands)

    for result in results:
        if result.return_code != 0:
            payload["failed_to_check_systems"][result.system_addr] = (
                results[0].output or results[0].error
            )
            continue
        if not result or not result.output:
            payload["failed_to_check_systems"][
                result.system_addr
            ] = "No output from system"
            continue
        try:
            data = json.loads(result.output)
            if data["events"]:
                for evt in data["events"]:
                    type_str = evt["type"]
                    if type_str.endswith(("Completed", "Failed", "Timedout")):
                        payload["finished_systems"][result.system_addr] = type_str
                        break
        except json.JSONDecodeError as e:
            logging.debug(f"Error decoding JSON from system {result.system_addr}: {e}")
            payload["failed_to_check_systems"][result.system_addr] = f"Error: {e}"

    for system in system_list:
        if (
            system not in payload["finished_systems"]
            and system not in payload["failed_to_check_systems"]
        ):
            payload["pending_systems"].append(system)
            payload["pending_systems_time"].append(system_time[system])

    success_count = len(payload["finished_systems"])
    return CommandExecutionResult(
        success=success_count == len(system_list),
        message=f"{success_count} out of {len(system_list)} systems has finished",
        payload=payload,
        error="",
    )


def _check_event_issue_time(
    system_list: List[str], system_cred: Optional[str], event_filter: str
) -> Dict[str, str]:
    command = (
        f"events show --max-count 1 --event-type {event_filter} --output-format json"
    )
    commands = [command] * len(system_list)
    results = _run_parallel_commands(system_list, system_cred, commands)
    system_time = {
        system: datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
        .replace(tzinfo=None)
        .isoformat()
        for system in system_list
    }

    for result in results:
        if result.return_code != 0:
            logging.debug(
                f"Failed to get time from {result.system_addr}: {result.error}"
            )
            continue
        if not result.output:
            logging.debug(f"No output from system {result.system_addr}")
            continue
        try:
            data = json.loads(result.output)
            ts = int(data["events"][0]["triggerTime"]) // 1000
            system_time[result.system_addr] = (
                datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
                .replace(tzinfo=None)
                .isoformat()
            )
        except json.JSONDecodeError as e:
            logging.debug(f"Error decoding JSON from system {result.system_addr}: {e}")
        except (KeyError, IndexError) as e:
            logging.debug(
                f"Error parsing event data from system {result.system_addr}: {e}"
            )
        except Exception as e:
            logging.debug(f"Error: {e}")

    return system_time


def system_standby_handler(
    system_list: List[str], system_cred: Optional[str], unused_args: List[str]
) -> CommandExecutionResult:
    standby_command = "system standby --no-confirm --output-format json"
    commands = [standby_command] * len(system_list)
    results = _run_parallel_commands(system_list, system_cred, commands)
    payload: Dict[str, Any] = {
        "success_systems": [],
        "success_systems_time": [],
        "failed_systems": [],
    }

    error = ""
    for result in results:
        if result.return_code == 0:
            payload["success_systems"].append(result.system_addr)
        else:
            payload["failed_systems"].append(result.system_addr)
            error += f"{result.system_addr}: {result.output or result.error}\n"

    system_time = _check_event_issue_time(
        payload["success_systems"], system_cred, "EventStandbyIssued"
    )

    for system in payload["success_systems"]:
        payload["success_systems_time"].append(system_time[system])

    failed_count = len(payload["failed_systems"])
    return CommandExecutionResult(
        success=failed_count == 0,
        message=f"{len(system_list) - failed_count} out of "
        f"{len(system_list)} systems were successful",
        payload=payload,
        error=error,
    )


def system_activate_handler(
    system_list: List[str], system_cred: Optional[str], unused_args: List[str]
) -> CommandExecutionResult:
    activate_command = "system activate --no-confirm --output-format json"
    commands = [activate_command] * len(system_list)
    results = _run_parallel_commands(system_list, system_cred, commands)
    payload: Dict[str, Any] = {
        "success_systems": [],
        "success_systems_time": [],
        "failed_systems": [],
    }

    error = ""
    for result in results:
        if result.return_code == 0:
            payload["success_systems"].append(result.system_addr)
        else:
            payload["failed_systems"].append(result.system_addr)
            error += f"{result.system_addr}: {result.output or result.error}\n"

    system_time = _check_event_issue_time(
        payload["success_systems"], system_cred, "EventActivateIssued"
    )
    for system in payload["success_systems"]:
        payload["success_systems_time"].append(system_time[system])

    failed_count = len(payload["failed_systems"])
    return CommandExecutionResult(
        success=failed_count == 0,
        message=f"{len(system_list) - failed_count} out of "
        f"{len(system_list)} systems were successful",
        payload=payload,
        error=error,
    )


def system_system_show_handler(
    system_list: List[str], system_cred: Optional[str], unused_args: List[str]
) -> CommandExecutionResult:
    show_command = "system show --output-format json"
    commands = [show_command] * len(system_list)
    results = _run_parallel_commands(system_list, system_cred, commands)

    system_status = {}
    success_count = 0
    for result in results:
        if result.return_code != 0:
            system_status[result.system_addr] = "Error: Failed to get system status"
            continue
        if not result.output:
            system_status[result.system_addr] = "Error: No output from system"
            continue
        try:
            data = json.loads(result.output)
            system_status[result.system_addr] = data["systemHealth"]
            success_count += 1
        except json.JSONDecodeError as e:
            system_status[result.system_addr] = f"Error: {e}"

    success = success_count == len(system_list)
    error = ""
    if not success:
        error = (
            "Failed to get system status from "
            f"{len(system_list) - success_count} systems"
        )
    return CommandExecutionResult(
        success=success,
        message=f"{success_count} out of {len(system_list)} systems were successful",
        payload=system_status,
        error=error,
    )


def _get_system_wse_config_info(
    system_list: List[str], system_cred: Optional[str]
) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    wse_config_show_command = (
        "entersupportmode --noConfirm; wse config show --output-format json"
    )
    commands = [wse_config_show_command] * len(system_list)
    results = _run_parallel_commands(system_list, system_cred, commands)
    system_active_configs = {}
    system_configs = {}
    for result in results:
        if result.return_code != 0:
            logging.debug(
                f"Failed to get WSE config from {result.system_addr}: {result.error}"
            )
            continue
        if not result.output:
            logging.debug(f"No output from system {result.system_addr}")
            continue
        try:
            data = json.loads(result.output)
            system_active_configs[result.system_addr] = data["activeConfig"]
            system_configs[result.system_addr] = data["availConfigs"]
        except json.JSONDecodeError as e:
            logging.debug(f"Error decoding JSON from system {result.system_addr}: {e}")
    return system_active_configs, system_configs


def system_wse_config_show_handler(
    system_list: List[str], system_cred: Optional[str], unused_args: List[str]
) -> CommandExecutionResult:
    system_active_configs, system_configs = _get_system_wse_config_info(
        system_list, system_cred
    )

    success = len(system_active_configs) == len(system_list)
    error = ""
    if not success:
        error = (
            "Failed to get WSE config from "
            f"{len(system_list) - len(system_active_configs)} systems"
        )

    payload = {}
    for system in system_list:
        payload[system] = {
            "active_config": system_active_configs.get(system, "N/A"),
            "available_configs": system_configs.get(system, []),
        }

    return CommandExecutionResult(
        success=success,
        message=f"{len(system_active_configs)} out of {len(system_list)} systems were "
        "successful",
        payload=payload,
        error=error,
    )


def system_wse_config_set_handler(
    system_list: List[str], system_cred: Optional[str], args: List[str]
) -> CommandExecutionResult:
    if not args:
        return CommandExecutionResult(
            success=False,
            message="WSE config not specified",
            payload={},
            error="WSE config not specified",
        )

    wse_config = args[0]
    _, system_configs = _get_system_wse_config_info(system_list, system_cred)

    applicable_system_count = 0
    for system_addr, configs in system_configs.items():
        if wse_config in configs:
            applicable_system_count += 1

    if applicable_system_count != len(system_list):
        return CommandExecutionResult(
            success=False,
            message="Config set was skipped on all systems.",
            payload={},
            error=f"{len(system_list) - applicable_system_count} systems do not have "
            "the specified WSE config",
        )

    wse_set_command = f"entersupportmode --noConfirm; wse config set --config-name {wse_config} --no-confirm"
    commands = [wse_set_command] * len(system_list)
    results = _run_parallel_commands(system_list, system_cred, commands)
    system_result = {}
    for result in results:
        system_result[result.system_addr] = {
            "returnCode": result.return_code,
            "output": result.output,
            "error": result.error,
        }

    fail_count = sum(1 for result in results if result.return_code != 0)
    success = fail_count == 0 and len(results) == len(system_list)
    error = ""
    if not success:
        error = f"{fail_count} systems failed to set WSE config"

    return CommandExecutionResult(
        success=success,
        message=f"{len(system_list) - fail_count} out of {len(system_list)} systems were successful",
        payload=system_result,
        error=error,
    )


def system_wse_data_update_handler(
    system_list: List[str], system_cred: Optional[str], args: List[str]
) -> CommandExecutionResult:
    if len(args) != 1:
        return CommandExecutionResult(
            success=False,
            message="Invalid arguments, expect target images",
            payload={},
            error="Invalid arguments",
        )
    target_images = args[0].split(",")
    if len(system_list) != len(target_images):
        return CommandExecutionResult(
            success=False,
            message="Number of systems and images do not match",
            payload={},
            error="Number of systems and images do not match",
        )

    images = _show_system_software_image(system_list, system_cred)

    missing_wse_systems = []
    target_image_dict = dict(zip(system_list, target_images))
    for system, image in target_image_dict.items():
        if image not in images[system]:
            missing_wse_systems.append(system)
            continue

    if missing_wse_systems:
        failed_systems = ", ".join(missing_wse_systems)
        return CommandExecutionResult(
            success=False,
            message="WSE_DATA image not found on all systems",
            payload={},
            error=f"{failed_systems} do not have the specified WSE_DATA image",
        )

    # Update WSE_DATA image on all systems
    fail_count = 0
    payload = {}
    commands = []
    for _, image in target_image_dict.items():
        update_command = f"entersupportmode --noConfirm; wse data update --name {image}"
        commands.append(update_command)

    results = _run_parallel_commands(system_list, system_cred, commands)

    for result in results:
        payload[result.system_addr] = {
            "returnCode": result.return_code,
            "output": result.output,
            "error": result.error,
        }
        if result.return_code != 0:
            fail_count += 1
    return CommandExecutionResult(
        success=fail_count == 0,
        message=f"{len(system_list) - fail_count} out of {len(system_list)} systems were successful",
        payload=payload,
        error=f"{fail_count} systems failed to update WSE_DATA image",
    )


def system_sua_handler(
    system_list: List[str], system_cred: Optional[str], args: List[str]
) -> CommandExecutionResult:
    if not args:
        return CommandExecutionResult(
            success=False,
            message="WSE image not provided",
            payload={},
            error="WSE image not provided",
        )
    image_list = args[0].split(",")
    if len(system_list) != len(image_list):
        return CommandExecutionResult(
            success=False,
            message="Number of systems and images do not match",
            payload={},
            error="Number of systems and images do not match",
        )

    images = _show_system_software_image(system_list, system_cred)
    missing_wse_systems = []
    image_dict = dict(zip(system_list, image_list))
    for system, image in image_dict.items():
        if image not in images[system]:
            missing_wse_systems.append(system)
            continue

    if missing_wse_systems:
        failed_systems = ", ".join(missing_wse_systems)
        return CommandExecutionResult(
            success=False,
            message="WSE_DATA image not found on all systems",
            payload={},
            error=f"{failed_systems} do not have the specified WSE_DATA image",
        )

    # Standby
    result = system_standby_handler(system_list, system_cred, [])
    if not result.success:
        return result

    wait_systems = result.payload["success_systems"]
    wait_system_time = result.payload["success_systems_time"]
    # Wait for event
    result = _wait_for_event(
        system_cred, wait_system_time, wait_systems, "EventStandby*")
    if not result.success:
        return result

    # Update uscd image
    result = system_wse_data_update_handler(system_list, system_cred, args)
    if not result.success:
        return result

    # Activate
    result = system_activate_handler(wait_systems, system_cred, [])
    if not result.success:
        return result

    wait_systems = result.payload["success_systems"]
    wait_system_time = result.payload["success_systems_time"]
    # Wait for event
    result = _wait_for_event(
        system_cred, wait_system_time, wait_systems, "EventActivate*")
    if not result.success:
        return result

    # Activate complete
    return CommandExecutionResult(
        success=True,
        message="Standby, Update and Activate completed successfully",
        payload={},
        error="",
    )


def _wait_for_event(
        system_cred: Optional[str], wait_system_time: List[str],
        wait_systems: List[str], event_filter: str) -> CommandExecutionResult:
    start_time = time.time()
    wait_duration = 10 * 60  # 10 minutes
    pending_systems = wait_systems
    while time.time() - start_time < wait_duration:
        result = system_check_for_event_handler(
            wait_systems, system_cred, [event_filter, ",".join(wait_system_time)])
        if result.success:
            return result
        time.sleep(10)
        pending_systems = result.payload["pending_systems"]
    return CommandExecutionResult(
        success=False,
        message=f"Timed out waiting for event {event_filter}",
        payload={"pending_systems": pending_systems},
        error=f"Timed out waiting for {len(pending_systems)} systems",
    )


def handle_custom_action(
    action: str, system_list: List[str], system_cred: Optional[str], args: List[str]
) -> CommandExecutionResult:
    custom_handlers = {
        "system_show": system_system_show_handler,
        "software_image_show": system_software_image_show_handler,
        "system_standby": system_standby_handler,
        "system_activate": system_activate_handler,
        "check_for_event": system_check_for_event_handler,
        "wse_config_show": system_wse_config_show_handler,
        "wse_config_set": system_wse_config_set_handler,
        "wse_data_update": system_wse_data_update_handler,
        # consolidated commands
        "standby_update_activate": system_sua_handler,
    }

    handler = custom_handlers.get(action)
    if handler:
        return handler(system_list, system_cred, args)
    else:
        return CommandExecutionResult(
            success=False,
            message="Unrecognized action",
            payload={},
            error="Unrecognized action",
        )


def main() -> int:
    level = logging.DEBUG if os.environ.get("DEBUG") else logging.INFO
    logging.basicConfig(level=level, stream=sys.stderr)
    logging.getLogger("asyncssh").disabled = True

    parser = argparse.ArgumentParser(description="Run command on multiple systems.")
    parser.add_argument(
        "--system_list", required=True, help="Comma-separated list of system addresses"
    )
    parser.add_argument(
        "--system_cred",
        required=True,
        help="System credentials in the format user:password",
    )
    parser.add_argument(
        "additional_args",
        nargs="*",
        help="Additional arguments that can specify predefined actions",
    )
    args = parser.parse_args()

    system_list = args.system_list.split(",")
    system_cred = args.system_cred

    if args.additional_args:
        action = args.additional_args[0]
        results = handle_custom_action(
            action, system_list, system_cred, args.additional_args[1:]
        )
        print(json.dumps(results.to_dict(), indent=4))
        if results.success is not True:
            sys.exit(1)
        return 0
    else:
        sys.exit(2)


if __name__ == "__main__":
    main()
