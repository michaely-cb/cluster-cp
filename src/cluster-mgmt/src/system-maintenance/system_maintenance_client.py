"""A gRPC client for communicating with system."""

from typing import AsyncGenerator, Iterable, Optional

import grpc

from pb.cli.diagnostics import CliDiagnostics_pb2, CliDiagnostics_pb2_grpc
from pb.cli.system import CliSystem_cs2_pb2, CliSystem_cs2_pb2_grpc

# vddc set/unset can take 1 or 2 minutes, using 5 minutes as a timeout value
# TODO: update to use a busy polling strategy, with exponential backoff
DEFAULT_GRPC_TIMEOUT_SEC = 300

class SystemClientGrpcError(Exception):
    pass

class SystemClient:
    def __init__(
        self, channel: grpc.aio.Channel, timeout_sec: Optional[int] = None
    ) -> None:
        self.channel = channel
        self.timeout_sec = timeout_sec or DEFAULT_GRPC_TIMEOUT_SEC

    async def system_show(
        self,
        detailed: Optional[bool] = False
    ) -> CliSystem_cs2_pb2.ShowResponse:
        """Retrieve the system summary information.

        Args:
            detailed: Whether to show detailed system hardware status.

        Returns:
            A ShowResponse object containing system details.
        """
        request = CliSystem_cs2_pb2.ShowRequest(detailed=detailed)
        stub = CliSystem_cs2_pb2_grpc.SystemStub(self.channel)

        try:
            return await stub.show(request, timeout=self.timeout_sec)
        except grpc.RpcError as e:
            raise SystemClientGrpcError(f"gRPC call failed: {str(e)}")

    async def vddc_set(
        self,
        vddc_offset: int,
        no_confirm: bool = False,
        skip_failed_ops: bool = False
    ) -> CliDiagnostics_pb2.VddcShowResponse:
        """Set VDDC voltage to the provided offset.

        Args:
            no_confirm: Do not ask for confirmation (default: False).
            vddc_offset: VDDC offset to be applied (in mV).
            skip_failed_ops: Continue without failing if setting VDDC offset fails (default: False).

        Returns:
            A VddcShowResponse object containing the operation details.
        """
        request = CliDiagnostics_pb2.VddcSetRequest(
            noConfirm=no_confirm,
            vddcOffset=vddc_offset,
            skipFailedOps=skip_failed_ops,
        )
        stub = CliDiagnostics_pb2_grpc.DiagnosticsStub(self.channel)

        try:
            return await stub.vddcSet(request, timeout=self.timeout_sec)
        except grpc.RpcError as e:
            raise SystemClientGrpcError(f"gRPC call failed: {str(e)}")

    async def vddc_unset(
        self,
        no_confirm: bool = False,
        skip_set_check: bool = False,
        skip_failed_ops: bool = False
    ) -> CliDiagnostics_pb2.VddcShowResponse:
        """Unset VDDC voltage to default.

        Args:
            no_confirm: Do not ask for confirmation (default: False).
            skip_set_check: Skip checking if VDDC offset is set (default: False).
            skip_failed_ops: Continue without failing if unsetting VDDC offset fails (default: False).

        Returns:
            A VddcShowResponse object containing the operation details.
        """
        request = CliDiagnostics_pb2.VddcUnsetRequest(
            noConfirm=no_confirm,
            skipSetCheck=skip_set_check,
            skipFailedOps=skip_failed_ops,
        )
        stub = CliDiagnostics_pb2_grpc.DiagnosticsStub(self.channel)

        try:
            return await stub.vddcUnset(request, timeout=self.timeout_sec)
        except grpc.RpcError as e:
            raise SystemClientGrpcError(f"gRPC call failed: {str(e)}")

    async def hl_mem_bist_start(
        self,
        request: CliDiagnostics_pb2.HlMemBistStartRequest
    ) -> CliDiagnostics_pb2.HlMemBistStartResponse:
        """Start HL Memory BIST test.

        Args:
            vddc_offset: VDDC offset to be applied during the test (in mV).
            no_confirm: If True, skips the confirmation prompt (default: False).
            skip_failed_ops: If True, continues without exiting early on any failed operations (default: False).

        Returns:
            An HlMemBistStartResponse object containing the test ID and message.
        """
        stub = CliDiagnostics_pb2_grpc.DiagnosticsStub(self.channel)

        try:
            return await stub.hlMemBistStart(request, timeout=self.timeout_sec)
        except grpc.RpcError as e:
            raise SystemClientGrpcError(f"gRPC call failed: {str(e)}")

    async def hl_mem_bist_show(
        self,
        test_id: str
    ) -> CliDiagnostics_pb2.HlMemBistShowResponse:
        """Show the status of the HL Memory BIST test.

        Args:
            test_id: The test ID to query.

        Returns:
            An HlMemBistShowResponse object containing the test status.
        """
        request = CliDiagnostics_pb2.HlMemBistShowRequest(testId=test_id)
        stub = CliDiagnostics_pb2_grpc.DiagnosticsStub(self.channel)

        try:
            return await stub.hlMemBistShow(request, timeout=self.timeout_sec)
        except grpc.RpcError as e:
            raise SystemClientGrpcError(f"gRPC call failed: {str(e)}")