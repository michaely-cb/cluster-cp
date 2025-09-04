"""A gRPC client for communicating with system."""

from typing import AsyncGenerator, Iterable, Optional

import grpc
from pb.cli.dev.stats import CliDevStats_pb2, CliDevStats_pb2_grpc
from pb.cli.events import CliEvents_pb2, CliEvents_pb2_grpc

DEFAULT_GRPC_TIMEOUT_SEC = 5


class SystemClientGrpcError(Exception):
    pass


class SystemClient:
    def __init__(
        self, channel: grpc.aio.Channel, timeout_sec: Optional[int] = None
    ) -> None:
        self.channel = channel
        self.timeout_sec = timeout_sec or DEFAULT_GRPC_TIMEOUT_SEC

    async def dev_stats_list(self) -> CliDevStats_pb2.ListResponse:
        """Lists all available metrics on system.

        Returns:
            A ListResponse object containing all available metrics.
        Raises:
            SystemClientGrpcError: If the gRPC call fails.
        """
        request = CliDevStats_pb2.ListRequest()
        stub = CliDevStats_pb2_grpc.DevStatsStub(self.channel)

        try:
            return await stub.list(request, timeout=self.timeout_sec)
        except grpc.RpcError as e:
            raise SystemClientGrpcError(f"gRPC call failed: {str(e)}")

    async def dev_stats_show(
        self,
        from_time: Optional[str] = None,
        end_time: Optional[str] = None,
        counters: Optional[Iterable[str]] = None,
        show_all: Optional[bool] = None,
        rollup_type: Optional[str] = None,
        max_data_points: Optional[int] = None,
    ) -> AsyncGenerator[CliDevStats_pb2.ShowResponse, None]:
        """Retrieve the metrics for the given counters.

        Args:
            from_time: Show stats at or after this time. E.g: 2019-01-01 or
                2019-01-01T00:00:00.
            end_time: Show stats at or before this time, E.g: 2019-01-01 or
                2019-01-01T00:00:00. Defaults to now.
            counters: Show data about these counters.
                Each counter should be of the form
                metricName:rollupType:ComponentName
                E.g: NetworkInterface.rx_bytes_sec:AVG:DATA_NETWORK_IF_1
                Glob patterns like NetworkInterface* are allowed.
            show_all: Show all available counters.
            rollup_type: Return data only from specified rollup type.
            max_data_points: Return maximum number of data points.

        Returns:
            A ShowResponse object containing the requested metrics.
        """
        request = CliDevStats_pb2.ShowRequest(
            fromTime=from_time,
            endTime=end_time,
            counters=counters,
            showAll=show_all,
            rollupType=rollup_type,
            maxDataPoints=max_data_points,
        )
        stub = CliDevStats_pb2_grpc.DevStatsStub(self.channel)
        try:
            call = stub.show(request, timeout=self.timeout_sec)
            async for response in call:
                yield response
        except grpc.RpcError as e:
            raise SystemClientGrpcError(f"gRPC call failed: {str(e)}")

    async def events_show(
        self,
        from_time: Optional[str] = None,
        end_time: Optional[str] = None,
        event_type: Optional[Iterable[str]] = None,
        severity: Optional[str] = None,
        max_count: Optional[int] = None,
        user_mode: Optional[str] = None,
    ) -> AsyncGenerator[CliEvents_pb2.ShowResponse, None]:
        """Retrieve the metrics for the given counters.

        Args:
            from_time: Show stats at or after this time. E.g: 2019-01-01 or
                2019-01-01T00:00:00.
            end_time: Show stats at or before this time, E.g: 2019-01-01 or
                2019-01-01T00:00:00. Defaults to now.
            event_type: Filter events matching the specified event type or matching the specified pattern.\n"
            "Each eventType can be in the form of a specific event, such as \"EventUpgradeStarted\", or glob patterns "
            "like \"EventUpgrade*\".
            severity: Show events of specified severity. Can be one of CRITICAL, ERROR, WARN, INFO.
            max_count: Maximum number of events to show. E.g: 50.
            user_mode: Shows events limited to the user level. Can be one of ADMIN, SUPPORT(same as cmd shows), DEBUG.

        Returns:
            A ShowResponse object containing the requested metrics.
        """
        request = CliEvents_pb2.ShowRequest(
            fromTime=from_time,
            endTime=end_time,
            eventType=event_type,
            severity=severity,
            maxCount=max_count,
            userMode=user_mode,
        )
        stub = CliEvents_pb2_grpc.EventsStub(self.channel)
        try:
            call = stub.show(request, timeout=self.timeout_sec)
            async for response in call:
                yield response
        except grpc.RpcError as e:
            raise SystemClientGrpcError(f"gRPC call failed: {str(e)}")
