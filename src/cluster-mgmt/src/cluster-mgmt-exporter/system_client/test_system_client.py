from typing import AsyncGenerator
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from cli.dev.stats import CliDevStats_pb2
from system_client import system_client


@pytest.mark.asyncio
async def test_dev_stats_list_success():
    mock_channel = MagicMock()
    counter_info_list = [
        CliDevStats_pb2.CounterInfo(name="cpu_usage", unit="percentage"),
        CliDevStats_pb2.CounterInfo(name="memory_usage", unit="megabytes"),
    ]
    mock_response = CliDevStats_pb2.ListResponse(counters=counter_info_list)

    mock_stub = MagicMock()
    mock_stub.list = AsyncMock(return_value=mock_response)

    with mock.patch(
        "pb.cli.dev.stats.CliDevStats_pb2_grpc.DevStatsStub", return_value=mock_stub
    ):
        mock_channel.__enter__.return_value = mock_stub
        client = system_client.SystemClient(channel=mock_channel)

        response = await client.dev_stats_list()

        assert (
            response == mock_response
        ), "The response from dev_stats_list should match the mock response"

        mock_stub.list.assert_called_once_with(CliDevStats_pb2.ListRequest(), timeout=5)


@pytest.mark.asyncio
async def test_dev_stats_list_failure():
    mock_channel = MagicMock()
    mock_stub = MagicMock()

    rpc_error = grpc.RpcError("Test gRPC failure")
    mock_stub.list = AsyncMock(side_effect=rpc_error)

    with mock.patch(
        "pb.cli.dev.stats.CliDevStats_pb2_grpc.DevStatsStub", return_value=mock_stub
    ):
        mock_channel.__enter__.return_value = mock_stub
        client = system_client.SystemClient(channel=mock_channel)

        with pytest.raises(system_client.SystemClientGrpcError) as exc_info:
            await client.dev_stats_list()

        assert (
            str(exc_info.value) == "gRPC call failed: Test gRPC failure"
        ), "The raised exception should match the expected message"

        mock_stub.list.assert_called_once_with(CliDevStats_pb2.ListRequest(), timeout=5)


@pytest.mark.asyncio
async def test_dev_stats_show_success():
    mock_channel = MagicMock()
    mock_stub = MagicMock()

    response_data = [
        CliDevStats_pb2.ShowResponse(
            counters=["cpu_usage"],
            datapoints=[CliDevStats_pb2.DataPoint(timestamp=1590000000, values=[0.5])],
        ),
        CliDevStats_pb2.ShowResponse(
            counters=["memory_usage"],
            datapoints=[CliDevStats_pb2.DataPoint(timestamp=1590000001, values=[256])],
        ),
    ]

    async def async_response_generator(
            responses,
    ) -> AsyncGenerator[CliDevStats_pb2.ShowResponse, None]:
        for res in responses:
            yield res

    mock_stub.show.return_value = async_response_generator(response_data)

    with mock.patch(
        "pb.cli.dev.stats.CliDevStats_pb2_grpc.DevStatsStub", return_value=mock_stub
    ):
        client = system_client.SystemClient(channel=mock_channel)

        results = []
        async for response in client.dev_stats_show(
            from_time="2024-07-10T00:00:00",
            end_time="2024-07-10T00:00:10",
            counters=["cpu_usage", "memory_usage"],
            show_all=True,
            rollup_type="AVG",
            max_data_points=10,
        ):
            results.append(response)

        assert len(results) == 2
        assert results[0].counters == ["cpu_usage"]
        assert results[1].counters == ["memory_usage"]

        mock_stub.show.assert_called_once_with(
            CliDevStats_pb2.ShowRequest(
                fromTime="2024-07-10T00:00:00",
                endTime="2024-07-10T00:00:10",
                counters=["cpu_usage", "memory_usage"],
                showAll=True,
                rollupType="AVG",
                maxDataPoints=10,
            ),
            timeout=5,  # assuming you have a default timeout set
        )


@pytest.mark.asyncio
async def test_dev_stats_show_failure():
    mock_channel = MagicMock()
    mock_stub = MagicMock()

    rpc_error = grpc.RpcError("Test gRPC failure during show")
    mock_stub.show.side_effect = rpc_error

    with mock.patch(
        "pb.cli.dev.stats.CliDevStats_pb2_grpc.DevStatsStub", return_value=mock_stub
    ):
        client = system_client.SystemClient(channel=mock_channel)

        with pytest.raises(system_client.SystemClientGrpcError) as exc_info:
            async for _ in client.dev_stats_show(
                from_time="2024-07-10T00:00:00",
                end_time="2024-07-10T00:00:10",
                counters=["cpu_usage", "memory_usage"],
                show_all=True,
                rollup_type="AVG",
                max_data_points=10,
            ):
                pass
        assert "gRPC call failed: Test gRPC failure during show" in str(
            exc_info.value
        ), "The raised exception should match the expected message"

        mock_stub.show.assert_called_once_with(
            CliDevStats_pb2.ShowRequest(
                fromTime="2024-07-10T00:00:00",
                endTime="2024-07-10T00:00:10",
                counters=["cpu_usage", "memory_usage"],
                showAll=True,
                rollupType="AVG",
                maxDataPoints=10,
            ),
            timeout=5,
        )
