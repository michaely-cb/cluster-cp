import json
from random import randint, shuffle
import random
from typing import Generator
from unittest import mock

import asyncssh
import pytest
from fastapi.testclient import TestClient
from cli.dev.stats import CliDevStats_pb2
from prometheus_client.core import GaugeMetricFamily

from system_exporter import app
from system_exporter_helper import parse_dev_stats_agg
from system_exporter_constants import status_mapping


@pytest.fixture(scope="module")
def client() -> Generator[TestClient, None, None]:
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="module")
def mock_asyncssh() -> Generator[mock.MagicMock, None, None]:
    with mock.patch("asyncssh.connect", new_callable=mock.AsyncMock) as mock_connect:
        # Create a mock connection object
        mock_conn = mock.AsyncMock()
        mock_connect.return_value = mock_conn
        yield mock_conn  # Provide the mocked connection to the test


# should return health metrics if dashboard query success + other empty metrics
def test_happy_path(mock_asyncssh: mock.MagicMock, client: TestClient) -> None:
    mock_asyncssh.run.return_value = mock.MagicMock(
        stdout=json.dumps(
            {
                "systemHealth": "OK",
                "subsystemHealth": {
                    "psus": "OK"
                },
                "network": {
                    "n00": "UP"
                },
                "maintenanceMode": {
                    "enabled": "YES",
                    "purpose": "VDDC"
                }
            }
        ),
        stderr=""
    )

    response = client.get(
        "/probe?module=ssh&target=fake-system,fake-mgmt-addr,fake-ctr-addr"
    )
    assert response.status_code == 200
    assert "system_overall_health{system=\"fake-system\"} 1.0" in response.text
    assert "system_subsystem_health{subsystem=\"psus\",system=\"fake-system\"} 1.0" in response.text
    assert "system_network_health{port=\"n00\",system=\"fake-system\"} 1.0" in response.text

    assert "system_software_info" in response.text
    assert "system_events" in response.text
    assert "system_dev_stats" in response.text
    assert "system_dev_stats_labels" in response.text
    assert "system_network_flap_count" in response.text
    assert "system_network_rx_pkt_bad_fcs" in response.text
    assert "system_network_rx_fec_corrected_pkts" in response.text
    assert "system_network_rx_fec_uncorrected_pkts" in response.text
    assert "system_network_rx_local_link_flt" in response.text
    assert "system_network_rx_remote_link_flt" in response.text
    assert "system_network_err_qps" in response.text
    assert "system_network_active_qps" in response.text
    assert "system_network_sqd_qps" in response.text
    assert "system_network_rx_sequence_errors" in response.text
    assert "system_network_tx_sequence_errors" in response.text
    assert "system_network_rx_pause" in response.text
    assert "system_network_tx_pause" in response.text
    assert "system_network_ack_timeouts" in response.text
    assert "system_network_rx_bytes_per_second" in response.text
    assert "system_network_tx_bytes_per_second" in response.text

    assert "system_maintenance_mode" in response.text
    assert "system_maintenance_mode{system=\"fake-system\"} 1.0" in response.text

# if no admin svc ssh error out but ctr ip reachable, then mark as unknown
def test_admin_svc_unknown(mock_asyncssh: mock.MagicMock, client: TestClient) -> None:
    mock_asyncssh.run.side_effect = [asyncssh.Error(123, "mock error"), None]

    response = client.get(
        "/probe",
        params={"target": "fake-system,fake-mgmt-addr,127.0.0.1", "module": "ssh"},
    )
    assert response.status_code == 200
    assert (
        "system_overall_health{system=\"fake-system\"} "
        + str(status_mapping.get("ADMIN_SVC_UNKNOWN"))
        in response.text
    )


# if no admin svc ssh error out and ctr ip unreachable, then mark as not reachable
def test_system_unreachable(mock_asyncssh: mock.MagicMock, client: TestClient) -> None:
    # mock ssh connection failure
    mock_asyncssh.run.side_effect = asyncssh.Error(123, "mock error")
    response = client.get(
        "/probe",
        params={"target": "fake-system,fake-mgmt-addr,10.10.10.10", "module": "ssh"},
    )
    assert response.status_code == 200
    assert (
        "system_overall_health{system=\"fake-system\"} "
        + str(status_mapping.get("SYSTEM_NOT_REACHABLE"))
        in response.text
    )


# each request should only return metrics of the target as collector mode
def test_collector_mode(mock_asyncssh: mock.MagicMock, client: TestClient) -> None:
    response = client.get(
        "/probe",
        params={"target": "fake-system0,fake-mgmt-addr,fake-ctr", "module": "ssh"},
    )
    assert response.status_code == 200
    assert "system_overall_health{system=\"fake-system0\"}" in response.text
    response = client.get(
        "/probe",
        params={"target": "fake-system1,fake-mgmt-addr,fake-ctr", "module": "ssh"},
    )
    assert response.status_code == 200
    assert "system_overall_health{system=\"fake-system0\"}" not in response.text
    assert "system_overall_health{system=\"fake-system1\"}" in response.text


def test_parse_dev_stats_agg() -> None:
    random.seed(1234)
    system_dev_stats = GaugeMetricFamily(
        "system_dev_stats",
        "system dev stats",
        labels=["system", "category", "type", "component", "agg"],
    )
    system_dev_stats_labels = GaugeMetricFamily(
        "system_dev_stats_labels",
        "system dev stats labels",
        labels=["category", "type", "component"],
    )

    system_name = "unittestsystem"
    down_sample_interval_s = 20
    counters = ["Sensor.counter1:NONE:TEST1", "Sensor.counter2:NONE:TEST2"]
    points = [
        dict(ts=i, counter1=i if i < 30 else 60-i, counter2=randint(0, 999)) for i in range(1, 61)
    ]

    # Test interval boundaries by putting maxima at the: end, start, start.
    points[down_sample_interval_s-1]['counter1'] = 1111
    points[down_sample_interval_s-1]['counter2'] = 2222
    points[down_sample_interval_s]['counter1'] = 11111
    points[down_sample_interval_s]['counter2'] = 22222
    points[down_sample_interval_s*2]['counter1'] = 111111
    points[down_sample_interval_s*2]['counter2'] = 222222

    datapoints = [CliDevStats_pb2.DataPoint(
        timestamp=p['ts'], values=[p['counter1']], floatValues=[p['counter2']]) for p in points]
    # Datapoints can arrive out of order.
    shuffle(datapoints)
    # Split the datapoints across two responses.
    responses = [
        CliDevStats_pb2.ShowResponse(
            counters=counters, datapoints=datapoints[:30]),
        CliDevStats_pb2.ShowResponse(
            counters=counters, datapoints=datapoints[30:]),
    ]

    parse_dev_stats_agg(
        system_name, responses,
        down_sample_interval_s,
        system_dev_stats
    )

    # 2 counters * 3 samples.
    assert len(system_dev_stats.samples) == 6
    counter1_samples = [
        s for s in system_dev_stats.samples if s.labels['type'] == 'counter1']
    counter2_samples = [
        s for s in system_dev_stats.samples if s.labels['type'] == 'counter2']
    counter1_samples.sort(key=lambda s: s.timestamp)
    counter2_samples.sort(key=lambda s: s.timestamp)

    assert counter1_samples[0].timestamp == points[down_sample_interval_s-1]['ts']
    assert counter1_samples[0].value == points[down_sample_interval_s-1]['counter1']
    assert counter1_samples[1].timestamp == points[(down_sample_interval_s*2)-1]['ts']
    assert counter1_samples[1].value == points[down_sample_interval_s]['counter1']
    assert counter1_samples[2].timestamp == points[-1]['ts']
    assert counter1_samples[2].value == points[down_sample_interval_s*2]['counter1']

    assert counter2_samples[0].timestamp == points[down_sample_interval_s-1]['ts']
    assert counter2_samples[0].value == points[down_sample_interval_s-1]['counter2']
    assert counter2_samples[1].timestamp == points[(down_sample_interval_s*2)-1]['ts']
    assert counter2_samples[1].value == points[down_sample_interval_s]['counter2']
    assert counter2_samples[2].timestamp == points[-1]['ts']
    assert counter2_samples[2].value == points[down_sample_interval_s*2]['counter2']