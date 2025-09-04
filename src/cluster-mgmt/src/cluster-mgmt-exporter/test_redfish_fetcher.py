import asyncio
import json
import logging
import os
import re
from glob import glob
from typing import List

import pytest
from aioresponses import aioresponses
from docdb_client import DeviceDBClient
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.samples import Sample
from redfish_fetcher import NodeHealth, ServerRedfishStats


def get_sample(samples: List[Sample], label: str, value: str) -> Sample:
    return next((x for x in samples if x.labels[label] == value), None)


def check_health_metrics(
    redfish_metrics: ServerRedfishStats,
    node_name: str,
    ipmi_addr: str,
    vendor: str,
):
    logging.info(
        f"Checking health for node_name={node_name}, ipmi_addr={ipmi_addr}, vendor={vendor}"
    )
    assert "health" in redfish_metrics.metrics
    assert "node_ipmi_health_status" in redfish_metrics.metrics["health"].metrics
    samples = redfish_metrics.metrics["health"].metrics["node_ipmi_health_status"].samples

    # Get the range of expected values for health status:
    status_values = NodeHealth.status_encoding.values()
    status_min, status_max = min(status_values), max(status_values)

    overall = get_sample(samples, "category", "Overall")
    assert overall
    assert overall.labels["node"] == node_name
    assert status_min <= overall.value <= status_max
    processor = get_sample(samples, "category", "Processor")
    assert processor
    assert status_min <= processor.value <= status_max
    memory = get_sample(samples, "category", "Memory")
    assert memory
    assert status_min <= memory.value <= status_max

    fan = get_sample(samples, "category", "Fan")
    assert fan
    assert status_min <= fan.value <= status_max
    power_supply = get_sample(samples, "category", "Power supply")
    assert power_supply
    assert status_min <= power_supply.value <= status_max
    temperatures = get_sample(samples, "category", "Temperatures")
    assert temperatures
    assert status_min <= temperatures.value <= status_max

    # supermicro doesn't have these
    if vendor not in ["DL", "HP"]:
        return
    cpu = get_sample(samples, "category", "CPU")
    assert cpu
    assert status_min <= cpu.value <= status_max
    storage = get_sample(samples, "category", "Storage")
    assert storage
    assert status_min <= storage.value <= status_max


def check_thermal_metrics(
    redfish_metrics: ServerRedfishStats,
    node_name: str,
    ipmi_addr: str,
    vendor: str,
):
    # These metrics were formerly in metrics["thermals"]
    logging.info(
        f"Checking thermals for node_name={node_name}, ipmi_addr={ipmi_addr}, vendor={vendor}"
    )
    assert "health" in redfish_metrics.metrics
    assert "fan" in redfish_metrics.metrics["health"].metrics
    assert "fan_thresh" in redfish_metrics.metrics["health"].metrics
    assert "thermometer" in redfish_metrics.metrics["health"].metrics
    assert "thermometer_thresh" in redfish_metrics.metrics["health"].metrics
    assert len(redfish_metrics.metrics["health"].metrics["thermometer_thresh"].samples)
    if vendor in ["DL", "SM"]:
        assert len(redfish_metrics.metrics["health"].metrics["fan_thresh"].samples)
    for fan in redfish_metrics.metrics["health"].metrics["fan"].samples:
        assert "units" in fan.labels
        assert fan.labels["node"] == node_name
        units = fan.labels["units"]
        if units == "RPM":
            assert 0 <= fan.value <= 44444
        elif units == "Percent":
            assert 0 <= fan.value <= 100
        else:
            assert False, f"Unexpected fan units: {units}"
    for fan in redfish_metrics.metrics["health"].metrics["fan_thresh"].samples:
        assert "units" in fan.labels
        assert fan.labels["node"] == node_name
        assert fan.labels["type"] in ["warn", "error"]
        units = fan.labels["units"]
        if units == "RPM":
            assert 0 <= fan.value <= 44444
        elif units == "Percent":
            assert 0 <= fan.value <= 100
        else:
            assert False, f"Unexpected fan units: {units}"
    for therm in redfish_metrics.metrics["health"].metrics["thermometer"].samples:
        assert "units" in therm.labels
        assert therm.labels["node"] == node_name
        units = therm.labels["units"]
        if units == "degreesC":
            assert 0 <= therm.value <= 222
        else:
            assert False, f"Unexpected thermometer units: {units}"
    for therm in redfish_metrics.metrics["health"].metrics["thermometer_thresh"].samples:
        assert "units" in therm.labels
        assert therm.labels["node"] == node_name
        assert therm.labels["type"] in ["warn", "error"]
        units = therm.labels["units"]
        if units == "degreesC":
            assert 0 <= therm.value <= 222
        else:
            assert False, f"Unexpected thermometer units: {units}"


def devices():
    db_client = DeviceDBClient()
    nodes = db_client.list_devices()

    devices = []
    for node in nodes:
        # Ignore devices without IPMI
        if not node.get("properties", {}).get("ipmi_info", {}):
            continue
        devices.append(
            dict(
                node_name=node["name"],
                vendor=node["properties"]["vendor"]["name"],
                ipmi_addr=node["properties"]["ipmi_info"]["ip"],
                user=node["properties"]["ipmi_credentials"]["user"],
                password=node["properties"]["ipmi_credentials"]["password"],
            )
        )
    return devices


@pytest.mark.parametrize("device", devices(), ids=lambda d: f"{d['node_name']} ({d['vendor']})")
@pytest.mark.skipif(os.getenv("CLUSTER_MGMT_HARDWARE_SMOKE_TEST") != "1",
                    reason="requires access to IPMI services of real servers")
def test_device(device):
    node_name, vendor, ipmi_addr = (
        device["node_name"],
        device["vendor"],
        device["ipmi_addr"],
    )
    redfish_metrics = ServerRedfishStats(device["user"], device["password"])
    modules = ["health"]
    for module in modules:
        logging.info(f"Fetching module={module}, node_name={node_name}, ipmi_addr={ipmi_addr}")
        asyncio.run(redfish_metrics.fetch_for_system(node_name, ipmi_addr, module))
        if module == "health":
            check_health_metrics(redfish_metrics, node_name, ipmi_addr, vendor)
            check_thermal_metrics(redfish_metrics, node_name, ipmi_addr, vendor)
        else:
            assert False, f"Unexpeted module type: {module}"


@pytest.fixture
def mocked():
    with aioresponses() as m:
        yield m


def test_parsing(mocked):
    # load all expected results
    expected_results = dict()
    for testcase_hostname_path in glob(
        os.path.join(os.path.dirname(__file__), "test-inputs/ipmi/expected_metrics/*")
    ):
        testcase_hostname = os.path.basename(testcase_hostname_path)
        expected_results[testcase_hostname] = dict()
        with open(testcase_hostname_path) as f:
            metrics = text_string_to_metric_families(f.read())
            expected_results[testcase_hostname] = list(metrics)

    # load all jsons
    for testcase_hostname_path in glob(
        os.path.join(os.path.dirname(__file__), "test-inputs/ipmi/redfish_jsons/*")
    ):
        testcase_hostname = os.path.basename(testcase_hostname_path)
        for uri in glob(os.path.join(testcase_hostname_path, "*.json")):
            with open(uri) as f:
                slashpath = re.sub("_", "/", os.path.splitext(os.path.basename(uri))[0])
                # use repeat=True so that we can answer the same request more than once; default is reply only once
                mocked.get(
                    f"https://{testcase_hostname}-ipmi/{slashpath}",
                    status=200,
                    payload=json.load(f),
                    repeat=True,
                )

    # Make the eventloop for the test run
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None # a previous test created and destroyed a loop, so make a new one

    if loop:
        loop.stop()
        loop.close()

    loop = asyncio.new_event_loop()

    for tc in expected_results:
        print(f"Testing {tc}")

        stats = ServerRedfishStats("unused", "unused")  # auth ignored by aioresponses
        loop.run_until_complete(stats.fetch_for_system(tc, f'{tc}-ipmi', 'health'))
        assert expected_results[tc] == list(stats.metrics['health'].metrics.values())
