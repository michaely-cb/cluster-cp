"""system exporter"""
import datetime
import json
import math
import os
import re
from typing import (Any, AsyncGenerator, Dict, List, NamedTuple, Optional)

import asyncio
import asyncssh
from prometheus_client.core import GaugeMetricFamily

import redfish_fetcher
from pb.cb.options_pb2 import EventSeverity
from pb.cli.dev.stats import CliDevStats_pb2
from pb.cli.events import CliEvents_pb2
from system_client import system_client
from system_exporter_constants import component_mapping, prom_probe_timeout
from system_exporter_logger import logger

REDFISH_METRIC_CATEGORY = "NetworkInterface"
REDFISH_METRIC_PREFIX = "DATA_NETWORK_IF_"
EVENTS_USER_MODE = os.getenv("EVENTS_USER_MODE", "SUPPORT")

LEGACY_USER = "admin"
LEGACY_PASSWORD = "admin"

class SystemMetricIdentifier(NamedTuple):
    category: str
    type_: str
    component: str


async def fetch_dashboard(
    system_name: str, addr: str,
    user: str, password: str,
) -> Optional[Dict[str, Any]]:
    """Fetch system dashboard json.

    Args:
        system_name: The system name
        addr: The address used to connect to the system
        user: The user used to connect to the system
        password: The password used to connect to the system
    Returns:
        The system dashboard json, or None if the fetch failed.
    """
    cmd = "entersupportmode --noConfirm; dashboard show --output-format=json"
    logger.info(f"system dashboard: {system_name}, cmd: {cmd}")
    try:
        output = await ssh_run_command(user, password, addr, cmd)
        output = json.loads(output)
        if not isinstance(output, dict):
            logger.error(f"Invalid dashboard output: {output}")
            return None
        logger.debug(f"system dashboard: {system_name}, ret: {output}")
        return output
    except Exception as e:
        logger.info(f"error fetching system dashboard for {system_name}: {e}")
    return None


async def fetch_system_software(
    system_name: str, addr: str,
    user: str, password: str,
) -> Optional[Dict[str, Any]]:
    """Fetch system software info json.

    Args:
        system_name: The system name
        addr: The address used to connect to the system
        user: The user used to connect to the system
        password: The password used to connect to the system

    Returns:
        The system software info json
    """
    cmd = "software show --detailed --output-format=json"
    output = ""
    try:
        logger.info(f"system software: {system_name}, cmd: {cmd}")
        output = await ssh_run_command(user, password, addr, cmd)
        output = json.loads(output)
        if not isinstance(output, dict):
            logger.error(f"Invalid software output: {output}")
            return None
        ret = {"product": output["product"], "execmode": output["execmode"],
               "version": output["product"]["version"], "buildid": output["product"]["buildid"]}
        logger.debug(f"system software: {system_name}, ret: {ret}")
        return ret
    except Exception as e:
        logger.error(f"error fetching system software for {system_name}: {e}, {output}")
    return None


async def fetch_system_wse_config(
    system_name: str, addr: str,
    user: str, password: str,
) -> Optional[Dict[str, Any]]:
    """Fetch system wse config info json.

    Args:
        system_name: The system name
        addr: The address used to connect to the system
        user: The user used to connect to the system
        password: The password used to connect to the system

    Returns:
        The system software info json
    """
    cmd = "entersupportmode --noConfirm; wse config show --output-format=json"
    output = ""
    try:
        logger.info(f"system wse config: {system_name}, cmd: {cmd}")
        output = await ssh_run_command(user, password, addr, cmd)
        output = json.loads(output)
        if not isinstance(output, dict):
            logger.error(f"Invalid wse config output: {output}")
            return None
        ret = {"configName": output["activeConfigDetails"]["configName"],
               "coreFreq": output["activeConfigDetails"]["coreFreq"],
               "ioFreq": output["activeConfigDetails"]["ioFreq"]}
        logger.debug(f"system wse config: {system_name}, ret: {ret}")
        return ret
    except Exception as e:
        logger.error(f"error fetching system wse config for {system_name}: {e}, {output}")
    return None


async def collect_system_events(
    system_name: str,
    system_grpc_client: system_client.SystemClient,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
) -> AsyncGenerator[CliEvents_pb2.ShowResponse, None]:
    """Fetch all system events.

    Args:
        system_name: The target system name
        system_grpc_client: The system gRPC client
        start_time: The start time for the fetch
        end_time: The end time for the fetch

    Yields:
        The ShowResponse object for each event fetch
    """
    # ensure next start time/end time non overlapping
    formatted_start_time = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    formatted_end_time = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    logger.info(
        f"{system_name}: Collecting system events from {formatted_start_time} "
        f"to {formatted_end_time}"
    )
    time_now = datetime.datetime.utcnow()
    try:
        async for response in system_grpc_client.events_show(
            from_time=formatted_start_time,
            end_time=formatted_end_time,
            user_mode=EVENTS_USER_MODE,
        ):
            yield response
    except system_client.SystemClientGrpcError as e:
        logger.error(f"SystemClient gRPC call failed: {e}")
        return
    except Exception as e:
        logger.error(f"SystemClient unknown failure: {e}")
        return
    finally:
        logger.info(
            f"{system_name}: events_show "
            f"took {(datetime.datetime.utcnow() - time_now).total_seconds():.2f}s"
        )


async def collect_overall_dev_stats(
    system_name: str,
    system_grpc_client: system_client.SystemClient,
    down_sample_interval_sec: int,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
) -> AsyncGenerator[CliDevStats_pb2.ShowResponse, None]:
    """Fetch all dev stats.

    Args:
        system_name: The target system name
        system_grpc_client: The system gRPC client
        down_sample_interval_sec: Apply down sampling interval, e.g. every 30s
        start_time: The start time for the fetch
        end_time: The end time for the fetch

    Yields:
        The ShowResponse object for each dev stats fetch
    """
    scrape_time = start_time

    while scrape_time < end_time:
        # ensure next start time/end time non overlapping
        formatted_start_time = scrape_time.strftime("%Y-%m-%dT%H:%M:%S")
        scrape_time += datetime.timedelta(seconds=down_sample_interval_sec)
        if down_sample_interval_sec == 0 or scrape_time > end_time:
            scrape_time = end_time
        formatted_end_time = (scrape_time - datetime.timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")
        logger.info(
            f"{system_name}: Collecting dev stats from {formatted_start_time} "
            f"to {formatted_end_time}"
        )
        time_now = datetime.datetime.utcnow()
        try:
            async for response in system_grpc_client.dev_stats_show(
                from_time=formatted_start_time,
                end_time=formatted_end_time,
                counters=["*:NONE:*"],
                max_data_points=1,
            ):
                yield response
        except system_client.SystemClientGrpcError as e:
            logger.error(f"SystemClient gRPC call failed: {e}")
            return
        except Exception as e:
            logger.error(f"SystemClient unknown failure: {e}")
            return
        finally:
            logger.info(
                f"{system_name}: dev_stats_show all "
                f"took {(datetime.datetime.utcnow() - time_now).total_seconds():.2f}s"
            )


async def collect_specific_dev_stats(
    system_name: str,
    system_grpc_client: system_client.SystemClient,
    counters: List[str],
    start_time: datetime.datetime,
    end_time: datetime.datetime,
) -> AsyncGenerator[CliDevStats_pb2.ShowResponse, None]:
    """Fetch all datapoints for specified counters.
    Args:
        system_name: The target system name
        system_grpc_client: The system gRPC client
        counters: The counters to collect, e.g., "*:NONE:GLTCH_EXCUR*", "*:NONE:GLTCH_TRANS*"
        start_time: The start time for the fetch
        end_time: The end time for the fetch
    """
    # add 1s to avoid jitter causing window overlapping
    start_time += datetime.timedelta(seconds=1)
    formatted_start_time = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    formatted_end_time = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    logger.info(
        f"{system_name}: Collecting {'|'.join(counters)} counters from {formatted_start_time} "
        f"to {formatted_end_time}"
    )
    time_now = datetime.datetime.utcnow()
    try:
        async for response in system_grpc_client.dev_stats_show(
            from_time=formatted_start_time,
            end_time=formatted_end_time,
            counters=counters,
        ):
            yield response
    except system_client.SystemClientGrpcError as e:
        logger.error(f"SystemClient gRPC call failed: {e}")
        return
    except Exception as e:
        logger.error(f"SystemClient unknown failure: {e}")
        return
    finally:
        logger.info(
            f"{system_name}: dev_stats_show {'|'.join(counters)} "
            f"took {(datetime.datetime.utcnow() - time_now).total_seconds():.2f}s"
        )


def parse_system_events(
    system_name: str,
    show_responses: List[CliEvents_pb2.ShowResponse],
    system_events: GaugeMetricFamily,
):
    """Process ShowResponse generator.

    Args:
        system_name: The system name
        show_responses: The ShowResponse list from the gRPC call
        system_events: Metrics to post 
    """

    try:
        datapoint_counter = 0
        last_ts = None
        first_ts = None
        for response in show_responses:
            for event in response.events:
                ts = event.trigger_time / 1000
                if not first_ts or ts < first_ts:
                    first_ts = ts
                if not last_ts or ts > last_ts:
                    last_ts = ts

                severity_str = EventSeverity.Name(event.severity)
                component_str = component_mapping.get(event.component, str(event.component))
                system_events.add_metric(
                    [
                        system_name,
                        severity_str,
                        event.type,
                        component_str,
                    ],
                    1,
                    timestamp=ts,
                )
                logger.debug(f"{system_name} {severity_str} {event.type} {component_str}")
                datapoint_counter += 1
        logger.info(
            f"{system_name}: Processed {datapoint_counter} events "
            f"for system events, "
            f"first_ts: {datetime.datetime.fromtimestamp(first_ts) if first_ts else ''}, "
            f"last_ts: {datetime.datetime.fromtimestamp(last_ts) if last_ts else ''}"
        )
    except Exception as e:
        logger.error(f"Failed to parse system events: {e}")


def parse_dev_stats(
    system_name: str,
    show_responses: List[CliDevStats_pb2.ShowResponse],
    system_dev_stats: GaugeMetricFamily,
    system_dev_stats_labels: GaugeMetricFamily,
):
    """Process ShowResponse generator.

    Args:
        system_name: The system name
        show_responses: The ShowResponse list from the gRPC call
        system_dev_stats: Metrics to post
        system_dev_stats_labels: Label metrics to post

    Yields:
        Dev stats metric object
    """

    def _parse_dev_stats_datapoint(
        counters: List[str],
        timestamp: float,
        datapoint_values: List[int],
        datapoint_float_values: List[float],
        export_labels: bool,
    ):
        """Processes a single datapoint from the response.
    
        A single datapoint normally corresponds to metrics collected at the same
        timestamp.
   
        Args:
           counters: The list of counter names
           timestamp: The timestamp of the datapoint
           datapoint_values: The list of integer values
           datapoint_float_values: The list of float values
           export_labels: Whether to export the _labels metric
   
        Yields:
           The system dev stats metric object
        """
        for i in range(len(counters)):
            system_metric_id = _parse_dev_stat_counter(counters[i])
            if not system_metric_id:
                continue
            if export_labels:
                system_dev_stats_labels.add_metric(
                    [
                        system_metric_id.category,
                        system_metric_id.type_,
                        system_metric_id.component,
                    ],
                    1,
                )
            # Ignore current and GLTCH; they're collected separately and aggregated.
            if system_metric_id.type_ == "current":
                continue
            if system_metric_id.component.startswith('GLTCH_TRANS_') or system_metric_id.component.startswith('GLTCH_EXCUR_'):
                continue

            if i < values_len:
                metric_value = datapoint_values[i]
            else:
                metric_value = datapoint_float_values[i - values_len]
            if math.isnan(metric_value):
                continue

            system_dev_stats.add_metric(
                [
                    system_name,
                    system_metric_id.category,
                    system_metric_id.type_,
                    system_metric_id.component,
                ],
                metric_value,
                timestamp=timestamp,
            )

    try:
        datapoint_counter = 0
        last_ts = None
        first_ts = None
        for response in show_responses:
            for datapoint in response.datapoints:
                if last_ts and last_ts >= datapoint.timestamp:
                    logger.warning(
                        f"Ignore out-of-order data for dev metric, "
                        f"timestamps is not in increasing order: "
                        f"last: {datetime.datetime.fromtimestamp(last_ts)} "
                        f"current: {datetime.datetime.fromtimestamp(datapoint.timestamp)}"
                    )
                    continue
                last_ts = datapoint.timestamp
                if not first_ts:
                    first_ts = last_ts
                ts = datetime.datetime.fromtimestamp(datapoint.timestamp)
                values_len = len(datapoint.values)
                float_values_len = len(datapoint.floatValues)
                if len(response.counters) != values_len + float_values_len:
                    logger.error(
                        f"Counters and values have different lengths: "
                        f"{len(response.counters)} vs {values_len + float_values_len}"
                    )
                    continue

                export_labels = datapoint_counter == 0
                _parse_dev_stats_datapoint(
                    response.counters,
                    ts.timestamp(),
                    datapoint.values,
                    datapoint.floatValues,
                    export_labels,
                )
                datapoint_counter += 1
        logger.info(
            f"{system_name}: Processed {datapoint_counter} datapoints for dev metrics, "
            f"first_ts: {datetime.datetime.fromtimestamp(first_ts) if first_ts else ''}, "
            f"last_ts: {datetime.datetime.fromtimestamp(last_ts) if last_ts else ''}"
        )
    except Exception as e:
        logger.error(f"Failed to parse dev stats: {e}")



def parse_dev_stats_agg(
    system_name: str,
    show_responses: List[CliDevStats_pb2.ShowResponse],
    down_sample_interval_sec: int,
    system_dev_stats: GaugeMetricFamily,
):
    """Process ShowResponse generator by aggregating all the datapoints.
    Only supports max aggregation for now.

    Args:
        system_name: The system name
        show_responses: The ShowResponse list from the gRPC call
        down_sample_interval_sec: Apply down sampling interval, e.g., every 30s
        system_dev_stats: Metrics to post

    Yields:
        Dev stats metric object
    """

    try:
        counters = None
        datapoints = []
        for response in show_responses:
            if not counters:
                counters = response.counters
            elif counters != response.counters:
                logger.error(
                    f"Responses have different counters: "
                    f"{len(counters)} vs {len(response.counters)}"
                )
                continue
            for datapoint in response.datapoints:
                values_len = len(datapoint.values)
                float_values_len = len(datapoint.floatValues)
                if len(response.counters) != values_len + float_values_len:
                    logger.error(
                        f"Counters and values have different lengths: "
                        f"{len(response.counters)} vs {values_len + float_values_len}"
                    )
                    continue
                datapoints.append(datapoint)

        datapoints.sort(key=lambda p: p.timestamp)
        last_ts = None
        first_ts = None
        first_interval_ts = None
        rows = []
        sample_count = 0
        for idx, datapoint in enumerate(datapoints):
            last_ts = datapoint.timestamp
            if not first_ts:
                first_ts = last_ts
            if not first_interval_ts:
                first_interval_ts = last_ts
            rows.append(list(datapoint.values) + list(datapoint.floatValues))

            # Check if we have enough points for one interval.
            # The first point spans a second, so account for it with +1.
            interval_so_far = last_ts - first_interval_ts + 1
            is_last_point = idx == (len(datapoints) - 1)
            if not is_last_point and interval_so_far < down_sample_interval_sec:
                continue

            # Transpose:
            columns = [list(c) for c in zip(*rows)]
            # Reduce (max):
            aggregated_datapoints = list(map(max, columns))

            timestamp = datetime.datetime.fromtimestamp(last_ts).timestamp()
            for counter, value in zip(response.counters, aggregated_datapoints):
                if math.isnan(value):
                    continue
                system_metric_id = _parse_dev_stat_counter(counter)
                if not system_metric_id:
                    continue

                system_dev_stats.add_metric(
                    [
                        system_name,
                        system_metric_id.category,
                        system_metric_id.type_,
                        system_metric_id.component,
                        "max",
                    ],
                    value,
                    timestamp=timestamp,
                )
            sample_count += 1
            # reset interval:
            first_interval_ts = None
            rows = []
        logger.info(
            f"{system_name}: Aggregated {len(datapoints)} datapoints "
            f"({len(show_responses)} responses), "
            f"generating {sample_count} samples for each of {len(counters)} counters, "
            f"first_ts: {datetime.datetime.fromtimestamp(first_ts) if first_ts else ''}, "
            f"last_ts: {datetime.datetime.fromtimestamp(last_ts) if last_ts else ''}"
        )
    except Exception as e:
        logger.error(f"Failed to parse/aggregate dev stats: {e}")


def parse_redfish_metrics(
    system_name: str,
    redfish_net_stat: redfish_fetcher.SystemRedfishNetStats,
    redfish_metric_source: CliDevStats_pb2.ShowResponse,
) -> None:
    """Process redfish metrics from a ShowResponse.

    Args:
        system_name: The system name
        redfish_net_stat: The redfish net stats object
        redfish_metric_source: The ShowResponse object that's used to
            extract redfish metrics.
    """

    def translate_component_to_port(component: str) -> Optional[str]:
        # Components look like "DATA_NETWORK_IF_5", ports look like "n04"
        if not component.startswith(REDFISH_METRIC_PREFIX):
            return None
        match = re.search(r"(\d+)$", component)
        if not match:
            return None
        port_num = int(match.group(1)) - 1
        return f"n{port_num:02d}"

    try:
        if not redfish_metric_source or not redfish_metric_source.datapoints:
            return
        # Create a mapping between dev stat ids to redfish metric ids
        type_mapping = {metric.name: metric for metric in redfish_net_stat.metrics}
        datapoint = redfish_metric_source.datapoints[0]
        values_len = len(datapoint.values)
        float_values_len = len(datapoint.floatValues)
        if len(redfish_metric_source.counters) != values_len + float_values_len:
            logger.error(
                "Counters and values have different lengths: "
                f"{len(redfish_metric_source.counters)} vs "
                f"{values_len + float_values_len}"
            )
            return

        for i, counter in enumerate(redfish_metric_source.counters):
            counter_id = _parse_dev_stat_counter(counter)
            if not counter_id or counter_id.category != REDFISH_METRIC_CATEGORY:
                continue
            if counter_id.type_ in type_mapping:
                translated_port = translate_component_to_port(counter_id.component)
                if not translated_port:
                    continue

                if i < values_len:
                    metric_value = datapoint.values[i]
                else:
                    metric_value = datapoint.floatValues[i - values_len]
                type_mapping[counter_id.type_].metric_family.add_metric(
                    [system_name, translated_port],
                    metric_value,
                    timestamp=datapoint.timestamp,
                )
    except Exception as e:
        logger.error(f"Failed to parse redfish metrics: {e}")


def _parse_dev_stat_counter(counter: str) -> Optional[SystemMetricIdentifier]:
    """Process a counter string and return the category, component, and type.

    Counter format example: Sensor.flow_rate:NONE:INLET_FLOW_RATE.
    In this case, the category is Sensor, the component is INLET_FLOW_RATE,
    and the type is flow_rate.

    Args:
       counter: The counter string

    Returns:
       The SystemMetricIdentifier object containing the category, component,
           and type of the metric.
    """
    try:
        parts = counter.split(".")
        category = parts[0]
        subparts = parts[1].split(":")
        type_ = subparts[0]
        component = subparts[2]
        return SystemMetricIdentifier(category, type_, component)
    except IndexError as e:
        logger.error(f"Error parsing counter: {counter}, {e}")
    except AttributeError as e:
        logger.error(f"Error parsing counter: {counter}, {e}")
    return None


async def ssh_run_command(user: str, password: str, host: str, command: str) -> str:
    """
    Asynchronously run a command over SSH using asyncssh and return the output.
    """
    conn: Optional[asyncssh.SSHClientConnection] = None
    accounts = [ (user, password), (LEGACY_USER, LEGACY_PASSWORD) ]
    for account in accounts:
        try:
            conn = await asyncio.wait_for(
                asyncssh.connect(
                    host,
                    username=account[0],
                    password=account[1],
                    known_hosts=None,
                    options=asyncssh.SSHClientConnectionOptions(
                        connect_timeout=3,
                        preferred_auth="password",
                    ),
                ),
                timeout=5 # Total timeout for DNS resolution + connection establishment
            )
            result = await conn.run(command, timeout=5)
            return result.stdout
        except asyncio.TimeoutError:
            logger.error(f"Connection to {host} timed out (includes DNS resolution)")
        except asyncssh.PermissionDenied:
            logger.warning(f"Failed to login to {host} with user {account[0]}.")
        except (asyncssh.Error, OSError) as e:
            raise Exception(f"SSH command for {host} failed: {e}")
        finally:
            if conn:
                conn.close()
                await conn.wait_closed()
