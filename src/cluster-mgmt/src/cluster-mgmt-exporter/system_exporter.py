"""system exporter"""
import asyncio
import collections
import datetime
import gc
import ipaddress
import json
import os
import shlex
import subprocess
from typing import (
    Generator,
    NamedTuple,
    Optional,
)

import fastapi
import prometheus_client
import psutil
import uvicorn
from prometheus_client import registry
from prometheus_client.core import GaugeMetricFamily
from starlette import concurrency, responses

import redfish_fetcher
import system_exporter_constants
from system_client.tunnel_system_client import TunneledSystemClient
from system_exporter_helper import (
    collect_overall_dev_stats, collect_specific_dev_stats, collect_system_events,
    fetch_dashboard, fetch_system_software, fetch_system_wse_config,
    parse_dev_stats, parse_dev_stats_agg, parse_redfish_metrics, parse_system_events,
)
from system_exporter_logger import logger

DEFAULT_EXPORTER_PORT = 8006
SYSTEM_GRPC_PORT = 8080
# down sample for all dev metrics except "current" metrics
DOWN_SAMPLE_INTERVAL_SEC = int(os.getenv("DOWN_SAMPLE_INTERVAL_SEC", "30"))
# scrape window length, should match probe interval
SCRAPE_INTERVAL_SEC = int(os.getenv("SCRAPE_INTERVAL_SEC", "60"))
# delay grpc scrape to avoid missing metrics that may take time to generate
DELAYED_GRPC_SCRAPE_SEC = int(os.getenv("DELAYED_GRPC_SCRAPE_SEC", "5"))

SYSTEM_CREDENTIAL_PATH = "/etc/secrets/system-credential"
SYSTEM_CREDENTIAL_EXPIRATION_SEC = 60

MAX_CONCURRENT_CONNECTIONS = int(os.getenv("MAX_CONCURRENT_CONNECTIONS", "100"))
REQUESTS_BEFORE_RESTART = int(os.getenv("REQUESTS_BEFORE_RESTART", "0"))

app = fastapi.FastAPI()


class SystemInfo(NamedTuple):
    system_name: str
    mgmt_addr: str
    ctr_addr: str


system_credential_last_check_time = datetime.datetime.min
system_credential = collections.defaultdict(str)


class SystemExporter(registry.Collector):
    def __init__(self):
        self.system_info: Optional[SystemInfo] = None
        self.time_now: Optional[datetime.datetime] = None

        _refresh_system_credentials()
        secret_user = system_credential["user"]
        secret_password = system_credential["password"]

        if secret_user and secret_password:
            self.user, self.password = secret_user, secret_password
        else:
            self.user = os.getenv("SYSTEM_LOGIN_USER")
            self.password = os.getenv("SYSTEM_LOGIN_PASSWORD")

        self.export_dev_stats: bool = (
            os.getenv("EXPORT_SYSTEM_DEV_STATS", "true").lower() == "true"
        )
        self.collected_metrics = []
        self.registry = registry.CollectorRegistry(auto_describe=True)
        self.registry.register(self)

    def __del__(self):
        self.registry.unregister(self)

    def collect(self) -> Generator[GaugeMetricFamily, None, None]:
        """Collect metrics.

        Yields:
            The metrics.
        """
        for metric in self.collected_metrics:
            yield metric
        self.collected_metrics = []

    async def collect_async(self) -> None:
        """
        Get metrics from application and refresh Prometheus metrics with
        new values.
        """
        if self.system_info is None:
            return

        system_overall_health = GaugeMetricFamily(
            "system_overall_health", "system overall health status", labels=["system"]
        )
        system_subsystem_health = GaugeMetricFamily(
            "system_subsystem_health",
            "subsystem health status",
            labels=["system", "subsystem"],
        )
        system_network_health = GaugeMetricFamily(
            "system_network_health",
            "system network interface status",
            labels=["system", "port"],
        )
        system_software_info = GaugeMetricFamily(
            "system_software_info",
            "system software info",
            labels=["system", "software", "version", "buildid", "execmode"],
        )
        system_wse_config_info = GaugeMetricFamily(
            "system_wse_config_info",
            "system wse config info",
            labels=["system", "configName", "coreFreq", "ioFreq"],
        )
        system_events = GaugeMetricFamily(
            "system_events",
            "system events",
            labels=["system", "severity", "type", "component"],
        )
        system_dev_stats = GaugeMetricFamily(
            "system_dev_stats",
            "system dev stats",
            labels=["system", "category", "type", "component", "agg"],
        )
        # to speed up job dashboard loading
        system_dev_stats_labels = GaugeMetricFamily(
            "system_dev_stats_labels",
            "system dev stats labels",
            labels=["category", "type", "component"],
        )
        system_maintenance_mode = GaugeMetricFamily(
            "system_maintenance_mode",
            "system maintenance mode status",
            labels=["system"],
        )

        redfish_metrics = redfish_fetcher.SystemRedfishNetStats(
            self.user, self.password
        )

        self.collected_metrics.extend(
            [
                system_overall_health,
                system_subsystem_health,
                system_network_health,
                system_software_info,
                system_wse_config_info,
                system_events,
                system_dev_stats,
                system_dev_stats_labels,
                system_maintenance_mode,
            ]
        )
        for m in redfish_metrics.metrics:
            self.collected_metrics.append(m.metric_family)

        logger.info(f"system info: {self.system_info}")
        # Sample output: 'systemf161', '172.28.139.224', '198.18.74.1'
        system_name, mgmt_addr, ctr_addr = self.system_info
        system_addr = system_name

        await self.collect_metrics_from_ssh(
            system_name, system_addr, mgmt_addr, ctr_addr,
            system_overall_health,
            system_subsystem_health,
            system_network_health,
            system_software_info,
            system_wse_config_info,
            system_maintenance_mode,
        )

        await self.collect_metrics_from_grpc(
            system_name, system_addr,
            redfish_metrics,
            system_events,
            system_dev_stats,
            system_dev_stats_labels,
        )

    async def collect_metrics_from_ssh(self, system_name, system_addr, mgmt_addr, ctr_addr,
                                       system_overall_health,
                                       system_subsystem_health,
                                       system_network_health,
                                       system_software_info,
                                       system_wse_config_info,
                                       system_maintenance_mode,
                                       ) -> None:
        dashboard = await fetch_dashboard(system_name, system_addr, self.user, self.password)
        if dashboard is None and mgmt_addr:
            logger.error(
                f"Cannot reach system {system_name} at {system_addr}. "
                f"Trying IP address {mgmt_addr} instead."
            )
            system_addr = mgmt_addr
            dashboard = await fetch_dashboard(system_name, system_addr, self.user, self.password)
        # exit early if dashboard query failed
        if dashboard is None:
            logger.error(f"Failed to reach system {system_name} for dashboard.")
            system_state = -1
            try:
                ipaddress.ip_address(ctr_addr)
                subprocess.check_output(shlex.split(f"ping -c 1 -W 3 {ctr_addr}"))
            except ValueError:
                logger.error(f"Skip checking non-ip: {system_name}/{ctr_addr}")
            except subprocess.CalledProcessError as e:
                logger.error(
                    f"Failed to ping system: {system_name}/{ctr_addr}, err: {e}"
                )
                system_state = -2
            finally:
                system_overall_health.add_metric([system_name], system_state)
                return

        # continue if dashboard query success
        for component, status in dashboard["subsystemHealth"].items():
            system_subsystem_health.add_metric(
                [system_name, component],
                system_exporter_constants.status_mapping.get(status, -1),
            )
        # for backwards compatible
        if "isSystemCfgStandard" in dashboard:
            is_system_cfg_standard = (
                1
                if dashboard.get("isSystemCfgStandard", "") != "NO"
                else system_exporter_constants.status_mapping["NON_STANDARD"]
            )
            system_subsystem_health.add_metric(
                [system_name, "config"], is_system_cfg_standard
            )

        # https://github.com/Cerebras/platform/blob/master/sas/adminsvc/impl/network.go#L24
        for port, status in dashboard["network"].items():
            system_network_health.add_metric(
                [system_name, port], 1 if status == "UP" else 0
            )

        system_software = await fetch_system_software(
            system_name, system_addr, self.user, self.password
        )
        if system_software:
            system_software_info.add_metric(
                [system_name,
                 # todo: remove the "software" label at rel-2.7
                 json.dumps(
                     {"product": system_software["product"],
                      "execmode": system_software["execmode"]}
                 ),
                 system_software["version"],
                 system_software["buildid"],
                 system_software["execmode"],
                 ], 1
            )

        system_wse_config = await fetch_system_wse_config(
            system_name, system_addr, self.user, self.password
        )
        if system_wse_config:
            system_wse_config_info.add_metric(
                [system_name,
                 system_wse_config["configName"],
                 system_wse_config["coreFreq"],
                 system_wse_config["ioFreq"],
                 ], 1
            )

        system_overall_health.add_metric(
            [system_name],
            system_exporter_constants.status_mapping.get(dashboard.get("systemHealth", ""), -1)
        )
        # Check if "maintenanceMode" key exists in output
        if "maintenanceMode" in dashboard and dashboard["maintenanceMode"].get("enabled"):
            metric_value = None
            enabled = dashboard["maintenanceMode"]["enabled"].upper()
            if enabled == "YES":
                metric_value = 1
            elif enabled == "NO":
                metric_value = 0
            if metric_value is not None:
                system_maintenance_mode.add_metric([system_name], metric_value)
            else:
                logger.warning(f"{system_name}: Unknown maintenance mode value: {enabled}")
        else:
            logger.warning(f"{system_name}: Maintenance mode is not supported")

    async def collect_metrics_from_grpc(self, system_name, system_addr,
                                        redfish_metrics,
                                        system_events,
                                        system_dev_stats,
                                        system_dev_stats_labels,
                                        ) -> None:
        if not self.export_dev_stats:
            logger.info("Dev stats disabled, skip all grpc calls")
            return

        # add delayed time since metrics can take time to generate at system side
        start_time = self.time_now - datetime.timedelta(
            seconds=DELAYED_GRPC_SCRAPE_SEC + SCRAPE_INTERVAL_SEC
        )
        # trim window at end to avoid duplicate data across scrapes
        # i.e. filter window is inclusive at both ends, e.g 0-60 will collect 61s data
        end_time = self.time_now - datetime.timedelta(seconds=DELAYED_GRPC_SCRAPE_SEC + 1)
        tunnel_client = TunneledSystemClient(system_name, system_addr, self.user, self.password)
        redfish_metric_source = None
        try:
            await tunnel_client.connect()
            system_grpc_client = tunnel_client.system_client
            if not system_grpc_client:
                return

            system_events_responses = []
            async for response in collect_system_events(
                system_name, system_grpc_client, start_time, end_time
            ):
                system_events_responses.append(response)
            if not system_events_responses or not system_events_responses[0].events:
                logger.info(f"{system_name}: No system events collected")
            else:
                parse_system_events(system_name, system_events_responses, system_events)

            dev_stats_responses = []
            async for response in collect_overall_dev_stats(
                system_name, system_grpc_client, DOWN_SAMPLE_INTERVAL_SEC, start_time, end_time
            ):
                dev_stats_responses.append(response)
            if not dev_stats_responses or not dev_stats_responses[0].datapoints:
                logger.warning(f"{system_name}: No dev stats collected")
            else:
                parse_dev_stats(
                    system_name, dev_stats_responses,
                    system_dev_stats, system_dev_stats_labels
                )
                redfish_metric_source = dev_stats_responses[0]

            current_gltch_responses = []
            async for response in collect_specific_dev_stats(
                system_name, system_grpc_client, [
                    "Sensor.current:NONE:*", "*:NONE:GLTCH_EXCUR*", "*:NONE:GLTCH_TRANS*"],
                start_time, end_time
            ):
                current_gltch_responses.append(response)
            if not current_gltch_responses or not current_gltch_responses[0].datapoints:
                logger.warning(
                    f"{system_name}: No current or gltch metrics collected"
                )
            else:
                parse_dev_stats_agg(
                    system_name, current_gltch_responses,
                    DOWN_SAMPLE_INTERVAL_SEC,
                    system_dev_stats
                )

            if redfish_metric_source:
                parse_redfish_metrics(
                    system_name, redfish_metrics, redfish_metric_source
                )
            else:
                await redfish_metrics.fetch_for_system(system_name, system_addr)
        except Exception as e:
            logger.error(f"Error fetching system grpc metrics for {system_name}: {e}")
        finally:
            await tunnel_client.close()


@app.get("/probe", response_model=None)
async def probe(module: str, target: str):
    logger.info(f"Received a probe request: {module}, {target}")
    # current scrape time, get it at start of scrape to ensure consistency
    time_now = datetime.datetime.utcnow()
    try:
        # requests look like
        # /probe?module=ssh&target=xs10046,172.28.97.224,198.111.77.18:9000
        system_name, mgmt_addr, ctr_addr = target.split(",")
        ctr_addr = ctr_addr.split(":")[0]
        # new instance/registry for every call
        exporter = SystemExporter()
        exporter.system_info = SystemInfo(system_name, mgmt_addr, ctr_addr)
        exporter.time_now = time_now

        try:
            await asyncio.wait_for(exporter.collect_async(),
                                   timeout=system_exporter_constants.prom_probe_timeout)
            msg = "probe took"
        except asyncio.TimeoutError:
            msg = "Returning partial metrics due to probe timeout after"

        latest_metrics = await concurrency.run_in_threadpool(
            prometheus_client.generate_latest, exporter.registry
        )
        logger.info(
            f"{system_name}: {msg} {(datetime.datetime.utcnow() - time_now).total_seconds():.2f}s"
        )
        return responses.Response(content=latest_metrics, media_type="text/plain")
    except Exception as e:
        logger.error(f"Error processing probe: {e}")
        return responses.Response(content="Internal error in system-exporter", status_code=500, media_type="text/plain")


@app.get("/memory", response_model=None)
async def memory_usage():
    gc.collect()
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return {
        "rss_MB": mem_info.rss / (1024**2),  # Resident Set Size in MB
        "vms_MB": mem_info.vms / (1024**2),  # Virtual Memory Size in MB
        "shared_MB": mem_info.shared / (1024**2),  # Shared memory in MB
        "text_MB": mem_info.text / (1024**2),  # Text memory usage in MB
        "lib_MB": mem_info.lib / (1024**2),  # Library memory usage in MB
        "data_MB": mem_info.data / (1024**2),  # Data memory usage in MB
        "dirty_MB": mem_info.dirty / (1024**2),  # Dirty memory usage in MB
    }


def _refresh_system_credentials():
    global system_credential_last_check_time
    global system_credential

    current_time = datetime.datetime.now()
    time_delta = datetime.timedelta(seconds=SYSTEM_CREDENTIAL_EXPIRATION_SEC)
    # check if system credentials have expired
    if current_time - time_delta > system_credential_last_check_time:
        system_credential_last_check_time = current_time
        secret_path = SYSTEM_CREDENTIAL_PATH
        secrets = collections.defaultdict(str)

        try:
            for filename in os.listdir(secret_path):
                file_path = os.path.join(secret_path, filename)
                if os.path.isfile(file_path):
                    with open(file_path, "r") as f:
                        secrets[filename] = f.read().strip()
            if not secrets:
                logger.warning("No secret files found")
            system_credential = secrets
        except Exception as e:
            logger.error(f"Error loading secrets: {e}")
            system_credential = collections.defaultdict(str)


if __name__ == "__main__":
    if REQUESTS_BEFORE_RESTART == 0:
        REQUESTS_BEFORE_RESTART = None

    uvicorn.run("system_exporter:app", host="0.0.0.0", port=DEFAULT_EXPORTER_PORT, limit_max_requests=REQUESTS_BEFORE_RESTART, limit_concurrency=MAX_CONCURRENT_CONNECTIONS)
