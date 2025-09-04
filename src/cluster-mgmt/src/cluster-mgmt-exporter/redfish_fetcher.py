"""redfish exporter - http metric scraper lib"""

import asyncio
import logging
import re
from json import JSONDecodeError
from typing import Any, Dict, List, Optional, Union

import aiohttp
from prometheus_client.core import GaugeMetricFamily

REDFISH_COLLECTION_SYSTEMS_URI = "/redfish/v1/Systems"
REDFISH_COLLECTION_CHASSIS_URI = "/redfish/v1/Chassis"
REDFISH_COLLECTION_ROOT_URI = "/redfish/v1"
REDFISH_PSU_ROOT_LENOVO = "/redfish/v1/Chassis/1/PowerSubsystem/PowerSupplies"
REDFISH_PSU_ROOT_SUPERMICRO = "/redfish/v1/Chassis/1/Power"


# Attempts to fetch each URL in a list until a good response is found.
# This is useful for fetching Redfish IPMI data, since the content is similar across
# vendors, but the URL paths differ. Decodes the resulting JSON and returns it.
async def fetch_and_decode(
    url_list: List[str], target_name: str, addr: str, user: str, password: str
) -> Optional[Dict[str, Any]]:
    async with aiohttp.ClientSession() as session:
        for item in url_list:
            url = f"https://{addr}{item}"
            try:
                # ssl=False disables cert validation
                async with session.get(
                    url, auth=aiohttp.BasicAuth(user, password), timeout=30, ssl=False
                ) as response:
                    logging.info(f"redfish request: {target_name}, request_uri: {url}")
                    if response.status != 200:
                        logging.error(
                            f"failed to fetch redfish metric {url} for {target_name}, "
                            f"({addr}): status {response.status}"
                        )
                    else:
                        try:
                            content = await response.json()
                            if not isinstance(content, dict):
                                logging.error(
                                    f"error parsing result {url} for {target_name}: "
                                    f"expected JSON object, got {type(content)}"
                                )
                                return None
                            return content
                        except JSONDecodeError as e:
                            logging.error(f"error parsing result {url} for {target_name}: {e}")
                            return None
            except (TimeoutError, asyncio.TimeoutError):
                # before python 3.10, these are different exceptions.
                logging.error(f"Error getting redfish metric from {target_name}: timed out")
            except Exception as e:
                logging.error(f"Error getting redfish metric from {target_name}: {e}")
    return None


# HPE and Dell use different URL paths for the same set of data, and have the
# occasional differing field name. Support both server types without specifying
# ahead of time by querying the resource collections first, in order to find the
# URI for the specific Member (e.g., System, Thermal).
async def get_system_uri(node_name, ipmi_name, user, password) -> str:
    content = await fetch_and_decode(
        [REDFISH_COLLECTION_SYSTEMS_URI], node_name, ipmi_name, user, password
    )
    try:
        return content["Members"][0]["@odata.id"]
    except:
        logging.error(f"error parsing Systems dict for {node_name}: failed to find URI for system.")
        return None


async def get_thermal_uri(node_name, ipmi_name, user, password) -> str:
    content = await fetch_and_decode(
        [REDFISH_COLLECTION_CHASSIS_URI], node_name, ipmi_name, user, password
    )
    try:
        chassis_uri = content["Members"][0]["@odata.id"]
        try:
            content = await fetch_and_decode([chassis_uri], node_name, ipmi_name, user, password)
            try:
                thermal_uri = content["Thermal"]["@odata.id"]
                return thermal_uri
            except:
                logging.error(
                    f"error parsing Chassis dict for {node_name}: failed to find URI for thermal."
                )
                return None
        except:
            logging.error(f"error fetching Chassis dict for {node_name} at {chassis_uri}")
            return None

    except:
        logging.error(
            f"error parsing Chassis Collection dict for {node_name}: failed to find URI for chassis."
        )
        return None


class RedfishMetric:
    def __init__(self, name: str, prom_name: str, prom_desc: str, prom_dtype: type):
        self.name = name
        # use this to report flaps as int(), others as float()
        self.prom_dtype = prom_dtype
        self.metric_family = GaugeMetricFamily(prom_name, prom_desc, labels=['system', 'port'])

    async def fetch(self, system_name: str, addr: str, user: str, password: str) -> None:
        url = f"/redfish/v1/TelemetryService/MetricReports/NetworkInterface.{self.name}"
        content = await fetch_and_decode([url], system_name, addr, user, password)
        if content is None:
            return

        # api might not return metric values, use empty dict as placeholder
        for value in content.get("MetricValues", {}):
            metric_nic_id = value.get("MetricId", "")
            re_nic_id = re.findall(r"^.*DATA_NETWORK_IF_(\d+)$", metric_nic_id)
            if not re_nic_id:
                # not a DATA_NETWORK_IF. Skip.
                continue

            try:
                # map the DATA_NETWORK_IF to 0-based index for network interface
                # (i.e. n00)
                nic_id = f"n{int(re_nic_id[0]) - 1:0>2}"
                self.metric_family.add_metric(
                    [system_name, nic_id],
                    self.prom_dtype(value.get("MetricValue", 0)),
                )
            except ValueError as e:
                logging.error(e)


class SystemRedfishNetStats:
    def __init__(self, user: str, password: str) -> None:
        self.user = user
        self.password = password
        self.metrics = [
            RedfishMetric(
                "link_flap_count",
                "system_network_flap_count",
                "system network flap count",
                int,
            ),
            RedfishMetric(
                "rx_bytes",
                "system_network_rx_bytes",
                "system network RX bytes",
                float,
            ),
            RedfishMetric(
                "rx_pkt_bad_fcs",
                "system_network_rx_pkt_bad_fcs",
                "system network packets with bad FCS",
                float,
            ),
            RedfishMetric(
                "rx_fec_corr",
                "system_network_rx_fec_corrected_pkts",
                "system network packets with corrected FEC errors",
                float,
            ),
            RedfishMetric(
                "rx_fec_uncorr",
                "system_network_rx_fec_uncorrected_pkts",
                "system network packets with uncorrectable FEC errors",
                float,
            ),
            RedfishMetric(
                "rx_local_link_flt",
                "system_network_rx_local_link_flt",
                "system network local link faults",
                int,
            ),
            RedfishMetric(
                "rx_remote_link_flt",
                "system_network_rx_remote_link_flt",
                "system network remote link faults",
                int,
            ),
            RedfishMetric(
                "err_qps",
                "system_network_err_qps",
                "system network error QPs",
                int,
            ),
            RedfishMetric(
                "active_qps",
                "system_network_active_qps",
                "system network active QPs",
                int,
            ),
            RedfishMetric(
                "sqd_qps",
                "system_network_sqd_qps",
                "system network squad QPs",
                int,
            ),
            RedfishMetric(
                "rx_sequence_errors",
                "system_network_rx_sequence_errors",
                "system network RX sequence errors",
                int,
            ),
            RedfishMetric(
                "tx_sequence_errors",
                "system_network_tx_sequence_errors",
                "system network TX sequence errors",
                int,
            ),
            RedfishMetric(
                "rx_pause",
                "system_network_rx_pause",
                "system network RX pause count",
                float,
            ),
            RedfishMetric(
                "tx_pause",
                "system_network_tx_pause",
                "system network TX pause count",
                float,
            ),
            RedfishMetric(
                "ack_timeouts",
                "system_network_ack_timeouts",
                "system network ACK timeouts",
                int,
            ),
            RedfishMetric(
                "rx_bytes_sec",
                "system_network_rx_bytes_per_second",
                "system network RX bytes/s",
                float,
            ),
            RedfishMetric(
                "tx_bytes_sec",
                "system_network_tx_bytes_per_second",
                "system network TX bytes/s",
                float,
            ),
        ]

    async def fetch_for_system(self, system_name: str, addr: str) -> None:
        await asyncio.gather(
            *[m.fetch(system_name, addr, self.user, self.password) for m in self.metrics]
        )


class NodeHealth:
    status_encoding = {
        "ok": 0,
        "warn": 1,
        "warning": 1,
        "degraded": 1,
        "error": 2,
        "critical": 2,
        "unknown": 3,
    }
    thermal_metrics_to_descr = {
        'fan': 'Fan speed (pct of max for HPE, RPM for Dell)',
        'fan_thresh': 'Fan speed threshold (type label indicates criticality)',
        'thermometer': 'Node temperature at labelled sensor',
        'thermometer_thresh': 'Node temperature threshold (type label indicates criticality)',
    }

    def __init__(self) -> None:
        readings_labels = ["node", "index", "description", "health", "units"]
        threshs_labels = ["node", "index", "description", "health", "units", "type"]

        self.metrics = {
            "node_ipmi_health_status": GaugeMetricFamily(
                "node_ipmi_health_status",
                "Node health: ok=0, other values indicate warning/error",
                labels=["node", "category"],
            ),
            "node_ipmi_info": GaugeMetricFamily(
                "node_ipmi_info",
                "Node Info",
                labels=["node", "vendor", "model"],
            ),
            "node_ipmi_memory_installed": GaugeMetricFamily(
                "node_ipmi_memory_installed",
                "Node installed memory, in GiB as reported by IPMI",
                labels=["node"],
            ),
        }
        for k, v in self.thermal_metrics_to_descr.items():
            self.metrics[k] = GaugeMetricFamily(
                "node_thermals_" + k,
                v,
                labels=threshs_labels if k.endswith("_thresh") else readings_labels,
            )

    def encode_state(self, s: str) -> int:
        s = s.lower() if s else None
        return self.status_encoding.get(s, self.status_encoding["unknown"])

    async def fetch(self, node_name: str, ipmi_name: str, user: str, password: str) -> None:
        # docs say use one session for everything?
        vendor = None

        root_data = await fetch_and_decode(
            [REDFISH_COLLECTION_ROOT_URI], node_name, ipmi_name, user, password
        )
        if root_data is None:
            logging.error(f"Failed to get redfish root for {node_name}")
            return

        vendor = root_data.get('Vendor', None)
        if not vendor:
            vendor = root_data.get('Oem', {}).get('Supermicro', None)
            if vendor is None:
                vendor = "Unknown"
                logging.warning(f"Unknown vendor for node: {node_name}, assuming Dell")
            else:
                vendor = "Supermicro"

        system_uri = await get_system_uri(node_name, ipmi_name, user, password)
        if system_uri is None:
            logging.error(f"Failed to lookup the System's component name for {node_name}")
            return
        system_content = await fetch_and_decode([system_uri], node_name, ipmi_name, user, password)
        if system_content is None:
            logging.error(
                f"Failed to fetch the System component data at {system_uri} for {node_name}"
            )
            return

        model = system_content.get("Model", "")
        self.metrics["node_ipmi_info"].add_metric([node_name, vendor, model], 1)

        # overall, processor, and memory health are toplevel entries
        health_rollup = system_content.get("Status", {}).get("HealthRollup")
        health = system_content.get("Status", {}).get("Health")
        # note: supermicro doesn't have HealthRollup, so use Health
        overall_health = self.encode_state(health_rollup if health_rollup else health)
        processor = self.encode_state(
            system_content.get("ProcessorSummary", {}).get("Status", {}).get("HealthRollup")
        )
        memory = self.encode_state(
            system_content.get("MemorySummary", {}).get("Status", {}).get("HealthRollup")
        )

        ok = self.status_encoding["ok"]
        if not (overall_health == ok and processor == ok and memory == ok):
            logging.info(f"Unhealthy node: {node_name}({ipmi_name}), returned {system_content}")

        self.metrics["node_ipmi_health_status"].add_metric([node_name, "Overall"], overall_health)
        self.metrics["node_ipmi_health_status"].add_metric([node_name, "Processor"], processor)
        self.metrics["node_ipmi_health_status"].add_metric([node_name, "Memory"], memory)

        mem_installed = system_content.get("MemorySummary", {}).get("TotalSystemMemoryGiB", None)
        if mem_installed:
            self.metrics["node_ipmi_memory_installed"].add_metric([node_name], mem_installed)

        thermal_uri = await get_thermal_uri(node_name, ipmi_name, user, password)
        if thermal_uri is None:
            logging.error(f"Could not lookup the thermal_uri for {node_name}")
            return
        thermal_content = await fetch_and_decode(
            [thermal_uri], node_name, ipmi_name, user, password
        )
        if thermal_content is None:
            logging.error(f"Could not get ({thermal_uri}) for {node_name}")
            return

        # Lenovo/SM don't have System rollups for Fan/Temp health, so find those here
        fan_overall_health = self.encode_state("ok")
        for f in thermal_content.get("Fans", []):
            state = f.get("Status", {}).get("State")
            if state != "Enabled":
                logging.info(f"Node {node_name}'s fan {desc} is absent, skipping")
                continue

            # HPE/SM use Name, Dell uses FanName
            desc = f.get("FanName", "")
            if not desc:
                desc = f.get("Name", "")
            idx = f.get("MemberId", "")
            fan_health = self.encode_state(f.get("Status", {}).get("Health", None))
            fan_overall_health = max(fan_overall_health, fan_health)

            units = f.get("ReadingUnits", "")
            speed = f.get("Reading")

            noncritical_thresh = f.get("LowerThresholdNonCritical")
            critical_thresh = f.get("LowerThresholdCritical")
            if speed is not None:
                self.metrics['fan'].add_metric(
                    [node_name, idx, desc, str(fan_health), units], int(speed)
                )
                if noncritical_thresh is not None:
                    self.metrics['fan_thresh'].add_metric(
                        [node_name, idx, desc, str(fan_health), units, "warn"],
                        float(noncritical_thresh),
                    )
                if critical_thresh is not None:
                    self.metrics['fan_thresh'].add_metric(
                        [node_name, idx, desc, str(fan_health), units, "error"],
                        float(critical_thresh),
                    )

        thermal_overall_health = self.encode_state("ok")
        for t in thermal_content.get("Temperatures", []):
            state = t.get("Status", {}).get("State", None)
            if state != "Enabled":
                logging.info(f"Node {node_name}'s temp sensor {desc} is absent, skipping")
                continue

            desc = t.get("Name", "")
            idx = t.get("MemberId", "")

            temp_health = self.encode_state(t.get("Status", {}).get("Health", None))
            thermal_overall_health = max(thermal_overall_health, temp_health)

            units = "degreesC"
            value = t.get("ReadingCelsius")
            noncritical_thresh = t.get("UpperThresholdNonCritical")
            critical_thresh = t.get("UpperThresholdCritical")
            if value is not None:
                self.metrics['thermometer'].add_metric(
                    [node_name, idx, desc, str(temp_health), units], float(value)
                )
                if noncritical_thresh is not None:
                    self.metrics['thermometer_thresh'].add_metric(
                        [node_name, idx, desc, str(temp_health), units, "warn"],
                        float(noncritical_thresh),
                    )
                if critical_thresh is not None:
                    self.metrics['thermometer_thresh'].add_metric(
                        [node_name, idx, desc, str(temp_health), units, "error"],
                        float(critical_thresh),
                    )

        # Vendor specific section
        if vendor == "Dell":
            dell = system_content.get("Oem", {}).get("Dell", {}).get("DellSystem")
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "CPU"], self.encode_state(dell.get("CPURollupStatus"))
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Fan"], self.encode_state(dell.get("FanRollupStatus"))
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Power supply"], self.encode_state(dell.get("PSRollupStatus"))
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Storage"], self.encode_state(dell.get("StorageRollupStatus"))
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Temperatures"], self.encode_state(dell.get("TempRollupStatus"))
            )

        elif vendor == "HPE":
            hpe = system_content.get("Oem", {}).get("Hpe", {}).get("AggregateHealthStatus")
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "CPU"],
                self.encode_state(hpe.get("Processors", {}).get("Status", {}).get("Health")),
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Fan"],
                self.encode_state(hpe.get("Fans", {}).get("Status", {}).get("Health")),
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Power supply"],
                self.encode_state(hpe.get("PowerSupplies", {}).get("Status", {}).get("Health")),
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Storage"],
                self.encode_state(hpe.get("Storage", {}).get("Status", {}).get("Health")),
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Temperatures"],
                self.encode_state(hpe.get("Temperatures", {}).get("Status", {}).get("Health")),
            )

        elif vendor == "Lenovo":
            psu_overall_health = self.encode_state("ok")

            # Get the root PowerSupplies object to find out how many PSUs we have and their names
            ps_ps_root = await fetch_and_decode(
                [REDFISH_PSU_ROOT_LENOVO], node_name, ipmi_name, user, password
            )

            psu_members = ps_ps_root.get("Members", {})
            if psu_members:
                for psu_member in psu_members:
                    psu_content = await fetch_and_decode(
                        [psu_member.get("@odata.id", "")], node_name, ipmi_name, user, password
                    )
                    if psu_content is None:
                        logging.error(
                            f"Could not get PSU odata ({psu_member.get('@odata.id', '')}) for {node_name}"
                        )
                        return

                    psu_health = self.encode_state(
                        psu_content.get("Status", {}).get("Health", None)
                    )
                    psu_overall_health = max(psu_overall_health, psu_health)
            else:
                logging.error(f"Could not get PSU ({REDFISH_PSU_ROOT_LENOVO}) for {node_name}")
                return

            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Power supply"], psu_overall_health
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Fan"], fan_overall_health
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Temperatures"], thermal_overall_health
            )

        elif vendor == "Supermicro":
            psu_overall_health = self.encode_state("ok")

            # Get the root PowerSupplies object to find out how many PSUs we have and their names
            ps_root = await fetch_and_decode(
                [REDFISH_PSU_ROOT_SUPERMICRO], node_name, ipmi_name, user, password
            )

            psus = ps_root.get("PowerSupplies", {})
            if psus:
                for psu in psus:
                    psu_health = self.encode_state(psu.get("Status", {}).get("Health", None))
                    psu_overall_health = max(psu_overall_health, psu_health)
            else:
                logging.error(
                    f"Could not get Power ({REDFISH_PSU_ROOT_SUPERMICRO}) for {node_name}"
                )
                return

            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Power supply"], psu_overall_health
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Fan"], fan_overall_health
            )
            self.metrics["node_ipmi_health_status"].add_metric(
                [node_name, "Temperatures"], thermal_overall_health
            )

        else:
            # else not Dell or HPE
            logging.info(
                f"Node {node_name}({ipmi_name}) from vendor {vendor} does not have OEM-specific health data."
            )


class ServerRedfishStats:
    def __init__(self, user: str, password: str) -> None:
        self.user = user
        self.password = password
        self.metrics: Dict[str, Union[NodeHealth]] = {"health": NodeHealth()}

    async def fetch_for_system(self, node_name: str, ipmi_name: str, module: str) -> None:
        if module in self.metrics:
            await self.metrics[module].fetch(node_name, ipmi_name, self.user, self.password)
