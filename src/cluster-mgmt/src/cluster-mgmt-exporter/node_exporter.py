"""node exporter"""
import datetime
import json
import logging
import threading
import time
import os
import re
import pathlib
import shutil
import socket
import subprocess
import yaml

from kubernetes import client, config
from typing import Iterable
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY, Metric, Histogram
from prometheus_client.registry import Collector
from pygtail import Pygtail
from pyNfsClient import Portmap, Mount, MNT3_OK, MOUNTSTAT3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)

VERSION_LABEL = "cerebras/platform-version"


class NodeExporter(Collector):
    def __init__(self, node_name):
        self.node_name = node_name
        self.is_cp_node = False
        self.is_worker = False

    def collect(self) -> Iterable[Metric]:
        node_healthcheck_error = GaugeMetricFamily("node_healthcheck_error",
                                                   "node healthcheck errors",
                                                   labels=['node', 'error_type', 'message'])
        node_nfs_server_health = GaugeMetricFamily("node_nfs_server_health",
                                                   "nfs volume mount connection health per node",
                                                   labels=['node', 'volume'])
        platform_metrics = {
            "node_platform_version":
                GaugeMetricFamily("node_platform_version",
                                  "node platform version",
                                  labels=['node', 'package', 'config', 'cbcore']),
            "node_cluster_platform_version_mismatch":
                GaugeMetricFamily("node_cluster_platform_version_mismatch",
                                  "cluster platform software versions per node",
                                  labels=['node', 'package', 'expected', 'installed']),
            "node_bios_version_mismatch":
                GaugeMetricFamily("node_bios_version_mismatch",
                                  "BIOS versions per node",
                                  labels=['node', 'expected', 'installed']),
            "device_roce_status_mismatch":
                GaugeMetricFamily("device_roce_status_mismatch",
                                  "RoCE configuration status per device",
                                  labels=['device', 'expected', 'configured']),
            "node_bios_config_mismatch":
                GaugeMetricFamily("node_bios_config_mismatch",
                                  "BIOS config per node",
                                  labels=['node', 'attribute', 'expected', 'configured'])
        }

        def output() -> Iterable[Metric]:
            yield node_healthcheck_error
            yield node_nfs_server_health
            for _, metric in platform_metrics.items():
                yield metric

        try:
            self.sync()
            self.check_node_version(platform_metrics)
            self.node_health_check(node_healthcheck_error)
            self.check_worker_volume_health(node_nfs_server_health)
        except Exception as e:
            logging.error(e)
        finally:
            yield from output()

    # sync cluster config in case of update
    def sync(self):
        try:
            with open("/cluster-config/clusterConfiguration.yaml", 'r') as f:
                cluster_config = yaml.safe_load(f)
                node = next((n for n in cluster_config['nodes'] if n['name'] == self.node_name), None)
                if node:
                    self.is_cp_node = node.get("properties", {}).get("controlplane", False)
                    self.is_worker = node.get("role", "") in ["worker", "any"]
        except Exception as e:
            logging.error(f"error parsing cluster config: {e}")

    def node_health_check(self, node_healthcheck_error):
        default_cmd = f"bash /node-health-check/node-health-check.sh"
        cmd = os.environ.get("NODE_CHECK_CMD", default_cmd)
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(
            f"node health check: {result.stdout.decode('utf-8').strip()} {result.stderr.decode('utf-8').strip()}")
        if result.returncode == 0:
            try:
                error_file = os.environ.get("ERROR_OUTPUT_FILE", "/errors.json")
                with open(error_file, 'r') as f:
                    errors = json.load(f)
                    for error in errors:
                        node_healthcheck_error.add_metric([self.node_name, error["error_type"], error["message"]], 1)
            except Exception as e:
                logging.error(f"error parsing errors.json: {e}")

    # todo: should be refined based on session assignment and alert
    def check_worker_volume_health(self, node_nfs_server_health):
        if not self.is_worker:
            return

        volume_configs = []
        for name in os.listdir("/cluster-volumes"):
            file_path = os.path.join("/cluster-volumes", name)
            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    try:
                        volume_config = json.loads(file.read())
                        volume_configs.append((name, volume_config))
                    except Exception as e:
                        logging.error(f"error parsing volume config from {file_path}: {e}")

        server_to_mount_paths = {}
        for volume, cfg in volume_configs:
            if cfg["type"] != "nfs":
                continue
            host = cfg["server"]
            mount_path = cfg["serverPath"]
            if host not in server_to_mount_paths:
                server_to_mount_paths[host] = []
            server_to_mount_paths[host].append((volume, mount_path))
        for server, volumes in server_to_mount_paths.items():
            # check mounts from one server together to save connections
            nfs_health = is_nfs_mount_healthy(server, volumes)
            for v, _ in volumes:
                node_nfs_server_health.add_metric([self.node_name, v], 1 if nfs_health.get(v, False) else 0)

    def check_node_version(self, platform_metrics):
        self.check_platform_version(platform_metrics)
        version_file_path = os.environ.get("VERSION_FP", "/opt/cerebras/platform-version.json")
        marker_file_path = generate_marker_path(version_file_path)
        version, updated = check_version_change(version_file_path, marker_file_path)
        if version:
            # k8s labels can't contain comma
            cbcore_compatible_versions = "__".join(version["cbcore"])
            platform_metrics["node_platform_version"].add_metric(
                [self.node_name, version["package"], version["config"], cbcore_compatible_versions], 1)
            if updated and self.label_node_version(cbcore_compatible_versions):
                update_version_marker(version_file_path, marker_file_path)

    def label_node_version(self, version):
        if self.node_name == "test":
            return True
        config.load_incluster_config()
        try:
            logging.info(f"start node version update to {version}")
            v1 = client.CoreV1Api()
            node = v1.read_node(name=self.node_name)
            labels = node.metadata.labels
            if labels is None:
                labels = {}
            labels[VERSION_LABEL] = version
            body = {
                "metadata": {
                    "labels": labels
                }
            }
            # Apply the label to the node
            patched_node = v1.patch_node(name=self.node_name, body=body)
            if VERSION_LABEL in patched_node.metadata.labels and patched_node.metadata.labels[VERSION_LABEL] == version:
                return True
        except Exception as e:
            logging.exception(f"update node version label failed: {e}")
            return False

    # deprecated, todo: remove after version logic fully implemented
    def check_platform_version(self, platform_metrics):
        if not self.is_cp_node:
            return

        try:
            with open("/cluster-platform/cp_version_check.json", "r") as f:
                cluster_platform_check = json.load(f)
        except Exception as e:
            logging.exception(f"failed to read cluster platform config map: {e}")
            return

        # The config map is expected to only contain entries for
        # version mismatches. That is why a hard coded value of 1
        # is assigned to each added metric.
        for p in cluster_platform_check.get("packages", []):
            expected_version = p['expected_version'] or ""
            installed_version = p['installed_version'] or ""
            platform_metrics["node_cluster_platform_version_mismatch"].add_metric(
                [p['node'], p['package'], expected_version,
                 installed_version], 1)
        for b in cluster_platform_check.get("bios", []):
            expected_version = b['expected_version'] or ""
            installed_version = b['installed_version'] or ""
            platform_metrics["node_bios_version_mismatch"].add_metric(
                [b['node'], expected_version, installed_version], 1)
        for r in cluster_platform_check.get(
                "config", {}).get("network", {}).get("roce", []):
            expected = "Enabled" if r['expected_status'] else "Disabled"
            configured = "Enabled" if r['configured_status'] else "Disabled"
            platform_metrics["device_roce_status_mismatch"].add_metric(
                [r['device'], expected, configured], 1)
        for bc in cluster_platform_check.get(
                "config", {}).get("bios", []):
            platform_metrics["node_bios_config_mismatch"].add_metric(
                bc["device"], bc["attribute"],
                bc["expected_value"], bc["configured_value"])
        logging.info(f"checking cluster platform version complete")


def generate_marker_path(version_file_path):
    dir_name = os.path.dirname(version_file_path)
    file_name = os.path.basename(version_file_path)
    new_file_name = f".{file_name}.marker"
    marker_fp = os.path.join(dir_name, new_file_name)
    return marker_fp


def check_version_change(version_file_path, marker_file_path):
    """
    version changes detected if markers not exist or not in sync with version file
    """
    if not os.path.exists(version_file_path):
        logging.info(f"skip node version check: {version_file_path} not exist")
        return "", False
    try:
        with open(version_file_path, "r") as f:
            versions = json.load(f)
    except Exception as e:
        logging.exception(f"failed to read versions: {e}")
        return "", False

    if not os.path.exists(marker_file_path):
        logging.info(f"{marker_file_path} not exist")
        return versions, True
    try:
        with open(marker_file_path, "r") as f:
            marker_versions = json.load(f)
        return versions, marker_versions != versions
    except Exception as e:
        logging.exception(f"failed to read marker versions: {e}")
        return versions, True


def update_version_marker(version_file_path, marker_file_path):
    try:
        shutil.copy(version_file_path, marker_file_path)
    except Exception as e:
        logging.exception(f"Error update marker file: {e}")


def is_nfs_mount_healthy(host, volumes):
    """
    test worker nfs volume mount by connecting to server and accessing the serverPath
    this is for pod access only and doesn't require host mount
    """
    portmap = Portmap(host, timeout=10)
    try:
        portmap.connect()
    except socket.timeout as e:
        logging.error(f"cannot connect nfs server {host}: {e}")
        return {}

    nfs_health = {}
    try:
        # Mount initialization
        mnt_port = portmap.getport(Mount.program, Mount.program_version)
        mount = Mount(host=host, port=mnt_port, timeout=10, auth=None)
        try:
            mount.connect()
        except socket.timeout as e:
            logging.error(f"cannot connect nfs mount {host}: {e}")
            return {}

        try:
            for volume, mount_path in volumes:
                # Attempt to mount the server path
                mnt_res = mount.mnt(mount_path)
                if not mnt_res or mnt_res["status"] != MNT3_OK:
                    logging.error(f"test volume {volume} failed: mount operation failed for {host}:{mount_path}, "
                                  f"status: {MOUNTSTAT3[mnt_res['status']] if mnt_res else 'none'}")
                else:
                    logging.info(f"nfs volume {volume} {host}:{mount_path} mount is healthy")
                    nfs_health[volume] = True
        finally:
            # Disconnect mount
            mount.disconnect()
    finally:
        # Disconnect portmap
        portmap.disconnect()

    return nfs_health


# see k8s-whereabouts metrics.go for generation of this log pattern
WB_IPAM_PATTERN = re.compile(r"^\d+ (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z).* observed IPAM.*operation,status,retries,duration (\w+),(\w+),(\d+),(\d*\.\d+).*")

operation_duration_hist = Histogram(
    'whereabouts_operation_duration_seconds',
    'Duration in seconds for IPAM allocate/deallocate operations scraped from whereabouts.log',
    ['node', 'operation', 'status'],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60]
)

operation_retries_hist = Histogram(
    'whereabouts_operation_retries',
    'Number of retries for IPAM allocate/deallocate operations scraped from whereabouts.log',
    ['node', 'operation', 'status'],
    buckets=[0, 1, 2, 3, 4, 5, 10]
)

class WhereaboutsLogWatcher:
    def __init__(self, node: str, log_path: str):
        self._node = node
        self._log_path = pathlib.Path(log_path)
        self._offset_file = "/tmp/whereabouts.log.offset"
        self._logger = logging.getLogger("whereaboutsLogWatcher")
        self._last_parsed = time.time()

    def _run_once(self):
        # reads logs after last recorded offset
        for line in Pygtail(str(self._log_path.absolute()), offset_file=self._offset_file):
            try:
                m = WB_IPAM_PATTERN.match(line)
                if not m:
                    continue
                log_time_str = m.group(1)
                log_time_dt = datetime.datetime.strptime(log_time_str, "%Y-%m-%dT%H:%M:%SZ")
                if log_time_dt.timestamp() < self._last_parsed:  # skip initial logs on container restart
                    continue
                self._last_parsed = log_time_dt.timestamp()
                operation, status, retries, duration = m.group(2), m.group(3), int(m.group(4)), float(m.group(5))
                operation_duration_hist.labels(node=self._node, operation=operation, status=status).observe(duration)
                operation_retries_hist.labels(node=self._node, operation=operation, status=status).observe(retries)
            except Exception as e:
                self._logger.error(f"failed to parse line '{line}': {e}")

    def run(self):
        if not self._log_path.exists():
            self._logger.warning(f"log file {self._log_path} does not exist, waiting for file to appear...")
            while not self._log_path.exists():
                time.sleep(10)
        self._logger.info(f"log file {self._log_path} exists, tailing")
        self._last_parsed = time.time()

        while True:
            next_parse = time.time() + 10
            self._run_once()
            await_duration = next_parse - time.time()
            if await_duration > 0:
                time.sleep(await_duration)
            else:
                self._logger.warning(f"took more than 10s to scrape {self._log_file}")


def main():
    exporter_port = 8003
    node_name = os.getenv("NODE_NAME")
    exporter = NodeExporter(node_name)
    start_http_server(exporter_port)
    REGISTRY.register(exporter)

    wb_log_path = os.getenv("WB_LOG_PATH")
    if wb_log_path:
        watcher = WhereaboutsLogWatcher(node_name, wb_log_path)
        log_thread = threading.Thread(target=watcher.run, daemon=True)
        log_thread.start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
