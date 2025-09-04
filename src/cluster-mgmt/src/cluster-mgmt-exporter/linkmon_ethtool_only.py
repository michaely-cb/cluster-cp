"""node ethtool exporter"""
import logging
import os
import time
from typing import Iterable

from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from nodeinfo import NodeInfo

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
)


class NodeEthtoolExporter(Collector):
    def __init__(self, node_name):
        self.node_name = node_name

    def collect(self) -> Iterable[Metric]:
        return NodeInfo.prom_collect(self.node_name)


def main():
    exporter_port = 8004
    node_name = os.getenv("NODE_NAME")

    exporter = NodeEthtoolExporter(node_name)
    start_http_server(exporter_port)
    REGISTRY.register(exporter)
    while True:
        time.sleep(1)  # more?


if __name__ == "__main__":
    main()
