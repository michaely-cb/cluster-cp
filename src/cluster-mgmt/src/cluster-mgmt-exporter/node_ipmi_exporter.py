"""Node IPMI exporter"""
import asyncio
import logging
import os
from http.server import HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlparse, urlsplit

from prometheus_client import MetricsHandler
from prometheus_client.core import CollectorRegistry
from prometheus_client.registry import Collector
from redfish_fetcher import ServerRedfishStats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)


class NodeIPMIExporter(MetricsHandler, Collector):
    nodes = None
    user = os.getenv("NODE_IPMI_LOGIN_USER", "metrics")
    password = os.getenv("NODE_IPMI_LOGIN_PASSWORD", "metrics")
    module = "health"

    def do_GET(self):
        # init a new registry for each request to ensure each scrape only get requested target metrics
        self.registry = CollectorRegistry(auto_describe=True)
        self.registry.register(self)
        parsed_path = urlsplit(self.path)
        query = parse_qs(parsed_path.query)
        logging.info(f"query: {query}")
        params = parse_qs(urlparse(self.path).query)
        logging.info(f"params: {params}")
        if 'target' in query:
            ipmi_name = query['target'][0]
            self.nodes = [ipmi_name]

        if 'module' in query:
            self.module = query['module'][0]
        else:
            self.module = "health"

        super().do_GET()

    def collect(self):
        """
        Get metrics from application and refresh Prometheus metrics with
        new values.
        """
        redfish_metrics = ServerRedfishStats(self.user, self.password)

        def output():
            if redfish_metrics.metrics.get(self.module):
                for m in redfish_metrics.metrics[self.module].metrics.values():
                    yield m

        # this is required for collector mode's first call to get metric names for later filtering purpose
        if self.nodes is None:
            yield from output()
            return

        logging.info(f"nodes: {self.nodes}")
        for ipmi_name in self.nodes:
            node_name = ipmi_name.replace('-ipmi', '').replace('-idrac', '')
            asyncio.run(redfish_metrics.fetch_for_system(node_name, ipmi_name, self.module))
            yield from output()


class ThreadingSimpleServer(ThreadingMixIn, HTTPServer):
    pass


def main():
    exporter_port = 8007
    server = ThreadingSimpleServer(('', exporter_port), NodeIPMIExporter)
    server.serve_forever()


if __name__ == "__main__":
    main()
