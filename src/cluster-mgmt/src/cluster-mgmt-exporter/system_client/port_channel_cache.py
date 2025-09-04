"""Channel cache for grpc client"""
import logging
import random
import socket
from typing import (
    Optional,
    Tuple,
)

import grpc

PORT_SCAN_MAX_ATTEMPT = 100
FREE_PORT_START = 12345

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)


class PortChannelCache:
    """
    Grpc client channel connects to local port and kept in cache for reuse
    """

    def __init__(self, start_port: int, end_port: int = 65535):
        self.cached_port_channel = dict()
        self.port_pool = set(range(start_port, end_port + 1))

    def set_cached_port_channel(
        self, system_addr: str, port: int, channel: grpc.aio.Channel
    ) -> None:
        """
        Set the cached port and channel for a system address.

        Args:
            system_addr: The address used to reach system
            port: The port number to be used for the channel
            channel: The gRPC channel to be cached
        """
        self.cached_port_channel[system_addr] = (port, channel)
        logging.info(f"Size of cached_port_channel: {len(self.cached_port_channel)}")

    def get_cached_port_channel(
        self, system_addr: str, service_config: Optional[str] = None
    ) -> Optional[Tuple[int, grpc.aio.Channel]]:
        """
        Get the cached port and channel for a system address.

        Args:
            system_addr: The address used to reach the system.
            service_config: Optional gRPC service configuration.

        Returns:
            The cached port and channel for the system address.
        """
        if system_addr not in self.cached_port_channel:
            port = self.find_free_port()
            if port is None:
                logging.error("Failed to find a free port after exhausting the pool.")
                return None

            options = []
            if service_config:
                options.append(("grpc.service_config", service_config))

            channel = grpc.aio.insecure_channel(
                f"localhost:{port}", options=options
            )
            self.set_cached_port_channel(system_addr, port, channel)
        return self.cached_port_channel[system_addr]

    def remove_cached_port_channel(self, system_addr: str) -> None:
        """
        Remove the cached port and channel for a system address after channel close.

        Args:
            system_addr: The address used to reach system
        """
        if system_addr in self.cached_port_channel:
            del self.cached_port_channel[system_addr]

    def find_free_port(self, max_tries: int = PORT_SCAN_MAX_ATTEMPT) -> Optional[int]:
        """
        Find a free port from the port pool.

        Args:
            max_tries: The maximum number of tries to find a free port

        Returns:
            The free port found from the port pool
        """
        random_ports = list(self.port_pool)
        random.shuffle(random_ports)
        counter = 0
        for port in random_ports:
            if not self.check_port_in_use("localhost", port):
                self.port_pool.remove(port)
                return port
            counter += 1
            if counter >= max_tries:
                break
        return None

    @staticmethod
    def check_port_in_use(host: str, port: int) -> bool:
        """
        Check if a port is in use.

        Args:
            host: The host to check the port on
            port: The port to check

        Returns:
            True if the port is in use, False otherwise
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind((host, port))
                return False  # The port is available
            except socket.error:
                return True  # The port is already in use


# GRPC library appears to leak memory just by creating new channels.
# Need to limit the impact by caching the channels.
port_channel_cache = PortChannelCache(FREE_PORT_START)
