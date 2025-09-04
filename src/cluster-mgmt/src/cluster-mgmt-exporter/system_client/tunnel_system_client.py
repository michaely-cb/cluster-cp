import json
import logging
from typing import Optional

import asyncio
import asyncssh
from system_client.port_channel_cache import port_channel_cache
from system_client.system_client import SystemClient

SYSTEM_GRPC_PORT = 8080
SSH_PORT = 22

LEGACY_USER = "admin"
LEGACY_PASSWORD = "admin"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)

RETRY_POLICY = {
    "methodConfig": [
        {
            "name": [{}],
            "retryPolicy": {
                "maxAttempts": 5,
                "initialBackoff": "2s",
                "maxBackoff": "10s",
                "backoffMultiplier": 2,
                "retryableStatusCodes": [
                    "UNAVAILABLE",
                    "UNKNOWN",
                    "RESOURCE_EXHAUSTED",
                ],
            },
        }
    ]
}

class TunneledSystemClient:
    """
    Build tunnel to redirect grpc traffic from local port to remote grpc port with ssh
    """

    def __init__(
        self,
        system_name: str,
        system_addr: str,
        user: str,
        password: str,
    ) -> None:
        self.system_name = system_name
        self.system_addr = system_addr
        self.user = user
        self.password = password
        self.system_client = None
        self.conn = None
        self.tunnel = None
        self.local_port = None
        self.channel = None

    async def connect(self, service_config: str = json.dumps(RETRY_POLICY)) -> None:
        local_port, channel = port_channel_cache.get_cached_port_channel(self.system_addr, service_config)
        self.local_port = local_port
        self.channel = channel
        try:
            conn = await self._create_ssh_connection()
            if not conn:
                logging.error(f"Failed to create SSH connection: {self.system_name}")
                return
            logging.info(
                f"{self.system_name}: Creating SSH tunnel "
                f"from local {self.local_port} to remote {SYSTEM_GRPC_PORT}"
            )
            self.conn = conn

            tunnel = await conn.forward_local_port(
                "localhost", self.local_port, "localhost", SYSTEM_GRPC_PORT
            )
            if not tunnel:
                logging.error(
                    f"Failed to create SSH tunnel for {self.system_name} "
                    "before grpc transfer."
                )
                return
            self.tunnel = tunnel

            self.system_client = SystemClient(self.channel)
        except Exception as e:
            logging.error(
                f"Failed to build tunnel for {self.system_name}: {e}"
            )
            return

    async def _create_ssh_connection(self) -> Optional[asyncssh.SSHClientConnection]:
        accounts = [ (self.user, self.password), (LEGACY_USER, LEGACY_PASSWORD) ]
        for account in accounts:
            try:
                return await asyncio.wait_for(
                    asyncssh.connect(
                        self.system_addr,
                        port=SSH_PORT,
                        username=account[0],
                        password=account[1],
                        known_hosts=None,
                    ),
                    timeout=5 # Total timeout for DNS resolution + connection establishment
                )
            except asyncio.TimeoutError:
                logging.error(f"Connection to {self.system_name} timed out (includes DNS resolution)")
            except asyncssh.PermissionDenied:
                logging.warning(f"Failed to login to {self.system_name} with user {account[0]} for the gRPC tunnel.")
            except asyncssh.Error as e:
                logging.error(f"Failed to create SSH connection for the gRPC tunnel: {e}")
            except Exception as e:
                logging.error(f"Unexpected error connecting to {self.system_name}: {e}")

        return None

    async def close(self, close_channel=False):
        try:
            if self.tunnel:
                logging.info(f"{self.system_name}: Closing tunnel on local port {self.local_port}")
                self.tunnel.close()
                await self.tunnel.wait_closed()
            if self.conn:
                self.conn.close()
                await self.conn.wait_closed()
            if close_channel:
                await self.system_client.channel.close()
                port_channel_cache.remove_cached_port_channel(self.system_addr)

        except Exception as e:
            logging.error(f"Failed to close tunnel: {e}")
