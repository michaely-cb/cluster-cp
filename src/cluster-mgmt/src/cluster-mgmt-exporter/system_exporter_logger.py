"""system exporter"""
import logging
import os

log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
)
logger = logging.getLogger("system-exporter")
logger.setLevel(log_level)
asyncssh_logger = logging.getLogger("asyncssh")
asyncssh_logger.setLevel(logging.ERROR)
