import dataclasses
import datetime
import os
import fcntl
import traceback
import logging
from functools import wraps
from threading import Lock
from typing import Dict

logger = logging.getLogger(__name__)

DEPLOYMENT_LOCK_DIR = os.getenv("DEPLOYMENT_LOCKS", "/var/run/lock")

LOCK_ANSIBLE = "config_ansible"
LOCK_NETWORK = "config_network"


@dataclasses.dataclass
class _LockCtx:
    fd: int
    count: int
    lock: Lock


class FileLockContext:
    """ threadsafe reentrant inter-process locking of the deployment tooling components """

    _lock_map: Dict[str, _LockCtx] = {}
    _lock_map_lock = Lock()  # xxx: use 1 lock per lock path

    def __init__(self, lock_name, lock_dir=DEPLOYMENT_LOCK_DIR):
        self.lock_name = lock_name
        self.lock_path = os.path.join(lock_dir, f"{lock_name}.lock")

    def __enter__(self):
        with FileLockContext._lock_map_lock:
            if not self.lock_path in FileLockContext._lock_map:
                FileLockContext._lock_map[self.lock_path] = _LockCtx(-1, 0, Lock())

        ctx = FileLockContext._lock_map[self.lock_path]
        with ctx.lock:
            if ctx.count > 0:
                logger.debug(f"increment lock count {self.lock_path}")
                ctx.count += 1
                return self

            ctx.fd = os.open(self.lock_path, os.O_CREAT | os.O_WRONLY, 0o600)

            try:
                fcntl.flock(ctx.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                logger.info(f"{self.lock_name} lock required but held by another cscfg process, waiting...")
                fcntl.flock(ctx.fd, fcntl.LOCK_EX)

            os.ftruncate(ctx.fd, 0)
            trace_str = "".join(traceback.format_stack())
            os.write(ctx.fd, f"ENTER {datetime.datetime.now(datetime.timezone.utc).isoformat()} PID={os.getpid()}\n".encode())
            os.write(ctx.fd, trace_str.encode())  # for debugging
            os.fsync(ctx.fd)
            logger.debug(f"acquire lock {self.lock_path}")
            ctx.count += 1

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ctx = FileLockContext._lock_map.get(self.lock_path)
        if not ctx:
            return
        with ctx.lock:
            ctx.count -= 1
            if ctx.count == 0:
                logger.debug(f"release lock {self.lock_path}")
                fcntl.flock(ctx.fd, fcntl.LOCK_UN)
                os.write(ctx.fd, f"EXIT {datetime.datetime.now(datetime.timezone.utc).isoformat()}".encode())
                os.close(ctx.fd)
            else:
                logger.debug(f"decrement lock {self.lock_path}")


def with_lock(lock_name):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with FileLockContext(lock_name):
                return func(*args, **kwargs)

        return wrapper

    return decorator
