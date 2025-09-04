"""
Fileystem-based locking using lockf

Copied from src/infra/devinfra/common/lockfile.py since cs_cluster.py isn't
integrated with the source tree.

Example:
  lock = LockFile("mylock.txt")
  with lock:
      ...
"""

import os
import fcntl
import socket
import logging

logger = logging.getLogger(__name__)


class LockFile(object):
    """
    Provides a lockfile implementation that looks similar to threading.Lock.
    This uses lockf, and can be used over NFS.
    """

    def __init__(self, lockfile_path):
        """ :param str lockfile_path: """
        self._lockfile_path = lockfile_path
        self._lockfile_fh = None

    def __enter__(self):
        """ Enter a 'with' block """
        self.acquire()

    def __exit__(self, _exctype, _excval, _exctb):
        """ Leave a 'with' block """
        self.release()

    def acquire(self):
        """ Acquire the lock.  This is a blocking call.  """
        if self._lockfile_fh is not None:
            raise OSError("Already locked; this would deadlock")
        self._lockfile_fh = _acquire_file_lock(self._lockfile_path)

    def release(self):
        """ Release the lock.  (Repeated calls are a no-op.) """
        if self._lockfile_fh is not None:
            _release_file_lock(self._lockfile_fh)
            self._lockfile_fh = None


def _acquire_file_lock(lockfile_path):
    """
    This is a blocking call.
    Locks the file, returns its file handle (which should be passed to
    _release_file_lock() to release the lock).
    :param str lockfile_path: Path to the lockfile.
    """
    # Create the lock file if necessary:
    if not os.path.exists(lockfile_path):
        open(lockfile_path, 'a').close()
        os.chmod(lockfile_path, 0o777)
    # If anybody currently has the lock, let's find out who, so we
    # can log it:
    with open(lockfile_path, 'rb') as fh:
        prev_lockfile_contents = str(fh.read(),
                                     'utf-8', 'replace').strip()
        if prev_lockfile_contents:
            msg = f"Wait for lock {lockfile_path}: " + repr(prev_lockfile_contents)
            logger.debug(msg)

    # Open the lockfile:
    lockfile_fh = open(lockfile_path, 'r+')
    # Acquire the lock (may block):
    fcntl.lockf(lockfile_fh.fileno(), fcntl.LOCK_EX)
    # Write a message in the lockfile identifying who has the lock now:
    lockfile_fh.write("Locked by PID {} on {}\n".format(
        str(os.getpid()), socket.gethostname()))
    lockfile_fh.flush()
    lockfile_fh.seek(0)
    logger.debug(f"Lock {lockfile_path} acquired.")
    return lockfile_fh


def _release_file_lock(lockfile_fh):
    """
    Releases the lock file and closes the file handle.
    Due to NFS connection issues, the lock might have already been revoked by
    NFS server, in which case any I/O to file causes OSError. This scenario is
    handled gracefully, but what it means is that the content of the lock file
    can't be fully trusted, which is not a big deal since the content is for
    logging purposes anyways.

    :param file lockfile_fh: The file handle to release the lock on.
    """
    try:
        lockfile_fh.seek(0)
        lockfile_fh.truncate()
    except OSError:
        logger.debug(
            "Failed to modify file! File content may be stale. This can happen "
            "in the case of lost locks due to NFS connection issues."
        )
    finally:
        fcntl.lockf(lockfile_fh.fileno(), fcntl.LOCK_UN)
        lockfile_fh.close()

