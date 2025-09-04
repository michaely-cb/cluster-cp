import os
import subprocess
import sys
import logging

rootLogger = logging.getLogger(__name__)
logger = logging.getLogger("stdout_logger")

class CephSyncer:
    """
    Encapsulates mounting-and-rsync logic for multiple CephFS (group, subvolume) pairs.

    volumes: list of tuples [(group_name, subvolume_name), ...]
    unmount_script: optional path to unmount script (defaults to direct umount if not provided)
    cleanup_mounts: whether to unmount filesystems after sync (default: True)

    Usage:
        syncer = CephSyncer(
            mount_script="/path/to/mount_ceph.sh",
            sync_script="/path/to/sync_volume.sh",
            unmount_script="/path/to/unmount_ceph.sh",  # optional
            cleanup_mounts=True,  # optional
            ...
        )
        syncer.run()
    """

    def __init__(
        self,
        mount_script: str,
        sync_script: str,
        src_conf: str,
        src_keyring: str,
        dst_conf: str,
        dst_keyring: str,
        volumes: list[tuple[str, str]],
        unmount_script: str = None,
        cleanup_mounts: bool = True,
        src_mnt: str = "/mnt/src_ceph",
        dst_mnt: str = "/mnt/dst_ceph",
    ):
        self.mount_script = mount_script
        self.sync_script = sync_script
        self.unmount_script = unmount_script
        self.cleanup_mounts = cleanup_mounts
        self.src_conf = src_conf
        self.src_keyring = src_keyring
        self.dst_conf = dst_conf
        self.dst_keyring = dst_keyring
        self.volumes = volumes
        self.src_mnt = src_mnt
        self.dst_mnt = dst_mnt


    def _run_shell_cmd(self, cmd_list):
        try:
            logger.info(f"🔧 Running shell command: {' '.join(cmd_list)}")
            result = subprocess.run(
                cmd_list,
                check=True,
                capture_output=True,
                text=True,
            )
            logger.debug(f"stdout: {result.stdout.strip()}")
            logger.debug(f"stderr: {result.stderr.strip()}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {' '.join(e.cmd)}")
            logger.error(f"Return code: {e.returncode}")
            logger.error(f"stdout: {e.stdout.strip()}")
            logger.error(f"stderr: {e.stderr.strip()}")
            raise


    def _mount_ceph(self, conf: str, keyring: str, mount_point: str) -> None:
        """
        Idempotently mount a CephFS share at mount_point.
        If already mounted, unmount first.
        """
        os.makedirs(mount_point, exist_ok=True)

        # If already mounted, unmount
        if subprocess.run(["mountpoint", "-q", mount_point]).returncode == 0:
            logger.info("%s already mounted; unmounting first", mount_point)
            subprocess.run(["umount", mount_point], check=True)

        cmd = [self.mount_script, conf, keyring, mount_point]
        self._run_shell_cmd(cmd)
        logger.info("Mounted CephFS %s → %s", conf, mount_point)

    def _unmount_ceph(self, mount_point: str) -> None:
        """
        Safely unmount a CephFS share at mount_point using the unmount script.
        If unmount script is not available, fall back to direct umount command.
        """
        try:
            if self.unmount_script and os.path.isfile(self.unmount_script):
                cmd = [self.unmount_script, mount_point]
                self._run_shell_cmd(cmd)
                logger.info("Unmounted CephFS from %s using script", mount_point)
            else:
                # Fallback to direct umount if script not available
                if subprocess.run(["mountpoint", "-q", mount_point]).returncode == 0:
                    subprocess.run(["umount", mount_point], check=True)
                    logger.info("Unmounted CephFS from %s using direct umount", mount_point)
                else:
                    logger.debug("Mount point %s was not mounted", mount_point)
        except subprocess.CalledProcessError as e:
            logger.warning("Failed to unmount %s: %s", mount_point, e)
            # Don't raise - unmount failure shouldn't break the sync operation
        except Exception as e:
            logger.warning("Unexpected error unmounting %s: %s", mount_point, e)

    def _rsync_volume(self, group: str, subvol: str) -> None:
        """
        Rsync one (group, subvolume) from src_mnt to dst_mnt.
        The helper script finds the UUID directory under /volumes/<group>/<subvol>.
        """
        src_path = os.path.join(self.src_mnt, "volumes", group, subvol)
        if not os.path.isdir(src_path):
            logger.warning("Source path not found, skipping: %s", src_path)
            return

        logger.info("Syncing group=%s, subvolume=%s", group, subvol)
        cmd = [self.sync_script, group, subvol, self.src_mnt, self.dst_mnt]
        self._run_shell_cmd(cmd)

    def run(self) -> None:
        """
        1) Mount source CephFS
        2) Mount destination CephFS
        3) For each (group, subvol) in self.volumes, perform rsync
        """
        # 1) Mount source
        self._mount_ceph(self.src_conf, self.src_keyring, self.src_mnt)
        # 2) Mount destination
        self._mount_ceph(self.dst_conf, self.dst_keyring, self.dst_mnt)

        # 3) Rsync each (group, subvolume)
        for group, subvol in self.volumes:
            self._rsync_volume(group, subvol)

        logger.info("Finished syncing all specified Ceph volumes.")

        # 4) Cleanup mounts if requested
        if self.cleanup_mounts:
            logger.info("Cleaning up CephFS mounts...")
            self._unmount_ceph(self.src_mnt)
            self._unmount_ceph(self.dst_mnt)
            logger.info("Mount cleanup completed.")
