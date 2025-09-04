import logging
import os
import tarfile
import sqlite3
import tempfile
import json
from datetime import datetime
from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.tools.utils import prompt_confirm, get_version

logger = logging.getLogger(__name__)

class Create(SubCommandABC):
    """
    Create a backup of the cluster deployment files (dm.db and meta) 
    from the cluster deployment base. This command generates a backup 
    tarball that can be stored in a specified output directory 
    (specified by --output_dir) or the default backup directory 
    (/n0/cluster-deployment-backups). If the default directory is used, 
    only the latest 5 backups are retained.
    """
    name = "create"
    DEFAULT_BACKUP_DIR = "/n0/cluster-deployment-backups"

    def construct(self):
        self.parser.add_argument("--output_dir",
                                    help="Path to write backup tarball",
                                    default=None)

    def run(self, args):
        default_backup_dir = self.DEFAULT_BACKUP_DIR
        cluster_deployment_base = os.getenv("CLUSTER_DEPLOYMENT_BASE")

        if args.output_dir:
            if os.path.commonpath([cluster_deployment_base, args.output_dir]) == cluster_deployment_base:
                logger.error(f"Output directory {args.output_dir} cannot be within the cluster deployment base.")
                raise ValueError(f"Output directory {args.output_dir} cannot be within the cluster deployment base.")
            backup_dst_dir = args.output_dir
        else:
            backup_dst_dir = default_backup_dir
        
        
        # Fetch dm.db and meta from the input directory
        dm_db_path = os.path.join(cluster_deployment_base, "dm.db")
        meta_path = os.path.join(cluster_deployment_base, "meta")

        # Validate the input directory and files
        if not os.path.exists(backup_dst_dir) and args.output_dir:
            logger.error(f"Backup destination directory {backup_dst_dir} does not exist.")
            raise FileNotFoundError(f"Backup destination directory {backup_dst_dir} does not exist. Please create it or specify a valid output file path.")
        elif not os.path.exists(backup_dst_dir):
            os.makedirs(backup_dst_dir)
        
        if not os.path.exists(dm_db_path) or not os.path.exists(meta_path):
            logger.error(f"Required files 'dm.db' or 'meta' not found in {cluster_deployment_base}.")
            raise FileNotFoundError(f"Required files 'dm.db' or 'meta' not found in {cluster_deployment_base}.")
        
        # Prepare the database snapshot

        with tempfile.TemporaryDirectory() as temp_dir:
            backup_db_path = os.path.join(temp_dir, "dm.db")
            try:
                # Connect to the source database
                with sqlite3.connect(dm_db_path) as source_conn:
                    # Connect to the backup database (destination)
                    with sqlite3.connect(backup_db_path) as backup_conn:
                        # Perform the backup
                        source_conn.backup(backup_conn)
                logger.info(f"Sqlite backup completed successfully.")
            except sqlite3.Error as e:
                logger.error(f"Failed to create a backup: {e}")
                return 1

            # Tar these files and place it in the backup directory
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(backup_dst_dir, f"cluster-deployment-backup_{timestamp}.tar.gz")
            # Add metadata JSON file
            metadata = {
                "timestamp": timestamp,
                "cscfg_version": get_version(),
            }
            metadata_path = os.path.join(temp_dir, "metadata.json")
            with open(metadata_path, "w") as metadata_file:
                json.dump(metadata, metadata_file)
            try:
                with tarfile.open(output_path, "w:gz") as tar:
                    tar.add(backup_db_path, arcname="dm.db")
                    tar.add(meta_path, arcname="meta")
                    tar.add(metadata_path, arcname="metadata.json")
            except (tarfile.TarError, OSError) as e:
                logger.error(f"Failed to create tarball {output_path}: {e}")
                raise RuntimeError(f"Failed to create tarball {output_path}: {e}")

            logger.info(f"Creating backup from {cluster_deployment_base} to {output_path}")

        # if using the stnadard backup directory, keep only the last 5 backups
        if not args.output_dir:
            backups = sorted(os.listdir(backup_dst_dir))
            if len(backups) > 5:
                for old_backup in backups[:-5]:
                    os.remove(os.path.join(backup_dst_dir, old_backup))
                    logger.info(f"Removed older backups: {old_backup}")

        print(f"Backup created successfully: {output_path}")
                
        return 0
           


class Restore(SubCommandABC):
    """
    Restore files from a backup tarball.
    This command restores files (dm.db, meta/, and metadata.json) from a specified backup tarball to the 
    cluster-deployment base, overwriting existing files.
    """
    name = "restore"

    def construct(self):
        self.parser.add_argument("input_file",
                     help="Path to the backup tarball to restore from")
        self.parser.add_argument("-y", "--noconfirm",
                                 action="store_true",
                                 help="Bypass restore confirmation prompt")

    def run(self, args):
        # User confirmation for restore
        if not args.noconfirm:
            if not prompt_confirm("Restoring dm.db and meta files? This will overwrite existing files (y/n): "):
                logger.info("Restore canceled.")
                return 0
        
        input_file = args.input_file
        if not os.path.exists(input_file):
            logger.error(f"Backup file {input_file} does not exist.")
            raise FileNotFoundError(f"Backup file {input_file} does not exist.")
        if not tarfile.is_tarfile(input_file):
            logger.error(f"File {input_file} is not a valid tarball.")
            raise ValueError(f"File {input_file} is not a valid tarball.")
        # check if name begins with "cluster-deployment-backup_"
        if not os.path.basename(input_file).startswith("cluster-deployment-backup_"):
            logger.error(f"Backup file {input_file} is not created by the backup command")
            raise ValueError(f"Backup file {input_file} does not have the expected naming convention.")

        # Extract the tarball (with overwrite)
        try:
            with tarfile.open(input_file, "r:gz") as tar:
                required_files = {"dm.db", "metadata.json"}
                tar_files = set(tar.getnames())
                
                # Check for required files
                if not required_files.issubset(tar_files):
                    logger.error(f"Tarball {input_file} does not contain the required files: {required_files}")
                    raise ValueError(f"Tarball {input_file} does not contain the required files: {required_files}")
                
                # Check if meta directory exists (any file starting with "meta/")
                has_meta_dir = any(name.startswith("meta/") for name in tar_files)
                if not has_meta_dir:
                    logger.error(f"Tarball {input_file} does not contain the meta/ directory")
                    raise ValueError(f"Tarball {input_file} does not contain the meta/ directory")

                tar.extractall(path="/opt/cerebras/cluster-deployment")
                logger.info(f"Restored files from {input_file} to /opt/cerebras/cluster-deployment")
        except (tarfile.TarError, OSError) as e:
            logger.error(f"Failed to restore from tarball {input_file}: {e}")
            raise RuntimeError(f"Failed to restore from tarball {input_file}: {e}")
        
        print(f"Restore completed successfully from {input_file} to /opt/cerebras/cluster-deployment")
        return 0

CMDS = [Create, Restore]
HAS_MODULE = True