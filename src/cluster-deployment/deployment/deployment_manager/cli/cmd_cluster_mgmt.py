import argparse
import difflib
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import tarfile
import time
import typing
from typing import Optional

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import DeploymentDeviceRoles
from deployment_manager.db.models import Device
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.ssh import SSHConn
from deployment_manager.tools.utils import exec_cmd, exec_remote_cmd, ping_check, prompt_confirm
from deployment_manager.tools.kubernetes.utils import get_k8s_lead_mgmt_node

logger = logging.getLogger(__name__)

CLUSTER_CFG_PATH = pathlib.Path("/opt/cerebras/cluster/cluster.yaml")

DEPLOY_PACKAGE_PATH = pathlib.Path(os.getenv("DEPLOYMENT_PACKAGES"))
CLUSTER_PKG_PATH = pathlib.Path("/opt/cerebras/packages")


def _is_cluster_mgmt_running() -> bool:
    try:
        subprocess.check_call("kubectl get cm -n job-operator cluster".split())
    except subprocess.CalledProcessError:
        return False
    return True


def _find_pkg(for_usernode=False) -> pathlib.Path:
    """
    Returns:
        cluster-pkg.tar.gz or cluster-pkg dir when for_usernode = False
        cluster-pkg dir when for_usernode = True
    Raises:
        ValueError: if no package can be found or multiple packages were found
    """
    path_isdir = {}

    def extend_candidates(path):
        nonlocal path_isdir
        if path.is_dir():
            logger.info(f"searching {path} for cluster-pkg...")
            path_isdir.update({
                str(path): path.is_dir()
                for path in path.glob("cluster-pkg*")
                if path.is_dir() or path.name.endswith(".tar.gz")
            })

    extend_candidates(DEPLOY_PACKAGE_PATH)
    extend_candidates(CLUSTER_PKG_PATH)
    candidates = []
    # For usernode deployment, the cluster package should have been unpacked
    # and installed.  We will only look for cluster pkg directory.
    for k, is_dir in path_isdir.items():
        if for_usernode and not is_dir:
            continue
        if not is_dir and k.replace(".tar.gz", "") in path_isdir:
            continue
        candidates.append(k)

    if not candidates:
        if for_usernode:
            raise ValueError(f"unable to find cluster-pkg in {DEPLOY_PACKAGE_PATH}, {CLUSTER_PKG_PATH}. "
                             f"Please make sure a cluster pkg has been unpacked and installed.")
        else:
            raise ValueError(f"unable to find cluster-pkg in {DEPLOY_PACKAGE_PATH}, {CLUSTER_PKG_PATH}. "
                             f"Please copy the latest version of cluster-pkg-xxx.tar.gz to {CLUSTER_PKG_PATH} and retry")

    if len(candidates) == 1:
        return pathlib.Path(candidates[0])

    if for_usernode:
        raise ValueError(f"found multiple candidate cluster-pkg directories: {', '.join(candidates)}. "
                        "Please remove all but 1 cluster-pkg dir to proceed, or use --pkg-path to specify the exact one.")
    else:
        raise ValueError(f"found multiple candidate cluster-pkg files: {', '.join(candidates)}. "
                        "Please remove all but 1 cluster-pkg file/dir to proceed, or use --pkg-path to specify the exact one.")


def resolve_pkg_path(pkg_path: typing.Union[str, pathlib.Path] = "", no_confirm: bool = True) -> Optional[pathlib.Path]:
    """ Return path to unpacked cluster tarball dir or else None if user did not accept the suggested path """
    if pkg_path:
        pkg_path = pathlib.Path(pkg_path)
        if not pkg_path.exists():
            raise ValueError(f"given pkg_path {pkg_path} does not exist")
    else:
        pkg_path = _find_pkg()
        if not no_confirm and not prompt_confirm(f"Use package {pkg_path}?"):
            return None

    # if the pkg_path is a tar/tar.gz, unpack
    if not pkg_path.is_dir():
        unpacked_path = pathlib.Path(CLUSTER_PKG_PATH) / re.sub(r'(\.tar\.gz|\.tgz|\.tar)$', '', pkg_path.name)
        logger.info(f"unpacking {pkg_path} -> {unpacked_path}")
        mode = "r:gz" if pkg_path.name.endswith(".gz") else "r"
        with tarfile.open(pkg_path, mode) as tar:
            tar.extractall(CLUSTER_PKG_PATH)
        pkg_path = unpacked_path
    logger.info(f"using pkg_path={pkg_path}")
    return pkg_path


def update_cluster_config(network_conf_path: str, pkg_path: str, no_confirm: bool = True) -> bool:
    """
    Args:
        network_conf_path:
        pkg_path: path where to find csadm.sh
        no_confirm: do not prompt user for diff confirmation

    Returns: false if config update aborted by user
    """
    backup = None
    if CLUSTER_CFG_PATH.is_file():
        rv, out, _ = exec_cmd(
            "kubectl get cm cluster -n job-operator -ojsonpath='{.data.clusterConfiguration\.yaml}'",
        )
        if rv == 0:
            # ensure that the diff shown to the user is against the latest cluster config
            CLUSTER_CFG_PATH.write_text(out)
        backup = CLUSTER_CFG_PATH.with_suffix(".bak")
        shutil.copy(CLUSTER_CFG_PATH, backup)
    rv, out, err = exec_cmd(f"bash csadm.sh update-cluster-config {network_conf_path} {'' if no_confirm else '--dry-run'}", cwd=pkg_path)
    if rv != 0:
        logger.error(f"failed to generate cluster.yaml from updated network.json: {out}\n{err}")
        if no_confirm:
            # See SW-120463 for reason for this message
            logger.warning("\nIf csadm.sh failed to apply the config due to running jobs and you believe your "
                               "update is non-disruptive, you can run\n\n  kubectl create cm -n job-operator cluster "
                               "--from-file=clusterConfiguration.yaml=/opt/cerebras/cluster/cluster.yaml "
                               "--dry-run=client -oyaml | kubectl replace -f-\n\n to apply the change anyways")
        raise RuntimeError("failed to update cluster.yaml")

    if not no_confirm and backup:
        diff = "\n".join(difflib.unified_diff(backup.read_text().splitlines(),
                                              CLUSTER_CFG_PATH.read_text().splitlines(),
                                              fromfile=str(backup),
                                              tofile=str(CLUSTER_CFG_PATH)))
        has_diff = bool(diff.strip())
        if has_diff:
            print(diff)
            if not prompt_confirm("accept these changes? y/n:"):
                generated = f"/tmp/cluster.yaml.{int(time.time())}"
                shutil.copy(CLUSTER_CFG_PATH, generated)
                if backup:
                    shutil.move(backup, CLUSTER_CFG_PATH)
                print(f"old config restored, saved generated config to {generated}")
                return False
        else:
            print(f"no diff in generated cluster.yaml generated from {network_conf_path}")
    return True


def update_cluster_pkg(pkg_path: pathlib.Path,
                       network_conf_path: Optional[str],
                       manifest_path: Optional[str] = None,
                       config_update: bool = False,
                       skip_validate: bool = False,
                       skip_k8s: bool = False,
                       no_confirm: bool = True):
    """
    Run k8 pkg install on given pkg path, optionally asking for confirmation if there was a diff
    between the old and new configuration.
    Args:
        pkg_path: path where to find csadm.sh
        network_conf_path:
        manifest_path:
        config_update:
        skip_validate:
        skip_k8s:
        no_confirm: do not prompt user for diff confirmation
    """
    manifest_path = pkg_path / "manifest.json" if not manifest_path else pathlib.Path(manifest_path)
    csadm_install_cmd = (
        f"bash csadm.sh install {manifest_path} --yes"
        f" --preflight {'--skip-k8s' if skip_k8s else ''} --debug{' --validate' if not skip_validate else ''}"
    )

    if network_conf_path and config_update:
        if not no_confirm and not update_cluster_config(network_conf_path, str(pkg_path.absolute()), no_confirm=False):
            return 0
        csadm_install_cmd = f"{csadm_install_cmd} --update-config={network_conf_path}"
    elif not _is_cluster_mgmt_running():
        # need to create the cluster config on initial install
        if not update_cluster_config(network_conf_path, str(pkg_path.absolute()), no_confirm=no_confirm):
            return 0

    manifest_doc = json.loads(manifest_path.read_text())
    component_manifest = [
        (component, component.split("/")[1],)
        for component in manifest_doc["componentPaths"]
    ]
    for log in pkg_path.glob("*.log"):  # clean the logs so we can tell which component failed
        log.unlink()
    logger.info(f"installing components: {', '.join([c[0] for c in component_manifest])}")
    rv, _, err = exec_cmd(csadm_install_cmd, cwd=str(pkg_path.absolute()), stream=True)
    if rv == 0:
        logger.info("success")
        return

    raise RuntimeError(f"command '{csadm_install_cmd}' returned with code {rv}")


def apply_common_args(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--pkg-path", "-p",
        default="", required=False,
        help="Location of the cluster-pkg tarball. If not provided, "
             f"searches {DEPLOY_PACKAGE_PATH}, {CLUSTER_PKG_PATH} for the latest cluster-pkg"
    )
    parser.add_argument(
        "--no-confirm", "-y",
        help="Skip confirmation prompt",
        default=False,
        action='store_true',
    )


class PkgUpdateConfig(SubCommandABC):
    """
    Run csadm.sh update-cluster-config. Any diffs between existing and updated config will be displayed
    prior to application.
    """
    name = "update_config"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._network_conf_path = ConfGen(self.profile).network_config_file

    def construct(self):
        apply_common_args(self.parser)

    def run(self, args):
        try:
            pkg_path = resolve_pkg_path(args.pkg_path)
            should_continue = update_cluster_config(self._network_conf_path, pkg_path, no_confirm=args.no_confirm)
            if not args.no_confirm and should_continue:
                # run once more to actually apply the config to k8
                update_cluster_config(self._network_conf_path, pkg_path, no_confirm=True)
        except (RuntimeError, ValueError) as e:
            logger.error(f"failed with: {e}")
            return 1


class PkgUpdate(SubCommandABC):
    """
    Run csadm install, updating the cluster.yaml with the latest network_config.json contents.
    """
    name = "update"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._network_conf_path = ConfGen(self.profile).network_config_file

    def construct(self):
        apply_common_args(self.parser)
        self.parser.add_argument(
            "--manifest",
            default="", required=False,
            help="Path to the manifest to install. Defaults to manifest.json of the discovered package"
        )
        self.parser.add_argument(
            "--config-update",
            default=False,
            action='store_true',
            help=str(
                "Trigger a cluster config update and perform the 'incremental update' flow which "
                "only runs portions of the cluster installer in order to add nodes to the cluster. Use this option if "
                "adding/remove nodes/systems from the cluster."
            )
        )
        self.parser.add_argument(
            "--skip-k8s",
            default=False,
            action='store_true',
            help="Skip updating the k8s controlplane."
        )
        self.parser.add_argument(
            "--skip-validate",
            default=False,
            action='store_true',
            help="Skip running `csadm install` validate step"
        )

    def run(self, args):
        try:
            pkg_path = resolve_pkg_path(args.pkg_path, args.no_confirm)
            if not pkg_path:
                return 0
            update_cluster_pkg(pkg_path,
                               self._network_conf_path,
                               manifest_path=args.manifest,
                               config_update=args.config_update,
                               skip_k8s=args.config_update,
                               no_confirm=args.no_confirm)
        except (RuntimeError, ValueError) as e:
            logger.error(f"failed with: {e}")
            return 1


class DeployUserPkg(SubCommandABC):
    """
    Deploy usernode package to user nodes
    """

    name = "deploy_usernode"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def check(self, pkg_dir, usernodes):
        """Check if pkg_dir and usernodes are valid"""
        pkg_path = pathlib.Path(pkg_dir)
        pkg_name = os.path.basename(pkg_path)

        if not pkg_path.exists():
            raise ValueError(
                f"Given cluster package directory {pkg_dir} does not exist"
            )

        if not pkg_path.is_dir():
            raise ValueError(f"Given {pkg_path} is not a directory")

        if not pkg_name.startswith("cluster-pkg-"):
            raise ValueError(
                f"The cluster package directory {pkg_name} does not have the proper 'cluster-pkg-' prefix"
            )

        script = os.path.join(pkg_path, "usernode", "stage-mgmt-node.sh")
        if not pathlib.Path(script).exists():
            raise ValueError(f"Script {script} does not exist")

        id_str = pkg_name.replace("cluster-pkg-", "")
        user_pkg_path = os.path.join(
            pkg_path, "usernode", "user-pkg-" + id_str + ".tar.gz"
        )

        if not pathlib.Path(user_pkg_path):
            raise ValueError(f"Usernode package {user_pkg_path} not found")

        if len(usernodes) == 0:
            raise ValueError("No usernode specified and in the cluster inventory")

        for usernode in usernodes:
            if not ping_check(usernode):
                raise ValueError(f"Usernode {usernode} not reachable")

    def create_user_pkg(
        self, pkg_dir: str, session: str
    ) -> Optional[pathlib.Path]:
        """Create the usernode package based on the session name"""
        pkg_path = pathlib.Path(pkg_dir)
        pkg_name = os.path.basename(pkg_path)
        script = os.path.join(pkg_path, "usernode", "stage-mgmt-node.sh")

        # Create the usernode package tarball using the given session name
        env = os.environ.copy()
        if len(session):
            env["namespace"] = session
        ret, out, err = exec_cmd(cmd=script, env=env, stream=True)

        if ret != 0:
            logger.info(out)
            logger.error(err)
            raise ValueError(f"Error in creating the usernode package")

        id_str = pkg_name.replace("cluster-pkg-", "")
        user_pkg_path = os.path.join(
            pkg_path, "usernode", "user-pkg-" + id_str + ".tar.gz"
        )

        if not pathlib.Path(user_pkg_path):
            raise ValueError(f"Usernode package {user_pkg_path} not found")

        logger.info(f"Created usernode package {user_pkg_path}")
        return user_pkg_path

    def deploy_user_pkg(
        self, user_pkg: str, usernode: str, username: str, password: str
    ):
        """Deploy the usernode package to the usernode"""
        ret = exec_remote_cmd(
            "mkdir -p /opt/cerebras/packages", usernode, username, password
        )

        user_pkg_file = os.path.basename(user_pkg)

        with SSHConn(usernode, username, password) as conn:
            conn.scp_file(
                user_pkg, os.path.join("/opt/cerebras/packages", user_pkg_file)
            )

        user_pkg_name = user_pkg_file.replace(".tar.gz", "")

        cmd = f"cd /opt/cerebras/packages && tar xzf {user_pkg_file} && cd {user_pkg_name} && ./install.sh"
        ret, out, err = exec_remote_cmd(cmd, usernode, username, password)
        if ret:
            logger.error(f"Error in installing usernode package on {usernode}")
            logger.error(out)
            logger.error(err)

        return ret

    def construct(self):
        self.parser.add_argument(
            "--no-confirm",
            "-y",
            help="Skip confirmation prompt",
            default=False,
            action="store_true",
        )
        # Keep it simple for now.  The user needs to explicitly provide the
        # following options.
        self.parser.add_argument(
            "--pkg-path",
            type=str,
            required=False,
            default=argparse.SUPPRESS,
            help="Location of the cluster-pkg directory. If not provided, "
            f"searches {DEPLOY_PACKAGE_PATH}, {CLUSTER_PKG_PATH} for the latest cluster-pkg",
        )
        self.parser.add_argument(
            "--usernode",
            type=str,
            nargs="+",
            required=False,
            help="Usernode(s) to deploy the package. Default is all usernodes in the inventory.",
            default=argparse.SUPPRESS,
        )
        self.parser.add_argument(
            "--session",
            type=str,
            required=False,
            default="job-operator",
            help="Session (namespace) name",
        )
        self.parser.add_argument(
            "--username",
            type=str,
            required=False,
            default="root",
            help="Usernode login name",
        )
        self.parser.add_argument(
            "--password",
            type=str,
            required=True,
            default=argparse.SUPPRESS,
            help="Usernode login password",
        )

    def run(self, args):
        if hasattr(args, "usernode"):
            usernodes = args.usernode
        else:
            usernodes = [
                n.name
                for n in Device.get_servers(self.profile)
                if n.device_role == DeploymentDeviceRoles.USER.value
            ]
        sorted(usernodes)

        if hasattr(args, "pkg_path"):
            pkg_path = args.pkg_path
        else:
            pkg_path = str(_find_pkg(for_usernode=True))

        self.check(pkg_path, usernodes)

        print(
            f"Create usernode package from {pkg_path} with session '{args.session}' and install it on {' ,'.join(usernodes)}"
        )
        if not args.no_confirm and not prompt_confirm("Proceed?"):
            return 0

        try:
            user_pkg_path = self.create_user_pkg(pkg_path, args.session)
            if not user_pkg_path:
                return 1

            # We don't have that many usernodes, okay to deploy them serially.
            for usernode in usernodes:
                if (
                    self.deploy_user_pkg(
                        user_pkg_path, usernode, args.username, args.password
                    )
                    != 0
                ):
                    raise ValueError(
                        f"Error in deploying the usernode package {usernode}"
                    )

                logger.info(
                    f"Usernode package successfully deployed to {usernode}"
                )
            return 0

        except (RuntimeError, ValueError) as e:
            logger.error(f"failed with: {e}")
            return 1


CMDS = [
    PkgUpdate,
    PkgUpdateConfig,
    DeployUserPkg,
]
