import json
import pathlib
import re
import sys
import time
from typing import Optional

import click

import common
from common import ECR_URL, fix_pathlib_path
from common.context import CliCtx
from common.devinfra import DevInfraDB

from .utils import _validate_release_artifact_path

from cs_cluster import pass_ctx

import logging
logger = logging.getLogger("cs_cluster.session")
logger.setLevel(logging.INFO)
logger.propagate = True

system_count_option = click.option(
    '--system-count',
    type=click.INT,
    default=0,
    help="Number of systems the session must own.",
)
large_memory_rack_count_option = click.option(
    '--large-memory-rack-count',
    type=click.INT,
    default=0,
    help="Minimum number of racks of large memory (1TB) memoryx nodes the session must own.",
)
xlarge_memory_rack_count_option = click.option(
    '--xlarge-memory-rack-count',
    type=click.INT,
    default=0,
    help="Minimum number of racks of xlarge memory (2.3TB) memoryx nodes the session must own.",
)
parallel_compile_count_option = click.option(
    '--parallel-compile-count',
    type=click.INT,
    help="Estimated number of parallel compiles the session should minimally be capable of running. "
         "Unless being explicitly set, this value defaults to 0 if both '--system-names' and '--system-count' were not set, otherwise defaults to 1.",
)
parallel_execute_count_option = click.option(
    '--parallel-execute-count',
    type=click.INT,
    help="Estimated number of parallel training runs the session should minimally be capable of running."
         "Unless being explicitly set, this value defaults to 0 if both '--system-names' and '--system-count' were not set, otherwise defaults to 1.",
)
suppress_affinity_errors_option = click.option(
    '--suppress-affinity-errors',
    help="Ignore errors from CS port affinity requirements",
    is_flag=True, default=False,
)
workload_type_option = click.option(
    '--workload-type',
    type=click.STRING,
    help="Workload type of the session. Valid options: training, inference, sdk. If not provided, no workload type is set by default."
)


session_output_fmt_option = click.option(
    '--output', '-o',
    help="Select output format: table, wide, yaml, json",
    default="wide",
)


def common_session_create_or_update_options(f):
    f = system_count_option(f)
    f = large_memory_rack_count_option(f)
    f = xlarge_memory_rack_count_option(f)
    f = parallel_compile_count_option(f)
    f = parallel_execute_count_option(f)
    f = suppress_affinity_errors_option(f)
    f = workload_type_option(f)
    f = session_output_fmt_option(f)
    return f


session_show_unassigned_option = click.option(
    '--unassigned-only',
    help="Show only unassigned resources",
    is_flag=True, default=False,
)

wsjob_log_preferred_storage_type_option = click.option(
    '--wsjob-log-preferred-storage-type', default="local", type=click.Choice(['nfs', 'local']),
    help="The preferred storage type of wsjob logs. If the preferred storage type was not specified, "
         "the deployment script would use an internal decision tree to set the location for the logs."
)

cached_compile_preferred_storage_type_option = click.option(
    '--cached-compile-preferred-storage-type', default="local", type=click.Choice(['nfs', 'local']),
    help="The preferred storage type of compile artifacts. If the preferred storage type was not specified, "
         "the deployment script would use an internal decision tree to set the location for the artifacts."
)
release_artifact_path_option = click.option(
    '--release-artifact-path', "--rel-path",
    help="The path for release artifacts, e.g, /cb/artifacts/release-stable/rel-1.6.0/latest/components/cbcore. "
         "Cluster-server, cbcore images and usernode installer will be found under this path. "
         "If this option is not specified, then the session will only get created/updated without the software being deployed.",
    type=click.Path(path_type=pathlib.Path)
)
usernode_hosts_option = click.option(
    '--usernodes',
    type=click.STRING,
    help="Comma separated list of hosts to deploy to. Users must specify one or more usernodes during deployment."
)
usernode_port_option = click.option(
    '--usernode-port',
    default=22, type=click.INT, hidden=True,
    help="SSH Port for usernode. Should only be used in dev mode."
)
usernode_login_username_option = click.option(
    '--usernode-user',
    default="root", type=click.STRING,
    help="Username to use when connecting to the usernode. This should be a user with root privileges."
)
usernode_login_password_option = click.option(
    '--usernode-password',
    default="-", type=click.STRING,
    help="SSH password for the usernode. Required if the usernode is not configured for passwordless SSH."
)


def common_usernode_login_options(f):
    f = usernode_hosts_option(f)
    f = usernode_port_option(f)
    f = usernode_login_username_option(f)
    f = usernode_login_password_option(f)
    return f


usernode_config_only_option = click.option(
    '--usernode-config-only',
    help="Skip loading the wheels/csctl as part of the usernode installation. This is useful for deploying to colovore "
         "clusters where the wheels are available on NFS anyways.",
    is_flag=True, default=False,
)
usernode_skip_csctl_install_option = click.option(
    '--usernode-skip-csctl-install',
    help="Skips installing the default csctl executable under /usr/local/bin on user nodes. "
         "We should always install the default csctl in production environment.",
    is_flag=True, default=False,
)
skip_usernode_cleanup_option = click.option(
    '--skip-usernode-cleanup',
    help="On session delete, the corresponding namespace and so as the TLS certificate will get deleted. "
         "Ideally we should also clean up the orphaned (non-functional) certificate on the usernode(s). To "
         "ease operational pain, we do not mandate the usernode cleanup, given it may be possible that "
         "usernode deployment had yet to be done or the admins simply lost track of which usernode(s) should "
         "get cleaned up. Please use best judgement and exercise with caution.",
    is_flag=True, default=False,
)
session_name_option = click.option(
    '--session-name',
    help="Name of the session.",
    type=click.STRING, required=True,
)
volume_name_option = click.option(
    '--volume-name',
    help="Name of the volume.",
    type=click.STRING, required=True,
)
volume_server_option = click.option(
    '--server',
    help="The NFS server IP or DNS record where volume is set up.",
    type=click.STRING
)
volume_server_path_option = click.option(
    '--server-path',
    help="The path on the NFS server where volume is set up.",
    type=click.STRING
)
volume_container_path_option = click.option(
    '--container-path',
    help="The path it should mount to inside the container",
    type=click.STRING, required=True,
)
volume_read_only_option = click.option(
    '--readonly',
    help="Whether this volume should be made read-only",
    is_flag=True, default=False,
)
volume_allow_venv_option = click.option(
    '--allow-venv',
    help="Whether this volume should be allowed for venv copying",
    is_flag=True, default=False,
)
volume_use_hostpath_option = click.option(
    '--use-hostpath',
    help="Developer option for testing only. Whether we should use hostpath type for creating the volume.",
    is_flag=True, default=False, hidden=True,
)


def common_create_or_update_volume_options(f):
    f = volume_name_option(f)
    f = volume_server_option(f)
    f = volume_server_path_option(f)
    f = volume_container_path_option(f)
    f = volume_read_only_option(f)
    f = volume_allow_venv_option(f)
    f = volume_use_hostpath_option(f)
    return f


@click.command()
@click.argument('name', type=click.STRING)
@common_session_create_or_update_options
@common.system_namespace_option
@wsjob_log_preferred_storage_type_option
@cached_compile_preferred_storage_type_option
@release_artifact_path_option
@common.load_cbcore_image_option
@common_usernode_login_options
@usernode_config_only_option
@usernode_skip_csctl_install_option
@pass_ctx
def create(ctx: CliCtx, **kwargs):
    """
    API to create a session. Users can optionally provide a release artifact path
    for cluster server and usernode software deployment.
    """
    _handle_session(ctx=ctx, operation='create', **kwargs)
    rel_path = kwargs.get('release_artifact_path')
    if rel_path:
        _deploy_cluster_server(ctx, rel_path, **kwargs)
    else:
        logger.info("Skip cluster-server deployment given the release artifact path was not set.")

    if rel_path or "usernode_config_only" in kwargs:
        _deploy_usernode(ctx, rel_path, **kwargs)
    else:
        logger.info("Skip usernode deployment given the release artifact path or --config-only was not set.")


@click.command()
@session_show_unassigned_option
@session_output_fmt_option
@pass_ctx
def list(ctx: CliCtx, **kwargs):
    """
    API to list sessions.
    """
    _handle_session(ctx=ctx, operation='list', **kwargs)


@click.command()
@click.argument('name', type=click.STRING)
@session_output_fmt_option
@pass_ctx
def get(ctx: CliCtx, **kwargs):
    """
    API to get a session.
    """
    _handle_session(ctx=ctx, operation='get', **kwargs)


@click.command()
@click.argument('name', type=click.STRING)
@common_session_create_or_update_options
@common.system_namespace_option
@wsjob_log_preferred_storage_type_option
@cached_compile_preferred_storage_type_option
@release_artifact_path_option
@common_usernode_login_options
@usernode_config_only_option
@usernode_skip_csctl_install_option
@pass_ctx
def update(ctx: CliCtx, **kwargs):
    """
    API to update a session. Users can optionally provide a release artifact path
    for cluster server and usernode software deployment.
    """
    _handle_session(ctx=ctx, operation='update', **kwargs)
    rel_path = kwargs.get('release_artifact_path')
    if rel_path:
        _deploy_cluster_server(ctx, rel_path, **kwargs)
    else:
        logger.info("Skip cluster-server deployment given the release artifact path was not set.")

    if rel_path or "usernode_config_only" in kwargs:
        _deploy_usernode(ctx, rel_path, **kwargs)
    else:
        logger.info("Skip usernode deployment given the release artifact path or --config-only was not set.")


@click.command()
@click.argument('name', type=click.STRING)
@common.system_namespace_option
@release_artifact_path_option
@common_usernode_login_options
@click.option('--force', '-f', help="Force the delete", is_flag=True, required=False, default=False)
@skip_usernode_cleanup_option
@pass_ctx
def delete(ctx: CliCtx, **kwargs):
    """
    API to delete a session. On deleting, the corresponding namespace and all resources,
    including cluster server and completed jobs in that namespace, will be deleted.
    """
    # Currently we let log rotation to clean up the wsjob logs and cached compiles.
    # In the future if we need more immediate cleanup, we could integrate with the
    # storage reset script. (TODO)
    _handle_session(ctx=ctx, operation='delete', **kwargs)

    kwargs["reset_usernode_configs"] = True
    _deploy_usernode(ctx=ctx, rel_path=kwargs.get('release_artifact_path'), **kwargs)


@click.command()
@session_name_option
@common_create_or_update_volume_options
@pass_ctx
def create_volume(ctx: CliCtx, **kwargs):
    """
    API to create a volume configuration in a session.
    """
    _handle_volume(ctx=ctx, operation='create', **kwargs)


@click.command()
@session_name_option
@pass_ctx
def list_volumes(ctx: CliCtx, **kwargs):
    """
    API to list volume configurations in a session.
    """
    _handle_volume(ctx=ctx, operation='list', **kwargs)


@click.command()
@session_name_option
@volume_name_option
@pass_ctx
def test_volume(ctx: CliCtx, **kwargs):
    """
    API to test a volume configuration in a session.
    """
    _handle_volume(ctx=ctx, operation='test', **kwargs)


@click.command()
@session_name_option
@common_create_or_update_volume_options
@pass_ctx
def update_volume(ctx: CliCtx, **kwargs):
    """
    API to update a volume configuration in a session.
    """
    _handle_volume(ctx=ctx, operation='update', **kwargs)


@click.command()
@session_name_option
@volume_name_option
@pass_ctx
def delete_volume(ctx: CliCtx, **kwargs):
    """
    API to delete a volume configuration in a session.
    """
    _handle_volume(ctx=ctx, operation='delete', **kwargs)


@click.command()
@click.option('--session-name', help="Name of the session.", type=str)
@pass_ctx
def list_nodegroups(ctx: CliCtx, **kwargs):
    """
    API to list node groups of one or more sessions.
    """
    cmd = "csctl get nodegroups"
    name = kwargs.get('session_name')
    if name:
        cmd = f"csctl --namespace {name} get nodegroups"
    _xcmd(ctx, cmd)


def _handle_session(ctx: CliCtx, operation: str, **kwargs):
    """
    Manages sessions in a cluster. Each session maps to one namespace reservation in the backend.
    """
    _assert_operator_readiness(ctx)

    name = kwargs.get('name')
    system_count = kwargs.get('system_count')
    large_memory_rack_count = kwargs.get('large_memory_rack_count')
    xlarge_memory_rack_count = kwargs.get('xlarge_memory_rack_count')
    parallel_compile_count = kwargs.get('parallel_compile_count')
    parallel_execute_count = kwargs.get('parallel_execute_count')
    suppress_affinity_errors = kwargs.get('suppress_affinity_errors', False)
    workload_type = kwargs.get('workload_type')
    output_fmt = "" if not "output" in kwargs else f"-o {kwargs['output']}"
    if operation in ['create', 'update']:
        options = [output_fmt]

        if system_count:
            options.append(f'--system-count {system_count}')
        else:
            options.append('--system-count 0')

        if parallel_compile_count is None and system_count:
            parallel_compile_count = 1
        if parallel_compile_count:
            options.append(f"--parallel-compile-count {parallel_compile_count}")

        if parallel_execute_count is None and system_count:
            parallel_execute_count = 1
        if parallel_execute_count:
            options.append(f"--parallel-execute-count {parallel_execute_count}")

        if large_memory_rack_count:
            options.append(f"--large-memory-rack-count {large_memory_rack_count}")

        if xlarge_memory_rack_count:
            options.append(f"--xlarge-memory-rack-count {xlarge_memory_rack_count}")

        if suppress_affinity_errors:
            options.append('--suppress-affinity-errors')

        if workload_type:
            options.append(f'--workload-type {workload_type}')

        if operation == "update":
            options.append("--no-confirm")

        cmd = f'csctl session {operation} {name} {" ".join(options)}'

    elif operation == "list":
        cmd = f"csctl session list {output_fmt}"
        if kwargs.get("unassigned_only", False):
            cmd += " --unassigned-only"

    elif operation == "delete":
        cmd = f"csctl session {operation} {name} {output_fmt}"
        if kwargs.get("force", False):
            cmd += " --force"
    else:
        # get
        cmd = f"csctl session {operation} {name} {output_fmt}"

    # Sanity check on the usernodes input
    # For session API, we mandate users to provide a non-empty list of usernodes as an approach
    # to remind users about the isolation aspect.
    require_usernode_arg = (operation in ['create', 'update'] and kwargs.get("release_artifact_path")) or \
                           (operation == 'delete' and not kwargs.get("skip_usernode_cleanup"))
    if not kwargs.get("usernodes") and require_usernode_arg:
        msg = "Please specify at least one usernode for the session API using flag --usernodes."
        with DevInfraDB() as db:
            user_hosts = db.list_usernodes(ctx.cluster_name)
            if user_hosts:
                msg += f" Found {len(user_hosts)} user hosts: {', '.join(user_hosts)}"
        raise click.UsageError(msg)

    _xcmd(ctx, cmd)


def _handle_volume(ctx: CliCtx, operation: str, **kwargs):
    """
    Manages volumes in a cluster.
    """
    namespace = kwargs.get('session_name')

    cmd = f"kubectl get ns {namespace}"
    rv, _, se = ctx.cluster_ctr.exec(cmd)
    if rv != 0:
        logger.error(f"Failed to get session '{namespace}': {se}")
        logger.error("Please create the session first prior to invoking the volume APIs.")
        sys.exit(rv)

    volume_name = kwargs.get('volume_name')
    server = kwargs.get('server')
    server_path = kwargs.get('server_path')
    container_path = kwargs.get('container_path')
    readonly = kwargs.get('readonly', False)
    allow_venv = kwargs.get('allow_venv', False)
    if operation in ['create', 'update']:
        if not kwargs.get("use_hostpath"):
            if not kwargs.get("server") or not kwargs.get("server_path"):
                raise ValueError(
                    "Please specify '--server' and '--server-path' options when creating a volume."
                )
        if not kwargs.get("container_path"):
            raise ValueError(
                "Please specify the '--container-path' option when creating a volume"
            )

        cmd = (
            f"NAMESPACE={namespace} cluster-volumes.sh put {volume_name} "
            f"--container-path {container_path} "
        )
        if server:
            cmd += f"--server {server} "
        if server_path:
            cmd += f"--server-path {server_path} "
        if readonly:
            cmd += "--readonly "
        if allow_venv:
            cmd += "--allow-venv "
        if operation == 'update':
            cmd += "--force "
    elif operation == "list":
        cmd = (
            f"NAMESPACE={namespace} cluster-volumes.sh get"
        )
    else:
        cmd = (
            f"NAMESPACE={namespace} cluster-volumes.sh {operation} {volume_name}"
        )
    cmd = f"bash -c '{cmd.strip()}'"
    _xcmd(ctx, cmd)


def _deploy_cluster_server(ctx: CliCtx, rel_path: str, **kwargs):
    kwargs["namespace"] = kwargs.get("name")
    rel_path = fix_pathlib_path(rel_path)
    _validate_release_artifact_path(ctx, rel_path)

    cluster_server_image_file = None
    try:
        cluster_server_image_file = next(
            rel_path.glob("cluster-server-*.docker")
        )
        logger.info(
            f"using cluster-server image file: {cluster_server_image_file}"
        )
    except StopIteration:
        pass
    kwargs["image_file"] = cluster_server_image_file

    cbcore_image = None
    with open(f'{rel_path}/buildinfo-cbcore.json', 'r') as f:
        buildinfo = json.load(f)
        dockerids = buildinfo['dockerids']
        for dockerid in dockerids:
            if dockerid.startswith(f"{ECR_URL}/cbcore:"):
                cbcore_image = dockerid
                break
    kwargs["cbcore_image"] = cbcore_image
    kwargs["use_isolated_dashboards"] = True
    try:
        kwargs["tools_tar_file"] = next(
            rel_path.glob("cluster-mgmt-tools-*.tar.gz")
        )
        logger.info(
            f"using cluster tools tar file: {kwargs['tools_tar_file']}"
        )
    except StopIteration:
        pass

    from commands.deploy import _cluster_server
    _cluster_server(ctx=ctx, **kwargs)


def _deploy_usernode(ctx: CliCtx, rel_path, **kwargs):
    if "usernodes" not in kwargs or not kwargs.get("usernodes"):
        logger.info("Skipping usernode deploy because --usernodes was not set")
        return

    kwargs["namespace"] = kwargs.get("name")
    if rel_path:
        rel_path = fix_pathlib_path(rel_path)
        _validate_release_artifact_path(ctx, rel_path)
    kwargs["release_artifact_path"] = rel_path

    kwargs["reset_usernode_configs"] = kwargs.get("reset_usernode_configs", False)
    kwargs["skip_install_default_csctl"] = kwargs.get("skip_install_default_csctl", False)
    kwargs["enable_usernode_monitoring"] = kwargs.get("enable_usernode_monitoring", False)

    # By default we make a usernode have exclusive access to one session.
    # If integ testing has a different requirement later, we can introduce a counter option
    # to disable when raised.
    kwargs["overwrite_usernode_configs"] = True

    kwargs["hosts"] = kwargs.get("usernodes")
    kwargs["port"] = kwargs.get("usernode_port")
    kwargs["username"] = kwargs.get("usernode_user")
    kwargs["password"] = kwargs.get("usernode_password")
    kwargs["config_only"] = kwargs.get("usernode_config_only")

    from commands.deploy import _usernode
    _usernode(ctx=ctx, **kwargs)


def _assert_operator_readiness(ctx: CliCtx):
    system_ns = "job-operator"

    # Check if job operator is deployed
    cmd = f"kubectl -n {system_ns} get deploy job-operator-controller-manager -o jsonpath='{{.status.replicas}}'"
    rv, so, se = ctx.cluster_ctr.exec(cmd)
    if rv != 0 or so == "":
        logger.error(f"Please deploy job operator in the '{system_ns}' session prior to proceeding further.")
        if se:
            logger.error(se)
        sys.exit(rv or 1)

    cmd = f"kubectl -n {system_ns} get cm {system_ns}-cluster-env -o jsonpath='{{.data.DISABLE_CLUSTER_MODE}}'"
    rv, so, se = ctx.cluster_ctr.exec(cmd)
    if rv != 0 or so.lower() != "false":
        logger.error(
            f"The job operator in the '{system_ns}' is not running in cluster mode. "
            f"Please redeploy job operator by enabling cluster mode prior to proceeding further.",
        )
        if se:
            logger.error(se)
        sys.exit(rv or 1)


def _xcmd(ctx: CliCtx, cmd: str):
    cmd = cmd.strip()
    rv, so, se = ctx.cluster_ctr.exec(cmd)
    if rv != 0:
        logger.error(f"Command failed on '{ctx.cluster_name}': '{cmd}'")
        if se:
            logger.error(se)
        nsr_not_exist_pattern = re.compile('Error: namespacereservations.jobs.cerebras.com ".+" not found')
        if nsr_not_exist_pattern.match(se):
            logger.error(
                "Session does not exist. "
                "Please check existing sessions by running 'cs_cluster.py session list'.",
            )
        sys.exit(rv)
    else:
        print(so)
        if se:
            logger.error(se)
