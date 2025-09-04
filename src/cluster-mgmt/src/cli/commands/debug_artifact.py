import logging
import subprocess
import sys
from typing import Optional
from pathlib import Path

import click
from common.context import CliCtx
from cs_cluster import pass_ctx

logger = logging.getLogger("cs_cluster.debug_artifact")

@click.command()
@click.option('--namespace', '-n', required=True, help='Namespace of the wsjob')
@click.option('--job', '-j', required=True, help='Name of the wsjob')
@click.option('--path', '-p', type=click.Path(), help='Local path to save content to')
@click.option('--timeout', default=3600, help='Timeout in seconds (default: 3600)')
@pass_ctx
def download(ctx: CliCtx, namespace: str, job: str, path: Optional[str] = None, timeout: int = 3600) -> None:
    """
    Download artifacts of an appliance job from debug volume.
    """
    system_namespace = "job-operator"
    debugviz_deploy_name = "debugviz-server"

    # Check if deployment exists and has rsync-container
    check_deploy_cmd = f"kubectl -n {system_namespace} get deploy {debugviz_deploy_name} -o jsonpath='{{.spec.template.spec.containers[*].name}}'"
    rv, containers_output, se = ctx.cluster_ctr.exec(check_deploy_cmd)
    if rv != 0:
        raise click.ClickException(f"Failed to get deployment {debugviz_deploy_name}: {se}")
    
    # Check if rsync-container exists in the deployment
    containers = containers_output.split()
    if "rsync-container" not in containers:
        raise click.ClickException(
            f"Deployment {debugviz_deploy_name} does not have 'rsync-container'. "
            f"Please upgrade debugviz-server to the latest version."
        )
    logger.info(f"Found rsync-container in deployment {debugviz_deploy_name}")

    # Verify artifact path exists
    artifact_path = f"{namespace}/{job}"
    cmd = f"kubectl -n {system_namespace} exec deploy/{debugviz_deploy_name} -c rsync-container -- ls {artifact_path}"
    rv, _, se = ctx.cluster_ctr.exec(cmd)
    if rv != 0:
        raise click.ClickException(f"Failed to verify artifact path {artifact_path}: {se}")
    
    if path is None:
        path = f"/cb/swdebug-cold/debug-artifacts/{job}"
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)

    deploy_node = ctx.cluster_ctr._deploy_node
    username = ctx.cluster_ctr._username
    password = ctx.cluster_ctr._password

    ssh_cmd = f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {username}@{deploy_node} kubectl -n {system_namespace} exec -i deploy/{debugviz_deploy_name} -c rsync-container --"
    rsync_cmd = [
        "rsync", "-av", "--partial", "--partial-dir=.rsync-partial", "--stats",
        "-e", ssh_cmd, f":{artifact_path}/", f"{path}/"
    ]
    logger.debug(f"Rsync command: {rsync_cmd}")
    if password:
        rsync_cmd = ["sshpass", "-p", password] + rsync_cmd
    logger.info(f"Starting download from {artifact_path}")
    
    try:
        result = subprocess.run(
            rsync_cmd,
            timeout=timeout,
            capture_output=False,  # Let output go directly to console
            text=True
        )
        if result.returncode != 0:
            raise click.ClickException(f"Download failed with return code: {result.returncode}")

        logger.info(f"Successfully downloaded content to {path.absolute()}")
    except subprocess.TimeoutExpired:
        raise click.ClickException(f"Download timed out after {timeout} seconds")
    except subprocess.SubprocessError as e:
        raise click.ClickException(f"Download failed: {str(e)}")
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
        if process:
            process.terminate()
        sys.exit(1)
