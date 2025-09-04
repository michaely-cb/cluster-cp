import click
import logging
import hashlib
import json
import os
import time
import yaml

from common import CENTOS7_IMAGE
from common.cluster import MkdirCmd
from common.cluster import FileCopyCmd
from common.cluster import DirCopyCmd
from common.context import CliCtx
import common

logger = logging.getLogger("cs_cluster.utils")

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


def _nginx_values_resolve(ctx: CliCtx):
    def get_hostpath(values):
        return values.get('controller', {}).get('extraVolumes', [{}])[0].get('hostPath', {}).get('path', "")

    with open(f'{ctx.apps_dir}/nginx/values.yaml', 'r') as value_file:
        values = yaml.safe_load(value_file)
        remote_log_path = get_hostpath(values)
    vals = {'remote_log_path': remote_log_path}
    logger.debug(f"resolved nginxValues: {vals}")
    return vals


def _job_operator_values_resolve(ctx: CliCtx):
    remote_log_path = ""
    with open(ctx.resolve_file('job-operator/patch.yaml'), 'r') as file:
        values = yaml.safe_load(file)
        if values is not None and "volumes" in values['spec']['template']['spec']:
            remote_log_path = values['spec']['template']['spec']['volumes'][0]['hostPath']['path']
    vals = {'remote_log_path': remote_log_path}
    logger.debug(f"resolved jobOperatorValues: {vals}")
    return vals


def _cluster_server_values_resolve(ctx: CliCtx):
    with open(f'{ctx.src_dir}/src/cluster/server/charts/values.yaml', 'r') as value_file:
        base = yaml.safe_load(value_file)
    with open(ctx.resolve_file("cluster-server/values-override.yaml"), 'r') as override_file:
        values = yaml.safe_load(override_file)
    if values is None:
        values = {}
    vals = {
        "remote_log_path": values.get('serverLogHostPath', base['serverLogHostPath']),
    }

    logger.debug(f"resolved clusterServerValues: {vals}")
    return vals


def _deploy_orchestration_package(ctx: CliCtx, pkg_path, main_component, env_vars="", namespace=""):
    """
    Deploy cluster-server or job-operator with an orchestration package to the cluster.
    :param ctx: CliCtx object containing context information
    :param pkg_path: Path to the package directory
    :param main_component: The main component to deploy, e.g., "job-operator" or "cluster-server"
    :param namespace: Kubernetes namespace to deploy into
    """
    logger.info(f"deploying orchestration package in {pkg_path} to namespace {namespace}")

    if main_component == "job-operator":
        target_manifest = "manifest.job-operator.json"
    elif main_component == "cluster-server":
        target_manifest = "manifest.cluster-server.json"
    else:
        target_manifest = "manifest.json"

    ctx.cluster_ctr.exec_cmds([
        MkdirCmd(f"{ctx.remote_pkg_dir}"),
        DirCopyCmd(f"{pkg_path}", f"{ctx.remote_pkg_dir}/"),
    ])

    _cluster_status_check(ctx)
    logger.info(f"start installing")
    csadm_flags = "--debug --yes "
    if ctx.deploy_preflight_checks:
        csadm_flags += "--preflight "
    if ctx.skip_k8s:
        csadm_flags += "--skip-k8s "
    rv, sout, ser = ctx.cluster_ctr.exec(
        f"bash -c 'cd {ctx.remote_pkg_dir} && chmod a+x */*.sh && "
        f"chmod a+x ./csadm.sh && {env_vars} ./csadm.sh install {target_manifest} {csadm_flags} "
        f"--namespace={namespace}'")
    if rv != 0:
        if main_component in ["job-operator", "cluster-server"]:
            labels = {"job-operator": "-lcontrol-plane=controller-manager",
                      "cluster-server": "-lapp.kubernetes.io/name=cluster-server"}
            rv, so, se = ctx.cluster_ctr.exec(f"kubectl get no")
            logger.error(f"node status: {so}, {se}")
            rv, so, se = ctx.cluster_ctr.exec(f"kubectl describe deploy {labels[main_component]} -n{namespace}")
            logger.error(f"deploy status: {so}, {se}")
            rv, so, se = ctx.cluster_ctr.exec(f"kubectl describe po {labels[main_component]} -n{namespace}")
            logger.error(f"po status: {so}, {se}")
            rv, so, se = ctx.cluster_ctr.exec(f"kubectl logs {labels[main_component]} --tail=50 -n{namespace}")
            logger.error(f"po logs: {so}, {se}")
        logger.error(f"install {main_component} failed")
        logger.error(f"stdout: {sout}")
        logger.error(f"stderr: {ser}")
        raise click.ClickException(f"install {main_component} failed")
    logger.info(f"deployed orchestration package in {pkg_path} to namespace {namespace}")


def _deploy_with_package(ctx: CliCtx, script_path, namespace=""):
    component = script_path[0].parts[0]
    logger.info(f"copying package {ctx.pkg_dir}/{component} to remote dir:{ctx.remote_pkg_dir}/{component}")
    # handle case where parallel deploy occurs in same remote staging dir
    manifest_hash = hashlib.sha256(open(f"{ctx.pkg_dir}/manifest.json", 'rb').read()).hexdigest()[:8]
    target_manifest = f"manifest.json.{manifest_hash}"
    # Copy each package directory
    for path in script_path:
        app = path.parts[0]
        ctx.cluster_ctr.exec_cmds([
            MkdirCmd(f"{ctx.remote_pkg_dir}/{app}"),
            DirCopyCmd(f"{ctx.pkg_dir}/{app}", f"{ctx.remote_pkg_dir}/{app}"),
        ])
    # Copy the common files
    ctx.cluster_ctr.exec_cmds([
        FileCopyCmd(f"{ctx.pkg_dir}/manifest.json", f"{ctx.remote_pkg_dir}/{target_manifest}"),
        FileCopyCmd(f"{ctx.pkg_dir}/pkg-common.sh", f"{ctx.remote_pkg_dir}/pkg-common.sh"),
        MkdirCmd(f"{ctx.remote_pkg_dir}/pkg-functions"),
        DirCopyCmd(f"{ctx.pkg_dir}/pkg-functions", f"{ctx.remote_pkg_dir}/pkg-functions"),
        FileCopyCmd(f"{ctx.apps_dir}/csadm/csadm.sh", f"{ctx.remote_pkg_dir}/csadm.sh"),
        MkdirCmd(f"{ctx.remote_pkg_dir}/csadm"),
        DirCopyCmd(f"{ctx.apps_dir}/csadm/csadm", f"{ctx.remote_pkg_dir}/csadm"),
        FileCopyCmd(common.models.__file__, f"{ctx.remote_pkg_dir}/csadm/models.py"),
        FileCopyCmd(f"{ctx.resolve_file('common/internal-pkg-properties.yaml')}",
                    f"{ctx.remote_pkg_dir}/internal-pkg-properties.yaml"),
        FileCopyCmd(f"{ctx.resolve_file('common/cluster-pkg-properties.yaml.template')}",
                    f"{ctx.remote_pkg_dir}/cluster-pkg-properties.yaml.template")
    ])
    # Copy images
    if component in common.COMPONENTS_USE_SHARED_IMAGES_DIR:
        ctx.cluster_ctr.exec_cmds([
            MkdirCmd(f"{ctx.remote_pkg_dir}/{common.IMAGES_SUBPATH}"),
            DirCopyCmd(f"{ctx.pkg_dir}/{common.IMAGES_SUBPATH}", f"{ctx.remote_pkg_dir}/{common.IMAGES_SUBPATH}"),
        ])

    _cluster_status_check(ctx)
    logger.info(f"start installing")
    csadm_flags = "--debug --yes "
    if ctx.deploy_preflight_checks:
        csadm_flags += "--preflight "
    if ctx.skip_k8s:
        csadm_flags += "--skip-k8s "
    rv, sout, ser = ctx.cluster_ctr.exec(
        f"bash -c 'cd {ctx.remote_pkg_dir} && chmod a+x */*.sh && "
        f"chmod a+x ./csadm.sh && ./csadm.sh install {target_manifest} {csadm_flags} "
        f"--namespace={namespace} --component={component}'")
    if rv != 0:
        if component in ["job-operator", "cluster-server"]:
            labels = {"job-operator": "-lcontrol-plane=controller-manager",
                      "cluster-server": "-lapp.kubernetes.io/name=cluster-server"}
            rv, so, se = ctx.cluster_ctr.exec(f"kubectl get no")
            logger.error(f"node status: {so}, {se}")
            rv, so, se = ctx.cluster_ctr.exec(f"kubectl describe deploy {labels[component]} -n{namespace}")
            logger.error(f"deploy status: {so}, {se}")
            rv, so, se = ctx.cluster_ctr.exec(f"kubectl describe po {labels[component]} -n{namespace}")
            logger.error(f"po status: {so}, {se}")
            rv, so, se = ctx.cluster_ctr.exec(f"kubectl logs {labels[component]} --tail=50 -n{namespace}")
            logger.error(f"po logs: {so}, {se}")
        logger.error(f"install {component} failed")
        logger.error(f"stdout: {sout}")
        logger.error(f"stderr: {ser}")
        raise click.ClickException(f"install {component} failed")


def _cluster_name_check(ctx: CliCtx):
    """
    Return true if the given name matches with the name in cluster.yaml resided in the cluster.
    """
    cluster = ctx.cluster_cfg_k8s
    if cluster.name != ctx.cluster_name:
        logger.error(f"Name '{ctx.cluster_name}' does not match with the name inside the cluster: '{cluster.name}'.")
        raise click.ClickException(str(
            f"Name '{ctx.cluster_name}' is not the same as the name inside the cluster. "
            "This cluster might have its name changed recently and need to be reinstalled before use."
        ))


# check if cluster has disk pressure
def _cluster_has_disk_pressure(ctx: CliCtx) -> bool:
    cmd = "bash -c \"kubectl get nodes -o json " \
          "| jq '.items[] | select(.spec.taints | length > 0)' " \
          "| jq '{name:.metadata.name, taints:.spec.taints}'\""
    # best effort only
    rv, sout, ser = ctx.cluster_ctr.exec(cmd)
    if rv != 0:
        logger.warning(f"Not able to check disk pressure, skip as best effort only: {sout}, {ser}")
        return False
    if "disk-pressure" in str(sout).strip():
        logger.warning(sout)
        logger.warning("node disk pressure detected, wait for at most 5min of GC")
        return True
    return False


# check cluster health before deploy. could add more conditions if needed
def _cluster_status_check(ctx: CliCtx):
    logger.info("checking cluster status")
    waited = 0
    while waited <= 300:
        if not _cluster_has_disk_pressure(ctx):
            break
        else:
            time.sleep(10)
            waited += 10
    if waited > 300:
        raise click.ClickException(str(
            f"Failed to deploy due to disk pressure persisted for 5min+. Please contact cluster mgmt team"
        ))
    return


def _get_image_tag(
        ctx: CliCtx,
        namespace: str,
        deploy: str,
) -> str:
    jsonpath = '{.spec.template.spec.containers[0].image}'
    cmd = f"kubectl get deployment {deploy} -n {namespace} -ojsonpath='{jsonpath}' --ignore-not-found"
    _, sout, _ = ctx.cluster_ctr.must_exec(cmd)
    splits = sout.split(':')
    if len(splits) != 2:
        return ""
    return splits[1]


def _idempotence_check(
        ctx: CliCtx,
        namespace: str,
        component: str,
        deploy: str,
        image_tag: str,
        wsjob_log_preferred_storage_type: str = "",
        cached_compile_preferred_storage_type: str = "",
) -> bool:
    """
    Return true if the component in the cluster is already running with the same version.
    To return true, the image tag must be the same as the deployment running in the cluster.
    For cluster-server, the storage type also needs to be the same.
    """
    if _get_image_tag(ctx, namespace, deploy) != image_tag:
        logger.debug(f"{namespace}/{component} with image tag '{image_tag}' does not exist, continue deployment")
        return False

    # make sure that the pods are in running state, and the number of replicas is not 0.
    cmd = str(
        f"sh -c \"kubectl get deployment {deploy} -n {namespace} -ojson | "
        "jq -r 'select(.status.replicas != 0 and .status.replicas == .status.readyReplicas) | .metadata.name'\""
    )
    _, sout, _ = ctx.cluster_ctr.must_exec(cmd)
    if sout.strip() != deploy:
        logger.info(f"The replicas for deployment {deploy} are not ready: '{sout.strip()}'")
        return False

    if deploy != 'cluster-server' or (wsjob_log_preferred_storage_type == "" and cached_compile_preferred_storage_type == ""):
        logger.info(f"{namespace}/{component} with image tag '{image_tag}' already exists. skip deployment")
        return True

    # Shell is required, or the piped command would fail in docker
    cmd = str(
        f"sh -c \"helm get values {deploy} -n {namespace} -ojson | jq -r '.wsjobLogsPreferredStorageType'\""
    )
    _, previous_log_storage, _ = ctx.cluster_ctr.must_exec(cmd)
    if wsjob_log_preferred_storage_type != previous_log_storage:
        logger.info(str(
            f"{namespace}/{component} has different wsjob logs storage type '{previous_log_storage}' "
            f"than requested '{wsjob_log_preferred_storage_type}'. continue deployment"
        ))
        return False

    cmd = str(
        f"sh -c \"helm get values {deploy} -n {namespace} -ojson | jq -r '.cachedCompilePreferredStorageType'\""
    )
    _, previous_compile_storage, _ = ctx.cluster_ctr.must_exec(cmd)
    if cached_compile_preferred_storage_type != previous_compile_storage:
        logger.info(str(
            f"{namespace}/{component} has different cached compile storage type '{previous_compile_storage}' "
            f"than requested '{cached_compile_preferred_storage_type}'. continue deployment"
        ))
        return False

    logger.info(str(
        f"{namespace}/{component} has same image tag '{image_tag}', wsjob logs storage type '{previous_log_storage}' "
        f"and cached compile storage type '{previous_compile_storage}' as requested. skip deployment"
    ))
    return True


def _validate_release_artifact_path(
        ctx: CliCtx,
        release_artifact_path: str,
):
    if not release_artifact_path.is_dir():
        raise click.ClickException(f"release artifact path {release_artifact_path} is not a directory")
    if not (release_artifact_path / "buildinfo-cbcore.json").is_file():
        raise click.ClickException(
            f"release artifact path {release_artifact_path} does not contain a "
            "buildinfo-cbcore.json file, is it really a build folder?")


def _download_k8s_rpm(ctx, k8s_versions, ctr_version, rpm_dir):
    download_container = f"download-k8s-rpm-{int(time.time())}"
    download_timeout_sec = 60 * 5
    ret, std, err = ctx.exec(f'docker run --rm --name {download_container} '
                             f'-e rpm_uid={os.getuid()} -e rpm_gid={os.getgid()} '
                             f'-e k8s_versions="{k8s_versions}" '
                             f'-e ctr_version={ctr_version} '
                             f'-e rpm_dir=/k8s-rpm -v {rpm_dir}:/k8s-rpm '
                             f'-v {CURRENT_DIR}/../apps/k8s/rpm_download.sh:/rpm_download.sh '
                             f'{CENTOS7_IMAGE} timeout {download_timeout_sec} bash /rpm_download.sh')
    if ret != 0:
        logger.error(f"download k8s rpm failed: {std}, {err}")
        raise Exception("download k8s rpm failed")
