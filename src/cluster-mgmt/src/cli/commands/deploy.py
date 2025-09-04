import base64
import json
import logging
import os
import pathlib
import re
import shutil
import string
from distutils.version import StrictVersion
from typing import Optional

import click
import common
import paramiko
from common import (
    DockerImage,
    cluster_mgmt_retention_days_option,
    cluster_mgmt_rotate_at_size_option,
    cluster_mgmt_rotate_count_max_option,
    fix_pathlib_path,
    REGISTRY_URL,
)
from common.cluster import Cmd, DirCopyCmd, FileCopyCmd, FileGetCmd, MkdirCmd
from common.context import CliCtx
from common.devinfra import DevInfraDB
from common.image import (
    extract_image,
    get_image_target_nodes,
    is_image_loaded,
)
from cs_cluster import pass_ctx
from packaging import version
from packaging.version import Version

from commands.package import (
    _alert_router,
    _binary_deps,
    _ceph,
    _cerebras_internal_setup,
    _cilium,
    _cluster_tools,
    _common_images,
    _debugviz_server,
    _gather_built_wheels,
    _get_custom_images,
    _get_ecr_common_images,
    _k8s,
    _kafka,
    _kube_vip,
    _kube_webhook,
    _log_rotation,
    _log_scraping,
    _multus,
    _nginx,
    _nvme_of,
    _prometheus,
    _cpingmesh,
    _rdma_device_plugin,
    _registry,
    _registry_sync,
)
from commands.package import (
    _cluster_server as __cluster_server,
)
from commands.package import (
    _image as package_image,
)
from commands.package import (
    _job_operator as __job_operator,
)
from commands.package import (
    _usernode as __usernode,
)

from .cluster import (
    _create_kind_config,
)

from .utils import (
    _cluster_name_check,
    _deploy_orchestration_package,
    _deploy_with_package,
    _get_image_tag,
    _idempotence_check,
    _validate_release_artifact_path,
)

logger = logging.getLogger("cs_cluster.deploy")


@click.command()
@pass_ctx
def binary_deps(ctx: CliCtx):
    """Deploy helm."""
    logger.info("start installing binary_deps")
    script_path = _binary_deps(ctx)
    _deploy_with_package(ctx, script_path)
    logger.info("success install binary_deps")


@click.command()
@pass_ctx
def cilium(ctx: CliCtx):
    """Deploy cilium."""
    if ctx.in_kind_cluster:
        _, out, _ = ctx.exec("bash -c 'uname -r | cut -d- -f1'")
        kernel_release = StrictVersion(out)
        # https://docs.cilium.io/en/v1.13/operations/system_requirements/
        installable = True if kernel_release >= StrictVersion("4.18.0") else False
        if not installable:
            logger.info("Kernel version too low for cilium, skip install")
            _, stdout, stderr = ctx.cluster_ctr.exec("kubectl get no")
            logger.debug(f"checking nodes: {stdout}, {stderr}")
            _, stdout, stderr = ctx.cluster_ctr.exec("ls /etc/cni/net.d/")
            logger.debug(f"checking cni: {stdout}, {stderr}")
            return

    logger.info("start installing cilium")
    script_path = _cilium(ctx)
    _deploy_with_package(ctx, script_path)
    logger.info("success install cilium")


def _has_multiple_mgmt_nodes(ctx: CliCtx) -> bool:
    cmd = "kubectl get nodes -lk8s.cerebras.com/node-role-management -o=name"
    _, sout, _ = ctx.cluster_ctr.must_exec(cmd)
    return len(sout.split()) > 1


@click.command()
@common.force_option
@pass_ctx
def ceph(ctx: CliCtx, force: Optional[bool] = False):
    """Deploy ceph."""
    if ctx.in_kind_cluster and not force:
        logger.info("skip installing ceph for kind")
        return

    if not _has_multiple_mgmt_nodes(ctx) and not force:
        logger.info("No multiple management nodes. Skip ceph deployment")
        return

    status, _, _ = ctx.cluster_ctr.exec("kubectl -n kube-system get svc kube-webhook")
    if status != 0:
        logger.info("installing kube-webhook as a prerequisite")
        script_path = _kube_webhook(ctx)
        _deploy_with_package(ctx, script_path)

    logger.info("start installing ceph")
    script_path = _ceph(ctx)
    _deploy_with_package(ctx, script_path)
    logger.info("success install ceph")


@click.command()
@pass_ctx
@common.hosts_option
@common.allow_reboot_option
@click.option(
    '--namespace',
    help="Optional namespace for nvme-of deployment. If not present, it will use the hosts option.",
    show_default=True, default="-", type=click.STRING, required=False
)
@common.force_option
def nvme_of(
    ctx: CliCtx,
    hosts: str = "-",
    namespace: str = "-",
    allow_reboot: bool = False,
    force: Optional[bool] = False):
    """Deploy NVMe-oF."""
    if ctx.in_kind_cluster:
        logger.info("skip installing nvme-of for kind")
        return

    if not _has_multiple_mgmt_nodes(ctx):
        logger.info("no multiple management nodes. Skip nvme-of deployment")
        return

    logger.info("start installing nvme-of")
    script_path = _nvme_of(ctx,
                           hosts=hosts,
                           namespace=namespace,
                           allow_reboot=allow_reboot,
                           force=force)
    _deploy_with_package(ctx, script_path)
    logger.info("success install nvme-of")


@click.command()
@common.force_option
@pass_ctx
def registry_sync(ctx: CliCtx, force: Optional[bool] = False):
    """Deploy registry_sync."""
    if ctx.in_kind_cluster and not force:
        logger.info("skip registry sync for kind without force")
        return

    cmd = "kubectl get nodes -lk8s.cerebras.com/node-role-management -o=jsonpath='{.items[*].metadata.name}'"
    _, sout, _ = ctx.cluster_ctr.must_exec(cmd)
    splits = sout.split(' ')
    if len(splits) <= 1 and not force:
        logger.info("No multiple management nodes. Skip registry sync")
        return

    logger.info("start registry sync")
    script_path = _registry_sync(ctx)
    _deploy_with_package(ctx, script_path)
    logger.info("success registry sync")


@click.command()
@common.force_option
@pass_ctx
def multus(ctx: CliCtx,
           force: Optional[bool] = False):
    """Deploy multus."""
    common_images(ctx, namespace="kube-system", system_namespace="job-operator")
    script_path = _multus(ctx, force=force)
    _deploy_with_package(ctx, script_path)
    logger.info("multus/whereabouts network install success")
    rdma_device_plugin_install(ctx)


@click.command()
@pass_ctx
def kube_vip(ctx: CliCtx):
    """Deploy kube-vip on the data network."""
    if not ctx.in_kind_cluster and not _has_multiple_mgmt_nodes(ctx):
        logger.info("cluster does not have multiple mgmt nodes or is not kind, skip installing kube-vip")
        return

    logger.info("start installing kube_vip")
    script_path = _kube_vip(ctx)
    _deploy_with_package(ctx, script_path)
    logger.info("successfully installed kube-vip")


@click.command()
@pass_ctx
def kube_webhook(ctx: CliCtx):
    """Deploy kube-webhook."""
    mgmt_nodes = len([True for n in ctx.cluster_cfg_k8s.nodes if n.role == "management"])
    if not ctx.in_kind_cluster and mgmt_nodes <= 1 and len(ctx.cluster_cfg_k8s.systems) <= 1:
        logger.info("Skip installing kube-webhook for cluster with single mgmt node and system")
        return

    logger.info("start installing kube-webhook")
    script_path = _kube_webhook(ctx)
    _deploy_with_package(ctx, script_path)
    logger.info("successfully installed kube-webhook")

@click.command()
@common.force_option
@pass_ctx
def alert_router(
    ctx: CliCtx,
    force: Optional[bool] = False):
    """Deploy alert router."""
    if ctx.in_kind_cluster and not force:
        logger.debug("skip installing alert router for kind without force option")
        return
    script_path = _alert_router(ctx, force=force)
    _deploy_with_package(ctx, script_path)
    logger.info("successfully installed alert router")


@click.command()
@common.force_option
@common.dashboard_only_option
@common.exporter_only_option
@common.prom_only_option
@common.force_new_tag_option
@pass_ctx
def prometheus(ctx: CliCtx,
               force: Optional[bool] = False,
               dashboard_only: Optional[bool] = False,
               exporter_only: Optional[bool] = False,
               prom_only: Optional[bool] = False,
               force_new_tag: bool = False):
    """Deploy prometheus and grafana."""
    if ctx.in_kind_cluster and not force:
        logger.debug("skip installing prometheus for kind without force option")
        return
    common_images(ctx, namespace="prometheus", system_namespace="job-operator")
    script_path = _prometheus(ctx, force=force, dashboard_only=dashboard_only,
                              exporter_only=exporter_only, prom_only=prom_only,
                              force_new_tag=force_new_tag)
    _deploy_with_package(ctx, script_path)
    logger.info("prometheus install success")


@click.command()
@common.force_option
@pass_ctx
def cpingmesh(ctx: CliCtx, force: Optional[bool] = False):
    """Deploy cpingmesh."""
    if ctx.in_kind_cluster and not force:
        logger.debug("skip installing cpingmesh for kind without force option")
        return

    script_path = _cpingmesh(ctx, force=force)
    _deploy_with_package(ctx, script_path)
    logger.info("cpingmesh install success")

@click.command()
@common.force_option
@pass_ctx
def log_scraping(ctx: CliCtx, force: Optional[bool] = False):
    """Deploy log scraping."""
    if ctx.in_kind_cluster and not force:
        logger.debug("skip installing log scraping for kind")
        return
    common_images(ctx, namespace="loki", system_namespace="job-operator")
    script_path = _log_scraping(ctx, force=force)
    _deploy_with_package(ctx, script_path)
    logger.info("log scraping install success")


@click.command()
@common.force_option
@click.option("--validate-only",
              help="Applying this flag will skip the deployment and only validate by "
                   "publishing and polling messages against a predefined topic.",
              is_flag=True, required=False)
@pass_ctx
def kafka(ctx: CliCtx,
          force: Optional[bool] = False,
          validate_only: bool = False):
    """Deploy kafka."""
    status, _, _ = ctx.cluster_ctr.exec("kubectl -n kube-system get svc kube-webhook")
    if not validate_only and status != 0:
        logger.info("install kube-webhook as a prerequisite")
        script_path = _kube_webhook(ctx)
        _deploy_with_package(ctx, script_path)

    script_path = _kafka(ctx, force=force, validate_only=validate_only)
    _deploy_with_package(ctx, script_path)
    logger.info("kafka install success")


@click.command()
@pass_ctx
def nginx(ctx: CliCtx):
    """Deploy ingress-nginx."""
    status, sout, _ = ctx.cluster_ctr.exec("bash -c 'helm -n ingress-nginx list -ojson | jq -r \".[]|.app_version\"'")
    if status != 0 or sout != '1.12.1':
        logger.info("install kube-webhook as a prerequisite")
        script_path = _kube_webhook(ctx)
        _deploy_with_package(ctx, script_path)

    logger.info("start installing ingress-nginx")
    script_path = _nginx(ctx, with_manifest=True)
    _deploy_with_package(ctx, script_path)
    logger.info("ingress-nginx install success")


def rdma_device_plugin_install(ctx: CliCtx):
    _rdma_device_plugin_path = _rdma_device_plugin(ctx, with_manifest=True)
    _deploy_with_package(ctx, _rdma_device_plugin_path)
    logger.info("rdma_device_plugin install success")


def _find_orchestration_pkg_path(ctx: CliCtx, image_tag:str) -> str:
    """Find orchestration package in the build directory that has the given image tag."""

    if not image_tag:
        return ""

    # The offical release build's image_tag should be in this form:
    #   RELEASE_ID-DATE-BUILD_NUMBER-GITHASH
    #   Example: 3.0.0-202506051146-40-e985c3c0
    # And the directory should be under `/cb/artifacts/builds/cluster-orchestration/`.
    # We construct the build directory here.
    orchestration_release_build_root = "/cb/artifacts/builds/cluster-orchestration"
    orchestration_release_build_dir = f"{orchestration_release_build_root}/{image_tag}"
    tag_splits = image_tag.split('-')
    if len(tag_splits) == 4:
        # This is the expected format for the image tag.
        # Example: 3.0.0-202506051146-40-e985c3c0
        release_id, date, build_number, githash = tag_splits
        orchestration_release_build_dir = f"{orchestration_release_build_root}/{release_id}/{date}-{build_number}-{githash}"

    # Here are the order of finding proper artifacts for the deployment:
    # 1. If image_tag shows up in $GITTOP/build directory, use that. The `make k8s-dev`
    #    will create such a package under $GITTOP/build directory.
    # 2. If image_tag shows up in the new release build directory, use that.
    if os.path.exists(ctx.src_dir / f"../../build/cluster-orchestration-{image_tag}.tar.gz"):
        return f"{ctx.src_dir}/../../build/cluster-orchestration-{image_tag}.tar.gz"

    elif os.path.isdir(orchestration_release_build_dir):
        return f"{orchestration_release_build_dir}/cluster-orchestration-{image_tag}.tar.gz"

    # otherwise, return empty string.
    else:
        return ""


def _adjust_orchestration_package(ctx: CliCtx, pkg_path: str, main_component: str) -> str:
    """
    Adjust the orchestration package for deployment. This is to reduce the package size, to avoid
    uploading unnecessary files to the cluster. The common-images currently have almost 800MB size
    if none of the common images exists in private registry.

    This function checks and skips common-images if images have already been loaded. It also removes
    either job-operator or cluster-server deployment if it is not the main_component.

    Return a string representing the list of skipped common-images steps.
    """
    with open(f"{pkg_path}/manifest.json", 'rb') as f:
        try:
            manifest = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode manifest.json: {e}")
            raise click.ClickException("Invalid manifest.json format")

    if main_component == "job-operator":
        # remove cluster-server from manifest.
        if "cluster-server/apply-cluster-server.sh" in manifest["componentPaths"]:
            manifest["componentPaths"].remove("cluster-server/apply-cluster-server.sh")
        target_manifest = "manifest.job-operator.json"
        ctx.must_exec(f"rm -rf {pkg_path}/cluster-server")
    elif main_component == "cluster-server":
        # remove job-operator from manifest.
        if "job-operator/apply-job-operator.sh" in manifest["componentPaths"]:
            manifest["componentPaths"].remove("job-operator/apply-job-operator.sh")
        target_manifest = "manifest.cluster-server.json"
        ctx.must_exec(f"rm -rf {pkg_path}/job-operator")
    else:
        target_manifest = "manifest.json"

    all_images = ctx.must_exec(f"cat {pkg_path}/common-images/all_images.txt").strip().split("\n")
    steps_to_skip = []
    for image in all_images:
        # skip empty lines. Mostly for testing purpose.
        if not image:
            continue
        image_filename = image.replace(".", "-").replace(":", "-").replace("/", "-")
        # check if the image is already loaded in the cluster
        if is_image_loaded(ctx, REGISTRY_URL + '/' + image):
            logger.info(f"Skipping common image {image} as it is already loaded in the cluster.")
            # remove this image related files from the package
            ctx.must_exec(f"rm -f {pkg_path}/common-images/{image_filename}.tar.gz")
            steps_to_skip.append(f"load-image-{image_filename}.sh")

    if len(steps_to_skip) == len(all_images):
        logger.warning("All common images are already loaded in the cluster, skipping common images deployment.")
        # remove common-images from manifest.
        if "common-images/apply-common-images.sh" in manifest["componentPaths"]:
            manifest["componentPaths"].remove("common-images/apply-common-images.sh")
        ctx.must_exec(f"rm -rf {pkg_path}/common-images")

    with open(f"{pkg_path}/{target_manifest}", 'w') as f:
        json.dump(manifest, f, indent=2)

    return " ".join(steps_to_skip)


@click.command()
@common.namespace_option
@common.system_namespace_option
@common.image_option
@common.image_file_option
@common.cbcore_image_option
@common.load_cbcore_image_option
@common.local_cross_build_option
@common.skip_prerequisite_check_option
@common.system_deploy_force_option
@common.force_new_tag_option
@common.force_option
@common.wsjob_log_preferred_storage_type_option
@common.cached_compile_preferred_storage_type_option
@common.regen_tls_cert_option
@common.pull_ecr_image_option
@common.disable_user_auth_option
@common.isolated_dashboards_option
@common.tools_tar_file_option
@common.disable_fabric_json_check_option
@common.skip_internal_volumes_setup_option
@pass_ctx
def cluster_server(ctx: CliCtx,
                   namespace: str,
                   system_namespace: str,
                   image: Optional[DockerImage] = None,
                   image_file: Optional[pathlib.Path] = None,
                   cbcore_image: Optional[DockerImage] = None,
                   load_cbcore_image: bool = False,
                   local_cross_build: bool = False,
                   skip_prerequisite_check: bool = False,
                   force_new_tag: bool = False,
                   force: bool = False,
                   recreate_tls_secret: bool = False,
                   wsjob_log_preferred_storage_type: Optional[str] = "",
                   cached_compile_preferred_storage_type: Optional[str] = "",
                   pull_ecr_image: bool = False,
                   system_deploy_force: bool = False,
                   disable_user_auth: bool = False,
                   use_isolated_dashboards: bool = False,
                   tools_tar_file: Optional[pathlib.Path] = None,
                   disable_fabric_json_check: bool = False,
                   skip_internal_volumes_setup: bool = False,
                   ):
    """
    Deploy ClusterServer from a given image or by building it from scratch.
    Examples
    \b
    # deploy using an existing cluster-server image overriding the cbcore image set in a values file.
    python cs_cluster.py -v deploy --remote-dir /tmp/$USER cluster-server \\
      --image-file /cb/artifacts/release-stage/default/latest/components/cbcore/cluster-server-*.docker \\
      --cbcore-image 171496337684.dkr.ecr.us-west-2.amazonaws.com/cbcore:1.4.0-202206281146-40-e985c3c0
    \b
    # deploy, building from source
    python cs_cluster.py -v deploy --remote-dir /tmp/$USER cluster-server
    \f
    Overridden values files may be placed in clusters/<cluster>/cluster-server/values.yaml
    """
    return _cluster_server(ctx, namespace, system_namespace, image, image_file, cbcore_image,
                           load_cbcore_image, local_cross_build, skip_prerequisite_check, force_new_tag,
                           force, recreate_tls_secret, wsjob_log_preferred_storage_type,
                           cached_compile_preferred_storage_type, pull_ecr_image, system_deploy_force,
                           disable_user_auth, use_isolated_dashboards, tools_tar_file,
                           disable_fabric_json_check, skip_internal_volumes_setup)


def _cluster_server(ctx: CliCtx,
                    namespace: str,
                    system_namespace: str,
                    image: Optional[DockerImage] = None,
                    image_file: Optional[pathlib.Path] = None,
                    cbcore_image: Optional[DockerImage] = None,
                    load_cbcore_image: bool = False,
                    local_cross_build: bool = False,
                    skip_prerequisite_check: bool = False,
                    force_new_tag: bool = False,
                    force: bool = False,
                    recreate_tls_secret: bool = False,
                    wsjob_log_preferred_storage_type: Optional[str] = "",
                    cached_compile_preferred_storage_type: Optional[str] = "",
                    pull_ecr_image: bool = False,
                    system_deploy_force: bool = False,
                    disable_user_auth: bool = False,
                    use_isolated_dashboards: bool = False,
                    tools_tar_file: Optional[pathlib.Path] = None,
                    disable_fabric_json_check: bool = False,
                    skip_internal_volumes_setup: bool = False,
                    **kwargs,
                    ):
    _cluster_name_check(ctx)
    image_file = fix_pathlib_path(image_file)
    # simple validation on cases when user specified wrong image
    if image is not None and "cluster-server" not in image.name:
        raise Exception("image name must contain cluster-server")
    if image_file is not None and "cluster-server" not in image_file.name:
        raise Exception("image name must contain cluster-server")

    image_tag = image.tag if image is not None else extract_image(image_file).tag
    logger.debug(f"check {image_tag} for cluster-server exists in the cluster")
    if not force_new_tag and not recreate_tls_secret and not force and \
            _idempotence_check(ctx, namespace, 'cluster-server', 'cluster-server', image_tag,
                               wsjob_log_preferred_storage_type, cached_compile_preferred_storage_type):
        return

    orchestration_pkg_path = _find_orchestration_pkg_path(ctx, image_tag)
    if orchestration_pkg_path:
        orchestration_pkg_name = os.path.basename(orchestration_pkg_path)
        orchestration_pkg_name = orchestration_pkg_name[0:len(orchestration_pkg_name) - len(".tar.gz")]
        _ = ctx.must_exec(f"rm -rf {ctx.build_dir}/{orchestration_pkg_name}")
        _ = ctx.must_exec(f"tar xfz {orchestration_pkg_path} -C {ctx.build_dir}")

        # Add cbcore image override for cluster-server deployment, for backward compatibility.
        if load_cbcore_image:
            if not cbcore_image:
                raise ValueError("--cbcore-image is required if --load-cbcore-image is set")
            cbcore_image = deploy_image(ctx, cbcore_image)

        if not cbcore_image:
            cbcore_image = f"171496337684.dkr.ecr.us-west-2.amazonaws.com/cbcore:{image_tag}"

        steps_to_skip = _adjust_orchestration_package(ctx, ctx.build_dir / orchestration_pkg_name, "cluster-server")

        # set up extra environment variables for cluster-server deployment
        env_vars = str(
            f"wsjob_log_preferred_storage_type={wsjob_log_preferred_storage_type} "
            f"cached_compile_preferred_storage_type={cached_compile_preferred_storage_type} "
            f"cbcore_image={cbcore_image} "
            f"use_isolated_dashboards={use_isolated_dashboards} "
            f"disable_user_auth={disable_user_auth} "
            f"disable_fabric_json_check={disable_fabric_json_check} "
            f"system_deploy_force={system_deploy_force} "
            f"steps_to_skip=\" {steps_to_skip} \" "
        )

        logger.info("start installing cluster server")
        _deploy_orchestration_package(ctx, ctx.build_dir / orchestration_pkg_name, "cluster-server", env_vars=env_vars, namespace=namespace)
        logger.info("cluster server install success")

        semantic_version_compatible_check(ctx, namespace, system_namespace)
        return

    if not skip_prerequisite_check:
        namespace_required_setup(
            ctx=ctx,
            system_namespace=system_namespace,
            namespace=namespace,
            image_tag=image_tag,
            tools_tar_file=tools_tar_file,
            skip_internal_volumes_setup=skip_internal_volumes_setup)
        if load_cbcore_image:
            if not cbcore_image:
                raise ValueError("--cbcore-image is required if --load-cbcore-image is set")
            cbcore_image = deploy_image(ctx, cbcore_image)
    elif not cbcore_image:
        cbcore_image = f"171496337684.dkr.ecr.us-west-2.amazonaws.com/cbcore:{image_tag}"

    logger.info("start installing cluster server")
    script_path = __cluster_server(
        ctx,
        namespace=namespace,
        system_namespace=system_namespace,
        image=image,
        image_file=image_file,
        local_cross_build=local_cross_build,
        force_new_tag=force_new_tag,
        with_manifest=True,
        cbcore=cbcore_image,
        recreate_tls_secret=recreate_tls_secret,
        wsjob_log_preferred_storage_type=wsjob_log_preferred_storage_type,
        cached_compile_preferred_storage_type=cached_compile_preferred_storage_type,
        system_deploy_force=system_deploy_force,
        disable_user_auth=disable_user_auth,
        use_isolated_dashboards=use_isolated_dashboards,
        disable_fabric_json_check=disable_fabric_json_check,
    )
    _deploy_with_package(ctx, script_path, namespace=namespace)

    logger.info("cluster server install success")
    semantic_version_compatible_check(ctx, namespace, system_namespace)


@click.command()
@common.force_option
@common.namespace_option
@common.system_namespace_option
@common.image_option
@common.image_file_option
@common.pull_ecr_image_option
@common.regen_tls_cert_option
@pass_ctx
def debugviz_server(ctx: CliCtx,
                    namespace: str,
                    system_namespace: str,
                    image: Optional[DockerImage] = None,
                    image_file: Optional[pathlib.Path] = None,
                    pull_ecr_image: bool = False,
                    force: Optional[bool] = False,
                    recreate_tls_secret: bool = False):
    if ctx.in_kind_cluster and not force:
        logger.debug("skip installing debugviz server for kind without force option")
        return

    if namespace != system_namespace:
        return

    # Debugviz requires the rsync image to be loaded
    common_images(ctx, system_namespace=system_namespace, namespace=namespace)

    logger.info("start installing debugviz server")
    script_path = _debugviz_server(ctx, namespace, system_namespace, image=image, image_file=image_file, pull_image=pull_ecr_image,
                                   force=force, with_manifest=True, recreate_tls_secret=recreate_tls_secret)

    _deploy_with_package(ctx, script_path)
    logger.info("debugviz server install success")


@click.command()
@common.image_option
@common.image_file_option
@common.local_cross_build_option
@common.force_new_tag_option
@common.skip_rebuild_option
@common.skip_prerequisite_check_option
@common.system_namespace_option
@common.enable_multi_coordinator_option
@common.disable_cluster_mode_option
@common.namespace_option
@common.system_deploy_force_option
@common.force_option
@common.pull_ecr_image_option
@common.tools_tar_file_option
@pass_ctx
def job_operator(ctx: CliCtx, system_namespace: str, namespace: str, image: Optional[DockerImage] = None,
                 image_file: Optional[pathlib.Path] = None, enable_multi_coordinator: bool = False,
                 disable_cluster_mode: bool = False,
                 system_deploy_force: bool = False,
                 skip_prerequisite_check: bool = False,
                 local_cross_build: bool = False, force_new_tag: bool = False, skip_rebuild: bool = False,
                 force: bool = False, pull_ecr_image: bool = False,
                 tools_tar_file: Optional[pathlib.Path] = None):
    """Deploy JobOperator from a given image or by building it from scratch.
    Examples
    \b
    # deploy using an existing image
    python cs_cluster.py -v deploy --remote-dir /tmp/$USER job-operator \\
      --image-file /cb/artifacts/release-stage/default/latest/components/cbcore/job-operator-*.docker
    \b
    # deploy, building from source
    python cs_cluster.py -v deploy --remote-dir /tmp/$USER job-operator
    """
    return _job_operator(ctx, system_namespace, namespace, image, image_file, enable_multi_coordinator,
                         disable_cluster_mode, system_deploy_force,
                         skip_prerequisite_check, local_cross_build,
                         force_new_tag, skip_rebuild, force, pull_ecr_image, tools_tar_file)


def _job_operator(ctx: CliCtx,
                  system_namespace: str,
                  namespace: str,
                  image: Optional[DockerImage] = None,
                  image_file: Optional[pathlib.Path] = None,
                  enable_multi_coordinator: bool = False,
                  disable_cluster_mode: bool = False,
                  system_deploy_force: bool = False,
                  skip_prerequisite_check: bool = False,
                  local_cross_build: bool = False,
                  force_new_tag: bool = False, skip_rebuild: bool = False,
                  force: bool = False, pull_ecr_image: bool = False,
                  tools_tar_file: Optional[pathlib.Path] = None):
    _cluster_name_check(ctx)
    single_box = False
    if len(ctx.cluster_cfg_k8s.nodes) <= 1 or len(ctx.cluster_cfg_k8s.groups) <= 1:
        single_box = True
    if not disable_cluster_mode and namespace != system_namespace:
        ctx.cluster_ctr.exec(
            f"kubectl delete deploy {namespace}-controller-manager -n{namespace} --ignore-not-found")
        ctx.cluster_ctr.exec(f"kubectl delete cm {namespace}-cluster-env -n{namespace} --ignore-not-found")
        # continue to upgrade system NS for single box cluster
        if single_box:
            namespace = system_namespace
            logger.info("default to system NS deploy for single box")
        else:
            return
    if enable_multi_coordinator and ctx.cluster_cfg_k8s.has_multi_mgmt:
        logger.warning("multi-crd mode is internal only for single mgmt node cluster, "
                       "auto disable for multi-mgmt cluster.")
        enable_multi_coordinator = False
    # avoid disable multi-crd by mistake for single mgmt node cluster
    if (not single_box and not enable_multi_coordinator and
            namespace == system_namespace and not ctx.cluster_cfg_k8s.has_multi_mgmt):
        ret, std, _ = ctx.cluster_ctr.exec(
            "kubectl -n job-operator get cm job-operator-cluster-env -ojsonpath={.data.ENABLE_MULTI_COORDINATOR}")
        if ret == 0 and str(std).strip().lower() == "true":
            if click.confirm(
                    f"Reminder: multi-crd mode is enabled in {ctx.cluster_name}, do you want to keep it on?\n"
                    'enter y to continue with multi-crd or enter n to disable'):
                logger.info("continue with multi-crd mode")
                enable_multi_coordinator = True

    image_file = fix_pathlib_path(image_file)
    # simple validation on cases when user specified wrong image
    if image is not None and "job-operator" not in image.name:
        raise Exception("image name must contain job-operator")
    if image_file is not None and "job-operator" not in image_file.name:
        raise Exception("image name must contain job-operator")

    image_tag = image.tag if image is not None else extract_image(image_file).tag
    logger.debug(f"check {image_tag} for {namespace}-controller-manager exists in the cluster")
    if not force_new_tag and not force and \
            _idempotence_check(ctx, namespace, 'job-operator', f'{namespace}-controller-manager', image_tag):
        return

    logger.info("start installing job-operator")

    orchestration_pkg_path = _find_orchestration_pkg_path(ctx, image_tag)
    if orchestration_pkg_path:
        orchestration_pkg_name = os.path.basename(orchestration_pkg_path)
        orchestration_pkg_name = orchestration_pkg_name[0:len(orchestration_pkg_name) - len(".tar.gz")]
        _ = ctx.must_exec(f"rm -rf {ctx.build_dir}/{orchestration_pkg_name}")
        _ = ctx.must_exec(f"tar xfz {orchestration_pkg_path} -C {ctx.build_dir}")

        logger.info("start installing job-operator")
        steps_to_skip = _adjust_orchestration_package(ctx, ctx.build_dir / orchestration_pkg_name, "job-operator")
        env_vars = str(
            f"system_deploy_force={system_deploy_force} "
            f"enable_multi_coordinator={enable_multi_coordinator} "
            f"disable_cluster_mode={disable_cluster_mode} "
            f"steps_to_skip=\" {steps_to_skip} \" "
        )
        _deploy_orchestration_package(ctx, ctx.build_dir / orchestration_pkg_name, "job-operator", env_vars=env_vars, namespace=namespace)
        logger.info("job-operator install success")
        return

    if not skip_prerequisite_check:
        # note: update regcred for ECR in colo has to be done before job-operator start
        namespace_required_setup(
            ctx=ctx,
            system_namespace=system_namespace,
            namespace=namespace,
            image_tag=image_tag,
            tools_tar_file=tools_tar_file)

    script_path = __job_operator(
        ctx,
        system_namespace=system_namespace,
        namespace=namespace,
        image=image,
        image_file=image_file,
        local_cross_build=local_cross_build,
        force_new_tag=force_new_tag,
        enable_multi_coordinator=enable_multi_coordinator,
        disable_cluster_mode=disable_cluster_mode,
        system_deploy_force=system_deploy_force,
        with_manifest=True,
    )
    _deploy_with_package(ctx, script_path, namespace=namespace)
    logger.info("job-operator install success")


@click.command()
@common.system_namespace_option
@common.namespace_option
@common.tools_tar_file_option
@click.option(
    '--nfs-server',
    help="Optional NFS server reachable from the appliance containing docker config.json for ecr access",
    default=""
)
@pass_ctx
def colovore_secrets_cron(
        ctx: CliCtx,
        system_namespace: str,
        namespace: str,
        nfs_server: str,
        tools_tar_file: Optional[pathlib.Path] = None):
    """ Deploys all cerebras namespace required components. """
    namespace_required_setup(ctx, system_namespace, namespace, nfs_server, tools_tar_file=tools_tar_file)


# check deployed server/operator version as info only
def semantic_version_compatible_check(ctx: CliCtx, namespace: str, system_namespace: str) -> None:
    ret, std, stderr = ctx.cluster_ctr.exec(
        f"kubectl -n {system_namespace} get cm job-operator-cluster-env" + " -ojsonpath={.data.SEMANTIC_VERSION}")
    if ret == 0 and str(std).strip():
        system_sv = version.parse(str(std).strip())
    else:
        logger.debug(f"skip semantic version check. {std, stderr}")
        return
    ret, std, stderr = ctx.cluster_ctr.exec(
        f"kubectl -n {namespace} get deploy cluster-server" + " -ojsonpath={.metadata.labels.SEMANTIC_VERSION}")
    if ret == 0 and str(std).strip():
        user_sv = version.parse(str(std).strip())
    else:
        logger.debug(f"skip semantic version check. {std, stderr}")
        return

    # can be extended to reject or auto upgrade system NS based on policy
    if system_sv < user_sv:
        logger.warning(f"Cluster operator version:{system_sv} is behind deployed cluster-server version:{user_sv}! \n"
                       f"Strongly suggested to request cluster-mgmt team to upgrade cluster operator before run jobs.")
    return


# setup namespace required volumes/tools/images/ecr-certs
def namespace_required_setup(
        ctx: CliCtx,
        system_namespace: str,
        namespace: str,
        nfs_server: str = "",
        image_tag: str = "",
        tools_tar_file: Optional[pathlib.Path] = None,
        skip_internal_volumes_setup: bool = False):
    """Deploy a cronjob to refresh the docker secrets in colovore + other cerebras specific components."""
    status, _, _ = ctx.cluster_ctr.exec(f"kubectl get ns {namespace}")
    if status != 0:
        ctx.cluster_ctr.must_exec(f"kubectl create namespace {namespace}")

    # deploy image+tools for volumes (internal setup)
    common_images(ctx, system_namespace=system_namespace, namespace=namespace, tag_latest=(system_namespace == namespace))
    if not image_tag and not tools_tar_file:
        image_tag = _get_image_tag(ctx, namespace, "cluster-server")
    _cluster_tools_path = _cluster_tools(ctx, system_namespace=system_namespace, namespace=namespace,
                                         image_tag=image_tag, tar_file=tools_tar_file, with_manifest=True)
    _deploy_with_package(ctx, _cluster_tools_path)

    # setup volumes+ecr certs
    _cerebras_setup_path = _cerebras_internal_setup(ctx, namespace=namespace, nfs_server=nfs_server,
                                                    skip_internal_volumes_setup=skip_internal_volumes_setup,
                                                    with_manifest=True)
    _deploy_with_package(ctx, _cerebras_setup_path)


@click.command()
@common.recreate_tls_secret_option
@pass_ctx
def registry(ctx: CliCtx, recreate_tls_secret: Optional[bool] = False, is_static: Optional[bool] = False):
    """
    Deploy private registry and configure all nodes' containerd to use this registry.
    This command should be idempotent such that we can reconfigure registries if needed (such as if
    adding new nodes to the cluster).
    """
    logger.info(("static" if is_static else "private") + "-registry install start")
    if recreate_tls_secret:
        ctx.cluster_ctr.exec("kubectl delete secret registry-tls-secret -n kube-system --ignore-not-found")
    script_path = _registry(ctx)
    _deploy_with_package(ctx, script_path)
    logger.info("registry install success")


def common_images(
        ctx: CliCtx,
        namespace: str,
        system_namespace: str,
        tag_latest: Optional[bool] = True):
    all_loaded = True
    skip_images = {}
    ecr_common_images = _get_ecr_common_images()
    for img in ecr_common_images:
        if not is_image_loaded(ctx, img.short_name):
            all_loaded = False
        else:
            skip_images[img.short_name] = True

    custom_images = _get_custom_images()
    for img, _ in custom_images.items():
        if not is_image_loaded(ctx, img):
            all_loaded = False
        else:
            skip_images[img] = True
    # skip deploy as we use version upgrade for new changes
    if all_loaded:
        logger.info("common-images already exist in registry, skip reinstall")
        return

    script_path = _common_images(ctx, namespace=namespace, system_namespace=system_namespace,
        skip_images=skip_images, tag_latest=tag_latest)
    _deploy_with_package(ctx, script_path)
    logger.info("common-images install success")


@click.command()
@click.argument('image', type=click.STRING)
@click.option('--preload', is_flag=True,
              help="Loads the image to every node's local containerd cache. Useful when loading cbcore since otherwise "
                   "jobs will be delayed in startup due to the initial image pull."
              )
@common.systems_option
@pass_ctx
def image(ctx: CliCtx, image: str, preload: bool = False, systems: str = ""):
    """Deploy IMAGE where IMAGE is the path to tar file containing a docker image or a reference to a remote image.
    Prints the name of the image which was uploaded to the cluster registry and can then be used in PodSpecs.
    Examples

    \b
    # deploy an image from ecr, pulled locally if needed before pushing remotely
    python cs_cluster.py -v deploy image 171496337684.dkr.ecr.us-west-2.amazonaws.com/cluster-server:latest

    \b
    # deploy an image file, preloading it to every node on the cluster.
    python cs_cluster.py -v deploy image /cb/artifacts/release-stage/default/latest/components/cbcore/job-operator-*.docker --preload

    \f
    Args
        image: May be a filepath or docker image name of the form <repo>:<tag> If filepath, file should be an image
            that was created with 'docker save' or similar. This function assumes the file is gzip'd if it has a
            .docker or .gz extension and will gzip the file if it does not.
    """
    target_nodes = None
    if systems:
        target_nodes = get_image_target_nodes(ctx.cluster_cfg_k8s, systems.split(","))
    local_ref = deploy_image(ctx, image, preload=(preload or bool(systems)), nodes=target_nodes)
    logger.info(f"loaded image as {local_ref}")
    return local_ref


def deploy_image(ctx: CliCtx, image, preload=False, nodes=None, **kwargs) -> str:
    """ Deploys image to cluster, returning the locally loaded name """
    image_file = None
    image_ref = None
    local_image_ref = None
    if isinstance(image, (str, pathlib.Path,)) and pathlib.Path(image).exists():
        image_file = pathlib.Path(image)
        local_image_ref = extract_image(image_file).local_name
    else:
        image_ref = DockerImage.of(image)
        local_image_ref = image_ref.local_name

    if is_image_loaded(ctx, local_image_ref):
        logger.info(f"{local_image_ref} already exists in registry")
        return local_image_ref

    pull_image = False
    rv, so, _ = ctx.cluster_ctr.exec(
        "kubectl -n job-operator get secret regcred -ojsonpath='{.data.\\.dockerconfigjson}'")
    if rv != 0 or len(so) < 10:  # bogus data from testing
        pull_image = True

    deploy_script = package_image(ctx, image=image_ref, image_file=image_file, pull_image=pull_image,
                                  load_all_nodes=preload, load_nodes=nodes, **kwargs)
    _deploy_with_package(ctx, deploy_script)
    return local_image_ref


class ParamikoClient:
    def __init__(self, host, port, username, password=None, key_filename=None, pem_file=None):
        auth_methods = sum(1 if v else 0 for v in (password, key_filename, pem_file))
        if auth_methods > 1:
            raise ValueError("invalid arguments: only provide zero or one auth methods")
        self._connect_args = dict(
            hostname=host,
            port=port,
            username=username,
            pkey=pem_file,
            key_filename=key_filename,
            look_for_keys=auth_methods == 0,
            password=password,
        )

    def __enter__(self) -> paramiko.SSHClient:
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(**self._connect_args)
        return self.ssh

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ssh.close()
        self.ssh = None


def _check_usernode_preconditions(ctx: CliCtx, namespace: str):
    # ensure cluster-server's cert has an alt name for the cluster specific domain
    _, cert, _ = ctx.cluster_ctr.must_exec(
        f"kubectl get secret cluster-server-grpc-secret-v2 -n {namespace} "
        "-o jsonpath='{.data.tls\\.crt}'")
    cert_path = ctx.build_dir / "cluster-server.crt"
    cert_path.write_text(base64.b64decode(cert).decode())
    alt_names = ctx.must_exec(f"openssl x509 -in {cert_path.absolute()} -text -noout")
    alt_names = re.search(r"X509v3 Subject Alternative Name: \n +([^\n]+)", alt_names).group(1)
    alt_names = [n.strip() for n in alt_names.split(",")]

    logger.debug(f"cluster server cert altnames: {alt_names}, looking for {ctx.cluster_name}")
    if not any([ctx.cluster_name in name for name in alt_names]):
        raise click.ClickException(
            "cluster-server's cert does not have an alt name for the cluster specific domain. "
            f"Please run './cs_cluster.py -v --cluster {ctx.cluster_name} deploy cluster-server --recreate-tls-secret' "
            "to regenerate the cert.")


@click.command()
@pass_ctx
@common.release_artifact_path_option
@common.config_only_option
@common.namespace_option
@common.system_namespace_option
@common.overwrite_usernode_configs_option
@common.reset_usernode_configs_option
@common.skip_install_default_csctl_option
@common.enable_usernode_monitoring_option
@common.hosts_option
@common.usernode_port_option
@common.usernode_login_username_option
@common.usernode_login_password_option
def usernode(
        ctx: CliCtx,
        system_namespace: str,
        namespace: str,
        overwrite_usernode_configs: bool,
        release_artifact_path: Optional[pathlib.Path],
        config_only: bool,
        reset_usernode_configs: bool,
        skip_install_default_csctl: bool,
        enable_usernode_monitoring: bool,
        hosts: str,
        port: int,
        username: str,
        password: str,
):
    """
    Stages usernode artifacts on the mgmt node then runs usernode installer on each usernode specified. If the option
    --release-artifact-path is not specified, then the artifacts from ${GITTOP}/build will be used.

    Examples

    \b
    # Deploy the latest release to the specified usernodes
    python cs_cluster.py -v deploy usernode -h usernode0,usernode1 \\
        --release-artifact-path /cb/artifacts/release-stage/default/latest/components/cbcore
    \b
    # Deploy the wheels in GITTOP/build to all usernodes discovered in mongodb (need to build the wheels first)
    python cs_cluster.py -v deploy usernode
    """
    return _usernode(ctx, system_namespace, namespace, overwrite_usernode_configs, release_artifact_path, config_only,
                     reset_usernode_configs, skip_install_default_csctl, enable_usernode_monitoring, hosts, port, username, password)


def _usernode(
        ctx: CliCtx,
        system_namespace: str,
        namespace: str,
        overwrite_usernode_configs: bool,
        release_artifact_path: Optional[pathlib.Path],
        config_only: bool,
        reset_usernode_configs: bool,
        skip_install_default_csctl: bool,
        enable_usernode_monitoring: bool,
        hosts: str,
        port: int,
        username: str,
        password: str,
        **kwargs,
):
    release_artifact_path = fix_pathlib_path(release_artifact_path)

    if not reset_usernode_configs:
        _check_usernode_preconditions(ctx, namespace)

    if not release_artifact_path and not config_only:
        if not _gather_built_wheels():
            logger.warning("No wheels found in build directory, skipping packaging of wheels")
            config_only = True

    # resolve and check that each usernode is reachable prior to starting the deploy which takes some time
    hostnames = []
    if hosts == "-":
        logger.info("no usernode hosts provided, searching devinfra database for usernode hosts")
        with DevInfraDB() as db:
            hostnames = db.list_usernodes(ctx.cluster_name)
        if not hostnames:
            logger.warning(
                f"No usernodes found in devinfra database for cluster {ctx.cluster_name}, "
                "packaging artifacts but skipping usernode deploy"
            )
    elif hosts != "":
        hostnames = hosts.split(",")

    usernode_ssh_args = {"username": username, "port": port}
    if hostnames and password == "-":
        cluster_creds = ctx.resolve_ssh_creds()
        for key in ("key_filename", "pem", "password",):
            if key in cluster_creds:
                usernode_ssh_args[key] = cluster_creds[key]
                logger.info(f"Using ssh {key} from cluster creds to access usernodes")
                break
        else:
            logger.info(f"Using passwordless login to access usernodes")
    else:
        usernode_ssh_args["password"] = password


    logger.info(f"usernodes: {hostnames}")
    for hostname in hostnames:
        try:
            logger.info(f"checking ssh connectivity to {hostname}")
            with ParamikoClient(host=hostname, **usernode_ssh_args):
                pass
        except Exception as e:
            logger.error(f"Failed to connect to {usernode}:{password}@{hostname}:{port}: {e}")
            raise

    # stage the artifacts on the mgmt node

    my_pid = os.getpid()
    pkg_path = ctx.build_dir / f"pkg-user-{my_pid}"
    if pkg_path.exists():
        shutil.rmtree(pkg_path)
    pkg_path.mkdir(parents=True)
    __usernode(ctx, system_namespace, namespace, overwrite_usernode_configs,
               pkg_path, release_artifact_path, config_only, reset_usernode_configs, 
               skip_install_default_csctl, enable_usernode_monitoring, False)
    pkg_path = pkg_path / "usernode"
    base_pkg_file = str(next(pkg_path.glob("base-user-pkg-*.tar.gz"))).split("/")[-1]
    version = re.search(r"base-user-pkg-(.*).tar.gz", base_pkg_file).group(1)
    pkg_file = base_pkg_file[len("base-"):]

    # copy the usernode installer to mgmt node and run the installer

    remote_pkg_path = f"{ctx.remote_build_dir}/{my_pid}/pkg-user"
    ctx.cluster_ctr.must_exec(f"mkdir -p {remote_pkg_path}")
    ctx.cluster_ctr.exec_cmds([
        DirCopyCmd(pkg_path, remote_pkg_path),
        FileCopyCmd(f"{ctx.apps_dir}/common/pkg-common.sh", f"{ctx.remote_build_dir}/{my_pid}/pkg-common.sh"),
        MkdirCmd(f"{ctx.remote_build_dir}/{my_pid}/pkg-functions"),
        DirCopyCmd(f"{ctx.apps_dir}/common/pkg-functions", f"{ctx.remote_build_dir}/{my_pid}/pkg-functions"),
    ])
    ctx.cluster_ctr.must_exec(
        f"bash -c 'chmod u+x {remote_pkg_path}/*.sh && {remote_pkg_path}/stage-mgmt-node.sh --skip-copy'")

    # retrieve customized artifacts, then install the artifacts on the usernodes
    # The reason for using my_pid is to prevent race conditions if jenkins is running multiple tests which run the
    # usernode script in parallel
    user_pkg_download_dir = ctx.build_dir / f"pkg-user-{my_pid}-download"
    shutil.rmtree(user_pkg_download_dir, ignore_errors=True)
    user_pkg_download_dir.mkdir(parents=True)
    user_pkg = user_pkg_download_dir / pkg_file
    ctx.cluster_ctr.exec_cmds([
        FileGetCmd(f"{remote_pkg_path}/{pkg_file}", user_pkg),
        Cmd(f"rm -rf {remote_pkg_path}"),  # clean up the remote pkg dir
    ])

    def _usernode_install(hostname: str):
        with ParamikoClient(hostname, **usernode_ssh_args) as ssh:
            def exec_cmd(cmd: str, raise_on_error=True) -> str:
                logger.debug(f"usernode/{hostname} executing {cmd}")
                _, stdout, stderr = ssh.exec_command(cmd)
                se = stderr.read().decode('utf-8')
                so = stdout.read().decode('utf-8')
                if stderr.channel.recv_exit_status() != 0:
                    msg = f"Failed to execute command: {cmd}, stderr: {se}, stdout: {so}"
                    if raise_on_error:
                        raise RuntimeError(msg)
                    else:
                        logger.error(msg)
                logger.debug(so)
                return so

            exec_cmd(f"mkdir -p /opt/cerebras/packages/user-pkg-{my_pid}")

            with ssh.open_sftp() as sftp:
                logger.debug(f"cp {user_pkg} -> {hostname}:/opt/cerebras/packages/user-pkg-{my_pid}")
                rv = sftp.put(user_pkg, f"/opt/cerebras/packages/user-pkg-{my_pid}/user-pkg.tar.gz")
                if rv.st_size == 0:
                    raise RuntimeError(f"Failed to copy user-pkg.tar.gz to {hostname}:{port}")

            try:
                exec_cmd(
                    f"set -o pipefail ; cd /opt/cerebras/packages/user-pkg-{my_pid} && tar xzf user-pkg.tar.gz && cd user-pkg-{version} && ./install.sh | tee install.log")
                exec_cmd(
                    f"rm -rf /opt/cerebras/packages/user-pkg-{my_pid} && find /opt/cerebras/packages -name 'user-pkg*' -mtime +7 -exec rm -rf \"{{}}\" \\;",
                    raise_on_error=False)
            except:
                # in case of error, leave the dir around for investigation, but remove the tarball which may be large
                exec_cmd(f"rm -rf /opt/cerebras/packages/user-pkg-{my_pid}/user-pkg.tar.gz", raise_on_error=False)
                raise

    for hostname in hostnames:
        logger.info(f"installing user-pkg-{version}.tar.gz on {hostname}")
        _usernode_install(hostname)


@click.command()
@cluster_mgmt_retention_days_option
@cluster_mgmt_rotate_at_size_option
@cluster_mgmt_rotate_count_max_option
@common.tools_tar_file_option
@pass_ctx
def log_rotation(
        ctx: CliCtx,
        cluster_mgmt_retention_days: Optional[int] = 30,
        cluster_mgmt_rotate_at_size: Optional[str] = "100M",
        cluster_mgmt_rotate_count_max: Optional[int] = 5,
        namespace: str = "job-operator",
        tools_tar_file: Optional[pathlib.Path] = None,
):
    """Deploy log_rotation."""
    logger.info("log rotation install start")
    # Log rotation depends on cluster server volume setup in order to decide whether NFS cleanup is needed.
    namespace_required_setup(
        ctx=ctx,
        system_namespace=namespace,
        namespace=namespace,
        tools_tar_file=tools_tar_file)

    script_path = _log_rotation(ctx,
                                cluster_mgmt_retention_days=cluster_mgmt_retention_days,
                                cluster_mgmt_rotate_at_size=cluster_mgmt_rotate_at_size,
                                cluster_mgmt_rotate_count_max=cluster_mgmt_rotate_count_max)

    _deploy_with_package(ctx, script_path)
    ctx.cluster_ctr.must_exec("bash -c 'dryrun=true skip_nfs_cleanup=true bash /etc/cron.hourly/log-cleanup'")
    ctx.cluster_ctr.must_exec("bash -c 'dryrun=true bash /etc/cron.hourly/registry-cleanup'")
    logger.info("log rotation install success")


@click.command()
@common.force_option
@common.namespace_option
@common.system_namespace_option
@common.tools_tar_file_option
@pass_ctx
def cluster_tools(ctx: CliCtx, system_namespace: str, namespace: str, tools_tar_file: Optional[pathlib.Path] = None,
                  force: bool = False):
    """Deploy cluster_tools."""
    if ctx.in_kind_cluster and not force:
        logger.info("skip deploying cluster tools for kind")
        return

    logger.info("cluster tools install start")
    image_tag = ""
    if not tools_tar_file:
        image_tag = _get_image_tag(ctx, namespace, "cluster-server")
    script_path = _cluster_tools(ctx, system_namespace=system_namespace, namespace=namespace,
                                 image_tag=image_tag, tar_file=tools_tar_file)
    _deploy_with_package(ctx, script_path)
    logger.info("cluster tools install success")


@click.command()
@pass_ctx
def validation(ctx: CliCtx):
    """software/network validation."""
    if ctx.in_kind_cluster:
        logger.info("skip validation for kind")
        return

    logger.info("software/network validation start, will take around two minutes")
    ctx.cluster_ctr.must_exec("bash /opt/cerebras/tools/network-validation.sh")
    ret, stdout, _ = ctx.cluster_ctr.exec("bash /opt/cerebras/tools/software-validation.sh")
    if ret != 0:
        logger.error(f"warning, software validation failed: {stdout}")
    else:
        logger.info("all validations success")


@click.command()
@common.from_k8s_version_option
@common.k8s_version_option
@common.system_namespace_option
@pass_ctx
def k8s(ctx: CliCtx, from_k8s_version: str, k8s_version: str, system_namespace: str):
    """Deploy k8s."""
    logger.info("k8s install start")
    if ctx.in_kind_cluster:
        _create_kind_config(ctx, system_namespace=system_namespace)
    _, stdout, _ = ctx.cluster_ctr.exec("kubectl version -ojson")
    try:
        v = json.loads(stdout)
        if "serverVersion" in v:
            cluster_k8s_version = v["serverVersion"]["gitVersion"][1:]
        else:
            cluster_k8s_version = v["clientVersion"]["gitVersion"][1:]
        if Version(cluster_k8s_version) > Version(from_k8s_version):
           from_k8s_version = cluster_k8s_version
    except json.JSONDecodeError:
        pass
    logger.info(f"from k8s version: {from_k8s_version}, target version: {k8s_version}")
    script_path = _k8s(ctx, from_k8s_version, k8s_version)
    _deploy_with_package(ctx, script_path)
    logger.info("k8s install success")


@click.command()
@common.release_artifact_path_option
@click.option(
    '--cluster-server-image',
    help="Docker image in the form of '<repo>:<tag>'. If not provided, the image will be built from source.",
    type=common.DockerImageParamType(), required=False)
@click.option(
    '--job-operator-image',
    help="Docker image in the form of '<repo>:<tag>'. If not provided, the image will be built from source.",
    type=common.DockerImageParamType(), required=False)
@common.from_k8s_version_option
@common.k8s_version_option
@common.cbcore_image_option
@common.load_cbcore_image_option
@common.local_cross_build_option
@common.skip_rebuild_option
@common.config_only_option
@common.force_option
@common.skip_validate_option
@common.namespace_option
@common.system_namespace_option
@common.enable_multi_coordinator_option
@common.wsjob_log_preferred_storage_type_option
@common.cached_compile_preferred_storage_type_option
@common.disable_user_auth_option
@common.overwrite_usernode_configs_option
@common.tools_tar_file_option
@common.hosts_option
@common.allow_reboot_option
@click.pass_context
def all(ctx,
        system_namespace: str,
        namespace: str,
        k8s_version: str,
        from_k8s_version: Optional[str] = "",
        release_artifact_path: Optional[pathlib.Path] = None,
        cluster_server_image: Optional[DockerImage] = None,
        job_operator_image: Optional[DockerImage] = None,
        cbcore_image: Optional[DockerImage] = None,
        load_cbcore_image: bool = False,
        local_cross_build: bool = False,
        skip_validate: bool = False,
        force: bool = False,
        skip_rebuild: bool = False,
        config_only: bool = False,
        enable_multi_coordinator: bool = False,
        wsjob_log_preferred_storage_type: Optional[str] = "",
        cached_compile_preferred_storage_type: Optional[str] = "",
        disable_user_auth: bool = False,
        overwrite_usernode_configs: bool = False,
        tools_tar_file: Optional[pathlib.Path] = None,
        hosts: str = "-",
        allow_reboot: bool = False,
        ):
    """
    Deploy all services to the cluster as if from scratch. It's recommended to run this command with
    '--release-artifact-path' option to avoid building the images from source.

    Example:
    \b
    ./cs_cluster.py -v deploy all --release-artifact-path /cb/artifacts/release-stage/default/latest/components/cbcore
    """
    release_artifact_path = fix_pathlib_path(release_artifact_path)

    cluster_server_image_file, cbcore_image_file, job_operator_image_file = None, None, None
    if release_artifact_path is not None:
        _validate_release_artifact_path(ctx, release_artifact_path)
        if not cluster_server_image:
            try:
                cluster_server_image_file = next(release_artifact_path.glob("cluster-server-*.docker"))
                logger.info(f"using cluster-server image file: {cluster_server_image_file}")
            except StopIteration:
                pass
        if not cbcore_image:
            try:
                cbcore_image_file = next(release_artifact_path.glob("cbcore-*.docker"))
                logger.info(f"using cbcore image file: {cbcore_image_file}")
            except StopIteration:
                pass
        if not job_operator_image:
            try:
                job_operator_image_file = next(release_artifact_path.glob("job-operator-*.docker"))
                logger.info(f"using job-operator image file: {job_operator_image_file}")
            except StopIteration:
                pass
        if not tools_tar_file:
            try:
                tools_tar_file = next(release_artifact_path.glob("cluster-mgmt-tools-*.tar.gz"))
                logger.info(f"using tools tar file: {tools_tar_file}")
            except StopIteration:
                pass
    if cbcore_image_file:
        cbcore_image = str(ctx.invoke(image, image=str(cbcore_image_file.absolute())))

    # if deploying all at non-system namespace, skip system components deployment
    if namespace == system_namespace:
        ctx.invoke(k8s, from_k8s_version=from_k8s_version, k8s_version=k8s_version, system_namespace=system_namespace)
        ctx.invoke(cilium)
        ctx.invoke(kube_vip)
        ctx.invoke(multus, force=force)
        ctx.invoke(kube_webhook)
        ctx.invoke(ceph)
        # SW-156424: One scalability issue was discovered just before rel-2.4 was due.
        # Disabling the NVMe-oF deployment until the issue is resolved.
        ctx.invoke(nvme_of, hosts=hosts, allow_reboot=allow_reboot, force=False)
        # private registry depends on ceph
        ctx.invoke(registry)
        ctx.invoke(registry_sync)
        ctx.invoke(nginx)
        ctx.invoke(log_scraping, force=force)
        ctx.invoke(prometheus, force=force)
        ctx.invoke(cpingmesh, force=force)
        ctx.invoke(log_rotation, tools_tar_file=tools_tar_file)
        # SW-109779
        # We will revive kafka when the adoption plan is clear
        # Skipping kafka deployment in "deploy all" to speed up canary tests
        # ctx.invoke(kafka)
        ctx.invoke(cluster_tools,
                   system_namespace=system_namespace,
                   namespace=namespace,
                   tools_tar_file=tools_tar_file,
                   force=force)
        ctx.invoke(job_operator,
                   namespace=namespace,
                   system_namespace=system_namespace,
                   system_deploy_force=(namespace == system_namespace),
                   image=job_operator_image,
                   image_file=job_operator_image_file,
                   local_cross_build=local_cross_build,
                   skip_rebuild=skip_rebuild,
                   enable_multi_coordinator=enable_multi_coordinator,
                   force=force,
                   tools_tar_file=tools_tar_file)

    # only cluster-server/user-node needs deployment for user NS
    ctx.invoke(cluster_server,
               namespace=namespace,
               system_namespace=system_namespace,
               system_deploy_force=(namespace == system_namespace),
               image=cluster_server_image,
               image_file=cluster_server_image_file,
               cbcore_image=cbcore_image,
               load_cbcore_image=load_cbcore_image,
               local_cross_build=local_cross_build,
               wsjob_log_preferred_storage_type=wsjob_log_preferred_storage_type,
               cached_compile_preferred_storage_type=cached_compile_preferred_storage_type,
               force=force,
               disable_user_auth=disable_user_auth,
               tools_tar_file=tools_tar_file)
    ctx.invoke(debugviz_server, namespace=namespace, system_namespace=system_namespace, force=force)
    ctx.invoke(usernode, system_namespace=system_namespace, namespace=namespace,
               overwrite_usernode_configs=overwrite_usernode_configs,
               release_artifact_path=release_artifact_path,
               config_only=config_only)

    if not skip_validate and namespace == system_namespace:
        ctx.invoke(validation)
