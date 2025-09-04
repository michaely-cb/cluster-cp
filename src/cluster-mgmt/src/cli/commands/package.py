import datetime
import glob
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import tarfile
from typing import (
    List,
    Optional,
    Tuple,
)

import click
import yaml

import common
from common import (
    BINARY_DEPS,
    BUSYBOX_IMAGE,
    BUSYBOX_IMAGE_SHORT_NAME,
    CEPH_CEPH_IMAGE,
    CEPH_COMPONENT,
    CEPH_CSI_IMAGE,
    CILIUM_ALPINE_CURL_IMAGE,
    CILIUM_CERTGEN_IMAGE,
    CILIUM_HUBBLE_RELAY_IMAGE,
    CILIUM_HUBBLE_UI_BACKEND_IMAGE,
    CILIUM_HUBBLE_UI_IMAGE,
    CILIUM_CLI_VERSION,
    CILIUM_VERSION,
    CILIUM_IMAGE,
    CILIUM_JSON_MOCK_IMAGE,
    CILIUM_OPERATOR_GENERIC_IMAGE,
    CILIUM_OPERATOR_IMAGE,
    COREDNS_IMAGE,
    CSI_ATTACHER_IMAGE,
    CSI_NODE_DRIVER_REGISTRAR_IMAGE,
    CSI_PROVISIONER_IMAGE,
    CSI_RESIZER_IMAGE,
    CSI_SNAPSHOTTER_IMAGE,
    CTR_VERSION,
    ECR_URL,
    FLUENTBIT_IMAGE,
    IMAGES_SUBPATH,
    K8S_SIDECAR_IMAGE,
    KAFKA_IMAGE,
    K8S_IMAGES,
    KUBE_RBAC_PROXY_IMAGE,
    KUBE_VIP_IMAGE,
    KUBE_WEBHOOK_IMAGE,
    LOKI_IMAGE,
    MULTUS_IMAGE,
    NGINX_CTRL_IMAGE,
    NGINX_WEBHOOK_IMAGE,
    PAUSE_IMAGES,
    PROM_ALERT_IMAGE,
    PROM_APP_VERSION,
    PROM_CONFIG_IMAGE,
    PROM_GRAFANA_IMAGE,
    PROM_IMAGE,
    PROM_NGINX_WEBHOOK_CERTGEN_IMAGE,
    PROM_NODE_EXPORTER_IMAGE,
    PROM_OPERATOR_IMAGE,
    PROM_SIDECAR_IMAGE,
    PROM_SNMP_EXPORTER_IMAGE,
    PROM_STATE_METRICS_IMAGE,
    PROM_THANOS_IMAGE,
    PROM_VERSION_FOR_GRAFANA_CONFIG,
    THANOS_VERSION_FOR_GRAFANA_CONFIG,
    PYTHON38_IMAGE,
    PYTHON38_IMAGE_SHORT_NAME,
    PYTHON311_IMAGE,
    PYTHON311_IMAGE_SHORT_NAME,
    RDMA_DEVICE_PLUGIN_IMAGE,
    REGISTRY_IMAGE,
    REGISTRY_SYNC_COMPONENT,
    REGISTRY_URL,
    ROOK_CEPH_IMAGE,
    WHEREABOUTS_IMAGE,
    DockerImage,
    cluster_mgmt_retention_days_option,
    cluster_mgmt_rotate_at_size_option,
    cluster_mgmt_rotate_count_max_option,
    context,
    fix_pathlib_path,
    scrape_docker_image_name,
)
from common.image import build_image
from cs_cluster import pass_ctx
from .__init__ import (
    CbcoreManifest,
    ClusterManifest,
    ImageStep,
    ScriptStep,
    StepsManifest,
)
from .utils import (
    _cluster_server_values_resolve,
    _download_k8s_rpm,
    _job_operator_values_resolve,
    _nginx_values_resolve,
)

logger = logging.getLogger("cs_cluster.package")
APP_ROOT = str(pathlib.Path(__file__).parent.parent.joinpath("apps").absolute())
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

ALPINE_KUBECTL_TAG = ""
ALPINE_KUBECTL_IMAGE = ""
ALPINE_CONTAINERD_TAG = ""
ALPINE_CONTAINERD_IMAGE = ""
ALPINE_KUBE_USER_AUTH_TAG = ""
ALPINE_KUBE_USER_AUTH_IMAGE = ""
SYSTEM_CLIENT_TAG = ""
SYSTEM_CLIENT_IMAGE = ""

ceph_pullable_images = {
    "rook_ceph_image": DockerImage.of(ROOK_CEPH_IMAGE),
    "ceph_ceph_image": DockerImage.of(CEPH_CEPH_IMAGE),
    "ceph_csi_image": DockerImage.of(CEPH_CSI_IMAGE),
    "csi_node_driver_registrar_image": DockerImage.of(CSI_NODE_DRIVER_REGISTRAR_IMAGE),
    "csi_provisioner_image": DockerImage.of(CSI_PROVISIONER_IMAGE),
    "csi_snapshotter_image": DockerImage.of(CSI_SNAPSHOTTER_IMAGE),
    "csi_attacher_image": DockerImage.of(CSI_ATTACHER_IMAGE),
    "csi_resizer_image": DockerImage.of(CSI_RESIZER_IMAGE),
    "k8s_sidecar_image": DockerImage.of(K8S_SIDECAR_IMAGE),
}

cilium_pullable_images = {
    "cilium_image": DockerImage.of(CILIUM_IMAGE),
    "certgen_image": DockerImage.of(CILIUM_CERTGEN_IMAGE),
    "hubble_replay_image": DockerImage.of(CILIUM_HUBBLE_RELAY_IMAGE),
    "hubble_ui_backend_image": DockerImage.of(CILIUM_HUBBLE_UI_BACKEND_IMAGE),
    "hubble_ui_image": DockerImage.of(CILIUM_HUBBLE_UI_IMAGE),
    "operator_image": DockerImage.of(CILIUM_OPERATOR_IMAGE),
    "operator_generic_image": DockerImage.of(CILIUM_OPERATOR_GENERIC_IMAGE),

    # used in connectivity test:
    "alpine_curl_image": DockerImage.of(CILIUM_ALPINE_CURL_IMAGE),
    "json_mock_image": DockerImage.of(CILIUM_JSON_MOCK_IMAGE),
    "coredns_image": DockerImage.of(COREDNS_IMAGE),
    "busybox_image": DockerImage.of(BUSYBOX_IMAGE).set_alias(BUSYBOX_IMAGE_SHORT_NAME),
}

registry_pullable_images = {
    "registry_image": DockerImage.of(REGISTRY_IMAGE),
}

kube_vip_pullable_images = {
    "kube_vip_image": DockerImage.of(KUBE_VIP_IMAGE),
}

multus_pullable_images = {
    "multus_image": DockerImage.of(MULTUS_IMAGE),
    "whereabouts_image": DockerImage.of(WHEREABOUTS_IMAGE),
}

kube_webhook_pullable_images = {
    "kube_webhook_image": DockerImage.of(KUBE_WEBHOOK_IMAGE),
}

pkg_path_option = click.option(
    '--pkg-path',
    help="The path to store all files to be packaged. Default to '$src-dir/src/cli/pkg",
    type=pathlib.Path, required=False)

pack_only_option = click.option(
    '--pack-only',
    help="Whether to pack all components that have already been staged",
    is_flag=True, required=False, default=False)

with_manifest_option = click.option(
    '--with-manifest/--without-manifest',
    help="Whether to write manifest along with other package files",
    required=False, default=True)

GET_CEREBRAS_TOKEN_BINARY = "get-cerebras-token"

def _validate_version_string(version_output: str, command_description: str) -> str:
    """
    Validate that the version output from make commands is a single clean string without
    whitespace or newlines, to ensure git submodule stderr doesn't pollute the output.
    """
    if not version_output or re.search(r"\s", version_output):
        raise ValueError(f"Invalid version output from {command_description}: '{version_output}'. "
                        f"Expected a single string without whitespace or newlines. "
                        f"This may be caused by git submodule stderr output polluting the version string.")
    return version_output

def get_pkg_path(ctx, pkg_path: Optional[pathlib.Path]) -> pathlib.Path:
    """ Return the actual package path. """
    pkg_path = fix_pathlib_path(pkg_path)
    return ctx.pkg_dir if pkg_path is None else pkg_path


def prepare_pkg_path(component: str, pkg_path: Optional[pathlib.Path]) -> Tuple[str, pathlib.Path]:
    """ Prepare staging area for packaging. Clear any files except those cached docker images. """
    component_pkg_path = pkg_path / component
    for f in component_pkg_path.glob("*"):
        if f.is_dir():
            shutil.rmtree(f)
        elif not f.name.endswith(".tar.gz") and not f.name.startswith(".platform_meta"):
            f.unlink()
    component_pkg_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Packaging {component} to '{component_pkg_path}'")
    return component, component_pkg_path


def _common_image_versions():
    global ALPINE_KUBECTL_TAG
    global ALPINE_KUBECTL_IMAGE
    global ALPINE_CONTAINERD_TAG
    global ALPINE_CONTAINERD_IMAGE
    global ALPINE_KUBE_USER_AUTH_TAG
    global ALPINE_KUBE_USER_AUTH_IMAGE
    global SYSTEM_CLIENT_TAG
    global SYSTEM_CLIENT_IMAGE
    ALPINE_KUBECTL_TAG = subprocess.run(
        f"make -C {CURRENT_DIR}/../../job-operator/images alpine-kubectl-version --no-print-directory",
        shell=True, check=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True).stdout.strip()
    ALPINE_KUBECTL_IMAGE = f"alpine-kubectl:{ALPINE_KUBECTL_TAG}"
    ALPINE_CONTAINERD_TAG = subprocess.run(
        f"make -C {CURRENT_DIR}/../../job-operator/images alpine-containerd-version --no-print-directory",
        shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True).stdout.strip()
    ALPINE_CONTAINERD_IMAGE = f"alpine-containerd:{ALPINE_CONTAINERD_TAG}"
    ALPINE_KUBE_USER_AUTH_TAG = subprocess.run(
        f"make -C {CURRENT_DIR}/../../job-operator/images alpine-kube-user-auth-version --no-print-directory",
        shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True).stdout.strip()
    ALPINE_KUBE_USER_AUTH_IMAGE = f"alpine-kube-user-auth:{ALPINE_KUBE_USER_AUTH_TAG}"
    SYSTEM_CLIENT_TAG = _validate_version_string(
        subprocess.run(
            f"make -C {CURRENT_DIR}/../../system-maintenance version --no-print-directory",
            shell=True, check=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True).stdout.strip(),
        "system-maintenance version")
    SYSTEM_CLIENT_IMAGE = f"system-client:{SYSTEM_CLIENT_TAG}"


def _get_ecr_common_images():
    # todo: this will be handled as k8s install loaded to static registry
    return [
        DockerImage.of(BUSYBOX_IMAGE).set_alias(BUSYBOX_IMAGE_SHORT_NAME),  # tool/tests usage
        # image build job and streamer uses this image
        DockerImage.of(PYTHON38_IMAGE).set_alias(PYTHON38_IMAGE_SHORT_NAME),
        DockerImage.of(PYTHON311_IMAGE).set_alias(PYTHON311_IMAGE_SHORT_NAME),
    ]


def _get_custom_images():
    _common_image_versions()
    # image name:tag / makefile target / makefile path
    return {
        ALPINE_KUBECTL_IMAGE: ("alpine-kubectl-build", f"{CURRENT_DIR}/../../job-operator/images"),
        ALPINE_CONTAINERD_IMAGE: ("alpine-containerd-build", f"{CURRENT_DIR}/../../job-operator/images"),
        ALPINE_KUBE_USER_AUTH_IMAGE: ("alpine-kube-user-auth-build", f"{CURRENT_DIR}/../../job-operator/images"),
        SYSTEM_CLIENT_IMAGE: ("docker-build", f"{CURRENT_DIR}/../../system-maintenance"),
    }


def _common_images(ctx: context.CliCtx,
                   system_namespace: str,
                   namespace: str,
                   pkg_path: Optional[pathlib.Path] = None,
                   skip_images: dict = {},
                   tag_latest: bool = True,
                   with_manifest: Optional[bool] = True) -> List[pathlib.Path]:
    """Catch-all for images used across different services and tools"""
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("common-images", pkg_path)
    manifest = StepsManifest(component)

    for image in _get_ecr_common_images():
        if image.short_name in skip_images:
            continue
        manifest.add_step(ImageStep.from_pullable_image(ctx, image, component_pkg_path))

    if namespace != system_namespace:
        skip_images[SYSTEM_CLIENT_IMAGE] = True

    custom_images = _get_custom_images()
    # image name / docker file / public base image / ecr base image
    for img, (target, path) in custom_images.items():
        if img in skip_images:
            continue
        img = build_image(ctx, img, path, target)
        image_step = ImageStep.from_pullable_image(ctx, img, component_pkg_path)
        if tag_latest:
            image_step = image_step.tag_latest()
        manifest.add_step(image_step)

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@common.system_namespace_option
@common.namespace_option
@pkg_path_option
@with_manifest_option
@pass_ctx
def common_images(
        ctx: context.CliCtx,
        system_namespace: str,
        namespace: str,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True) -> List[pathlib.Path]:
    """Package images common to several services"""
    return _common_images(ctx, system_namespace, namespace, pkg_path, with_manifest=with_manifest)


def _job_operator(
        ctx: context.CliCtx,
        system_namespace: str,
        namespace: str,
        pkg_path: Optional[pathlib.Path] = None,
        image: Optional[DockerImage] = None,
        image_file: Optional[pathlib.Path] = None,
        local_cross_build: bool = False,
        force_new_tag: bool = False,
        with_manifest: Optional[bool] = True,
        enable_multi_coordinator: Optional[bool] = False,
        disable_cluster_mode: bool = False,
        system_deploy_force: bool = True,
        pull_image: Optional[bool] = True) -> List[pathlib.Path]:
    """
    Package job-operator
    """
    generic_user_namespace = "generic-user-namespace"
    if namespace == generic_user_namespace:
        raise click.ClickException(f"namespace {generic_user_namespace} is reserved")

    image_file = fix_pathlib_path(image_file)
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("job-operator", pkg_path)
    manifest = StepsManifest(component)

    remote_log_path = _job_operator_values_resolve(ctx)['remote_log_path']
    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/job-operator/permission.sh.jinja2",
        {
            "remote_log_path": remote_log_path,
        }
    ))
    operator_image_name = "dummy"
    if not disable_cluster_mode and namespace != system_namespace:
        logger.info("skip user NS deploy for job operator by default with cluster mode, exit")
        return []
    elif image:
        operator_image_step = ImageStep.from_pullable_image(ctx, image, component_pkg_path, pull_image=pull_image)
    elif image_file:
        operator_image_step = ImageStep.from_file(image_file)
    else:
        logger.debug("--image or --image-file not set, building image...")
        local_build_option = "-local" if local_cross_build else ""
        git_hash_option = "" if not force_new_tag else f"{ctx.githash_make_param()}"
        stdout = ctx.must_exec(f"make -C {ctx.src_dir}/src/{component} docker-build{local_build_option} "
                               f"platform={ctx.build_platform()} {git_hash_option}".strip())
        operator_image = scrape_docker_image_name(stdout)
        operator_image_step = ImageStep.from_pullable_image(ctx, operator_image, component_pkg_path)
    if operator_image_step:
        manifest.add_step(operator_image_step)
        manifest.add_step(ImageStep.from_pullable_image(
            ctx,
            DockerImage.of(KUBE_RBAC_PROXY_IMAGE),
            component_pkg_path,
        ))
        operator_image_name = operator_image_step.image.with_registry(REGISTRY_URL)
    semantic_version = operator_image_step.image.get_labels(ctx).get('semantic_version', '')

    ctx.must_exec(f"mkdir -p {component_pkg_path}")

    # remove the old config directory and job-operator.yaml if any.
    system_config_path = component_pkg_path.joinpath("config-system")
    user_config_path = component_pkg_path.joinpath("config-user")
    ctx.must_exec(f"rm -rf {system_config_path} {user_config_path}")
    ctx.must_exec(f"rm -f {component_pkg_path}/{component}.{system_namespace}.yaml")
    ctx.must_exec(f"rm -f {component_pkg_path}/{component}.{generic_user_namespace}.yaml")

    # Best effort to find charts from provided dir or cb release dir and fallback to local build
    charts_file = None
    if image_file and image_file.name.startswith("job-operator-") and image_file.name.endswith(".docker"):
        version = image_file.name[len("job-operator-"):len(image_file.name) - len(".docker")]
        charts_file = image_file.parent.joinpath(f"job-operator-charts-{version}.tar.gz")
        if not charts_file.exists():
            charts_file = None
    elif image:
        # image tag example: "0.0.0-202503182329-5239-5543eb87", "2512-inference-202503172051-43-2aff6229"
        parts = image.tag.split("-")
        if len(parts) > 3:
            build_id = "-".join(parts[-3:])
            charts_file = pathlib.Path(
                f"/cb/artifacts/builds/cbcore/{build_id}/job-operator-charts-{image.tag}.tar.gz")
            if not charts_file.exists():
                charts_file = None

    if charts_file:
        logger.debug(f"Found charts tar file {charts_file}, untaring it into {component_pkg_path}")
        ctx.must_exec(f"tar xfz {charts_file} -C {component_pkg_path}")
    else:
        # Build the charts from the source tree
        ctx.must_exec(
            f"make -C {ctx.src_dir}/src/{component} build-charts")
        ctx.must_exec(f"tar xfz {ctx.src_dir}/src/job-operator/build/config.tar.gz -C {component_pkg_path}")

    _common_image_versions()

    ctx.must_exec(f"cp -r {component_pkg_path}/config {system_config_path}")
    ctx.must_exec(f"cp -r {component_pkg_path}/config {user_config_path}")

    ctx.must_exec(f"cp -r {ctx.resolve_dir('job-operator').absolute()}/. {system_config_path}")
    if system_deploy_force and namespace == system_namespace:
        ctx.must_exec("kustomize edit set annotation system-update:true", cwd=f"{system_config_path}/default")

    # We always generate 2 yaml file, one for system namespace, and one for a generic user namespace
    ctx.must_exec(f"kustomize edit set namespace {system_namespace}", cwd=f"{system_config_path}/default")
    ctx.must_exec(f"kustomize edit set nameprefix {system_namespace}-", cwd=f"{system_config_path}/default")

    ctx.must_exec(f"kustomize edit set annotation alpine-kubectl-tag:{ALPINE_KUBECTL_TAG} "
                  f"alpine-containerd-tag:{ALPINE_CONTAINERD_TAG}", cwd=f"{system_config_path}/manager")
    ctx.must_exec(f"kustomize edit set image controller={operator_image_name}", cwd=f"{system_config_path}/manager")
    ctx.must_exec(f"kustomize edit add configmap cluster-env "
                  f"--from-literal=SEMANTIC_VERSION={semantic_version} "
                  f"--from-literal=CRD_COMPILE_MIN_GI=CRD_COMPILE_MIN_GI_SED_REPLACE "
                  f"--from-literal=CRD_EXECUTE_MIN_GI=CRD_EXECUTE_MIN_GI_SED_REPLACE "
                  f"--from-literal=CP_RESERVED_MEM=CP_RESERVED_MEM_SED_REPLACE "
                  f"--from-literal=CP_RESERVED_CPU=CP_RESERVED_CPU_SED_REPLACE "
                  f"--from-literal=GENOA_SERVER=GENOA_SERVER_SED_REPLACE "
                  f"--from-literal=MIN_MEMX_PER_POP_NODEGROUP=10 "
                  f"--from-literal=PENDING_POD_TTL_SECONDS=300 "
                  f"--from-literal=NAMESPACE_SCALE_DOWN_CHECK_INTERVAL=1h "
                  f"--from-literal=SYSTEM_NAMESPACE={system_namespace} "
                  f"--from-literal=ENABLE_MULTI_COORDINATOR=ENABLE_MULTI_COORDINATOR_SED_REPLACE "
                  f"--from-literal=DISABLE_CLUSTER_MODE=DISABLE_CLUSTER_MODE_SED_REPLACE "
                  f"--from-literal=ENABLE_MANAGEMENT_SEPARATION=ENABLE_MANAGEMENT_SEPARATION_SED_REPLACE "
                  f"--from-literal=RESOURCE_DETAILS_MEMX_PRIMARY_GROUP_COUNT=12 "
                  f"--from-literal=RESOURCE_DETAILS_MEMX_SECONDARY_GROUP_COUNT=4 "
                  f"--from-literal=NODE_MIN_RESERVED_MEM=5Gi "
                  f"--from-literal=DISABLE_INFERENCE_JOB_SHARING=false ",
                  cwd=f"{system_config_path}/manager")

    kustomize_output = ctx.must_exec("kustomize build", cwd=f"{system_config_path}")
    yaml_path = component_pkg_path.joinpath(f"{component}.{system_namespace}.yaml")
    with open(yaml_path, "w") as f:
        f.write(kustomize_output)

    ctx.must_exec(f"cp -r {ctx.resolve_dir('job-operator').absolute()}/. {user_config_path}")
    ctx.must_exec(f"kustomize edit set namespace {generic_user_namespace}", cwd=f"{user_config_path}/default")
    ctx.must_exec(f"kustomize edit set nameprefix {generic_user_namespace}-", cwd=f"{user_config_path}/default")
    ctx.must_exec(f"kustomize edit set image controller={operator_image_name}", cwd=f"{user_config_path}/manager")
    ctx.must_exec(f"kustomize edit set annotation alpine-kubectl-tag:{ALPINE_KUBECTL_TAG} "
                  f"alpine-containerd-tag:{ALPINE_CONTAINERD_TAG}", cwd=f"{user_config_path}/manager")
    ctx.must_exec(f"kustomize edit add configmap cluster-env "
                  f"--from-literal=CRD_COMPILE_MIN_GI=CRD_COMPILE_MIN_GI_SED_REPLACE "
                  f"--from-literal=CRD_EXECUTE_MIN_GI=CRD_EXECUTE_MIN_GI_SED_REPLACE "
                  f"--from-literal=CP_RESERVED_MEM=CP_RESERVED_MEM_SED_REPLACE "
                  f"--from-literal=CP_RESERVED_CPU=CP_RESERVED_CPU_SED_REPLACE "
                  f"--from-literal=GENOA_SERVER=GENOA_SERVER_SED_REPLACE "
                  f"--from-literal=MIN_MEMX_PER_POP_NODEGROUP=10 "
                  f"--from-literal=PENDING_POD_TTL_SECONDS=300 "
                  f"--from-literal=SYSTEM_NAMESPACE={system_namespace} "
                  f"--from-literal=ENABLE_MULTI_COORDINATOR=ENABLE_MULTI_COORDINATOR_SED_REPLACE "
                  f"--from-literal=DISABLE_CLUSTER_MODE=True "
                  f"--from-literal=ENABLE_MANAGEMENT_SEPARATION=ENABLE_MANAGEMENT_SEPARATION_SED_REPLACE "
                  f"--from-literal=RESOURCE_DETAILS_MEMX_PRIMARY_GROUP_COUNT=12 "
                  f"--from-literal=RESOURCE_DETAILS_MEMX_SECONDARY_GROUP_COUNT=4 "
                  f"--from-literal=RESOURCE_DETAILS_MEMX_MEMORY=120Gi ",
                  cwd=f"{user_config_path}/manager")

    kustomize_output = ctx.must_exec("kustomize build", cwd=f"{user_config_path}")
    yaml_path = component_pkg_path.joinpath(f"{component}.{generic_user_namespace}.yaml")
    with open(yaml_path, "w") as f:
        # CRDs are cluster-scoped. To help avoid conflicts, we only apply them
        # to the system namespace.
        # Technically we should support versioning for CRD
        # https://cerebras.atlassian.net/browse/SW-92993?search_id=b119fb36-169c-4ad2-8f40-5183403f9f2d
        for doc in yaml.safe_load_all(kustomize_output):
            if doc.get("kind") != "CustomResourceDefinition":
                yaml.safe_dump(doc, f)
                f.write("---\n")

    manifest.add_step(
        ScriptStep(
            f"{APP_ROOT}/job-operator/apply-yaml.sh.jinja2",
            {
                "system_yaml_path": f"{component}.{system_namespace}.yaml",
                "user_yaml_path": f"{component}.{generic_user_namespace}.yaml",
                "namespace": namespace,
                "system_namespace": system_namespace,
                "system_deploy_force": system_deploy_force,
                "enable_multi_coordinator": enable_multi_coordinator,
                "disable_cluster_mode": disable_cluster_mode,
            }
        ))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


def _find_image_file(image_file_pattern: str) -> pathlib.Path:
    """ Find the image file based on a pattern. If such a file can't be found, raise an exception """
    try:
        matches = glob.glob(image_file_pattern)
        if len(matches) > 1:
            logger.warn(f"More than one image file found: {matches}. Use the first one.")
        return pathlib.Path(matches[0])
    except IndexError:
        raise click.ClickException(f"Can't find image with pattern {image_file_pattern}")


@click.command()
@pkg_path_option
@common.image_option
@common.image_file_option
@common.local_cross_build_option
@common.force_new_tag_option
@with_manifest_option
@common.system_namespace_option
@common.namespace_option
@common.systems_option
@common.enable_multi_coordinator_option
@common.parallel_compile_limit_option
@pass_ctx
def job_operator(
        ctx: context.CliCtx,
        system_namespace: str,
        namespace: str,
        parallel_compile_limit: int = 1,
        pkg_path: Optional[pathlib.Path] = None,
        image: Optional[DockerImage] = None,
        image_file: Optional[pathlib.Path] = None,
        local_cross_build: bool = False,
        force_new_tag: bool = False,
        enable_multi_coordinator: bool = False,
        systems: Optional[str] = None,
        with_manifest: Optional[bool] = True) -> List[pathlib.Path]:
    """Package job-operator"""
    return _job_operator(ctx, system_namespace, namespace,
                         pkg_path=pkg_path,
                         image=image,
                         image_file=image_file,
                         local_cross_build=local_cross_build,
                         force_new_tag=force_new_tag,
                         enable_multi_coordinator=enable_multi_coordinator,
                         with_manifest=with_manifest)


def _cbcore(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        image: Optional[DockerImage] = None,
        image_file: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> CbcoreManifest:
    """Package cbcore including preload step to each node."""
    image_file = fix_pathlib_path(image_file)
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("cbcore", pkg_path)

    if ctx.is_macos_build() and image is None and image_file is None:
        logger.info("skip building cbcore image on MacOS")
        return None

    manifest = StepsManifest(component)

    dummy_image = DockerImage.of(BUSYBOX_IMAGE)
    dummy_image_step = ImageStep.from_pullable_image(ctx, dummy_image, component_pkg_path)
    manifest.add_step(dummy_image_step)

    if image:
        cbcore_image_step = ImageStep.from_pullable_image(ctx, image, component_pkg_path)
        manifest.add_step(cbcore_image_step)
    elif image_file:
        cbcore_image_step = ImageStep.from_file(image_file)
        manifest.add_step(cbcore_image_step)
    else:
        raise click.ClickException("required argument --image or --image-file missing")
    cbcore_image_step.load_all_nodes()

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)], pkg_path)
    return CbcoreManifest(
        (component_pkg_path / script_path).relative_to(pkg_path),
        DockerImage.of(cbcore_image_step.image.local_name)
    )


def _image(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        image: Optional[DockerImage] = None,
        image_file: Optional[pathlib.Path] = None,
        pull_image: bool = True,
        load_all_nodes: bool = False,
        load_nodes: Optional[List[str]] = None,
        with_manifest: Optional[bool] = True,
        **kwargs,
) -> List[pathlib.Path]:
    """Package images in a generic way"""
    image_file = fix_pathlib_path(image_file)
    pkg_path = get_pkg_path(ctx, pkg_path)
    pkg_name = "image"
    if image:
        pkg_name = f"image-{image.format_name}"
    elif image_file:
        pkg_name = f"imagefile-{image_file.name.replace('.', '-')}"
    component, component_pkg_path = prepare_pkg_path(pkg_name, pkg_path)

    manifest = StepsManifest(component)
    if image:
        image_step = ImageStep.from_pullable_image(ctx, image, component_pkg_path, pull_image=pull_image)
    elif image_file:
        image_step = ImageStep.from_file(image_file)
    else:
        raise click.ClickException("required argument --image or --image-file missing")

    if load_all_nodes:
        image_step.load_all_nodes(load_nodes)
    else:
        image_step.cleanup_after_push()
    manifest.add_step(image_step)

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)], pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@common.image_option
@common.image_file_option
@with_manifest_option
@pass_ctx
def cbcore(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        image: Optional[DockerImage] = None,
        image_file: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True) -> CbcoreManifest:
    """Package cbcore"""
    return _cbcore(ctx, pkg_path, image, image_file, with_manifest)


def _cluster_server(
        ctx: context.CliCtx,
        namespace: str,
        system_namespace: str,
        pkg_path: Optional[pathlib.Path] = None,
        image: Optional[DockerImage] = None,
        image_file: Optional[pathlib.Path] = None,
        local_cross_build: bool = False,
        force_new_tag: bool = False,
        with_manifest: Optional[bool] = True,
        cbcore: Optional[str] = None,
        recreate_tls_secret: bool = False,
        wsjob_log_preferred_storage_type: Optional[str] = "",
        cached_compile_preferred_storage_type: Optional[str] = "",
        pull_image: Optional[bool] = True,
        system_deploy_force: bool = True,
        disable_user_auth: bool = False,
        use_isolated_dashboards: bool = False,
        disable_fabric_json_check: bool = False,
) -> List[pathlib.Path]:
    """
    Package ClusterServer from a given image or by building it from scratch.
    """
    image_file = fix_pathlib_path(image_file)
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("cluster-server", pkg_path)
    manifest = StepsManifest(component)
    manifest.include_files(
        f"{APP_ROOT}/cluster-server/config-ds.yaml.template",
        f"{APP_ROOT}/cluster-server/incr-config-ds.yaml.template",
        f"{APP_ROOT}/cluster-server/cluster-server-config.sh",
        f"{APP_ROOT}/cluster-server/cluster-server-config.yaml",
    )

    if image:
        cluster_image_step = ImageStep.from_pullable_image(ctx, image, component_pkg_path, pull_image=pull_image)
    elif image_file:
        cluster_image_step = ImageStep.from_file(image_file)
    else:
        logger.debug("--image or --image-file not set, building image...")
        local_build_option = "-local" if local_cross_build else ""
        git_hash_option = "" if not force_new_tag else f"{ctx.githash_make_param()}"
        stdout = ctx.must_exec(f"make -C {ctx.src_dir}/src cluster-docker-build{local_build_option} "
                               f"platform={ctx.build_platform()} {git_hash_option}".strip())
        cluster_image = scrape_docker_image_name(stdout)
        cluster_image_step = ImageStep.from_pullable_image(ctx, cluster_image, component_pkg_path)
    manifest.add_step(cluster_image_step)

    # Best effort to find charts from provided dir or cb release dir and fallback to local build
    charts_file = None
    if image_file and image_file.name.startswith("cluster-server-") and image_file.name.endswith(".docker"):
        version = image_file.name[len("cluster-server-"):len(image_file.name) - len(".docker")]
        charts_file = image_file.parent.joinpath(f"cluster-server-charts-{version}.tar.gz")
        if not charts_file.exists():
            charts_file = None
    elif image:
        # image tag example: "0.0.0-202503182329-5239-5543eb87", "2512-inference-202503172051-43-2aff6229"
        parts = image.tag.split("-")
        if len(parts) > 3:
            build_id = "-".join(parts[-3:])
            charts_file = pathlib.Path(
                f"/cb/artifacts/builds/cbcore/{build_id}/cluster-server-charts-{image.tag}.tar.gz")
            if not charts_file.exists():
                charts_file = None

    if charts_file:
        logger.debug(f"Found charts tar file {charts_file}, untaring it into {component_pkg_path}")
        ctx.must_exec(f"tar xfz {charts_file} -C {component_pkg_path}")
    else:
        ctx.must_exec(f"cp -r {ctx.src_dir}/src/cluster/server/charts {component_pkg_path}")

    cluster_override_file = ctx.resolve_file("cluster-server/values-override.yaml")
    ctx.must_exec(f"cp {cluster_override_file} {component_pkg_path}/charts")

    # Copy over the system maintenance scripts
    system_maintenance_scripts = ["vddc.py", "membist.py", "wafer_diag.py", "wafer_diag_wrapper.sh", "wafer_diag_init.sh"]
    system_maintenance_path = f"{ctx.src_dir}/src/system-maintenance"
    scripts_path = f"{component_pkg_path}/charts/scripts"
    ctx.must_exec(f"mkdir -p {scripts_path}")

    for script in system_maintenance_scripts:
        source_path = ctx.resolve_file(f"{system_maintenance_path}/{script}")
        ctx.must_exec(f"cp {source_path} {scripts_path}/{script}")

    is_cbcore_pullable = True
    cbcore_image = ""
    if cbcore:
        try:
            DockerImage.of(cbcore)
            cbcore_image = cbcore
        except:
            # if this is not a pullable image, assume that it is a file.
            cbcore_image_step = ImageStep.from_file(cbcore)
            is_cbcore_pullable = False
            cbcore_image = f"{REGISTRY_URL}/{cbcore_image_step.image}"

    # create the secret
    ssl_conf = ctx.resolve_file("cluster-server/ssl.conf.template")
    target_ssl_conf = component_pkg_path.joinpath("ssl.conf.template")
    target_ssl_conf.write_bytes(ssl_conf.read_bytes())

    manifest.add_step(ScriptStep(f"{APP_ROOT}/cluster-server/create-secret.sh.jinja2", {
        "recreate_secret": "true" if recreate_tls_secret else "false",
        "is_kind_cluster": "true" if ctx.in_kind_cluster else "false",
        "namespace": namespace,
        "system_namespace": system_namespace,
    }))

    supported_sidecar_images = [PYTHON38_IMAGE_SHORT_NAME, PYTHON311_IMAGE_SHORT_NAME]

    _common_image_versions()
    vals = _cluster_server_values_resolve(ctx)
    manifest.add_step(
        ScriptStep(f"{APP_ROOT}/cluster-server/helm-upgrade.sh.jinja2", {
            "namespace": namespace,
            "system_namespace": system_namespace,
            "cluster_image": cluster_image_step.image,
            "semantic_version": cluster_image_step.image.get_labels(ctx).get("semantic_version", ""),
            "cbcore_image": cbcore_image,
            "alpine_kubectl_tag": ALPINE_KUBECTL_TAG,
            "alpine_containerd_tag": ALPINE_CONTAINERD_TAG,
            "alpine_kube_user_auth_tag": ALPINE_KUBE_USER_AUTH_TAG,
            "remote_log_path": vals['remote_log_path'],
            "wsjob_log_preferred_storage_type": wsjob_log_preferred_storage_type,
            "cached_compile_preferred_storage_type": cached_compile_preferred_storage_type,
            "is_kind_cluster": ctx.in_kind_cluster,
            "system_deploy_force": system_deploy_force,
            "disable_user_auth": disable_user_auth,
            "use_isolated_dashboards": use_isolated_dashboards,
            "disable_fabric_json_check": disable_fabric_json_check,
            "system_client_tag": SYSTEM_CLIENT_TAG,
            "supported_sidecar_images": ",".join(supported_sidecar_images),
        })
    )
    manifest.add_step(ScriptStep(f"{APP_ROOT}/cluster-server/update-csctl-config.sh.jinja2", {
        "namespace": namespace,
        "system_namespace": system_namespace,
    }))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@common.namespace_option
@common.system_namespace_option
@common.image_option
@common.image_file_option
@common.local_cross_build_option
@common.force_new_tag_option
@with_manifest_option
@common.regen_tls_cert_option
@click.option("--cbcore",
              help="Cbcore image file or pullable image. If it is an image file, this image needs to be loaded into the cluster in the cbcore step.",
              type=str, required=False)
@pass_ctx
def cluster_server(
        ctx: context.CliCtx,
        namespace: str,
        system_namespace: str,
        pkg_path: Optional[pathlib.Path] = None,
        image: Optional[DockerImage] = None,
        image_file: Optional[str] = None,
        local_cross_build: bool = False,
        force_new_tag: bool = False,
        with_manifest: Optional[bool] = True,
        cbcore: Optional[str] = None,
        recreate_tls_secret: bool = False,
) -> List[pathlib.Path]:
    """
    Package ClusterServer from a given image or by building it from scratch.
    """
    return _cluster_server(ctx, namespace, system_namespace, pkg_path, image, image_file, local_cross_build,
                           force_new_tag, with_manifest, cbcore, recreate_tls_secret)


def _debugviz_server(
        ctx: context.CliCtx,
        namespace: str,
        system_namespace: str,
        image: Optional[DockerImage] = None,
        image_file: Optional[pathlib.Path] = None,
        pull_image: Optional[bool] = True,
        force: Optional[bool] = False,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
        recreate_tls_secret: bool = False,
) -> List[pathlib.Path]:
    """
    Package DebugVizServer by building it from scratch.
    """
    if ctx.in_kind_cluster and not force:
        logger.debug("skip installing debug viz server for kind without force option")
        return

    if namespace != system_namespace:
        return

    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("debugviz-server", pkg_path)
    manifest = StepsManifest(component)

    manifest.include_files(
        f"{APP_ROOT}/debugviz-server/ssl.conf.template",
        f"{APP_ROOT}/debugviz-server/charts",
    )

    debugviz_image_step = None
    image_file = fix_pathlib_path(image_file)
    if image:
        debugviz_image_step = ImageStep.from_pullable_image(ctx, image, component_pkg_path, pull_image=pull_image)
    elif image_file:
        debugviz_image_step = ImageStep.from_file(image_file)

    debugviz_image = ""
    if debugviz_image_step:
        manifest.add_step(debugviz_image_step)
        debugviz_image = debugviz_image_step.image.with_registry(REGISTRY_URL)
    else:
        raise click.ClickException("required argument --image or --image-file missing")

    manifest.add_step(ScriptStep(f"{APP_ROOT}/debugviz-server/create-secret.sh.jinja2", {
        "recreate_secret": "true" if recreate_tls_secret else "false",
        "is_kind_cluster": "true" if ctx.in_kind_cluster else "false",
        "system_namespace": system_namespace,
    }))

    manifest.add_step(
        ScriptStep(f"{APP_ROOT}/debugviz-server/helm-upgrade.sh.jinja2", {
            "system_namespace": system_namespace,
            "debugviz_image": debugviz_image,
            "is_kind_cluster": ctx.in_kind_cluster,
        })
    )

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@common.force_option
@common.namespace_option
@common.system_namespace_option
@common.image_option
@common.image_file_option
@with_manifest_option
@common.regen_tls_cert_option
@pass_ctx
def debugviz_server(
        ctx: context.CliCtx,
        namespace: str,
        system_namespace: str,
        image: Optional[DockerImage] = None,
        image_file: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
        recreate_tls_secret: bool = False,
) -> List[pathlib.Path]:
    """
    Package DebugVizServer by building it from scratch.
    """
    return _debugviz_server(ctx, namespace, system_namespace, 
                            image=image, 
                            image_file=image_file, 
                            force=force, 
                            pkg_path=pkg_path, 
                            with_manifest=with_manifest, 
                            recreate_tls_secret=recreate_tls_secret)


def _ceph(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package ceph.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path(CEPH_COMPONENT, pkg_path)
    _, image_pkg_path = prepare_pkg_path(IMAGES_SUBPATH, pkg_path)
    manifest = StepsManifest(component)
    manifest.include_files(
        f"{APP_ROOT}/ceph/webhook-config-template.yaml",
        f"{APP_ROOT}/ceph/chmod_writable_to_subdirs.sh",
    )
    manifest.add_step(ScriptStep(f"{APP_ROOT}/ceph/webhook-config-install.sh.jinja2", {}))

    charts_path = component_pkg_path.joinpath("charts")
    charts_path.mkdir(parents=True, exist_ok=True)

    version = "v1.13.9"
    ctx.must_exec("helm repo add rook-release https://charts.rook.io/release")
    ctx.must_exec("helm repo update rook-release")
    ctx.must_exec(f"helm pull rook-release/rook-ceph --version {version} --destination {charts_path}")
    ctx.must_exec(f"helm pull rook-release/rook-ceph-cluster --version {version} --destination {charts_path}")

    rook_ceph_values_override_yaml_file = \
        charts_path.joinpath("rook-ceph-values-override.yaml")
    rook_ceph_values_override_yaml_file.write_text(
        ctx.resolve_file("ceph/rook-ceph-values-override.yaml").read_text())

    rook_ceph_cluster_values_override_yaml_file = \
        charts_path.joinpath("rook-ceph-cluster-values-override.yaml")
    rook_ceph_cluster_values_override_yaml_file.write_text(
        ctx.resolve_file("ceph/rook-ceph-cluster-values-override.yaml").read_text())

    for _, image in ceph_pullable_images.items():
        manifest.add_step(ImageStep.from_pullable_image(ctx, image, image_pkg_path))

    manifest.include_files(f"{APP_ROOT}/ceph/rook-ceph-service-monitor.yaml")
    manifest.include_files(f"{APP_ROOT}/ceph/rook-ceph-exporter-service-monitor.yaml")
    manifest.include_files(f"{APP_ROOT}/ceph/rook-ceph-rbac.yaml")
    manifest.include_files(f"{APP_ROOT}/ceph/rook-ceph-localrules.yaml")

    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/ceph/helm-upgrade.sh.jinja2",
        {
            "version": version,
            **ceph_pullable_images,
        },
        "helm-upgrade.sh"
    ))
    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@pass_ctx
def ceph(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package ceph.
    """
    return _ceph(ctx, pkg_path, with_manifest)


def _nvme_of(
        ctx: context.CliCtx,
        hosts: str = "-",
        namespace: str = "-",
        allow_reboot: bool = True,
        force: Optional[bool] = False,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package nvme-of.
    """
    if hosts == "":
        logger.info("no hosts were specified. Skip nvme-of deployment")
        return

    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("nvme-of", pkg_path)
    manifest = StepsManifest(component)
    manifest.include_files(
        f"{APP_ROOT}/nvme-of/inventory.yaml.template",
        f"{APP_ROOT}/nvme-of/preflight.yaml",
        f"{APP_ROOT}/nvme-of/nvme-targets-cm.yaml",
        f"{APP_ROOT}/nvme-of/sync-cluster-config-to-nodes.yaml",
        f"{APP_ROOT}/nvme-of/update-k8s-cluster-config.yaml",
        f"{APP_ROOT}/nvme-of/main.yaml",
        f"{APP_ROOT}/nvme-of/cleanup.yaml",
        f"{APP_ROOT}/nvme-of/nvme-target-enable.sh",
        f"{APP_ROOT}/nvme-of/nvme-target-enable.service",
        f"{APP_ROOT}/nvme-of/nvme-target-rename-rule.sh",
        f"{APP_ROOT}/nvme-of/nvme-target-udev.rules",
        f"{APP_ROOT}/nvme-of/nvme-initiator-enable.sh",
        f"{APP_ROOT}/nvme-of/nvme-initiator-enable.service",
        f"{APP_ROOT}/nvme-of/nvme-initiator-rename-rule.sh",
        f"{APP_ROOT}/nvme-of/nvme-initiator-udev.rules",
    )
    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/nvme-of/deploy.sh.jinja2",
        {
            "hosts": hosts,
            "namespace": namespace,
            "allow_reboot": "true" if allow_reboot else "false",
            "force": "true" if force else "false",
        }
    ))
    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@common.hosts_option
@click.option(
    '--namespace',
    help="Optional namespace for nvme-of deployment. If not present, it will use the hosts option.",
    show_default=True, default="-", type=click.STRING, required=False
)
@pass_ctx
def nvme_of(
        ctx: context.CliCtx,
        hosts: str = "-",
        namespace: str = "-",
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package nvme-of.
    """
    return _nvme_of(ctx,
                    hosts,
                    namespace,
                    allow_reboot=True,
                    # SW-156424: One scalability issue was discovered just before rel-2.4 was due.
                    # Disabling the NVMe-oF deployment until the issue is resolved.
                    force=False,
                    pkg_path=pkg_path,
                    with_manifest=with_manifest)


def _cilium(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package cilium.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("cilium", pkg_path)
    _, images_pkg_path = prepare_pkg_path(IMAGES_SUBPATH, pkg_path)

    values_yaml_file = component_pkg_path.joinpath("values.yaml")
    values_yaml_file.write_text(ctx.resolve_file("cilium/values.yaml").read_text())
    values_override_yaml_file = pkg_path.joinpath(component, "values-override.yaml")
    values_override_yaml_file.write_text(ctx.resolve_file("cilium/values-override.yaml").read_text())

    # https://github.com/cilium/cilium/blob/master/Documentation/installation/cli-download.rst
    cli_file = "cilium-linux-amd64.tar.gz"
    cli_url = f"https://github.com/cilium/cilium-cli/releases/download/{CILIUM_CLI_VERSION}/{cli_file}"
    cli_sum_url = f"{cli_url}.sha256sum"
    cli_sum_file = f"{cli_file}.sha256sum"
    ctx.must_exec(f"curl -L --fail --retry 3 {cli_url} -o {component_pkg_path}/{cli_file}")
    ctx.must_exec(f"curl -L --fail --retry 3 {cli_sum_url} -o {component_pkg_path}/{cli_sum_file}")
    ctx.must_exec(f"shasum -a 256 -c {cli_sum_file}", cwd=component_pkg_path)

    ctx.must_exec("helm repo add cilium https://helm.cilium.io")
    ctx.must_exec("helm repo update cilium")
    ctx.must_exec(f"helm pull cilium/cilium --version {CILIUM_VERSION} --destination {component_pkg_path}")

    manifest = StepsManifest(component)

    for _, image in cilium_pullable_images.items():
        manifest.add_step(ImageStep.from_pullable_image(ctx, image, images_pkg_path))

    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/cilium/helm-upgrade.sh.jinja2",
        {
            "cilium_version": CILIUM_VERSION,
            **cilium_pullable_images,
        },
        "helm-upgrade.sh"
    ))
    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@pass_ctx
def cilium(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package cilium.
    """
    return _cilium(ctx, pkg_path, with_manifest)


def _nginx(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True
) -> List[pathlib.Path]:
    """
    Package ingress-nginx.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("nginx", pkg_path)

    # download the helm charts
    ctx.must_exec("helm repo add nginx https://kubernetes.github.io/ingress-nginx")
    ctx.must_exec("helm repo update nginx")
    ctx.must_exec(f"helm pull nginx/ingress-nginx --version 4.12.1 --destination {component_pkg_path}")

    manifest = StepsManifest(component)

    nginx_ctrl_image = DockerImage.of(NGINX_CTRL_IMAGE)
    nginx_webhook_image = DockerImage.of(NGINX_WEBHOOK_IMAGE)
    pullable_images = [
        nginx_ctrl_image,
        nginx_webhook_image,
    ]

    for image in pullable_images:
        manifest.add_step(ImageStep.from_pullable_image(ctx, image, component_pkg_path))

    remote_log_path = _nginx_values_resolve(ctx)['remote_log_path']
    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/nginx/helm-upgrade.sh.jinja2",
        {
            "ctrl_image": nginx_ctrl_image,
            "webhook_image": nginx_webhook_image,
            "remote_log_path": remote_log_path,
        }
    ))

    manifest.include_files(
        ctx.resolve_file("nginx/values.yaml"),
        ctx.resolve_file("nginx/values-override.yaml"),
        ctx.resolve_file("nginx/nginx-snat-template.sh"),
        ctx.resolve_file("nginx/nginx-snat.service"),
    )

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


def _kube_vip(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: bool = True):
    """
    Package kube-vip.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("kube-vip", pkg_path)

    manifest = StepsManifest(component)
    manifest.include_files(
        f"{APP_ROOT}/kube-vip/kube-vip",
        f"{APP_ROOT}/kube-vip/values.yaml",
    )

    image_step = ImageStep.from_pullable_image(ctx, DockerImage.of(KUBE_VIP_IMAGE), component_pkg_path)

    manifest.add_step(image_step)
    manifest.add_step(ScriptStep(f"{APP_ROOT}/kube-vip/helm-upgrade.sh.jinja2", {
        "image_tag": image_step.image.tag,
    }))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@pass_ctx
def kube_vip(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: bool = True):
    """
    Package kube-vip.
    """
    return _kube_vip(ctx, pkg_path, with_manifest)


@click.command()
@pkg_path_option
@with_manifest_option
@pass_ctx
def nginx(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True
) -> List[pathlib.Path]:
    """
    Package ingress-nginx.
    """
    return _nginx(ctx, pkg_path, with_manifest)


def _binary_deps(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True
) -> List[pathlib.Path]:
    """
    Package helm.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("binary-deps", pkg_path)

    envs = {}
    for binary, url, sha256, version in BINARY_DEPS:
        tar_name = f"{binary}-{version}.tar.gz"
        download = component_pkg_path.joinpath(tar_name)
        ctx.must_exec(f"curl -SsL --fail --retry 3 {url} -o {download}")
        result = ctx.must_exec(f"openssl sha1 -sha256 {download}")
        checksum = result.split()[1].strip()
        if checksum != sha256:
            logger.error(f"Checksum for {binary} does not match: {checksum} (expected {sha256}")
            raise click.ClickException(f"{binary} checksum does not match")
        envs[f"{binary}_tar"] = tar_name
        envs[f"{binary}_version"] = version

    manifest = StepsManifest(component)
    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/binary-deps/install.sh.jinja2", envs
    ))

    manifest.include_files(f"{APP_ROOT}/binary-deps/binary-deps-installer.sh",
                           f"{APP_ROOT}/binary-deps/binary-deps-installer-ds.yaml",
                           )

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@pass_ctx
def binary_deps(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True
) -> List[pathlib.Path]:
    """
    Package binary_deps.
    """
    return _binary_deps(ctx, pkg_path, with_manifest)


def _registry(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: bool = True):
    """
    Package registry and configure all nodes' containerd to use this registry.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("registry", pkg_path)
    _, images_pkg_path = prepare_pkg_path(IMAGES_SUBPATH, pkg_path)

    manifest = StepsManifest(component)
    # copy the config files to the package
    manifest.include_files(
        str(ctx.resolve_file("registry/config.toml")),  # kind uses a different config
        str(ctx.resolve_file("registry/registry-template.yaml")),
        str(ctx.resolve_file("registry/registry-service-monitor.yaml")),
        str(ctx.resolve_file("registry/registry-functions.sh")),
        str(ctx.resolve_file("registry/static-registry.sh")),
        str(ctx.resolve_file("registry/private-registry.sh")),
        str(ctx.resolve_file("registry/registry-installer.sh")),
        str(ctx.resolve_file("registry/registry-installer-ds.yaml")),
    )

    for image in PAUSE_IMAGES:
        pause_image_step = ImageStep.from_pullable_image(
            ctx,
            DockerImage.of(image),
            images_pkg_path).skip_push().load_mgmt_nodes()
        manifest.add_step(pause_image_step)
    # no need to push registry image itself
    registry_image_step = ImageStep.from_pullable_image(
        ctx,
        registry_pullable_images["registry_image"],
        images_pkg_path).skip_push().load_mgmt_nodes()
    manifest.add_step(registry_image_step)
    manifest.add_step(ScriptStep(f"{APP_ROOT}/registry/install.sh.jinja2", {
        "registry_image": registry_image_step.image.short_name,
    }))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@pass_ctx
def registry(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: bool = True):
    """
    Package registry and configure all nodes' containerd to use this registry.
    """
    return _registry(ctx, pkg_path, with_manifest)


def _registry_sync(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: bool = True):
    """
    Package registry_sync to reload images to the private registry.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path(REGISTRY_SYNC_COMPONENT, pkg_path)
    _, images_pkg_path = prepare_pkg_path(IMAGES_SUBPATH, pkg_path)

    pullable_images = {
        **ceph_pullable_images,
        **cilium_pullable_images,
        **registry_pullable_images,
        **kube_vip_pullable_images,
        **multus_pullable_images,
        **kube_webhook_pullable_images,
    }

    manifest = StepsManifest(component)
    for _, image in pullable_images.items():
        manifest.add_step(ImageStep.from_pullable_image(ctx, image, images_pkg_path))
    for image in PAUSE_IMAGES:
        manifest.add_step(ImageStep.from_pullable_image(ctx, DockerImage.of(image), images_pkg_path))

    manifest.add_step(ScriptStep(f"{APP_ROOT}/registry-sync/sync-kubeadm-images.sh", {}))
    manifest.set_skip_step(
        ScriptStep(
            f"{APP_ROOT}/registry-sync/check-need-reload.sh",
            {}
        ))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@pass_ctx
def registry_sync(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: bool = True):
    """
    Package registry_sync to reload images to the private registry. This is useful when supporting
    multiple management nodes. The registry will be running on the shared storage, which will be created
    after some components have been installed. So the new registry instance won't have those images loaded.
    """
    return _registry_sync(ctx, pkg_path, with_manifest)


def _kafka(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        validate_only: Optional[bool] = False,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package Kafka.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("kafka", pkg_path)

    kafka_pkg_name = "kafka"
    chart_version = "23.0.1"
    image_repository = "bitnami-kafka"
    image_tag = "3.5.0-debian-11-r1"
    ctx.must_exec("helm repo add bitnami https://charts.bitnami.com/bitnami")
    ctx.must_exec("helm repo update bitnami")
    ctx.must_exec(f"helm pull bitnami/kafka --version {chart_version} --destination {component_pkg_path}")

    manifest = StepsManifest(component)
    manifest.add_step(ImageStep.from_pullable_image(ctx, DockerImage.of(KAFKA_IMAGE), component_pkg_path))
    manifest.add_step(ScriptStep(f"{APP_ROOT}/kafka/label-kafka-nodes.sh", {}))
    manifest.include_files(f"{APP_ROOT}/kafka/webhook-config-template.yaml")
    manifest.add_step(ScriptStep(f"{APP_ROOT}/kafka/webhook-config-install.sh.jinja2", {}))
    manifest.include_files(f"{APP_ROOT}/kafka/custom-hostpath-sc.yaml.template")
    manifest.include_files(f"{APP_ROOT}/kafka/hostpath-pv.yaml.template")
    manifest.include_files(f"{APP_ROOT}/kafka/values-override.yaml")
    manifest.include_files(f"{APP_ROOT}/kafka/kafka-validation-job.yaml")
    manifest.include_files(f"{APP_ROOT}/kafka/cleanup.sh")

    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/kafka/helm-upgrade.sh.jinja2",
        {
            "kafka_pkg_name": kafka_pkg_name,
            "kafka_pkg_version": chart_version,
            "image_repository": image_repository,
            "image_tag": image_tag,
            # The stateful only allows a limited fields to be updated.
            # The "force" option can be used to redeploy the statefulset after deletion.
            # The persistent layer should be intact from redeploy.
            "force": "true" if force else "false",
            "validate_only": "true" if validate_only else "false",
            "is_kind_cluster": "true" if ctx.in_kind_cluster else "false",
        },
    ))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@common.force_option
@with_manifest_option
@pass_ctx
def kafka(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        with_manifest: Optional[bool] = True,
):
    """
    Package kafka.
    """
    # SW-109779
    # We will revive kafka when the adoption plan is clear
    # return _kafka(ctx, pkg_path, force, with_manifest)
    pass


def _cluster_tools(
        ctx: context.CliCtx,
        system_namespace: str,
        namespace: str,
        image_tag: str = "",
        pkg_path: Optional[pathlib.Path] = None,
        tar_file: Optional[pathlib.Path] = None,
        with_manifest: bool = True,
):
    """
    Package cluster_tools. If the tarfile is given, package it. Otherwise, find csctl
    and cluster-volumes in the source tree and package them.
    """
    tar_file = fix_pathlib_path(tar_file)
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("cluster-tools", pkg_path)

    if not tar_file and image_tag:
        res = image_tag.split("-", 1)
        if len(res) > 1:
            date = res[1]
            tar_file = pathlib.Path(
                f"/cb/artifacts/builds/cbcore/{date}/cluster-mgmt-tools-{image_tag}.tar.gz")
            if not tar_file.exists():
                tar_file = None

    if not tar_file:
        tar_file = component_pkg_path.joinpath("cluster-mgmt-tools.tar.gz").absolute()
        # generally installs will be for multibox systems which are linux+amd64
        arch_args = "GOOS=linux" if ctx.in_kind_cluster else "GOOS=linux GOARCH=amd64"
        tools_archive = f"TOOLS_ARCHIVE={tar_file}"
        ctx.must_exec(f"make -C {ctx.src_dir}/src cluster-mgmt-tools-archive {arch_args} {tools_archive}")
    else:
        ctx.must_exec(f"cp {tar_file} {component_pkg_path}/cluster-mgmt-tools.tar.gz")

    manifest = StepsManifest(component)

    manifest.include_files(
        f"{APP_ROOT}/cluster-tools/README.md",
        f"{APP_ROOT}/cluster-tools/software-validation.sh",
        f"{APP_ROOT}/cluster-tools/network-validation.sh",
        f"{APP_ROOT}/cluster-tools/access-cached-compile-job.yaml",
        f"{APP_ROOT}/cluster-tools/access-log-export-job.yaml",
        f"{APP_ROOT}/cluster-tools/access-debug-artifact-job.yaml",
        f"{APP_ROOT}/cluster-tools/cluster-tools-installer.sh",
        f"{APP_ROOT}/cluster-tools/cluster-tools-installer-ds.yaml",
    )

    manifest.add_step(ScriptStep(f"{APP_ROOT}/cluster-tools/deploy-tools.sh.jinja2", {
        "namespace": namespace,
        "system_namespace": system_namespace,
    }))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@common.tools_tar_file_option
@with_manifest_option
@common.namespace_option
@common.system_namespace_option
@pass_ctx
def cluster_tools(
        ctx: context.CliCtx,
        namespace: str,
        system_namespace: str,
        pkg_path: Optional[pathlib.Path] = None,
        tools_tar_file: Optional[pathlib.Path] = None,
        with_manifest: bool = True):
    """
    Package csctl.
    """
    return _cluster_tools(ctx, system_namespace=system_namespace, namespace=namespace,
                          pkg_path=pkg_path, tar_file=tools_tar_file, with_manifest=with_manifest)


def _gather_usernode_build_artifacts_monolith(ctx: context.CliCtx) -> List[pathlib.Path]:
    """ Gather artifacts from the source tree this is executing in. """
    artifacts = [f for f in (ctx.src_dir / "src" / "csctl").glob("csctl*.md")]
    artifacts.append(ctx.resolve_file("usernode/README.md"))
    return artifacts


def _gather_usernode_release_artifacts(ctx: context.CliCtx, release_artifact_path: pathlib.Path) -> List[pathlib.Path]:
    """ Gathers wheels, csctl, and the linker lib using artifacts at the given path. """
    usernode_artifacts = [whl for whl in release_artifact_path.glob("*.whl")]
    if not usernode_artifacts:
        raise ValueError(f"No wheels found in {release_artifact_path}")

    for tools in release_artifact_path.glob("cluster-mgmt-tools*.tar.gz"):
        tools_file = pathlib.Path(tools).name
        tools_path = f"{ctx.build_dir}/{tools_file[:-len('.tar.gz')]}"
        ctx.must_exec(f"cp {tools} {ctx.build_dir}/{tools_file}")
        ctx.must_exec(f"mkdir -p {tools_path}")
        ctx.must_exec(f"tar xfz {ctx.build_dir}/{tools_file} -C {tools_path} ")
        get_token_bin = pathlib.Path(f"{tools_path}/{GET_CEREBRAS_TOKEN_BINARY}")
        if get_token_bin.is_file():
            usernode_artifacts.append(get_token_bin)
        usernode_artifacts.append(pathlib.Path(f"{tools_path}/csctl"))
    return usernode_artifacts


def _gather_built_wheels() -> List[pathlib.Path]:
    """ Returns a list of appliance, and pytorch wheels if ALL found, else an empty list. """
    monolith_path = os.environ.get("GITTOP")
    if not monolith_path:
        logger.warning("GITTOP not set, cannot gather wheels")
        return list()
    build_root = pathlib.Path(monolith_path) / "build"

    rv = list()
    for wheel_dir in ["appliance", "cerebras_pytorch"]:
        wheel_files = glob.glob(str(build_root / wheel_dir / "*.whl"))
        if not wheel_files:
            logger.warning(f"No wheels found in {build_root / wheel_dir}")
            return list()
        for wheel_file in wheel_files:
            rv.append(pathlib.Path(wheel_file))
    return rv


def _usernode(
        ctx: context.CliCtx,
        system_namespace: str,
        namespace: str,
        overwrite_usernode_configs: bool,
        pkg_path: Optional[pathlib.Path],
        release_artifact_path: Optional[pathlib.Path] = None,
        config_only: bool = False,
        reset_usernode_configs: bool = False,
        skip_install_default_csctl: bool = False,
        enable_usernode_monitoring: bool = False,
        with_manifest: bool = True):
    """
    Package the usernode. This takes usernode artifacts and packages them into a tarball which is then
    staged on the mgmt node to be later copied and installed on the usernode.
    """
    release_artifact_path = fix_pathlib_path(release_artifact_path)
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("usernode", pkg_path)

    version = None
    if reset_usernode_configs:
        config_only = True
        overwrite_usernode_configs = True
    if release_artifact_path and not config_only:
        try:
            build_info = json.loads(release_artifact_path.joinpath("buildinfo-cbcore.json").read_text())
            if "version" in build_info and "buildid" in build_info:
                version = build_info.get("version") + "-" + build_info.get("buildid")
        except Exception:
            raise ValueError(f"Failed to get version from {release_artifact_path}/buildinfo-cbcore.json")
    else:
        version = _get_pkg_version(pkg_path)

    if not release_artifact_path:
        usernode_artifacts = []
        if not config_only:
            logger.info("Collecting wheels from monolith build")
            usernode_artifacts.extend(_gather_built_wheels())
            if not usernode_artifacts:
                raise ValueError("Not all required wheels found in $GITTOP/build/**, you may need to build them "
                                 "or run with --skip-wheels")

        usernode_artifacts.append(ctx.src_dir / "src" / "kube-user-auth" / f"{GET_CEREBRAS_TOKEN_BINARY}")
        usernode_artifacts.append(ctx.src_dir / "src" / "csctl" / "csctl")
    else:
        logger.info(f"Using release artifacts from {release_artifact_path}")
        usernode_artifacts = _gather_usernode_release_artifacts(ctx, release_artifact_path)

    ctx.must_exec(f"docker pull {PROM_NODE_EXPORTER_IMAGE}")
    _, cid, _ = ctx.exec(f"docker create {PROM_NODE_EXPORTER_IMAGE}")
    ctx.must_exec(f"docker cp {cid.strip()}:/bin/node_exporter {component_pkg_path}/node_exporter")
    ctx.must_exec(f"docker rm {cid}")
    usernode_artifacts.append(component_pkg_path / "node_exporter")

    usernode_artifacts.append(ctx.resolve_file("usernode/install.sh"))
    usernode_artifacts.append(ctx.resolve_file("usernode/session_exporter.sh"))
    usernode_artifacts.extend(_gather_usernode_build_artifacts_monolith(ctx))

    # create a tarball all the artifacts
    def set_perms(tarinfo: tarfile.TarInfo):
        tarinfo.uid = tarinfo.gid = 0
        tarinfo.uname = tarinfo.gname = "root"
        is_executable = tarinfo.name.endswith(".so") or tarinfo.name.endswith(".sh")
        tarinfo.mode = 0o755 if is_executable else 0o644
        return tarinfo

    tarball = component_pkg_path / f"base-user-pkg-{version}.tar.gz"
    with tarfile.open(tarball, "w:gz") as tar:
        for fp in usernode_artifacts:
            file_name = fp.name
            if fp.is_symlink():
                fp = fp.resolve()
            tar.add(fp, arcname=f"base-user-pkg-{version}/{file_name}", filter=set_perms)

    ctx.must_exec(f"cp {ctx.apps_dir}/usernode/install.sh {component_pkg_path}/install.sh")

    manifest = StepsManifest(component)
    manifest.add_step(ScriptStep(f"{APP_ROOT}/usernode/stage-mgmt-node.sh.jinja2", {
        "system_namespace": system_namespace,
        "namespace": namespace,
        "update_config_only": "true" if config_only else "false",
        "overwrite_usernode_configs": "true" if overwrite_usernode_configs else "false",
        "reset_usernode_configs": "true" if reset_usernode_configs else "false",
        "skip_install_default_csctl": "true" if skip_install_default_csctl else "false",
        "enable_usernode_monitoring": "true" if enable_usernode_monitoring else "false",
    }))
    manifest.add_step(ScriptStep(f"{APP_ROOT}/usernode/deploy-user.sh", {}))
    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@common.system_namespace_option
@common.namespace_option
@common.overwrite_usernode_configs_option
@common.reset_usernode_configs_option
@common.skip_install_default_csctl_option
@common.release_artifact_path_option
@common.config_only_option
@common.enable_usernode_monitoring_option
@with_manifest_option
@pass_ctx
def usernode(
        ctx: context.CliCtx,
        system_namespace: str,
        namespace: str,
        overwrite_usernode_configs: bool,
        pkg_path: Optional[pathlib.Path],
        release_artifact_path: Optional[pathlib.Path] = None,
        config_only: Optional[bool] = False,
        reset_usernode_configs: Optional[bool] = False,
        skip_install_default_csctl: Optional[bool] = False,
        enable_usernode_monitoring: Optional[bool] = False,
        with_manifest: Optional[bool] = True):
    """ Package usernode staging scripts. """
    return _usernode(ctx, system_namespace, namespace, overwrite_usernode_configs, pkg_path, release_artifact_path, config_only,
                     reset_usernode_configs, skip_install_default_csctl, enable_usernode_monitoring, with_manifest)


def _alert_router(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        with_manifest: Optional[bool] = True,
):
    """Package alert router."""
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("alert-router", pkg_path)
    manifest = StepsManifest(component)

    # Include relevant files for running
    manifest.include_files(f"{APP_ROOT}/alert-router/alert-router-deploy.yaml")
    manifest.include_files(f"{APP_ROOT}/alert-router/alert-classification.yaml")
    manifest.include_files(f"{APP_ROOT}/alert-router/wsjob-reader.yaml")

    monolith_path = os.environ.get("GITTOP")
    ALERT_ROUTER_DIR = pathlib.Path(monolith_path) / "src" / "cluster_mgmt" / "src" / "alert-router"

    # Add step for building docker
    git_hash= f"{ctx.githash_make_param()}"
    ALERT_ROUTER_TAG = subprocess.run(
        f"make -C {ALERT_ROUTER_DIR} version {git_hash} --no-print-directory",
        shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    ).stdout.strip()
    ALERT_ROUTER_IMAGE = f"{ECR_URL}/alert-router:{ALERT_ROUTER_TAG}"
    alert_router_image = build_image(
        ctx, ALERT_ROUTER_IMAGE,
        f"{ALERT_ROUTER_DIR}", f"docker-build {git_hash}"
    )

    # Reference image in manifest
    alert_router_image_step = ImageStep.from_pullable_image(
        ctx, alert_router_image,
        component_pkg_path
    )
    manifest.add_step(alert_router_image_step)

    # Deployment script that references the image and tag
    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/alert-router/alert-router-deploy.sh.jinja2",
        {
            "alert_router_tag": ALERT_ROUTER_TAG,
        }
    ))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest(
            [(component_pkg_path / script_path).relative_to(pkg_path)],
            pkg_path
        )
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


def _prometheus(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        dashboard_only: Optional[bool] = False,
        exporter_only: Optional[bool] = False,
        prom_only: Optional[bool] = False,
        force_new_tag: bool = False,
        with_manifest: bool = True):
    """
    Package prometheus.
    """
    if ctx.in_kind_cluster and not force:
        logger.debug("skip installing prometheus for kind")
        return

    include_dashboards = not (exporter_only or prom_only)
    include_exporters = not (dashboard_only or prom_only)
    include_core = not (dashboard_only or exporter_only)

    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("prometheus", pkg_path)
    manifest = StepsManifest(component)

    _common_image_versions()
    alert_image = DockerImage.of(PROM_ALERT_IMAGE)
    busybox_image = DockerImage.of(BUSYBOX_IMAGE)
    webhook_image = DockerImage.of(PROM_NGINX_WEBHOOK_CERTGEN_IMAGE)
    operator_image = DockerImage.of(PROM_OPERATOR_IMAGE)
    config_image = DockerImage.of(PROM_CONFIG_IMAGE)
    prom_image = DockerImage.of(PROM_IMAGE)
    thanos_image = DockerImage.of(PROM_THANOS_IMAGE)
    kubectl_image = DockerImage.of(ALPINE_KUBECTL_IMAGE)
    node_exporter_image = DockerImage.of(PROM_NODE_EXPORTER_IMAGE)
    snmp_exporter_image = DockerImage.of(PROM_SNMP_EXPORTER_IMAGE)
    sidecar_image = DockerImage.of(PROM_SIDECAR_IMAGE)
    grafana_image = DockerImage.of(PROM_GRAFANA_IMAGE)
    state_metrics_image = DockerImage.of(PROM_STATE_METRICS_IMAGE)
    prom_pkg_name = "kube-prometheus-stack"
    prom_pkg_version = "66.1.1"
    thanos_pkg_name = "thanos"
    thanos_pkg_version = "15.8.1"
    prom_version_for_grafana_config = PROM_VERSION_FOR_GRAFANA_CONFIG
    thanos_version_for_grafana_config = THANOS_VERSION_FOR_GRAFANA_CONFIG

    sflow_exporter_tag = subprocess.run(
        f"make -C {CURRENT_DIR}/../../cluster-mgmt-exporter/sflow version --no-print-directory",
        shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    ).stdout.strip()
    sflow_exporter_image_name = f"{ECR_URL}/sflow-exporter:{sflow_exporter_tag}"
    sflow_exporter_image = DockerImage.of(sflow_exporter_image_name)

    cluster_mgmt_exporter_image = DockerImage(repo="", tag="")

    if include_exporters:
        git_hash_option = "" if not force_new_tag else f"{ctx.githash_make_param()}"

        # Define Cluster-Mgmt Exporter image
        PROM_CLUSTER_MGMT_EXPORTER_TAG = _validate_version_string(
            subprocess.run(
                f"make -C {CURRENT_DIR}/../../cluster-mgmt-exporter version {git_hash_option} --no-print-directory",
                shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            ).stdout.strip(),
            f"cluster-mgmt-exporter version {git_hash_option}")
        PROM_CLUSTER_MGMT_EXPORTER_IMAGE = f"{ECR_URL}/cluster-mgmt-exporter:{PROM_CLUSTER_MGMT_EXPORTER_TAG}"
        cluster_mgmt_exporter_image = \
            build_image(
                ctx, PROM_CLUSTER_MGMT_EXPORTER_IMAGE,
                f"{CURRENT_DIR}/../../cluster-mgmt-exporter", f"docker-build {git_hash_option}"
            )
        cluster_mgmt_exporter_image_step = ImageStep.from_pullable_image(
            ctx, cluster_mgmt_exporter_image,
            component_pkg_path
        )
        manifest.add_step(cluster_mgmt_exporter_image_step)

        # Add other prometheus files
        manifest.include_files(f"{APP_ROOT}/prometheus/cluster-mgmt-exporter-rbac.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/system-exporter-deploy.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/system_probe_targets_generate.sh")
        manifest.include_files(f"{APP_ROOT}/prometheus/ipmi-probes.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/node-exporter-daemonset.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/linkmon-switch-exporter-deployment.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/sflow-exporter-daemonset.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/snmp-exporter-deploy.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/snmp.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/net_json_to_configmap.sh")
        manifest.include_files(f"{APP_ROOT}/prometheus/net_json_to_configmap_cronjob")
        manifest.include_files(f"{APP_ROOT}/prometheus/cluster-config-sync.sh")
        manifest.include_files(f"{APP_ROOT}/prometheus/values-remotewrite.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/values-vast-scrape.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/values-prometheus-networkpolicy.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/annotate-stock-alerts.sh")
        manifest.include_files(f"{APP_ROOT}/prometheus/stock-alerts-dashboards.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/grafana-alerting-cluster-mgmt.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/slack-notification.tmpl")
        manifest.include_files(f"{APP_ROOT}/prometheus/alertmanager-slack-config-base.yaml")
        manifest.include_files(f"{ctx.src_dir}/src/cluster-mgmt-exporter/generate_interface_list.py")
        manifest.include_files(f"{ctx.src_dir}/src/cluster-mgmt-exporter/node-health-check.sh")

        for image in [
            sflow_exporter_image,
            snmp_exporter_image,
        ]:
            manifest.add_step(ImageStep.from_pullable_image(ctx, image, component_pkg_path))

    if include_core:
        values_file = component_pkg_path.joinpath("values.yaml")
        values_file.write_text(ctx.resolve_file('prometheus/values.yaml').read_text())
        values_override_file = component_pkg_path.joinpath("values-override.yaml")
        values_override_file.write_text(
            ctx.resolve_file("prometheus/values-override.yaml").read_text()
        )
        values_override_multi_nodes_file = component_pkg_path.joinpath("values-override-multi-nodes.yaml")
        values_override_multi_nodes_file.write_text(
            ctx.resolve_file("prometheus/values-override-multi-nodes.yaml").read_text()
        )
        hostpath_pv_file = component_pkg_path.joinpath("hostpath-pv.yaml")
        hostpath_pv_file.write_text(ctx.resolve_file('prometheus/hostpath-pv.yaml').read_text())
        values_remotewrite_file = component_pkg_path.joinpath("values-remotewrite.yaml")
        values_remotewrite_file.write_text(
            ctx.resolve_file("prometheus/values-remotewrite.yaml").read_text()
        )

        for image in [
            alert_image,
            busybox_image,
            webhook_image,
            operator_image,
            config_image,
            prom_image,
            thanos_image,
            node_exporter_image,
            sidecar_image,
            grafana_image,
            state_metrics_image,
        ]:
            manifest.add_step(ImageStep.from_pullable_image(ctx, image, component_pkg_path))

        manifest.include_files(f"{APP_ROOT}/prometheus/thanos-values.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/rules.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/haproxy-service-monitor.yaml")

        ctx.must_exec(
            "helm repo add prometheus-community https://prometheus-community.github.io/helm-charts"
        )
        ctx.must_exec("helm repo update prometheus-community")
        ctx.must_exec(
            f"helm pull prometheus-community/{prom_pkg_name} --version {prom_pkg_version} "
            f"--destination {component_pkg_path}"
        )

        ctx.must_exec(
            "helm repo add bitnami https://charts.bitnami.com/bitnami"
        )
        ctx.must_exec("helm repo update bitnami")
        ctx.must_exec(
            f"helm pull bitnami/{thanos_pkg_name} --version {thanos_pkg_version} "
            f"--destination {component_pkg_path}"
        )

        manifest.add_step(
            ScriptStep(
                f"{APP_ROOT}/prometheus/create-tls-secrets.sh", {}
            )
        )

    # Always run this step - the script handles the _only flags internally
    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/prometheus/helm-upgrade.sh.jinja2",
        {
            "prom_pkg_name": prom_pkg_name,
            "prom_pkg_version": prom_pkg_version,
            "prom_app_version": PROM_APP_VERSION,
            "thanos_pkg_name": thanos_pkg_name,
            "thanos_pkg_version": thanos_pkg_version,
            "prom_version_for_grafana_config": prom_version_for_grafana_config,
            "thanos_version_for_grafana_config": thanos_version_for_grafana_config,
            "alert_image": alert_image,
            "busybox_image": busybox_image,
            "webhook_image": webhook_image,
            "operator_image": operator_image,
            "config_image": config_image,
            "thanos_image": thanos_image,
            "prom_image": prom_image,
            "kubectl_image": kubectl_image,
            "cluster_mgmt_exporter_image": cluster_mgmt_exporter_image,
            "cluster_mgmt_exporter_tag": cluster_mgmt_exporter_image.tag,
            "exporter_image": node_exporter_image,
            "sflow_exporter_tag": sflow_exporter_tag,
            "snmp_exporter_image": snmp_exporter_image,
            "snmp_exporter_tag": snmp_exporter_image.tag,
            "grafana_image": grafana_image,
            "sidecar_image": sidecar_image,
            "state_metrics_image": state_metrics_image,
            "dashboard_only": dashboard_only,
            "exporter_only": exporter_only,
            "prom_only": prom_only,
        }
    ))

    # Run the dashboard scripts last, since they need grafana to be up (and post-restart, if that would happen)
    if include_dashboards:
        ctx.must_exec(f"cp -r {ctx.apps_dir}/prometheus/dashboards {component_pkg_path}")
        git_describe = subprocess.run(
            f"git describe", shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        ).stdout.strip()

        if git_describe:
            fields_monitoring_branch = re.match(r"(monitoring(?:-v\d+)?)-(.*)", git_describe)
            fields_release = re.match(r"Cerebras_CS\d-(\d+\.\d+\.\d+)-.*-g(.*)$", git_describe)
            fields_other = re.match(r".*-g(.*)$", git_describe)

            if fields_monitoring_branch: # if it is a monitoring weekly branch
                monitoring_pkg_name = fields_monitoring_branch.group(1)
                monitoring_version = fields_monitoring_branch.group(2)
            elif fields_release: # if it is a proper release
                monitoring_pkg_name = "monitoring"
                monitoring_version = fields_release.group(1)
            elif fields_other: # if it is from anywhere else
                monitoring_pkg_name = "monitoring-test"
                monitoring_version = f"0.1.1+{fields_other.group(1)}"
            else: # shouldn't happen
                monitoring_pkg_name = "monitoring-test"
                monitoring_version = f"0.1.0"

        else:
            monitoring_pkg_name = "monitoring-test"
            monitoring_version = "0.1.0"

        ctx.must_exec(f"cp -r {ctx.apps_dir}/prometheus/monitoring {component_pkg_path}")
        manifest.include_files(f"{APP_ROOT}/prometheus/monitoring-dashboard-helper.py")

        manifest.add_step(ScriptStep(
            f"{APP_ROOT}/prometheus/monitoring.sh.jinja2",
            {
                "monitoring_pkg_name": monitoring_pkg_name,
                "monitoring_version": monitoring_version,
            }
        ))

        # run this last so we catch both old folders and the new monitoring folders
        manifest.include_files(f"{APP_ROOT}/prometheus/grafana-setup.yaml")
        manifest.include_files(f"{APP_ROOT}/prometheus/grafana-setup.sh")
        manifest.add_step(ScriptStep(
            f"{APP_ROOT}/prometheus/launch-grafana-setup.sh.jinja2",
            {
                "alpine_containerd_tag": ALPINE_CONTAINERD_TAG,
            }
        ))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest(
            [(component_pkg_path / script_path).relative_to(pkg_path)],
            pkg_path
        )
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@common.force_option
@with_manifest_option
@pass_ctx
def prometheus(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        with_manifest: Optional[bool] = True):
    """
    Package prometheus.
    """
    return _prometheus(ctx, pkg_path, force,
                       dashboard_only=False,
                       exporter_only=False,
                       prom_only=False,
                       force_new_tag=False,
                       with_manifest=with_manifest)

def _cpingmesh(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        with_manifest: Optional[bool] = True,
):
    """
    Package cpingmesh.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("cpingmesh", pkg_path)
    manifest = StepsManifest(component)

    # include the needed yamls
    manifest.include_files(f"{APP_ROOT}/cpingmesh/cping-agent-daemonset.yaml")
    manifest.include_files(f"{APP_ROOT}/cpingmesh/cping-controller.yaml")
    manifest.include_files(f"{APP_ROOT}/cpingmesh/cping-viz.yaml")

    # include the script to create secrets
    manifest.include_files(f"{APP_ROOT}/cpingmesh/create-tls-secrets.sh")

    monolith_path = os.environ.get("GITTOP")
    pingmesh_dir = pathlib.Path(monolith_path) / "src" / "hostio" / "pingmesh"
    cping_docker_tag = subprocess.run(
        f"make -C {pingmesh_dir} gittag --no-print-directory",
        shell=True, check=True, stdout=subprocess.PIPE, text=True
    ).stdout.strip()
    cping_docker_repo = subprocess.run(
        f"make -C {pingmesh_dir} repository --no-print-directory",
        shell=True, check=True, stdout=subprocess.PIPE, text=True
    ).stdout.strip()
    subprocess.run(
        f"make -C {pingmesh_dir} containers",  shell=True, check=True
    )

    cping_agent = f"{cping_docker_repo}:agent-{cping_docker_tag}"
    cping_controller = f"{cping_docker_repo}:controller-{cping_docker_tag}"
    cping_viz = f"{cping_docker_repo}:viz-{cping_docker_tag}"

    # add the required docker images
    for image in [
        DockerImage.of(cping_agent),
        DockerImage.of(cping_controller),
        DockerImage.of(cping_viz)
    ]:
        manifest.add_step(ImageStep.from_pullable_image(ctx, image, component_pkg_path))

    # deployment script
    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/cpingmesh/cpingmesh-deploy.sh.jinja2",
        {
            "cping_agent_tag": cping_agent,
            "cping_controller_tag": cping_controller,
            "cping_viz_tag": cping_viz
        }
    ))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest(
            [(component_pkg_path / script_path).relative_to(pkg_path)],
            pkg_path
        )
    return [(component_pkg_path / script_path).relative_to(pkg_path)]

@click.command()
@pkg_path_option
@common.force_option
@with_manifest_option
@pass_ctx
def cpingmesh(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        with_manifest: Optional[bool] = True,
):
    """
    Package cpingmesh.
    """
    return _cpingmesh(ctx, pkg_path, force, with_manifest)


@click.command()
@pkg_path_option
@common.force_option
@with_manifest_option
@pass_ctx
def alert_router(
    ctx: context.CliCtx,
    pkg_path: Optional[pathlib.Path] = None,
    force: Optional[bool] = False,
    with_manifest: Optional[bool] = True,
):
    """
    Package alertrouter.
    """
    return _alert_router(ctx, pkg_path, force, with_manifest)


def _log_scraping(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        with_manifest: bool = True):
    """
    Package log scraping.
    """
    if ctx.in_kind_cluster and not force:
        logger.debug("skip installing log scraping for kind")
        return

    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("log-scraping", pkg_path)
    manifest = StepsManifest(component)

    loki_image = DockerImage.of(LOKI_IMAGE)
    loki_pkg_name = "loki"
    loki_pkg_version = "5.10.0"

    fluentbit_image = DockerImage.of(FLUENTBIT_IMAGE)
    fluentbit_pkg_name = "fluent-bit"
    fluentbit_pkg_version = "0.40.0"

    with open(ctx.resolve_file('log-scraping/loki/hostpath-pv.yaml'), 'r') as pv_file:
        pv_content = yaml.safe_load(pv_file)
    remote_storage_path = pv_content['spec']['hostPath']['path']

    ctx.must_exec(
        "helm repo add grafana https://grafana.github.io/helm-charts")
    ctx.must_exec("helm repo update grafana")
    ctx.must_exec(f"helm pull grafana/{loki_pkg_name} --version {loki_pkg_version} "
                  f"--destination {component_pkg_path}")

    ctx.must_exec(
        "helm repo add fluent https://fluent.github.io/helm-charts")
    ctx.must_exec("helm repo update fluent")
    ctx.must_exec(f"helm pull fluent/{fluentbit_pkg_name} --version {fluentbit_pkg_version} "
                  f"--destination {component_pkg_path}")

    manifest.include_files(f"{APP_ROOT}/log-scraping/loki")
    manifest.include_files(f"{APP_ROOT}/log-scraping/fluent-bit")

    manifest.add_step(ImageStep.from_pullable_image(ctx, loki_image, component_pkg_path))
    manifest.add_step(ImageStep.from_pullable_image(ctx, fluentbit_image, component_pkg_path))

    manifest.add_step(ScriptStep(f"{APP_ROOT}/log-scraping/create-tls-secrets.sh", {}))

    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/log-scraping/helm-upgrade.sh.jinja2",
        {
            "remote_storage_path": remote_storage_path,
            "loki_image": loki_image,
            "loki_pkg_name": loki_pkg_name,
            "loki_pkg_version": loki_pkg_version,
            "fluentbit_image": fluentbit_image,
            "fluentbit_pkg_name": fluentbit_pkg_name,
            "fluentbit_pkg_version": fluentbit_pkg_version,
        }
    ))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@common.force_option
@with_manifest_option
@pass_ctx
def log_scraping(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        with_manifest: Optional[bool] = True,
):
    """
    Package log scraping.
    """
    return _log_scraping(ctx, pkg_path, force, with_manifest)


def _multus(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package multus.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("multus", pkg_path)

    manifest = StepsManifest(component)
    manifest.include_files(
        f"{APP_ROOT}/multus/net-attach-def.jsontemplate",
        f"{APP_ROOT}/multus/net-attach-config-ceph.jsontemplate",
        f"{APP_ROOT}/multus/node",
        f"{APP_ROOT}/multus/charts",
        f"{APP_ROOT}/multus/net-attach-def-installer.sh",
        f"{APP_ROOT}/multus/net-attach-def-installer-ds.yaml",
    )

    for _, image in multus_pullable_images.items():
        manifest.add_step(ImageStep.from_pullable_image(ctx, image, component_pkg_path))

    manifest.add_step(ScriptStep(
        f"{APP_ROOT}/multus/multus.sh.jinja2",
        {
            **multus_pullable_images,
            # We only set it to force if we would like to remove the helm charts and redeploy
            "force": "true" if force else "false",
        }
    ))
    manifest.add_step(ScriptStep(f"{APP_ROOT}/multus/net-attach-def.sh", {}))
    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@common.force_option
@with_manifest_option
@pass_ctx
def multus(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        force: Optional[bool] = False,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package multus.
    """
    return _multus(ctx, pkg_path, force, with_manifest)


def _log_rotation(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
        cluster_mgmt_retention_days: Optional[int] = 30,
        cluster_mgmt_rotate_at_size: Optional[str] = "100M",
        cluster_mgmt_rotate_count_max: Optional[int] = 5,
) -> List[pathlib.Path]:
    """
    Package log-rotation.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("log-rotation", pkg_path)

    with open(ctx.resolve_file('log-rotation/cluster-mgmt.defaults'), 'r', encoding='utf-8') as f:
        logrotate_defaults = f.readlines()

    logrotate_config = []
    for line in logrotate_defaults:
        if cluster_mgmt_retention_days != 30 and "maxage" in line:
            logger.info(f"Overriding default maximum age (30 days) for log retention: {cluster_mgmt_retention_days}")
            line = f"    maxage {cluster_mgmt_retention_days}\n"
        if cluster_mgmt_rotate_at_size != "100M" and "size" in line:
            logger.info(
                f"Overriding default maximum size (100M) to trigger log rotation: {cluster_mgmt_rotate_at_size}")
            line = f"    size {cluster_mgmt_rotate_at_size}\n"
        if cluster_mgmt_rotate_count_max != 5 and "rotate" in line:
            logger.info(
                f"Overriding default maximum count (5) of rotated logs to retain: {cluster_mgmt_rotate_count_max}")
            line = f"    rotate {cluster_mgmt_rotate_count_max}\n"
        logrotate_config.append(line)

    logger.info(f"Writing log rotation config: {component_pkg_path}/cluster-mgmt")
    with open(f"{component_pkg_path}/cluster-mgmt", 'w', encoding='utf-8') as f:
        f.writelines(logrotate_config)

    log_rotation_abs_path = ctx.resolve_dir('log-rotation').absolute()
    ctx.must_exec(
        f"rsync -av --exclude 'mocks*' --exclude 'smoke-test*' --exclude 'dryrun*' --exclude 'metadata*' {log_rotation_abs_path}/. {component_pkg_path}")
    manifest = StepsManifest(component)
    manifest.include_files(
        f"{APP_ROOT}/log-rotation/log-rotation-installer.sh",
        f"{APP_ROOT}/log-rotation/log-rotation-installer-ds.yaml",
    )
    manifest.add_step(ScriptStep(f"{APP_ROOT}/log-rotation/deploy.sh", {}))
    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@cluster_mgmt_retention_days_option
@cluster_mgmt_rotate_at_size_option
@cluster_mgmt_rotate_count_max_option
@pass_ctx
def log_rotation(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
        cluster_mgmt_retention_days: Optional[int] = 30,
        cluster_mgmt_rotate_at_size: Optional[str] = "100M",
        cluster_mgmt_rotate_count_max: Optional[int] = 5,
) -> List[pathlib.Path]:
    """
    Package log rotation.
    """
    return _log_rotation(ctx, pkg_path, with_manifest,
                         cluster_mgmt_retention_days,
                         cluster_mgmt_rotate_at_size,
                         cluster_mgmt_rotate_count_max)


def _kube_webhook(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package kube-webhook.
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("kube-webhook", pkg_path)
    manifest = StepsManifest(component)
    manifest.include_files(
        f"{APP_ROOT}/kube-webhook/webhook-server-template.yaml",
        f"{APP_ROOT}/kube-webhook/webhook-server-tls.conf",
        f"{APP_ROOT}/kube-webhook/webhook-systemdeployadmission-template.yaml",
        f"{APP_ROOT}/kube-webhook/webhook-adjust-resource-template.yaml",
        f"{APP_ROOT}/kube-webhook/webhook-add-ingress-annotation-template.yaml",
    )
    manifest.add_step(ImageStep.from_pullable_image(ctx, DockerImage.of(KUBE_WEBHOOK_IMAGE), component_pkg_path))
    manifest.add_step(ScriptStep(f"{APP_ROOT}/kube-webhook/webhook-server-install.sh.jinja2", {}))
    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@pass_ctx
def kube_webhook(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package kube-webhook.
    """
    return _kube_webhook(ctx, pkg_path, with_manifest)


def _rdma_device_plugin(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    rdma_plugin_image = DockerImage.of(RDMA_DEVICE_PLUGIN_IMAGE)

    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("rdma-device-plugin", pkg_path)

    manifest = StepsManifest(component)
    manifest.add_step(ImageStep.from_pullable_image(ctx, rdma_plugin_image, component_pkg_path))

    manifest.include_files(f"{APP_ROOT}/rdma-device-plugin/rdma-device-plugin-configmap.yaml",
                           f"{APP_ROOT}/rdma-device-plugin/rdma-device-plugin-daemonset.yaml",
                           f"{APP_ROOT}/rdma-device-plugin/rdma-device-plugin-test-daemonset.yaml",
                           f"{APP_ROOT}/rdma-device-plugin/rdma-device-plugin-test-daemonset-incremental.yaml",
                           f"{APP_ROOT}/rdma-device-plugin/roce-limitmemlock-infinity.conf",
                           f"{APP_ROOT}/rdma-device-plugin/rdma-device-plugin-installer.sh",
                           f"{APP_ROOT}/rdma-device-plugin/rdma-device-plugin-installer-ds.yaml",
                           )

    manifest.add_step(ScriptStep(f"{APP_ROOT}/rdma-device-plugin/rdma-device-plugin.sh.jinja2", {}))

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)], pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@pass_ctx
def rdma_device_plugin(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True
) -> List[pathlib.Path]:
    """
    K8s rdma plugin. Must be installed after job-operator so that all nodes are labelled.
    """
    return _rdma_device_plugin(ctx, pkg_path, with_manifest)


def _cerebras_internal_setup(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        namespace: str = "job-operator",
        nfs_server: str = "",
        skip_internal_volumes_setup: bool = False,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("cerebras-internal-setup", pkg_path)

    manifest = StepsManifest(component)
    manifest.include_files(f"{APP_ROOT}/cerebras-internal-setup/init-volumes-cmd.sh",
                           f"{CURRENT_DIR}/../../../tools/cluster-admin/cluster-volumes.sh")
    manifest.add_step(
        ScriptStep(f"{APP_ROOT}/cerebras-internal-setup/init-volumes.sh.jinja2",
                   {"ns": namespace, "skip_internal_volumes_setup": "true" if skip_internal_volumes_setup else "false"}))
    manifest.add_step(
        ScriptStep(f"{APP_ROOT}/cerebras-internal-setup/ecr-secret-rotation.sh.jinja2",
                   {"ns": namespace, "nfs": nfs_server}))
    manifest.include_files(f"{APP_ROOT}/cerebras-internal-setup/regcred-refresh")
    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)], pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


@click.command()
@pkg_path_option
@with_manifest_option
@common.namespace_option
@pass_ctx
def cerebras_internal_setup(
        ctx: context.CliCtx,
        pkg_path: Optional[pathlib.Path] = None,
        namespace: str = "job-operator",
        with_manifest: Optional[bool] = True
) -> List[pathlib.Path]:
    """
    Internal setup for Cerebras clusters.
    """
    return _cerebras_internal_setup(ctx, pkg_path, namespace, "", with_manifest)


def _k8s(
        ctx: context.CliCtx,
        from_k8s_version: str,
        k8s_version: str,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True,
) -> List[pathlib.Path]:
    """
    Package k8s (includes binary_deps and registry as prerequisites).
    """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("k8s", pkg_path)
    manifest = StepsManifest(component)

    # ensure install bin-deps/static-registry if k8s reconcile is skipped
    manifest.add_step(ScriptStep(f"{APP_ROOT}/k8s/setup.sh.jinja2", {
            "install_deps": "true" if ctx.skip_k8s else "false",
        }
    ))

    manifest.add_step(ScriptStep(f"{APP_ROOT}/k8s/rotate_certs.sh", {}))

    # https://github.com/prometheus-community/helm-charts/tree/kube-prometheus-stack-51.0.0/charts/kube-prometheus-stack#from-50x-to-51x
    # install CRD for system components(cilium/ceph/...) monitoring before prom installation
    svc_monitor_yaml_url = str(
        f"https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/{PROM_APP_VERSION}/example/"
        "prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml"
    )
    svc_monitor_yaml_file = component_pkg_path.joinpath("monitoring.coreos.com_servicemonitors.yaml")
    ctx.must_exec(f"curl -SsL --fail --retry 3 {svc_monitor_yaml_url} -o {svc_monitor_yaml_file}")

    pod_monitor_yaml_url = str(
        f"https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/{PROM_APP_VERSION}/example/"
        "prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml"
    )
    pod_monitor_yaml_file = component_pkg_path.joinpath("monitoring.coreos.com_podmonitors.yaml")
    ctx.must_exec(f"curl -SsL --fail --retry 3 {pod_monitor_yaml_url} -o {pod_monitor_yaml_file}")

    probe_yaml_url = str(
        f"https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/{PROM_APP_VERSION}/example/"
        "prometheus-operator-crd/monitoring.coreos.com_probes.yaml"
    )
    probe_yaml_file = component_pkg_path.joinpath("monitoring.coreos.com_probes.yaml")
    ctx.must_exec(f"curl -SsL --fail --retry 3 {probe_yaml_url} -o {probe_yaml_file}")

    prometheus_rules_yaml_url = str(
        f"https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/{PROM_APP_VERSION}/example/"
        "prometheus-operator-crd/monitoring.coreos.com_prometheusrules.yaml"
    )
    prometheus_rules_yaml_file = component_pkg_path.joinpath("monitoring.coreos.com_prometheusrules.yaml")
    ctx.must_exec(f"curl -SsL --fail --retry 3 {prometheus_rules_yaml_url} -o {prometheus_rules_yaml_file}")

    manifest.include_files(f"{APP_ROOT}/k8s/*")

    if not ctx.skip_k8s:
        # download all k8s images <= version into the package for upgrade purpose
        # output of kubeadm config images list --kubernetes-version=xxx
        img_dir = ctx.build_dir / "images"
        img_dir.mkdir(exist_ok=True)
        k8s_rpm_versions = ""
        if not from_k8s_version:
            from_k8s_version = k8s_version
        for version, images in sorted(K8S_IMAGES.items()):
            if version < from_k8s_version:
                continue
            k8s_rpm_versions += f"{version} "
            for image in images:
                manifest.add_step(ImageStep.from_pullable_image(ctx, DockerImage.of(image), component_pkg_path))
            if version == k8s_version:
                break
        manifest.include_files(img_dir)

        rpm_dir = ctx.build_dir / "rpm"
        rpm_dir.mkdir(exist_ok=True)
        _download_k8s_rpm(ctx, k8s_rpm_versions, CTR_VERSION, rpm_dir)
        manifest.include_files(rpm_dir)

    script_path = manifest.render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)],
                                               pkg_path, ctx.in_kind_cluster)
    # Include binary_deps and registry as part of k8s package (called internally)
    return [(component_pkg_path / script_path).relative_to(pkg_path)] + \
            _binary_deps(ctx, pkg_path, with_manifest=False) + _registry(ctx, pkg_path, with_manifest=False)


@click.command()
@common.from_k8s_version_option
@common.k8s_version_option
@pkg_path_option
@with_manifest_option
@pass_ctx
def k8s(
        ctx: context.CliCtx,
        from_k8s_version: str,
        k8s_version: str,
        pkg_path: Optional[pathlib.Path] = None,
        with_manifest: Optional[bool] = True
) -> List[pathlib.Path]:
    """
    Package k8s. Currently, only set up a directory /etc/kubernetes/manifests on all nodes.
    """
    return _k8s(ctx, from_k8s_version=from_k8s_version, k8s_version=k8s_version,
        pkg_path=pkg_path, with_manifest=with_manifest)

@click.command(hidden=True)
@pkg_path_option
@with_manifest_option
@pass_ctx
def empty(ctx: context.CliCtx, pkg_path: Optional[pathlib.Path] = None, with_manifest: Optional[bool] = True) -> List[
    pathlib.Path]:
    """ Empty package for test purposes. """
    pkg_path = get_pkg_path(ctx, pkg_path)
    component, component_pkg_path = prepare_pkg_path("empty", pkg_path)
    script_path = StepsManifest(component).render(component_pkg_path)
    if with_manifest:
        ClusterManifest.write_partial_manifest([(component_pkg_path / script_path).relative_to(pkg_path)], pkg_path)
    return [(component_pkg_path / script_path).relative_to(pkg_path)]


next_num = 0


def _get_next_num():
    global next_num
    next_num += 1
    return next_num


# Component mapping to its command and the deployment order. The components
# with smaller numbers should be deployed first.
COMPONENT_LIST = {
    "k8s": (k8s, _get_next_num()),
    "cilium": (cilium, _get_next_num()),
    "multus": (multus, _get_next_num()),
    "kube-webhook": (kube_webhook, _get_next_num()),
    CEPH_COMPONENT: (ceph, _get_next_num()),
    "nvme-of": (nvme_of, _get_next_num()),
    "kube-vip": (kube_vip, _get_next_num()),
    "private-registry": (registry, _get_next_num()),
    REGISTRY_SYNC_COMPONENT: (registry_sync, _get_next_num()),
    "common-images": (common_images, _get_next_num()),
    "rdma-device-plugin": (rdma_device_plugin, _get_next_num()),
    "nginx": (nginx, _get_next_num()),
    "log-scraping": (log_scraping, _get_next_num()),
    "prometheus": (prometheus, _get_next_num()),
    "cpingmesh": (cpingmesh, _get_next_num()),
    "log-rotation": (log_rotation, _get_next_num()),
    "alert-router": (alert_router, _get_next_num()),
    # SW-109779
    # We will revive kafka when the adoption plan is clear
    # "kafka": (kafka, _get_next_num()),
    "cluster-tools": (cluster_tools, _get_next_num()),
    "job-operator": (job_operator, _get_next_num()),
    "cbcore": (cbcore, _get_next_num()),
    "cerebras-internal-setup": (cerebras_internal_setup, _get_next_num()),
    "cluster-server": (cluster_server, _get_next_num()),
    # As of rel-2.6, cbcore official builds no longer support debugviz server.
    # Longer term plan is to use wheel-based installation for the debugviz server.
    # Until that is available, debug team will manually build special cbcore images if bug fixes are needed.
    # "debugviz-server": (debugviz_server, _get_next_num()),
    "usernode": (usernode, _get_next_num()),
}


def _get_pkg_version(pkg_path: pathlib.Path):
    """ Obtain the package version from the pkg_path if any. Otherwise, create one. """
    matches = re.search(r'(cluster-pkg-)(.*)', pkg_path.name)
    if matches:
        version = matches.group(2)
    else:
        version = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")
    return version


def _pack(ctx: context.CliCtx, pkg_path: pathlib.Path, components: List[str]):
    """ Check pre-built components and generate the manifest for the packages """

    version = _get_pkg_version(pkg_path)
    cluster_manifest = ClusterManifest(version, [])

    for component in components:
        component_path = pkg_path / component
        if not component_path.is_dir():
            raise click.ClickException(f"Component dir {component_path} does not exist")

        component_apply_file = component_path / f"apply-{component}.sh"
        if not component_apply_file.is_file():
            raise click.ClickException(f"Component file {component_apply_file} does not exist")

        cluster_manifest.component_paths.extend([str(component_apply_file.relative_to(pkg_path))])

    cluster_manifest.write_manifest(pkg_path)


@click.command()
@pkg_path_option
@common.image_option
@common.image_file_option
@common.cbcore_image_file_option
@common.release_artifact_path_option
@common.local_cross_build_option
@common.force_option
@common.tools_tar_file_option
@common.system_namespace_option
@common.namespace_option
@common.from_k8s_version_option
@common.k8s_version_option
@common.systems_option
@common.enable_multi_coordinator_option
@click.option(
    '--components',
    help="Comma-separated list of components to be packaged. Default to None to mean all components",
    type=str, required=False,
)
@click.option(
    '--skip-components',
    help="Comma-separated list of components to be skipped for packaging. Default to None to mean no components are skipped",
    type=str, required=False,
)
@pack_only_option
@click.pass_context
def all(
        ctx,
        namespace: str,
        system_namespace: str,
        from_k8s_version: str,
        k8s_version: str,
        systems: Optional[str] = None,
        pkg_path: Optional[pathlib.Path] = None,
        image: Optional[DockerImage] = None,
        image_file: Optional[pathlib.Path] = None,
        cbcore_image_file: Optional[pathlib.Path] = None,
        release_artifact_path: Optional[pathlib.Path] = None,
        local_cross_build: bool = False,
        force: Optional[bool] = False,
        tools_tar_file: Optional[pathlib.Path] = None,
        components: Optional[str] = None,
        pack_only: Optional[bool] = False,
        skip_components: Optional[str] = None,
        enable_multi_coordinator: Optional[bool] = False,
):
    """ Package all components or list of components
    Examples
    # generate a package using a release artifact path
    python3 cs_cluster.py -v package all --release-artifact-path \\
      /cb/artifacts/release-stage/default/latest/components/cbcore

    # stage a component, say job-operator, to be packaged later.
    python3 cs_cluster.py -v package job-operator \\
        --image-file /cb/artifacts/release-stage/default/latest/components/cbcore/job-operator-$(TAG).docker \\
        --without-manifest

    # generate a package for pre-built components under the pkg_path.
    python3 cs_cluster.py -v package all --pack-only

    """
    if components is None:
        component_list = list(COMPONENT_LIST.keys())
    else:
        component_list = components.split(',')
        unknown = [c for c in component_list if c not in COMPONENT_LIST]
        if unknown:
            raise click.ClickException(f"unknown components: {unknown}. Options: {COMPONENT_LIST.keys()}")

    if skip_components:
        skip_component_list = skip_components.split(',')
        unknown = [c for c in skip_component_list if c not in COMPONENT_LIST]
        if unknown:
            raise click.ClickException(f"unknown components: {unknown}. Options: {COMPONENT_LIST.keys()}")
        for skip_component in skip_component_list:
            component_list.remove(skip_component)

    image_file = fix_pathlib_path(image_file)
    cbcore_image_file = fix_pathlib_path(cbcore_image_file)
    release_artifact_path = fix_pathlib_path(release_artifact_path)
    tools_tar_file = fix_pathlib_path(tools_tar_file)

    component_list.sort(key=lambda elem: COMPONENT_LIST[elem][1])
    logger.info(f"The list of components to be packaged: {component_list}")

    pkg_path = get_pkg_path(ctx.obj, pkg_path)
    version = _get_pkg_version(pkg_path)

    if pack_only:
        _pack(ctx.obj, pkg_path, [COMPONENT_LIST[c][0].name for c in component_list])
        return

    cluster_manifest = ClusterManifest(version, [])
    cbcore = ""
    for component in component_list:
        component_paths = []
        if component == "k8s":
            component_paths = ctx.invoke(
                COMPONENT_LIST[component][0],
                from_k8s_version=from_k8s_version,
                k8s_version=k8s_version,
                pkg_path=pkg_path,
                with_manifest=False
            )
        if component == "job-operator":
            operator_image_file = \
                f"{release_artifact_path}/job-operator-*.docker" if release_artifact_path else image_file.name
            operator_image_file = _find_image_file(operator_image_file) if operator_image_file else None
            component_paths = ctx.invoke(
                COMPONENT_LIST[component][0],
                system_namespace=system_namespace,
                namespace=namespace,
                pkg_path=pkg_path,
                image=None if release_artifact_path else image,
                image_file=operator_image_file,
                local_cross_build=local_cross_build,
                enable_multi_coordinator=enable_multi_coordinator,
                with_manifest=False)
        if component == "cbcore":
            # The order for setting the cbcore image based on various input:
            # 1. If release_artifact_path is set, use it.
            # 2. If cbcore_image_file is set, use it.
            # 3. use either image or image_file.
            # This makes it possible to package both cbcore and cluster-server together.
            final_cbcore_image_file = \
                f"{release_artifact_path}/cbcore-*.docker" if release_artifact_path else cbcore_image_file.name
            final_cbcore_image_file = final_cbcore_image_file if final_cbcore_image_file else image_file.name if image_file else None
            final_cbcore_image_file = _find_image_file(final_cbcore_image_file) if final_cbcore_image_file else None
            cbcore_manifest = ctx.invoke(
                COMPONENT_LIST[component][0],
                pkg_path=pkg_path,
                image=None if final_cbcore_image_file else image,
                image_file=final_cbcore_image_file,
                with_manifest=False)
            if cbcore_manifest:
                component_paths.append(cbcore_manifest.path)
                cbcore = cbcore_manifest.image
        if component == "cluster-server":
            cluster_image_file = \
                f"{release_artifact_path}/cluster-server-*.docker" if release_artifact_path else image_file.name
            cluster_image_file = _find_image_file(cluster_image_file) if cluster_image_file else None
            component_paths = ctx.invoke(
                COMPONENT_LIST[component][0],
                namespace=namespace,
                system_namespace=system_namespace,
                pkg_path=pkg_path,
                image=None if release_artifact_path else image,
                image_file=cluster_image_file,
                local_cross_build=local_cross_build,
                cbcore=cbcore,
                with_manifest=False)
        if component == "debugviz-server":
            component_paths = ctx.invoke(
                COMPONENT_LIST[component][0],
                namespace=namespace,
                system_namespace=system_namespace,
                force=force,
                pkg_path=pkg_path,
                with_manifest=False
            )
        if component in [
            "static-registry", "private-registry", "nginx", "binary-deps", "cilium",
            CEPH_COMPONENT, REGISTRY_SYNC_COMPONENT, "kafka", "kube-webhook", "kube-vip", "multus",
            "rdma-device-plugin", "log-rotation", "nvme-of", "cpingmesh"]:
            component_paths = ctx.invoke(
                COMPONENT_LIST[component][0],
                pkg_path=pkg_path,
                with_manifest=False)
        if component in ["prometheus", "log-scraping"]:
            component_paths = ctx.invoke(
                COMPONENT_LIST[component][0],
                pkg_path=pkg_path,
                force=force,
                with_manifest=False
            )
        if component == "cluster-tools":
            component_paths = ctx.invoke(
                COMPONENT_LIST[component][0],
                namespace=namespace,
                system_namespace=system_namespace,
                pkg_path=pkg_path,
                tools_tar_file=tools_tar_file,
                with_manifest=False
            )
        if component == "usernode":
            component_paths = ctx.invoke(
                COMPONENT_LIST[component][0],
                system_namespace=system_namespace,
                namespace=namespace,
                pkg_path=pkg_path,
                release_artifact_path=release_artifact_path,
                with_manifest=False
            )
        if component == "common-images":
            component_paths = ctx.invoke(
                COMPONENT_LIST[component][0],
                namespace=namespace,
                system_namespace=system_namespace,
                pkg_path=pkg_path,
                with_manifest=False
            )
        if component_paths:
            cluster_manifest.component_paths.extend(component_paths)

    cluster_manifest.write_manifest(pkg_path)
