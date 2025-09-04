import functools
import json
import logging
import os
import pathlib
import re
import socket
import time
import typing
import warnings
from dataclasses import dataclass

import click

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

IMAGES_SUBPATH = "images"

NFS_SHARE_DIR = "/cb/tests/cluster-mgmt/build"
REGISTRY_URL = "registry.local"
ECR_URL = "171496337684.dkr.ecr.us-west-2.amazonaws.com"
ECR_PUBLIC_REPO = f"{ECR_URL}/ecr-public/docker/library"

DIND_IMAGE = f"{ECR_PUBLIC_REPO}/docker:20.10-dind"
CENTOS7_IMAGE = f"{ECR_PUBLIC_REPO}/centos:7"

ALPINE_IMAGE = "alpine:3.16.2"
BUSYBOX_IMAGE = f"{ECR_PUBLIC_REPO}/busybox:1.34.1"
BUSYBOX_IMAGE_SHORT_NAME = "busybox:1.34.1"
KIND_NODE_IMAGE = f"{ECR_URL}/cerebras-kind-node:v1.29.0-5"
KUBE_VIP_IMAGE = f"{ECR_URL}/kube-vip:vip-20250509-0"
HAPROXY_IMAGE = f"{ECR_PUBLIC_REPO}/haproxy:2.8"
KAFKA_IMAGE = f"{ECR_URL}/bitnami-kafka:3.5.0-debian-11-r1"
PYTHON38_IMAGE = f"{ECR_PUBLIC_REPO}/python:3.8"
PYTHON38_IMAGE_SHORT_NAME = "python:3.8"
PYTHON311_IMAGE = f"{ECR_PUBLIC_REPO}/python:3.11"
PYTHON311_IMAGE_SHORT_NAME = "python:3.11"

CILIUM_CLI_VERSION = "v0.18.5"
CILIUM_VERSION = "1.17.6"
HUBBLE_VERSION = "0.13.2"
CILIUM_IMAGE = f"{ECR_URL}/quay/cilium/cilium:v{CILIUM_VERSION}"
CILIUM_HUBBLE_RELAY_IMAGE = f"{ECR_URL}/quay/cilium/hubble-relay:v{CILIUM_VERSION}"
CILIUM_OPERATOR_IMAGE = f"{ECR_URL}/quay/cilium/operator:v{CILIUM_VERSION}"
CILIUM_OPERATOR_GENERIC_IMAGE = f"{ECR_URL}/quay/cilium/operator-generic:v{CILIUM_VERSION}"
CILIUM_HUBBLE_UI_BACKEND_IMAGE = f"{ECR_URL}/quay/cilium/hubble-ui-backend:v{HUBBLE_VERSION}"
CILIUM_HUBBLE_UI_IMAGE = f"{ECR_URL}/quay/cilium/hubble-ui:v{HUBBLE_VERSION}"
CILIUM_CERTGEN_IMAGE = f"{ECR_URL}/quay/cilium/certgen:v0.2.1"
CILIUM_ALPINE_CURL_IMAGE = f"{ECR_URL}/quay/cilium/alpine-curl:v1.10.0"
CILIUM_JSON_MOCK_IMAGE = f"{ECR_URL}/quay/cilium/json-mock:v1.3.8"

NGINX_CTRL_IMAGE = "registry.k8s.io/ingress-nginx/controller:v1.12.1"

LOKI_IMAGE = "docker.io/grafana/loki:2.8.3"
FLUENTBIT_IMAGE = "fluent/fluent-bit:2.2.0"

PAUSE_IMAGES = ["registry.k8s.io/pause:3.7",
                "registry.k8s.io/pause:3.8",
                "registry.k8s.io/pause:3.9",
                "registry.k8s.io/pause:3.10"
                ]

CTR_VERSION = "1.7.20"
K8S_IMAGES = {
    "1.24.4": [
        "registry.k8s.io/kube-apiserver:v1.24.4",
        "registry.k8s.io/kube-controller-manager:v1.24.4",
        "registry.k8s.io/kube-scheduler:v1.24.4",
        "registry.k8s.io/kube-proxy:v1.24.4",
        "registry.k8s.io/pause:3.7",
        "registry.k8s.io/etcd:3.5.3-0",
        "registry.k8s.io/coredns/coredns:v1.8.6",
        KUBE_VIP_IMAGE,
        HAPROXY_IMAGE,
    ],
    "1.25.16": [
        "registry.k8s.io/kube-apiserver:v1.25.16",
        "registry.k8s.io/kube-controller-manager:v1.25.16",
        "registry.k8s.io/kube-scheduler:v1.25.16",
        "registry.k8s.io/kube-proxy:v1.25.16",
        "registry.k8s.io/pause:3.8",
        "registry.k8s.io/etcd:3.5.9-0",
        "registry.k8s.io/coredns/coredns:v1.9.3",
        KUBE_VIP_IMAGE,
        HAPROXY_IMAGE,
    ],
    "1.26.15": [
        "registry.k8s.io/kube-apiserver:v1.26.15",
        "registry.k8s.io/kube-controller-manager:v1.26.15",
        "registry.k8s.io/kube-scheduler:v1.26.15",
        "registry.k8s.io/kube-proxy:v1.26.15",
        "registry.k8s.io/pause:3.9",
        "registry.k8s.io/etcd:3.5.10-0",
        "registry.k8s.io/coredns/coredns:v1.9.3",
        KUBE_VIP_IMAGE,
        HAPROXY_IMAGE,
    ],
    "1.27.16": [
        "registry.k8s.io/kube-apiserver:v1.27.16",
        "registry.k8s.io/kube-controller-manager:v1.27.16",
        "registry.k8s.io/kube-scheduler:v1.27.16",
        "registry.k8s.io/kube-proxy:v1.27.16",
        "registry.k8s.io/pause:3.9",
        "registry.k8s.io/etcd:3.5.12-0",
        "registry.k8s.io/coredns/coredns:v1.10.1",
        KUBE_VIP_IMAGE,
        HAPROXY_IMAGE,
    ],
    "1.28.13": [
        "registry.k8s.io/kube-apiserver:v1.28.13",
        "registry.k8s.io/kube-controller-manager:v1.28.13",
        "registry.k8s.io/kube-scheduler:v1.28.13",
        "registry.k8s.io/kube-proxy:v1.28.13",
        "registry.k8s.io/pause:3.9",
        "registry.k8s.io/etcd:3.5.12-0",
        "registry.k8s.io/coredns/coredns:v1.10.1",
        KUBE_VIP_IMAGE,
        HAPROXY_IMAGE,
    ],
    "1.29.9": [
        "registry.k8s.io/kube-apiserver:v1.29.9",
        "registry.k8s.io/kube-controller-manager:v1.29.9",
        "registry.k8s.io/kube-scheduler:v1.29.9",
        "registry.k8s.io/kube-proxy:v1.29.9",
        "registry.k8s.io/pause:3.9",
        "registry.k8s.io/etcd:3.5.12-0",
        "registry.k8s.io/coredns/coredns:v1.11.1",
        KUBE_VIP_IMAGE,
        HAPROXY_IMAGE,
    ],
    "1.30.4": [
        "registry.k8s.io/kube-apiserver:v1.30.4",
        "registry.k8s.io/kube-controller-manager:v1.30.4",
        "registry.k8s.io/kube-scheduler:v1.30.4",
        "registry.k8s.io/kube-proxy:v1.30.4",
        "registry.k8s.io/pause:3.9",
        "registry.k8s.io/etcd:3.5.12-0",
        "registry.k8s.io/coredns/coredns:v1.11.1",
        KUBE_VIP_IMAGE,
        HAPROXY_IMAGE,
    ],
    "1.31.10": [
        "registry.k8s.io/kube-apiserver:v1.31.10",
        "registry.k8s.io/kube-controller-manager:v1.31.10",
        "registry.k8s.io/kube-scheduler:v1.31.10",
        "registry.k8s.io/kube-proxy:v1.31.10",
        "registry.k8s.io/pause:3.10",
        "registry.k8s.io/etcd:3.5.15-0",
        "registry.k8s.io/coredns/coredns:v1.11.3",
        KUBE_VIP_IMAGE,
        HAPROXY_IMAGE,
    ],
    "1.32.6": [
        "registry.k8s.io/kube-apiserver:v1.32.6",
        "registry.k8s.io/kube-controller-manager:v1.32.6",
        "registry.k8s.io/kube-scheduler:v1.32.6",
        "registry.k8s.io/kube-proxy:v1.32.6",
        "registry.k8s.io/pause:3.10",
        "registry.k8s.io/etcd:3.5.16-0",
        "registry.k8s.io/coredns/coredns:v1.11.3",
        KUBE_VIP_IMAGE,
        HAPROXY_IMAGE,
    ]
}

COREDNS_IMAGE = "registry.k8s.io/coredns/coredns:v1.11.1"

PAUSE_37_IMAGE = "registry.k8s.io/pause:3.7"
KUBE_RBAC_PROXY_IMAGE = "registry.k8s.io/kubebuilder/kube-rbac-proxy:v0.8.0"
MULTUS_IMAGE = f"{ECR_URL}/multus-cni:v4.0.2-cb-thin-rc1"
WHEREABOUTS_IMAGE = f"{ECR_URL}/whereabouts:v0.7.0-cb-rc23"
# This image is not in ECR
NGINX_WEBHOOK_IMAGE = "registry.k8s.io/ingress-nginx/kube-webhook-certgen:v1.5.2"
REGISTRY_IMAGE = f"{ECR_PUBLIC_REPO}/registry:2.8.3"

PROM_ALERT_IMAGE = f"{ECR_URL}/quay/prometheus/alertmanager:v0.27.0"
# This is not in ECR.
PROM_NGINX_WEBHOOK_CERTGEN_IMAGE = \
    "registry.k8s.io/ingress-nginx/kube-webhook-certgen:v20221220-controller-v1.5.1-58-g787ea74b6"
PROM_OPERATOR_IMAGE = f"{ECR_URL}/quay/prometheus-operator/prometheus-operator:v0.78.1"
PROM_CONFIG_IMAGE = f"{ECR_URL}/quay/prometheus-operator/prometheus-config-reloader:v0.78.1"
PROM_IMAGE = f"{ECR_URL}/quay/prometheus/prometheus:v2.55.1"
PROM_THANOS_IMAGE = f"{ECR_URL}/quay/thanos/thanos:v0.36.1"

# Need to set the Prom/Thanos version numbers in Grafana's connection config block
# We can't specify the exact version, but instead choose the nearest API change version.
# It's not intuitive. Select the most accurate value from this file (from the appropriate Grafana release tag)
# https://github.com/grafana/grafana/blob/v11.3.1/packages/grafana-prometheus/src/configuration/PromFlavorVersions.ts
PROM_VERSION_FOR_GRAFANA_CONFIG = "2.50.1"
THANOS_VERSION_FOR_GRAFANA_CONFIG = "0.31.1"

# https://github.com/timescale/timescaledb/issues/5414
PROM_SIDECAR_IMAGE = f"{ECR_URL}/quay/kiwigrid/k8s-sidecar:1.28.0"
PROM_GRAFANA_IMAGE = "grafana/grafana:11.3.1"
PROM_NODE_EXPORTER_IMAGE = f"{ECR_URL}/quay/prometheus/node-exporter:v1.8.2"
PROM_SNMP_EXPORTER_IMAGE = f"{ECR_URL}/quay/prometheus/snmp-exporter:v0.26.0"
PROM_STATE_METRICS_IMAGE = "registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.14.0"
PROM_APP_VERSION = "v0.78.1"

ROOK_CEPH_IMAGE = "docker.io/rook/ceph:v1.13.9"
CEPH_CEPH_IMAGE = f"{ECR_URL}/quay/ceph/ceph:v18.2.4"
CEPH_CSI_IMAGE = f"{ECR_URL}/quay/cephcsi/cephcsi:v3.10.2"
CSI_NODE_DRIVER_REGISTRAR_IMAGE = "registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.10.0"
CSI_PROVISIONER_IMAGE = "registry.k8s.io/sig-storage/csi-provisioner:v4.0.1"
CSI_SNAPSHOTTER_IMAGE = "registry.k8s.io/sig-storage/csi-snapshotter:v7.0.2"
CSI_ATTACHER_IMAGE = "registry.k8s.io/sig-storage/csi-attacher:v4.5.1"
CSI_RESIZER_IMAGE = "registry.k8s.io/sig-storage/csi-resizer:v1.10.1"
K8S_SIDECAR_IMAGE = f"{ECR_URL}/quay/csiaddons/k8s-sidecar:v0.8.0"

KUBE_WEBHOOK_IMAGE = f"{ECR_URL}/kube-webhook:webhook-20250616-0"

RDMA_DEVICE_PLUGIN_IMAGE = "ghcr.io/mellanox/k8s-rdma-shared-dev-plugin:v1.5.1"

# binary, url, sha256, version
BINARY_DEPS = [
    ("helm", "https://get.helm.sh/helm-v3.11.1-linux-amd64.tar.gz",
     "0b1be96b66fab4770526f136f5f1a385a47c41923d33aab0dcb500e0f6c1bf7c", "v3.11.1+g293b50c"),
    ("nerdctl", "https://github.com/containerd/nerdctl/releases/download/v1.5.0/nerdctl-1.5.0-linux-amd64.tar.gz",
     "6dc945e3dfdc38e77ceafd2ec491af753366a3cf83fefccb1debaed3459829f1", "1.5.0"),
]

K8_COMPONENT = "k8s"
CEPH_COMPONENT = "ceph"
CILIUM_COMPONENT = "cilium"
REGISTRY_COMPONENT = "registry"
REGISTRY_SYNC_COMPONENT = "registry-sync"
# List of components that needs the shared images directory
COMPONENTS_USE_SHARED_IMAGES_DIR = {
    K8_COMPONENT,
    CEPH_COMPONENT,
    CILIUM_COMPONENT,
    REGISTRY_COMPONENT,
    REGISTRY_SYNC_COMPONENT,
}

logger = logging.getLogger("cs_cluster.init")


def flat_map(f, xs):
    ys = []
    for x in xs:
        ys.extend(f(x))
    return ys


def expand_home(paths: typing.Union[str, typing.List[str]]) -> typing.Union[str, typing.List[str]]:
    if isinstance(paths, list):
        return list(map(expand_home, paths))
    return str(pathlib.Path(paths).expanduser())


def fix_pathlib_path(path) -> pathlib.Path:
    """ In Click 7.0, pathlib.Path might be passed around as bytes. Fix it in this function. """
    if path and type(path) == bytes:
        return pathlib.Path(path.decode())
    return path


dry_run_flag = click.option('--dry-run', help="Print intended actions but does not perform them.",
                            is_flag=True, required=False)


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def log_duration(logger):
    def _wrapper(fn):
        @functools.wraps(fn)
        def _wraps(*args, **kwargs):
            start_time = time.time()
            try:
                return fn(*args, **kwargs)
            finally:
                logger.debug(f"executed {fn.__name__} in {round(time.time() - start_time, 1)}s")

        return _wraps

    return _wrapper


@dataclass
class DockerImage:
    repo: str
    tag: str
    alias = ""
    labels = {}
    dind = False

    @staticmethod
    def of(value) -> 'DockerImage':
        v = str(value).strip().split(":")
        if len(v) != 2:
            raise ValueError(f"image ({value}) must be of the form '<repo>:<tag>'")
        return DockerImage(v[0], v[1])

    def with_registry(self, registry: str) -> 'DockerImage':
        repo_path = self.repo if "/" not in self.repo else self.repo.split("/", 1)[1]
        return DockerImage(registry + "/" + repo_path, self.tag)

    def set_alias(self, alias: str) -> 'DockerImage':
        self.alias = alias
        return self

    # Deprecated since there's auto fallback to dind after failure
    def require_dind(self) -> 'DockerImage':
        self.dind = True
        return self

    def set_labels(self, labels: typing.Dict) -> 'DockerImage':
        self.labels = labels
        return self

    def get_labels(self, ctx) -> typing.Dict:
        """
        Get the labels in the image config
        """
        if not self.labels:
            rv, stdout, stderr = ctx.exec(f"docker image inspect {self.name}" + " -f '{{json .Config.Labels}}' ")
            if rv == 0:
                self.labels = json.loads(stdout)
            else:
                logger.debug(f"no labels found for {self.name}: {stdout}, {stderr}")
        return self.labels

    @property
    def tgz_name(self) -> str:
        """
        Get the tgz name for an image, e.g. ecr-public-docker-busybox-1-34-1.tar.gz
        """
        return f'{self.tar_name}.gz'

    @property
    def tar_name(self) -> str:
        """
        Get the tar file name for an image, e.g. ecr-public-docker-busybox-1-34-1.tar
        """
        return f'{self.format_name}.tar'

    @property
    def format_name(self) -> str:
        """
        Get the formatted short name for an image, e.g. ecr-public-docker-busybox-1-34-1
        """
        return self.short_name.replace(".", "-").replace(":", "-").replace("/", "-")

    @property
    def short_name(self) -> str:
        """
        Get the short name for an image, which includes the string after the last `/` from
        image.repo concatenating with image.tag using ':'.

        Example:
            The short name for image 'xyz.ecr.aws.com/ecr-public/docker/busybox:1.34.1'
            is 'ecr-public/docker/busybox:1.34.1'.
            If alias defined, return directly.
        """
        return self.alias if self.alias else f"{self.short_repo}:{self.tag}"

    @property
    def local_name(self) -> str:
        """ Generic image name for lookup in the cluster's local registry. """
        return f"{REGISTRY_URL}/{self.short_name}"

    @property
    def latest_tag_local_name(self) -> str:
        """ Generic image name for lookup in the cluster's local registry with latest tag. """
        return f"{REGISTRY_URL}/{self.short_repo}:latest"

    @property
    def short_repo(self) -> str:
        """
        Get the short repo for an image, which includes the string after the first `/` from
        image.repo.

        Example:
            The short repo name for image 'xyz.ecr.aws.com/ecr-public/docker/busybox:1.34.1'
            is 'ecr-public/docker/busybox'.
        """
        return self.repo[self.repo.find('/') + 1:]

    def build_dind(self, ctx) -> str:
        builder_container = f"dind-builder-{int(time.time())}"
        build_timeout_sec = 60 * 5
        docker_config_json = os.path.realpath(os.path.expanduser('~/.docker/config.json'))
        ctx.must_exec(f'docker run --privileged -d --rm --name {builder_container} '
                      f'-v {docker_config_json}:/root/.docker/config.json:ro '
                      f'--entrypoint /bin/sh {DIND_IMAGE} -c "dockerd-entrypoint.sh & sleep {build_timeout_sec}"')
        attempt = 0
        max_attempts = 10
        while True:
            time.sleep(1)
            rt, so, _ = ctx.exec(f"docker exec -t {builder_container} docker info")
            if rt == 0:
                logger.debug(f"{builder_container} ready for pull: {so}")
                break
            attempt += 1
            if attempt >= max_attempts:
                ctx.exec(f"docker rm -f {builder_container}")
                raise Exception(f"{builder_container} not ready after 10s, unable to pull {self.name}: {so}")
        return builder_container

    def pull(self, ctx, builder_container=None, with_dind=False) -> str:
        cmd_prefix = ""
        if builder_container:
            cmd_prefix = f"docker exec -t {builder_container} "
        rv, stdout, _ = ctx.exec(f"{cmd_prefix}docker image inspect {self.name}" + " -f {{.Architecture}} ")
        if rv != 0 or ctx.build_arch() not in stdout:
            ctx.exec(f"bash -c '{cmd_prefix}docker rmi {self.name} >/dev/null 2>&1'")
            logger.info(f"start pulling {self.name}")
            if with_dind and not builder_container:
                builder_container = self.build_dind(ctx)
                return self.pull(ctx, builder_container=builder_container)
            rv, stdout, _ = ctx.exec(f"{cmd_prefix}docker pull {self.name} --platform={ctx.build_platform()}")
            if rv != 0:
                # workaround to fallback to dind
                if "mediaType in manifest should be" in stdout and not with_dind:
                    logger.warning(f"fall back to pull with dind for {self.name} due to docker version too low")
                    return self.pull(ctx, with_dind=True)
                if ECR_URL not in self.repo:
                    raise RuntimeError(stdout)
                # fall back to public repo and retry
                original_repo = self.repo
                self.repo = self.repo.replace(f"{ECR_URL}/quay", "quay.io")
                self.repo = self.repo.replace(f"{ECR_URL}/ecr-public/docker", "docker.io")
                if ECR_URL not in self.repo:
                    self.pull(ctx, builder_container=builder_container)
                    # revert repo to avoid duplicate copying on image path
                    non_ecr_name = self.name
                    self.repo = original_repo
                    ctx.exec(f"{cmd_prefix}docker tag {non_ecr_name} {self.name}")
                    return builder_container
            else:
                logger.info(f"{cmd_prefix}docker pull {self.name} {ctx.build_platform()} success")
                return builder_container
        else:
            logger.info(f"{self.name} {ctx.build_platform()} already exists locally, skip pulling")
            return builder_container

    def export(self, ctx, tar_image_path, zip_image_path, platform_metadata, gzip=True, with_dind=False):
        # If there were issues with saving images due to legacy/incompatible docker version,
        # we could use a dind builder as a workaround.
        builder_container = self.pull(ctx, with_dind=with_dind)
        cmd_prefix = ""
        if builder_container:
            cmd_prefix = f"docker exec -t {builder_container} "
        ctx.exec(f"{cmd_prefix}docker rmi {self.local_name}")
        # tag local before exporting to avoid external repo on remote cluster which will make image clean trickier
        ctx.must_exec(f"{cmd_prefix}docker tag {self.name} {self.local_name}")
        # if a dind builder is used, the image will be saved in the builder container and we need to copy it out
        if cmd_prefix:
            tmp_tar_image_path = "/home/output.tar"
            ctx.must_exec(f"{cmd_prefix}docker save {self.local_name} --output {tmp_tar_image_path}")
            ctx.must_exec(f"docker cp {builder_container}:{tmp_tar_image_path} {tar_image_path}")
            ctx.exec(f"docker rm -f {builder_container}")
        else:
            logger.info(f"Saving {self.name} to {tar_image_path}")
            ctx.must_exec(f"docker save {self.local_name} --output {tar_image_path}")
        if gzip:
            logger.info(f"Compressing {tar_image_path} to {zip_image_path}")
            ctx.must_exec(f"bash -c 'gzip {tar_image_path} --force > {zip_image_path}'")
        platform_metadata.write_text(ctx.build_platform())

    @property
    def name(self) -> str:
        return f"{self.repo}:{self.tag}"

    def __str__(self):
        return f"{self.repo}:{self.tag}"


def scrape_docker_image_name(stdout: str) -> DockerImage:
    matches = re.compile(r"docker build -t ([^:]+:[^ ]+)").findall(stdout)
    err = "0" if not matches else "many" if len(matches) > 1 else None
    if err:
        raise Exception(f"stdout contained {err} docker images, images: {matches}")
    return DockerImage.of(matches[0])


class DockerImageParamType(click.ParamType):
    name = "Image"

    def convert(self, value, param, ctx):
        if isinstance(value, DockerImage):
            return value
        try:
            return DockerImage.of(str(value))
        except ValueError as e:
            self.fail(str(e), param, ctx)


def deprecated_option_warning(msg):
    def _deprecated_option_callback(ctx, param, value):
        if value is not None:
            warnings.warn(msg, DeprecationWarning)
        return value

    return _deprecated_option_callback


from_k8s_version_option = click.option(
    '--from-k8s-version',
    help="Lowest version of rpm/images to be included, can be tuned to skip lower versions if needed",
    show_default=True, default="1.25.16", type=click.STRING, required=True
)

k8s_version_option = click.option(
    '--k8s-version',
    help="Target k8s version for install/upgrade",
    show_default=True, default="1.32.6", type=click.STRING, required=True
)

image_option = click.option(
    '--image', "-i",
    help="Docker image in the form of '<repo>:<tag>'. If not provided, the image will be built from source.",
    type=DockerImageParamType(), required=False,
)

image_file_option = click.option(
    "--image-file",
    help="Path to a docker image archive file.",
    type=click.Path(dir_okay=False, path_type=pathlib.Path), required=False,
)

cbcore_image_option = click.option(
    '--cbcore-image', '--cb',
    help="cbcore docker image in the form of '<repo>:<tag>'. Sets the wsjob.image in the cluster-server helm chart "
         "to this value. Value may also be set in the values-override.yaml file for the target cluster in which case, "
         "this may be left unset. Value may also be set with envvar WSJOB_IMAGE.",
    type=DockerImageParamType(),
    envvar="WSJOB_IMAGE",
)

release_artifact_path_option = click.option(
    '--release-artifact-path', "--rel-path",
    help="The path for release artifacts, e.g, /cb/artifacts/release-stable/rel-1.6.0/latest/components/cbcore. "
         "When this option is provided, the `image`, `image_file` and `cbcore_image_file` are ignored."
         "job-operator, cluster-server and cbcore images will be found under this path.",
    type=click.Path(path_type=pathlib.Path), required=False,
)

cluster_config_path_flag = click.option(
    '--cluster-config', "-f",
    help="Optional path to cluster config file. Defaults to /clusters/<cluster>/cluster.yaml if not provided",
    type=click.Path(dir_okay=False, path_type=pathlib.Path, exists=True), required=False,
)

config_only_option = click.option(
    '--config-only',
    help="Skip loading the wheels/csctl as part of the usernode installation. This is useful for deploying to colovore "
         "clusters where the wheels are available on NFS anyways.",
    is_flag=True, default=False,
)

overwrite_usernode_configs_option = click.option(
    '--overwrite-usernode-configs',
    help="When set, the user node would only have access to the deployed namespace. "
         "Starting from release 2.1, cluster servers from different namespaces are behind different TLS certificates. "
         "Without this option set, a user node could have access to multiple namespaces.",
    show_default=True, default=False, is_flag=True
)

reset_usernode_configs_option = click.option(
    '--reset-usernode-configs',
    help="Removes all namespaced configs in the event of customer offboarding and session teardown. "
         "When set, '--config-only' and '--overwrite-usernode-configs' will be implicitly set to True.",
    is_flag=True, default=False,
)

skip_install_default_csctl_option = click.option(
    '--skip-install-default-csctl',
    help="Skips installing the default csctl executable under /usr/local/bin on user nodes. "
         "We should always install the default csctl in production environment.",
    is_flag=True, default=False,
)

enable_usernode_monitoring_option = click.option(
    '--enable-usernode-monitoring',
    help="Configures the monitoring stack on the user node to collect metrics.",
    show_default=True, is_flag=True, default=False,
)

cbcore_image_file_option = click.option(
    '--cbcore-image-file',
    help="Path to cbcore docker image file. During package, if this option is provided "
         "together with either `image` or `image_file` option, this option is used for "
         "cbcore component.",
    type=click.Path(dir_okay=False, path_type=pathlib.Path), required=False,
)

load_cbcore_image_option = click.option(
    '--load-cbcore-image', '--load',
    help="Set to load cbcore image to the private registry. For internal testing, skip loading "
         "cbcore image and instead load from ECR directly. For customer deployment, apply this "
         "flag to load the cbcore image to the private registry.",
    is_flag=True, required=False,
)

recreate_tls_secret_option = click.option("--recreate-tls-secret",
                                          help="Applying this flag will force re-creation of the registry TLS secret.",
                                          is_flag=True, required=False)

skip_prerequisite_check_option = click.option("--skip-prerequisite-check", "--sp",
                                              help="Skip preparation steps before actual install, "
                                                   "for dev testing purpose only ",
                                              is_flag=True, required=False, default=False)

skip_rebuild_option = click.option("--skip-rebuild",
                                   help="Skip docker rebuild for same image tag, "
                                        "set to True if you don't want to rebuild the same tag",
                                   is_flag=True, required=False, default=False)

force_new_tag_option = click.option("--force-new-tag", "--tag",
                                    help="Build new image tag with timestamp ",
                                    is_flag=True, required=False, default=False)

local_cross_build_option = click.option(
    '--local-cross-build', '-c',
    help="Set to build job-operator/cluster-server docker image from golang cross build "
         "which reduce build time mostly for local development only",
    is_flag=True, required=False,
)

force_option = click.option(
    '--force', '-f',
    help="Force the deployment",
    is_flag=True, required=False, default=False
)

dashboard_only_option = click.option(
    '--dashboard-only',
    help="Only update dashboard",
    is_flag=True, required=False, default=False
)

exporter_only_option = click.option(
    '--exporter-only',
    help="Only update custom exporters",
    is_flag=True, required=False, default=False
)

prom_only_option = click.option(
    '--prom-only',
    help="Only update prom stack",
    is_flag=True, required=False, default=False
)

skip_validate_option = click.option(
    '--skip-validate',
    help="Skip running the validation",
    is_flag=True, required=False, default=False
)

enable_multi_coordinator_option = click.option(
    '--enable-multi-coordinator', '--mc',
    help="Optional enable multi coordinator mode.",
    is_flag=True, required=False, default=False
)

disable_cluster_mode_option = click.option(
    '--disable-cluster-mode', '--dc',
    help="Optional disable cluster mode so jobs will only be reconciled for its own operator."
         "Mostly for dev purpose or in case there are incompatible changes cluster mode can't handle ",
    is_flag=True, required=False, default=False
)

all_namespaces_option = click.option(
    '--all-namespaces', '--all',
    help="Deprecated: get all namespaces.",
    is_flag=True, required=False, default=False
)

cluster_info_only_option = click.option(
    '--cluster-info-only', '--cluster',
    help="Deprecated: cluster rack/system info only.",
    is_flag=True, required=False, default=False
)

assign_mode_option = click.option(
    '--assign-mode', '--mode',
    help="Deprecated: systems assignment mode."
         "Example: --mode=append/append-all/remove/remove-all",
    show_default=True, default="",
    type=click.Choice(['append', 'append-all', 'remove', 'remove-all', ''], case_sensitive=False), required=False
)

owner_option = click.option(
    '--owner',
    help="Deprecated: Namespace owner for tracking and coordination purpose in case of systems conflicts."
         "Example: --owner=[your name]",
    show_default=True, default="", type=click.STRING, required=False
)

systems_option = click.option(
    '--systems',
    help="Deprecated: systems assignment for current namespace deploy. By default empty, all systems will be assigned."
         "Example: --systems systemf123,systemf124",
    show_default=True, default="", type=click.STRING, required=False
)

system_deploy_force_option = click.option(
    '--system-deploy-force', "--sdf",
    help="This is for internal cluster to avoid user accidentally update system NS. "
         "Only when specified, system NS operator/server can be updated as dev option. "
         "Deploy all flow is not affected. "
         "Only as short term solution, long term will be with deployment svc or role permission. ",
    is_flag=True, required=False, default=False
)

namespace_option = click.option(
    '--namespace', '--name', '-n',
    help="Optional namespace for cluster-server/job-operator/usernode. If not present, it will be job-operator by default.",
    show_default=True, default="job-operator", type=click.STRING, required=False
)

# note: this is not ready for use now, currently only job-operator namespace as system is tested
# todo: populate all components with the right system namespace
system_namespace_option = click.option(
    '--system-namespace',
    help="Optional system namespace for cluster-server/job-operator. "
         "If not present, it will be job-operator by default. "
         "The system namespace is for deployment for internal multi-version support to "
         "make sure there's only one system controller cluster wide for node labelling/certs/...",
    show_default=True, default="job-operator", type=click.STRING, required=False
)

# keep for backward compatibility
parallel_compile_limit_option = click.option(
    '--parallel-compile-limit',
    help="Deprecated. This has no effect.",
    type=click.INT, required=False, default=1, callback=deprecated_option_warning(
        "--parallel-compile-limit is deprecated and has no effect."
        "It has been replaced with cpu/mem based limits."
    )
)

tools_tar_file_option = click.option(
    '--tools-tar-file', '-t',
    help="Optional cluster-mgmt-tools tar file. If not present, it will be generated from source.",
    type=click.Path(dir_okay=False, path_type=pathlib.Path), required=False
)

cluster_mgmt_retention_days_option = click.option(
    '--cluster-mgmt-retention-days', '--cm-rd', show_default=True, default=30, type=click.INT,
    help="Maximum age (days) before cluster-mgmt logs are automatically removed."
)

cluster_mgmt_rotate_at_size_option = click.option(
    '--cluster-mgmt-rotate-at-size', '--cm-ras', show_default=True, default="100M", type=click.STRING,
    help="Maximum log file size (Ex. 400M, 2G) which will trigger rotation if exceeded."
)

cluster_mgmt_rotate_count_max_option = click.option(
    '--cluster-mgmt-rotate-count-max', '--cm-rcm', show_default=True, default=5, type=click.INT,
    help="Maximum number of rotated logs to retain per directory. Old logs in excess of this number "
         "are automatically removed, regardless of age."
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

disable_user_auth_option = click.option(
    "--disable-user-auth",
    help="Whether to disable user authentication.",
    is_flag=True, required=False
)

skip_internal_volumes_setup_option = click.option(
    "--skip-internal-volumes-setup",
    help="Whether to skip the cerebras internal volumes setup.",
    is_flag=True, required=False
)

disable_fabric_json_check_option = click.option(
    "--disable-fabric-json-check",
    help="Whether to disable fabric.json check during compile.",
    is_flag=True, required=False
)

pull_ecr_image_option = click.option(
    '--pull-ecr-image',
    help="Whether to pull ECR image during packaging time",
    is_flag=True, required=False, default=False
)

regen_tls_cert_option = click.option(
    "--recreate-tls-secret",
    help="Applying this flag will force the re-creation of the GRPC TLS secret. This will invalidate any "
         "certs stored on client machines so use with discretion.",
    is_flag=True, required=False
)

hosts_option = click.option(
    '--hosts', '-h', default="-", type=click.STRING,
    help="Comma separated list of hosts to deploy to. If not specified, all nodes discovered using "
         "the devinfra database. It's permissible to set hosts as empty string to force deploying "
         "to no hosts."
)

usernode_port_option = click.option(
    '--port', default=22, type=click.INT,
    help="SSH Port for usernode. Should only be used in dev mode."
)
usernode_login_username_option = click.option(
    '--username', "-u", default="root", type=click.STRING,
    help="Username to use when connecting to the usernode. This should be a user with root privileges."
)
usernode_login_password_option = click.option(
    '--password', "-p", default="-", type=click.STRING,
    help="SSH password for the usernode. Required if the usernode is not configured for passwordless SSH."
)

isolated_dashboards_option = click.option(
    '--use-isolated-dashboards',
    help="In cloud setup, csctl should present dashboard links in an isolated fashion for user namespaces.",
    is_flag=True, required=False
)

allow_reboot_option = click.option(
    '--allow-reboot',
    help="Allow node reboot if required during deployment",
    is_flag=True, required=False, default=False
)
