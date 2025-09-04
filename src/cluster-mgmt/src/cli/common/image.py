import json
import logging
import os
import tarfile
import pathlib
import random
import re
import time
from tempfile import TemporaryDirectory
from typing import List
from typing import Union

import click

from common import DockerImage
from common import ECR_PUBLIC_REPO
from common.cluster import FileCopyCmd
from common.context import CliCtx
from common.models import Cluster

logger = logging.getLogger("cs_cluster.image")


def extract_image(image: Union[pathlib.Path, DockerImage]) -> DockerImage:
    if image is None:
        return DockerImage(None, None)

    if isinstance(image, DockerImage):
        return image

    # short-circuit if .docker images since cbcore can be very large to untar
    pattern = r'^(cbcore|cluster-server|job-operator)-(.+)\.docker$'
    match = re.match(pattern, image.name)
    if match:
        component, image_tag = match.groups()
        return DockerImage(component, image_tag)

    # otherwise, lookup the tag in a generic way
    with tarfile.open(image) as tar:
        manifest_file = tar.extractfile('manifest.json')
        manifest = json.load(manifest_file)
    repo_tags = manifest[0]['RepoTags']
    return DockerImage.of(repo_tags.pop())


def is_image_loaded(ctx: CliCtx, img: str) -> bool:
    img = DockerImage.of(img)
    repo, tag = img.local_name.split("/", 1)[1].split(":", 1)
    # This command always runs on the lead mgmt node, so using localhost:5000 should work reliably.
    # If this changes in future, we need to update the registry URL.
    cmd = (
        f"curl -sk --fail --retry 3 --max-time 5 -o /dev/null "
        '-H "Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json" '
        f"https://localhost:5000/v2/{repo}/manifests/{tag}"
    )
    is_loaded = ctx.cluster_ctr.exec(cmd)[0] == 0
    logger.info(f"Image {img} loaded: {is_loaded}")
    return is_loaded


# build image if not found locally
def build_image(ctx: CliCtx,
                img: Union[DockerImage, str], makefile: str, target: str,
                skip_rebuild: bool = True) -> DockerImage:
    img = DockerImage.of(img)
    platform = ctx.build_platform()
    target_arch = ctx.build_arch()
    ret, stdout, _ = ctx.exec(f"docker image inspect {img} -f {{{{.Architecture}}}}")
    if not skip_rebuild or ret != 0 or stdout.strip() != target_arch:
        if not skip_rebuild:
            logger.info("force rebuild to ensure pick up local change without upgrading version")
        elif ret != 0:
            logger.info(f"{img} doesn't exist locally, start building")
        else:
            logger.info(f"current arch:{stdout.strip()} doesn't match target-arch:{target_arch}, start rebuilding")
            # start rebuilding if architecture not same
            # we need to remove the images with all tags.
            # otherwise, old images can stick around and rebuild didn't really happen.
            ret, stdout, _ = ctx.exec(f"docker image inspect {img} -f {{{{.Id}}}}")
            if ret == 0:
                logger.info(f"removing image {img} with its id {stdout}")
                ctx.exec(f"docker rmi -f {stdout}")
            else:
                ctx.exec(f"docker rmi -f {img}")
        cmd = f"make -C {makefile} {target} platform={platform}"
        logger.info(f"calling {cmd} to build image")
        ctx.must_exec(cmd)
    return DockerImage.of(img)


def copy_image_to_remote(ctx: CliCtx, image_ref: str, remote_image: str):
    """
    Ensure the image tarball exists as 'remote_image' using the most efficient route possible.
    Ensure image names are unique on remote host since this is a shared directory.
    """
    remote_image = pathlib.Path(remote_image)
    remote_image_dir = remote_image.parent
    ctx.cluster_ctr.must_exec(f"mkdir -p {remote_image_dir}")

    # ensure image exists at remote_image_file_copy
    if ctx.cluster_ctr.exec(f"stat {image_ref}")[0] == 0:
        remote_image_file = pathlib.Path(image_ref)
        remote_image_file_copy = remote_image_dir / f"image-{_uniq_id()}{''.join(remote_image_file.suffixes)}"
        # TODO: Could this be made into a symlink?
        ctx.cluster_ctr.exec(f"cp {remote_image_file} {remote_image_file_copy}")
    else:
        local_image_file = image_ref
        if ctx.exec(f"stat {local_image_file}")[0] == 0:
            local_image_file = pathlib.Path(local_image_file)
        else:
            local_image_file = ctx.build_dir / f"image-{_uniq_id()}.tar"
            ctx.must_exec(f"docker pull {image_ref}")
            if ctx.exec(f"docker save {image_ref} -o {local_image_file}")[0] != 0:
                raise click.ClickException(f"failed to save image {image_ref} to {local_image_file}")
        remote_image_file_copy = remote_image_dir / local_image_file.name
        ctx.cluster_ctr.exec_cmds([
            FileCopyCmd(local_image_file, remote_image_file_copy)
        ])

    if re.match(r".*\.(gz|docker)$", remote_image_file_copy.name):
        ctx.cluster_ctr.exec(
            f"bash -c 'gzip --force --decompress < {remote_image_file_copy} > {remote_image} && "
            f"rm {remote_image_file_copy}'")
    else:
        ctx.cluster_ctr.exec(f"mv {remote_image_file_copy} {remote_image}")


def get_image_target_nodes(cluster: Cluster, systems: List[str]) -> List[str]:
    """ Get nodes targeted by the systems or all node if no systems. Always put mgmt node first. """

    mgmt_node = next((n.name for n in cluster.nodes if n.role == "management"), None)

    if not systems:
        rv = [n.name for n in cluster.nodes]
    else:
        rv = set()
        for sys in systems:
            rv.update({n.name for n in cluster.get_system_nodes(sys)})
        if len(systems) > 1:
            rv.update({n.name for n in cluster.get_system_nodes()})  # add BR nodes
        rv = list(rv)

    if mgmt_node in rv:
        return sorted(rv, key=lambda x: x == mgmt_node, reverse=True)
    elif mgmt_node:
        return [mgmt_node] + rv
    return rv


def _uniq_id() -> str:
    return str(int(time.time())) + "-" + str(random.randint(0, 1000))
