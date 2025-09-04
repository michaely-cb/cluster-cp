import json
import logging
from typing import Any, Dict, List, Optional

import click
from common.context import CliCtx
from cs_cluster import pass_ctx

logger = logging.getLogger("cs_cluster.registry")

REGISTRY_URL = "https://127.0.0.1:5000"

def request_registry(
    ctx: CliCtx,
    endpoint: str,
    headers=None,
):
    """
    Helper function to make requests to the registry.
    """

    curl_cmd = f"curl -sk -X GET '{REGISTRY_URL}/{endpoint}'"

    if headers:
        for key, value in headers.items():
            curl_cmd += f" -H '{key}: {value}'"

    _, stdout, _ = ctx.cluster_ctr.must_exec(curl_cmd)

    return stdout


def image_digest(ctx: CliCtx, image_name: str, tag: str) -> str:
    headers = {"Accept": "application/vnd.docker.distribution.manifest.v2+json"}

    response = request_registry(
        ctx,
        endpoint=f"v2/{image_name}/manifests/{tag}",
        headers=headers,
    )

    manifest = json.loads(response)
    config = manifest.get('config')
    if not config:
        raise click.ClickException(f"Config not found in manifest for image {image_name} with tag {tag}")
    digest = config.get('digest')
    if not digest:
        raise click.ClickException(f"Digest not found in manifest for image {image_name} with tag {tag}")
    return digest


def image_labels(ctx: CliCtx, image_name: str, tag: str) -> Dict[str, str]:
    digest = image_digest(ctx, image_name, tag)

    response = request_registry(ctx, endpoint=f"v2/{image_name}/blobs/{digest}")

    container_config = json.loads(response).get('container_config')
    if not container_config:
        raise click.ClickException(f"Container config not found for image {image_name} with tag {tag}")
    
    labels = container_config.get('Labels')
    if not labels:
        {}

    return labels


@click.command()
@click.option(
    "--image",
    help="Name of the image with tag to list labels for.",
    multiple=True,
    required=True,
)
@pass_ctx
def list_labels(ctx: CliCtx, image: str):
    """
    List labels of an image.
    """

    labels = {}

    for i in image:
        if ":" not in i:
            raise click.ClickException("Image must be specified in the format 'image_name:tag'")

        image_name = i.split(":")[0]
        tag = i.split(":")[1]

        try:
            labels[i] = image_labels(ctx, image_name, tag)
        except Exception as e:
            raise click.ClickException(f"Error retrieving labels: {e}")
    
    click.echo(json.dumps(labels, indent=2))


@click.command()
@click.option(
    "--repo",
    help="Name of the repo to list tags for.",
    required=True,
)
@pass_ctx
def list_tags(ctx: CliCtx, repo: str):
    """
    List tags from the registry for a given image repository.
    """

    try:
        response = request_registry(ctx, f"v2/{repo}/tags/list")

        tags = json.loads(response).get('tags')
        if tags is None:
            raise click.ClickException(f"repository name {repo} not known to registry")
    except Exception as e:
        raise click.ClickException(f"Error retrieving tags: {e}")

    try:
        click.echo(json.dumps(tags, indent=2))
    except Exception as e:
        logger.error(f"Error formatting tags as JSON: {e}")
        raise click.ClickException(f"Error formatting tags as JSON: {e}")
