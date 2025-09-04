import json
import logging
import shlex

from click.testing import CliRunner

from common.context import load_cli_ctx
from cs_cluster import cli

from .conftest import (
    get_cluster_server_image,
    assert_cluster_server_ready,
)

import pytest


logger = logging.getLogger("cs_cluster.test.e2e_registry")


def test_labels(caplog, kind_cluster: str, run_with_caplog, system_ns: str = 'job-operator'):
    """ This test checks if the registry labels command works correctly. """
    ctx = load_cli_ctx(None, kind_cluster)

    assert_cluster_server_ready(ctx, system_ns)
    
    server_image = get_cluster_server_image(kind_cluster)

    cmd = f"--cluster {kind_cluster} registry list-labels --image {server_image}"

    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, f"Command failed with exit code {rv}: {se}"
  
    labels = json.loads(so)
    assert server_image in labels

    image_labels = labels[server_image]
    assert "semantic_version" in image_labels, f"Expected 'semantic_version' in labels but got {image_labels}"


def test_tags(
    caplog, kind_cluster: str, run_with_caplog, system_ns: str = 'job-operator'
):
    """ This test checks if the registry tags command works correctly. """
    ctx = load_cli_ctx(None, kind_cluster)

    assert_cluster_server_ready(ctx, system_ns)

    # get server_image of format name:tag
    server_image = get_cluster_server_image(kind_cluster) 
    server_image_name = server_image.split(':')[0]

    cmd = f"--cluster {kind_cluster} registry list-tags --repo {server_image_name}"

    rv, so, se = run_with_caplog(cmd)
    assert rv == 0, f"Command failed with exit code {rv}: {se}"

    tags = json.loads(so)
    assert isinstance(tags, list), f"Expected a list of tags. got  {tags} of type {type(tags)}"
