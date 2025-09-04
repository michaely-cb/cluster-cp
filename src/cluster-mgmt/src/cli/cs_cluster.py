#!/usr/bin/env python3
# Copyright 2022 Cerebras Systems, Inc.

import importlib
import pathlib
# disable annoying warning log about blowfish deprecation
import warnings
from cryptography.utils import CryptographyDeprecationWarning

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
from typing import Type
import logging

import click

from common.context import CliCtx
from common.context import load_cli_ctx
from common.lockfile import LockFile

pass_ctx = click.make_pass_decorator(CliCtx, ensure=True)

logger = logging.getLogger("cs_cluster")


def SubCLI(module: str) -> Type[click.Group]:
    mod = importlib.import_module(module)

    class Cli(click.Group):
        def list_commands(self, ctx):
            rv = []
            for attr in dir(mod):
                if attr.startswith("_"):
                    continue
                if isinstance(getattr(mod, attr), click.Command):
                    rv.append(attr.replace("_", "-"))
            return rv

        def get_command(self, ctx, name):
            name = name.replace("-", "_")
            try:
                return getattr(mod, name)
            except AttributeError:
                return

    return Cli


@click.group()
@click.option(
    "--src-dir",
    help="Override the default cluster-mgmt source root directory search paths. "
         "If unspecified, the system assumes the cli is being run from '$src_dir/src/cli'.",
    type=pathlib.Path, default=None, required=False
)
@click.option(
    "--cluster",
    help="Cluster within the 'clusters' to use. You can set a default using the command 'cluster use'.",
    required=False
)
@click.option("--verbose", "-v", is_flag=True, help="Enables verbose mode.")
@click.version_option("1.0")
@click.pass_context
def cli(ctx: click.Context, cluster, src_dir, verbose):
    """Cerebras cluster management utilities."""
    logging.basicConfig(
        format='%(asctime)s:%(name)s:%(levelname)s - %(message)s',
        level=logging.INFO  # root level logging level
    )
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logger = logging.getLogger("cs_cluster")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logging.getLogger("common").setLevel(logging.DEBUG if verbose else logging.INFO)

    ctx.obj = load_cli_ctx(src_dir, cluster)


remote_dir_option = click.option(
    '--remote-dir',
    help='Remote directory in k8s nodes for staging files. A reasonable default will be chosen if not set. In summary, '
         '/home is reasonable for KIND clusters while /tmp/deployment-packages/$DATE-$USER is reasonable otherwise.',
    default=""
)

preflight_check_option = click.option(
    '--preflight',
    help='Runs the csadm.sh preflight check before deploying.',
    default=False, is_flag=True,
)

skip_k8s_option = click.option(
    '--skip-k8s',
    help='Skip k8s cluster reconcile before installing, still do the general cluster setup of label nodes/CRD install'
         'Only required case is clusters brought up by kind directly for cli/canary test cases.',
    default=False, is_flag=True,
)


@cli.command(cls=SubCLI("commands.deploy"))
@remote_dir_option
@preflight_check_option
@skip_k8s_option
@click.option(
    '--cleanup-remote-dir',
    is_flag=True,
    help="Cleans up the remote build directory."
)
@click.pass_context
def deploy(ctx: click.Context, remote_dir: str, preflight: bool, skip_k8s: bool, cleanup_remote_dir: bool):
    """ Deploy directly to target cluster. """
    if remote_dir:
        ctx.obj.remote_build_dir = remote_dir
    ctx.obj.deploy_preflight_checks = preflight
    ctx.obj.skip_k8s = skip_k8s
    # would use ctx.with_resource, but Click version on jnodes is 6.0, require 7.0
    lock = LockFile(ctx.obj.build_dir / "deploy_lock")
    lock.acquire()

    @ctx.call_on_close
    def close_cluster_ctr():
        if cleanup_remote_dir:
            if ctx.obj.remote_build_dir.startswith("/tmp/"):
                logging.info(f"Cleaning up remote build directory: {ctx.obj.remote_build_dir}")
                ctx.obj.cluster_ctr.exec(f"rm -rf {ctx.obj.remote_build_dir}")
            else:
                logger.warning(
                    f"Remote build directory cleanup not completed because the directory {ctx.obj.remote_build_dir} is not located under /tmp. "
                    "Please clean up manually if needed."
                )
        ctx.obj.close_cluster_ctr()
        lock.release()


@cli.command(cls=SubCLI("commands.registry"))
@click.pass_context
def registry(ctx: click.Context):
    """
    Commands to interact with the container registry. Users can list all tags
    in with a repository, or get the labels associated with an image.
    """
    pass


@cli.command(cls=SubCLI("commands.cluster"))
@remote_dir_option
@click.pass_context
def cluster(ctx: click.Context, remote_dir: str):
    """ Cluster commands. """
    if remote_dir:
        ctx.obj.remote_build_dir = remote_dir


@cli.command(cls=SubCLI("commands.package"))
@remote_dir_option
@click.pass_context
def package(ctx: click.Context, remote_dir: str):
    """ Prepare a package for deployment. """
    if remote_dir:
        ctx.obj.remote_build_dir = remote_dir


@cli.command(cls=SubCLI("commands.session"))
@click.pass_context
def session(ctx: click.Context):
    """
    The session APIs help session management on a cluster. Sessions behind the scene
    are Kubernetes namespaces. Users can use the session APIs to create, update, get,
    list and delete sessions. When creating and updating sessions, users also have an
    option to specify a release artifact path, so job operator, cluster server and
    usernode deployment respects the artifacts under that path.
    """
    pass


@cli.command(cls=SubCLI("commands.debug_artifact"))
@click.pass_context
def debug_artifact(ctx: click.Context):
    """
    The debug artifact APIs help users download debug artifacts from a wsjob.
    """
    pass


if __name__ == '__main__':
    cli()
