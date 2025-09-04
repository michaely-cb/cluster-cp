import base64
import json
import logging
import pathlib
import typing

import click
import yaml
import time

import common
import common.models
import common.validate
from common import fix_pathlib_path
from common.cluster import Cmd
from common.cluster import DirCopyCmd
from common.cluster import FileCopyCmd
from common.cluster import FileGetCmd
from common.cluster import MkdirCmd
from common.cluster import FileCopyCmd
from common.cluster import SSHHelper
from common.context import CliCtx
from common.context import REMOTE_CLUSTERS_PATH
from common.devinfra import DevInfraDB
from common.models import parse_cluster_config
from common.scrape import scrape_cluster_config
from cs_cluster import pass_ctx

logging.getLogger("paramiko").setLevel(logging.WARNING)
logger = logging.getLogger("cs_cluster.cluster")


def _make_credentials(username: str, password: str = None, pem: str = None) -> dict:
    """
    :param username:
    :param password: must be specified if pem not specified
    :param pem: rsa private key file base64 encoded
    :return: credential file for all host machines
    """
    pass_or_pem = {}
    if pem:
        pass_or_pem['pem'] = pem
    elif password:
        pass_or_pem['password'] = password
    else:
        raise Exception("pem or password must be specified")

    return {"credentials": [{
        "kind": "ssh",
        "host": "*",
        "username": username,
        **pass_or_pem
    }]}


def _create_kind_config(ctx: CliCtx, system_namespace):
    cluster_config = ctx._cluster_dir / "cluster.yaml"
    if not cluster_config.exists():
        if ctx.cluster_cfg_k8s is None:
            raise click.ClickException(f"{cluster_config} does not exist")
        else:
            logger.debug("given cluster is already initialized, skipping")
            return

    _update_cluster_config(ctx, cluster_config, system_namespace)
    return


@click.command()
@click.option('--name', "-n",
              help="Name of the cluster. This name should correspond with a name known to the devinfra team's central "
                   "database of clusters. This parameter is required if 'clusterjson' not provided.",
              required=False, type=str,
              )
@click.option('--clusterjson', "-j",
              help="Path to cluster json file. These files are generally maintained by the devinfra team in their "
                   "central database and can be fetched automatically using the --name parameter. This cluster file "
                   "is a json object containing keys like 'X_hosts' where X is the type of node (swarmx, memoryx, etc) "
                   "and the value is a list of strings which are the hostnames of those nodes. Additionally, it may "
                   "contain a field 'systems' which is a string list containing the hostnames of the systems in the "
                   "cluster.",
              type=click.File(), required=False
              )
@click.option('--username', "-u",
              help="Username to ssh and scrape netconfig on linux hosts.",
              type=str, default="root", required=False,
              )
@click.option('--password', "-p",
              help="Password to ssh and scrape netconfig on linux hosts. Required if pem not set.",
              type=str, required=False
              )
@click.option('--pem',
              help="Pem file to ssh and scrape netconfig on linux hosts. Required if password not set.",
              type=click.Path(dir_okay=False), required=False
              )
@click.option('--network',
              help="Network configuration file. Required to associate broadcastreduce's NICs with csPorts. "
                   "This JSON file comes from IT and is sometimes referred to as a network tiering file. "
                   "If it is not specified, but a file 'network.json' exists in the cluster/<cluster> folder, then "
                   "it will be used automatically. If no network file exists and is not given, csPorts will be "
                   "assigned in a round-robin fashion to broadcastreduce nodes which may produce unusable "
                   "configuration.",
              type=click.Path(dir_okay=False, path_type=pathlib.Path)
              )
@click.option("--skip-validate",
              help="Skip validation of the generated configuration.",
              type=bool, required=False, is_flag=True,
              )
@click.option('--output', "-o",
              help="Path to output file. If not specified, the config will be output to the specified file rather than "
                   "uploaded to the cluster.",
              type=click.Path(dir_okay=False), required=False
              )
@common.system_namespace_option
@click.option("--use-short-name",
              help="Whether to use short names for nodes/systems.",
              type=bool, required=False, is_flag=True,
              )
@pass_ctx
def create_config(ctx: CliCtx,
                  name=None, clusterjson=None,
                  username=None, password=None, pem=None,
                  network: typing.Optional[pathlib.Path] = None,
                  skip_validate: bool = False,
                  system_namespace: str = None,
                  use_short_name: bool = False,
                  output: typing.Optional[pathlib.Path] = None):
    """
    Create a cluster.yaml file by scraping the nodes discovered from either the passed in configuration file or via
    pulling the cluster details from the devinfra database. Additionally, amend the broadcastreduce network with
    csPort metadata with a network.yaml file.
    """
    if ctx.in_kind_cluster:
        _create_kind_config(ctx, system_namespace)
        return

    network = fix_pathlib_path(network)
    output = fix_pathlib_path(output)
    if clusterjson:
        cluster_doc = json.loads(clusterjson.read())
    else:
        if not name:
            name = ctx.cluster_name
        with DevInfraDB() as db:
            cluster_doc = db.get_cluster(name)
            if not cluster_doc:
                all_names = '\n' + '\n'.join(db.list_cluster_names())
                click.echo(f"cluster '{name}' was not found. Try one of:{all_names}", err=True)
                return 1
        logger.debug(f"found json: {json.dumps(cluster_doc)}")

    if not network:
        system_data_dir = REMOTE_CLUSTERS_PATH / ctx.cluster_name
        network = system_data_dir / "network.json"
        if not network.is_file():
            logger.warning(f"network json file not present in {system_data_dir}, the resulting config may be invalid")

    if network.is_file():
        logger.info(f"network info provided at {network.absolute()}, generating config without scraping nodes")
        cluster = parse_cluster_config(json.loads(network.read_text()))
    else:
        logger.info("network information not provided, scraping config from nodes")
        if not password and not pem:
            logger.debug("password or pem not provided, try resolve creds from env")
            try:
                creds = ctx.resolve_ssh_creds()
                ssh_helper = SSHHelper(creds["username"], password=creds.get("password"), pem=creds.get("pem"))
            except:
                click.echo("must pass --pem or --password to scrape info from linux nodes", err=True)
                return 1
        else:
            ssh_helper = SSHHelper(username, password=password, pem=pem)
        cluster = scrape_cluster_config(cluster_doc, ssh_helper.must_exec, use_short_name)

    rv = cluster.to_yaml()
    logger.debug(f"generated cluster.yaml:\n{rv}")
    fp = ctx.write_build_file("cluster.yaml", rv)

    if not skip_validate:
        problems = common.validate.static_validate_config(cluster)
        if len(problems) > 0:
            raise click.ClickException("\n".join(problems))

    if output:
        logger.info(f"writing config to {output}")
        with open(output, "w") as f:
            f.write(rv)
        return

    try:
        logger.info(f"updating cluster config in namespace {system_namespace}")
        _update_cluster_config(ctx, pathlib.Path(fp), system_namespace)
    except Exception as e:
        logger.error(f"failed to update cluster config: {e}")
        logger.warning(
            "You may need to manually update the cluster config with the following commands\n\n"
            "# copy the cluster.yaml file to the mgmt node as 'cluster.yaml'\n"
            f"scp {fp} root@$MGMT_NODE:/tmp/cluster.yaml\n\n"
            "# then ssh to the mgmt node and run:\n"
            "kubectl create cm cluster --from-file=clusterConfiguration.yaml=/tmp/cluster.yaml "
            f"-n {system_namespace} --dry-run -o yaml | kubectl apply -f -"
        )
        raise click.ClickException("failed to update cluster config")


@click.command()
@click.argument('cluster', type=str, required=False)
@pass_ctx
@click.pass_context
def use(click_ctx: click.Context, cli_ctx: CliCtx, cluster: typing.Optional[str] = None):
    """
    Set the default cluster to use for the command line tool. The cluster may not exist, in which case, the user will
    be prompted to create it.
    Parameters:
        cluster: The name of the cluster to use in the clusters/ directory.
    """
    cluster_flag = click_ctx.parent.parent.params.get("cluster", None)
    if bool(cluster) and bool(cluster_flag) and cluster != cluster_flag:
        click.echo("only one of --cluster or CLUSTER must be specified for 'use CLUSTER' command", err=True)
        exit(1)
    if cluster_flag:
        cluster = cluster_flag

    cs_cluster_conf = f"{cli_ctx.src_dir}/src/cli/.cs_cluster.conf"
    click.echo(f"Writing {cluster} in {cs_cluster_conf}")
    pathlib.Path(f"{cs_cluster_conf}").write_text(f"cluster: {cluster}")


@click.command()
@click.option('--username', "-u",
              help="Username to ssh on every node in the cluster.yaml file.",
              type=str, required=True,
              )
@click.option('--password', "-p",
              help="Password for given username. Required if pem not set.",
              type=str, required=False
              )
@click.option('--pem',
              help="Pem file path for given username. Required if password not set.",
              type=click.Path(dir_okay=False, path_type=pathlib.Path), required=False
              )
@pass_ctx
def signin(ctx: CliCtx, username, password=None, pem: typing.Optional[pathlib.Path] = None):
    """ Create a .ssh file with credentials to access a cluster.
    Note: this is only necessary if your cluster uses non-default credentials (unlikely).
    """
    if ctx.in_kind_cluster:
        logger.debug("no need to sign in to kind")
        return

    if not password and not pem:
        raise click.ClickException("must set --password or --pem")

    if pem:
        pem = str(base64.b64encode(pem.read_bytes()), 'utf-8')

    output = yaml.safe_dump(_make_credentials(username, password, pem))
    ctx.write_file(".ssh.conf", output)


def _update_cluster_config(ctx: CliCtx, cluster_config: pathlib.Path, system_namespace: str):
    ctx.cluster_ctr.must_exec("mkdir -p /opt/cerebras/cluster")
    cfg_path_remote = "/opt/cerebras/cluster/cluster.yaml"
    ctx.cluster_ctr.exec_cmds([FileCopyCmd(
        src=cluster_config,
        dst=cfg_path_remote
    )])

    k8_installed, _, _ = ctx.cluster_ctr.exec("kubectl get nodes")
    if k8_installed != 0:
        return

    verb = "create"
    ret, stdout, _ = ctx.cluster_ctr.exec(
        f"bash -c 'kubectl get cm cluster -n{system_namespace} -oyaml'"
    )
    if ret == 0:
        logger.debug(f"existing config:\n{stdout}")
        verb = "replace"
    logger.debug(f"using replacement config:\n{cluster_config.read_text()}")

    ctx.cluster_ctr.exec(f"kubectl create ns {system_namespace}")  # create namespace if it doesn't exist
    ctx.cluster_ctr.exec(f"kubectl label ns {system_namespace} system-namespace= --overwrite")
    ctx.cluster_ctr.must_exec(
        f"bash -c 'kubectl create cm cluster -n{system_namespace} " +
        f"--from-file=clusterConfiguration.yaml={cfg_path_remote} --dry-run=client -o yaml | kubectl {verb} -f-'"
    )


@click.command()
@common.k8s_version_option
@common.force_option
@click.option(
    '--reboot',
    help="Reboot nodes after wiping",
    is_flag=True, required=False, default=False
)
@pass_ctx
def clean(ctx: CliCtx, k8s_version: str, force: bool, reboot: bool):
    """ Teardown kubernetes cluster and cleanup k8s state on cluster nodes. """
    # Generate the teardown node list from clusterDB, prompt for confirmation
    nodes = [n.name for n in ctx.cluster_cfg.nodes]
    logger.info(f"preparing to clean {len(nodes)} nodes: {','.join(nodes)}")
    if not force:
        if not click.confirm(
                f"Are you sure you want to wipe k8s from {ctx.cluster_name} ({len(nodes)} nodes)?",
                default=False
        ):
            logger.info("aborted")
            return

    logger.info("copying teardown scripts")
    from commands.package import _k8s  # avoid circular import
    script_path = _k8s(ctx, from_k8s_version=k8s_version, k8s_version=k8s_version)
    component = script_path[0].parts[0]
    remote_script_root = f"{ctx.remote_pkg_dir}/{component}"

    # Create list of nodes to teardown using the info from clusterDB rather than
    # whatever config is on the cluster - this avoids inconsistencies
    head_node = ctx.cluster_ctr.control_plane_name()
    teardown_cp_list = ctx.build_dir.joinpath(".controlplane.remove.list")
    teardown_cp_list.write_text("\n".join([
        n.name for n in ctx.cluster_cfg.nodes
        if n.role == "management" and n.name != head_node
    ]))
    teardown_wk_list = ctx.build_dir.joinpath(".worker.remove.list")
    teardown_wk_list.write_text("\n".join([n.name for n in ctx.cluster_cfg.nodes if n.role != "management"]))

    # Copy artifacts and start the teardown process
    ctx.cluster_ctr.exec_cmds([
        MkdirCmd(remote_script_root),
        DirCopyCmd(f"{ctx.pkg_dir}/{component}", remote_script_root),
        FileCopyCmd(teardown_cp_list, f"{remote_script_root}/{teardown_cp_list.name}"),
        FileCopyCmd(teardown_wk_list, f"{remote_script_root}/{teardown_wk_list.name}"),
    ])
    logger.info("starting teardown - this may take some time...")
    flags = f"-y"
    if reboot:
        flags += " --reboot"
    rv, so, se = ctx.cluster_ctr.exec(f"{remote_script_root}/k8_init.sh teardown_cluster {flags}")
    logger.info(f"stderr:\n{se}")
    logger.info(f"stdout:\n{so}")
    if rv != 0:
        raise click.ClickException("teardown failed")

    # reboot the head node as this is not done in the cleanup script itself...
    if reboot:
        logger.info("teardown completed, rebooting mgmt node...")
        ctx.cluster_ctr.exec("reboot")
        time.sleep(30)
        timeout = time.time() + 60 * 15
        while True:
            try:
                rv, _, _ = ctx.cluster_ctr.exec("uptime")
                if rv == 0:
                    _, so, _ = ctx.cluster_ctr.exec("systemctl is-system-running")
                    logger.info(f"{head_node} state: {so}")
                    if "starting" not in so:  # ensure system not still in boot-up phase
                        break
                elif time.time() > timeout:
                    raise RuntimeError(f"node {head_node} didn't reboot within 15 minutes")
            except:
                pass
            logger.info("waiting for node to reboot...")
            time.sleep(10)

    logger.info("teardown succesful")
