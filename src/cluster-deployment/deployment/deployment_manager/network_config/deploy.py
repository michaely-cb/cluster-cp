"""
This tool can be used to configure the L3 parameters for a CS cluster.
It calls network_config.tool commands internally.
"""
import json
import logging
import os
import shutil
import sys
import tempfile
import typing
from argparse import ArgumentParser
from collections import defaultdict
from typing import Optional

from deployment_manager.network_config.common.context import NetworkCfgCtx, ObjCredProviderFn, OBJ_TYPE_SR, \
    NetworkCfgDoc, AppCtx
from deployment_manager.network_config.common.task import summarize_task_result_errors, NetworkTask, \
    generate_tasks_for_entity_type, run_tasks, generate_tasks_for_names, ConfigTask
from deployment_manager.network_config.deployment import (
    NodeInterfaceStatusCheck, SwitchVersionAndModelCheck, generation_classes,
    init_classes,
    switch_deployment_classes,
    SwitchEnableRoce,
    SwitchGetRoceStatus,
)
from deployment_manager.network_config.deployment.base import DeploymentConfig
from deployment_manager.network_config.deployment.config import NetworkConfig
from deployment_manager.network_config.deployment.config_uploader import SwitchUpload
from deployment_manager.network_config.deployment.precheck import PrecheckOutput
from deployment_manager.network_config.deployment.switches import AWSwitches
from deployment_manager.network_config.deployment.validation import SwitchPingCheck
from deployment_manager.network_config.tasks.node import NodeNMConnectionCleanupNetworkTask, NodeSysctlReloadTask, \
    NodeNetworkManagerRestartTask, NodeIfaceReloadTask, NodeRebootTask, NodeAwaitRebootTask, NodeUploadFilesNetworkTask, \
    NodeInterfaceCheckNetworkTask, NodePingTestNetworkTask
from deployment_manager.network_config.tasks.node_ping import NodePingTestGenerator, PingTestKind
from deployment_manager.network_config.tasks.system import SystemActivateTask, SystemNetworkConfigTask, \
    SystemStandbyTask

logger = logging.getLogger(__name__)


def cmd_precheck(config_file: str) -> typing.Tuple[int, typing.List[PrecheckOutput]]:
    """ Run pre-flight checks for L3 deployment
    Args
        config_file: path to master_config.yaml
    """

    cfg = DeploymentConfig(config_file)
    ret = 0
    output = list()

    tmp_cfg = tempfile.mktemp()
    with open(tmp_cfg, 'w') as f:
        json.dump(cfg.create_tmp_config(), f, indent=4)

    with NetworkCfgCtx(tmp_cfg, deploy_config=cfg) as ctx:
        for cls in (NodeInterfaceStatusCheck, SwitchVersionAndModelCheck):
            retval = cls(ctx).run()
            ret |= retval[0]
            output += retval[1]
    return ret, output


def cmd_init(config_file: str, output_file: str, app_ctx: AppCtx, dry_run=False) -> typing.Tuple[int, str]:
    """ Initialize network config file for cluster
    """
    cfg = DeploymentConfig(config_file, output_file=output_file)
    ret = 0
    msg = None
    for cls in init_classes:
        if issubclass(cls, ConfigTask):
            with NetworkCfgCtx(
                    network_config_path=cfg.network_config_filename,
                    app_ctx=app_ctx,
                    deploy_config=cfg,
                    persist_config_updates=True,
            ) as ctx:
                result = cls(ctx).run()
                if not result.ok:
                    return 1, result.message
        else:
            ret, _, msg = cls(cfg).run(dry_run=dry_run)
            if ret:
                break
    return ret, msg


def cmd_generate(
        master_config_path,
        network_config_path,
        artifacts_dir,
        app_ctx: AppCtx,
        dry_run=False,
) -> typing.Tuple[int, str]:
    """
    Generate L3 config for cluster.
    Returns:
        Tuple[int, str]: non-zero return code if error occurred, non-empty error message if error occurred
    """
    logger.info("re-generating network config")

    cfg = DeploymentConfig(master_config_path, output_file=network_config_path, artifacts_dir=artifacts_dir)

    ret = 0
    msg = None
    err_msg = ""
    for cls in generation_classes:
        if issubclass(cls, (ConfigTask, NetworkTask,)):
            with NetworkCfgCtx(
                    network_config_path=cfg.network_config_filename,
                    app_ctx=app_ctx,
                    deploy_config=cfg,
                    persist_config_updates=True,
            ) as ctx:
                if issubclass(cls, ConfigTask):
                    # execute directly
                    result = cls(ctx).run()
                    if not result.ok:
                        return 1, result.message
                else:
                    # generate tasks for the entity type and run them
                    tasks = generate_tasks_for_entity_type(ctx, cls)
                    results = run_tasks(tasks)
                    err_msg = summarize_task_result_errors(results)
            if err_msg:
                return 1, f"Task {cls.__name__} had unrecoverable failures: {err_msg}"
        else:
            ret, _, msg = cls(cfg).run(dry_run=dry_run)
            if ret:
                break
    return ret, msg


def cmd_push_switches(args):
    """ Upload L3 config to switches
    """
    fname = args.config_file
    cfg = DeploymentConfig(
        fname,
        output_file=args.network_config,
        artifacts_dir=args.artifacts_dir
    )
    ret = 0
    msg = None
    dry_run = getattr(args, "dry_run", False)
    # Enable RoCE for Dell and Edgecore switches, if there are any
    # This needs to be done separately because switches from these vendors need a reboot for the config to stick.
    # Since this is an expensive operation, the current RoCE status is read and updated in network.json. The enable
    # operation only runs on switches where it isn't already enabled.
    for cls in [SwitchGetRoceStatus, SwitchEnableRoce] + switch_deployment_classes:
        ret, _, msg = cls(cfg, names=args.names).run(dry_run=dry_run)
        if ret:
            break
    if not ret:
        for cls in switch_deployment_classes:
            ret, _, msg = cls(cfg, config_section="switches", ports_for=args.names).run(
                dry_run=dry_run
            )
            if ret:
                break
    return ret, msg


def cmd_push_systems(
        names: typing.List[str],
        config_file: str,
        network_config: str,
        artifacts_dir: str,
        app_ctx: AppCtx,
        dry_run: bool = False,
):
    """ Upload L3 config to systems
    Args:
        names: list of system names to deploy to
        config_file: YAML config file which contains cluster information, master config
        network_config: File which contains the generated JSON network config
        artifacts_dir: Directory which contains generated config files
        app_ctx: Application deployment context
    """
    logger.info(f"pushing network config to systems, names={names}")

    fname = config_file
    cfg = DeploymentConfig(fname, output_file=network_config, artifacts_dir=artifacts_dir)
    for cls in switch_deployment_classes:
        ret, _, msg = cls(cfg, config_section="systems", ports_for=names).run(
            dry_run=dry_run
        )
        if ret:
            return ret, msg

    def _network_reload_supported(ctx: NetworkCfgCtx, name: str) -> bool:
        vers = ctx.network_config.get_system_version(name)
        if not vers:
            return False
        major, minor, patch = vers.split('.', maxsplit=2)
        if int(major) > 2:
            return True
        elif int(major) == 2:
            if int(minor) < 5:
                return False
            else:
                return True
        else:
            return False

    # TODO: if a single system fails to standby or activate, the rest of the tasks should proceed
    with NetworkCfgCtx(cfg.network_config_filename, app_ctx=app_ctx, deploy_config=cfg) as ctx:
        # For versions >= 2.5 (SystemActivate, SystemNetworkConfig) is the preferred
        # and much quicker sequence
        names_no_standby = []
        names_standby = []
        for n in names:
            if _network_reload_supported(ctx, n):
                names_no_standby.append(n)
            else:
                names_standby.append(n)
        if names_no_standby:
            for cls in (SystemActivateTask, SystemNetworkConfigTask,):
                tasks = generate_tasks_for_names(ctx, cls, names_no_standby)
                results = run_tasks(tasks)
                err_msg = summarize_task_result_errors(results)
                if err_msg:
                    return 1, f"Task {cls.__name__} had unrecoverable failures: {err_msg}"
        if names_standby:
            for cls in (SystemStandbyTask, SystemNetworkConfigTask, SystemActivateTask,):
                tasks = generate_tasks_for_names(ctx, cls, names)
                results = run_tasks(tasks)
                err_msg = summarize_task_result_errors(results)
                if err_msg:
                    return 1, f"Task {cls.__name__} had unrecoverable failures: {err_msg}"
    return 0, ""


def cmd_push_nodes(
        config_file: str,
        network_config: str,
        artifacts_dir: str,
        names: Optional[typing.List[str]] = None,
        app_ctx: Optional[AppCtx] = None,
        dry_run: bool = False,
) -> typing.Tuple[int, str]:
    """
    Upload L3 config to nodes
    Args:
        config_file: master config file path
        network_config: network json file path
        artifacts_dir: base directory containing config files
        names: list of names to deploy to, no list = deploy to all
        app_ctx: application deployment context
    """
    logger.info(f"pushing network config to nodes, names={names}")

    fname = config_file
    cfg = DeploymentConfig(fname, output_file=network_config, artifacts_dir=artifacts_dir)
    for cls in switch_deployment_classes:
        ret, _, msg = cls(cfg, config_section="nodes", ports_for=names).run(dry_run=dry_run)
        if ret:
            return ret, msg

    # this ends up looking a lot like ansible and could probably be replaced with ansible
    with NetworkCfgCtx(network_config, app_ctx=app_ctx, deploy_config=cfg) as ctx:
        names = names if names else ctx.network_config.get_names_by_type(OBJ_TYPE_SR)
        reboot_names = [n for n in names if not n == cfg.root_server]
        for cls in [
                NodeNMConnectionCleanupNetworkTask,
                NodeUploadFilesNetworkTask,
                NodeSysctlReloadTask,
                NodeNetworkManagerRestartTask,
                NodeIfaceReloadTask,
                (NodeRebootTask, reboot_names),  # don't reboot self
                (NodeAwaitRebootTask, reboot_names),
        ]:
            if isinstance(cls, tuple):
                cls, target_names = cls
            else:
                target_names = names
            tasks = generate_tasks_for_names(ctx, cls, target_names)
            results = run_tasks(tasks)
            err = summarize_task_result_errors(results)
            if err:
                return 1, f"{cls.__name__} failed: {err}"
    return 0, ""


def _optimize_node_ping_sources(cfg: NetworkCfgDoc, srcs: typing.List[str], dsts: typing.List[str]) -> typing.List[str]:
    """
    Optimize srcs by filtering any srcs which share the same switches as the dsts to exercise routing during ping
    check if possible. If there's no non-local srcs, return the original srcs.

    Note we could optimize further by splitting the dsts into non-intersecting groups of switches with groups of srcs
    but that would be more complex and not worth the effort for now.
    """
    dst_switches = set()
    for dst in dsts:
        obj = cfg.get_raw_object(dst) or {}
        for inf in obj.get('interfaces', []):
            if inf.get('switch_name'):
                dst_switches.add(inf['switch_name'])
    non_local_srcs = []
    for src in srcs:
        obj = cfg.get_raw_object(src) or {}
        for inf in obj.get('interfaces', []):
            if inf.get('switch_name') in dst_switches or inf.get('state') != 'up':  # don't risk using down interfaces
                break
        else:
            non_local_srcs.append(src)
    if non_local_srcs:
        return non_local_srcs
    return srcs


def validate_nodes(
        network_config: str,
        node_names: typing.List[str],
        app_ctx: Optional[AppCtx] = None,
) -> typing.Dict[str, str]:
    """
    Run validation on new nodes. First check the interface status on all the nodes in the cluster to:
    1. get a list of reachable nodes for ping validation
    2. update the node interface status for ping validation
    Then, run the gateway ping test as a quick validation that new nodes can reach their gateway / switch was configured
    Then, run node-to-node ping test to see that routing works.
    Returns:
        Dict[str, str]: node name to non-empty error message if validation failed for this node
    """
    if not node_names:
        return {}

    node_err = {name: "" for name in node_names}
    validation_set = set(node_names)

    with NetworkCfgCtx(network_config, app_ctx=app_ctx) as ctx:

        # refresh all node's interface status for purpose of ping check
        tasks = generate_tasks_for_entity_type(ctx, NodeInterfaceCheckNetworkTask)
        if_check_result = run_tasks(tasks)

        # of all the nodes, record the ones we can talk to be used as source nodes in the node ping check
        reachable_sources = []
        for name, result in if_check_result.items():
            if result.ok:
                reachable_sources.append(name)

        # record any interface errors associated with the validation set and remove failed nodes from validation set
        for name in node_names:
            if not name in if_check_result:
                node_err[name] = f"node {name} is not in the network config doc"
                validation_set.remove(name)
                continue

            task_ok = if_check_result[name].ok
            node = ctx.network_config.get_raw_object(name)
            down_interfaces = [inf['name'] for inf in node.get('interfaces', []) if inf.get('state') != 'up']
            if not task_ok or down_interfaces or not node.get('interfaces'):
                validation_set.remove(name)
                if not task_ok:
                    node_err[name] = f"failed to run interface check: {if_check_result[name].message}"
                elif down_interfaces:
                    node_err[name] = f"down interfaces: {','.join(down_interfaces)}"
                else:
                    node_err[name] = f"unexpected state, no interfaces"

        if not validation_set:
            return node_err

        # run gateway ping check on validation set
        gen = NodePingTestGenerator(ctx.network_config, include_src=list(validation_set))
        tasks = generate_tasks_for_names(ctx, NodePingTestNetworkTask, names=list(validation_set), generator=gen, test_kinds=[PingTestKind.GATEWAY])
        gw_ping_result = run_tasks(tasks)
        for name, result in gw_ping_result.items():
            if not result.ok or len(result.data) == 0:
                node_err[name] = f"execution of gateway ping test failed: {result.message}"
                validation_set.remove(name)
            else:
                for ping_result in result.data:
                    if not ping_result.ok:
                        node_err[name] = str(
                            f"gateway ping test {ping_result.src_if} failed: {ping_result.details}. "
                            f"Check {ping_result.dst_name}:{ping_result.dst_if} config"
                        )
                        validation_set.remove(name)
                        break

        if not validation_set:
            return node_err

        # run node ping test on validation set from potentially every node in the cluster
        total_pings, failure_tolerance = 5, 1
        failures = defaultdict(list)
        node_ping_test_srcs = _optimize_node_ping_sources(ctx.network_config, reachable_sources, list(validation_set))
        gen = NodePingTestGenerator(
            ctx.network_config,
            include_src=node_ping_test_srcs,
            include_dst=list(validation_set),
            exclude_down_src_interfaces=True,
            tests_per_dst_node_if=total_pings,
        )
        tasks = generate_tasks_for_names(ctx, NodePingTestNetworkTask, names=node_ping_test_srcs, generator=gen, test_kinds=[PingTestKind.NODE])
        node_ping_result = run_tasks(tasks)
        for src_name, result in node_ping_result.items():
            if not result.ok and src_name in validation_set:
                node_err[src_name] = f"execution of node ping test failed: {result.message}"
                validation_set.remove(src_name)
            elif not result.ok:
                continue  # failed from src
            else:
                for ping_result in result.data:
                    if ping_result.dst_name in validation_set and not ping_result.ok:
                        failures[ping_result.dst_name].append(ping_result)
        for name in validation_set:
            if len(failures[name]) > failure_tolerance:
                node_err[name] = f"failed {len(failures[name])} node ping tests: {','.join(str(t) for t in failures[name])}"

    return node_err


def validate_systems(
        network_config: str,
        system_names: typing.List[str],
        app_ctx: Optional[AppCtx] = None,
) -> typing.Dict[str, str]:
    if not system_names:
        return {}

    sys_err = {name: "" for name in system_names}

    with NetworkCfgCtx(network_config, app_ctx=app_ctx) as ctx:

        # select a sample of set nodes to use for ping check
        src_candidates = []
        max_candidates = 50
        for name in ctx.network_config.get_names_by_type(OBJ_TYPE_SR):
            if ctx.network_config.get_entity(name).role != "user":  # exclude usernodes since they might have route restrictions
                src_candidates.append(name)
                if len(src_candidates) >= max_candidates:
                    break

        # refresh all node's interface status for purpose of ping check
        tasks = generate_tasks_for_names(ctx, NodeInterfaceCheckNetworkTask, names=src_candidates)
        if_check_result = run_tasks(tasks)

        # of all the nodes, record the ones we can talk to be used as source nodes in the node ping check
        reachable_sources = []
        for name, result in if_check_result.items():
            if result.ok:
                reachable_sources.append(name)

        # odd case where no nodes are reachable for validation - fail every system
        if not reachable_sources:
            return {name: "failed to reach any node for ping validation" for name in system_names}

        # run ping check to systems
        total_pings, failure_tolerance = 5, 1
        failures = defaultdict(lambda : defaultdict(list))  # system -> interface -> failures
        gen = NodePingTestGenerator(ctx.network_config, include_src=src_candidates, include_dst=system_names, exclude_down_src_interfaces=True, tests_per_dst_system_if=total_pings)
        generate_tasks_for_names(ctx, NodePingTestNetworkTask, names=src_candidates, generator=gen, test_kinds=[PingTestKind.SYSTEM])
        node_ping_result = run_tasks(tasks)
        for src_name, result in node_ping_result.items():
            if result.ok and result.data:
                for ping_result in result.data:
                    if not ping_result.ok:
                        failures[ping_result.dst_name][ping_result.dst_if].append(ping_result)

        for sys_name, inf_failures in failures.items():
            failed_infs = []
            for inf, failures in inf_failures.items():
                if len(failures) > failure_tolerance:
                    failed_infs.append(inf)
            if failed_infs:
                sys_err[sys_name] = f"ping check failed for interfaces {','.join(inf)}"
    return sys_err


def validate_switches(
        network_config: str,
        switch_names: typing.List[str],
        app_ctx: Optional[AppCtx] = None,
) -> typing.Dict[str, str]:
    """ Validate deployed L3 config
    Args
        network_config: path to network config json
        switch_names: switches to validate
        app_ctx: application deployment context
    """
    if not switch_names:
        return {}

    retval = SwitchPingCheck(network_config, names=switch_names, app_ctx=app_ctx).run()
    switch_failures = retval[1]
    rv = {name: "" for name in switch_names}
    for failure in switch_failures:
        rv[failure['switch']] = failure['error'] + " " + ', '.join(failure.get("errors", []))

    return rv

def cmd_apply_switch_config(args):
    """ Apply supplied config file to given switches
    """
    # create temporary config
    fname = "tmp_cfg"
    tc = NetworkConfig.get_tool_cmd(fname, "tmp_cluster")
    tc.run()
    if tc.retcode:
        logger.error(f"Failed to create temp config: {tc.retcode}, {tc.stderr}")
        return tc.retcode
    # setup directory structure for config files
    base_dir = "tmp_base_dir"
    os.makedirs(base_dir, exist_ok=True)
    os.makedirs(f"{base_dir}/switches", exist_ok=True)
    # add switches to config
    for n in args.name:
        tc = AWSwitches.get_tool_cmd(
                fname, n, args.vendor, None, args.username,
                args.password)
        tc.run()
        if tc.retcode:
            logger.error(
                f"Failed to add switch {n}: {tc.retcode}, {tc.stderr}"
            )
            return tc.retcode
        # create base dir and copy config file
        os.makedirs(f"{base_dir}/switches/{n}", exist_ok=True)
        os.system(f"cp {args.filename} {base_dir}/switches/{n}/")
    # upload switch config
    tc = SwitchUpload.get_tool_cmd(fname, f"{base_dir}")
    tc.run()
    if tc.retcode:
        logger.error(f"Failed to apply switch config: {tc.retcode}, {tc.stderr}")
    shutil.rmtree(f"{base_dir}")
    os.remove(f"{fname}")
    return tc.retcode


def parse_args(argv):
    """ Argument parser
    """
    parser = ArgumentParser(
        description=(
            "Push-button script for Cerebras cluster "
            "L3 network configuration"
        )
    )

    subparsers = parser.add_subparsers()

    # push_switches
    parser_push_sw = subparsers.add_parser("push_switches",
            help="Push generated L3 configuration to switches in the cluster")
    parser_push_sw.add_argument("-c", "--config-file", required=True,
            help="YAML config file which contains cluster information")
    parser_push_sw.add_argument("-n", "--network-config",
            help="File which contains the generated JSON network config")
    parser_push_sw.add_argument("-d", "--artifacts-dir",
            help="Directory which contains generated config files")
    parser_push_sw.add_argument("-l", "--log-file",
            help="Log output to this file instead of stdout")
    parser_push_sw.add_argument("--names", action="append",
            help="Names of the switches to push config for")
    parser_push_sw.add_argument("--dry-run", action="store_true", default=False,
            help="Print network_config.tool commands without executing them")
    parser_push_sw.add_argument("--verbose", action="store_true", default=False,
            help="Print debug information")
    parser_push_sw.set_defaults(func=cmd_push_switches)

    # apply_switch_config
    parser_switch_config = subparsers.add_parser("apply_switch_config",
            help="Apply config in given file to provided switches")
    parser_switch_config.add_argument("-n", "--name", action="append",
            help="Host name(s) of the switches to which config is to be applied")
    parser_switch_config.add_argument("-f", "--filename", required=True,
            help="File with configuration commands to be applied")
    parser_switch_config.add_argument("-v", "--vendor", required=True,
            help="Switch vendor")
    parser_switch_config.add_argument("-u", "--username", required=True,
            help="Switch admin username")
    parser_switch_config.add_argument("-p", "--password",
            help="Switch admin password")
    parser_switch_config.add_argument("-l", "--log-file",
            help="Log output to this file instead of stdout")
    parser_switch_config.add_argument("--verbose", action="store_true", default=False,
            help="Print debug information")
    parser_switch_config.set_defaults(func=cmd_apply_switch_config)

    return parser.parse_args(argv)

def _setup_logging(args):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    if args.log_file:
        handler = logging.FileHandler(args.log_file)
        formatter = logging.Formatter(
                        '%(asctime)s | %(levelname)s | %(message)s'
                    )
        handler.setFormatter(formatter)
    else:
        handler = logging.StreamHandler(sys.stdout)
    if args.verbose:
        handler.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
    root.addHandler(handler)


# pylint: disable=missing-function-docstring
def main(argv):
    args = parse_args(argv)

    _setup_logging(args)

    if hasattr(args, "func"):
        ret = args.func(args)
    else:
        logger.error("Choose a valid command. -h for help.")
        ret = 1
    if isinstance(ret, tuple):
        ret = ret[0]
    return ret


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
