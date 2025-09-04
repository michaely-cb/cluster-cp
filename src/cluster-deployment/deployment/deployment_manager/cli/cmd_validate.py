import inspect
import logging

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.deploy.network import (
    DeployNetworkGenerate
)
from deployment_manager.tools.config import ClusterProfile, ConfGen
from deployment_manager.tools.const import (
    CLUSTER_DEPLOYMENT_BASE
)
from deployment_manager.tools.utils import (
    exec_cmd
)

logger = logging.getLogger(__name__)


class ValidateNetwork(SubCommandABC):
    """
    Network tests on the cluster.

    Exists as a separate command from `deploy --stage NETWORK_PUSH --section VALIDATE`
    to avoid updating the device deployment state.
    """
    name = "network"

    def construct(self):
        self.parser.add_argument(
            "--nodes", "-n",
            action="append",
            help="List of node hosts to test from."
        )
        self.parser.add_argument(
            "--switches", "-s",
            action="append",
            help="List of switches to test from."
        )
        self.parser.add_argument(
            "--regen-network-config",
            action="store_true",
            help="Regenerate the network_config.json"
        )

    def run(self, args):
        cg = ConfGen(self.profile)
        if args.regen_network_config:
            logger.info("# Generate master_config")
            if not cg.write_master_config():
                self.cli_instance.returncode = 1
                return

            # create network_config.json file
            logger.info("# Generate network_config")
            # This will force re-init of network config
            ClusterProfile.force_network_init(self.profile)
            deploy_generate = DeployNetworkGenerate(self.profile)
            for stage, fn in [
                ("precheck", deploy_generate.precheck),
                ("execute", deploy_generate.execute),
                ("validate", deploy_generate.validate)
            ]:
                if "update_status" in inspect.getargspec(fn)[0]:
                    ret = fn("", update_status=False)
                else:
                    ret = fn("")
                if ret:
                    logger.error(
                        f"\t{stage} failed"
                    )
                    break
                else:
                    logger.info(
                        f"\t{stage} completed successfully"
                    )
            if ret:
                return

        nodesstr = ""
        if args.nodes:
            joinnodes = '" -n "'.join(args.nodes)
            nodesstr = f"-n \"{joinnodes}\""

        switchesstr = ""
        if args.switches:
            joinswitches = '" -n "'.join(args.switches)
            switchesstr = f"-n \"{joinswitches}\""

        ping_cmds = [
            f"python -m network_config.tool node_tasks -c {cg.network_config_file} --destination_type nodes {nodesstr} ping_check",
            f"python -m network_config.tool node_tasks -c {cg.network_config_file} --destination_type systems {nodesstr} ping_check",
            f"python -m network_config.tool node_tasks -c {cg.network_config_file} {nodesstr} ping_check",
            f"python -m network_config.tool switch_tasks -c {cg.network_config_file} {switchesstr} ping_check"
        ]

        self.cli_instance.returncode = 0
        for cmdstr in ping_cmds:
            ret, out, err = exec_cmd(
                cmd=cmdstr,
                cwd=f"{CLUSTER_DEPLOYMENT_BASE}/network_config",
                stream=True
            )
            if ret or "failed" in out:
                self.cli_instance.returncode = 1


CMDS = [
    ValidateNetwork,
]
