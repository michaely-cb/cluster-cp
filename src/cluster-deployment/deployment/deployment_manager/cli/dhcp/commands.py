import difflib
import logging
import os
import pathlib
import tempfile
from typing import Tuple, List, Dict

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.dhcp import interface as dhcp
from deployment_manager.tools.dhcp.interface import LeaseEntry, DhcpProvider
from deployment_manager.deploy.utils import run_dhcp_updates
from deployment_manager.tools.pb1.ansible_task import (UpdateDhcpMirrorServer, UpdateDhcpServer)
from deployment_manager.tools.pb1.device_status import Status
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)


class Update(SubCommandABC):
    """ Update and restart DHCP servers based on current inventory """

    name = "update"

    def construct(self):
        self.add_arg_dryrun()
        self.parser.add_argument(
            "--noconfirm",
            "-y",
            default=False,
            action="store_true",
            help="Apply generated config to dnsmasq without diffing",
        )
        self.parser.add_argument(
            "--skip-mirrors",
            default=False,
            action="store_true",
            help="Skip applying generated config to mirror dnsmasq servers",
        )
        self.parser.add_argument(
            "--skip-root",
            default=False,
            action="store_true",
            help="Skip applying generated config to root dnsmasq server",
        )

    def run(self, args):
        cg = ConfGen(self.profile)
        cfg = cg.parse_profile()
        if cfg is None:
            raise RuntimeError("unable to parse input.yaml file")

        if args.skip_root and args.skip_mirrors:
            logger.info("Please remove --skip-mirrors or --skip-root to apply config")
            return 0

        root_server = cfg["basic"]["root_server"]
        dhcp_provider = dhcp.get_provider(self.profile, cfg)
        server_configs = dhcp_provider.generate_configs()
        root_configs = server_configs.get(root_server, [])

        if args.dryrun:
            output_dir = os.getenv("OUTPUT_DIR")  # for tests
            if not output_dir:
                outdir = pathlib.Path(tempfile.mkdtemp(prefix="dhcp"))
            else:
                outdir = pathlib.Path(output_dir)
                outdir.mkdir(exist_ok=True)
            for server, fp_contents in server_configs.items():
                (outdir / server).mkdir(exist_ok=True)
                for fp, content in fp_contents:
                    (outdir / server / fp.name).write_text(content)
            logger.info(f"wrote config files to {outdir}")
            return 0

        diffs = []
        for fp, new_content in root_configs:
            old_content = "" if not fp.is_file() else fp.read_text()
            diff = "\n".join(
                difflib.unified_diff(
                    old_content.split("\n"),
                    new_content.split("\n"),
                    str(fp) + "::OLD",
                    str(fp) + "::NEW"))
            if diff.strip():
                diffs.append(diff)

        has_mirrors = len(cfg.get("basic", {}).get("mirror_dhcp_servers", [])) > 0

        if diffs and args.skip_mirrors:
            logger.info("No changes detected, nothing to apply")
            return 0
        elif diffs:
            logger.info("\n".join(diffs))
        elif not diffs and has_mirrors:
            logger.info("No local changes detected, will sync local and mirror servers")

        if not args.noconfirm and not prompt_confirm("Continue?"):
            print("Aborted")
            return 0

        err = run_dhcp_updates(
            self.profile,
            dhcp_provider,
            server_configs,
            args.skip_root,
            mirror_servers=[] if args.skip_mirrors else cfg["basic"].get("mirror_dhcp_servers", []),
        )
        if err:
            logger.error(err)
            return 1
        return 0


class ShowLeases(SubCommandABC):
    """ Show current dhcp leases """

    name = "show_leases"

    def construct(self):
        self.add_arg_output_format()

    def run(self, args):
        cg = ConfGen(self.profile)
        cfg = cg.parse_profile()
        if cfg is None:
            raise RuntimeError("unable to parse input.yaml file")

        dhcp_provider = dhcp.get_provider(self.profile, cfg)
        leases = dhcp_provider.get_leases()
        print(LeaseEntry.format_reprs(leases, args.output))
