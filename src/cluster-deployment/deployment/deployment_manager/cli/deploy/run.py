import argparse
import logging
import textwrap

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import DeploymentSections
from deployment_manager.db.models import DeploymentProfile
from deployment_manager.deploy.runner import DeploymentRunner

logger = logging.getLogger(__name__)

class RunDeploy(SubCommandABC):
    """ Run deployment """
    name = "run"

    def construct(self):
        class parse_formatter(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
            """ Combine the raw text and defaults help formatter """
            pass
        self.parser.formatter_class = parse_formatter
        self.parser.add_argument("--flow",
                                 default="all",
                                 choices=DeploymentRunner.flow_names(),
                                 help=textwrap.dedent(f"""
                                    The flow to execute.
                                        'all' - all stages
                                        'PB1' - {','.join(DeploymentRunner.stage_names(flow_name='PB1'))}
                                        'PB2' - {','.join(DeploymentRunner.stage_names(flow_name='PB2'))}
                                        'PB2.5' - {','.join(DeploymentRunner.stage_names(flow_name='PB2.5'))}
                                        'PB3' - {','.join(DeploymentRunner.stage_names(flow_name='PB3'))}
                                 """).strip()
                                 )
        self.parser.add_argument("--stage", action="append",
                                 help="The specific stages of deployment to run",
                                 choices=DeploymentRunner.stage_names()
                                 )
        self.parser.add_argument("--section", action="append",
                                 help="Sections of the deployment stages to run",
                                 choices=[d.name.lower() for d in DeploymentSections]
                                 )
        self.parser.add_argument("--skip-os", action="store_true",
                                 help="Skip OS installation",
                                 default=False
                                 )
        self.parser.add_argument("--force", action="store_true",
                                 help="Force the deployment regardless of its current deployment status"
                                 )
        self.add_arg_filter(required=False)
        self.add_arg_noconfirm()

    def run(self, args):
        profile = DeploymentProfile.get_profile(self.profile)
        if not profile.deployable:
            logger.error("This profile is not deployable")
            return 1
        dr = DeploymentRunner(self.profile)
        dr.run(args)
        self.returncode = dr.returncode
