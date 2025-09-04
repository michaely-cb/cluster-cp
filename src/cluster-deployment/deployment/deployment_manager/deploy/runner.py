import datetime
import logging
import time
from typing import List

import pytz
import tabulate

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.const import (
    DeploymentSections,
    DeploymentStageStatus,
)
from deployment_manager.db.models import (
    DeploymentCtx,
    Device, )
from deployment_manager.deploy.common import DEPLOYMENT_SKIP, DEPLOYMENT_SUCCESSFUL, FATAL_EC
from deployment_manager.deploy.network import DeployNetworkGenerate, DeployNetworkPush
from deployment_manager.deploy.node import DeployFreeIpa, DeployK8s, DeployNfsMounts, DeployNics, DeployNodes, DeployOs, \
    DeployLockdownNodes
from deployment_manager.deploy.root import DeployRootServer
from deployment_manager.deploy.switch import DeploySwitchZtp
from deployment_manager.deploy.system import DeploySystems
from deployment_manager.deploy.flow_controller import DeployFlowController
from deployment_manager.tools.config import (
    ConfGen,
)
from deployment_manager.tools.utils import prompt_confirm, to_duration

logger = logging.getLogger(__name__)

class DeploymentRunner:
    # The return code of the execution
    returncode = 0

    def __init__(self, profile=None):
        self._profile = profile
        self._cg = ConfGen(profile)
        self._ctx = DeploymentCtx.get_ctx(profile)

    @classmethod
    def flow(cls, flow_name: str = "all"):
        if flow_name == "PB1":
            return [
                DeployOs,
                DeployNodes,
                DeployNics,
                DeployFlowController,
                DeploySystems,
            ]
        elif flow_name == "PB2":
            return [
                DeployNetworkGenerate,
                DeployNetworkPush
            ]
        elif flow_name == "PB2.5":
            return [
                DeployFreeIpa
            ]
        elif flow_name == "PB3":
            return [
                DeployK8s
            ]
        else:  # all
            return [
                DeployRootServer,
                DeploySwitchZtp,
                DeployOs,
                DeployNodes,
                DeployNics,
                DeployFlowController,
                DeploySystems,
                DeployNetworkGenerate,
                DeployNetworkPush,
                DeployNfsMounts,
                DeployFreeIpa,
                DeployK8s,
                DeployLockdownNodes,
            ]

    @classmethod
    def stage_names(cls, flow_name: str = "all") -> List[str]:
        return [stage.name() for stage in DeploymentRunner.flow(flow_name=flow_name)]

    @classmethod
    def flow_names(cls):
        return ["all", "PB1", "PB2", "PB2.5", "PB3"]

    def mark_deployment_done(self):
        self._ctx.update_stage(
            self.stage_names()[-1],
            DeploymentSections.VALIDATE.value,
            DeploymentStageStatus.COMPLETED.value
        )
        self._ctx.mark_completed()

    def run(self, args):
        ret = DEPLOYMENT_SUCCESSFUL

        devices = SubCommandABC.filter_devices(args, query_set=Device.get_all(self._profile))
        self._ctx.mark_started()

        stages = [
            deploy_stage(self._profile) for deploy_stage in DeploymentRunner.flow(flow_name=args.flow)
        ]

        for s in stages:
            # if specific stages are chosen, run only those
            if args.stage and s.name() not in args.stage:
                continue

            quit_flag = False
            for sub_stage, fn in [
                (DeploymentSections.PRECHECK, s.precheck),
                (DeploymentSections.EXECUTE, s.execute),
                (DeploymentSections.VALIDATE, s.validate)
            ]:
                # if specific sections are chosen, run only those
                if args.section and sub_stage.name.lower() not in args.section:
                    continue

                stage_section = f"{s.name()}::{sub_stage.name}"

                if hasattr(s, f"{sub_stage.name.lower()}_get_targets"):
                    get_targets = getattr(s, f"{sub_stage.name.lower()}_get_targets")
                    if get_targets(devices, force=args.force):
                        if sub_stage == DeploymentSections.EXECUTE:
                            # Print the devices that will be deployed in the execute section
                            target_names = '   \n'.join(sorted(s._targets))
                            print(f"{s.name()} will be deployed on devices:\n{target_names}\n"
                                               f"{len(s._targets)} total devices")
                            if not args.noconfirm and not prompt_confirm(f"Continue deploying {s.name()}? y/n: "):
                                quit_flag = True
                                break
                    else:
                        msg = f"Skipping: no devices matched"
                        logger.info(msg)
                        s.write_to_log_ts(msg)
                        continue

                msg = f"Starting {stage_section}"
                logger.info(msg)
                s.write_to_log_ts(msg)
                self._ctx.update_stage(
                    s.name(), sub_stage.value,
                    DeploymentStageStatus.STARTED.value
                )

                start_time = datetime.datetime.now(tz=pytz.UTC)
                ret = fn(args)
                duration = to_duration(time.time() - start_time.timestamp())

                # log probable failed devices from this deploy step
                updated = Device.get_deployed_after(self._profile, start_time)
                updated_count = updated.count()
                # avoid COMPLETED, STARTED, etc. Usually INCOMPLETE or FAILED indicate execute or validate failed
                incomplete = updated.filter(deployment_status__in=(DeploymentStageStatus.INCOMPLETE.value, DeploymentStageStatus.FAILED.value))
                incomplete_count = incomplete.count()
                if incomplete_count > 0:
                    summary = [(d.name, DeploymentStageStatus(d.deployment_status).name,  d.deployment_msg,) for d in incomplete]
                    logger.info("\n" + tabulate.tabulate(summary, headers=["name", "status", "msg"]))
                    logger.info(f"{updated_count} device deployment statuses updated with "
                                f"{incomplete_count} incomplete, see 'cscfg deploy show' for details")
                else:
                    logger.info(f"{updated_count} device deployment statuses updated")

                if ret == DEPLOYMENT_SUCCESSFUL:
                    self._ctx.update_status(
                        DeploymentStageStatus.COMPLETED.value
                    )
                    logger.info(
                        f"{stage_section} completed successfully in {duration}"
                    )
                elif ret == DEPLOYMENT_SKIP:
                    self._ctx.update_status(
                        DeploymentStageStatus.COMPLETED.value
                    )
                    logger.info(
                        f"{stage_section} skipped"
                    )
                else:
                    self._ctx.update_status(
                        DeploymentStageStatus.FAILED.value
                    )
                    logger.error(
                        f"{stage_section} failed in {duration}"
                    )
                    break
            if quit_flag or ret == FATAL_EC:
                break

        if ret == FATAL_EC:
            self._ctx.mark_failed()
            self.returncode = 1
        else:
            self._ctx.mark_completed()
            ret = DEPLOYMENT_SUCCESSFUL

        return ret

    def get_next_stage(self, current_stage_name):
        stage_names = self.stage_names()
        if current_stage_name is None:
            return stage_names[0]
        elif current_stage_name not in stage_names:
            logger.error(f"Invalid deployment stage {current_stage_name}")
            return None
        else:
            i = stage_names.index(current_stage_name)
            # current stage is last
            if i == len(stage_names) - 1:
                return current_stage_name
            else:
                return stage_names[i + 1]
