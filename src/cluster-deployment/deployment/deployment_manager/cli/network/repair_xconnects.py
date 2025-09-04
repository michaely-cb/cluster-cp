import logging

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db.models import Device
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.network_config_tool import NetworkConfigTool
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)

class RepairXconnects(SubCommandABC):
    """ Ping-check xconnects, re-apply PB2 configs if needed. Provided filter should target spine/leaf switches for
    which to target with update.
    """
    name = "repair_xconnects"

    def construct(self):
        self.add_arg_filter()
        self.add_arg_noconfirm()

    def run(self, args):
        qs = Device.objects.filter(profile__name=self.profile, device_role__in=("SX", "SP", "LF", "MX"))
        qs = self.filter_devices(args, device_type="SW", query_set=qs)
        nw = NetworkConfigTool.for_profile(self.profile)
        # only ping from SP or SX
        switch_result = nw.switch_ping_check(qs.filter(device_role__in=("SX", "SP",)))
        # but filter results to dsts in the query containing all switch types - allows filtering for specific leafs
        all_switch_names = qs.values_list('name', flat=True).distinct()
        reapply = []
        for switch, results in switch_result.items():
            for result in results:
                if result.ok:
                    continue
                if result.is_fatal():
                    logger.error(f"{switch} had a fatal error: {result.details}")
                elif result.dst_name in all_switch_names:
                    reapply.append(result)

        if not reapply:
            logger.info("nothing to do")
            return 0

        print("\n".join([f"{x.src_name}:{x.src_if} -> {x.dst_name}:{x.dst_if}" for x in reapply]))
        print(f"Will reapply {len(reapply)} links")
        if not prompt_confirm("continue?"):
            return 0

        pairs = list(set([f"{x.src_name}:{x.dst_name}" for x in reapply]))
        cg = ConfGen(self.profile)
        nw.push_links(cg.network_config_templates_dir, pairs)
