import json
import logging
import pathlib
from typing import List, Optional, Tuple

import django.db.transaction

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.cli.switch.link.link import create_or_update_links
from deployment_manager.cli.switch.link.reprs import LinkRepr
from deployment_manager.db.models import Link, Device
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.utils import prompt_confirm

logger = logging.getLogger(__name__)


def extract_network_config_links(doc: dict) -> List[LinkRepr]:
    """ Parse connection information from network_config doc """
    rv = []
    for node_type in (
        "activation",
        "memoryx",
        "management",
        "worker",
        "swarmx",
    ):
        for node in doc.get(f"{node_type}_nodes", []):
            for i in node.get("interfaces", []):
                if "switch_name" not in i:
                    continue
                rv.append(LinkRepr(
                    src_name=i.get("switch_name", ""),
                    src_if=i.get("switch_port", ""),
                    dst_name=node["name"],
                    dst_if=i["name"],
                    speed=i.get("gbps", 100),
                    origin="lldp",
                ))
    for conn in doc.get("system_connections", []):
        rv.append(LinkRepr(
            src_name=conn["switch_name"],
            src_if=conn["switch_port"],
            dst_name=conn["system_name"],
            dst_if=conn["system_port"],
            speed=100,
            origin="lldp",
        ))

    # mx/sx topology will always be 100G while leaf_spine may be 400, 800, or greater
    spine_speed_guess = 400 if doc.get("environment", {}).get("topology", "leaf_spine") == "leaf_spine" else 100
    switch_port_speed = {
        f"{sw['name']}:{i['name']}": i["speed"] // 10 ** 9 if "speed" in i else None
        for sw in doc.get("switches", [])
        for i in sw.get("ports", [])
    }
    for conn in doc.get("xconnect", {}).get("connections", []):
        s, d = conn["links"][0], conn["links"][1]
        rv.append(LinkRepr(
            src_name=s["name"],
            src_if=s["port"],
            dst_name=d["name"],
            dst_if=d["port"],
            speed=switch_port_speed.get(f"{s['name']}:{s['port']}", spine_speed_guess),
            origin="lldp",
        ))
    return rv


def get_duplicate_spine_links(profile: str, actual: List[LinkRepr]) -> List[Link]:
    """
    Spines should have only 1 connection to LF switch. Given a list of links that are known to
    exist via LLDP, this function returns a list of links which connect a SP-LF but which are
    on different interfaces to those given SP-LF links, meaning they're likely invalid.
    """
    sw_cache = {}

    def get_switch(name) -> Optional[Device]:
        if name in sw_cache:
            return sw_cache[name]
        d = Device.get_device(name, profile_name=profile)
        if d and d.device_type == "SW":
            sw_cache[name] = d
        else:
            sw_cache[name] = None
        return sw_cache[name]

    rm = []
    for link in actual:
        sp, lf, sif = link.src_name, link.dst_name, link.src_if
        sp_sw, lf_sw = get_switch(sp), get_switch(lf)
        if lf_sw and lf_sw.device_role == "SP":
            lf, sp = link.src_name, link.dst_name
            sif = link.dst_if
        elif not sp_sw or sp_sw.device_role != "SP":
            continue
        db_links = Link.objects.filter(src_device__profile__name=profile,
                                       src_device__name=sp, dst_name=lf)
        for db_link in db_links:
            if db_link.src_if != sif:
                rm.append(db_link)
    return rm


@django.db.transaction.atomic()
def update_dst_references(profile: str) -> Tuple[List[str], List[str]]:
    """ Updates references to dst devices and returns names of updated dst devices and still-missing dsts """
    cache = {}
    links = Link.objects.filter(src_device__profile__name=profile, dst_device__isnull=True)
    for link in links:
        if link.dst_name in cache and cache[link.dst_name] is None:
            continue
        try:
            dst = cache.get(link.dst_name)
            if not dst:
                dst = Device.objects.get(profile__name=profile, name=link.dst_name)
                cache[link.dst_name] = dst
            link.dst_device = dst
            link.save()
        except Device.DoesNotExist:
            cache[link.dst_name] = None
    return (
        sorted([k for k, v in cache.items() if v is not None]),
        sorted([k for k, v in cache.items() if v is None]),
    )


class LinkSync(SubCommandABC):
    """ Synchronize link data with a given network config json file and updates any dst references """
    name = "sync"

    def construct(self):
        self.parser.add_argument('--config', '-c', help="network_config.json filepath", required=False, default="")
        self.parser.add_argument("--skip-config",
                                 help="skips updating the links from network file, just update dst references",
                                 action="store_true")
        self.add_arg_noconfirm()
        self.add_arg_output_format()

    def run(self, args):
        rv = 0
        if not args.skip_config:
            config_file = args.config
            if not config_file:
                cg = ConfGen(self.profile)
                config_file = cg.network_config_file
            doc = json.loads(pathlib.Path(config_file).read_text())
            nw_doc_links = extract_network_config_links(doc)
            logger.info(f"extracted {len(nw_doc_links)} links from {config_file}")

            if not args.noconfirm:
                print("Updated links:")
                print(LinkRepr.format_reprs(nw_doc_links, args.output))
                if not prompt_confirm("Commit updates?"):
                    return 0
            updated, errs = create_or_update_links(self.profile, nw_doc_links)
            if errs:
                for err in errs:
                    logger.warning(err)
                logger.error(f"{len(errs)} errors while updating links from network doc")
                rv = 1
            duplicates = get_duplicate_spine_links(self.profile, nw_doc_links)
            print(LinkRepr.format_reprs([LinkRepr.from_link(l) for l in duplicates]))
            print(f"found {len(duplicates)} probable non-existing spine links")
            if duplicates and (args.noconfirm or prompt_confirm("delete probable non-existing spine links?")):

                @django.db.transaction.atomic
                def remove_all():
                    for link in duplicates:
                        link.delete()
                remove_all()

        logger.info("updating destination references...")
        updated, missing = update_dst_references(self.profile)
        if updated:
            logger.info(f"{len(updated)} dst devices updated: {', '.join(updated)}")
        if missing:
            logger.info(f"{len(missing)} dst devices missing")
            logger.debug("missing: " + ', '.join(missing))
        return rv
