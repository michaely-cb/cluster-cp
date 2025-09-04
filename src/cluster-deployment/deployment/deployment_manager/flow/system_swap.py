import dataclasses
import json
import logging
import os
import pathlib
import re
import tempfile
import time
from typing import List

import django.db.transaction

import deployment_manager.tools.cbscheduler as cbscheduler
import deployment_manager.tools.systemdb as systemdb
from deployment_manager.deploy.utils import run_dhcp_updates
from deployment_manager.common.lock import with_lock, LOCK_NETWORK
from deployment_manager.common.models import well_known_secrets, K_SYSTEM_ROOT_PASSWORD, K_SYSTEM_ADMIN_PASSWORD
from deployment_manager.db import device_props as props
from deployment_manager.db.const import DeploymentDeviceRoles
from deployment_manager.db.models import DeploymentDeviceTypes, DeploymentProfile, DeploymentStageStatus, Device, \
    DeviceDeploymentStages, MissingPropVals, get_devices_with_prop
from deployment_manager.flow.task import Task
from deployment_manager.network_config.common.context import NetworkCfgCtx, ObjCred
from deployment_manager.network_config.common.task import run_task
from deployment_manager.network_config.tasks.system import SystemPortDiscoverTask
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.dhcp import interface as dhcp
from deployment_manager.tools.middleware import AppCtxImpl
from deployment_manager.tools.network_config_tool import NetworkConfigTool
from deployment_manager.tools.secrets import SecretsProvider
from deployment_manager.tools.system import SystemCtl, change_system_password
from deployment_manager.tools.utils import exec_cmd, resolve_ip_address, ping_check

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class SwapRequest:
    profile: str
    old: str
    new: str
    alias_mode: bool = False

    def make_ctl(self) -> SystemCtl:
        if self.alias_mode:
            d = get_devices_with_prop(self.profile, props.prop_management_info_name, self.new)
            if len(d) == 1:
                d = d[0]
            elif d:
                assert False, f"more than one system with management_info.name={self.new} found"
        else:
            d = Device.get_device(self.new, self.profile)
        if not d:
            raise ValueError(f"device {self.new} not found")
        return SystemCtl(
            self.new,
            d.get_prop(props.prop_management_credentials_user),
            d.get_prop(props.prop_management_credentials_password),
        )


    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        aliased = "" if not self.alias_mode else "(aliased)"
        return f"{self.old}{aliased}:{self.new}"


class PreflightCheck(Task):
    """ Check that the old system is not reachable and the new system is reachable. Guards against mistakes """

    def __init__(self, config: dict, reqs: List[SwapRequest]):
        self._reqs = reqs

        self._ip_strategy = config.get("mgmt_network_int_config", {}).get("ip_allocation", {}).get(
            "allocation_strategy", "v1")

    def run(self):
        if self._ip_strategy == "v1":
            # not sure of a great way to check in v1 case since the IP will be re-used. Skip since v1 deprecated
            return True

        # In alias mode, if the dns server has already updated the cname record for the new system, then
        # this check will fail though it's actually OK to proceed. A better check would be to see if the dns record
        # points to an old system and if so, make sure that the old one is not pingable
        reachable_old = [pair for pair in self._reqs if ping_check(pair.old)]
        if reachable_old:
            logger.error(f"Requested systems {','.join(p.old for p in reachable_old)} to swap out but they are "
                         "pingable. This may be OK if the old system uses an alias name (e.g. wse001-cs-sy01) and the "
                         "dns server has already been updated. Otherwise, double check that the old system is really "
                         "supposed to be removed and if so run with --skip-tasks PreflightCheck. If not, check with "
                         "data center team.")

        unreachable_new = [pair for pair in self._reqs if not ping_check(pair.new)]
        if unreachable_new:
            logger.error(f"Requested systems {','.join(p.new for p in unreachable_new)} to swap in but failed "
                         "to respond to ping. Are you sure this is the right system name? "
                         "If so, check management network connectivity for this system.")

        return not (bool(reachable_old) or bool(unreachable_new))


class UpdateDMCfgsTask(Task):
    """ PB1 swap system in inventory, update DB with new system. """

    def __init__(self, req: SwapRequest):
        self._req = req

        self._confgen = ConfGen(req.profile)

    def _swap_cfg_files(self):
        """ Idempotent swap system in config """
        # may be references to the system in the vlans section
        input_yml_path = pathlib.Path(self._confgen.input_file)
        doc = input_yml_path.read_text()
        doc = re.sub(r'\b' + self._req.old + r'\b', self._req.new, doc)
        input_yml_path.write_text(doc)

    @django.db.transaction.atomic
    def _swap_db_entry(self):
        """ Idempotent swap of system in DB """
        old_dev = Device.get_device(self._req.old, self._req.profile)
        old_props = {}
        if old_dev:
            # Save the device properties
            old_props = old_dev.get_prop_attr_dict(include_mode=MissingPropVals.EXCLUDE)
            old_dev.delete()

        new_dev = Device.get_device(self._req.new, self._req.profile)
        assert old_dev or new_dev, f"invalid state: neither of {self._req} were in the database, unable to swap"
        if not new_dev:
            new_dev = Device.add(
                self._req.new,
                DeploymentDeviceTypes.SYSTEM.value,
                DeploymentDeviceRoles.SYSTEM.value,
                DeploymentProfile.get_profile(self._req.profile),
            )

        # Update the inventory properties from the old entry
        if old_props:
            mac = old_props.get(props.prop_management_info_mac.name, {}).get(props.prop_management_info_mac.attr)
            if mac:
                del old_props[props.prop_management_info_ip.name][props.prop_management_info_mac.attr]
            new_dev.batch_update_properties(old_props)

    @django.db.transaction.atomic
    def _update_db_entry(self):
        """ Idempotent swap of system in DB """
        dev = Device.get_device(self._req.old, self._req.profile)
        assert dev, f"old system {self._req.old} does not exist in device database but must"
        new = Device.get_device(self._req.new, self._req.profile)
        assert not new, f"new system {self._req.new} must not exist in device database but did not"
        alias_name = dev.get_prop(props.prop_management_info_name)
        if alias_name != self._req.new:
            dev.set_prop(props.prop_management_info_name, self._req.new)
            dev.set_prop(props.prop_management_info_mac, None)

    def run(self):
        if self._req.alias_mode:
            self._update_db_entry()
        else:
            self._swap_db_entry()
            self._swap_cfg_files()
        return True

    def __str__(self):
        return f"{self.name}[{self._req}]"


class UpdateLegacyDnsmasqConf(Task):
    """ Patch the dnsmasq conf file. Specifically for clusters using v1 IP assignment strategy where there's no range
    of addresses that can be dynamically allocated by dnsmasq and we must swap the specific system entry out.
    """

    def __init__(self, profile: str, reqs: List[SwapRequest]):
        self._profile = profile
        self._reqs = reqs
        self._config = ConfGen(self._profile).parse_profile()

    def _replace_hosts_entry(self, file_path: str, require_present=True) -> bool:
        p = pathlib.Path(file_path)
        if not p.is_file():
            return False
        contents = p.read_text()
        lines = []
        mod = False
        for req in self._reqs:
            ip = Device.get_device(req.new, self._profile).get_prop(props.prop_management_info_ip)
            present = False
            for line in contents.splitlines():
                if line.endswith(" " + req.old):
                    line = f"{ip} {req.new}"
                    present, mod = True, True
                elif line.endswith(" " + req.new):
                    if present:  # duplicate
                        continue
                    line = f"{ip} {req.new}"
                    present, mod = True, True
                lines.append(line)

            if not present and require_present:
                lines.append(f"{ip} {req.new}")
                mod = True

        if mod:
            back = pathlib.Path(f"/tmp/{p.name}.backup")
            back.write_text(contents)
            p.write_text("\n".join(lines))
            logger.info(f"{file_path} updated")
        return mod

    def force_expire_leases(self):
        """ hacky way to force expiration of system leases - new systems probably got leases associated with
        the old system names. A better way would be to have a range of addresses where systems could be assigned
        so there'd never be a conflict. This is done in the `v2` variant of IP assignment
        """
        old_names = set(s.old for s in self._reqs)
        try:
            dhcp_provider = dhcp.get_provider(self._profile, self._config)
            dhcp_provider.expire_lease(list(old_names))
            logger.info("removed old system names from lease file and restarting dnsmasq")
        except NotImplementedError:
            logger.warning("dhcp provider does not support expiring leases!")

    def run(self):
        ip_s = self._config.get("mgmt_network_int_config", {}).get("ip_allocation", {}).get("allocation_strategy", "v1")
        if ip_s != "v1":
            return True
        if any(r.alias_mode for r in self._reqs):
            logger.error(f"Cannot used aliased system names with v1 DHCP strategy")
            return False

        logger.info(f"v1 ip allocation strategy detected: updating host files and restarting dnsmasq")

        require_etc_hosts_entry = True
        if pathlib.Path("/etc/hosts.d/hosts.conf").is_file():
            self._replace_hosts_entry("/etc/hosts.d/hosts.conf")
            require_etc_hosts_entry = False
        self._replace_hosts_entry("/etc/hosts", require_present=require_etc_hosts_entry)

        # CG1 uses system names directly in dnsmasq conf, other clusters, this should have no effect
        for req in self._reqs:
            exec_cmd(f"sed -i 's/\\b{req.old}\\b/{req.new}/g' /etc/dnsmasq.conf", throw=True)

        if self._config["basic"]["name"] != "testmock":  # skip restart for docker test
            exec_cmd("systemctl restart dnsmasq", throw=True)

        self.force_expire_leases()

        sys_ips = {
            req.new: str(Device.get_device(req.new, self._profile).get_prop(props.prop_management_info_ip))
            for req in self._reqs
        }
        await_sys = sys_ips.copy()

        # ping the system IPs - dnsmasq may have issued a long-term lease to the old system name so we can't trust
        # the dnsmasq lease file
        timeout = 5 * 60
        logger.info(f"scanning for system IPs for a maximum of {timeout}s")
        expire_time = time.time() + timeout
        while time.time() < expire_time and await_sys:
            time.sleep(1)
            for sys, ip in sys_ips.items():
                if sys in await_sys and ping_check(ip):
                    del await_sys[sys]

        if not await_sys:
            return True

        logger.error(f"systems {', '.join(await_sys.keys())} failed ping after {timeout}s timeout. "
                     f"Check mgmt network connectivity. Mgmt cable may not be plugged in.")
        return False

    def __str__(self):
        return f"{self.name}"


class DiscoverMgmtNetworkTask(Task):
    """ Find the mgmt IP/MAC for the system by ssh'ing to it by its hostname. """

    def __init__(self, req: SwapRequest):
        self._req = req

    def run(self):
        resolved_ip = resolve_ip_address(self._req.new)  # check locally managed DNS name
        if not resolved_ip:
            resolved_ip = resolve_ip_address(f"{self._req.new}.cerebrassc.local")  # check IT managed DNS name
        if not resolved_ip:
            logger.error(f"Unable to resolve IP from hostname {self._req.new}. Check mgmt network")
            return False

        # discover mac for propose of updating dhcp reservation
        ctl = self._req.make_ctl()
        ctl.hostname = resolved_ip
        network = json.loads(ctl.network_show(json=True))
        resolved_mac = network["mgmt"]["macAddress"]

        # update DB entry
        name = self._req.old if self._req.alias_mode else self._req.new
        d = Device.get_device(name, self._req.profile)
        d.set_prop(props.prop_management_info_ip, resolved_ip)
        d.set_prop(props.prop_management_info_mac, resolved_mac)
        return True

    def __str__(self):
        return f"{self.name}[{self._req}]"


class UpdateDnsmasqTask(Task):
    """ Runs dnsmasq update for clusters using v2 allocation mode """

    def __init__(self, profile: str, config: dict):
        self._profile = profile
        self._cfg = config

    def run(self):
        alloc_strat = self._cfg.get("mgmt_network_int_config", {}).get("ip_allocation", {}).get("allocation_strategy")
        if alloc_strat == "v2":
            logger.info("Re-deploying dnsmasq for v2 ip allocation strategy")
            dhcp_provider = dhcp.get_provider(self._profile, self._cfg)
            err = run_dhcp_updates(self._profile, dhcp_provider)
            if err:
                logger.error(f"failed to update dnsmasq: {err}")
                return False
        else:
            # eventually when CG1 is updated to use v1 properly, we'll need to re-generate dnsmasq as well
            logger.info("Skip updating dnsmasq for v1 ip allocation strategy")
        return True

    def __str__(self):
        return f"{self.name}"


class UpdatePasswordTask(Task):
    """ Updates the system password according to the cluster's secrets provider configuration """

    def __init__(
            self,
            req: SwapRequest,
            secrets_provider: SecretsProvider,
    ):
        self._req = req
        self._device_name = self._req.old if self._req.alias_mode else self._req.new
        self._secrets_provider: SecretsProvider = secrets_provider
        self._wks = well_known_secrets()
        try_root_pws = {self._secrets_provider.get(K_SYSTEM_ROOT_PASSWORD), self._wks.get(K_SYSTEM_ROOT_PASSWORD)}
        self._try_root_pws = list(try_root_pws)

    def maybe_update_password(self, target_user: str, target_pw_key: str) -> bool:
        target_pw = self._secrets_provider.get(target_pw_key)
        if self._wks.get(K_SYSTEM_ADMIN_PASSWORD) != target_pw:
            rv = change_system_password(self._req.new, "root", self._try_root_pws, target_user, target_pw)
            if rv.status == "error":
                logger.error(f"{self._req.new} failed to change {target_user} password: {rv.messsage}")
                return False

        # ensure the device record is updated
        sys = Device.get_device(self._device_name, self._req.profile)
        if sys is not None:
            mgmt_user = sys.get_prop(props.prop_management_credentials_user)
            if mgmt_user == target_user:
                sys.set_prop(props.prop_management_credentials_password, target_pw)

        return True

    def run(self) -> bool:
        if self._secrets_provider.get(K_SYSTEM_ADMIN_PASSWORD) == self._wks.get(K_SYSTEM_ADMIN_PASSWORD):
            # this logic assumes the passwords are set correctly if the well known secrets match the requested ones
            # Ideally we would check but this is purposeful to avoid handling when root SSH is disabled, see CS-1272
            logger.info(f"skipping password update for {self._req.new} since admin password is unchanged from default")
            return True

        if not self.maybe_update_password("admin", K_SYSTEM_ADMIN_PASSWORD):
            return False
        if not self.maybe_update_password("root", K_SYSTEM_ROOT_PASSWORD):
            return False
        return True

    def __str__(self):
        return f"{self.name}[{self._req}]"


class ConfigureSystemTask(Task):
    """
    PB2/network config tasks. Updates network_config.json, re-gens config, pushes config to system.
    Also applies any site-specific system configuration settings specified in input.yaml
    """

    def __init__(self, confgen: ConfGen, config: dict, req: SwapRequest):
        self._confgen = confgen
        self._cfg = config
        self._req = req

        self._network = NetworkConfigTool.for_profile(self._req.profile)

    @with_lock(LOCK_NETWORK)
    def run(self):
        ctl = self._req.make_ctl()

        if self._req.alias_mode:
            name = self._req.old
            default_hostname = self._req.new
        else:
            name = self._req.new
            default_hostname = ""
        ip = Device.get_device(name, self._req.profile).get_prop(props.prop_management_info_ip)
        self._network.must_run_cmd(
            f"swap_system -c {self._confgen.network_config_file} "
            f"--old-name {self._req.old} --new-name {name} --mgmt-ip {ip} --default-hostname={default_hostname}"
        )

        sys_conns = self._cfg.get("cluster_network_config", {}).get("system_connections_file")
        if sys_conns:
            # This cluster cannot discover system connections through LLDP - update connections manually
            sys_conns = pathlib.Path(sys_conns)
            conns = json.loads(sys_conns.read_text())
            for c in conns:
                if c.get("system_name") == self._req.old:
                    c["system_name"] = name
            sys_conns.write_text(json.dumps(conns, indent=2))
            self._network.must_run_cmd(f"set_system_connections --filename {sys_conns.absolute()} "
                                       f"-c {self._confgen.network_config_file}")


        cfg_dir = f"{self._confgen.config_dir}/config"
        with NetworkCfgCtx(self._confgen.network_config_file,
                           app_ctx=AppCtxImpl(self._confgen.profile),
                           config_base_dir=cfg_dir,) as ctx:

            # update network config mac addresses
            result = run_task(SystemPortDiscoverTask(ctx, name))
            if not result.ok:
                logger.exception(f"failed to run port discover: {result.message}", exc_info=result.data)
                return False

        self._network.must_run_cmd(
            f"build_configs -t systems -c {self._confgen.network_config_file} -o {cfg_dir}",
        )

        # upload system config through redfish
        ok, msg = ctl.update_network_config(f"{cfg_dir}/systems/{name}/var/lib/tftpboot/network-cfg.json")
        if not ok:
            logger.error(f"failed to post network config: {msg}")
            return False
        elif ctl.is_system_activated():
            # according to the logic of the flow, the system should be put into standby but someone could retry
            # therefore, sleep 60 seconds to account for the time it takes for the network to reload in the activated case
            time.sleep(60)

        # if the cluster has global CS settings that need to be applied, then apply them
        syscfgs = self._cfg.get("basic", {}).get("global_system_configs", {})
        if syscfgs:
            ctl.update_syscfg(syscfgs)
        return True

    def __str__(self):
        return f"{self.name}[{self._req}]"


class UpdateSystemImageTask(Task):
    """ Update the image if version differs. Skip activation if the system was upgraded. """

    def __init__(self, req: SwapRequest, image: str):
        self._req = req
        self._image = image

    def run(self):
        ctl = self._req.make_ctl()
        ctl.update_service(self._image, force=False, skip_activate=True)

    def __str__(self):
        return f"{self.name}[{self._req.new}]"


class SystemStandbyTask(Task):
    """Standby the system if in activated state """

    def __init__(self, req: SwapRequest):
        self._req = req

    def run(self):
        ctl = self._req.make_ctl()
        if not ctl.is_system_standby():
            ctl.standby()

    def __str__(self):
        return f"{self.name}[{self._req.new}]"


class SystemActivateTask(Task):
    """ Activates the system if in standby """

    def __init__(self, req: SwapRequest):
        self._req = req

    def run(self):
        # also check if the system is in a non-standard config and warn the user if so
        ctl = self._req.make_ctl()
        show_out = ctl.get_system_show()
        if show_out.get("isSystemCfgStandard", "YES") != "YES":
            logger.warning(f"System {self._req.new} reported "
                           f"isSystemCfgStandard={show_out['isSystemCfgStandard']}. "
                           f"This may cause issues while activating the system. "
                           "Ask system users slack channel if activate fails")

        if ctl.is_system_standby():
            logger.info(f"system {self._req.new} is in standby. Activating")
            ctl.activate()

    def __str__(self):
        return f"{self.name}[{self._req.new}]"


class ValidateSystemIPsTask(Task):
    """ Check that the expected IPs were actually applied to the system """

    def __init__(self, cg: ConfGen, reqs: List[SwapRequest]):
        self._reqs = reqs
        self._confgen = cg

    def run(self):
        net_doc = json.loads(pathlib.Path(self._confgen.network_config_file).read_text())
        sys_docs = {s["name"]: s for s in net_doc["systems"]}
        errors = []
        for req in self._reqs:
            name = req.old if req.alias_mode else req.new
            sys_doc = sys_docs.get(name)
            assert sys_doc, f"unexpected state: {name} not in {self._confgen.network_config_file}"
            target_ips = {
                inf["address"].split("/")[0]
                for inf in sys_doc["interfaces"]
                if inf["name"].startswith("data")
            }

            network_show_doc = req.make_ctl().get_network_show()
            ip_up, inf_down = [], []
            for inf in network_show_doc.get("data", []):
                if not inf["ifaceName"].startswith("N"):
                    continue
                if inf["linkState"] == "UP":
                    ip_up.append(inf["ipAddress"].split("/")[0])
                else:
                    inf_down.append(inf["ifaceName"])

            if inf_down:
                errors.append(f"System {name} interfaces down={', '.join(inf_down)}")
            elif set(target_ips) != set(ip_up):
                errors.append(
                    f"System {name} did not have expected IPs target ips={', '.join(sorted(list(target_ips)))},"
                    f" actual ips up={', '.join(sorted(ip_up))}"
                )
        if errors:
            logger.error(f"Errors during IP validation: {', '.join(errors)}")
            return False
        return True

    def __str__(self):
        return f"{self.name}[{','.join([str(r) for r in self._reqs])}]"


class ValidateSystemLinksTask(Task):
    """
    PB2/Re-runs LLDP on any switch connected to the system, diffing the old/new
    network_config.json. If any differences are discovered, then the system was
    cabled incorrectly and IT must re-cable.
    """

    def __init__(self, cg: ConfGen, config: dict, reqs: List[SwapRequest]):
        self._reqs = reqs

        self._confgen = cg
        self._network = NetworkConfigTool.for_profile(reqs[0].profile)
        self._cfg = config

    @with_lock(LOCK_NETWORK)
    def run(self):
        names = [r.old if r.alias_mode else r.new for r in self._reqs]
        sys_conns = self._cfg.get("cluster_network_config", {}).get("system_connections_file")
        if not sys_conns:
            # Re-run LLDP and then run the sanity checker looking for inconsistent port assignments
            nw_path = pathlib.Path(self._confgen.network_config_file)

            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp_file:
                tmp_file.write(nw_path.read_text())
                tmp_file_name = tmp_file.name

            self._network.must_run_cmd(f"switch_tasks -c {tmp_file_name} lldp")
            # update systems' switch conn info
            self._network.must_run_cmd(f"system_iface_from_tier -c {tmp_file_name} -n {' -n '.join(names)}")
            # Replace the previous network json with the updated one since the prior steps succeeded
            pathlib.Path(tmp_file_name).rename(nw_path)
        else:
            self._network.must_run_cmd(
                f"system_iface_from_tier -c {self._confgen.network_config_file} -n {' -n '.join(names)}")

        sanity_output = self._network.run_sanity_check()
        inconsistent_conns = sanity_output.get("inconsistent_system_connections", [])
        errs = []
        for err in inconsistent_conns:
            if err.get("system") in names:
                errs.append(err['error'])

        if errs:
            msg = str(
                "Inconsistent system connections to switches detected! Please file a ticket for the datacenter team "
                "to reconfigure the cabling on the system.\n  " + "\n  ".join(errs)
            )
            logger.error(msg)
            return False
        return True

    def __str__(self):
        return f"{self.name}[{','.join([str(r) for r in self._reqs])}]"


class UpdateK8Task(Task):
    """
    PB3/Cluster-mgmt update step. Replaces the old system with the new system in the
    cluster configuration.
    """

    def __init__(self, profile, reqs: List[SwapRequest]):
        self._profile = profile
        self._reqs = reqs

    def run(self):
        # TODO: this should be done through csadm.sh update-cluster-config but there's too many small differences between the
        # generated file and the one in the cluster itself to use that method safely

        _, out, _ = exec_cmd(
            "kubectl get cm cluster -n job-operator -ojsonpath='{.data.clusterConfiguration\.yaml}'",
            throw=True
        )

        for req in self._reqs:
            name = req.old if req.alias_mode else req.new
            if not req.alias_mode:
                out = re.sub(f"\\b{req.old}\\b", req.new, out)
            # replace the mgmt addresses
            ip = Device.get_device(name, self._profile).get_prop(props.prop_management_info_ip)
            m = re.findall(rf"name: {name}\n[^\-]*managementAddress: (.+)\n", out)
            if len(m) == 1:
                old_ip = m[0]
                out = re.sub(f"\\b{old_ip}\\b", str(ip), out)

        tmp_path = os.environ.get("K8_TMP_PATH")  # for testing
        if tmp_path:
            clusteryaml = pathlib.Path(f"{tmp_path}/clusteryaml")
        else:
            clusteryaml = pathlib.Path(tempfile.mktemp("clusteryaml"))
        clusteryaml.write_text(out)
        clustercm = pathlib.Path(tempfile.mktemp("clusteryamlcm"))
        logger.debug(f"config path: {clusteryaml}, cm path: {clustercm}, new config: {out}")

        exec_cmd(
            f"kubectl create cm cluster -n job-operator "
            f"--from-file=clusterConfiguration.yaml={clusteryaml} -oyaml --dry-run=client >{clustercm}",
            throw=True
        )
        exec_cmd(f"kubectl replace cm -n job-operator cluster -f {clustercm}", throw=True)

        # restart the prometheus system exporter pods to clear their known hosts file which might contain
        # old system key mapping
        restart_cmd = "kubectl -n prometheus delete pod -lapp=cluster-mgmt-system-exporter --wait=false"
        rv, stdout, stderr = exec_cmd(restart_cmd)
        if rv != 0:
            logger.warning(f"failed to restart prometheus cluster-mgmt-system-exporter pods ({restart_cmd}): \n"
                           f"{stdout}\n{stderr}\nYou may need to restart cluster-mgmt-system-exporter manually if "
                           "grafana shows ADMIN_SVC_NOT_READY warnings for the new system")
        return True

    def __str__(self):
        return f"{self.name}[{','.join([str(r) for r in self._reqs])}]"


class CompleteTask(Task):
    """ Marks system as NETWORK_PUSH complete """

    def __init__(self, profile_name: str, req: SwapRequest):
        self._profile_name = profile_name
        self._req = req

    def run(self):
        name = self._req.old if self._req.alias_mode else self._req.new
        sys = Device.get_device(name, self._profile_name)
        assert sys, f"device {name} missing from device database"
        sys.update_stage(
            DeviceDeploymentStages.NETWORK_PUSH.value,
            DeploymentStageStatus.COMPLETED.value,
            msg=f"system swapped successfully",
            force=True,
        )

    def __str__(self):
        return f"{self.name}[{self._req}]"


class UpdateCBInfra(Task):
    """ Updates cerebras DBs """

    def __init__(self, profile_name: str, reqs: List[SwapRequest]):
        self._profile_name = profile_name
        self._reqs = reqs

    @staticmethod
    def test() -> bool:
        return ping_check(systemdb.HOST)

    def run(self):
        for req in self._reqs:
            name = req.old if req.alias_mode else req.new
            status, body, reason = cbscheduler.add_resource(name)
            if not 200 <= status <= 299 and status not in (400, 409):
                logger.warning(f"failed to update cbscheduler: {status} {body}, {reason}")

        cluster_name = DeploymentProfile.get_profile(self._profile_name).cluster_name
        status, body = systemdb.get_entry(cluster_name)
        if status == 200 and body.get("count", 0) > 0:
            # update - only update the system entries
            entry = systemdb.generate_systemdb_multibox_entry(self._profile_name, system_only=True)
            status, resp = systemdb.update_systemdb(entry, merge_update=True)
        else:
            # new - update everything
            entry = systemdb.generate_systemdb_multibox_entry(self._profile_name, location="", purpose="")
            status, resp = systemdb.update_systemdb(entry)

        if not 200 <= status <= 299:
            logger.warning(f"failed to update systemdb: {status} {resp}")
        return True

    def __str__(self):
        return f"{self.name}[{','.join([str(r) for r in self._reqs])}]"
