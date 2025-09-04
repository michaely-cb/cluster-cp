import concurrent
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device
from deployment_manager.tools.ssh import SSHConn
from deployment_manager.tools.utils import parse_mac, prompt_confirm

logger = logging.getLogger(__name__)


def _scape_mgmt_mac_arista_eos(name, cip, cuser, cpass, muser, mpass) -> str:
    timeout = 10
    with SSHConn(cip, cuser, cpass) as conn:
        with conn.shell_session() as shell:
            # console output is too messy and unpredictable when ZTP mode is enabled
            # just wait for a few seconds after each call and hope for the best
            shell.prompt_detect = lambda x: True
            out = ""

            time.sleep(5)  # console takes some time to init

            def exec(cmd, wait=2) -> str:
                nonlocal out
                o = shell.exec(cmd, timeout=timeout, capture_init=True)
                out += o
                time.sleep(wait)
                return o + shell.read()

            try:
                ctrl_c = '\x03'
                o = exec(ctrl_c)
                if "login:" in o:
                    o = exec(muser)
                    if "assword" in o:
                        exec(mpass)
                mgmt_inf = exec("show interface Management 1", wait=10)
                mac = ""
                for m in re.finditer(r"Hardware is Ethernet, address is ([.0-9a-f]+)", mgmt_inf):
                    mac = m.group(1)
                    break
                shell.exec("exit")
                if not mac:
                    raise RuntimeError(f"failed to parse mac from {name}, output: {mgmt_inf}")
                return parse_mac(mac)
            except:
                logger.warning(f"console session with {name} failed, output:\n{out}")
                raise


def _scape_mgmt_mac_dell_os10(name, cip, cuser, cpass, muser, mpass) -> str:
    default_user, default_pass = "admin", "admin"  # dell OS10 defaults prior to ZTP
    timeout = 10
    with SSHConn(cip, cuser, cpass) as conn:
        with conn.shell_session() as shell:
            # console output is too messy and unpredictable when ZTP mode is enabled
            # just wait for a few seconds after each call and hope for the best
            shell.prompt_detect = lambda x: True
            out = ""

            time.sleep(5)  # console takes some time to init

            def exec(cmd, wait=2) -> str:
                nonlocal out
                o = shell.exec(cmd, timeout=timeout, capture_init=True)
                out += o
                time.sleep(wait)
                return o + shell.read()

            try:
                ctrl_c = '\x03'
                o = exec(ctrl_c, wait=5)
                if "login:" in o:
                    o = exec(default_user)
                    if "assword:" in o:
                        o = exec(default_pass, wait=10)  # OS10 login takes a long time

                # try again in case this is running after mgmt user created
                if "login:" in o and "Last login:" not in o:
                    o = exec(muser)
                    if "assword:" in o:
                        o = exec(mpass, wait=10)  # OS10 login takes a long time
                    if "login" in o:
                        raise RuntimeError(f"failed to login to {name}: {o}")

                mgmt_inf = exec("show interface mgmt 1/1/1 | grep Hardware")
                mac = ""
                for m in re.finditer(r"Hardware is Eth, address is ([.:0-9a-f]+)", mgmt_inf):
                    mac = m.group(1)
                    break
                shell.exec("exit")
                if not mac:
                    raise RuntimeError(f"failed to parse mac from {name}, output: {mgmt_inf}")
                return parse_mac(mac)
            except:
                logger.warning(f"console session with {name} failed, output:\n{out}")
                raise


def fetch_device_mac(device: Device) -> str:
    cip = device.get_prop(props.prop_console_ip)
    cuser = device.get_prop(props.prop_console_username)
    cpass = device.get_prop(props.prop_console_password)
    if not (cip and cuser and cpass):
        raise ValueError(f"device {device.name} missing properties.console.* properties")

    muser = device.get_prop(props.prop_management_credentials_user)
    mpass = device.get_prop(props.prop_management_credentials_password)
    vendor = device.get_prop(props.prop_vendor_name)
    if vendor == "AR":
        return _scape_mgmt_mac_arista_eos(device.name, cip, cuser, cpass, muser, mpass)
    elif vendor == "DL" and device.device_role == "MG":
        # only scrape for dell MGMT switches which will be running OS10
        # dell LF/SP switches run sonic OS
        return _scape_mgmt_mac_dell_os10(device.name, cip, cuser, cpass, muser, mpass)
    else:
        raise NotImplementedError(f"{device.name} console MAC scraping not "
                                  f"supported for vendor/role {vendor}/{device.device_role}")


class UpdateMacAddress(SubCommandABC):
    """
    Update mac address for mgmt port of switches via their console connection.
    Useful during initial cluster provisioning in large clusters where we may
    have console access prior to ZTP

    TODO: this only works with Arista
    """

    name = "update_mac_address"

    def construct(self):
        self.add_arg_filter(required=False)
        self.add_arg_noconfirm()

    def run(self, args):
        query_set = Device.get_all(self.profile, device_type="SW")
        if args.filter:
            query_set = self.filter_devices(args, query_set=query_set)

        if query_set.count() == 0:
            logger.info("No device found to update its mac address")
            return 0

        if not args.noconfirm:
            print(f"updating {len(query_set)} devices:\n" + "\n".join([d.name for d in query_set]))
            if not prompt_confirm("continue?"):
                return 0

        devices = {d.name: d for d in query_set}
        failures = {}
        results = {}
        with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
            future_to_device = {
                executor.submit(fetch_device_mac, device): device
                for device in devices.values()
            }
            done, _ = concurrent.futures.wait(future_to_device.keys(), return_when=concurrent.futures.ALL_COMPLETED)
            for future in done:
                device = future_to_device[future]
                try:
                    results[device.name] = future.result()
                except Exception as e:
                    failures[device.name] = str(e)

        for name, mac in results.items():
            d: Device = devices[name]
            d.set_prop(props.prop_management_info_mac, mac)
            d.set_prop(props.prop_switch_info_mac_verified, "true")

        logger.info(f"updated {len(results)} of {len(devices)} targets")
        if failures:
            for k, v in failures.items():
                logger.error(f"{k}: {v}")
            return 1
        return 0
