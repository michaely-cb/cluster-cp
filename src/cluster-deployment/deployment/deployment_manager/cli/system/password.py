import concurrent
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List

from deployment_manager.cli.subcommand import SubCommandABC
from deployment_manager.common.models import K_SYSTEM_ROOT_PASSWORD, well_known_secrets, K_SYSTEM_ADMIN_PASSWORD
from deployment_manager.db import device_props as props
from deployment_manager.db.models import Device
from deployment_manager.tools.config import ConfGen
from deployment_manager.tools.secrets import create_secrets_provider
from deployment_manager.tools.system import change_system_password, PasswordChange
from deployment_manager.tools.utils import prompt_confirm
from deployment_manager.common.constants import MAX_WORKERS

logger = logging.getLogger(__name__)

_SYSTEM_ROOT_PASSWORD_ENVVAR = "DM_SYSTEM_ROOT_PASSWORD"

def _update_system_pw(system: Device, hostname: str, login_as: str, try_login_passwords: List[str], target_user: str, target_password: str) -> PasswordChange:
    """ Also updates the database password - TODO: this should delegate to the secretsProvider eventually """
    rv = change_system_password(hostname, login_as, try_login_passwords, target_user, target_password)
    if rv.status != "error":
        username = system.get_prop(props.prop_management_credentials_user)
        if username == target_user:
            system.set_prop(props.prop_management_credentials_password, target_password)
    return rv


class Password(SubCommandABC):
    """ Set or reset system password. Set changes the password to the one defined in the secrets provider. Reset sets
    to factory default.
    """
    name = "password"

    def construct(self):
        self.add_arg_filter()
        self.add_arg_output_format()
        self.add_error_only_flag()
        self.add_arg_noconfirm()
        self.parser.add_argument(
            "--root-password",
            help=f"Use this root user password to set or reset user passwords. Does not set the root user password, just"
                 f" uses it to login as root to use passwd utility. Alternatively set with {_SYSTEM_ROOT_PASSWORD_ENVVAR}",
        )
        self.parser.add_argument("--user", choices=("admin", "root", "both"), default="both", help="which user to update")
        self.parser.add_argument(
            "command",
            choices=("set", "reset"),
            help="Whether to SET the password to the one specified in the secrets provider (usually input.yml) or RESET to factory default",
        )

    def run(self, args):
        query_set = self.filter_devices(args, query_set=Device.get_all(self.profile, device_type="SY"))
        systems = [d.name for d in query_set]

        if not args.noconfirm:
            print(f"{args.command}:\n  " + "\n  ".join(systems))
            if not prompt_confirm(f"Update {len(systems)} systems?"):
                return 0

        cfg = ConfGen(self.profile)
        provider = create_secrets_provider(cfg.parse_profile())
        wks = well_known_secrets()
        try_root_pws = {provider.get(K_SYSTEM_ROOT_PASSWORD), wks.get(K_SYSTEM_ROOT_PASSWORD)}
        if args.root_password:
            try_root_pws.add(args.root_password)
        if os.getenv(_SYSTEM_ROOT_PASSWORD_ENVVAR):
            try_root_pws.add(os.getenv(_SYSTEM_ROOT_PASSWORD_ENVVAR))
        try_root_pws = list(try_root_pws)

        source = wks if args.command == "reset" else provider
        target_admin_pw = source.get(K_SYSTEM_ADMIN_PASSWORD)
        target_root_pw = source.get(K_SYSTEM_ROOT_PASSWORD)

        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # run in batches to avoid running passwd concurrently on a system
            def _process_futures(futures: dict):
                done, _ = concurrent.futures.wait(list(futures.keys()), return_when=concurrent.futures.ALL_COMPLETED)
                for future in done:
                    sys_name = futures[future].name
                    try:
                        results.append(future.result())
                    except Exception as e:
                        results.append(PasswordChange(sys_name, "*", "error", f"Threw exception {e}"))

            if args.user in ("both", "admin",):
                futures = {executor.submit(_update_system_pw, s, s.name, "root", try_root_pws, "admin", target_admin_pw): s for s in query_set}
                _process_futures(futures)

            if args.user in ("both", "root",):
                futures = {executor.submit(_update_system_pw, s, s.name, "root", try_root_pws, "root", target_root_pw): s for s in query_set}
                _process_futures(futures)

        if args.error_only:
            results = [r for r in results if r.status == "error"]

        print(PasswordChange.format_reprs(results))

        return 1 if any(r for r in results if r.status == "error") else 0