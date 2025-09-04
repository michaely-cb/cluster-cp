import argparse
import cmd
import logging
import re
import textwrap
from abc import ABCMeta, abstractmethod
from argparse import (
    ArgumentParser,
    ArgumentDefaultsHelpFormatter,
    RawTextHelpFormatter,
    _SubParsersAction,
)
from typing import Callable, List, Optional

from django.db.models import QuerySet

from deployment_manager.db.device_props import INVENTORY_DEVICE_PROPS

_INVENTORY_ROW_HEADER = ["name", "type", "role"] + [
    t for t in INVENTORY_DEVICE_PROPS.keys()
]

from .filter import process_filter

logger = logging.getLogger(__name__)

class _Formatter(ArgumentDefaultsHelpFormatter, RawTextHelpFormatter):
    pass


def string_pattern(pattern: str) -> Callable[[str], str]:
    """ Regex match support for use in type= argument of argparse """
    def _string_pattern(arg_str: str) -> str:
        if not re.match(pattern, arg_str):
            raise argparse.ArgumentTypeError(f"'{arg_str}' does not match the pattern '{pattern}'")
        return arg_str
    return _string_pattern


def bool_type(v):
    """ Argparse boolean type support """
    if isinstance(v, bool):
        return v
    v = v.lower()
    if v in ('yes','true','t','1'): return True
    if v in ('no','false','f','0'): return False
    raise argparse.ArgumentTypeError('Boolean value expected.')


class SubCommandABC(metaclass=ABCMeta):
    """Base class for subcommands"""

    # Name of the subcommand defined by the child class
    name: str

    parser: ArgumentParser
    cli_instance: cmd.Cmd
    profile: str
    parallelism: int = 32

    def __init__(
        self,
        subparsers: _SubParsersAction,
        cli_instance: cmd.Cmd,
        profile: str = None,
        help: str = None,
        description: str = "",
    ):
        self.parser = subparsers.add_parser(
            self.name,
            help=textwrap.dedent(
                help if help is not None else self.__doc__
            ).strip(),
            description=description,
            formatter_class=_Formatter,
        )
        self.cli_instance = cli_instance
        self.profile = profile

    @abstractmethod
    def construct(self):
        """Construct command arg parser"""

    def build(self):
        """Add default to parser"""
        self.construct()
        self.parser.set_defaults(func=self._run)

    def _run(self, args):
        rv = self.run(args)
        if isinstance(rv, int):
            self.returncode = rv
        return rv

    @abstractmethod
    def run(self, args):
        """Execute command with parsed args"""

    ### common args ###
    def add_arg_noconfirm(
        self, help="Do not prompt before applying the action"
    ):
        self.parser.add_argument(
            "--noconfirm",
            "-y",
            default=False,
            action="store_true",
            help=help,
        )

    def add_arg_output_format(self):
        self.parser.add_argument("-o", "--output",
            help="Output format",
            choices=["table", "csv", "json", "yaml"],
            default="table"
        )

    def add_arg_dryrun(self, help="Do not persist results"):
        self.parser.add_argument("--dryrun", action="store_true", default=False, help=help)

    def add_arg_filter(self, required: bool = False, group=None):
        help_message = textwrap.dedent(
            f"""
            Filter which takes a space-separated list of <field><op><val> tuples where
                <field> is case-insensitive device property like 'name|type|role|<prop_key>.<prop_attr>'.
                            <prop_key>.<prop_attr>
                            See `cscfg device show -oyaml` for all valid properties
                <op>    is an operator =, != for equality/inequality and =~, !~ for regex match, regex not match
                <val>   is is the value or regular expression
            Tuple matchers are AND'd together in the search query.
            examples:
              --filter name!~'.*sx.*' returns devices with names that do not contain 'sx'
              --filter ipmi_credentials.user='root' returns devices whose ipmi user is named 'root'
            """
        )

        if group is None:
            self.parser.add_argument(
                "-f",
                "--filter",
                nargs="+",
                required=required,
                help=help_message,
            )
        else:
            group.add_argument(
                "-f",
                "--filter",
                nargs="+",
                required=required,
                help=help_message,
            )

    def add_error_only_flag(self, help="only display records with non-OK status"):
        self.parser.add_argument('--error_only', '-e',
                                 help=help,
                                 required=False, action="store_true")

    @classmethod
    def filter_devices(
        cls,
        args,
        query_set: QuerySet,
        device_type: Optional[str] = None,
        name: List[str] = (),
    ) -> QuerySet:

        if not args.filter:
            args.filter = []

        if device_type:
            query_set = query_set.filter(device_type=device_type)

        if name is not None and len(name) > 0:
            query_set = query_set.filter(name__in=name)

        for f in args.filter:
            query_set = process_filter(f, query_set)

        return query_set
