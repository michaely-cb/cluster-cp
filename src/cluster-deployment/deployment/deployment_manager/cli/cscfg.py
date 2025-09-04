import argparse
import datetime
import logging
from logging.handlers import RotatingFileHandler
import os

from deployment_manager.cli.shell import run_shell, cscfg_argparser

from deployment_manager.common import logger as cl

# Create the module logger
logger = logging.getLogger(__name__)

class StdoutLoggerFormatter(logging.Formatter):
    """ Format for stdout so that INFO will not print level
    """

    def format(self, record):
        if record.levelno == logging.INFO:
            self._style._fmt = ""
        else:
            self._style._fmt = "[%(levelname)s] "
        self._style._fmt += "%(message)s"
        return logging.Formatter.format(self, record)


def parse_args():
    parser = argparse.ArgumentParser(
        prog="cscfg",
        description="Deployment manager CLI"
    )
    parser.add_argument("-p", "--profile",
                        help="Name of the profile to handle"
                        )
    parser.add_argument("--version",
                        action="store_true",
                        help="Show the version of cscfg"
                        )
    parser.add_argument('-v', '--verbosity',
                    type=int,
                    choices=[0, 1, 2, 3],
                    help="Verbosity level",
                    )
    # for backwards compat, allows passing `cscfg --exec "some command"`. Should use `cscfg some command` now.
    parser.add_argument("-e", "--exec", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument('exec_cmd', nargs=argparse.REMAINDER,
                        help='Command to execute. If none given, will enter interpreter mode')

    return parser.parse_args()


def iso_timeformatter(self, record, datefmt=None):
    return datetime.datetime.fromtimestamp(record.created).isoformat(sep="T", timespec="milliseconds")


if __name__ == "__main__":

    parser = cscfg_argparser()
    # for backwards compat, allows passing `cscfg --exec "some command"`. Should use `cscfg some command` now.
    parser.add_argument("-e", "--exec", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument('exec_cmd', nargs=argparse.REMAINDER,
                        help='Command to execute. If none given, will enter interpreter mode')
    args = parser.parse_args()

    # Check the verbose flag
    if args.verbosity is not None:
        verb_level = args.verbosity
    elif (os.environ.get("DM_VERBOSITY") is not None) and (int(os.environ.get("DM_VERBOSITY")) in [0, 1, 2, 3]):
        verb_level = int(os.environ.get("DM_VERBOSITY"))
    else:
        verb_level = 1 # default verbosity level

    # Re-create the file handler with the correct attributes
    cl.root.removeHandler(cl.root.handlers[1])
    logfile_size_mb = 100
    logfile_num_bkp = 4
    logfile_name = os.getenv("CSCFG_LOGFILE", "cscfg.log")
    file_handler = RotatingFileHandler(
        logfile_name, # Base log file name
        maxBytes=1024 * 1024 * logfile_size_mb,    # logfile_size_mb MB size limit
        backupCount=logfile_num_bkp                # Keep logfile_num_bkp backup files
    )
    file_handler.set_name(file_handler.baseFilename) # Workaround to carry the logfile path along with the file handler
    cl.root.addHandler(file_handler)

    # Configure the formatters
    stderr_handler = cl.root.handlers[0]
    stderr_formatter = logging.Formatter("%(asctime)sZ [%(levelname)s] %(message)s")
    stderr_handler.setFormatter(stderr_formatter)
    file_formatter = logging.Formatter("%(asctime)sZ %(process)d [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)
    logging.Formatter.formatTime = iso_timeformatter

    # Set the levels of the handlers
    if verb_level == 0:
        stderr_handler.setLevel(logging.WARNING)
        file_handler.setLevel(logging.INFO)
    elif verb_level == 1:
        stderr_handler.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
    elif verb_level == 2:
        stderr_handler.setLevel(logging.INFO)
        file_handler.setLevel(logging.DEBUG)
    else:
        stderr_handler.setLevel(logging.DEBUG)
        file_handler.setLevel(logging.DEBUG)

    # Run the shell
    run_shell(args)
