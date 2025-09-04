"""
Common utilities
"""
import abc
import concurrent.futures
import csv
import difflib
import glob
import io
import json
import logging
import os
import re
import readline
import secrets
import socket
import string
import subprocess
import tabulate
import tempfile
import time
import typing
import yaml
import socket
from pathlib import Path
from typing import Tuple

from deployment_manager.tools.ssh import SSHConn
from deployment_manager.common.constants import MAX_WORKERS

logger = logging.getLogger(__name__)

class ReprFmtMixin:

    @classmethod
    @abc.abstractmethod
    def table_header(cls) -> list:
        pass

    @abc.abstractmethod
    def to_table_row(self) -> list:
        pass

    @classmethod
    def csv_header(cls) -> list:
        return cls.table_header()

    def to_csv_row(self) -> list:
        return self.to_table_row()

    def to_dict(self) -> dict:
        d = dict(self.__dict__)
        if "sort_key" in d:
            del d["sort_key"]
        return d

    @classmethod
    def format_reprs(cls, reprs: typing.List['ReprFmtMixin'], fmt="table") -> str:
        if hasattr(cls, "sort_key"):
            reprs = sorted(reprs, key=lambda o: o.sort_key)

        if fmt == "yaml":
            return yaml.safe_dump([l.to_dict() for l in reprs], default_flow_style=False)
        elif fmt == "json":
            return json.dumps([l.to_dict() for l in reprs], indent=2)
        elif fmt == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(cls.csv_header())
            for l in reprs:
                writer.writerow(l.to_csv_row())
            return output.getvalue()
        else:
            return tabulate.tabulate([l.to_table_row() for l in reprs], headers=cls.table_header())


def to_duration(sec: typing.Union[int, float]) -> str:
    """ seconds duration to friendly human duration, e.g. 1h3m, 5d, ... """
    sec = int(sec)
    days, sec = divmod(sec, 86400)
    hours, sec = divmod(sec, 3600)
    minutes, sec = divmod(sec, 60)

    duration = []
    if days > 0:
        duration.append(f"{days}d")
    if hours > 0:
        duration.append(f"{hours}h")
    if minutes > 0 and len(duration) < 2:
        duration.append(f"{minutes}m")
    if sec > 0 and len(duration) < 2:
        duration.append(f"{sec}s")

    return ''.join(duration) if duration else "0s"


def from_duration(dur: str) -> int:
    """ human friendly duration to seconds e.g. 1h3m, 5d, -> seconds """
    if isinstance(dur, (int, float,)) or re.match(r"^\d+$", dur):
        return int(dur)
    m = re.match(r"(?P<days>\d+d)?(?P<hours>\d+h)?(?P<minutes>\d+m)?(?P<seconds>\d+s)?", dur)
    if not m:
        raise ValueError(f"invalid duration '{dur}', should be in format like 1d2h or 4h or 60s etc")
    vals = m.groupdict()
    d = vals["days"]
    h = vals["hours"]
    m = vals["minutes"]
    s = vals["seconds"]

    def digit(s):
        if s:
            return int(s[:-1])
        return 0

    return digit(d) * 86400 + digit(h) * 3600 + digit(m) * 60 + digit(s)


def strip_json_dict(s: str) -> dict:
    o, c = _struct_closure(s, "{", "}")
    return json.loads(s[o:c])


def strip_json_list(s: str) -> list:
    o, c = _struct_closure(s, "[", "]")
    return json.loads(s[o:c])


def _struct_closure(doc: str, openc: str, closec: str) -> Tuple[int, int]:
    """ Attempt to find the start/end of an object, selecting the first occurrence of an object. """
    start, i, counter = -1, 0, 0
    while i < len(doc):
        if doc[i] == openc:
            if start == -1:
                start = i
            counter += 1
        elif doc[i] == closec and counter > 0:
            counter -= 1
        i += 1
        if counter == 0 and start != -1:
            break
    else:
        logger.debug(f"invalid json doc - object closure {openc} {closec} not matched, {doc}")
        raise ValueError("invalid JSON document")
    return start, i


def diff_files(old_file: Path, new_file: Path) -> str:
    old = "" if not old_file.is_file() else old_file.read_text()
    new = new_file.read_text()
    diffs = "\n".join(difflib.unified_diff(
        old.split("\n"), new.split("\n"), str(old_file), str(new_file),
    ))
    return diffs


def exec_cmd(cmd, cwd=None, env=None, logcmd=True, stream=False, lgr="root", throw=False) -> Tuple[int, str, str]:
    """ Execute shell command

        logcmd -- log the cmd and cwd value
        stream -- stream the output.  The output is logged per line.

        lgr -- the logger to use in case of an error, default is "root"
    """
    log = logging.getLogger() if lgr == "root" else logging.getLogger(lgr)
    
    if logcmd:
        logger.info(f"Running {cmd}, cwd={cwd}, env={'True' if env else 'False'}")

    try:
        stdout_pipe = subprocess.PIPE
        stderr_pipe = subprocess.STDOUT if stream else subprocess.PIPE
        proc = subprocess.Popen(cmd, stdout=stdout_pipe, stderr=stderr_pipe, shell=True, cwd=cwd, env=env)

        if stream:
            stdout = []
            for text in iter(proc.stdout.readline, b""):
                if hasattr(text, "decode"):
                    text = text.decode("utf-8").rstrip()
                stdout.append(text)
                logger.info(f"    {text}")
            returncode = proc.poll()
            stdout_str = "\n".join(stdout)
            stderr_str = ""
        else:
            stdout, stderr = proc.communicate()
            stdout_str = stdout.decode('utf-8')
            stderr_str = stderr.decode('utf-8') if stderr else ""
            returncode = proc.returncode

        if throw and returncode != 0:
            raise subprocess.CalledProcessError(returncode, cmd, output=stdout_str, stderr=stderr_str)

        return returncode, stdout_str, stderr_str
    except Exception as e:
        if throw:
            raise
        log.error(f"unexpected exception in exec_cmd: {e}")
        return -1, "", ""


def exec_remote_cmd(cmd, host, username, password=None, timeout=5, throw=False) -> Tuple[int, str, str]:
    """ Execute command on remote shell using ssh
    """
    try:
        with SSHConn(host, username, password) as conn:
            return conn.exec(cmd, throw=throw, timeout=timeout)
    except Exception as e:
        # SSHConn throwing exception
        if throw:
            raise
        else:
            return -1, "", str(e)

def make_dir(dirname):
    ret, _, _ = exec_cmd(f"mkdir -p {dirname}")
    return ret

def rm_dir(dirname):
    ret, _, _ = exec_cmd(f"rm -rf {dirname}")

def edit_file(fname):
    editor = os.getenv("EDITOR", "vim")
    subprocess.call([editor, "+1", fname])

def show_file(fname):
    subprocess.call(["less", fname])


def flat_map(l: typing.Iterable) -> list:
    """ Flatten a list of iterables into a list of elements contained in the iterables """
    v = []
    for ele in l:
        if isinstance(ele, (typing.List, typing.Set, typing.Tuple)):
            if ele:
                v.extend(flat_map(ele))
        else:
            v.append(ele)
    return v


def countdown(t):
    while t:
        mins, secs = divmod(t, 60)
        timer = '{:02d}:{:02d}'.format(mins, secs)
        print(timer, end='\r')
        time.sleep(1)
        t -= 1


def get_cluster_yaml(cluster_yaml_file):
    """
    Read in the cluster yaml file and slightly reformat to make it easier
    to compare
    """

    if isinstance(cluster_yaml_file, str):
        cluster_yaml_file = Path(cluster_yaml_file)

    if not cluster_yaml_file.exists():
        return "File does not exists"

    with open(cluster_yaml_file, "r") as fp:
        cluster_yaml = yaml.safe_load(fp)

    # modify the list[dict] to be just dict
    for key in ["nodes", "systems", "groups"]:
        new = {}
        if cluster_yaml.get(key, None) and isinstance(cluster_yaml[key], list):
            for item in cluster_yaml[key]:
                if "name" not in item:
                    continue
                entry = {}
                for k,v in item.items():
                    if k == "name":
                        continue
                    if key == "nodes" and k == "networkInterfaces":
                        entry[k] = {}
                        if v and isinstance(v,list):
                            ni_entry = {}
                            for i in v:
                                if "name" not in i:
                                    continue
                                ni_entry = {kk:vv for kk,vv in i.items() if kk != "name"}
                                entry[k][i["name"]] = ni_entry
                    else:
                        entry[k] = v
                new[item["name"]] = entry
        cluster_yaml[key] = new
    return cluster_yaml


def prompt(
        msg: str = "(y)es|(n)o: ",
        values: list = ("y", "yes", "n", "no"),
        response_type: str = "list",
        allow_empty=False,
        default: str = None):
    """
        Prompt the user for a response

        response_type is the tab autocomplete for the user's input.  This can be list, str, or path
        allow_empty=True if empty response is allowed
    """
    # Save history to temporary file
    temp_history_file = tempfile.NamedTemporaryFile(delete=False)
    temp_history_file.close()
    readline.write_history_file(temp_history_file.name)
    readline.clear_history()
    try:
        if response_type in ["path", "list"]:
            if response_type == "path":
                def complete(text, state):
                    return (glob.glob(text + '*') + [None])[state]
            elif response_type == "list":
                def complete(text, state):
                    line = readline.get_line_buffer()
                    if not line:
                        return [c for c in values][state]
                    else:
                        return [c for c in values if c.startswith(line)][state]
            readline.set_completer_delims(' \t\n;')
            readline.parse_and_bind("tab: complete")
            readline.set_completer(complete)

        answer = ""
        while True:
            answer = input(msg)
            if answer:
                if response_type == "list" and answer not in values:
                    logger.info(f"Invalid answer. Valid choices={values}")
                    continue
                break
            else:
                if default is not None:
                    answer = default
                    break
                if allow_empty:
                    answer = ""
                    break
    finally:
        # Restore the history from temp file and cleanup
        readline.clear_history()
        readline.read_history_file(temp_history_file.name)
        os.unlink(temp_history_file.name)
    return answer


def prompt_confirm(msg: str = "y/n:") -> bool:
    """ Prompt the user for a y/n response """
    values = ["y", "yes", "n", "no"]
    msg = msg.rstrip(" ") + " "

    def complete(text, state):
        line = readline.get_line_buffer()
        if not line:
            return [c for c in values][state]
        else:
            return [c for c in values if c.startswith(line)][state]

    readline.set_completer_delims(' \t\n;')
    readline.parse_and_bind("tab: complete")
    readline.set_completer(complete)

    while True:
        answer = input(msg)
        if answer:
            # Remove answer from input history
            if readline.get_current_history_length() > 0:
                readline.remove_history_item(readline.get_current_history_length()-1)

            answer = answer.strip()
            if answer in values:
                return answer.startswith("y")
            print(f"Invalid answer. Valid choices={values}")


def get_version():
    """ Return the version string of the cluster deployment tool """
    version_file = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "etc",
        'version.json',
    )
    if not (os.path.exists(version_file) and os.path.isfile(version_file)):
        return "Unknown version"

    with open(version_file) as f:
        data = json.load(f)
        return f"{data['version']} ({data['buildid']})"


def ping_check(ip_or_dns) -> bool:
    cmd = f"ping -W4 -c1 {ip_or_dns} >/dev/null 2>&1"
    ret, out, err = exec_cmd(cmd, logcmd=False)
    return ret == 0


def tcp_connection_check(host, port=443, timeout=5):
    try:
        # create connection expects string or none
        with socket.create_connection((str(host), port), timeout):
            return True
    except socket.error:
        return False


def resolve_ip_address(hostname: str) -> typing.Optional[str]:
    try:
        ip_address = socket.gethostbyname(hostname)
        return ip_address
    except socket.gaierror:
        return None


def try_parse_mac(s: str) -> typing.Optional[str]:
    """ Format mac to lowercase, colon-separated form """
    fmt = re.sub(r"[^0-9a-f]+", "", s.lower())
    if len(fmt) != 12:
        return None
    parts = []
    for i in range(0, 12, 2):
        parts.append(fmt[i:i + 2])
    return ":".join(parts)


def parse_mac(s: str) -> str:
    mac = try_parse_mac(s)
    if not mac:
        raise ValueError(f"invalid mac: {s}")
    return mac


def snake_to_camel(snake_str):
    components = snake_str.split('_')
    if len(components) > 1:
        return components[0] + ''.join(word.capitalize() for word in components[1:])
    return snake_str


def generate_secure_string(length=12) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


class TaskRunner:
    """ Execute task over a list of args in parallel and accumulate their results
    """

    def __init__(self, task_fn):
        self._task_fn = task_fn

    def run(self, arg_iter):
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = list()
            for arg_tuple in arg_iter:
                futures.append(executor.submit(self._task_fn, *arg_tuple))

        return [f.result() for f in futures]
