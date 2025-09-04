#!/usr/bin/env python3

import concurrent.futures
import datetime
import fcntl
import gzip
import json
import logging
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from typing import Optional

import grpc

DEFAULT_MAX_TRAIN_FABRIC_FREQ_MHZ = "900"

logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s:%(levelname)s %(message)s'
)

# Example: job-operator
namespace = os.environ.get("NAMESPACE")

# Example: wsjob-saewl2cc78bpaunacyqzuk
wsjob_id = os.environ.get("WSJOB_ID")

# Example: coordinator
replica_type = os.environ.get("REPLICA_TYPE")

# Example: 0
replica_id = int(os.environ.get("REPLICA_ID", "0"))

# Example: /n1/wsjob/workdir/job-operator/wsjob-saewl2cc78bpaunacyqzuk/coordinator-0
replica_workdir = os.environ.get("REPLICA_WORKDIR")

# Example: crd
role_name = ""

# Example: COMPILE, EXECUTE, SDK_COMPILE, SDK_EXECUTE, INFERENCE_COMPILE, INFERENCE_EXECUTE
wsjob_mode = os.environ.get("WS_JOB_MODE")

# Example: 2.1.0-202401051028-49-6c4a52d4
image_tag = os.environ.get("MY_IMAGE_TAG")

MAX_TRAIN_FABRIC_FREQ_MHZ = int(os.environ.get("MAX_TRAIN_FABRIC_FREQ_MHZ", DEFAULT_MAX_TRAIN_FABRIC_FREQ_MHZ))

# In order to facilitate LRU-based cleanup on compile artifacts and copied user venvs,
# the coordinator touches the compile artifact dir and the leader worker touches the
# copied user venv every 5 minutes. If those directories do not have a recent last
# modified datetime, it is a good indicator that the cleanup routine can safely clean
# them up.
access_marker_heartbeat_thread = None
stop_access_marker_event = None
compile_artifact_dir = os.environ.get("COMPILE_ARTIFACT_DIR")
user_venv_path = os.environ.get("USER_VENV_PATH")

pchild_pid = -1
test_mode = False

# Make sure these fields align with FW side code:
# https://github.com/Cerebras/monolith/blob/master/src/framework/appliance/roles/chief/chief_ws_service.cc
# in the ChiefWsService::program_device function
fields_to_extract = ["architecture", "core_frequency", "egress_pes", "fabric_dimensions", "ingress_pes", "meta"]


def start(cmd):
    global pchild_pid
    p = subprocess.Popen(cmd, shell=True)
    pchild_pid = p.pid
    logging.info(f"{wsjob_id}-{replica_type}-{replica_id} main subprocess pid: {pchild_pid}")
    logging.info(f"{wsjob_id}-{replica_type}-{replica_id} main subprocess command: {cmd}")
    logging.info(f"{wsjob_id}-{replica_type}-{replica_id} main subprocess launch time: {get_utc_now()}")
    return p


def sigterm_handler(signum, _):
    # sigterm 15 is normally called by k8s cleanup (with exception of node pressure caused eviction)
    # which means job already ended and current pod/container needs to be cleaned up to release resources
    signal_name = signal.strsignal(signum)
    logging.info(f"Caught signal {signal_name} to gracefully shutdown current pod/container. "
                 f"Now initiating termination of current container's runtime process with pid {pchild_pid}. "
                 f"This is the expected cleanup behavior after client side has terminated job, "
                 f"indicating current pod/container is usually not the cause of job termination. "
                 f"For actual cause, please check client log or run `csctl get job -n{namespace} {wsjob_id} -oyaml`")
    if pchild_pid != -1:
        os.kill(pchild_pid, signum)


def log_exit_msg(code):
    if code < 0:
        # negative code indicates termination by a signal
        signum = -code
        signal_name = signal.Signals(signum).name
        signal_description = signal.strsignal(signum)
        # sigterm 15 is normally called by k8s cleanup
        if signum != 15:
            logging.error(
                f"{wsjob_id}-{replica_type}-{replica_id} "
                f"was killed by signal {signum} ({signal_name}:{signal_description}) "
                f"at {get_utc_now()}")
        else:
            logging.info(
                f"{wsjob_id}-{replica_type}-{replica_id} "
                f"terminated successfully by {signal_name}:{signal_description} "
                f"at {get_utc_now()}")
    elif code > 0:
        logging.error(
            f"{wsjob_id}-{replica_type}-{replica_id} exited with non-zero return code {code} "
            f"at {get_utc_now()}")
    else:
        logging.info(
            f"{wsjob_id}-{replica_type}-{replica_id} complete successfully with code 0 "
            f"at {get_utc_now()}")


def get_cmd():
    all_details_fp, br_config_fp, debug_args_json = build_config()
    # Cbcore entrypoints will use these environment variables to implicitly
    # populate mandated configurations.
    os.environ["CEREBRAS_CLUSTER_ALL_DETAILS_FP"] = all_details_fp
    os.environ["CEREBRAS_CLUSTER_BR_CONFIG_FP"] = br_config_fp
    os.environ["CEREBRAS_CLUSTER_TASK_ID"] = str(replica_id)
    os.environ["CEREBRAS_CLUSTER_SHORT_TASK_TYPE"] = role_name

    # These environment variables will be defined on the appliance client side in long term.
    if role_name in ["cmd", "wgt"] and get_num_csx() >= 2:
        os.environ["HIO_MAX_EGRESS_DESCR_ISSUED"] = "262144"

    all_details_json = {}
    if os.path.exists(all_details_fp):
        with open(all_details_fp) as f:
            all_details_json = json.load(f)

    cmd = ""
    for task in all_details_json.get("clusterDetails", {}).get("tasks", []):
        if task.get("taskType").lower() == role_name:
            cmd = task.get("taskSidecarCommand") if is_user_sidecar() else task.get("taskCommand")
            break

    if cmd:
        return cmd, all_details_fp

    # Starting from cluster semantic version 1.0.14, cbcore and user sidecar container commands are provided
    # in the cluster details config map. The following logic is for backwards compatibility only and can be removed
    # after cluster release 3.2.0.
    if role_name == "br":
        # BR Verbose mode produces a lot of output, so we only enable it if the
        # user explicitly requests TRACE log level.
        br_verbose_mode = (
                              debug_args_json
                              .get("debugBr", {})
                              .get("logSettings", {})
                              .get("globalLevel", None)
                          ) == "TRACE"

        if br_verbose_mode:
            br_verbose_option = "--verbose"
            br_env = "cerebras_log_level_br=DEBUG "
        else:
            br_verbose_option = ""
            br_env = ""

        # In the context of a KAPI run, BR needs to first decorate the BR config before proceeding.
        cmd = f"{br_env}br {br_verbose_option} --id={replica_id} --config_file={br_config_fp} --skip_conn_test"
    elif role_name in ["cmd", "wgt", "act"]:
        # -d to disable the default behavior of listening on localhost default interface
        cmd = f"ws-srv {all_details_fp} -d"
        debug_args_role = debug_args_json.get(f"debug{role_name.capitalize()}")
        if debug_args_role:
            cmd_prefix = debug_args_role.get("cmdPrefix")
            if cmd_prefix:
                cmd = f"{cmd_prefix} {cmd}"
        if is_user_sidecar():
            from cerebras.pytorch.utils._app import cs_weight_app

            # Starting from rel-2.6, cs_weight_app starts supporting task type as a new argument
            # We only specify the task type argument when cs_weight_app can support that
            task_type_supported = False
            try:
                result = subprocess.run([
                    "python", cs_weight_app.__file__, "--help"
                ], capture_output=True, text=True, timeout=10)
                task_type_supported = result.returncode == 0 and "TASK_TYPE" in result.stdout
            except:
                pass

            cmd = f"python {cs_weight_app.__file__} -a {all_details_fp}"
            if task_type_supported:
                cmd += f" -t {role_name}"
    elif role_name == "crd":
        if wsjob_mode == "SDK_COMPILE":
            cmd = f"python /cbcore/py_root/cerebras/sdk/appliance/sdk_appliance_server.py -a {all_details_fp}"
        else:
            cmd = f"cs_coordinator_app {all_details_fp}"
    elif role_name == "chf":
        cmd = f"cs_chief_app {all_details_fp}"
    elif role_name == "wrk":
        if wsjob_mode == "SDK_EXECUTE":
            cmd = f"python /cbcore/py_root/cerebras/sdk/appliance/sdk_appliance_server.py -a {all_details_fp}"
        elif is_user_sidecar():
            from cerebras.pytorch.utils.data.streamer import cs_streamer_app
            cmd = f"python {cs_streamer_app.__file__} -a {all_details_fp}"
        else:
            cmd = f"cs_worker_app --send_every_file_path {all_details_fp}"
    elif role_name == "kvss":
        cmd = f"kv-srv {all_details_fp} -d"
        debug_args_role = debug_args_json.get(f"debug{role_name.capitalize()}")
        if debug_args_role:
            cmd_prefix = debug_args_role.get("cmdPrefix")
            if cmd_prefix:
                cmd = f"{cmd_prefix} {cmd}"
    elif test_mode:
        cmd = os.environ.get("TEST_CMD")
    else:
        logging.error(f"invalid task type: {role_name}, sys.exit")
        sys.exit(1)
    return cmd, all_details_fp


def setup_ini(all_details_fp):
    if not test_mode:
        from cerebras.framework.appliance.parsers.protobuf.debug_args_pybind import (
            DebugArgsParser,
        )
        from cerebras.stack.tools.contextpy import CsContext
        # DebugArgs should be the sole INI setter
        CsContext().config_clear()
        DebugArgsParser(all_details_fp)


def run():
    cmd, all_details_fp = get_cmd()
    if not is_user_sidecar():
        # User sidecar container does not contain deps in setup_ini()
        setup_ini(all_details_fp)
    launch_access_marker_heartbeat_thread()

    signal.signal(signal.SIGTERM, sigterm_handler)
    p = start(cmd)
    signal_svc_up()
    p.wait()
    log_exit_msg(p.returncode)

    if access_marker_heartbeat_thread:
        # Signal the directory heartbeat thread to stop
        stop_access_marker_event.set()
        access_marker_heartbeat_thread.join()
    sys.exit(p.returncode)


def build_config():
    if test_mode:
        return "", "br-test.json", {}
    # load cluster details config first so update check will ensure both configs updated
    cluster_details_json = load_cluster_details_from_config_map()
    br_config_json = load_br_config_from_config_map()
    debug_args_env_var = os.environ.get("WS_DEBUG_ARGS", {})
    debug_args_json = json.loads(debug_args_env_var)

    br_config_fp = save_br_config(br_config_json)
    if role_name in ["crd", "wrk"]:
        save_cluster_details(cluster_details_json)

    all_details = {
        "clusterDetails": cluster_details_json,
        "debugArgs": debug_args_json,
        "role": role_name,
        "id": replica_id,
        "brConfig": br_config_json,
    }

    all_details_fp = get_config_path()
    with open(all_details_fp, "w") as fp:
        fp.write(json.dumps(all_details, indent=2))
    return all_details_fp, br_config_fp, debug_args_json


def load_cluster_details_from_config_map():
    # in edge case of race condition that kubelet has not remounted the latest config before reading file
    start_time = time.time()
    # rel-2.2+ job-operator may delay mounting cluster details but use a file
    # to indicate when it's ready to be read.
    # rel-2.1 job-operator does not set this env variable, but it does mount
    # the cluster details file before starting this script.
    update_file = os.environ.get("WS_CLUSTER_DETAILS_UPDATE_SIGNAL")
    while update_file:
        with open(update_file, "r") as fp:
            if "updated" in fp.read():
                break
            # technically timeout not needed since remount action should only take one second
            # but adding here to ensure a deterministic behavior
            if time.time() - start_time > 90:
                logging.error("cluster detail config not updated after 90s, error out")
                sys.exit(1)
            logging.info("cluster detail config not updated, retry after 3s")
            time.sleep(3)
    return load_config("WS_CLUSTER_DETAILS_FP")


def load_br_config_from_config_map():
    return load_config("WS_BR_CONFIG_FP")


def load_config(env):
    # for backwards compatible check, remove at rel-2.5
    config = os.environ.get(env)
    if not config.endswith("_gz"):
        with open(config, "r") as fp:
            return json.load(fp)
    with gzip.open(config, 'rb') as fp:
        return json.load(fp)


def get_svc_up_file():
    svc_up = "user_sidecar_up" if is_user_sidecar() else "svc_up"
    if replica_workdir:
        svc_up = f"{replica_workdir}/{svc_up}"
    return svc_up


def signal_svc_up():
    with open(get_svc_up_file(), "w") as fp:
        endpoint = ""
        if role_name != "br" and not test_mode:
            replica = get_replica()
            if not is_user_sidecar():
                endpoint = replica['addressBook']['taskIpPort']
            elif "userSidecarAddress" in replica['addressBook']:
                endpoint = replica['addressBook']['userSidecarAddress']
            else:
                endpoint = replica['addressBook']['taskDebugAddress']
        fp.write(endpoint)
        logging.info("svc started")


def startup_probe():
    if not os.path.exists(get_svc_up_file()):
        sys.exit(1)
    with open(get_svc_up_file(), "r") as fp:
        ep = fp.read()
        # skip if not ep recorded, e.g. br is not covered yet
        if (not ep) or test_mode:
            return
        with grpc.insecure_channel(ep) as channel:
            try:
                grpc.channel_ready_future(channel).result()
            except Exception as e:
                print(f"caught exception {e}")
                sys.exit(1)


def liveness_probe():
    if not os.path.exists(get_svc_up_file()):
        sys.exit(1)
    with open(get_svc_up_file(), "r") as fp:
        ep = fp.read()
        ip = str.split(ep, ":")[0]
        # skip if not ip recorded, e.g. br is not covered yet
        if not ip:
            return
        cmd = f"ping -W 1 -c 1 {ip}"
        start(cmd)
        p = start(cmd)
        p.wait()
        if p.returncode != 0:
            sys.exit(p.returncode)


def get_config_path():
    cwd = os.getcwd()
    # User sidecar uses a different all details file to avoid racy read/write.
    _name = 'usc' if is_user_sidecar() else role_name
    return f"{cwd}/all-details-{_name}-{replica_id}.json"


def save_cluster_details(cluster_details_json=None):
    cwd = os.getcwd()
    if not cluster_details_json:
        cluster_details_json = load_cluster_details_from_config_map()
    saved_cluster_details_fp = f"{cwd}/cluster-details-config-map.json"
    with open(saved_cluster_details_fp, "w") as fp:
        fp.write(json.dumps(cluster_details_json, indent=2))
    return saved_cluster_details_fp


def save_br_config(br_config_json=None):
    cwd = os.getcwd()
    if not br_config_json:
        br_config_json = load_br_config_from_config_map()
    saved_br_config_fp = f"{cwd}/br-config-config-map.json"
    with open(saved_br_config_fp, "w") as fp:
        fp.write(json.dumps(br_config_json, indent=2))
    return saved_br_config_fp


def get_num_csx():
    br_config_json = load_br_config_from_config_map()
    return len(br_config_json["cs_nodes"])


def get_utc_now():
    utc_now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    return utc_now.isoformat()


def log_node_name():
    node_name = os.environ.get("MY_NODE_NAME")
    logging.info(f"{wsjob_id}-{replica_type}-{replica_id} node name: {node_name}")


def log_launch_time():
    logging.info(f"{wsjob_id}-{replica_type}-{replica_id} container launch time: {get_utc_now()}")


def log_build_version():
    global image_tag
    if test_mode:
        return
    logging.info(f"{wsjob_id}-{replica_type}-{replica_id} container image tag: {image_tag}")

    version_file = "/cbcore/etc/version.json"
    if os.path.exists(version_file):
        with open(version_file, "r") as fp:
            version_json = json.load(fp)
            cluster_semantic_version = version_json.get("cluster_semantic_version", "unknown")
            logging.info(f"{wsjob_id}-{replica_type}-{replica_id} container image semantic version: {cluster_semantic_version}")


def get_replica():
    cluster_details_json = load_cluster_details_from_config_map()
    for task in cluster_details_json["tasks"]:
        if task["taskType"].lower() != role_name:
            continue
        for replica in task["taskMap"]:
            if replica["taskId"]["taskId"] != replica_id:
                continue
            return replica


def launch_access_marker_heartbeat_thread():
    global stop_access_marker_event
    global access_marker_heartbeat_thread

    def update_access_marker():
        global stop_access_marker_event

        # Not all marker files are available at job start time
        # We dynamically build the marker files before we ping them.
        def build_marker_files():
            marker_files = []
            if role_name == "crd":
                # sessions_dir will take some time to be populated after launching cs_coordinator_app.
                # We will fall back to check compile_artifact_dir if sessions_dir does not exist and
                # enable the cached compile heartbeat in a more timely manner.
                sessions_dir = f"{os.getcwd()}/sessions"
                if os.path.isdir(sessions_dir):
                    session_workdirs = [os.path.join(sessions_dir, d) for d in os.listdir(sessions_dir)
                                        if os.path.isdir(os.path.join(sessions_dir, d))
                                        and not os.path.islink(os.path.join(sessions_dir, d))]
                    session_workdirs = sorted(session_workdirs)
                    for i in range(len(session_workdirs)):
                        cached_compile_loc_file = f"{session_workdirs[i]}/.compile_artifact_location.out"
                        if not os.path.isfile(cached_compile_loc_file):
                            continue
                        with open(cached_compile_loc_file, "r") as f:
                            cached_compile_loc = f.readline().strip()
                            if cached_compile_loc:
                                marker_files.append(f"{cached_compile_loc}.lock")
                elif compile_artifact_dir and os.path.isdir(compile_artifact_dir):
                    marker_files.append(f"{compile_artifact_dir}.lock")
            elif role_name == "wrk":
                # Start the thread to send heartbeat to the user venv access marker file
                global user_venv_path
                if replica_id == 0 and user_venv_path and os.path.exists(user_venv_path):
                    access_marker_file = f"{user_venv_path}/.access-marker.out"
                    marker_files.append(access_marker_file)
            # SW-104682: Add access marker file in the workdir to keep the workdir from getting cleaned up
            # Do not start the access marker heartbeat from the user sidecar
            if not is_user_sidecar():
                marker_files.append(f"{os.getcwd()}/.access-marker.out")
            return marker_files

        initialized = set()
        while not stop_access_marker_event.is_set():
            marker_files = build_marker_files()
            for marker_file in marker_files:
                with open(marker_file, 'a') as f:
                    # Acquire an exclusive lock
                    fcntl.flock(f, fcntl.LOCK_EX)
                    if marker_file not in initialized:
                        f.write(f"{namespace}/{wsjob_id}\n")
                        initialized.add(marker_file)
                        logging.info(f"{marker_file} added to heartbeat")
                    else:
                        os.utime(marker_file, None)
                    # Release the lock
                    fcntl.flock(f, fcntl.LOCK_UN)
            # Wait for 5 minutes
            stop_access_marker_event.wait(timeout=5 * 60)

    stop_access_marker_event = threading.Event()
    access_marker_heartbeat_thread = threading.Thread(
        target=update_access_marker
    )
    access_marker_heartbeat_thread.daemon = True
    access_marker_heartbeat_thread.start()


# This function is used to determine if the current container is a container
# that represents the user environment. In rel-2.5, this is used both in worker
# and weight pods.
def is_user_sidecar():
    return os.environ.get("IS_USER_SIDECAR") == "TRUE"


# test only
def set_test_mode(role=""):
    global test_mode
    global role_name
    test_mode = True
    role_name = role


class System:
    name: str
    cmaddr: str
    fabric_json_str: Optional[str]
    core_freq: Optional[float]

    def __init__(self, name: str, cmaddr: str, fabric_json_str: Optional[str] = None,
                 core_freq: Optional[float] = None):
        self.name = name
        self.cmaddr = cmaddr
        self.fabric_json_str = fabric_json_str
        self.core_freq = core_freq

    def __repr__(self):
        return f"({self.name},{self.cmaddr},{self.fabric_json_str})"


def get_fabric_json(work_no: int, systems: queue.Queue, fabric_contents: queue.Queue):
    """
    Pull the system info from systems and retrieve the fabric.json using
    cmaddr.

    Retrieve the contents as json, extract fields:
    ["architecture", "core_frequency", "egress_pes", "fabric_dimensions", "ingress_pes", "meta"],
    and construct a json_str for comparison purposes.

    The `work_no` is used to save one copy of fabric.json in the current workdir
    for the compile to use.
    """
    no = 0
    while True:
        try:
            system = systems.get_nowait()
        except queue.Empty:
            logging.info("no more systems in the queue")
            break

        system_name = system.name
        cmaddr = system.cmaddr
        logging.info(f"Fetching fabric.json for system {system_name}, cmaddr {cmaddr}")
        if cmaddr.startswith("~"):
            # skip fetching fabric.json for any cm_addr starting with '~'
            logging.info(f"Skip fetching fabric.json for system {system_name}, cmaddr {cmaddr}")
            if cmaddr.startswith("~cmaddr"):
                FAKE_JSON_STR = cmaddr
            else:
                FAKE_JSON_STR = "FAKE_JSON_STR"
            fabric_contents.put(System(system_name, cmaddr, FAKE_JSON_STR, None))
            systems.task_done()
            continue

        try:

            try:
                from cerebras.hostio.python.cm import (
                    get_fabric_from_cmaddr_with_timeout,
                )
                fabric = get_fabric_from_cmaddr_with_timeout(cmaddr, 15)
            except ImportError:
                logging.warning("module cerebras.hostio.python.cm.get_fabric_from_cmaddr_with_timeout not found")
                try:
                    from cerebras.hostio.python.cm import get_fabric_from_cmaddr
                    fabric = get_fabric_from_cmaddr(cmaddr)
                except ImportError as err:
                    logging.warning("module cerebras.hostio.python.cm.get_fabric_from_cmaddr not found")
                    raise ImportError(err)

            from cerebras.stack.tools.load_fabric import update_v2_to_v3
            if fabric.meta.fabric_connections_version == 2:
                update_v2_to_v3(fabric)

            from cerebras.common.protobuf import proto_msg_to_jsontext
            fabric_json_data = proto_msg_to_jsontext(fabric)
            filtered_fabric_json_str = extract_fabric_fields(fabric_json_data)
            fabric_contents.put(System(system_name, cmaddr, filtered_fabric_json_str, fabric.core_frequency))
            logging.info(f"Done fetching fabric.json for system {system_name}, filtered content {filtered_fabric_json_str}, original content {fabric_json_data}")

            no += 1
            if no == 1 and work_no == 0:
                # persist one copy of fabric.json if it doesn't exist in the current workdir
                fabric_json_filename = f"{os.getcwd()}/fabric.json"
                if not os.path.isfile(fabric_json_filename):
                    with open(fabric_json_filename, "w+") as fabric_json_file:
                        fabric_json_file.write(fabric_json_data)

            systems.task_done()
        except Exception as exp:
            logging.error(
                f"Caught {exp} when fetching fabric.json for system {system_name}, cm_addr {cmaddr}")
            systems.task_done()


def check_fabrics():
    """
    Check whether fabric.jsons are the same for all healthy systems in the namespace.
    The healthy systems and their cmaddrs are provided through an environment variable:
    OK_SYSTEMS_IN_NAMESPACE. This function calls to each system with its cmaddr to
    retrieve fabric.json, and compares them. It fails the run if any of the fabric.json
    mismatches.
    """
    system_list = os.environ.get("OK_SYSTEMS_IN_NAMESPACE")
    if not system_list:
        logging.warning(f"No healthy systems in namespace {namespace} or fabric check is disabled")
        return

    systems = system_list.split(",")
    system_infos = queue.Queue()
    for system in systems:
        splits = system.split("+")
        if len(splits) != 2:
            logging.error(f"Unexpected entry in OK_SYSTEMS_IN_NAMESPACE: '{system}', system_list: '{system_list}'")
            sys.exit(1)
        name = splits[0]
        cmaddr = splits[1]
        system_infos.put(System(name, cmaddr))

    if system_infos.qsize() == 0:
        logging.warning(f"No healthy systems in namespace {namespace}: '{system_list}'")
        return

    num_workers = 10 if system_infos.qsize() > 10 else system_infos.qsize()
    futures = []
    fabric_contents = queue.Queue()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        for i in range(num_workers):
            future = executor.submit(get_fabric_json, i, system_infos, fabric_contents)
            futures.append(future)
    for future in futures:
        future.result()
    system_infos.join()

    unique_fabrics = {}
    fabric_freq = None
    for system in fabric_contents.queue:
        contents = unique_fabrics.get(system.fabric_json_str, None)
        if contents == None:
            unique_fabrics[system.fabric_json_str] = [system.name]
        else:
            contents.append(system.name)
        if fabric_freq is None:
            # Only need to check for the freq once because of the content comparison check.
            fabric_freq = system.core_freq

    if len(unique_fabrics) > 1:
        mismatches = find_mismatches(unique_fabrics)
        logging.error(f"Fabric mismatches in namespace {namespace}: {json.dumps(mismatches, indent=2)}")
        sys.exit(1)
    else:
        logging.info(f"Fabric configurations for all systems in namespace {namespace} match: {system_list}")

    if fabric_freq is None:
        # Potentially, this is a cluster with mock systems.
        logging.warning(f"Could not determine fabric frequency for all systems in namespace {namespace}")
    elif wsjob_mode == "COMPILE":
        # We only check the fabric frequency for training compile jobs, which can't be greater than a max value.
        # Running higher than this value for a training job can break the chip.
        # The inference jobs can run on any frequency, though lower frequency can have lower performance.
        # The check here is to guard against breaking the chip.
        fabric_freq_mhz = int(fabric_freq / 1e6)
        logging.info(str(
            f"Fabric frequency for all systems in namespace {namespace} is {fabric_freq_mhz}. "
            f"The allowed max for training is {MAX_TRAIN_FABRIC_FREQ_MHZ}"
        ))
        if fabric_freq_mhz > MAX_TRAIN_FABRIC_FREQ_MHZ:
            logging.error(str(
                f"Fabric frequency for all systems in namespace {namespace} {fabric_freq_mhz} is "
                f"higher than the allowed max value for training: {MAX_TRAIN_FABRIC_FREQ_MHZ}"
            ))
            sys.exit(1)

def extract_fabric_fields(fabric_json):
    data = json.loads(fabric_json)
    filtered_data = {key: data.get(key) for key in fields_to_extract if key in data}
    return json.dumps(filtered_data)

def find_mismatches(unique_fabrics):
    mismatches = {}

    for field in fields_to_extract:
        unique_values = {}

        # Iterate through each fabric configuration
        for fabric_json_str, system_names in unique_fabrics.items():
            # skip checking for mismatch fabric.json for any cm_addr starting with '~'
            if fabric_json_str.startswith("~"):
                unique_values[fabric_json_str] = system_names
                continue

            data = json.loads(fabric_json_str)
            field_value = json.dumps(data.get(field, None))

            if field_value not in unique_values:
                unique_values[field_value] = []

            unique_values[field_value].extend(system_names)

        if len(unique_values) > 1:
            if field in ["ingress_pes", "egress_pes"]:
                for pes_value, system_list in unique_values.items():
                    logging.error(f"Mismatched {field} value for systems {system_list}: {json.dumps(pes_value)}")
                mismatches[field] = f"MISMATCH DETECTED for {field}, check previous loglines for detailed content"
            else:
                mismatches[field] = unique_values

    return mismatches

def main():
    usage = "python run.py <role-name> [get_box_id/startup_probe/liveness_probe]"
    if len(sys.argv) == 1:
        logging.error(f"not enough params, expect usage: {usage}")
        sys.exit(1)

    global role_name
    role_name = sys.argv[1]

    if len(sys.argv) > 2:
        action = sys.argv[2]
        if action == "get_box_id":
            return get_box_id()
        elif action == "startup_probe":
            return startup_probe()
        elif action == "liveness_probe":
            return liveness_probe()
        else:
            logging.error(f"invalid param, expect usage: {usage}")
            sys.exit(1)

    log_node_name()
    log_launch_time()
    log_build_version()
    if wsjob_mode == "COMPILE" or wsjob_mode == "INFERENCE_COMPILE":
        check_fabrics()
    run()


if __name__ == "__main__":
    main()
