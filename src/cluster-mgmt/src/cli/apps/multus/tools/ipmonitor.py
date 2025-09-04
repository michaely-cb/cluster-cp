import argparse
import json
import logging
import pathlib
import re
import shlex
import subprocess
import time
from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from ipaddress import ip_address, ip_network, IPv4Address
from typing import Dict, List, Tuple
from threading import Event
import signal

import paramiko

logger = logging.getLogger(__name__)

SCRIPT_NAME = "ipmonitor.sh"
MON_SCRIPT = pathlib.Path(__file__).parent / SCRIPT_NAME
SSH_USER = "root"
TARGET_SUBNET = ip_network("10.0.0.0/8")

CREATE_TYPE = "CREATE"
DELETE_TYPE = "DELETE"


@dataclass
class IPEvent:
    type: str  # "CREATE" or "DELETE"
    host: str
    time: float  # Unix timestamp
    ip_ns: int
    ip: ip_address

    def __str__(self):
        return f"[{self.host:<14} :: {self.ip_ns:03}] {datetime.fromtimestamp(self.time)} {self.type} {self.ip}"


def _start_one(host: str, ipaddr: str,) -> Tuple[str, str]:
    """Execute SSH command and return output"""
    client = paramiko.SSHClient()
    try:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            ipaddr,
            username=SSH_USER,
            look_for_keys=True,
        )
        with client.open_sftp() as sftp:
            sftp.put(str(MON_SCRIPT.absolute()), f"/tmp/{SCRIPT_NAME}.sh")

        for cmd in [
            f"bash -c \"ps aux | grep {SCRIPT_NAME} | grep -v grep | tr -s ' ' ' ' | cut -f2 -d' ' | xargs -n1 kill\"",
            "bash -c \"ps aux | grep 'ip monitor all-nsid' | grep -v grep | tr -s ' ' ' ' | cut -f2 -d' ' | xargs -n1 kill\"",
            f"chmod 755 /tmp/{SCRIPT_NAME}",
            f"nohup /tmp/{SCRIPT_NAME} >/dev/null 2>&1 & echo $!"
        ]:
            stdin, stdout, stderr = client.exec_command(cmd)
            output = stdout.read().decode() + stderr.read().decode()
        return host, output.strip()
    finally:
        try:
            client.close()
        except:
            pass


def _stop_one(host: str, ipaddr: str, pid: str) -> str:
    """Execute SSH command and return output"""
    remote_log_file = "/tmp/ipmon.log"
    local_log_file = f"/tmp/{SCRIPT_NAME}.{host}.log"
    client = paramiko.SSHClient()
    try:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            ipaddr,
            username=SSH_USER,
            look_for_keys=True,
        )

        client.exec_command(f"kill -TERM {pid}")
        with client.open_sftp() as sftp:
            sftp.get(remote_log_file, local_log_file)
        client.exec_command(f"bash -c \"ps aux | grep {SCRIPT_NAME} | grep -v grep | tr -s ' ' ' ' | cut -f2 -d' ' | xargs -n1 kill\"")
        client.exec_command("bash -c \"ps aux | grep 'ip monitor all-nsid' | grep -v grep | tr -s ' ' ' ' | cut -f2 -d' ' | xargs -n1 kill\"")
        client.exec_command(f"rm -f /tmp/{SCRIPT_NAME}")
        client.exec_command(f"rm -f {remote_log_file}")
    finally:
        try:
            client.close()
        except:
            pass
    return local_log_file


def _parse_log(host: str, log_path: str) -> List[IPEvent]:
    """Parse log file into IPEvent objects"""
    state = {}
    events = []
    ip_re = re.compile(r'(\d+\.\d+\.\d+\.\d+)')
    ns_res = [
        re.compile(r'nsid (\d+)'),
        re.compile(r'nsid current]\d+:'),
        re.compile(r'nsid current]Deleted'),
    ]

    def add(e: IPEvent):
        if e.type == CREATE_TYPE:
            if not state.get(e.ip):
                events.append(e)
                state[e.ip] = True
        else:
            if state.get(e.ip):
                events.append(e)
                state[e.ip] = False

    def parse_ts(ts: str) -> float:
        return datetime.strptime(
            ts[:-8],
            "%Y-%m-%dT%H:%M:%S.%f"
        ).timestamp()

    with open(log_path, 'r') as f:
        current_section = None
        init_time = 0
        init_ns = 0

        for line in f:
            line = line.strip()
            if not line:
                continue

            if line.split(" ")[0] == "MON_INIT":
                current_section = "INIT"
                init_time = parse_ts(line.split(" ")[1])
            elif line.startswith("MON_INIT_NSID:"):
                current_section = "INIT_NS"
                parts = line.split(" ")
                init_ns = int(parts[1]) if len(parts) > 1 else -1
            elif line == "MON_BEGIN":
                current_section = "MONITOR"
            else:
                if current_section in ["INIT", "INIT_NS"]:
                    # Parse initial IPs as CREATE events
                    match = ip_re.search(line)
                    if match:
                        ip = match.group(1)
                        add(IPEvent(
                            type=CREATE_TYPE,
                            host=host,
                            time=init_time,
                            ip=ip_address(ip.split('/')[0]),
                            ip_ns=init_ns,
                        ))
                elif current_section == "MONITOR":
                    # Parse monitoring events
                    parts = line.split(' ', 1)
                    if len(parts) == 2:
                        timestamp_str, event = parts
                        timestamp = parse_ts(timestamp_str)

                        if "Deleted" in event:
                            event_type = DELETE_TYPE
                        else:
                            event_type = CREATE_TYPE

                        ns_id = -1
                        for ns_re in ns_res:
                            m = ns_re.search(event)
                            if m and len(m.groups()) > 1:
                                ns_id = int(m.group(1))
                                break

                        m = ip_re.search(event)
                        if m:
                            ip = m.group(1)
                            add(IPEvent(
                                type=event_type,
                                host=host,
                                time=timestamp,
                                ip_ns=ns_id,
                                ip=ip_address(ip.split('/')[0])
                            ))
    return events


def _stop_and_parse_one(host: str, ipaddr: str, pid: str) -> List[IPEvent]:
    log_file = _stop_one(host, ipaddr, pid)
    results = _parse_log(host, log_file)
    # os.remove(log_file)
    return results


class DistributedIPMonitor:
    def __init__(self, hosts: Dict[str, str]):
        """
        Args:
            hosts: hostname -> IP
        """
        self._hosts = hosts
        self._process_ids = {}  # host -> pid

    def start_monitoring(self):
        """Start monitoring on all hosts"""
        ex, fts = ThreadPoolExecutor(16), {}
        for host, ipaddr in self._hosts.items():
            fts[ex.submit(_start_one, host, ipaddr)] = host
        done_fts, _ = futures.wait(fts.keys(), 600, futures.ALL_COMPLETED)
        for ft in done_fts:
            if ft.exception() is not None:
                logger.warning(f"failed to start monitor on host {fts[ft]}: {ft.exception()}")
            else:
                pid = ft.result()
                self._process_ids[fts[ft]] = pid

    def stop_monitoring(self) -> List[IPEvent]:
        """Stop monitoring and return sorted events"""
        all_events = []
        ex, fts = ThreadPoolExecutor(16), {}
        for host, proc_id in self._process_ids.items():
            fts[ex.submit(_stop_and_parse_one, host, self._hosts[host], proc_id)] = host
        done_fts, _ = futures.wait(fts.keys(), 600, futures.ALL_COMPLETED)

        for ft in done_fts:
            if ft.exception() is not None:
                logger.warning(f"failed to parse events on {fts[ft]}: {ft.exception()}")
            else:
                all_events.extend(ft.result())

        events = [e for e in all_events if e.ip in TARGET_SUBNET]
        return sorted(events, key=lambda x: x.time)


class EventRecorder:
    def __init__(self, tolerance: float):
        """
        Initialize the recorder with a specified overlap tolerance (seconds).
        We'll record events in an internal list, then do a pass to compute overlaps.
        """
        self.tolerance = tolerance
        self.events: List[IPEvent] = []  # all events ingested

    def add_event(self, event: IPEvent):
        """
        Add an IPEvent to the internal store.
        """
        self.events.append(event)

    def _build_intervals(self) -> Dict[Tuple[IPv4Address, str], List[Tuple[float, float]]]:
        """
        For each (ip, host, ns), build intervals [create_time, delete_time] from events.
        Return a dictionary (ip, host, ns) -> list of (start, end) intervals.
        """
        intervals: Dict[Tuple[IPv4Address, str], List[Tuple[float, float]]] = {}
        open_times: Dict[Tuple[IPv4Address, str], float] = {}

        for e in self.events:
            key = (e.ip, e.host)

            if e.type == CREATE_TYPE:
                if key in open_times:
                    # Consecutive CREATE without a DELETE in between.
                    # We'll just override the open time.
                    pass
                open_times[key] = e.time

            elif e.type == DELETE_TYPE:
                if key in open_times:
                    start_time = open_times.pop(key)
                    intervals.setdefault(key, []).append((start_time, e.time))
                else:
                    # DELETE without a CREATE => ignore
                    pass
            else:
                raise ValueError(f"Unknown event type: {e.type}")

        # for any opens without a close, record a totem time as the close
        default_endtime = max(2000000000.0, time.time())
        for key, start_time in open_times.items():
            intervals.setdefault(key, []).append((start_time, default_endtime))

        return intervals

    def compute_overlaps(self) -> List[List[IPEvent]]:
        """
        For each IP, find usage intervals from different (host, ns) combos that overlap.
        If an overlap is longer than the tolerance, we return the relevant CREATE/DELETE events.
        """
        # Sort all events once for final event collection
        self.events = sorted(self.events, key=lambda e: e.time)

        # Build intervals for each (ip, host, ns)
        intervals = self._build_intervals()

        # Group intervals by IP then by host
        ip_buckets: Dict[IPv4Address, Dict[str, List[Tuple[float,float]]]] = {}
        for (ip, host), ranges in intervals.items():
            ip_buckets.setdefault(ip, {}).setdefault(host, []).extend(ranges)

        final_overlaps: List[List[IPEvent]] = []

        # For each IP, compare intervals from different (host,ns)
        for ip_val, combos in ip_buckets.items():
            combo_keys = list(combos.keys())
            # If there's only one combo, no overlap possible
            if len(combo_keys) < 2:
                continue

            # sort intervals by start times, keeping a ref to the key
            sorted_intervals = []
            for key, intervals in combos.items():
                for interval in intervals:
                    sorted_intervals.append((interval, key))
            sorted_intervals = sorted(sorted_intervals, key=lambda i: i[0][0])  # interval -> start
            overlaps = []
            overlap_period_tracker = []  # overlap period for this IP
            active = None
            for interval, key in sorted_intervals:
                if not active:
                    active = (interval, key,)
                else:
                    other_end = active[0][1] - self.tolerance
                    start = interval[0]
                    if other_end > start:
                        if not overlap_period_tracker:
                            overlap_period_tracker.append(active)
                        overlap_period_tracker.append((interval, key,))
                        active = active if active[0][1] > interval[1] else (interval, key,)
                    else:
                        if overlap_period_tracker:
                            overlaps.append(overlap_period_tracker)
                            overlap_period_tracker = []
                        active = (interval, key)
            if overlap_period_tracker:
                overlaps.append(overlap_period_tracker)
            for overlap_list in overlaps:
                final_overlap = []
                for interval, key in overlap_list:
                    final_overlap.append(IPEvent(CREATE_TYPE, key, interval[0], -1, ip_val,))
                    final_overlap.append(IPEvent(DELETE_TYPE, key, interval[1], -1, ip_val,))
                final_overlap = sorted(final_overlap, key=lambda v: v.time)
                final_overlaps.append(final_overlap)

        return final_overlaps


def calculate_overlap_duration(err_events: List[IPEvent]):
    active = {}
    start_time = -1
    for e in err_events:
        if e.type == DELETE_TYPE:
            if e.host in active:
                del active[e.host]
            if len(active) == 1 and start_time > 0:
                return e.time - start_time
        else:
            active[e.host] = True
            if len(active) > 1 and start_time == -1:
                start_time = e.time
    return -1

exiting_event = Event()

def handle_interrupt(signum, frame):
    print(f"got signal {signum}")
    exiting_event.set()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor IP allocations on k8s hosts. Skips not ready hosts. Print periods of time where there were IP overlaps")
    parser.add_argument("--verbose", "-v", action="store_true", default=False)
    parser.add_argument("--nodes", "-n", help="node name regex matcher", default=".*")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    name_matcher = re.compile(args.nodes)
    nodes = json.loads(subprocess.check_output(shlex.split("kubectl get nodes -ojson")))
    hosts = {n["metadata"]["name"]:
                 [i["address"] for i in n["status"]["addresses"] if i["type"] == "InternalIP"][0]
             for n in nodes["items"] if name_matcher.match(n["metadata"]["name"]) and
                                       [c for c in n["status"]["conditions"] if c["type"] == "Ready" and c["status"] == "True"]}
    monitor = DistributedIPMonitor(hosts)
    monitor.start_monitoring()
    print("started. hit ctl+c to stop & analyze logs")

    signal.signal(signal.SIGINT, handle_interrupt)
    exiting_event.wait()
    print("\nstopping monitoring...")
    events = monitor.stop_monitoring()
    logger.info("stopped")

    if logger.isEnabledFor(logging.DEBUG):
        for event in events:
            logger.debug(event)

    recorder = EventRecorder(0.5)
    recorder.events = events
    overlaps = recorder.compute_overlaps()

    if overlaps:
        overlap_durations = []
        for i, err_events in enumerate(overlaps):
            overlap_duration = calculate_overlap_duration(err_events)
            logger.error(f"=== OVERLAP {i} ({overlap_duration:.2f}s) ===")
            overlap_durations.append(overlap_duration)
            for err in err_events:
                logger.error(f"overlap {i}: {err}")
        logger.error(f"found {len(overlaps)} anomalies in {len(events)} ip events")
        overlap_durations = sorted(overlap_durations)
        logger.error(f"min {overlap_durations[0]:.2f}s, max: {overlap_durations[-1]:.2f}s, mean: {sum(overlap_durations)/len(overlap_durations):.2f}s, median: {overlap_durations[len(overlap_durations)//2]:.2f}s")
    else:
        logger.info(f"found 0 anomalies in {len(events)} ip events")
