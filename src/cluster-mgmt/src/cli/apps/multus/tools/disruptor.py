import argparse
import json
import logging
import random
import shlex
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from threading import Thread
from typing import List, Tuple, Dict

import paramiko

logger = logging.getLogger(__name__)

SSH_USER = "root"


@dataclass
class DisruptionEvent:
    type: str
    host: str
    time: float  # Unix timestamp

    def __str__(self):
        return f"[{self.host:<14}] {datetime.fromtimestamp(self.time)} {self.type}"


def _exec_cmd(host: str, cmd: str) -> Tuple[str, str]:
    """Execute SSH command and return output"""
    client = paramiko.SSHClient()
    try:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            host,
            username=SSH_USER,
            look_for_keys=True,
        )
        stdin, stdout, stderr = client.exec_command(cmd)
        return stdout.read().decode(), stderr.read().decode()
    finally:
        try:
            client.close()
        except:
            pass


class Disruptor:
    def __init__(self, hosts: List[str], event_rates: Dict[str, float]):
        """
        :param hosts:
        :param event_rates: event_type => probability of event occurring in a 1 second interval
        """
        self._hosts = hosts
        self._rates = event_rates
        self._events = []
        self._stopped = False
        self._thread: Thread = None

    def _run(self):
        cycle_duration = 1
        while not self._stopped:
            sleep_time = 1 - cycle_duration
            if sleep_time > 0:
                time.sleep(sleep_time)
            start_time = time.time()

            for event, rate in self._rates.items():
                p = random.randint(0, 1_000_000) / 1_000_000.0
                if p >= rate:
                    continue
                victim = self._hosts[0]
                if event == "kubelet_restart":
                    victim = random.choice(self._hosts)
                    stdout, stderr = _exec_cmd(victim, "systemctl restart kubelet")
                    logger.debug(f"{victim} restarted kubelet: {stdout}, {stderr}")
                elif event == "apiserver_restart":
                    stdout, stderr = _exec_cmd(victim, "bash -c \"nerdctl -n k8s.io container ls | grep apiserver:v | cut -d' ' -f1 | xargs -n1 nerdctl -n k8s.io container kill\"")
                    logger.debug(f"{victim} restarted apiserver: {stdout}, {stderr}")
                else:
                    assert False, "invalid event type"
                self._events.append(DisruptionEvent(
                    type=event,
                    host=victim,
                    time=time.time(),
                ))
                logger.debug(f"{self._events[-1]}")
            cycle_duration = time.time() - start_time

    def start(self):
        """Start monitoring on all hosts"""
        self._thread = Thread(target=self._run)
        self._thread.start()

    def stop(self) -> List[DisruptionEvent]:
        """Stop monitoring and return sorted events"""
        self._stopped = True
        self._thread.join(timeout=10)
        self._thread = None
        return self._events



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Disrupt k8 cluster in various ways")
    parser.add_argument("--verbose", "-v", action="store_true", default=False)
    parser.add_argument("--kubelet-restarts-per-minute", "-k", type=float, default=0)
    parser.add_argument("--apiserver-restarts-per-minute", "-a", type=float, default=0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    nodes = json.loads(subprocess.check_output(shlex.split("kubectl get nodes -ojson")))
    cp = [n["metadata"]["name"] for n in nodes["items"] if "node-role.kubernetes.io/control-plane" in n["metadata"]["labels"]]
    wk = [n["metadata"]["name"] for n in nodes["items"] if n["metadata"]["name"] not in cp]
    disruptor = Disruptor(cp + wk, {
        "kubelet_restart": args.kubelet_restarts_per_minute / 60.0,
        "apiserver_restart": args.apiserver_restarts_per_minute / 60.0,
    })
    disruptor.start()
    logger.info("started. hit ctl+c to stop")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping disruptions...")
        events = disruptor.stop()
    logger.info("stopped")

    for event in events:
        logger.info(event)
