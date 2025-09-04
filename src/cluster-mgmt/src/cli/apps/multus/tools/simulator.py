import argparse
import json
import logging
import pathlib
import queue
import random
import shlex
import signal
import subprocess
import threading
import time
from datetime import datetime
from queue import Queue
from typing import Callable, Tuple

import dataclasses

logging.basicConfig(level=logging.INFO)

EK_JOB_CREATED = "JOB_CREATED"
EK_JOB_RUNNING = "JOB_RUNNING"
EK_JOB_RUNNING_TIMEOUT = "JOB_RUNNING_TIMEOUT"
EK_JOB_DELETED = "JOB_DELETED"
EK_JOB_REMOVED = "JOB_REMOVED"
EK_JOB_REMOVED_TIMEOUT = "JOB_REMOVED_TIMEOUT"

@dataclasses.dataclass
class Event:
    time: float
    level: int  # 0=error,1=warn,>=2=info/debug
    kind: str
    reporter: str
    details: str

    def __str__(self):
        return f"[{datetime.fromtimestamp(self.time)} {self.level}] {self.kind} {self.reporter} {self.details}"

logger = logging.getLogger(__name__)

NAMESPACE = "job-operator"


class KubernetesDriver:
    """ Drive kubernetes interactions through kubectl - could use client library but then need libraries """

    _deployment_template = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    app: simulator
spec:
  replicas: {replicas}
  selector:
    matchLabels:
      app: {name}
  template:
    metadata:
      labels:
        app: {name}
      annotations:
        k8s.v1.cni.cncf.io/networks: multus-data-net
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: k8s.cerebras.com/node-role-broadcastreduce
                operator: DoesNotExist
      terminationGracePeriodSeconds: 1
      tolerations:
      - key: "node-role.kubernetes.io/control-plane"
        operator: "Exists"
        effect: "NoSchedule"
      containers:
      - name: busybox
        image: registry.local/busybox:1.34.1
        # image: busybox:1.37.0
        command: ["/bin/sh","-c"]
        args: ["sleep infinity"]
    """

    def create_deployment(self, name: str, replicas: int) -> bool:
        fn = f"/tmp/{name}.yaml"
        try:
            with open(fn, 'w') as f:
                f.write(self._deployment_template.format(name=name,namespace=NAMESPACE, replicas=replicas))
            subprocess.check_output(shlex.split(f"kubectl apply -f {fn}"))
            return True
        except subprocess.CalledProcessError as e:
            logger.debug(f"failed to create deploy {name}: {e}")
            return False
        finally:
            pathlib.Path(fn).unlink(missing_ok=True)

    def delete_deployment(self, name: str) -> bool:
        try:
            subprocess.check_output(shlex.split(f"kubectl delete deploy -n {NAMESPACE} {name}"))
            return True
        except subprocess.CalledProcessError as e:
            if (e.stderr and "NotFound" in e.stderr) or (e.stdout and  "NotFound" in e.stdout):
                return True
            logger.debug(f"failed to delete deploy {name}: {e}")
            return False

    def count_pods_state(self, name: str) -> Tuple[int, int]:
        try:
            output = subprocess.check_output(shlex.split(f"kubectl get pods -ojson -n {NAMESPACE} -lapp={name}"))
            doc = json.loads(output.decode())
            running, not_running = 0, 0
            for pod in doc["items"]:
                if pod["status"]["phase"] == "Running":
                    running += 1
                else:
                    not_running += 1
            return running, not_running
        except subprocess.CalledProcessError as e:
            logger.debug(f"failed to list pods {name}: {e}")
            return -1, -1


class JobSimulator:
    def __init__(
        self,
        event_recorder: Callable[[Event], None],
        startup_timeout: float = 180.0,
        deletion_timeout: float = 180.0,
        max_backoff: float = 5.0,
        job_runtime: float = -1.0,
    ):
        """
        Simulate the creation of a job, watch until all pods are Running or the startup times out, then delete the
        deployment and wait until all pods vanish or the deletion times out

        :param event_recorder: function that takes an Event object to record.
        :param startup_timeout: seconds to wait for all pods to reach Running.
        :param deletion_timeout: seconds to wait for all pods to be deleted.
        :param max_backoff: maximum backoff in seconds if the API server is unavailable.
        """
        self.record_event = event_recorder
        self.startup_timeout = startup_timeout
        self.deletion_timeout = deletion_timeout
        self.job_runtime = job_runtime
        self.max_backoff = max_backoff
        self.num_replicas = 100

        self._name = f"job-simulator-{int(time.time())}"
        self._driver = KubernetesDriver()
        self._job_createtime = -1

    def create_deployment(self):
        attempt = 0
        while not self._driver.create_deployment(self._name, self.num_replicas):
            wait_time = self._backoff_delay(attempt)
            time.sleep(wait_time)
            attempt += 1
        self._job_createtime = time.time()
        self.record_event(
            Event(
                time=self._job_createtime,
                level=2,
                kind=EK_JOB_CREATED,
                reporter=self._name,
                details=f"Created deployment '{self._name}' with {self.num_replicas} pods."
            )
        )


    def wait_for_pods_running(self):
        start_time = time.time()
        attempt = 0
        while True:
            elapsed = time.time() - start_time
            if elapsed >= self.startup_timeout:
                self.record_event(
                    Event(
                        time=time.time(),
                        level=1,
                        kind=EK_JOB_RUNNING_TIMEOUT,
                        reporter=self._name,
                        details=(
                            f"Timeout waiting for all pods to be Running. Elapsed={elapsed:.1f}s "
                            f"(limit={self.startup_timeout:.1f}s). Proceeding."
                        )
                    )
                )
                break

            running, not_running = self._driver.count_pods_state(self._name)
            if not_running == 0 and running == self.num_replicas:
                self.record_event(
                    Event(
                        time=time.time(),
                        level=0,
                        kind=EK_JOB_RUNNING,
                        reporter=self._name,
                        details=f"took {elapsed}",
                    )
                )
                break
            elif running == -1:
                wait_time = self._backoff_delay(attempt)
                time.sleep(wait_time)
                attempt += 1
            else:
                time.sleep(1)  # poll interval

    def job_run_delay(self):
        if self.job_runtime > 0:
            await_time = self.job_runtime - (time.time() - self._job_createtime)
            if await_time > 0:
                time.sleep(await_time)
            else:
                logger.warning("skipping job run delay since deadline already exceeded")


    def delete_deployment(self):
        attempt = 0
        while not self._driver.delete_deployment(self._name):
            wait_time = self._backoff_delay(attempt)
            time.sleep(wait_time)
            attempt += 1
        self.record_event(
            Event(
                time=time.time(),
                level=0,
                kind=EK_JOB_DELETED,
                reporter=self._name,
                details=f"took {attempt} attempts",
            )
        )

    def wait_for_pods_deleted(self):
        start_time = time.time()
        attempt = 0
        while True:
            elapsed = time.time() - start_time
            if elapsed >= self.startup_timeout:
                self.record_event(
                    Event(
                        time=time.time(),
                        level=1,
                        kind=EK_JOB_REMOVED_TIMEOUT,
                        reporter=self._name,
                        details=(
                            f"Timeout waiting for all pods to be removed. Elapsed={elapsed:.1f}s "
                            f"(limit={self.startup_timeout:.1f}s). Proceeding."
                        )
                    )
                )
                break

            running, not_running = self._driver.count_pods_state(self._name)
            if not_running == 0 and running == 0:
                self.record_event(
                    Event(
                        time=time.time(),
                        level=0,
                        kind=EK_JOB_REMOVED,
                        reporter=self._name,
                        details=f"took {elapsed}",
                    )
                )
                break
            elif running == -1:
                wait_time = self._backoff_delay(attempt)
                time.sleep(wait_time)
                attempt += 1
            else:
                time.sleep(1)  # poll interval

    def _backoff_delay(self, attempt: int) -> float:
        """
        Compute an exponential backoff, up to self.max_backoff.
        """
        base = 1
        backoff = min(self.max_backoff, (base ** attempt))
        return backoff * 0.5 + random.uniform(0, backoff * 0.5)

    def run_simulation(self):
        """
        Orchestrate the entire flow: create deployment, wait for pods running,
        delete deployment, wait for pods deleted.
        """
        self.create_deployment()
        self.wait_for_pods_running()
        self.job_run_delay()
        self.delete_deployment()
        self.wait_for_pods_deleted()


class TimeRecorder:
    def __init__(self):
        self.job_start_times = {}
        self.job_delete_times = {}
        self.job_start_durations = []
        self.job_delete_durations = []

    def observe(self, e :Event):
        if e.kind == EK_JOB_CREATED:
            self.job_start_times[e.reporter] = e.time
        elif e.kind in (EK_JOB_RUNNING, EK_JOB_RUNNING_TIMEOUT):
            start = self.job_start_times.get(e.reporter)
            if start:
                self.job_start_durations.append(e.time - start)
        elif e.kind == EK_JOB_DELETED:
            self.job_delete_times[e.reporter] = e.time
        elif e.kind in (EK_JOB_REMOVED, EK_JOB_REMOVED_TIMEOUT):
            end = self.job_delete_times.get(e.reporter)
            if end:
                self.job_delete_durations.append(e.time - end)

    def __str__(self):
        def mmmm(durations):
            if not durations:
                return -1, -1, -1, -1
            durations.sort()
            return durations[0], durations[-1], sum(durations) / len(durations), durations[len(durations)//2]
        s0, s1, s2, s3 = mmmm(self.job_start_durations)
        e0, e1, e2, e3 = mmmm(self.job_delete_durations)
        return f"type,total,min,max,mean,median\nstartup,{len(self.job_start_durations)},{s0:.1f},{s1:.1f},{s2:.1f},{s3:.1f}\nteardown,{len(self.job_delete_durations)},{e0:.1f},{e1:.1f},{e2:.1f},{e3:.1f}"


def exit_handler(ev_queue: Queue):
    def handle_interrupt(signum, frame):
        ev_queue.put(Event(time.time(), 0, "EXIT", "handler", f"got signal {signum}"))
    return handle_interrupt


def main_driver_loop(duration: int, job_duration: float):
    evt_queue = queue.Queue()

    def record_event(evt: Event):
        evt_queue.put(evt)

    # Create and start a job in a separate thread
    job = JobSimulator(event_recorder=record_event, job_runtime=job_duration)
    job_thread = threading.Thread(target=job.run_simulation, daemon=True)
    job_thread.start()
    threads = {job._name: (job, job_thread,)}
    r = TimeRecorder()

    signal.signal(signal.SIGINT, exit_handler(evt_queue))

    start_time = time.time()
    end_time = start_time + duration
    def should_run() -> bool:
        if end_time <= start_time:
            return True
        return time.time() < end_time

    while should_run():
        e = evt_queue.get()
        if e.kind == "EXIT":
            break
        r.observe(e)
        logger.info(f"{e}")
        if e.kind == EK_JOB_DELETED:
            job = JobSimulator(event_recorder=record_event, job_runtime=job_duration)
            job_thread = threading.Thread(target=job.run_simulation, daemon=True)
            job_thread.start()
            threads[job._name] = (job, job_thread)
        elif e.kind == EK_JOB_REMOVED_TIMEOUT or e.kind == EK_JOB_REMOVED:
            if e.reporter in threads:
                del threads[e.reporter]
    print("stopping...")

    # XXX
    for job, _ in threads.values():
        job._driver.delete_deployment(job._name)

    print(r)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run job simulation")
    parser.add_argument("--duration", help="how many seconds to run for", default=-1, type=int)
    parser.add_argument("--job-duration",
        help="how many seconds the job should run for. If <= 0, then job immediately terminates after all pods starting",
        default=-1, type=float)
    args = parser.parse_args()
    main_driver_loop(args.duration, args.job_duration)