# Benchmarks: Connection Speed

These scripts can be useful to explore the throughput of communication
between two nodes of a cluster.

## Preliminaries

In order to run these scripts, you must install the `iperf3` and `pidstat`
utilities.

```
sudo yum install -y iperf3 sysstat
```

These scripts are capable of establishing and measuring multiple TCP/IP
connections in parallel, starting with the first connection at port 5000
and increasing by 1 for each additional concurrent connection.

The firewall settings must allowed for connections on these port ranges,
or the firewall must be disabled, in order for this benchmark to work.


## Usage

These scripts are fairly simple. At one end of the connection, first run the
`./server.sh` script with the following options:

```
# Params: ./server.sh <local_nic_ip> <num_connections>
# Example:

./server.sh 10.254.30.224 1
```

Once the server is online, it will tell you the port[s] it is listening on,
and block until you terminate the process with `CTRL-c`.

Now, at the other end of the link you're wishing to measure, invoke the
`client.sh` script:

```
# Params: ./client.sh <local_nic_ip> <remote_nic_ip> <num_connections>
# Example:

./client.sh 10.254.30.208 10.254.30.224 1
```

## Output

While the `server.sh` script outputs only to the screen, all of the client
scripts will generate a set of three files in the CWD where the script was
invoked. The three files begin with the prefixes: `mem`, `cpu`,
and `log`. The `log` file contains the measurements of the communication
between the `iperf3` client, and the `iperf3` server over 100 seconds,
bucketed into 20-second averages in bins. When running the reverse script,
the client-side `iperf3` instance is acting as a server, but will still be
the side that emits the output files.

The name of the files contain some information about the run:
`log-${TOTAL_STREAMS}.${STREAM_IDX}-${GROUP_PID}` where the ${GROUP_PID}
is the process ID of the script that invokes each of the streams.

_NOTE:_ The `cpu` and `mem` logs are measures of ONLY the OS-reported metrics
for ONLY the client-side `iperf3` PID, using the `pidstat` tool. The logs are
sampled at 2-second intervals, and will need to be projected over the
network measurements 20-second interval average-bitrate buckets. Remember
that even though work is happening in the first 5 seconds, the `log` averages
are omitting the first five seconds of communication to discount any initial
slow TCP/IP speeds due to protocol settings that may need a moment to ramp
up the amount of bandwidth the OS is allocating to the process.


## Advanced Use

### Convenience DaemonSet for Pod Benchmarking

An example DaemonSet `cbcore_daemonset_example.yaml` is provided for
launching a pod on each node with a cbcore container that is running
the `sleep` command for 1,000,000 seconds (11.5 Earth days).

You can invoke this DaemonSet multiple times in order to get multiple
independent pods on each node. Before you do so, get a listing of
all currently running pods in the `bench` namespace, to ensure you
do not cause confusion for others, and that any pods you've previously
created are not still running and available to you.

```
kubectl get pods -n bench
```

_IMPORTANT:_ When you are ready to bring up your own benchmarking pods,
ensure you have a namespace `bench` or your pods will never run. To create
a namespace, you can use the `kubectl create namespace bench` command, but
you may as well see if one already exists. In the below example, you can see
that the first listed namespace is `bench` so we're OK:

```
[root@master ~]# kubectl get namespaces
NAME              STATUS   AGE
bench             Active   9d
default           Active   11d
kube-node-lease   Active   11d
kube-public       Active   11d
kube-system       Active   11d
```

_NOTE:_ You can delete all the benchmarking pods by deleting the `bench`
namespace, just make sure someone else isn't using them, and you've
copied any desired output files to a persistent store somewhere:

```
kubectl delete namespace bench
```

When you're ready to create a new benchmarking workspace for each node
(including the controlplane node[s]), run the following command on the
provided example yaml or your modified version:

```
kubectl apply -f <yamlfile>
```

One all the pods are online, you can list them and connect to them like so:

```
[root@master ~]# kubectl get pods -n bench
NAME                       READY   STATUS    RESTARTS   AGE
bench-cbcore-cnzqb-9pvmm   1/1     Running   0          9d
bench-cbcore-cnzqb-x4l7v   1/1     Running   0          9d
bench-cbcore-d5qvp-m2zrf   1/1     Running   0          9d
bench-cbcore-d5qvp-zmrbm   1/1     Running   0          9d
bench-cbcore-g5tf9-6zbsn   1/1     Running   0          9d
bench-cbcore-g5tf9-bb9sp   1/1     Running   0          9d
bench-cbcore-njk77-cjnq7   1/1     Running   0          9d
bench-cbcore-njk77-hf7gt   1/1     Running   0          9d
bench-cbcore-t6mmv-2pd7t   1/1     Running   0          9d
bench-cbcore-t6mmv-llm4z   1/1     Running   0          9d
[root@master ~]# kubectl exec -it -n bench bench-cbcore-cnzqb-9pvmm -- /bin/bash
[root@bench-cbcore-cnzqb-9pvmm cbcore]#
```

This invokes an instance of `/bin/bash` inside the pod, and connects your
terminal to it, which is effectively like ssh'ing into the pod. When you are
done working in the pod, you can close the connection by typing `exit` and
ending the `/bin/bash` process.

_NOTE_: When all processes in the pod have ended, the pod will end. Because
this is a DaemonSet, that will cause Kubernetes to restart the pod and execute
a new `sleep 1000000` process within it. It will appear from the outside
that the pods you'd initially created are still running, but importantly, the
container environment will have been completely reset back to the original
image it was loaded from, removing all state and any files you'd created that
are not in a persistent store.

### Reverse Direction

With the same `server.sh` running already, and the same command options
as are used for normal `client.sh` runs, the direction of traffic from client
to server can be reversed by invoking the `client_reverse.sh` script.

### Task Affinity

If you wish to gain more observability over the `iperf3` processes,
especially in cases where multiple streams are being launched in parallel,
a version of the script is provided where the `taskset` command is used
to pin processes to specific cores.

Using this technique has in the past introduced up to 25% slowdown to
the Gbits/sec transfer rate per stream of data, as it potentially
prevents the CPU from fully exploiting its internal parallelism.

Versions of the above scripts are provided which facilitate task pinning.

For cases where bandwidth is being measured using multiple streams
from multiple pods on the same node, an additional parameter is added
to both scripts, allowing each invocation to begin pinning streams to
cores starting with that number, increasing by one for each parallel
stream requested.

These special versions of the scripts are `taskset_server.sh` and
`taskset_client.sh`.

It is NOT recommended for production runs that communication tasks
be pinned to specific cores, as this will likely degrade performance.


