# br-perf

Tests 100G NICs on broadcastreduce servers can be saturated by iperf clients
running on memoryx nodes.

Test runs the following psuedo code

```
tasks=[]

for br_node in br_nodes:
    tasks.append(
        lambda:
            with pop_free_memx_node() as memx_node:
                for network_interface in br_node.network_interfaces:
                    servers = create_iperf_servers(br_node,   src=network_interface)
                    clients = create_iperf_clients(memx_node, dst=network_interface)
                    await_finish(clients)
                    delete(clients, servers)
    )

await_async_tasks_complete(tasks)

report_results
```

## Prerequisites

- K8s installed
- CNI (Cilium) installed

Multus is not required as test pods use the host network directly.

## Usage

```
./run.sh [-m mem_node,mem_node,...] [-M FILE] [-b br_node,br_node,...] [-B FILE] [--report-only]
```

- `--report-only` is a flag that can be passed to parse results in case a test has already ran in the past.
- `-b or -B` must be provided always and `-m or -M` must be provided to run a test.
  - `-B/-M` must point to files containing ip or hostnames of BR or MEM nodes respectively
  - `-b/-m` must be comma-seperated lists of ip or hostnames of BR or MEM nodes respectively


Build this test on a dev server and copy the resultant tar file onto the
management node of the target cluster.

```sh
make
scp br-perf.tar ${MGMTNODE}:/home
```

Then, run the script like so

```sh
ssh ${MGMTNODE}

cd /home
tar xf br-perf.tar

cd br-perf
./run.sh -b sc-r11rb7-s5 -m sc-r11rb12-s4
BR_NODE        BR_IP           MEM_NODE        GBPS
sc-r11rb7-s5   10.250.160.161  sc-r11rb12-s4   100.4
...
```

You can run on all BR/MEMX in a k8s cluster like:

```
kubectl get nodes -lk8s.cerebras.com/node-role-memory -ojson | jq -r '.items[] | .metadata.name' > mem.list
kubectl get nodes -lk8s.cerebras.com/node-role-broadcastreduce -ojson | jq -r '.items[] | .metadata.name' > br.list

./run.sh -B br.list -M mem.list
```

This will produce a csv file `results.csv` in the working directory after about
5 to 10 minutes.

The output will look something like as follows

```
BR_NODE        BR_IP           MEM_NODE        GBPS
sc-r11rb7-s5   10.250.160.161  sc-r11rb12-s4   100.4
sc-r11rb7-s5   10.250.160.162  sc-r11rb12-s4   100.19
...
```

where

```
BR_NODE, MEM_NODE were the nodes where the server, client were launched respectively
BR_IP is the broadcastreduce NIC IP address
GBPS is the sum of all gigabit rates
```

The most important thing to look for is if GBPS is within 1 or 2 GPS of 100. The
return code of the function will be non-zero if any GBPS is < 98.

## Notes

Ideally, we'd run this without k8s since it could be used to qualify hardware
before kubernetes runs.

We could run it with containerd bare: testing on real hardware shows that
launching a container like `numactl -N0 ctr run ...` gives the intended numa
affinity while `numactl -N0 ctr run numactl -N0` or `ctr run numactl -N0 ...`
does not.

To achieve saturation with k8s, we create 8 parallel iperf processes on the
same node whereas in a numa-aware setting, we'd expect 3-4 parallel instances
would achieve saturation.
