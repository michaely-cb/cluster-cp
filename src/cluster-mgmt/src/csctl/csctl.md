# csctl

A CLI for the Cerebras appliance operations. This tool is intended to be used by ML app developers who need to inspect the state of jobs in the system as well as cluster operators who need to understand cluster state and configuration.

Some csctl capabilities include:

- list and get job details with varying levels of details
- list and get NFS volumes attached to the appliance which can be mounted by jobs
- list CS2 usage and system load of servers in appliance
- label jobs with additional metadata
- manage csctl configuration to connect to the appliance
- validate volume configuration on usernodes


# Cheatsheet

_Note: some of these commands use [`jq`](https://github.com/stedolan/jq#jq) to process JSON output, but it is not a
dependency of csctl._

Each command takes the flag `--help` which will output usage and examples. The majority of the tool's documentation exists in this form. The examples below show some common command usage.

```shell
# see help text for any command with --help
csctl get --help

# list out jobs
csctl get jobs [--all-states] [--namespace=test] [--workflow=test]

# get a particular job in yaml with additional debug fields visible in the output
csctl get job wsjob-example-0 -oyaml --debug=1

# List up to 100 jobs
csctl get jobs --max-jobs 100

# get a particular job in JSON format and only inspect the labels
csctl get job wsjob-example-0 -ojson | jq '.meta.labels'

# cancels a running job
csctl cancel job wsjob-example-0 [--namespace=test]

# download logs for wsjob-example-0 to current directory
csctl log-export wsjob-example-0 [--namespace=test]

# download last 10 lines of logs for wsjob-example-0 to current directory
csctl log-export wsjob-example-0 --num-lines 10

# download logs and binary artifacts for wsjob-example-0 into current directory (can be very large)
csctl log-export wsjob-example-0 --binaries

# download logs and compile artifacts for wsjob-example-0 into current directory (can be very large)
csctl log-export wsjob-example-0 --compile-artifacts

# download logs with a timeout specified in seconds
csctl log-export wsjob-example-0 --timeout 1800

# add a label x=y to a job and remove a label foo
csctl label job wsjob-example-0 x=y foo-

# list volumes
csctl get volumes

# list worker cache usage data (worker nodes only)
csctl get worker-cache

# clear worker cache data (worker nodes only)
csctl clear-worker-cache

# get cluster system/nodes status, cpu(5min avg), mem... 
csctl get cluster

# get cluster status with error node/system only
csctl get cluster --error-only

# get cluster CS2s status
csctl get system [--error-only]

# get cluster node status
csctl get node [--error-only] [--role=<role>] [--version=<version>|--version!=<version>]

# get cluster node groups
csctl get nodegroup

# inspect all the resource types the server supports for getting
csctl types

# inspect the current configuration
csctl config view

# add a new cluster to the config and use that cluster
csctl config set-cluster cerebras-x --server 10.0.0.1 --certificate-authority /share/certs/cerebras-x.crt
csctl config use-cluster cerebras-x

# make a copy of the global config and edit it for experimentation
cp /opt/cerebras/config_v2 /tmp/csctl-config
csctl --csconfig /tmp/csctl-config config set-cluster cerebras-x --server 10.0.0.2
csctl --csconfig /tmp/csctl-config get jobs

# List volume validity checks in a table
csctl check-volumes

# Show volume validity check output in JSON format
csctl check-volumes -ojson
```

# Configuration

Configurations files are set in `/opt/cerebras/config_v2` by default but may be overridden with the flag `--csconfig=PATH`. The
config contains enough information for the CLI or other Cerebras appliance clients to be able to connect to the
appliance API.

Examples

```shell
# view current config
csctl config view

# add or update a particular cluster
csctl config set-cluster cerebras-x --server 10.0.0.1 --certificate-authority /share/certs/cerebras-x.crt

# set a cluster as the active cluster
csctl config use-cluster

# delete a cluster
csctl config delete-cluster cerebras-x
```

# Advanced Usage
## Jobs
### List jobs
```
csctl get jobs -o json
```

### List most recent (sorted by creation timestamps) k jobs
```
csctl get jobs -o json | jq -r '.items[:k]'
```

### Get the (zero-based)kth job (sorted by creation timestamps) with debug fields
```
csctl -d1 get jobs -o json | jq -r '.items[k]'
```

### List custom fields only (job name, user, job status, systems used, start and completion time of job)
```
csctl get jobs -o json | jq -r '[.items[] | {name: .meta.name, user: .spec.user.username, status: .status.phase, systems: .status.systems | sort, numSystems: .status.systems | length, executionTime: .status.executionTime, completionTime: .status.completionTime}]'
```

### List jobs that used n systems
```
csctl get jobs -o json | jq -r '[.items[] | select (.status.systems | length == n)]'
```

### List jobs that used a particular system (systemf162 for example)
```
csctl get jobs -o json | jq -r '[.items[] | select (.status.systems | any(. == "systemf162"))]'
```

### List jobs that were requested from a particular user (lab for example)
```
csctl get jobs -o json | jq -r '[.items[] | select (.spec.user.username == "lab")]'
```

### List jobs of a particular status that were requested from a particular user (lab for example)
Possible statuses:
- CREATED
- RUNNING
- SUCCEEDED
- CANCELLED
- FAILED
```
csctl get jobs -o json | jq -r '[.items[] | select (.spec.user.username == "lab" and .status.phase == "RUNNING")]'
```

### List jobs that were running in a particular datetime range
```
csctl get jobs -o json | jq --arg s '2023-05-25T02' --arg e '2023-05-25T03' -r '.items | map(select(.status.executionTime | . >= $s and . <= $e + "z"))'
```

### List jobs with a label key-value pair
```
csctl get jobs -o json | jq -r '[.items[] | select (.meta.labels["mykey"] == "myvalue")]'
```

### Label a job with a key-value pair
```
csctl label job wsjob-tysjp2z49fqdrt3etzqfaj mykey=myvalue
```

### Get the network topology info of a job at per pod granularity
#### Fields:
- POD:                      The name of the pod
- NODE:                     The name of the physical node (AX, SX, ...) that the pod assigned and runs on
- NICS:                     NICs of pod ingress/egress traffic goes through
- NODE_LEAF_SWITCH:         The leaf switch that assigned node connects with
- EGRESS_TARGETS:           The target node/system of egress network traffic
- SYSTEM_PORTS:             The system and ports that the pod talks to
- SYSTEM_AFFINITY_SWITCH:   The leaf switch that the system connects with (may not be the same as the pod when spine traffic occurs)
- POD_PREFERRED_PORTS:      The preferred cs ports from the stack compile hint that the pod should target

```
csctl job topology wsjob-test
```