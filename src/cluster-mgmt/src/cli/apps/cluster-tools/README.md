# Cluster Mgmt Validation

## Network Validation

```
bash /opt/cerebras/tools/network-validation.sh
```

`network-validation.sh` runs tests for both cilium and multus, validating for connectivity over the management and data networks.

Errors will be reported for connectivity issues.

You can also run tests individually

`bash /opt/cerebras/tools/network-validation.sh [cilium|multus]`

Output looks like:

```
validating cilium start
ℹ️  Single-node environment detected, enabling single-node connectivity test
ℹ️  Monitor aggregation detected, will skip some flow validation steps
🔥 [kubernetes] Deleting connectivity check deployments...
✨ [kubernetes] Creating namespace cilium-test for connectivity check...
....

  ✅ All 1 tests (12 actions) successful, 22 tests skipped, 3 scenarios skipped.
validating cilium success
namespace "cilium-test" deleted

validating multus start
namespace/network-test configured
...

namespace "network-test" deleted
Validating multus success!
```


## Software Validation

```
bash /opt/cerebras/tools/software-validation.sh
```

`software-validation.sh` checks version consistency and performs a simple health check on cluster mgmt components.
Errors will be reported for version inconsistency.


Output looks like:


```
deployment "job-operator-controller-manager" successfully rolled out
job-operator healthy
deployment "cluster-server" successfully rolled out
cluster-server healthy
all k8s nodes healthy
cbcore version: registry.local/cbcore:0.0.0-202302061129-3715-be19d4bc
cluster-server version: registry.local/cluster-server:0.0.0-202302061129-3715-be19d4bc
job-operator version: registry.local/job-operator:0.0.0-202302061129-3715-be19d4bc
cilium version: 1.14.12
kubeadm version: v1.30.4
registry.local/kube-apiserver:v1.30.4
registry.local/kube-controller-manager:v1.30.4
registry.local/kube-scheduler:v1.30.4
registry.local/kube-proxy:v1.30.4
registry.local/pause:3.9
registry.local/etcd:3.5.12-0
registry.local/coredns/coredns:v1.11.1
cbcore/cluster-server/job-operator versions consistent
Software validation success!
```

## Access cached compile directories stored in ceph

The `access-cached-compile-job.yaml` defines a k8s job, which creates a pod with cached
compile root directory mounted. It provides a way to access the cached compile content
stored in ceph.

To create the job, run
```
kubectl apply -f access-cached-compile-job.yaml
```

This will create a pod with a name like `access-cached-compile-<hash>`. You can then
log into the pod to access the cached compile content:
```
kubectl exec -ti job/access-cached-compile -- sh
```

## Access log export directories stored in ceph

The `access-log-export-job.yaml` defines a k8s job, which creates a pod with log export
root directory mounted. It provides a way to access the log export content stored in ceph.

To create the job, run
```
kubectl apply -f access-log-export-job.yaml
```

This will create a pod with a name like `access-log-export-<hash>`. You can then
log into the pod to access the log export content:
```
kubectl exec -ti job/access-log-export -- sh
```

# Access debug artifact directories stored in ceph

The `access-debug-artifact-job.yaml` defines a k8s job, which creates a pod with debug artifact
root directory mounted. It provides a way to access the debug artifact content stored in ceph.

To create the job, run
```
kubectl apply -f access-debug-artifact-job.yaml
```

This will create a pod with a name like `access-debug-artifact-<hash>`. You can then
log into the pod to access the debug artifact content:
```
kubectl exec -ti job/access-debug-artifact -- sh
```
