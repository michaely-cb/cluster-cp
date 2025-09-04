# Kube VIP Health

Wrapper for kube-vip to add additional health check signal that can be watched
by container startup + liveness check. Launches the kube-vip server only after
initial health checks have succeeded, preventing flapping of kube-vip BGP
advertisement.

Checks
- Watches control plane (:6443) by default.
- Optionally watches registry through 100G interface discovered from cluster
  configmap.

## Usage

The container built by this script wraps the kube-vip command.

Launch the kube-vip pod like so:

```
spec:
  containers:
  - args:
    - /kube-vip
    - manager
    env:
    # optional port specification for health server, default is shown below
    - name: HEALTH_PORT
      value: "30901"
    # optionally enable checking private registry/api-server
    - name: WATCH_REGISTRY
      value: "true"
    - name: WATCH_CP
      value: "true"
    ... ordinary kube-vip env ...
```

# Updating

If you update the go code, please update the build tag in Makefile and then
update `cli/common/__init__.py`'s image reference as well as references in
apps/k8s.

Then push the updated image to ECR:

```
make
make push
```