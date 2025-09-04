# Registry

A cluster-private instance of a docker image registry.

Each node has `/etc/containerd` configured to redirect image requests to `registry.local` to
the nodePort service port of the private registry.

Registry storage is backed by local hostPath-based storage and therefore needs to be pinned to a 
single node.
