# Cluster Volumes Tool

A bash script for updating user managed volumes to be used by a cluster administrator to configure
appliance access to site NFS volumes. End users of the appliance can then reference these volumes
in their `run.py` scripts which the ML job will then have access.

The primary motivation of using NFS volumes is to enable worker containers to access training data
sets and modelzoo dependencies that on the NFS. The path mapping from host to container is one to one.
Therefore, the container-mounted files will have exactly the paths as they are on the user node.

Parameters:
- `--server`, the NFS server address, which can be a hostname or an IP
- `--server-path`, the file path on the NFS server
- `--container-path`, the file path we are seeing in the container and on the user node
- `--readonly`, whether the volume should be read-only
- `--allow-venv`, when user venv copying is needed, the cluster server looks up volumes
  with this label and uses one of them as the copying destination
- `--workdir-logs`, is used to filter out volumes that are intended for wsjob workdir logging purposes
- `--cached-compile`, is used to filter out volumes that are intended for cached compiles
- `--create-dir-if-not-exists`, should be used when we need to set up a volume with a non-existing
  server path from a management node which does not have NFS mounted
- `--permission`, is used to set the permission of the volume

Cluster admins can use the '--permission' flags to set permission of the volume.
This allows certain group of users to write to the volume with correct permissions from usernodes. A typical use case
is the appliance client copies the local venv to the venv volume to achieve dependency parity between the usernode and
worker Python environment.

This script must run on the management node of the cluster since internally,
it calls into kubernetes.

Run `./cluster-volumes.sh` for additional help text. Additonally, sub-commands have help text,
accessible like `./cluster-volumes.sh put`. The four sub commands are `get`, `put`, `delete`,
and `test`.

A typical workflow for adding a volume is demonstrated below.

```bash
$ ./cluster-volumes.sh put training-data-volume --container-path /cb/mldat --server nfs.example.com --server-path /mldat

# Note: put will test that kubernetes can mount the volume and will reject creates or updates that would
# lead to unmountable volumes.
$ ./cluster-volumes.sh put training-data-volume --container-path /cb/mldat --server broken.example.com --server-path /mldat
# test failed to complete, there was probably a mount error, see events:
# MountVolume.SetUp failed for volume "training-data-volume" : mount failed: exit status 32
# Mounting command: mount
# Mounting arguments: -t nfs broken.example.com:/mldat /var/lib/kubelet/pods/9a9f94e3-f13e-4902-b9c9-56f6e0bb07b0/volumes/kubernetes.io~nfs/training-data-volume
# Output: mount.nfs: mounting broken.example.com:/mldat failed, reason given by server: No such file or directory

$ ./cluster-volumes.sh get
# NAME                  TYPE  CONTAINER_PATH                    SERVER        SERVER_PATH                    READONLY  ALLOW_VENV
# home-volume           nfs   /cb/home                          10.254.152.8  /home                          false     false
# training-data-volume  nfs   /cb/ml                            10.254.152.8  /ml                            false     false
# user-venv-volume      nfs   /cb/tests/cluster-mgmt/user-venv  10.254.152.8  /tests/cluster-mgmt/user-venv  false     true

# now volume mounts referencing volume training-data-volume can be passed to run.py
```

An example usage of this volume in run.py is demonstrated below.

```python
from cerebras.appliance.cluster_config import ClusterConfig

cluster_config=ClusterConfig(
    python_paths=["/cb/tests/cslogs/cluster-mgmt/mbx/e2e/client/dependencies"],
    mount_dirs=["/cb/ml", "/cb/tests/cslogs/cluster-mgmt/mbx/e2e/client/dependencies"],
    ...
)

...
```
