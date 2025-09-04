# CLI

This directory contains the CLI scripts to manage the cluster management components.

## Quickstart

The CLI tool issues commands on a per-cluster basis. 

## Configuration

Cluster configuration files are stored under `clusters/<cluster>` locally and
`/cb/system_data/clusters` if running on your developer machine. If you use a cluster which is 
in both `/cb/system_data/clusters/<cluster>` and `clusters/<cluster>`, the local path 
`cluster/<cluster>` is always selected first.

The CLI needs to SSH to non-Kind (e.g. colo multibox clusters) to do some deploy commands. The 
configuration to SSH to machines in a cluster is created by `cs_cluster.py cluster signin` 
command and existing config is looked up first in `clusters/<cluster>/.ssh.conf` and then in 
`clusters/.ssh.conf`. This means that for most clusters, signing in should not be required.

### Setup

Ensure you have installed `python 3.7+`

Additionally, if you intend to build artifacts from source, you will need
- go 1.21+
- protoc
- docker

Then run these commands to set up your local env.

```shell
# optional! Remove any junk that might have been there from past builds and remove an old venv 
# if you really want a fresh start
make clean 
rm -rf venv

# This will initialize the venv
make test

# This changes your PYTHONPATH to look up cs_cluster's python deps
source venv/bin/activate

# Install dependencies (requirements.txt) and kind if needed
make dep-kind

# You can always inspect what commands are available
./cs_cluster.py --help
```

### KIND Cluster

The recommended way to bring up a KIND cluster is to follow the e2e test instructions later in this
document and use the option `--keep-cluster=true` with pytest. This will setup a kind cluster with
the recommended configuration.

```shell

# There's no need to use cluster signin for kind. Set the default cluster in cs_cluster.conf
python cs_cluster.py --cluster kind -v cluster use

# create cluster config map
python cs_cluster.py cluster create-config

# deploy all services
python cs_cluster.py -v deploy all

# deploy individual service with a specific image
python cs_cluster.py -v deploy cluster-service --image cluster-server:user-735863c
```

### Batcave Cluster

Batcave is one of the [experimental clusters](https://cerebras.atlassian.net/wiki/spaces/runtime/pages/2075197443/Experiment+Clusters).
It's useful for testing remote deployments.

```shell
# looks in clusters/batcave for configuration
python cs_cluster.py --cluster batcave -v cluster use

# you may have to signin (but only once)
python cs_cluster.py cluster signin --username=rocky --pem ~/.ssh/YOUR_PEM

# deploy job-operator only
python cs_cluster.py deploy job-operator
```

### Multibox

This flow shows how to configure and deploy to a new multibox cluster.

```shell
# you may be prompted to create a new cluster folder
python cs_cluster.py --cluster=multibox-1 -v cluster use

# this command will also scrape all the signin info, so you don't need to explicitly signin
# the --validate flag checks the generated configuration for errors
python cs_cluster.py -v cluster create-config --name multibox-1 --username root --password xxx

# creates cluster configmap, labels nodes for scheduling
python cs_cluster.py -v cluster create-config

# this command checks that a cluster's configuration matches the state of the k8s cluster
# use this to check for configuration drift, or incorrect hand-edits to the cluster
python cs_cluster.py -v cluster validate

# You can see help text and examples for commands using --help
python cs_cluster.py -v deploy job-operator --help

# deploy job-operator using an image from a nightly build (must be on a dev box)
python cs_cluster.py -v deploy job-operator \
  --image-file /cb/artifacts/release-stage/default/latest/components/cbcore/job-operator-*.docker

# deploy cluster-server using an image from a nightly build (must be on a dev box)
python cs_cluster.py -v deploy cluster-server \
  --image-file /cb/artifacts/release-stage/default/latest/components/cbcore/cluster-server-*.docker \
  --cbcore-image 171496337684.dkr.ecr.us-west-2.amazonaws.com/cbcore:1.4.0-202206281146-40-e985c3c0

# deploy all services. Use a remote directory in /tmp as a staging area for artifacts and config.
python cs_cluster.py -v deploy --remote-dir /tmp/$USER all
```

## Command: deploy

This directory contains scripts that handle the deployment of different cluster
management components.

### Assumptions

Here are a few assumptions:

* `containerd` must have been installed on the k8s nodes and the executable must be in the `$PATH`.
* The user used to SSH to the nodes must have sufficient permissions to use the ctr executable and
  other containerd processes.
* The environment where the deployment scripts are run must have a CPU architecture that is compatible
  to the target hosts. For example, we should not build the images on an ARM architecture, and then
  deploy the images on an x86 architecture.

### KIND Limitations

If you are deploying locally to a kind cluster, you will have to set up the nginx ingress 
manually [according to instructions here](https://kind.sigs.k8s.io/docs/user/ingress/).

## Directory Structure

### apps/<app>

The app folder for each app will contain base config and potentially helm charts/kustomize files 
required to create the k8s resource files for the application. This is intended to be base 
config which is overridden by configs in the `clusters/<cluster>` folder for each cluster, if 
required.

### clusters/<cluster>

Each cluster has its most up-to-date configuration in these folders. 

Note that although `.conf` files are stored in these folders, they are not checked-in to github 
since they contain sensitive information.

## Testing

Testing is done with pytest. There are unit tests and e2e tests. If you are running on an M1 mac,
you'll need to build the kind node image since the default ECR image is only built for x86.

```shell
# only required for mac
make docker-kind-node-build
export KIND_NODE_IMAGE=cerebras-kind-node:local
```

```shell
# run the unit tests
pytest tests --ignore tests/integration

# run the integration tests with a new cluster
pytest tests/integration

# run the integration tests with a new cluster, preserving it after the test is over
pytest tests/integration --keep-cluster=True

# run a specific integration test on an existing pytest cluster
pytest tests/integration/test_e2e_smoke.py --use-cluster=pytest-1656655839 -k image
```
