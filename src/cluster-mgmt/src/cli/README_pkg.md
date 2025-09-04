# Package Installation

For a customer deployment, all software components will be packaged into a single
tarball. Inside this tarball, the following are a few top-level files that drive
the installation.

## `manifest.json`

This file includes the manifest of an installation, which defines the entry-point
deployment script for each component.

The `manifest.json` file is created during the package step.

## `csadm.sh`

Cluster admin is the top-level script for cluster installation. The command to
install the cluster-pkg is as follows:

```sh
/bin/bash csadmin.sh install manifest.json
```

# Cluster Configuration

Differences in configuration from one customer cluster to another should be
minimal. However, there are some variables we cannot avoid customizing. Their
meaning and how to set these variables is given below:

## cluster.yaml

This is the main configuration file for a cluster. It is generated during early
cluster bring up using the cerebras-internal tooling via this command:

```sh
./cs_cluster.py cluster create-config
```

This generates the cluster configuration according to what hardware is found
in the devinfra db entry OR based on the given net.json file that contains
similar information.

You will need to manually add the properties specific to the cluster if there
are any. Locate or add the `properties` field to the cluster.yaml file as
shown below:

```yaml
kind: cluster-configuration
name: example-corp
properties:
  serviceDomain: cerebras.example-corp.com
  alertSmtp: cerebras.com
systems: [...]
nodes: [...]
```

* `.name` (Required): the name of the cluster.

* `properties.serviceDomain` (Required for customer clusters): the domain for a
  service. This value is combined with `cluster_name`, to construct domains for
  `cluster-server`, `private-registry`, `prometheus`, `grafana`, `alert-manager`
  as well as the domains present in TLS certs. Defaults to cerebras.com if not
  set.

* `properties.alertSmtp` (Not required): The SMTP email address to receive
   prometheus alerts.

This file is created during the package step, and the values will be empty.
