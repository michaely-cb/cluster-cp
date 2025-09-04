
# Running estimator + simfab in a 3-node Kubernetes cluster running in AWS EC-2 instances

This document only lists out a few unique pieces for running estimator + simfab
in a 3-node Kubernetes cluster running in AWS EC-2 instances.

The EC-2 instances were running Rocky Linux 8.5.

## Load job-operator and cbcore images

The images need to be loaded into all 3 nodes separately using `ctr` command:

```
sudo /usr/local/bin/ctr -n k8s.io images import --base-name job-operator:latest job-operator.tar

sudo /usr/local/bin/ctr -n k8s.io images import cbcore.joy-65eedb2.tar
```

## NFS mount point

NFS mount point is set up in the master node. The instruction is similar to [here](../minikube/README.md).