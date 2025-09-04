# Qualification test for MemoryX nodes

This directory contains the qualification test for memoryX nodes. Currently,
this directory only contains the wrapper scripts for the actual test. It assumes
that the actual test are packaged in a tarball named `test_artifacts.tar.gz` file.
This tarball is not included here yet.

# Assumptions

This test assumes that k8s and cluster-mgmt components are installed. The cbcore
image has been loaded into the cluster, and `cluster-server` has been configured
with the cbcore image.

# `test_memx.sh`

This is the entry point for the test. It should be run from the management node.

It finds all the memoryX nodes in the cluster by finding all k8s nodes labelled
with `k8s.cerebras.com/node-role-memory`, and creates a pod on each memoryX node,
which runs the actual test. This script will copy the test artifacts to all
memoryX node before creating the pod. The pod will use cbcore image. At the end,
this script collects the test results from all memoryX nodes back to the
management node.

To run this script, do the following:
```
/bin/bash test_memx.sh
```

The test results include 2 files from each memoryX nodes (this might change
depending on the final memoryX test scripts):
```
dbg_wgt_<node>.err
memx-test-<node>.out
```

# `test_one_memx.sh`

This is the wrapper script to run inside the pod. It calls to the actual test
script inside the tarball `test_artifacts.tar.gz`, and stores the output of
the tests to a file. This file will be collected by `test_memx.sh` at the end
of the test.
