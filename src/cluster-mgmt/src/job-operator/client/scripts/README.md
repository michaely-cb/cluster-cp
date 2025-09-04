# Scripts

This directory contains scripts to generate the code for clientset, informers, and listers
by using [k8s.io/code-generator](https://github.com/kubernetes/code-generator).

Since the code-generator does not support Go modules, the scripts include a Dockerfile and
a bash script to call the generator script inside a Docker container.

Warning: since the dockerfile uses `go get <pkg>` without pinning the version,
your generated code will be for the latest k8s API. You should manuall delete
the `Transform` functions in the `informers` to make it compatible with
job-operator. Run `git diff` against a previously generated client to understand
the difference.

TODO: use clientgen with a pinned version