Kind docker image for e2e testing

The intent of these scripts is to make the kind node look more like an IT
deployed node in colovore so that e2e tests can run in a meaningfully similar
env.

To add a newer version to ECR, edit the makefile to change the
image tag on the kind node to the next version. Then run these make
commands

```sh
make -C $GITTOP/src/cluster_mgmt/src/cli docker-kind-node-build
make -C $GITTOP/src/cluster_mgmt/src/cli docker-kind-node-push
```

Then, modify kind configuration files to use your docker image.
(grep codebase for `cerebras-kind-node` also)
- src/cluster_mgmt/src/cli/common/__init__.py
- src/cluster_mgmt/src/cli/clusters/kind-canary/kind.yaml
