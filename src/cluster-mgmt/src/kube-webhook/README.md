# Kube Webhook

A simple webhook server.

Adapted from code in this [tutorial](https://github.com/pluralsight-cloud/Deploying-Custom-Admission-Controllers-with-Terraform/blob/6960ccd7d347fd9a01377e437f6a375b9c9b78db/HOL_2/mutating-webhook/cmd/root.go).

## Mutate Ceph Pods

It hosts a single webhook that mutates ceph pods to change the ceph net-attach-def
from the chart's cluster-wide net-attach-def to one which has rack-aware config.
This is needed due to a limitation of rook-ceph's multus integration where
rook-ceph assumes

1. a single net-attach-def for every ceph pod
2. a `ipam.range` value is parsable from the net-attach-def object

Since our ceph pods span multiple racks, and therefore have multiple subnet
`ipam.range` values, we need different configs for each node. We achieve
this by deploying a net-attach-def with an empty config body in the CRD which
instructs multus to lookup the config on the host machine at /etc/cni/. However,
rook-ceph code parses the config-body of the net-attach-def CRD to determine
the range associated with the multus interface and therefore the config body
must not be empty. We therefore deploy rook-ceph with net-attach-def/A and
mutate the ceph pods to use net-attach-def/B at runtime.

This works because net-attach-def/A's range spans a range that covers all the
net-attach-def/B subnets. Ceph needs this range information so that when ceph
pods start, they can search their network interfaces and [bind to the interface](https://github.com/ceph/ceph/blob/05e449f9a2a65c297f31628af8f01f63cf36f261/src/common/pick_address.h#L37C1-L37C1)
which falls within the range specified by A, even though the interface will have
been configured by B.

References:
- [ceph multus integration](https://github.com/rook/rook/blob/master/design/ceph/multus-network.md#proposed-crd-changed)
- [rook-ceph multus code](https://github.com/rook/rook/blob/1913413ddf1251822ff17b2a94eabdc30997d510/pkg/operator/ceph/cluster/cluster.go#L313)


## Add Ingress Annotation

Upgrading nginx from 1.3.0 to 1.12.1 requires special handling due to a breaking change
on server-snippet, which is not supported any more in 1.12.1. This breaks our grpc
settings in our use cases. Instead in 1.12.1, annotations are added to ingress objects to
update grpc settings. This webhook is used to add the annotations to support the case when
job-operator is not updated with the changes to use the new annotations.

# Local Testing
If testing locally, whenever changes are made to the go code, update the kube-webhook tag in the
Makefile and codebase to a testing tag such as `<user>-dev-<yyyymmdd>-<version>` to ensure deploying the image
uses your local copy.

# Updating

If you update the go code, please do the following
1. Update the kube-webhook tag in the Makefile & codebase (by grepping for kube-webhook) with `webhook-<yyymmdd>-<version>`
2. build and push the new image to ECR (push before merging so that canaries can validate the change)
```
make
make push
```

3. If additional changes are needed, repeat steps 1 & 2, bumping the version
4. Merge after approved
