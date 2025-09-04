This folder exists for the `cs_cluster.py package` calls in the Makefile.
We want the nightly build to create a pkg file that contains all the artifacts
instead of the reduced set of artifacts for KIND so we purposefully point the
`--cluster` parameter at this fake cluster which importantly does not look like
a KIND cluster.

https://cerebras.atlassian.net/browse/SW-82823