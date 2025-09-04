# kube-user-auth

This is a user authentication package, which provides the following functions:

* Generate a user token based on the given username, uid, gid, and groups.
* Validate a user token and return user info, such as username, uid, gid and groups.

It also creates a binary to generate a user token based on the user who is invoking this binary.
More details can be found in this [link](https://cerebras.atlassian.net/wiki/spaces/runtime/pages/2741469192/User+Authentication).

In this package, the token is generated based on a shared secret key. This secret key needs
to be handled with extra care. The cluster deployment will package this secret in the usernode
package, and the usernode installer will take care of properly setting up the permissions on
this file and also the binary. It is important that the usernode package is handled with care.
Only the root user should be able to read this package.
