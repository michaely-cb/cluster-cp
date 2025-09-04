# Authenticate and Authorize an existing user

## `authz-user.sh`

This script generates the tls key and certificate for a given user to authenticate the user,
and creates proper ClusterRole and ClusterRoleBindig in k8s to authorize this user.

Currently, this script only creates read-only access for a given user. Use this command
to create read-only access for a given user:

```
    authz-user.sh ${USER_NAME} read
```

A file `${USER_NAME}-config` will be generated in the same directory where this script is invoked.

## Where to store `${USER_NAME}-config`

This file can be put as `$HOME/.kube/config` under the user's home directory. After that,
the user can issue read-only queries to k8s APIs.
