Copyright 2002, Cerebras Systems, Inc. All rights reserved.

# Cerebras Cluster 100G Network Toolkit

This python package is used to manage the 100G network in a Cerebras Cluster.

At a high level, that consists of:
1. Assembling the set of systems, switches and nodes that participate in the cluster.
2. Determining the connectivity map of the cluster.
3. Distributing subnets and interface addresses throughout the cluster.
4. Generating configuration for systems, switches and nodes.
5. Applying these configrations.

## Quick start

To run these tools, it is strongly recommended to already have familiarity with the Cerebras Cluster 100G L3 Networking architecture.

To run the CLI tool and get help: `python -m network_config.tool -h`

There are subcommands and sub-menus in the CLI.

## Additional documentation

- Open issues and outstanding feature requests: [docs/todo.md](./docs/todo.md)
- Cerebras Cluster 100G L3 Networking: [docs/l3.md](./docs/l3.md)
