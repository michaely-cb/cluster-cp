Copyright 2002, Cerebras Systems, Inc. All rights reserved.

# Missing features and well known bugs.

This toolchain is a work in progress. As such, there are both siginificant missing features as well as minor issues.

## Significant missing features or major bugs

### Render a graph from the connection map

A graph, ideally browsable through a tool of the stored connection map would be really useful to see how the cluster is put together.


### Additional switch vendors and types.

The toolchain only has support for Arista switches. It is missing support for Mellanox (incl HPE branded) switches as well as other types which may be in use with Cerebras Clusters soon.

### Switch configuration needs to be complete.

The switch templates generate configuration fragments and not the whole switch configuration.

The benefit of the fragments is that they can be applied live to a running switch. This was valuable during template development but leads to problems when switches move or change roles.

Now that the L3 templates are mature. We should be generating whole switch configurations and reloading onto them to eliminate stale configuration.

### The L3 tools only handle a single BR switch tier.

A single BR switch tier can support multiple tiers of BR nodes. It just refers to the number of levels of the BR switch hierarchy.

For example, 32 port BR switches can deploy an 8 system cluster with a single BR switch tier. Using 64 port switches allows up to a 16 system cluster and 128 port switches allws up to a 32 system cluster.

To grow beyond those limits requires additional architecture work for how BGP ASNs are distributed as well as the connectivity plan.

### Collapse node types and switch types into fields in entries in nodes and switches sections.

This package was originally part of a larger toolchain that treated MemoryX and SwarmX nodes differently. Since then, more node types have been added and how they are handled was unified.

That should be reflected in how nodes are treated in the database. Different node types should just become a field in the node object and they should be coalesced into a "nodes" section.

External switches have different schema rules than interior switches and are placed in their own section. It should be possible to unify their schema and place them into the same section as the other switches.

### Minimal sanity checking

The tools sanity check configuration in a few ways:
1. Schema validation for the networking configuration
2. Internal checking in the code for data types and missing fields.
3. Some rule based configuration analysis in the `sanity` subcommand.

Item 1 is fairly robust.

Item 2 tends to result in deep exceptions being thrown which are hard for a tools user who is not familiar with the code to understand and deal with.

Item 3 needs a lot more work.

There are also external efforts to generate test and validation for the cluster.

### L3 tools mostly probe for connections.

The toolchain uses LLDP to probe for connections for nodes due to having been built in environments where we were building ad-hoc clusters that didn't follow the production rules.

Critically missing is the ability to input a plan for how connections will be done. This would allow L3 integration to begin much earlier in the deployment as well as be able to use the networking tools to sanity check the deployment.

For systems, it has the ability to generate a map of CS 100G connections to switch based on the switch tier.

For exterior uplinks, there's the ability to cut and paste from a spreadsheet.

For interior cross connections, we also need the ability to input a connection plan for the same reasons we need one for nodes.

### Expertise needed to pilot the tools

It's not clear how much this workflow can be simplified to run without knowing a lot about how the L3 network is put together but that does limit the usability of these tools.

### Half-implemented features were left out for now.

#### Network planner.

The planner takes a high level description of the cluster (ex: "a 16 system CS cluster with 64 port BR switches") and produces a manifest of the required resources (ex: the prefix request).

A spreadsheet currently exists that provides guidance on the sizing of the prefixes for various networks but it relies on some large calculations which would be esaier to follow if they were in Python.

#### Object model from the JSON schema.

The JSON schema fully describes the structure of the network configuration database. Ideally, this should result in a static set of Python classes. Without those classes, the internal code operates on dictionary keys and there's no ability to push key consistenty checking all the way out to the pylint check where it would be very useful.

A subset of the schema is provided in order to operate with the JSON schema itself and manage some of the data types. This is not very OO since you can't bundle all of this into the data object.

## Minor issues

### Different switch OS releases have minor changes to the syntax of commands.

We may need more than just the vendor baked into the templates.

### Additional utilities are needed to forward port old configurations.

As the tools and their role evolved, old parts of the schema were left behind in order to make the new schema clear and easy to follow.

This has resulted in a number of clusters with configuration files that cannot be used by the current toolchain.

A set of utilities is needed for forward-port configuration files around schema changes. These will largely have to work on the JSON serialized object directly and either wholly bypass the JSON schema or only check the schema on save.

### Static uplink routes not re-advertised

The route filter on static routes prevents readvertising routes from a static uplink.

This has been worked around by hand by adding the prefix to the vip_addrs filter.

The tools should generate a template for static route readvertisement that includes both the VIPs and exterior prefixes.
