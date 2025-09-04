# k8 Initialization

Ensure that /opt/cerebras/cluster/cluster.yaml is up to date prior to starting
either flow.

You may need to reset the state of the cluster prior to starting. To do so,
run

```
./k8_init.sh lists_teardown_targets
./k8_init.sh teardown_cluster
```

Also note that running `./csadm.sh install manifest.json`
will automatically run the reconcile flow so long as `k8s/` component was bundled
with the package

## Single Controlplane Node

```
./k8_init.sh reconcile
```

## Multiple Controlplane Node

First, ensure that `cluster.yaml::properties` is updated to include the following

```
properties:
  managementVIP: x
  managementASN: x
  managementRouterIP: x
```

Then run:

```
./k8_init.sh reconcile
```

## HA Prereq's

HA requires that routers are set up to allow controlplane nodes to BGP peer.
Some general tips are below but note that each cluster may have its particularities
which prevents easy automation/guides.

Note that this guide is for setting up single-box clusters. If you're working in
a multibox cluster where there is a topology of BR/memx switches, then do not
configure anything manually but instead use the ASNs/switch config from the
network.json file for 100G. For 1G, consult IT.

### 1G Network

You can discover which switch a node is connected to using `lldpctl`.

Then ensure the 1G router(s) allow peering with the mgmt nodes by ssh'ing into
the switch as admin user:

```
# check bgp is enabled
show ip bgp

# enable bgp if not enabled
enable

# you may need to add an interface to the vlan hosting the mgmt nodes
config
  int vlan XXX
    ip address x.x.x.x/24

config
  ip routing
  router bgp 65999
    bgp listen range x.x.x.0/24 peer-group mgmt-node remote-as 65999

    # delete old neighbors if needed
    show active
    no neighbor <neighbor>

# save config changes
write memory
```

### 100G Network

Similar to 1G, you'll need to enable bgp on the 100G layer.