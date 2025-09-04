# Deployment recipe for multibox-21

Ref MB-19 for more details

This cluster is absorbing MB-21 and will re-use its allocations.

### Get inventory for cluster and cluster name

### Get a prefix size from the cluster sizing spreadsheet

### Allocate a prefix from the L3 allocation spreadsheet

- Reusing MB-20's allocation
- `10.250.96.0/20`
- `128 ASNs starting at 65001.896`

### Get uplink info

- `eth25/1 on each AW switch to sc-r9ra14-100gsw128-0`
- Re-using MB-20's uplinks

### Allocate uplinks from the management prefix in the L3 allocation spreadsheet

- `10.250.1.62/31`
- `10.250.1.64/31`
- `10.250.1.66/31`
- `10.250.1.68/31`

### Create a new network configuration

python -m network_config.tool new_config \
       -c ~/ws/l3/mb-21/network_config.json \
       -n 'multibox-21'

### Add the cluster environment to the network configuration

```
python -m network_config.tool environment \
    -c ~/ws/l3/mb-21/network_config.json \
    -p '10.250.96.0/20' \
    -e '10.0.0.0/8' \
    -a 65001.896,128
```

### Build the allocation extents from the prefix and cluster sizing spreadsheet.

- AW switch prefix addrs (512)
- BR switch prefix addrs (32)
- xconnects (48)

```
python -m network_config.tool allocate_tiers \
       -c ~/ws/l3/mb-21/network_config.json \
       -p '10.250.96.0/20' \
       --aw_size 512 --aw_count 4 \
       --br_size 32 --br_count 6 \
       --vip_count 4 \
       --xconn_count 48
```

### Add exterior switch

```
python -m network_config.tool add_exterior_switch \
       -c ~/ws/l3/mb-21/network_config.json \
       -n sc-r9ra14-100gsw128-0 \
       -v arista \
       -M 9000 \
       -a 4259905546
```

### Add switches

```
for switch in sc-r9rb{15,14,13,12}-100gsw64; do
    python -m network_config.tool add_switch \
           -c ~/ws/l3/mb-21/network_config.json \
           -n "$switch" \
           -v arista \
           -t AW -p 0
done

tier_pos=0
for switch in sc-r9rb{15,14,13}-100gsw32-{1,2}; do
    python -m network_config.tool add_switch \
           -c ~/ws/l3/mb-21/network_config.json \
           -n "$switch" \
           -v arista \
           -t BR -p "$tier_pos"

    tier_pos=$((tier_pos + 1))
done

```

### Populate exterior connections ("uplinks")

- Copy and paste the uplinks out of the spreadsheet into a text file (`exterior.txt`).

```
python -m network_config.tool add_exterior_conn \
    -c ~/ws/l3/mb-21/network_config.json \
    -i ~/ws/l3/mb-21/exterior.txt
```


### Add systems

```
for name in systemf{148,135,141,134}; do
        python -m network_config.tool add_system \
        -c ~/ws/l3/mb-21/network_config.json \
        -n "$name"
done

```

### Add nodes

```
for node in sc-r9rb15-s1; do
    python -m network_config.tool add_node \
        -c ~/ws/l3/mb-21/network_config.json \
        -s management_nodes \
        -n "$node"
done

for node in sc-r9rb15-s2 sc-r9rb{14,13,12}-s{1,2}; do
    python -m network_config.tool add_node \
        -c ~/ws/l3/mb-21/network_config.json \
        -s worker_nodes \
        -n "$node"
done

for node in sc-r9rb{15,14,13}-s19 sc-r9rb12-s15; do
    python -m network_config.tool add_node \
        -c ~/ws/l3/mb-21/network_config.json \
        -s user_nodes \
        -n "$node"
done

for node in sc-r9rb{15,14,13,12}-s{3..14}; do
    python -m network_config.tool add_node \
        -c ~/ws/l3/mb-21/network_config.json \
        -s memoryx_nodes \
        -n "$node"
done

for node in sc-r9rb{15,14,13}-s{15..18}; do
    python -m network_config.tool add_node \
        -c ~/ws/l3/mb-21/network_config.json \
        -s swarmx_nodes \
        -n "$node"
done

```

### Ensure all nodes have the required packages

```
python -m network_config.tool node_tasks \
    -c ~/ws/l3/mb-21/network_config.json \
    -p "$NODE_PASSWORD" \
    pkg_verify
```

### Collect LLDP information from the workers and switches

```
python -m network_config.tool node_tasks \
    -c ~/ws/l3/mb-21/network_config.json \
    -p "$NODE_PASSWORD" \
    lldp

python -m network_config.tool switch_tasks \
    -c ~/ws/l3/mb-21/network_config.json \
    -p "$SWITCH_PASSWORD" \
    lldp
```

## Populate the system interface connections based on the switch tier

```
python -m network_config.tool system_iface_from_tier \
    -c ~/ws/l3/mb-21/network_config.json
```

### Run the placer

```
python -m network_config.tool placer \
    -c ~/ws/l3/mb-21/network_config.json
```

### Calculate pod prefix size

```
python -m network_config.tool update_overlay_prefix_size \
    -c ~/ws/l3/mb-21/network_config.json
```

### Run a sanity check

```
python -m network_config.tool sanity \
    -c ~/ws/l3/mb-21/network_config.json
```

### Generate the uplink configuration for the exterior switch

```
python -m network_config.tool exterior_switch_cfg \
    -c ~/ws/l3/mb-21/network_config.json \
    -n sc-r9ra14-100gsw128-0
```

### Apply the uplink configuration to the exterior switch

- Since this cluster is taking over from MB-20, the ASNs may have been
  reallocated. Verify each interface and re-apply the BGP configs.


### Build and check the configuration files

```
python -m network_config.tool build_configs \
    -c ~/ws/l3/mb-21/network_config.json \
    -o ~/ws/l3/mb-21/out

```


### Deploy the switches and inspect them

- Since some of the switches are redeployed from MB-20, this required
  a hand audit to clean up stale static routes.

```
python -m network_config.tool switch_tasks \
    -c ~/ws/l3/mb-21/network_config.json \
    -p "$SWITCH_PASSWORD" \
    -b ~/ws/l3/mb-21/out \
    upload
```

### Deploy systems and check them

```
python -m network_config.tool system_tasks \
    -c ~/ws/l3/mb-21/network_config.json \
    -b ~/ws/l3/mb-21/out \
    upload

python -m network_config.tool system_tasks \
    -c ~/ws/l3/mb-21/network_config.json \
    system_show

python -m network_config.tool system_tasks \
    -c ~/ws/l3/mb-21/network_config.json \
    software_show

python -m network_config.tool system_tasks \
    -c ~/ws/l3/mb-21/network_config.json \
    standby

python -m network_config.tool system_tasks \
    -c ~/ws/l3/mb-21/network_config.json \
    activate

```
