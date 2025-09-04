# Deployment recipe for mb-systemf159

### Get inventory for the cluster and cluster name

- Switches and their roles, nodes and their roles, systems.
- This is scraped from various slack chats and put into info.txt

### Get a prefix size from the cluster sizing spreadsheet

- At least a /22 for just the 100G

### Allocate a prefix from the L3 allocation spreadsheet

- `10.254.80.0/22`
- `128 ASNs starting at 65001.768`


### Get uplink info
- Get the switch from IT
- Log into both aw switches and use LLDP to get the uplink information
  - Ex: `show lldp neighbor Ethernet 25/1 detail`

# - Get the uplink switch ASN from the running config (4259905546)
# - `eth32/1 on AW switch to sc-r11r11-100gsw-bot`

```
sc-r11rb14-100gsw Ethernet32/1 -> sc-r11r11-100gsw-bot Ethernet9/1
```

### Allocate uplinks from the management prefix in the L3 allocation spreadsheet

- `10.250.1.94/31`


### Create a new network configuration

```
python -m network_config.tool new_config \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -n 'mb-systemf159'
```

### Add the cluster environment to the network configuration

- All of `10.0.0.0/8` is the 100G network

```
python -m network_config.tool environment \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -p '10.254.80.0/22' \
    -e '10.0.0.0/8' \
    -a 65001.768,128
```

### Build the allocation extents from the prefix and cluster sizing spreadsheet.
- AW switch prefix addrs (256)

```
python -m network_config.tool allocate_tiers \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -p '10.254.80.0/22' \
    --aw_size 256 --aw_count 1 \
    --vip_count 1
```

### Add exterior switch

```
python -m network_config.tool add_exterior_switch \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -n sc-r11r11-100gsw-bot \
    -v arista \
    -M 9000 \
    -a 4259905549 
```

### Add switches

```
python -m network_config.tool add_switch \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -n sc-r11rb14-100gsw \ 
    -v arista \
    -t AW -p 0

### Add systems

```
for name in systemf159; do
    python -m network_config.tool add_system \
        -c ~/ws/l3/mb-systemf159/network_config.json \
	-n "$name"
done
```

### Add nodes

- Since the list came from various chats instead of the mb database, we're building on the command line.

```
python -m network_config.tool add_node \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -s management_nodes \
    -n sc-r11rb14-s1

python -m network_config.tool add_node \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -s user_nodes \
    -n sc-r11rb14-s9

for node in sc-r11rb14-s2; do
    python -m network_config.tool add_node \
        -c ~/ws/l3/mb-systemf159/network_config.json \
        -s worker_nodes \
	-n "$node"
done

for node in sc-r11rb14-s{3..8}; do
    python -m network_config.tool add_node \
        -c ~/ws/l3/mb-systemf159/network_config.json \
        -s memoryx_nodes \
	-n "$node"
done
```

### Populate exterior connections ("uplinks")

- Copy and paste the uplinks out of the spreadsheet into a text file (`exterior.txt`).

```
python -m network_config.tool add_exterior_conn \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -i ~/ws/l3/mb-systemf159/exterior.txt
```

### Collect LLDP information from the workers and switches

```
python -m network_config.tool node_tasks \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -p "$NODE_PASSWORD" \
    lldp

python -m network_config.tool switch_tasks \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -p "$SWITCH_PASSWORD" \
    lldp
```

## Populate the system interface connections based on the switch tier

- Since this is a single system cluster, there are no BR switches. All CS2 ports go to the AW switch. Inspect the resulting configuration.

```
python -m network_config.tool system_iface_from_tier \
    -c ~/ws/l3/mb-systemf159/network_config.json
```

### Run a sanity check

```
python -m network_config.tool sanity \
    -c ~/ws/l3/mb-systemf159/network_config.json

Wrong interface count: sc-r9rb8-s15 : 5
```

### Deal with the issue with sc-r9rb8-s15

- Tool missing. Hand edit the network config to add the missing interface to the switch and let Vivek know about it.
- Re-run sanity.


### Run the placer

- Also spot check the resulting configuration

```
python -m network_config.tool placer \
    -c ~/ws/l3/mb-systemf159/network_config.json --no-xconnect
```

### Calculate pod prefix size

```
python -m network_config.tool update_overlay_prefix_size \
    -c ~/ws/l3/mb-systemf159/network_config.json
```

### Generate the uplink configuration for the exterior switch

```
python -m network_config.tool exterior_switch_cfg \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -n sc-r11r11-100gsw-bot 
```

### Apply the uplink configuration to the exterior switch

- Older versions of Arista may have different syntax.
- Look at the old configuration and clean up existing interfaces.
- Step through the config one section and line at a time.


### Build and check the configuration files
- Audit at least one of each type of node
- Check the systems and switches

```
python -m network_config.tool build_configs \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -o ~/ws/l3/mb-systemf159/out
```

### Deploy the switches and inspect them

- Look at the running configuration and learned routes.

```
python -m network_config.tool switch_tasks \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -p "$SWITCH_PASSWORD" \
    -b ~/ws/l3/mb-systemf159/out \
    upload
```

### Deploy systems and check them

```
python -m network_config.tool system_tasks \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -b ~/ws/l3/mb-systemf159/out \
    upload

python -m network_config.tool system_tasks \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    system_show

python -m network_config.tool system_tasks \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    software_show

python -m network_config.tool system_tasks \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    activate
```

### Deploy nodes, reboot, and check

- Check at least one of each type

```
python -m network_config.tool node_tasks \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -p "$NODE_PASSWORD" \
    -b ~/ws/l3/mb-systemf159/out \
    upload

python -m network_config.tool node_tasks \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -p "$NODE_PASSWORD" \
    uptime

python -m network_config.tool node_tasks \
    -c ~/ws/l3/mb-systemf159/network_config.json \
    -p "$NODE_PASSWORD" \
    reboot
```
