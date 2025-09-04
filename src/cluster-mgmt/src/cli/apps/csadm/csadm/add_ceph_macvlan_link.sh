#!/bin/bash

# Add a link to allow the host to communicate with the Ceph cluster via a macvlan interface.
# Refer to https://rook.io/docs/rook/latest/CRDs/Cluster/network-providers/#macvlan-whereabouts-node-static-ips
#
# This link is only added when ceph is running with multus networking, and public-shim link is not present.

set -euo pipefail

if ! kubectl -n rook-ceph get cephcluster &>/dev/null; then
  echo "ERROR: Ceph cluster not found in rook-ceph namespace."
  exit 1
else
  network_provider=$(kubectl -n rook-ceph get cephcluster -ojson | jq -r '.items[0].spec.network.provider // empty')
  if [[ "$network_provider" != "multus" ]]; then
    echo "INFO: Ceph cluster is not using multus networking, skipping macvlan link creation."
    exit 0
  fi
fi

if ip link show public-shim &>/dev/null; then
  echo "INFO: public-shim link already exists, skipping macvlan link creation."
  exit 0
fi

# Find the IP range from the multus configuration.
multus_config_file="/etc/cni/multus/net.d/multus-data-net.conf"
if [[ ! -f "$multus_config_file" ]]; then
    multus_config_file="/etc/cni/net.d/multus-data-net.conf"
    if [[ ! -f "$multus_config_file" ]]; then
        echo "ERROR: Multus configuration file not found at both /etc/cni/multus/net.d/multus-data-net.conf and $multus_config_file."
        exit 1
    fi
fi

ip_range=$(jq -r '.ipam.range // empty' "$multus_config_file")
ip_start=$(jq -r '.ipam.range_start // empty' "$multus_config_file")
nic=$(jq -r '.master // empty' "$multus_config_file")
if [[ -z "$nic" ]] || [[ -z "$ip_range" ]] || [[ -z "$ip_start" ]]; then
  echo "ERROR: No '.master', or '.ipam.range', or '.ipam.range_start' found in multus configuration: ${multus_config_file}."
  exit 1
fi

convert_ip_to_int() {
    local ip="$1"
    local o1 o2 o3 o4
    IFS=. read -r o1 o2 o3 o4 <<< "$ip"
    local ip_int=$(( (o1 << 24) + (o2 << 16) + (o3 << 8) + o4 ))
    echo "$ip_int"
}

convert_int_to_ip() {
    local ip_int="$1"

    # Convert back to dot-decimal notation
    printf "%d.%d.%d.%d\n" \
        $(( (ip_int >> 24) & 0xFF )) \
        $(( (ip_int >> 16) & 0xFF )) \
        $(( (ip_int >> 8) & 0xFF )) \
        $(( ip_int & 0xFF ))
}

cidr_min_ip() {
    # Usage: cidr_min_ip 10.250.140.0/22
    local cidr="$1"
    local ip="${cidr%/*}"
    local mask_bits="${cidr#*/}"

    local ip_int=$(convert_ip_to_int "$ip")
    local mask=$(( 0xFFFFFFFF << (32 - mask_bits) & 0xFFFFFFFF ))
    local network_int=$(( ip_int & mask ))

    convert_int_to_ip "$network_int"
}

ip_range_min=$(cidr_min_ip "$ip_range")
ip_range_min_int=$(convert_ip_to_int "$ip_range_min")

mask_bits="${ip_range#*/}"

# Find the 100G IP that is not been used by any other interface. We have a convention that some 100G IPs are reserved
# for physical interfaces. These IPs are before `ip_start`. We will choose one that is not used in that range, starting
# from `ip_start-1`.
ip_start_int=$(convert_ip_to_int "$ip_start")
test_ip_int=$(( ip_start_int - 1 ))
while [[ "$test_ip_int" -ge "$ip_range_min_int" ]]; do
  test_ip=$(convert_int_to_ip "$test_ip_int")
  echo "INFO: Testing IP: $test_ip"
  if ! ping -c 5 "$test_ip"; then
    echo "INFO: Found available IP for macvlan link: $test_ip"
    break
  fi
  test_ip_int=$(( test_ip_int - 1 ))
done

if [[ -z "$test_ip" ]]; then
  echo "ERROR: No available IP found in the range $ip_range."
  exit 1
fi

# Create the macvlan link
echo "INFO: Creating macvlan link with IP $test_ip"

trap 'ip link delete public-shim' ERR SIGINT SIGTERM

ip link add public-shim link "$nic" type macvlan mode bridge
ip addr add "$test_ip/$mask_bits" dev public-shim
ip link set public-shim up

# validate that the route exists now
if ! ip route show | grep -q "$ip_range dev public-shim"; then
  echo "ERROR: Route for public-shim link not found."
  exit 1
fi
