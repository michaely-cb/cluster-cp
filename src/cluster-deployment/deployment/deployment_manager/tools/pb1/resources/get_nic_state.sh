#!/usr/bin/env bash

set -euo pipefail

# print CSV of physical network interface state
echo "name,operstate,current_speed_mbps,max_speed_mbps"
for iface in $(ls /sys/class/net/); do
  # Skip non-physical or virtual interfaces
  if [[ $(cat /sys/class/net/"$iface"/type) -ne 1 ]]; then
    continue
  fi
  if ! test -d /sys/class/net/"$iface"/device; then
    continue
  fi

  operstate=$(cat /sys/class/net/"$iface"/operstate 2>/dev/null)
  current_speed_mbs=$(cat /sys/class/net/"$iface"/speed 2>/dev/null)
  max_speed_mbps=$(ethtool "$iface" 2>/dev/null | grep -o -E -w '[0-9]+base.*' | xargs -n1 | cut -db -f1 | sort -g | tail -n1) ||:
  if [[ -z "$max_speed_mbps" ]]; then
    max_speed_mbps="0"
  fi

  echo "$iface,$operstate,$current_speed_mbs,$max_speed_mbps"
done