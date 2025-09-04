#!/usr/bin/bash

ADDR="{{ ansible_default_ipv4.address }}"
IP=$(ip a | grep $ADDR | sed 's/^[[:space:]]*//' | cut -d" " -f 2)
IFNAME=$(ip -j a | jq -r --arg ADDR $ADDR '.[]|select(.addr_info[].local == $ADDR)|.ifname')
CONN=$(nmcli -g general.connection device show "$IFNAME")
GATEWAY=$(nmcli -g ipv4.gateway con show "$CONN")

CURRENT_METHOD=$(nmcli -g ipv4.method con show "$CONN")
CURRENT_ADDRESS=$(nmcli -g ipv4.addresses con show "$CONN")
if [ "$CURRENT_METHOD" == "manual" ] && [ "$CURRENT_ADDRESS" == "$IP" ]; then
    echo "IP address is already static and configured as desired."
    exit 0
fi

nmcli con mod "$CONN" ipv4.address "$IP"
nmcli con mod "$CONN" ipv4.gateway "$GATEWAY"
nmcli con mod "$CONN" ipv4.method manual
nmcli con down "$CONN" && nmcli con up "$CONN"
