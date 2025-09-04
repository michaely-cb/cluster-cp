#!/bin/bash
set -e

ibdevs=$(ls /sys/class/infiniband/)
for d in $ibdevs; do
    if [[ $d == "mlx"* ]]; then
        ports=$(ls /sys/class/infiniband/$d/ports/)
        for port in $ports; do
            ethdev=$(cat /sys/class/infiniband/$d/ports/$port/gid_attrs/ndevs/0 2> /dev/null)
            if [[ ! -z $ethdev ]]; then
                mlnx_qos -i $ethdev --pfc=0,0,0,0,0,1,0,0
                mlnx_qos -i $ethdev --trust=dscp
                mlnx_qos -i $ethdev --dscp2prio=set,40,5
                mlnx_qos -i $ethdev --prio_tc=1,0,2,3,4,5,6,7
            fi     
        done
    fi
done
