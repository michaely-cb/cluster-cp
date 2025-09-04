#!/bin/sh
# $1: local ip to bind
# $2: number of processes to run
# $3: starting core for CPU binds
ip=$1
np=$2
for i in $(seq 1 1 $np)
do
	iperf3 -s -B $ip -p 500$i -i 20 &
	pid[$i]=$!
done
for i in $(seq 1 1 $np)
do
	wait ${pid[$i]}
done
echo done!
exit
