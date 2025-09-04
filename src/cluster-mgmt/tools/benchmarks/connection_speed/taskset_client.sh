#!/bin/sh
# $1: local ip to bind
# $2: remote ip to connect to
# $3: number of processes to run
# $4: starting core for CPU binds
local_ip=$1
peer_ip=$2
np=$3
firstcore=$4
#dir control the traffic direction
#dir="-R": from server side to client side
#dir="" : from client side to server side
dir=""
#size is the message size to send
size=131072
#
for i in $(seq 1 1 $np)
do
        let "core = $i + $firstcore - 1"
        taskset -c $core iperf3 -c $peer_ip -p 500$i -B $local_ip $dir -Z -N -O 5 -t 100 -i 20 -l $size --title "stream-$i" -P 1 > log-$3.$i-$$ &
        pid[$i]=$!
        pidstat 2 -u -p ${pid[$i]} > cpu-$3.$i-$$ &
        pidstat 2 -r -p ${pid[$i]} > mem-$3.$i-$$ &
        echo "[client.sh($$)]: Launching iperf3 stream-$i on core $core w/pid(${pid[$i]}) ..."
done
for i in $(seq 1 1 $np)
do
        wait ${pid[$i]}
done
echo "Done."
exit
