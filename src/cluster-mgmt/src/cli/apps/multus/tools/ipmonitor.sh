#!/bin/bash

LOG_FILE="/tmp/ipmon.log"
echo "MON_INIT $(date --utc +'%Y-%m-%dT%H:%M:%S.%N%z')" > $LOG_FILE
ip -o -d addr show >> $LOG_FILE
ip netns list | awk -F '[()]' '{print $1, $2}' | while read -r ns id; do
    ns_id=$(echo "$id" | awk '{print $2}')
    echo "MON_INIT_NSID: $ns_id" >> $LOG_FILE
    ip -n "$ns" -o -d addr show >> $LOG_FILE
    echo "" >> $LOG_FILE
done
echo "MON_BEGIN" >> $LOG_FILE
ip monitor all-nsid address | while read -r line; do
    echo "$(date --utc +'%Y-%m-%dT%H:%M:%S.%N%z') $line" >> $LOG_FILE
done