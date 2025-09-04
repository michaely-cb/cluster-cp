# cb-cluster-check

A simple and flexible cluster tool to check for misconfigurations in the Cerebras Wafer-Scale Cluster and flag for potential config deviations from the POR. This tool can also be enhanced as a online monitoring tool through crontab configuration. Individual system checks and the history of configs can be found at /opt/cbmon/var

'cb-check' prints summary report or checks the abnormal tasks. User can choose to confirm the status is abnormal or a new normal.

'cb-linux' with the hostname prints md5 checksum of the passwords, keys along with the versions of kernel, kubernetes, memory capacity, networking interfaces, RAID health, Root file system usage, mount points and permissions. It also supports custom commands.

'cb-ssh' is another parallel ssh based on GNU parallel.

'cbmon' runs a sub-command specified in crontab, compares its output with the standard file(s) under the TASK directory under var/, records the history, and sends out email alert if user specifed so in /etc/cbmon.conf if the output is different. This option is disabled by default and can be configured at a later date if we need it to do more online/active monitoring of sorts.


## Installation Steps

Install dependencies such as parallel and bc
'yum install -y parallel bc'

Extract the tar ball in /opt/

Place the network.json (Tiering file) in the folder /opt/cbmon


## Usage

Runs the cb-linux checks on all the nodes of the cluster and compares the diff from POR(Plan Of Record) configuration with the current cluster and prints the diff-report
'sh cbmon-cluster-check-script.sh'

These examples show individually how to run these commands on a single node
'cb-linux localhost -d 99P'
'cb-linux localhost -d 1P:/'
'cbmon -t test/localhost -- cb-linux localhost -d 99P'
'cbmon -t test/localhost -- cb-linux localhost -d 1P:/'
'cb-check -ar'	#print summary report for all 
'cb-check -a'	#check abnormal task


## Help

use '-h' with each command

## Contribute

Contributions are always welcome!

## Copyright

Developed by [Manhong Dai](mailto:manhongdai@gmail.com)

Copyright © 2002-2022 University of Michigan 

Copyright © 2022 KLA, Corporation

License [GPLv3+](https://gnu.org/licenses/gpl.html): GNU GPL version 3 or later 

This is free software: you are free to change and redistribute it.

There is NO WARRANTY, to the extent permitted by law.

## Acknowledgment

Ruth Freecban, MPH, former acbinistrator of MNI, UMICH

Fan Meng, Ph.D., Research Associate Professor, Psychiatry, UMICH

Huda Akil, Ph.D., Director of MNI, UMICH

Stanley J. Watson, M.D., Ph.D., Director of MNI, UMICH

Prashanth Thinakaran, KLA

Raghuram Bondalapati, KLA
