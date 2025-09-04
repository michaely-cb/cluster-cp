Instructions on how to update FPGA firmware versions on the appliance.

# Background

During the transition period to appliance model / weight-streaming mode, customers may have hybrid clusters where some jobs run in weight-stream mode and some run in pipelined mode. As of release 1.4, the fpga firmware version required to run weightstreaming mode on the system is `ampere` and the required version for pipeline is `faraday`. Eventually ampere will support pipelined mode execution and the need to swap firmwares will go away, however during 1.4 firmware swaps are necessary in the case the customer switches between execution modes.

This directory contains scripts to update the kubernetes cluster components to respect the change in execution mode. This procedure is intended to be performed by a user familiar with system deployment such as an IT administrator.

# Firmware Swap Instructions

1. Ensure there are no jobs running on the target cluster.
2. Change firmware version on the target system
    1. `ssh root@<system>` ssh into the target `system`
    2. `cs config execmode show --output-format=json` check that the execmode is not the target execmode
    3. `cs system standby`
    4. `cs config execmode setup`
    5. `cs system activate`
3. Inform the cluster management components of the change
    1. `scp sync-cluster-config.sh root@<control-plane-node>:/tmp/sync-cluster-config.sh` copy the update script to the control plane node.
    2. `ssh root@<control-plane-node> -c /tmp/sync-cluster-config.sh` execute the update script on the control plane node
4. The cluster has been updated.


