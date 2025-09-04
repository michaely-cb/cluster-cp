## OS Install and Provisioning

This directory contains Ansible playbooks that drive the OS provisioning processes

### Process Overview (WIP)

The following is a rough outline of the provisioning process for a new deployment

1. plug in the management node installer USB and boot into alpine
1. install the management node OS onto the disk and reboot
1. configure management node networking
1. PXE boot connected workers into Rocky
1. kickstart process installs OS to disk and reboots workers
1. configure workers
1. install cluster software on all nodes
1. run validation tests

While the first 2 steps are manual, the rest are driven by Ansible running from the management node

### Running the full deployment playbook

The top-level playbook that drives the entire process is `main.yml`, which sets the order that individual playbooks are run. See the documentation within the playbook files for details.

Prior to running, configure site and hardware-specific parameters in the following files/directories (documentation for each parameter included in the files):

- `host_vars/mgmt-node`
- `group_vars/all`

When ready, run `ansible-playbook main.yml` to start the process.

### Brief Ansible Command Guide

A short list of commonly-used ansible playbook commands. Flags are not order-dependent and multiple can be used together.

- `ansible-playbook [file]`: run a playbook file
- `ansible-playbook [file] -t [tags]`: run only the tasks with the given tags (tags given as comma-separated, e.g. `-t tag1,tag2,tag3`)
- `ansible-playbook [file] -l [hosts/groups]`: run on only the given hosts/host groups, which are defined in `inventory/` files (given as comma-separated and can use `!` for exclusion, e.g. `-l group1,host1,host2,!host3`)
- `ansible-playbook [file] -k`: prompt for an SSH password when connecting to hosts - can be useful if the SSH key files were lost/changed
- `ansible-playbook [file] --list-tasks`: list tasks to be run without running them
- `ansible-playbook [file] --start-at-task "[task name]"`: skip to the given task - can be useful to resume a play after addressing a failure (note that the task name should be in quotes, as they usually contain spaces)
