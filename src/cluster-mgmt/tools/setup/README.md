# Tools: Setup

This one is deprecated, Please checkout this page: https://cerebras.atlassian.net/wiki/spaces/IT/pages/2227601467/K8S+Installation+Steps

These scripts can be useful for understanding the initial bringup of a new
cluster.

Notes here will be presented as instructions for installation on two servers,
the `master` and the `worker` servers. For larger clusters, follow the
instructions for `worker` server for each of the worker nodes, Kubernetes
is designed to handle most of the labor for you, after the initial control plane
configuration.

*IMPORTANT:*
> These scripts MUST be to be tailored for your specific cluster's network
> settings. Please do not run them without giving them a thorough review
> and ensuring all of the values are correct for the cluster you are
> wanting to create. They are provided here merely as a record of some
> early experiments, and an example to build on for more sophiticated
> setup scripts in the future.
>
> These scripts were written for a default Rocky Linux 8.5 install.

## Kubernetes Cluster Install for Bare-Metal Linux Server

*STEP 0:*
> Before doing anything, make sure connectivity is established between your two
> or more servers over the proper high-speed NIC. ~~There are notes about this in
> the `example_nic_config.txt` file.~~

Run the following scripts in this order:

1. *MASTER* _and_ *WORKER*
    - [`install_deps.sh`](install_deps.sh)
    - [`disable_firewall.sh`](disable_firewall.sh)
    - [`get_containerd.sh`](get_containerd.sh)
    - [`get_kubernetes.sh`](get_kubernetes.sh)
    - [`start_containerd.sh`](start_containerd.sh)
    - Describe the nodes you'll be configuring in the `/etc/hosts` file _on each node_
      using their high-speed NIC IP, this will cause Kubernetes to utilize the
      correct interfaces for its software-defined networking traffic (pod network).
    - Note: Editing the hosts file can require additional configurations to allow for
      compatibility with Slurm and other site-wide services like user-authentication.
    - ~~Adjust the hostname to match: `hostnamectl set-hostname <yournewhostname>`~~
    - ~~Restart the hostname service: `systemctl restart systemd-hostnamed`~~
2. *MASTER*
    - **_Customize_** and run: [`setup_kubernetes_master.sh`](setup_kubernetes_master.sh)
    - Verify: `kubeadm init <options>` output was stored in `kubeadm_init.out`
    - Verify: existence of `.kube/` directory in `$HOME` path
    - Wait a moment for Kubernetes status to update. This should be fast because
      there is only one node defined at this point
    - Verify: `kubectl get nodes` shows master as "Ready"
3. *WORKER*
    - From a terminal on the `master` server, copy the text of the full command
      from the end of the `kubeadm_init.out` file that begins `kubeadm join ...`
      (it may be split into multiple lines, just copy all lines comprising the
      full command)
    - In a terminal on the `worker` server, logged in as root, type in `sudo `
      as a prefix, and then paste the full `kubeadm join ...` command and
      run it
    - Verify: The output displayed at the end of `kubeadm join` reports that
      it was successfully added to the cluster
    - You can deploy this join command to multiple workers at the same time,
      they do not need to be added one-at-a-time: (all is list of all other nodes) 
      `pssh -h all --inline "kubeadm join xxx`
4. *MASTER*
    - After the last worker has joined, wait up to 30s for Kubernetes status
      to update.
    - Verify: `kubectl get nodes` and verify that all nodes show a status
      of "Ready"
5. Update swap/nic to make sure node is using 100GB NIC and with swap on
   `pssh -h all -I<./nic_swap_update.sh`
   Make sure `kubectl get nodes -owide` shows nodes as "Ready" with the 100G NIC IP
6. Run the cli tooling to install cilium.
7. Done! You now have a bare-bones Kubernetes cluster installed and running.

## Load-Balancing for Control Plane

Please see comments in the [`setup_kubernetes_master.sh`](setup_kubernetes_master.sh)
file for how to specify that you are wanting Kubernetes to use a load-balanced
array of nodes for the _control plane_, when you want to run more than just a
single master server.

Specifically, the commented-out options that could be given to the
`kubeadm init` command. When those options are supplied, the output will
contain an additional version of the `kubeadm join` command which is used
to join a new *control plane* node to the load-balanced group, in addition
to the regular `kubeadm join` at the end of the file that is specifically
for worker nodes.

When you join a new server as a member of the _control plane_ group, it
should NOT be joined also as a worker, it's `kubeadm join` work is done
once it's joined the control plane.



