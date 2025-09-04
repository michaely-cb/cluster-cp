#!/usr/bin/env python3

"""
Utility for parsing network_config.json and cluster.yaml for incremental install
Invoke with --workdir=path-to-empty-dir --netjson=/opt/cerebras/cluster/network_config.json

Compares the node lists of the netjson and cluster yaml and finds new and removed nodes in the json.
Outputs list files for each type of node with each added and removed node.
Creates a temporary new cluster.yaml with the new nodes and all properties from the original file.
"""

import argparse
import sys
import glob
import json
import logging
import os
import platform
import shlex
import subprocess
import tempfile
import typing
import yaml

import models
from models import Node, System, Cluster

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format='%(levelname)s: %(message)s'
)

DEFAULT_CFG_PATH = "/opt/cerebras/cluster/network_config.json"
CURRENT_CLUSTER_CONFIG = "/opt/cerebras/cluster/cluster.yaml"

# USE_EXISTING_LISTS - if present, k8_init.sh will use existing files, not create new ones
# CONTROLPLANE_ADD_LIST - HOST of controlplane nodes (except this one) to add
# CONTROLPLANE_UPDATE_LIST - HOST of controlplane nodes (including this one) to update
# CONTROLPLANE_REMOVE_LIST - HOST of controlplane nodes (except this one) to remove
# WORKER_ADD_LIST - HOST for all k8 worker nodes to add to cluster
# WORKER_REMOVE_LIST - HOST for k8 worker nodes to remove from cluster
USE_EXISTING_LISTS=".use_existing_lists"
CONTROLPLANE_ADD_LIST=".controlplane.add.list"
CONTROLPLANE_UPDATE_LIST=".controlplane.update.list"
CONTROLPLANE_REMOVE_LIST=".controlplane.remove.list"
WORKER_ADD_LIST=".worker.add.list"
WORKER_REMOVE_LIST=".worker.remove.list"

def get_node_diff_list(workdir: str, nodes: typing.List[Node], curr_cluster_config: Cluster, safe_system_check: typing.Optional[bool] = False):
    """
There are several list categories used in models.py, k8_init's lists, and pkgcommon's PSSH types.
Create files for all of them for now. We can remove any unused ones later.
models has:    broadcastreduce (all from containing list in net json)
               memory
               worker
               management
k8_init has:   controlplane (yaml role=mgmt and props.cp=true...
                             this is determined during update_cluster_config.
                             if it's a new cluster, first 3 role=mgmt get prop[cp]=true label.
                             else it's all existing cp nodes that still exist in new json (will never increase)
                             so for incremental, we should never create these)
               worker (everything else)
        also has removed_ lists for both and control_plane_update
pkgcommon has: MGMT (yaml role=management; if none found, then role=any (for compilepods))
               NOT_BR (yaml role!=broadcastreduce)
               CRD (k8s label node-role-coordinator...
                    set during k8s/setup.sh to all _MGMT nodes (also sets node-role-management))
               WORKER (k8s label node-role-worker, added to nodes if yaml role=worker)
               STORAGE (yaml properties[storage-type]=ceph
                        if it's a new cluster, first 4 with role=mgmt.
                        else it's all existing ceph nodes that still exist in new json (will never increase)
                        so for incremental, we should never create these)
    """

    # k8_init's lists
    k8_controlplane_new = [] # should remain empty for now
    k8_worker_new = []
    k8_controlplane_update = []
    k8_controlplane_remove = []
    k8_worker_remove = []

    # pssh
    new_all = []
    new_br = []
    new_not_br = []
    new_memory = []
    new_worker = []
    new_mgmt = []
    new_storage = [] # empty for now

    if curr_cluster_config:
        newlist = [n.name for n in nodes]
        oldlist = [n.name for n in curr_cluster_config.nodes]
        newnodes = set(newlist) - set(oldlist)
       
        for newnode in nodes:
            if newnode.name in newnodes:
                with open(os.path.join(workdir, newnode.name + "-info"), "w") as f:
                    if len(newnode.networkInterfaces) > 1:
                        nic2 = newnode.networkInterfaces[1].name
                    else:
                        nic2 = ""
                    print(f"{newnode.role},{newnode.properties.get('group', '')},{newnode.networkInterfaces[0].name},{nic2}", file=f)

                # new_all is all nodes
                # since we won't promote a new mgmt node to a controlplane, all new nodes are workers
                # if we implement mgmt node promotion, then some of new_all will be in k8_controlplane_new
                new_all.append(newnode.name)
                k8_worker_new.append(newnode.name)

                if newnode.role == models.BROADCAST_REDUCE:
                    new_br.append(newnode.name)
                else:
                    new_not_br.append(newnode.name)
                if newnode.role == models.MEMORY:
                    new_memory.append(newnode.name)
                if newnode.role == models.WORKER:
                    new_worker.append(newnode.name)
                if newnode.role == models.MANAGEMENT:
                    new_mgmt.append(newnode.name)

                # TODO: make this work to support incr on compilepods
                # if newnode.role == "ANY":
                #     mgmt.append()
                #     crd.append()
                #     whatelse.append()

        rmnodes = set(oldlist) - set(newlist)
        for rmnode in curr_cluster_config.nodes:
            if rmnode.name in rmnodes:
                if rmnode.role == models.MANAGEMENT and rmnode.properties.get('controlplane') == "true":
                    k8_controlplane_remove.append(rmnode.name)
                else:
                    k8_worker_remove.append(rmnode.name)

    if safe_system_check:
        if new_all or k8_controlplane_remove or k8_worker_remove:
            return False
        else:
            return True

    # PSSH uses these files. They should always exist, even if empty.
    with open(os.path.join(workdir, "new_all"), "w") as f:
        for n in new_all:
            print(n, file=f)
    with open(os.path.join(workdir, "new_br"), "w") as f:
        for n in new_br:
            print(n, file=f)
    with open(os.path.join(workdir, "new_not_br"), "w") as f:
        for n in new_not_br:
            print(n, file=f)
    with open(os.path.join(workdir, "new_memory"), "w") as f:
        for n in new_memory:
            print(n, file=f)
    with open(os.path.join(workdir, "new_worker"), "w") as f:
        for n in new_worker:
            print(n, file=f)
    with open(os.path.join(workdir, "new_mgmt"), "w") as f:
        for n in new_mgmt:
            print(n, file=f)
    with open(os.path.join(workdir, "new_storage"), "w") as f:
        for n in new_storage:
            print(n, file=f)

    # k8_init.sh uses these files. They should only exist if they contain some nodes.
    # TODO handle new controlplanes - need to handle mgmt-to-controlplane and mgmt-to-ceph election logic
    if k8_controlplane_new:
        with open(os.path.join(workdir, "k8_controlplane_new"), "w") as f:
            for n in k8_controlplane_new:
                print(n, file=f)
    if k8_worker_new:
        with open(os.path.join(workdir, "k8_worker_new"), "w") as f:
            for n in k8_worker_new:
                print(n, file=f)
    if k8_controlplane_update:
        with open(os.path.join(workdir, "k8_controlplane_update"), "w") as f:
            for n in k8_controlplane_update:
                print(n, file=f)
    if k8_controlplane_remove:
        with open(os.path.join(workdir, "k8_controlplane_remove"), "w") as f:
            for n in k8_controlplane_remove:
                print(n, file=f)
    if k8_worker_remove:
        with open(os.path.join(workdir, "k8_worker_remove"), "w") as f:
            for n in k8_worker_remove:
                print(n, file=f)


    return new_all

def get_system_diff_list(workdir: str, systems: typing.List[System], curr_cluster_config: Cluster):
    new_systems = set()
    removed_systems = set()
    if curr_cluster_config:
        newlist = [n.name for n in systems]
        oldlist = [n.name for n in curr_cluster_config.systems]
        new_systems = set(newlist) - set(oldlist)
        removed_systems = set(oldlist) - set(newlist)

    with open(os.path.join(workdir, "systems_added"), "w") as f:
        for n in new_systems:
            print(n, file=f)
    with open(os.path.join(workdir, "systems_removed"), "w") as f:
        for n in removed_systems:
            print(n, file=f)


def main():
    level = logging.DEBUG if os.environ.get("DEBUG") else logging.INFO
    logging.basicConfig(level=level, stream=sys.stderr)


    parser = argparse.ArgumentParser(prog='csadm_list_helper',
                                     description='Creates node change lists and temporary cluster.yaml for '
                                     'csadm.sh to use during the incremental deploy process.')
    parser.add_argument('--workdir', required=True, help='Workdir to write list files')
    parser.add_argument('--netjson', nargs='?', default=DEFAULT_CFG_PATH, help='Path to network json file')
    parser.add_argument('--new-systems-only-check', action="store_true",
                        help='Check if this update is only adding new systems.')
    args = parser.parse_args()

    logging.debug(f"csadm_install reading network json from {args.netjson}")

    if not os.path.exists(args.netjson):
        logging.error(f"network json file {args.netjson} does not exist")
        sys.exit(1)

    with open(args.netjson) as f:
        network_json = json.load(f)

    curr_cluster_config = None
    if os.path.exists(CURRENT_CLUSTER_CONFIG):
        with open(CURRENT_CLUSTER_CONFIG, 'r') as file:
            curr_cluster_config = models.Cluster(**yaml.safe_load(file))

    cluster_cfg = models.parse_cluster_config(network_json, curr_cluster_config)

    if args.new_systems_only_check:
        only_added_systems = get_node_diff_list(args.workdir, cluster_cfg.nodes, curr_cluster_config, safe_system_check=args.new_systems_only_check)
        sys.exit(not only_added_systems)

    with open(os.path.join(args.workdir, "incremental-cluster.json"), "w") as f:
        f.write(cluster_cfg.to_json())

    if os.path.exists("/opt/cerebras/cluster/cluster.yaml"):
        with open(os.path.join(args.workdir, "cluster-properties-old.json"), "w") as outf:
            subprocess.run(shlex.split(f"yq -ojson '.properties // {{}}' /opt/cerebras/cluster/cluster.yaml"), stdout=outf, check=True)
    else:
        with open(os.path.join(args.workdir, "cluster-properties-old.json"), "w") as outf:
            outf.write("{}")

    with open(os.path.join(args.workdir,"cluster.json.merged"), "w") as outf:
        subprocess.run(shlex.split(f"jq -s '(.[0] * .[1].properties) as $mergedProps | .[1] | .properties = $mergedProps' {args.workdir}/cluster-properties-old.json {args.workdir}/incremental-cluster.json"), stdout=outf, check=True)

    subprocess.run(shlex.split(f"mv {args.workdir}/cluster.json.merged {args.workdir}/incremental-cluster.json"))

    with open(os.path.join(args.workdir,"incremental-cluster.yaml"), "w") as outf:
        subprocess.run(shlex.split(f"yq eval -P -oy {args.workdir}/incremental-cluster.json"), stdout=outf, check=True)

    new_nodes = get_node_diff_list(args.workdir, cluster_cfg.nodes, curr_cluster_config)
    get_system_diff_list(args.workdir, cluster_cfg.systems, curr_cluster_config)


if __name__ == "__main__":
    main()
