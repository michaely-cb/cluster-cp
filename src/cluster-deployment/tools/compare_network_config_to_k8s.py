#!/usr/bin/python3
import argparse
import concurrent.futures
import json
import logging
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import Set

logging.basicConfig()

logger = logging.getLogger(__name__)

NODE_TYPES = ("activation", "memoryx", "management", "swarmx", "worker")


def main(config: str, remove_mode="none"):
    network_doc_path = config
    with open(network_doc_path, 'r') as f:
        network_doc = json.loads(f.read())

    nw_nodes = set()
    nw_systems = set()
    for k in NODE_TYPES:
        for obj in network_doc.get(f"{k}_nodes", []):
            nw_nodes.add(obj["name"])
    for obj in network_doc.get("systems", []):
        nw_systems.add(obj["name"])

    k8_nodes = set(n.split("/")[1] for n in subprocess.check_output(shlex.split("kubectl get nodes -oname")).decode().strip().splitlines())
    k8_systems = set(n.split("/")[1] for n in subprocess.check_output(shlex.split("kubectl get systems -oname")).decode().strip().splitlines())

    new_nodes = nw_nodes.difference(k8_nodes)
    new_systems = nw_systems.difference(k8_systems)

    removed_nodes = k8_nodes.difference(nw_nodes)
    removed_systems = k8_systems.difference(nw_systems)

    unreachable_new_nodes = []

    def try_ssh(n):
        try:
            subprocess.check_call(shlex.split(f"ssh -oStrictHostKeyChecking=No -o ConnectTimeout=2 -o ConnectionAttempts=1 root@{n} uptime"))
            return n, True
        except subprocess.CalledProcessError as e:
            logger.warning(f"{n} unreachable: {e}")
            return n, False
        except Exception as e:
            logger.warning(f"{n} unknown: {e}")
            return n, False

    with ThreadPoolExecutor(32) as ex:
        futures = []
        for node in new_nodes:
            futures.append(ex.submit(try_ssh, node))
        concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
        for f in futures:
            n, ok = f.result()
            if not ok:
                unreachable_new_nodes.append(n)

    print(json.dumps({
        "new_nodes": sorted(list(new_nodes)),
        "new_systems": sorted(list(new_systems)),
        "removed_nodes": sorted(list(removed_nodes)),
        "removed_systems": sorted(list(removed_systems)),
        "unreachable_new_nodes": sorted(list(unreachable_new_nodes))
    }, indent=2))

    def remove_nodes(toremove: Set[str]):
        for k in NODE_TYPES:
            key = f"{k}_nodes"
            nodes = network_doc.get(key, [])
            network_doc[key] = [n for n in nodes if n["name"] not in toremove]
        with open(network_doc_path, 'w') as f:
            f.write(json.dumps(network_doc, indent=2))

    if remove_mode == "unreachable_new":
        remove_nodes(set(unreachable_new_nodes))
    elif remove_mode == "new":
        remove_nodes(new_nodes)
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare the network_config json doc with the current set of nodes in the k8s cluster. "
                                     "Optionally remove new or new+unreachable nodes")
    parser.add_argument("--config", "-C", help="network config doc",
                        default="/opt/cerebras/cluster/network_config.json")
    parser.add_argument("--remove", help="remove certain nodes from the network doc",
                        choices=("new", "unreachable_new", "none"), default="none")
    args = parser.parse_args()
    main(args.config, args.remove)