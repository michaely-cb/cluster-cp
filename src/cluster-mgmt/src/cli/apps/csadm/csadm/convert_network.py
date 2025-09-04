"""
Converts a network configuration file to a network configuration file for the
cluster mgmt clusterConfig.json. Note: you'll need to convert the output from
json to yaml using yq since I didn't want to depend on pyyaml in this script
in order to avoid setting up a virtualenv.

This code was copied from monolith:
- src/cluster_mgmt/src/cli/common/models.py
- src/cluster_mgmt/src/cli/common/network.py

and was modified to run as a standalone script with python.

This assumes the management node is running python3.6+.
"""

import argparse
import sys
import json
import logging
import os
import yaml

import models


logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger("convert_network")


DEFAULT_CFG_PATH = "/opt/cerebras/cluster/network_config.json"
CURRENT_CLUSTER_CONFIG = "/opt/cerebras/cluster/cluster.yaml"


def process_network_json(network_json, additional_group_count: str):
    with open(network_json) as f:
        doc = json.load(f)

    curr_cluster_config = None
    if os.path.exists(CURRENT_CLUSTER_CONFIG):
        with open(CURRENT_CLUSTER_CONFIG, 'r') as file:
            curr_cluster_config = models.Cluster(**yaml.safe_load(file))

    cluster = models.parse_cluster_config(doc, curr_cluster_config)

    if additional_group_count:
        existing, new = cluster.split_groups(additional_group_count)
        logger.info(f"existing groups {', '.join(existing)} were split, producing new groups {', '.join(new)}")
        for group in existing:
            logger.info(f"existing/{group}: {json.dumps(cluster.summarize_nodegroup(group))}")
        for group in new:
            logger.info(f"new/{group}: {json.dumps(cluster.summarize_nodegroup(group))}")

    unassigned_systems = cluster.get_unaffined_systems()
    if unassigned_systems:
        logger.warn(f"systems without nodegroup affinity: {', '.join(unassigned_systems)}")

    return cluster.to_json()


def main():
    # usage: python3 convert_network.py [network.json]
    #
    #   prints cluster config json to stdout

    level = logging.DEBUG if os.environ.get("DEBUG") else logging.INFO
    logging.basicConfig(level=level, stream=sys.stderr)

    parser = argparse.ArgumentParser(description='Process some arguments.')
    parser.add_argument('network_json', nargs='?', default=DEFAULT_CFG_PATH, help='Optional path to network json file')
    parser.add_argument('--create-depop-groups', default="", type=str, action='store',
                        help='Flag for splitting populated groups into depop groups for N additional groups. ' +
                        ' N=all splits to create a group for each unassigned system.')
    args = parser.parse_args()
    network_json = args.network_json

    logger.debug(f"reading network json from {network_json}")

    if not os.path.exists(network_json):
        logger.error(f"network json file {network_json} does not exist")
        sys.exit(1)

    print(process_network_json(network_json, args.create_depop_groups))


if __name__ == "__main__":
    main()
