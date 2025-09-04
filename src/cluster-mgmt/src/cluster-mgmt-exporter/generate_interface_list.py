#!/usr/bin/python3
"""
Network JSON parsing tool for linkmon/snmp.
When imported as a module, provides the parse_network_json() function for linkmon and the
interface_watch_list_server.
When executed as a script, generates the list of switch targets for snmp_exporter.
"""

import argparse
import gzip
import json
import logging
import re
from collections import defaultdict, namedtuple
from pathlib import Path

# Not used when running as a script directly from the host, only in k8s.
NETWORK_JSON_GZ_PATH = "/config/network_config.json.gz"


# On a multi-mgmt cluster, these files are only reliably available on the first
# mgmt node. But we only ever use them when the script is run from that node's
# cron job, never as an imported library. We can't use the kubernetes API
# for the cluster CM because it's not available in the system python
# installation.
CLUSTER_YAML_PATH = "/opt/cerebras/cluster/cluster.yaml"
NETWORK_JSON_PATH = "/opt/cerebras/cluster/network_config.json"


def generate_iwl_and_errors(port_dict):
    iwl = []
    iwl_errors = []

    for label_list in port_dict.values():
        if len(label_list) > 1:
            for entry in label_list:
                iwl_errors.append(
                    "interface_watch_list_errors{" + entry + ",error=\"Switch port used more than once\"} 1"
                )
        else:
            iwl.append("interface_watch_list{" + label_list[0] + "} 1")

    return iwl, iwl_errors


def parse_network_json(filename):
    # use a defaultdict(list) so that we can accumulate duplicates here, and then in the generate_iwl
    # step we can put the duplicated links in the errors list
    switch_ports = defaultdict(list)

    # let's assume we'll never see a device twice
    stamp_config = []

    switches_listed = set()  # switches explicitly listed in netjson - for consistency check
    leaf_switches = set()    # If a switch is connected to a node or system, it's a leaf switch
    connected_leaf_switches = set() # Directly connected leaf switches.

    node_watch_set = set()
    switch_watch_dict = dict()
    system_watch_set = set()

    switch_vendor_dict = dict()
    mgmt_ip_dict = dict()

    switch_users = dict()
    switch_passwords = dict()

    sys_conn_port_re = re.compile(r'Port (\d+)')
    sys_to_stamp_dict = dict()
    switch_to_stamp_dict = dict()

    interface_watch_list_errors = []

    if Path(filename).suffix == '.gz':
        with gzip.open(filename) as fp:
            net_data = json.load(fp)
    else:
        with open(filename) as fp:
            net_data = json.load(fp)

    # Get all switches and ports. Keep a set to verify later

    NodeType = namedtuple('NodeType', ['node_type', 'short_name', 'scrape_all_nics'])
    node_types = [
        NodeType('management_nodes', 'mgmt', False),
        NodeType('memoryx_nodes', 'memx', True),
        NodeType('worker_nodes', 'worker', False),
        NodeType('activation_nodes', 'ax', True),
        NodeType('swarmx_nodes', 'swarmx', True),
        NodeType('user_nodes', 'user', True),
    ]

    # Get stamps for switches before looking at links
    for s in net_data.get('switches', []):
        switch_to_stamp_dict[s['name']] = s.get('stamp', '')
        stamp_config.append(f"stamp_config{{stamp=\"{s.get('stamp', '')}\", switch=\"{s['name']}\"}} 1")

    # Links to nodes
    for ntype in node_types:
        for i in net_data.get(ntype.node_type, []):
            stamp_config.append(f"stamp_config{{stamp=\"{i.get('stamp', '')}\", node=\"{i['name']}\"}} 1")
            node_watch_set.add(i['name'])
            for idx, iface in enumerate(sorted(i.get('interfaces', []), key=lambda x: x['name'])):
                if not switch_watch_dict.get(iface['switch_name']):
                    switch_watch_dict[iface['switch_name']] = set()
                switch_watch_dict[iface['switch_name']].add(iface['switch_port'])
                leaf_switches.add(iface['switch_name'])

                # SNMP exporter only wants the first interface for certain node types
                if ntype.scrape_all_nics or idx == 0:
                    s_p = f"{iface['switch_name']}:{iface['switch_port']}"
                    switch_ports[s_p].append(
                        f"switch_id=\"{iface['switch_name']}\","
                        f"ifName=\"{iface['switch_port']}\","
                        f"node=\"{i['name']}\","
                        f"endpoint=\"{i['name']}\","
                        f"device=\"{iface['name']}\","
                        f"conn_type=\"node\","
                        f"conn_subtype=\"{ntype.short_name}\","
                        f"stamp=\"{i.get('stamp', '')}\""
                    )

    for s in net_data.get('systems', []):
        stamp_config.append(f"stamp_config{{stamp=\"{s.get('stamp', '')}\", system=\"{s['name']}\"}} 1")
        system_watch_set.add(s['name'])
        sys_to_stamp_dict[s['name']] = s.get('stamp', '')
        for iface in s.get('interfaces', []):
            if not switch_watch_dict.get(iface['switch_name']):
                switch_watch_dict[iface['switch_name']] = set()
            leaf_switches.add(iface['switch_name'])

    # Links to systems
    for i in net_data.get('system_connections', []):
        # Skip connections to systems that don't exist in this cluster
        if i['system_name'] not in sys_to_stamp_dict:
            logging.warning(f"Skipping connection to missing system: {i['system_name']} (ignore if the system is in the other side of the upgraded cluster)")
            continue

        if not switch_watch_dict.get(i['switch_name']):
            switch_watch_dict[i['switch_name']] = set()
        switch_watch_dict[i['switch_name']].add(i['switch_port'])
        leaf_switches.add(i['switch_name'])
        s_p = f"{i['switch_name']}:{i['switch_port']}"
        sys_port_match = sys_conn_port_re.match(i['system_port'])
        if sys_port_match:
            sys_port = int(sys_port_match.group(1))
            switch_ports[s_p].append(
                f"switch_id=\"{i['switch_name']}\","
                f"ifName=\"{i['switch_port']}\","
                f"node=\"{i['system_name']}\","
                f"endpoint=\"{i['system_name']}\","
                f"device=\"DATA_NETWORK_IF_{sys_port}\","
                f"conn_type=\"system\","
                f"stamp=\"{sys_to_stamp_dict[i['system_name']]}\""
            )

    # Links between switches
    for i in net_data.get('xconnect', {}).get('connections', []):
        for link in i['links']:
            if not switch_watch_dict.get(link['name']):
                switch_watch_dict[link['name']] = set()
            switch_watch_dict[link['name']].add(link['port'])

        # this should always be true, but just in case...
        if len(i['links']) == 2:
            j = i['links']

            is_leaf = (j[0]['name'] in leaf_switches, j[1]['name'] in leaf_switches)
            if is_leaf[0] and is_leaf[1]:
                connected_leaf_switches.add(tuple(sorted([j[0]['name'], j[1]['name']])))

            s_p = f"{j[0]['name']}:{j[0]['port']}"
            switch_ports[s_p].append(
                f"switch_id=\"{j[0]['name']}\","
                f"ifName=\"{j[0]['port']}\","
                f"node=\"{j[1]['name']}\","
                f"endpoint=\"{j[1]['name']}\","
                f"device=\"{j[1]['port']}\","
                "conn_type=\"ISL\","
                f'conn_subtype="{"leaf" if is_leaf[1] else "spine"}",'
                f"stamp=\"{switch_to_stamp_dict[j[0]['name']]}\""
            )
            s_p = f"{j[1]['name']}:{j[1]['port']}"
            switch_ports[s_p].append(
                f"switch_id=\"{j[1]['name']}\","
                f"ifName=\"{j[1]['port']}\","
                f"node=\"{j[0]['name']}\","
                f"endpoint=\"{j[0]['name']}\","
                f"device=\"{j[0]['port']}\","
                "conn_type=\"ISL\","
                f'conn_subtype="{"leaf" if is_leaf[0] else "spine"}",'
                f"stamp=\"{switch_to_stamp_dict[j[1]['name']]}\""
            )

    for i in net_data.get('exterior_connections', []):
        if not switch_watch_dict.get(i['exterior']['switch_name']):
            switch_watch_dict[i['exterior']['switch_name']] = set()
        switch_watch_dict[i['exterior']['switch_name']].add(i['exterior']['switch_port'])

        if not switch_watch_dict.get(i['interior']['switch_name']):
            switch_watch_dict[i['interior']['switch_name']] = set()
        switch_watch_dict[i['interior']['switch_name']].add(i['interior']['switch_port'])

        s_p = f"{i['interior']['switch_name']}:{i['interior']['switch_port']}"
        switch_ports[s_p].append(
            f"switch_id=\"{i['interior']['switch_name']}\","
            f"ifName=\"{i['interior']['switch_port']}\","
            f"node=\"{i['exterior']['switch_name']}\","
            f"endpoint=\"{i['exterior']['switch_name']}\","
            f"device=\"{i['exterior']['switch_port']}\","
            "conn_type=\"exterior\","
            "stamp=\"frontend_network\""
        )
        s_p = f"{i['exterior']['switch_name']}:{i['exterior']['switch_port']}"
        switch_ports[s_p].append(
            f"switch_id=\"{i['exterior']['switch_name']}\","
            f"ifName=\"{i['exterior']['switch_port']}\","
            f"node=\"{i['interior']['switch_name']}\","
            f"endpoint=\"{i['interior']['switch_name']}\","
            f"device=\"{i['interior']['switch_port']}\","
            "conn_type=\"exterior\","
            "stamp=\"frontend_network\""
        )

    # Before we scan the switches section of the netjson, first make a set of
    # all the switches we've seen when finding node/system links. Then we can
    # do a consistency check on the switches advertised in the netjson and the
    # switches observed in the connection tables.
    switches_observed = switch_watch_dict.keys()
    for s in net_data.get('switches', []):
        switches_listed.add(s['name'])

        if not switch_watch_dict.get(s['name']):
            switch_watch_dict[s['name']] = set()
        switch_vendor_dict[s['name']] = s['vendor']
        mgmt_ip_dict[s['name']] = s.get('management_address', None)
        # also get user/password?
        # for now, assume all the same
        try:
            switch_users[s['name']] = s['username']
            switch_passwords[s['name']] = s['password']
        except KeyError:
            pass

    interface_watch_list, duplicate_port_errors = generate_iwl_and_errors(switch_ports)

    interface_watch_list_errors += duplicate_port_errors

    # Warn if the network.json file seems inconsistent
    if switches_listed != switches_observed:
        logging.warning("Inconsistent records for switches in the network JSON")
    if len(interface_watch_list_errors):
        logging.warning(
            "Some switch ports used more than once, see the 'interface_watch_list_errors' metric in Prometheus"
        )
    for sw1, sw2 in connected_leaf_switches:
        logging.warning(
            f'Directly connected leaf switches: {sw1} and {sw2}. This is an error on v2 topo clusters.'
        )

    # Warn if network.json is missing management_address.
    for sw_name, mgmt_ip in mgmt_ip_dict.items():
        if mgmt_ip is not None:
            continue
        interface_watch_list_errors.append(
            f'interface_watch_list_errors{{switch_id="{sw_name}", error="Switch missing management_address in network_config.json"}} 1'
        )
        logging.warning(f"Switch {sw_name} missing management_address")

    return (
        switches_listed,
        switches_observed,
        interface_watch_list,
        interface_watch_list_errors,
        stamp_config,
        node_watch_set,
        switch_watch_dict,
        switch_vendor_dict,
        mgmt_ip_dict,
        system_watch_set,
        switch_users,
        switch_passwords,
    )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    switches_observed = set()
    interface_watch_list, iwl_errors = [], []
    parser = argparse.ArgumentParser(prog='generate_interface_list.py', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        '-o',
        '--output-dir',
        help='Write files to specified directory instead of the working directory',
    )
    args = parser.parse_args()

    if args.output_dir:
        output_root = Path(args.output_dir)
    else:
        output_root = Path("./")

    if Path(NETWORK_JSON_PATH).is_file():
        logging.info(f"Using {NETWORK_JSON_PATH} to configure interfaces for Prometheus monitoring.")
        _, switches_observed, interface_watch_list, iwl_errors, *_ = parse_network_json(NETWORK_JSON_PATH)
    else:
        # Sometimes the cluster.yaml has at least a few switch hostnames, but
        # it seems that if the system doesn't have a netjson then the cluster
        # yaml doesn't have anything either.
        logging.error("Can't find network_config.json.gz. Prometheus can't monitor switches!")

    # Prepare the service discovery file for prometheus
    snmp_config = [{'labels': {'snmp_exporter_host': 'prometheus-snmp-exporter:9116'}, 'targets': []}]
    for sname in sorted(switches_observed):
        snmp_config[0]['targets'].append(sname)

    with open(output_root / Path("snmp-sd.json"), "w") as f:
        json.dump(snmp_config, f)

    with open(output_root / Path("interface_watch_list.prom"), "w") as f:
        f.write("\n".join(interface_watch_list + iwl_errors))


if __name__ == "__main__":
    main()
