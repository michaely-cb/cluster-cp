"""
Linkmon main work loop
Used for both prometheus and standalone linkmon.
"""
import argparse
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime
from typing import Iterable

import urllib3
from json.decoder import JSONDecodeError
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, Metric
from prometheus_client.registry import Collector

# these are only used for non-prometheus runs
from csx import CSXSurvey
from generate_interface_list import parse_network_json
from node import NodeSurvey
from switch import SwitchSurvey

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s %(levelname)-8s %(message)s",
)

latest_run = None

class LinkmonTargets():
    def __init__(self, netjson_file, node_file, switch_file, system_file, no_nodes, no_switches, no_systems):
        self.nodes = self.switches = self.systems = set()
        self.switch_vendors = dict()
        self.switch_mgmt_ips = dict()
        self.switch_users = dict()
        self.switch_passwords = dict()
        self.netjson_file = netjson_file
        self.no_nodes = no_nodes
        self.no_switches = no_switches
        self.no_systems = no_systems

        self.switches_from_file = dict()
        self.nodes_from_file = set()
        self.cs2s_from_file = set()
        self.json_ok = False
        self.json_mtime = 0

        # If we have simple files and net json (unlikely in Prometheus case),
        # save the initial value of the simple files so that their contents are
        # always used even when the net json target set changes
        if switch_file and not no_switches:
            with open(switch_file) as sf:
                while sl := sf.readline():
                    if sl[0] == '#' or sl == "\n" or sl == '':
                        continue
                    self.switches_from_file[sl.rstrip()] = set()

        if node_file and not no_nodes:
            with open(node_file) as hf:
                while hl := hf.readline():
                    if hl[0] == '#' or hl == "\n" or hl == '':
                        continue
                    self.nodes_from_file.add(hl.rstrip())

        if system_file and not no_systems:
            with open(system_file) as csf:
                while csl := csf.readline():
                    if csl[0] == '#' or csl == "\n" or csl == '':
                        continue
                    self.cs2s_from_file.add(csl.rstrip())

        # also use them as the initial target set, in case there's no net json
        self.switches = self.switches_from_file
        self.nodes = self.nodes_from_file
        self.cs2s = self.cs2s_from_file

        # try to load the json
        self.try_update()

    def try_update(self):
        if self.netjson_file:
            try:
                new_json_mtime = os.stat(self.netjson_file).st_mtime
            except:
                logging.error("Could not read the network JSON file, will be unable to use JSON for target list until the file is updated.")
                new_json_mtime = 0

            if new_json_mtime > self.json_mtime:
                self.json_mtime = new_json_mtime
                try:
                    (
                        _,
                        _,
                        _,
                        _,
                        _, #stamp_config
                        nj_node,
                        nj_sw,
                        nj_svendors,
                        nj_mgmtips,
                        nj_sys,
                        switch_users,
                        switch_passwords,
                    ) = parse_network_json(self.netjson_file)

                    logging.info("Using a new network json file")

                    self.switch_users = switch_users
                    self.switch_passwords = switch_passwords
                    self.switch_vendors = nj_svendors
                    self.switch_mgmt_ips = nj_mgmtips

                    # reset the initial target set
                    self.switches = self.switches_from_file
                    self.nodes = self.nodes_from_file
                    self.cs2s = self.cs2s_from_file

                    # add everything found in network.json to everything found in simple files
                    if not self.no_nodes:
                        self.nodes |= nj_node
                    if not self.no_systems:
                        self.cs2s |= nj_sys
                    if not self.no_switches:
                        for k in nj_sw:
                            if self.switches.get(k):
                                # found some ports in the switch.txt file, merge in json ports
                                self.switches[k] |= nj_sw[k]
                            else:
                                # nothing from switch.txt (or no switch.txt), copy the json ports
                                self.switches[k] = nj_sw[k]
                except (FileNotFoundError, JSONDecodeError):
                    logging.error("Could not parse network JSON file, will be unable to use network JSON for target list until the file is updated.")


class LinkmonExporter(Collector):
    def __init__(self, node_name):
        self.node_name = node_name

    def collect(self) -> Iterable[Metric]:
        global latest_run
        if latest_run:
            stats = latest_run.prom_get()
            # don't delete after scrape - there may be multiple DB instances
            return stats
        else:
            logging.info("Linkmon: collect with no data")
            return SwitchSurvey(None, None, None, None, None, None, None).prom_get()


# db_init is not used when running under prometheus
def db_init(dbfile):
    survey_create_query = """
CREATE TABLE IF NOT EXISTS survey (
id INTEGER PRIMARY KEY,
survey_type string);
"""

    device_survey_create_query = """
CREATE TABLE IF NOT EXISTS device_survey (
survey INTEGER NOT NULL,
devicequery INTEGER NOT NULL);
"""

    tool_run_create_query = """
CREATE TABLE IF NOT EXISTS tool_run (
id INTEGER PRIMARY KEY,
query_time datetime,
node_survey_id INTEGER,
switch_survey_id INTEGER,
csx_survey_id INTEGER);
"""

    init_con = sqlite3.connect(dbfile)
    with init_con:
        init_con.execute(NodeSurvey.sqlite_create_table_cmd())
        init_con.execute(SwitchSurvey.sqlite_create_table_cmd())
        init_con.execute(CSXSurvey.sqlite_create_table_cmd())
        init_con.execute(survey_create_query)
        init_con.execute(device_survey_create_query)
        init_con.execute(tool_run_create_query)
    init_con.close()


# now create the nodesurvey, switchsurvey, etc tables
# possibly create a groupsurvey table?
parser = argparse.ArgumentParser(
    prog='linkmon.py',
    formatter_class=argparse.RawTextHelpFormatter,
    description="""
linkmonitor tool

The tool periodically checks node, switch, and CSX state and shows changes in
interface state, flap counts, and low-level 100G ethernet errors over time.
Nodes are scraped using SSH to get interface status with ethtool
and by reading /sys/class/net/. Switch state is scraped from the SSH admin
interface. CSX detail comes from the redfish stats service.

Connections to nodes use the default root user and password. Another user
can be specified by setting the NODE_USER environment variable. Another
password can be specified with the NODE_PASSWORD envvar, or key-based login
is also supported if the --node-ssh-key=path-to-key arg is given.

Connections to switches use the user and password specified in the network JSON
file, or if it is not available, the values of the SWITCH_USER and
SWITCH_PASSWORD environment variables.

Connections to redfish for system stats use the default admin/admin user and
password. Specify other values with the CSX_USER and CSX_PASSWORD env vars.

Linkmon needs lists of devices to query. If a network json file is available,
passing in the path with the -j argument will query all the devices found.
Linkmon can also get the device list from plaintext files, in case the net
json file is not ready or is incomplete. The node, switch, and csx list files
should be a plaintext file with one hostname/IP per line. Blank lines are
allowed. Use a '#' char in column zero to indicate a comment.

The --network-json arg can be combined with the --node-list (and etc.) args
and the tool will hit devices found in either file, so you can check devices
that are not included in the network.json file.

Linkmon can output statistics locally to stdout, plaintext files, or a
self-maintained SQLite DB. When running locally, all of the following
sample modes are supported:
The tool can run in "one-shot" mode by specifying -i (and not -t). Output is
written to stdout by default, but the -o or -d options can be used, which
also prevents writing to stdout.
In "multi-sample" mode, enabled with both -i and -t are given, the tool will
continue to collect samples at the given interval until the runtime limit
is passed. The -o or -d options must be specified, output to stdout only
is not allowed.
The -o and -d options can be combined.

Linkmon can also output stats to Prometheus. In this mode, -t is not allowed,
and -i is required. Linkmon only outputs switch stats to prometheus. When the
--prometheus arg is given in combination with other args, the --no-nodes and
--no-systems args must be given, as those devices report stats to Prometheus
using other means.

""",
    epilog="""
Examples:
- Scrape from all switches every 5 minutes and export to Prometheus only
    ./linkmon.py -j /opt/cerebras/cluster/network_config.json --no-nodes --no-systems -i 300 --prometheus
- Scrape from all devices listed in the network.json and show changes over a
  1h period. Also include the 1Gb NICs. Print to stdout.  
    ./linkmon.py -j /opt/cerebras/cluster/network_config.json --show-1g -i 3600
- Scrape from the devices in the given files. Run for 4 hours, checking every
  15m. Write to the 'my-output/' directory.
    ./linkmon -s nodes -n switches -c csxs -t 14400 -i 900 -o my-output

If not using a network json file, remember to set the SWITCH_USER and SWITCH_PASSWORD env vars!
""",
)


parser.add_argument('-s', '--switch-list', help='file containing a list of switches')
parser.add_argument('-n', '--node-list', help='file containing a list of nodes')
parser.add_argument('-c', '--csx-list', help='file containing a list of CSX systems')


parser.add_argument('-j', '--network-json', help='collect all devices from the given network.json')

parser.add_argument(
    '-t',
    '--run-time',
    help='total time in seconds to run collection, enables multi-sample mode - requires use of -o or -d',
)
parser.add_argument(
    '-i',
    '--interval',
    required=True,
    help='interval for sample collection [seconds], 0 to only make a new db update',
)

parser.add_argument('-o', '--output-directory',
                    help='write text to output directory, not stdout - dir must not exist')
parser.add_argument(
    '-d', '--database', help='store results to the named sqlite DB (for multi-sample runs)'
)

parser.add_argument(
    '--labelled-ports-only',
    action='store_true',
    help="Only show ports with a description label (unless specified in net.json)",
)
parser.add_argument(
    '--network-config-ports-only',
    action='store_true',
    help="Don't show switch ports that aren't specified in network config JSON (unless they have descriptions)",
)
parser.add_argument('--show-1g', action='store_true', help="Include 1Gb host NICs")

parser.add_argument(
    '--no-switches',
    action='store_true',
    help="Don't monitor switch ports (useful in combination with -j)",
)
parser.add_argument(
    '--no-nodes',
    action='store_true',
    help="Don't monitor node ports (useful in combination with -j)",
)
parser.add_argument(
    '--no-systems',
    action='store_true',
    help="Don't monitor system (CSX) ports (useful in combination with -j)",
)

parser.add_argument('-k', '--node-ssh-key', help='SSH key for node login')

parser.add_argument('--prometheus', action='store_true', help="prometheus mode")

args = parser.parse_args()

if args.prometheus:
    if not args.no_nodes:
        sys.exit("prometheus-linkmon does not support node stat collection")
    if not args.no_systems:
        sys.exit("prometheus-linkmon does not support system/cs2 stat collection")
    if args.no_switches:
        sys.exit("can't disable switch stat collection in prometheus mode")

targets = LinkmonTargets(args.network_json, args.node_list, args.switch_list, args.csx_list, args.no_nodes, args.no_switches, args.no_systems)

interval = int(args.interval)
if interval < 0:
    sys.exit("Must specify an interval >=0")

if interval == 0 and not args.database:
    sys.exit("Must specify an interval >0 unless making a single database update")

if args.prometheus:
    # if args.output_directory or args.database:
    #     sys.exit("Can't combine --prometheus and -o or -d")

    exporter_port = 8005
    node_name_env = os.getenv("NODE_NAME")

    exporter = LinkmonExporter(node_name_env)
    start_http_server(exporter_port)
    REGISTRY.register(exporter)

    logging.info("started up prometheus metric server on port %r" % exporter_port)
    logging.info("will collect at interval %r" % interval)
    logging.info("monitoring nodes: %r" % targets.nodes)
    logging.info("monitoring csxs: %r" % targets.cs2s)
    logging.info("len of switches: %r" % len(targets.switches))
    logging.info("monitoring switches: %r" % targets.switches.keys())
    logging.info("value of args.prom=%r" % args.prometheus)

    if args.output_directory or args.database:
        logging.warning("Running with --prometheus and -o or -d is not recommended")

if args.output_directory:
    outdir = args.output_directory
    try:
        os.mkdir(outdir)
    except FileExistsError:
        sys.exit("Can't reuse existing directory '%s', use a new directory name for this output mode" % outdir)

if args.database:
    # initialize the db
    logging.info("calling dbinit")
    db_init(args.database)

    logging.info("dbinit done")

if args.run_time:
    # multi-sample run
    if not args.output_directory and not args.database:
        sys.exit(
            "Must specify an output directory (-o) or database (-d) when --run-time is passed (multi-sample mode)"
        )

    run_time = int(args.run_time)
else:
    run_time = 0


# FIXME probably the wrong place or condition
if not args.prometheus:
    urllib3.disable_warnings()  # don't show certificate/https spam

if not args.prometheus:
    initial_node_stats = NodeSurvey(targets.nodes, args.show_1g, args.node_ssh_key)
    initial_csx_stats = CSXSurvey(targets.cs2s)

initial_switch_stats = SwitchSurvey(
    targets.switches,
    targets.switch_vendors,
    targets.switch_mgmt_ips,
    args.labelled_ports_only,
    args.network_config_ports_only,
    targets.switch_users,
    targets.switch_passwords,
)

if args.prometheus:
    latest_run = initial_switch_stats
    logging.info("set latestrun to initial_switch_stats")


if args.database:
    initial_node_stats.insert_run(args.database)
    initial_switch_stats.insert_run(args.database)
    initial_csx_stats.insert_run(args.database)

    con = sqlite3.connect(args.database)
    cur = con.cursor()
    trcmd = f"INSERT INTO tool_run VALUES(?, ?, ?, ?, ?)"
    data = (
        None,
        time.time(),
        initial_node_stats.key,
        initial_switch_stats.key,
        initial_csx_stats.key,
    )
    cur.execute(trcmd, data)
    con.commit()
    con.close()
    if interval == 0:
        sys.exit()

total_duration = 0
t_start = t_end = 0
while (total_duration <= run_time) or args.prometheus: # run forever in prom mode
    # try to keep the requested interval by accounting for the duration of the last scrape
    last_scrape_duration = t_end - t_start
    adj_interval = max(1, interval - last_scrape_duration)

    time.sleep(adj_interval)
    t_start = time.time()

    sample_time = datetime.now().strftime("%Y%m%d%H%M%S")

    targets.try_update()

    final_switch_stats = SwitchSurvey(
        targets.switches,
        targets.switch_vendors,
        targets.switch_mgmt_ips,
        args.labelled_ports_only,
        args.network_config_ports_only,
        targets.switch_users,
        targets.switch_passwords,
    )

    if not args.prometheus:
        final_node_stats = NodeSurvey(targets.nodes, args.show_1g, args.node_ssh_key)
        final_csx_stats = CSXSurvey(targets.cs2s)

    final_switch_stats.update_hf_monitoring(initial_switch_stats)
    if targets.switches and args.prometheus:
        latest_run = final_switch_stats

    if not args.prometheus:
        if targets.nodes:
            if args.output_directory:
                with open(f"{outdir}/nodes-{sample_time}", "w") as outf:
                    NodeSurvey.print_header(dest=outf)
                    NodeSurvey.print_two_runs(initial_node_stats, final_node_stats, dest=outf)
            if args.database:
                final_node_stats.insert_run(args.database)
            if not args.output_directory and not args.database:
                NodeSurvey.print_header(dest=sys.stdout)
                NodeSurvey.print_two_runs(initial_node_stats, final_node_stats, dest=sys.stdout)

        if targets.switches:
            if args.output_directory:
                with open(f"{outdir}/switches-{sample_time}", "w") as outf:
                    SwitchSurvey.print_header(dest=outf)
                    SwitchSurvey.print_two_runs(initial_switch_stats, final_switch_stats, dest=outf)
            if args.database:
                final_switch_stats.insert_run(args.database)
            if not args.output_directory and not args.database:
                SwitchSurvey.print_header(dest=sys.stdout)
                SwitchSurvey.print_two_runs(
                    initial_switch_stats, final_switch_stats, dest=sys.stdout
                )

        if targets.cs2s:
            if args.output_directory:
                with open(f"{outdir}/csxs-{sample_time}", "w") as outf:
                    CSXSurvey.print_header(dest=outf)
                    CSXSurvey.print_two_runs(initial_csx_stats, final_csx_stats, dest=outf)
            if args.database:
                final_csx_stats.insert_run(args.database)
            if not args.output_directory and not args.database:
                CSXSurvey.print_header(dest=sys.stdout)
                CSXSurvey.print_two_runs(initial_csx_stats, final_csx_stats, dest=sys.stdout)

        if args.database:
            con = sqlite3.connect(args.database)
            cur = con.cursor()
            trcmd = f"INSERT INTO tool_run VALUES(?, ?, ?, ?, ?)"
            data = (
                None,
                time.time(),
                final_node_stats.key,
                final_switch_stats.key,
                final_csx_stats.key,
            )
            cur.execute(trcmd, data)
            con.commit()
            con.close()

        initial_node_stats = final_node_stats
        initial_csx_stats = final_csx_stats

    initial_switch_stats = final_switch_stats

    t_end = time.time()
    total_duration += interval

    # if switches:
    # print(f"# Timing info: SwitchSurvey1 {issct}+{isspt} "
    # ... "-- SwitchSurvey2 {final_switch_stats.collect_time}+ "
    # ... "{final_switch_stats.process_time}")

# Stop all switch monitor threads.
SwitchSurvey.stop_hf_monitoring()
