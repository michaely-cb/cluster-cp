Exporters for node, system, and switch stats for Prometheus and a simple
self-contained network statistics reporting system (aka "linkmon").
The network stats collection includes flap and FEC error counts.

When run as part of Prometheus, the system exporter and the switch exporter
run in their own pods on a management node, while a node exporter pod is
created on every node. When running as the self-contained linkmon application,
the linkmon frontend runs on a single node and runs commands on the nodes over
SSH. In both modes, system data comes from the Redfish HTTP API and switch data
is collected from the switch CLI over SSH. Both modes mostly collect network
traffic and health stats, while the Prometheus mode also collects other stats. 

You can run linkmon from a monolith repo clone if you create a venv and install
the asyncssh package.
Or, to run using nerdctl on a cluster node, transfer the docker image and then
create a container with:
    nerdctl -n k8s.io run -it your-image-tag-or-file /bin/bash
Or, to run from a dev VM with docker,
    docker run -it your-image-tag-or-file /bin/bash
Then you can start linkmon with
    python ./linkmon.py -h
to view argument details and usage. Some example runs:

- Scrape from all switches every 5 minutes and export to Prometheus only
    ./linkmon.py -j /opt/cerebras/cluster/network_config.json --no-nodes --no-systems -i 300 --prometheus
- Scrape from all devices listed in the network.json and show changes over a
  1h period. Also include the 1Gb NICs. Print to stdout.
    ./linkmon.py -j /opt/cerebras/cluster/network_config.json --show-1g -i 3600
- Scrape from the devices in the given files. Run for 4 hours, checking every
  15m. Write to the 'my-output/' directory.
    ./linkmon -s nodes -n switches -c csxs -t 14400 -i 900 -o my-output

If not using a network json file, remember to set the SWITCH_USER and SWITCH_PASSWORD env vars!


More details on linkmon:

Linkmon can be run on any node. Some cluster switches are only accessible from
within that cluster's network, so linkmon needs to run on that network in those
cases. In general running from an AWS dev instance is sufficient.
The system running the linkmon tool needs several Python packages installed, or
should use the docker image with `docker` or `nerdctl`. However the target
nodes only need `bash` and `ethtool`.

Linkmon supports 3 switch OSes - Arista, HP Onyx, and HP Comware. HP Onyx
(rebadged Mellanox) switches typically have `-hpe` in the hostname, while the
Comware switches currently all have `256-hpe` in the hostname. Linkmon can't
auto-detect the switch OS type, but uses either the hostname or the vendor
field in the network_config.json to determine the OS type.
Arista switches don't accumulate FEC statistics, but instead they only show the
latest count of errors observed during the switch's (undocumented) sample
period. To work around this limitation, when an error is observed on an Arista
switch port, we create an additional "fast polling" monitoring thread to
continually query that port's status, constantly adapting the rate to avoid
missing updates but not query unnecessarily, since that can delay other
queries. If no errors are observed with fast polling monitoring for a while,
the fast polling thread terminates and the switch is scraped at the usual
interval only.

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
