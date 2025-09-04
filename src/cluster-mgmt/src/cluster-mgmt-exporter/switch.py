"""
Switch Survey module for linkmon
Used for both prometheus and standalone modes.
Connects to all switches over SSH and scrapes CLI data. Results can be reported
as Prometheus metrics or as text or DB in standalone mode.
"""

import asyncio
import json
import logging
import os
import pytimeparse2
import re
import socket
import sqlite3
import sys
import time
from enum import IntEnum

import asyncssh
from arista_switch_fast_polling import AristaFastPollingWorker
from prometheus_client.core import GaugeMetricFamily

from juniper import Juniper
from credo import Credo
from switch_utils import PortInfo, data_rate_to_kbps_converter, floatOrMinusOne

logger = logging.getLogger(__name__)


class SwitchSurvey:
    MAX_HPE_PORT_COUNT = 64
    MIN_COMWARE_SLOT = 2
    MAX_COMWARE_SLOT = 8
    MIN_COMWARE_PORT = 1
    MAX_COMWARE_PORT = 48

    fmt_str = "%-24s %-7s %-24s %-18s %-48s %-14s %6s %12s %10s %-23s %14s %18s %-10s"
    headings = (
        "Switch",
        "Vendor",
        "Port",
        "MAC",
        "Description",
        "OperState",
        "Flaps",
        "RxKBytes",
        "CRC errors",
        "FEC align",
        "FEC cor. cword",
        "FEC uncor. cword",
        "Health",
    )

    # Per-port metrics
    metrics_to_descr = {
        "delta_fec_corrected_codewords": "changes in FEC corr count",
        "delta_fec_uncorrected_codewords": "changes in FEC uncorr count",
        "oper_status": "port operstatus",
        "errdisabled": "port error disabled",
        "carrier_changes": "port carrier changes",
        "rx_bytes": "port rx bytes",
        "input_errors": "port input error count",
        "runts": "runt packets count",
        "crc_errors": "port CRC error count",
        "fec_corrected_codewords": "port FEC corr count",
        "fec_uncorrected_codewords": "port FEC uncorr count",
        "opt_temperature": "port xcvr temp",
        "opt_voltage": "port xcvr voltage",
        "opt_tx_current": "port TX current",
        "opt_tx_power": "port TX power",
        "opt_rx_power": "port RX power",
        "rx_pfc": "port RX priority flow control",
        "tx_pfc": "port TX priority flow control",
        "in_discards": "port input discards",
        "out_discards": "port output discards",
        "out_discards_prio5": "port output discards for priority 5",
        "rx_kbps": "port RX kbps",
        "tx_kbps": "port TX kbps",
        "ecn_marked_packets_tc5": "port packets marked with congestion on TC 5",
    }

    # Per-switch metrics
    switch_metrics_to_descr = {
        "psu_health": "health status of a power supply",
        "temperature": "current temperature (C) of a thermal sensor",
        "temperature_health": "health status of a thermal sensor",
        "fan_rpm": "Fan speed in RPM",
        "fan_health": "health status of a fan",
        "uptime": "switch uptime in seconds",
        "version_info": "switch model and version",
    }
    switch_metrics_labels = dict(
        version_info=['switch', 'vendor', 'model', 'firmware_version'],
        uptime=['switch'],
    )
    default_switch_metrics_labels = ['switch', 'desc']

    client = None
    arista_fast_polling_workers = {}

    def __init__(
        self, switch_list, vendor_dict, mgmt_ip_dict, exclude_unlabel, exclude_not_in_nj, users, passwords
    ):
        # TODO:
        # bring all the Metrics into the SwitchSurvey at top-level
        # implement a __next__() func

        self.key = 0
        self.exclude_unlabel = exclude_unlabel
        self.exclude_not_in_nj = exclude_not_in_nj
        self.stats = dict()
        self.vendors = dict()
        self.switch_mgmt_ips = dict()
        self.results = None
        self.collect_time = 0
        self.process_time = 0
        self.cmds = None # list of sent commands - also is the map of switches actually used

        if switch_list:
            # Note: These were originally strings, so all devices used the same user/pass
            # as the last switch seen in the netjson. Now they are a dict so each switch
            # can have unique credentials
            if users:
                self.users = users
            if passwords:
                self.passwords = passwords

            self.switches = switch_list

            self.vendors = vendor_dict
            if vendor_dict:
                # Assume a switch is arista unless it's labelled as hpe
                # TODO the 256 port HPE switches are a third OS type, we should
                # improve switch OS handling here and for vendors[]
                self.arista_hostnames = [
                    h for h in list(switch_list) if vendor_dict.get(h, '').lower() == "arista"
                ]
                self.hpe_hostnames = [
                    h for h in list(switch_list) if vendor_dict.get(h, '').lower() == "hpe"
                ]
                self.sonic_hostnames = [
                    h
                    for h in list(switch_list)
                    if (
                        vendor_dict.get(h, '').lower() == "dell"
                        or vendor_dict.get(h, '').lower() == "edgecore"
                    )
                ]
                self.juniper_hostnames = [
                    h for h in list(switch_list) if vendor_dict.get(h, '').lower() == "juniper"
                ]
                self.credo_hostnames = [
                    h for h in list(switch_list) if vendor_dict.get(h, '').lower() == "credo"
                ]
            else:
                self.arista_hostnames = [h for h in list(switch_list) if "arista" in h]
                self.hpe_hostnames = [h for h in list(switch_list) if "hpe" in h]
                self.sonic_hostnames = []
                self.juniper_hostnames = []
                self.credo_hostnames = []

            if mgmt_ip_dict is not None:
                self.switch_mgmt_ips = mgmt_ip_dict
                for s in self.switches:
                    # Fall-back to DNS lookup if the IP is missing.
                    if s in mgmt_ip_dict and mgmt_ip_dict[s]: continue
                    try:
                        self.switch_mgmt_ips[s] = str(socket.gethostbyname(s))
                    except:
                        logger.warning(f"Failed to resolve mgmt IP for switch {s}")
                        self.switch_mgmt_ips[s] = ""

            t1 = time.time()
            self.collect()
            t2 = time.time()
            self.populate()
            t3 = time.time()

            self.collect_time = t2 - t1
            self.process_time = t3 - t2
            self.tmp_start_time = t1
            return

    def update_hf_monitoring(self, init_stats):
        new_mons = self.arista_fec_reconcile(init_stats, __class__.arista_fast_polling_workers)
        pop_list = []
        for mkey in __class__.arista_fast_polling_workers:
            m = __class__.arista_fast_polling_workers[mkey]
            if m.stopped:
                m.thread.join()
                pop_list.append(mkey)
        for mkey in pop_list:
            __class__.arista_fast_polling_workers.pop(mkey)
        for sp in new_mons:
            if __class__.arista_fast_polling_workers.get(sp[0]):  # sp0 is key -- "switch:port"
                pass
            else:
                m = AristaFastPollingWorker(sp[1], sp[2], sp[3], sp[4], self.users, self.passwords)
                __class__.arista_fast_polling_workers[sp[0]] = m

    @classmethod
    def stop_hf_monitoring(cls):
        for b in cls.arista_fast_polling_workers.values():
            b.stop_request = True

        for m in cls.arista_fast_polling_workers.values():
            m.thread.join()

    @classmethod
    def create_metrics(cls):
        default_labels = [
            'switch',
            'port',
            'desc',
        ]
        extended_labels = [
            'switch',
            'port',
            'desc',
            'ifindex',
            'portindex', # alias for ifindex
            'speed',
            'mac',
            'opt_speed',
            'opt_vendor',
            'opt_partnum',
            'opt_rev',
            'opt_sernum',
            'switch_mgmt_ip',
            'agent', # alias for switch_mgmt_ip
            'fec_enabled' # only reported for media converters for now
        ]

        ext_label_metric = GaugeMetricFamily(
            "linkmon_switch_extra_labels",
            "extra_labels",
            labels=extended_labels,
        )
        # add metrics for linkmon itself
        scrape_duration = GaugeMetricFamily(
            "linkmon_switch_scrape_duration", "time needed for this scrape of all switches"
        )
        # Per-port metrics
        port_metrics = dict()
        for k, v in cls.metrics_to_descr.items():
            # these metrics have an extra lane='x' label
            # we can generalize this later if needed, but let's keep it simple for now
            if 'current' in k or 'power' in k:
                port_metrics[k] = GaugeMetricFamily("linkmon_switch_" + k, v, labels=default_labels + [ 'lane' ])
            elif k == 'errdisabled':
                port_metrics[k] = GaugeMetricFamily("linkmon_switch_" + k, v, labels=default_labels + ['reason'])
            else:
                port_metrics[k] = GaugeMetricFamily("linkmon_switch_" + k, v, labels=default_labels)
        # Per-switch metrics
        switch_metrics = dict()
        for k, v in cls.switch_metrics_to_descr.items():
            labels = cls.switch_metrics_labels.get(
                k, cls.default_switch_metrics_labels)
            switch_metrics[k] = GaugeMetricFamily(
                "linkmon_switch_" + k, v, labels=labels)

        return ext_label_metric, port_metrics, switch_metrics, scrape_duration

    def prom_get(self):
        ext_label_metric, port_metrics, switch_metrics, scrape_duration = SwitchSurvey.create_metrics()
        scrape_duration.add_metric([], self.collect_time + self.process_time)

        for s in self.stats:
            mgmt_ip = self.switch_mgmt_ips.get(s, "")
            for p in self.stats[s].port_list:
                ifindex = str(self.stats[s].port_list[p].ifindex or '')
                labels = [
                    s,
                    p,
                    self.stats[s].port_list[p].description,
                ]
                ext_labels = [
                    s,
                    p,
                    self.stats[s].port_list[p].description,
                    ifindex,
                    ifindex,
                    str(self.stats[s].port_list[p].speed) if self.stats[s].port_list[p].speed else "",
                    self.stats[s].port_list[p].mac,
                    self.stats[s].port_list[p].opt_speed,
                    self.stats[s].port_list[p].opt_vendor,
                    self.stats[s].port_list[p].opt_partnum,
                    self.stats[s].port_list[p].opt_rev,
                    self.stats[s].port_list[p].opt_sernum,
                    mgmt_ip,
                    mgmt_ip,
                    self.stats[s].port_list[p].fec_enabled
                ]
                ext_label_metric.add_metric(ext_labels, 1)

                for k, m in port_metrics.items():
                    try:
                        val = getattr(self.stats[s].port_list[p], k)
                        if type(val) == dict:
                            for lane_id, v in val.items():
                                m.add_metric(labels + [lane_id], v)
                        elif k == 'errdisabled' and val is not None:
                            reason = self.stats[s].port_list[p].errdisabled_reason
                            m.add_metric(labels + [reason], val)
                        elif val is not None:
                            m.add_metric(labels, val)
                    except AttributeError:
                        print(f"Missing attribute {k}, may be ok if no value found in switch data")

            for k, m in switch_metrics.items():
                for st in self.stats[s].switch_stats[k]:
                    label_names = self.switch_metrics_labels.get(
                        k, self.default_switch_metrics_labels)
                    labels = []
                    for label_name in label_names:
                        if label_name == 'switch':
                            labels.append(s)
                        elif label_name not in st:
                            logger.warning(
                                f"Couldn't find label {label_name} in switch_stats for the linkmon_switch_{k} metric")
                            labels.append('linkmon internal error')
                        else:
                            labels.append(st[label_name])
                    m.add_metric(labels, float(st["val"]))

        yield ext_label_metric
        for v in port_metrics.values():
            yield v
        for v in switch_metrics.values():
            yield v
        yield scrape_duration

    @staticmethod
    def find_ports_with_new_errors(survey1, survey2):
        mlist = []

        init_set = set()
        final_set = set()
        for s in survey1.stats:
            if s in survey1.arista_hostnames:
                for p in survey1.stats[s].port_list:
                    init_set.add(s + ':' + p)
        for s in survey2.stats:
            if s in survey2.arista_hostnames:
                for p in survey2.stats[s].port_list:
                    final_set.add(s + ':' + p)

        for k in sorted((init_set & final_set)):
            s_p = k.split(':')
            sname = s_p[0]
            pname = s_p[1]
            f = survey2.stats[sname].port_list[pname]
            i = survey1.stats[sname].port_list[pname]

            # Only needed for Arista switches
            if survey2.vendors and survey2.vendors.get(sname, '').lower() != "arista":
                continue

            # Won't have these set if the port is down
            if f.delta_fec_corrected_codewords is None or i.delta_fec_corrected_codewords is None:
                continue

            # If the corr/uncorr change count was more than 1, then we missed an update.
            # We should start HF monitoring on this port.
            if (f.delta_fec_corrected_codewords - i.delta_fec_corrected_codewords > 1) or (
                f.delta_fec_uncorrected_codewords - i.delta_fec_uncorrected_codewords > 1
            ):
                mlist.append(
                    (
                        k,
                        sname,
                        pname,
                        f.delta_fec_corrected_codewords,
                        f.delta_fec_uncorrected_codewords,
                    )
                )

        return mlist

    def arista_fec_reconcile(self, init_stats, active_arista_fast_polling_workers):
        # since arista FEC numbers are reported in an unhelpful way, use this func
        # to adjust calculation.

        # see if we missed any updates ('changes' column from switch), if so
        # request to start HF monitoring them
        new_hf_mon_needed = __class__.find_ports_with_new_errors(init_stats, self)

        # Now create a cumulative FEC error count out of the sample reports:
        # First, if there was a previous run (either from this instance, or (TODO) if it's the
        # first run of a new invocation but there's something to grab from the DB)
        # OR a worker is active for this port, use either source as an init value to add to
        # self's current update.
        #
        # if not arista_fast_polling_workers:// simple case
        #     if self.delta > init.delta:    // only add up if
        #         self += init               // a new error report arrived
        #                                    //
        # else:                              // ugly case, we are doing HF monitoring of this port
        #     if bm[0] != init.delta:        // switch updated count before HF monitoring started
        #         self += bm0                // so include both init and first HF sample
        #     self += bm[1:-2]               // now add all the samples between init & self
        #     if self.delta > bm[-1].delta:  // if HF terminated early, or still missed an update
        #         self +=  bm[-1]            // before self's sample, then add HF's last sample
        #                                    // (otherwise bm[-1] and self are duplicates

        for s in self.stats:
            for p in self.stats[s].port_list:
                spkey = f"{s}:{p}"

                # if the switch or port wasn't present in the first run, skip
                if not init_stats.stats.get(s):
                    continue
                if not init_stats.stats[s].port_list.get(p):
                    continue

                # grab a reference to keep the lines readable
                init_pi = init_stats.stats[s].port_list[p]
                pi = self.stats[s].port_list[p]

                # Won't have these set if the port is down
                if (
                    init_pi.delta_fec_corrected_codewords is None
                    or pi.delta_fec_corrected_codewords is None
                ):
                    continue

                # first, always add the previous run's stats, if it's not from the same sample
                # (as reported by the switch itself)
                if pi.delta_fec_corrected_codewords > init_pi.delta_fec_corrected_codewords:
                    pi.fec_corrected_codewords += init_pi.fec_corrected_codewords
                if pi.delta_fec_uncorrected_codewords > init_pi.delta_fec_uncorrected_codewords:
                    pi.fec_uncorrected_codewords += init_pi.fec_uncorrected_codewords

                if active_arista_fast_polling_workers.get(spkey):  # HF monitoring is active on s:p
                    m = active_arista_fast_polling_workers[spkey]
                    # grab the lock so that the (still running) HF thread waits for us
                    with m.loglock:
                        if len(m.log) < 1:
                            continue  # Shouldn't happen - worker was created but did nothing yet

                        # track changes in the change count to avoid double counting
                        last_changes_corr = m.init_delta_corr
                        last_changes_uncorr = m.init_delta_uncorr

                        last_before_self = 0
                        for r in m.log[0:-1]:
                            if (
                                r.corr_ch > pi.delta_fec_corrected_codewords
                                or r.uncorr_ch > pi.delta_fec_uncorrected_codewords
                            ):
                                # Once HF monitor samples get ahead of us (from the main thread),
                                # save it for the next run of this func
                                break
                            else:
                                last_before_self += 1

                            # Shouldn't happen, but if this sample is older than init, skip
                            if (
                                r.corr_ch < init_pi.delta_fec_corrected_codewords
                                and r.uncorr_ch < init_pi.delta_fec_uncorrected_codewords
                            ):
                                continue

                            # Normal case - if we're sampling fast enough, we'll occasionally see
                            # the same value twice. Don't count it twice.
                            if r.corr_ch != last_changes_corr:
                                last_changes_corr = r.corr_ch
                                pi.fec_corrected_codewords += r.corr
                            if r.uncorr_ch != last_changes_uncorr:
                                last_changes_uncorr = r.uncorr_ch
                                pi.fec_uncorrected_codewords += r.uncorr_ch

                        # now either the last item in m.log is the same as final, so don't overcount
                        # or it's different, so add both
                        if m.log[last_before_self - 1].corr_ch < pi.delta_fec_corrected_codewords:
                            pi.fec_corrected_codewords += m.log[last_before_self - 1].corr
                        if (
                            m.log[last_before_self - 1].uncorr_ch
                            < pi.delta_fec_uncorrected_codewords
                        ):
                            pi.fec_uncorrected_codewords += m.log[last_before_self - 1].uncorr

                        # generally last_before_self is the lenth of m.log, but if the main thread
                        # was slow there's more in the list. Delete everything up to the idx of
                        # l_b_s. If the main thread wasn't slow, it simply clears mlog
                        m.init_delta_corr = m.log[last_before_self - 1].corr_ch
                        m.init_delta_uncorr = m.log[last_before_self - 1].uncorr_ch
                        m.log = m.log[last_before_self:]

        return new_hf_mon_needed

    # note on asyncssh usage: the SSHConnection.run() function handles a lot of the low level stuff
    # with terminals, I/O, etc. But we can't use it everywhere: the 256 port (comware) switches
    # have so many ports we end up generating a command line that's too long. We could use
    # create_process() so send multiple commands on a single connection, but each command string
    # uses a new SSH session. There's a login delay, we have to re-send any init commands (like
    # pager disable), and of course, comware doesn't support starting additional sessions on an
    # existing connection. Some fun stuff comes up with open_session() -
    # - comware respects PTY_ECHO=0, the others don't
    # - comware sends weird control characters if you ask for FEC stats on slot 1 and then stops
    #   accepting new commands
    # - comware ignores EOF and requires explicit 'exit' commands to leave modes and exit
    # - Some switches can accept commands immediately, some reject commands sent before the
    #   switch sends the [hostname]> prompt
    @staticmethod
    async def run_client(host, cmd, users, passwords) -> asyncssh.SSHCompletedProcess:
        ### for testing
        if host == "kylel-dev":
            port = 22222
        else:
            port = 22
        ### end testing

        if users[host]:
            username = users[host]
        else:
            logger.warning(
                "Switch admin CLI user not found in network_config.json,"
                " checking SWITCH_USER environment variable."
            )
            try:
                username = os.environ['SWITCH_USER']
            except KeyError:
                logger.warning(
                    f"Could not find admin CLI user, switch {host} will not"
                    " be monitored."
                )
                return None

        if passwords[host]:
            password = passwords[host]
        else:
            logger.warning(
                "Switch admin CLI password not found in network_config.json,"
                " checking SWITCH_PASSWORD environment variable."
            )
            try:
                password = os.environ['SWITCH_PASSWORD']
            except KeyError:
                logger.warning(
                    f"Could not find admin CLI password, switch {host} will not"
                    " be monitored."
                )
                return None

        async with asyncssh.connect(
                host, username=username, password=password, known_hosts=None, port=port
        ) as conn:
            s_in, s_out, _ = await conn.open_session(
                encoding='ascii', term_type='xterm', term_modes={asyncssh.PTY_ECHO: 0}
            )

            await asyncio.wait_for(s_out.readuntil(re.compile(r'>|#|\$')), timeout=10)
            for c in cmd:
                s_in.write(c)

            await s_in.drain()

            # Originally we wrote EOF after all commands finished, but Credo's sshd
            # drops the connection even if it has output queued for us.
            # s_in.write_eof()
            cmdoutput = None
            try:
                cmdoutput = await asyncio.wait_for(s_out.read(), timeout=90)
            except (TimeoutError, asyncio.TimeoutError):
                logging.error(f"Error reading from switch SSH connection to {host}: timed out")
            except Exception as e:
                logging.error(f"Error reading from switch SSH connection to {host}: {e}")

            # So now we write EOF after we have received our data
            s_in.write_eof()

            return cmdoutput

    async def run_all(self) -> None:
        tasks = (
            SwitchSurvey.run_client(n, self.cmds[n], self.users, self.passwords) for n in self.cmds
        )
        self.results = await asyncio.gather(*tasks, return_exceptions=True)

    def collect(self):
        self.cmds = dict()
        # terminal length 0 disables the pager
        # terminal width 1000 is needed because some switches will change the output format
        # to fit the default 80 column terminal created for our ssh client
        arista_cmd = [
            "terminal length 0\n",
            "terminal width 1000\n",
            "show int\n",
            "show hostname\n",
            "show int phy detail\n",
            "show int transceiver dom\n",
            "show priority-flow-control counters\n",
            "show hostname\n",
            "show snmp mib ifmib ifindex\n",
            "show interfaces counters discards\n",
            "show interfaces counters queue detail\n",
            "show int counters rates\n",
            "show hostname\n",
            "show interfaces status errdisabled\n",
            "show hostname\n",
            "show version | json\n",
            "show environment power | json\n",
            "show system environment temperature | json\n",
            "show system environment cooling | json\n",
            "show qos interfaces ecn counters queue\n",
            "exit\n",
        ]
        hpe_cmd = [
            "no cli session paging enable\n",
            "show int eth\n",
            "show interfaces ethernet counters pfc prio 5\n",
            "show interfaces ethernet counters tc 5\n",
            "show int ethernet rates\n",
            "show version\n",
            "show inventory\n",
            "show power\n",
            "show temperature\n",
            "show fan\n",
            "show hosts\n",
        ]
        # hpe needs separate 'trans xyz' command for every port
        for i in range(1, 1 + SwitchSurvey.MAX_HPE_PORT_COUNT):
            hpe_cmd.append(f"show int eth 1/{i} trans\n")
            hpe_cmd.append(f"show int eth 1/{i} trans diag\n")

        # trans count doesn't include the interface name in the output
        # so use the "separator command" and start from eth 1/1
        hpe_cmd.append("show hosts\n")
        for i in range(1, 1 + SwitchSurvey.MAX_HPE_PORT_COUNT):
            hpe_cmd.append(f"show int eth 1/{i} trans count\n")
        hpe_cmd.append("exit\n")

        comware_cmd = [
            "screen-length disable\n",
            "system-view\n",
            "probe\n",
            "show int HundredGigE\n",
            "display link-state-change statistics interface HundredGigE\n",
            "display priority-flow-control interface hge\n",
            "display packet-drop interface hge\n",
            "display version\n",
            "display environment\n",
            "display power\n",
            "display fan\n",
        ]
        for slot in range(SwitchSurvey.MIN_COMWARE_SLOT, 1 + SwitchSurvey.MAX_COMWARE_SLOT):
            for port in range(SwitchSurvey.MIN_COMWARE_PORT, 1 + SwitchSurvey.MAX_COMWARE_PORT):
                comware_cmd.append(f"debug port command slot {slot} phy/fecstat/0/{port}\n")
                comware_cmd.append(f"show transceiver diagnosis interface hge {slot}/0/{port}\n")
                comware_cmd.append(f"show transceiver interface hge {slot}/0/{port}\n")
                comware_cmd.append(f"show transceiver manuinfo interface hge {slot}/0/{port}\n")

        comware_cmd.append("exit\n")  # leave probe
        comware_cmd.append("exit\n")  # leave system-view
        comware_cmd.append("exit\n")  # logout (EOF/C-d is ignored)

        # The sonic-cli command and some show sub-commands will consume all of stdin even though
        # they don't use the input. This means that typing in commands one-at-a-time works as
        # expected, but running a list of them like this (or pasting in several lines) will
        # cause command execution to stop with the connection remaining open until it times out.
        # The problem commands are wrapped in subshell calls with stdin redirection.
        # Note that EdgeCore switches have a broken sonic-cli package and don't support those
        # commands.
        sonic_cmd = [
            "intfutil -c status\n",
            "intfutil -c description\n",
            "show int counters\n",
            "echo addr cc rxb\n",  # cmd sep
            "for iface in /sys/class/net/Eth*;do iface_name=`basename $iface`; addr=$(<$iface/address); cc=$(<$iface/carrier_changes); rxb=$(<$iface/statistics/rx_bytes); echo \"$iface_name $addr $cc $rxb\";done\n",
            "echo phy\n",  # cmd sep
            "sh -c 'sonic-cli -q -b -e -c \"terminal length 0\" -c \"show interface phy counters\"' 0</dev/null\n",
            "echo dom\n",  # cmd sep
            "sh -c 'sonic-cli -q -b -e -c \"terminal length 0\" -c \"show interface transceiver dom\"' 0</dev/null\n",
            "echo eeprom\n",  # cmd sep
            "show int trans eeprom\n",
            "echo pktdrops\n",
            "sh -c 'show int pktdrops | grep -e FCS -e DISC -e RRPKT -e IFACE -e \"-----\"' 0</dev/null\n",
            "sh -c 'show pfc counter' 0</dev/null\n",
            "exit\n",
        ]

        for s in self.switches:
            if "gsw256" in s or "-mhp" in s:
                self.cmds[s] = comware_cmd
            elif s in self.arista_hostnames:
                self.cmds[s] = arista_cmd
            elif s in self.hpe_hostnames:
                self.cmds[s] = hpe_cmd
            elif s in self.sonic_hostnames:
                self.cmds[s] = sonic_cmd
            elif s in self.juniper_hostnames:
                self.cmds[s] = Juniper.cmd
            elif s in self.credo_hostnames:
                self.cmds[s] = Credo.cmd
        asyncio.get_event_loop().run_until_complete(self.run_all())

    def populate(self):
        for i, result in enumerate(self.results):
            sname = list(self.cmds)[i]
            if result and not isinstance(result,Exception):
                self.stats[sname] = SwitchInfo(sname)
                self.parse(sname, result.split("\n"))

    def parse(self, switchname, cmdoutput):
        try:
            if "gsw256" in switchname or "-mhp" in switchname:
                self.parse_comware(switchname, cmdoutput)
            elif switchname in self.hpe_hostnames:
                self.parse_hpe(switchname, cmdoutput)
            elif switchname in self.sonic_hostnames:
                self.parse_sonic(switchname, cmdoutput)
            elif switchname in self.juniper_hostnames:
                self.stats[switchname].port_list = Juniper.parse(switchname, cmdoutput)
            elif switchname in self.credo_hostnames:
                Credo.parse(switchname, cmdoutput, self.stats[switchname])
            else:
                self.parse_arista(switchname, cmdoutput)
        except KeyError as e:
            self.stats.pop(switchname, None)
            logging.error(f"Parsing failure for {switchname}: {e}")

    def parse_comware(self, switchname, cmdoutput):
        # State machine for this parser has fewer states because  lines can appear in any order -
        # sometimes MAC addr comes before bytes, but not always.

        # show int outputs a blank line after iface dump (whether one or all ifaces)
        # so does FEC state but we skip to init after reading our 2 values anyway

        ParserState = IntEnum(
            'ParserState',
            [
                'INIT',
                'START_SHOWINT',
                'IN_SHOWINT',
                'FLAPLIST',
                'PFC',
                'PKT_DROP',
                'VERSION',
                'TEMPERATURE',
                'POWER',
                'FAN',
                'START_FEC',
                'CORR',
                'UNCORR',
                'XCVR_DIAG',
                'XCVR',
                'XCVR_MANUINFO',
            ],
        )

        state = ParserState.INIT
        pname = None
        firmware_version = None
        fan_tray = None

        temperature_sensor_descs = []
        def get_unique_temperature_desc(desc):
            tmp_desc = desc
            i = 1
            # If desc already exists, append count to desc
            while tmp_desc in temperature_sensor_descs:
                i += 1
                tmp_desc = f"{desc} {i}"
            return tmp_desc

        title_re = re.compile(r'(HundredGigE\d/\d/\d+)$')  # show int doesn't end with :
        state_re = re.compile(r'Line protocol state:\s(\w+)')
        mac_re = re.compile(
            r'IP packet frame type: Ethernet II, hardware address: (\w\w\w\w-\w\w\w\w-\w\w\w\w)$'
        )
        desc_re = re.compile(r'Description: (.*)$')
        speed_re = re.compile(r'Bandwidth: (.*) kbps$')  # .*, (\d+)(.*), .*')
        in_bytes_re = re.compile(r' Input \(total\):\s*\d+ packets, (\d+) bytes')
        # out_bytes_re = re.compile(r' \s+ output, (\d+) bytes')
        data_rate_re = re.compile(
            r' Last \S+ seconds (\S+): (\S+) packets/sec (\S+) (bytes/sec) (\S+)'
        )

        input_runt_re = re.compile(r' Input:\s+(\d+) input errors, (\d+) runts, .*')
        crc_re = re.compile(r'\s+(\d+) CRC,.*')

        # fec_al_re not used in HPE
        fec_cc_re = re.compile(r'uiIntFecCorrect=0x(.+)')
        fec_uc_re = re.compile(r'uiIntFecUnCorrect=0x(.+)')

        flap_line_re = re.compile(r'HGE(\d+)/0/(\d+)\s*(\d+)')

        # show transceiver interface hge  -- will provide speed, vendor name, part number
        opt_speed_re = re.compile(r'  Transceiver Type\s+:\s+(.*)$')
        opt_vendor_re = re.compile(r'  Vendor Name\s+:\s+(.*)$')
        opt_partnum_re = re.compile(r'  Vendor Part Number\s+:\s+(.*)$')

        # show transceiver manuinfo interface -- will give Optics serial number
        opt_sernum_re = re.compile(r'  Manu. Serial Number\s+:\s+(.*)$')
        opt_no_diag_info = re.compile(r'The operation is not supported$')
        opt_no_xcvr_info = re.compile(r'The transceiver is absent.$')

        # separator string between show int and detailed output
        flapstart_re = re.compile(
            r'Interface\s+Change-times\s+Last-change-time\s+Reset link-state time'
        )

        pfc_start_re = re.compile(
            r'Interface                        AdminMode  OperMode  Dot1pList   Prio  Recv       Send       Inpps      Outpps'
        )
        pfc_line_re = re.compile(
            r'HGE(\d+)/0/(\d+)\s*\w+\s*\w+\s*\d+\s*\d+\s*(\d+)\s*(\d+)\s*\d+\s*\d+'
        )

        pkt_drop_re = re.compile(r'(HundredGigE\d/\d/\d+):$')  # show int doesn't end with :

        version_re = re.compile(r'HPE Comware Software, Version (.*), Release \S+')
        model_re = re.compile(r'HPE (.*) uptime is (.*)$')

        temperature_header_re = re.compile(r'\s*System temperature information ')
        temperature_re = re.compile(r' (\d+)\s+(.*)\s+([0-9.-]+) \s+[0-9.-]+ \s+([NA0-9.-]+) \s+([NA0-9.-]+) \s+[NA0-9.-]+')
        power_header_re = re.compile(r'\s+PowerID\s+State\s+InPower\(W\)\s+Current\(A\)\s+Voltage\(V\)\s+OutPower\(W\)\s+Type\s+Input')
        power_re = re.compile(r'\s+(\d+)\s+(.*)\s+[0-9.-]+\s+[0-9.-]+\s+[0-9.-]+\s+[0-9.-]+\s+.*\s+.*')
        fan_tray_re = re.compile(r'^\s*(Fan-tray .*):\s*$', re.IGNORECASE)
        fan_status_re = re.compile(r'^\s*Status\s*: (.*)\s*$')
        fan_rpm_re = re.compile(r'^\s*(\d+)\s+([0-9.-]+)\s*$')

        discard_re = re.compile(
            r'  Packets dropped due to insufficient data buffer. Input dropped:\s*(\d+)\s*Output dropped:\s*(\d+)'
        )

        fecstart_re = re.compile(r"---------------------")

        xcvr_diag_int_start_re = re.compile(r'(HundredGigE.*) transceiver diagnostic information:')
        opt_temp_voltage_re = re.compile(
            r'      \s+(\d+)\s+([0-9.]+)\s+[0-9.-]+\s+[0-9.-]+\s*$'
        )  # 37  3.32  6.65  5.22
        opt_rx_tx_bias_pwr_re = re.compile(
            r'    ([1-4])\s+([0-9.]+)\s+([0-9.-]+)\s+([0-9.-]+)\s*$'
        )  # 1  6.03  0.70  -0.90
        opt_alarm_thresholds_re = re.compile(
            r'  Alarm thresholds:'
        )
        xcvr_int_start_re = re.compile(r'(HundredGigE.*) transceiver information:')
        xcvr_manuinfo_int_start_re = re.compile(
            r'(HundredGigE.*) transceiver manufacture information:'
        )

        detail_slot = SwitchSurvey.MIN_COMWARE_SLOT
        detail_idx = SwitchSurvey.MIN_COMWARE_PORT

        for line in cmdoutput:
            line = line.strip('\r\n')
            # print("COMWARE switchname=%s state=%d got line: %r" % (switchname,state,line))
            if line == '':
                state = ParserState.INIT
                res = pname = None
                continue

            if state == ParserState.INIT:
                res = title_re.match(line)
                if res:
                    pname = res.group(1)
                    state = ParserState.START_SHOWINT
                    continue
                res = flapstart_re.match(line)
                if res:
                    state = ParserState.FLAPLIST
                    continue
                res = pkt_drop_re.match(line)
                if res:
                    state = ParserState.PKT_DROP
                    pname = res.group(1)
                    continue
                res = version_re.match(line)
                if res:
                    state = ParserState.VERSION
                    firmware_version = res.group(1).strip()
                    continue
                res = temperature_header_re.match(line)
                if res:
                    state = ParserState.TEMPERATURE
                    continue
                res = power_header_re.match(line)
                if res:
                    state = ParserState.POWER
                    continue
                res = fan_tray_re.match(line)
                if res:
                    state = ParserState.FAN
                    fan_tray = res.group(1).strip()
                    continue
                res = fecstart_re.match(line)
                if res:
                    state = ParserState.START_FEC
                    continue
                res = xcvr_diag_int_start_re.match(line)
                if res:
                    pname = res.group(1)
                    state = ParserState.XCVR_DIAG
                    continue
                res = xcvr_int_start_re.match(line)
                if res:
                    pname = res.group(1)
                    state = ParserState.XCVR
                    continue
                res = xcvr_manuinfo_int_start_re.match(line)
                if res:
                    pname = res.group(1)
                    state = ParserState.XCVR_MANUINFO
                    continue

            elif state == ParserState.START_SHOWINT:
                if self.exclude_not_in_nj and not (
                    self.switches.get(switchname) and pname in self.switches[switchname]
                ):
                    # it's not in net.json and we require that, so skip this iface block
                    title_re = None
                    state = ParserState.INIT
                else:
                    self.stats[switchname].port_list[pname] = PortInfo(pname)
                    state = ParserState.IN_SHOWINT

            elif state == ParserState.IN_SHOWINT:
                res = state_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].oper_status = (
                        1 if res.group(1).lower() == "up" else 0
                    )
                    continue
                res = desc_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].description = res.group(1)
                    continue
                res = mac_re.match(line)
                if res:
                    m = res.group(1)
                    mac = f"{m[0]}{m[1]}:{m[2]}{m[3]}:{m[5]}{m[6]}:{m[7]}{m[8]}:{m[10]}{m[11]}:{m[12]}{m[13]}"
                    self.stats[switchname].port_list[pname].mac = mac
                    continue
                res = speed_re.match(line)
                if res:
                    speedstr = res.group(1)
                    self.stats[switchname].port_list[pname].speed = int(int(speedstr) / 1000)
                    continue
                res = in_bytes_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].rx_bytes = int(res.group(1))
                    continue
                res = data_rate_re.match(line)
                if res:
                    direction = res.group(1)
                    rate = res.group(3)
                    unit = res.group(4)
                    rate = data_rate_to_kbps_converter(float(rate), unit)
                    if direction == "input":
                        self.stats[switchname].port_list[pname].rx_kbps = int(rate)
                        continue
                    else:
                        self.stats[switchname].port_list[pname].tx_kbps = int(rate)
                        continue
                res = input_runt_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].input_errors = int(res.group(1))
                    self.stats[switchname].port_list[pname].runts = int(res.group(2))
                    continue
                res = crc_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].crc_errors = int(res.group(1))
                    continue
                # could check here - if found all 6 items, go to INIT and stop checking these

            elif state == ParserState.FLAPLIST:
                res = flap_line_re.match(line)
                if res:
                    pname = f"HundredGigE{res.group(1)}/0/{res.group(2)}"
                    self.stats[switchname].port_list[pname].carrier_changes = int(res.group(3))
                    continue
                res = pfc_start_re.match(line)
                if res:
                    state = ParserState.PFC
                    continue

            elif state == ParserState.PFC:
                res = pfc_line_re.match(line)
                if res:
                    pname = f"HundredGigE{res.group(1)}/0/{res.group(2)}"
                    self.stats[switchname].port_list[pname].rx_pfc = int(res.group(3))
                    self.stats[switchname].port_list[pname].tx_pfc = int(res.group(4))

            elif state == ParserState.PKT_DROP:
                res = fecstart_re.match(line)
                if res:
                    state = ParserState.START_FEC
                    continue

                res = discard_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].in_discards = int(res.group(1))
                    self.stats[switchname].port_list[pname].out_discards = int(res.group(2))

            elif state == ParserState.VERSION:
                res = model_re.match(line)
                if res:
                    model = res.group(1).strip()
                    uptime = pytimeparse2.parse(res.group(2)) or 0
                    self.stats[switchname].switch_stats['version_info'].append(dict(
                        vendor='HPE', model=model, firmware_version=firmware_version, val=1))
                    self.stats[switchname].switch_stats['uptime'].append(dict(val=uptime))

            elif state == ParserState.TEMPERATURE:
                res = temperature_re.match(line)
                if res:
                    slot = res.group(1)
                    sensor = re.sub(' +', ' ', res.group(2).strip())
                    temp_c = floatOrMinusOne(res.group(3))
                    warn_thresh = floatOrMinusOne(res.group(4))
                    alarm_thresh = floatOrMinusOne(res.group(5))
                    desc = f'slot {slot} {sensor}'
                    desc = get_unique_temperature_desc(desc)
                    temperature_sensor_descs.append(desc)
                    if temp_c < 0:
                        health = 3 # unknown
                    elif alarm_thresh > 0 and alarm_thresh < temp_c:
                        health = 2 # error
                    elif warn_thresh > 0 and warn_thresh < temp_c:
                        health = 1 # warn
                    elif alarm_thresh < 0 or warn_thresh < 0:
                        health = 3 # unknown
                    else:
                        health = 0 # ok
                    self.stats[switchname].switch_stats['temperature_health'].append(dict(
                        desc=desc, val=health))
                    self.stats[switchname].switch_stats['temperature'].append(dict(
                        desc=desc, val=temp_c))
                    continue

            elif state == ParserState.POWER:
                res = power_re.match(line)
                if res:
                    power_id = res.group(1)
                    psu_state = res.group(2).strip().lower()
                    desc = f'PSU{power_id}'
                    # https://www.h3c.com/en/d_202305/1855563_294551_0.htm
                    if psu_state == 'absent':
                        health = 1 # warn
                    elif psu_state == 'normal':
                        health = 0 # ok
                    elif psu_state == 'fault':
                        health = 2 # error
                    elif psu_state == 'sleeping':
                        health = 3 # unknown
                    else:
                        health = 3 # unknown
                    self.stats[switchname].switch_stats['psu_health'].append(dict(
                        desc=desc, val=health))
                    continue

            elif state == ParserState.FAN:
                # The first fan_tray is parsed in the INIT state above
                res = fan_tray_re.match(line)
                if res:
                    fan_tray = res.group(1).strip()
                    continue
                res = fan_status_re.match(line)
                if res:
                    status = res.group(1).strip().lower()
                    if status == 'absent':
                        health = 1 # warn
                    elif status == 'normal':
                        health = 0 # ok
                    elif status == 'fault':
                        health = 2 # error
                    else:
                        health = 3 # unknown
                    self.stats[switchname].switch_stats['fan_health'].append(dict(
                        desc=fan_tray, val=health))
                    continue
                res = fan_rpm_re.match(line)
                if res:
                    fan_id = res.group(1).strip()
                    rpm = floatOrMinusOne(res.group(2))
                    desc = f'{fan_tray} / {fan_id}'
                    self.stats[switchname].switch_stats['fan_rpm'].append(dict(
                        desc=desc, val=rpm))
                    continue

            elif state == ParserState.START_FEC:
                # Maybe check if p=(\d+) field matches detail_idx
                pname = f"HundredGigE{detail_slot}/0/{detail_idx}"
                detail_idx += 1
                if detail_idx == SwitchSurvey.MAX_COMWARE_PORT + 1:
                    detail_idx = SwitchSurvey.MIN_COMWARE_PORT
                    detail_slot += 1
                state = ParserState.CORR

            elif state == ParserState.CORR:
                res = fec_cc_re.match(line)
                if res:
                    if res.group(1) == "ffffffff":
                        logger.info("{switchname}:{pname} FEC corr counter value -1, ignoring")
                        self.stats[switchname].port_list[pname].fec_corrected_codewords = 0
                        state = ParserState.UNCORR
                        continue

                    self.stats[switchname].port_list[pname].fec_corrected_codewords = int(
                        res.group(1), 16
                    )
                    state = ParserState.UNCORR

            elif state == ParserState.UNCORR:
                res = fec_uc_re.match(line)
                if res:
                    if res.group(1) == "ffffffff":
                        logger.info("{switchname}:{pname} FEC uncorr counter value -1, ignoring")
                        self.stats[switchname].port_list[pname].fec_uncorrected_codewords = 0
                        state = ParserState.INIT
                        continue

                    self.stats[switchname].port_list[pname].fec_uncorrected_codewords = int(
                        res.group(1), 16
                    )
                    state = ParserState.INIT

            elif state == ParserState.XCVR_DIAG:
                # pname is set before entering here

                # this marks end of channel list
                res = opt_alarm_thresholds_re.match(line)
                if res:
                    state = ParserState.INIT

                res = opt_temp_voltage_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].opt_temperature = float(res.group(1))
                    self.stats[switchname].port_list[pname].opt_voltage = float(res.group(2))
                    continue
                res = opt_rx_tx_bias_pwr_re.match(line)
                if res:
                    chan = res.group(1)
                    self.stats[switchname].port_list[pname].opt_tx_current[chan] = float(
                        res.group(2)
                    )
                    self.stats[switchname].port_list[pname].opt_rx_power[chan] = float(
                        res.group(3)
                    )
                    self.stats[switchname].port_list[pname].opt_tx_power[chan] = float(
                        res.group(4)
                    )

            elif state == ParserState.XCVR:
                # pname is set before entering here
                res = opt_speed_re.match(line)
                if res:
                    logger.info("{switchname}:{pname} got speed line, val={res.group(1)}")
                    self.stats[switchname].port_list[pname].opt_speed = res.group(1)
                    continue
                res = opt_vendor_re.match(line)
                if res:
                    logger.info("{switchname}:{pname} got vendor line, val={res.group(1)}")
                    self.stats[switchname].port_list[pname].opt_vendor = res.group(1)
                    continue
                res = opt_partnum_re.match(line)
                if res:
                    logger.info("{switchname}:{pname} got speed line, val={res.group(1)}")
                    self.stats[switchname].port_list[pname].opt_partnum = res.group(1)
                    state = ParserState.INIT
                    continue

            elif state == ParserState.XCVR_MANUINFO:
                # pname is set before entering here
                res = opt_sernum_re.match(line)
                if res:
                    logger.info("{switchname}:{pname} got sernum line, val={res.group(1)}")
                    self.stats[switchname].port_list[pname].opt_sernum = res.group(1)
                    state = ParserState.INIT

    def parse_hpe(self, switchname, cmdoutput):
        ParserState = IntEnum(
            'ParserState',
            [
                'START_SHOWINT',
                'STATE_OR_INT_ETH_COUNT',
                'LINKSTATE',
                'DESC',
                'MAC',
                'SPEED',
                'BYTES',
                'RX_ERRORS',
                'CRC',
                'RUNTS',
                'PFC',
                'PFC_RX',
                'PFC_TX',
                'TC',
                'RATES',
                'VERSION',
                'INVENTORY',
                'POWER',
                'TEMPERATURE',
                'FAN',
                'XCVR_ETHTYPE',
                'XCVR_VENDOR',
                'XCVR_PN',
                'XCVR_REV',
                'XCVR_SN',
                'XCVR_DIAG_TEMP',
                'XCVR_DIAG_VOLT',
                'XCVR_DIAG_TX_CURRENT',
                'XCVR_DIAG_TX_POWER',
                'XCVR_DIAG_RX_POWER',
                'XCVR_COUNT',
                'UNCORR',
                'CORR',
            ],
        )

        state = ParserState.START_SHOWINT
        pname = None

        title_re = re.compile(r'(Eth1/.*):')
        state_re = re.compile(r'  Operational state\s+:\s(\w+)')
        mac_re = re.compile(r'  Mac address\s*: (\w\w:\w\w:\w\w:\w\w:\w\w:\w\w)\s*$')
        desc_re = re.compile(r'  Description\s*: (.*)$')
        mtu_re = re.compile(r'  Ethernet MTU.*')
        speed_re = re.compile(r'  Actual speed\s+: (.*)$')  # .*, (\d+)(.*), .*')
        lsc_re = re.compile(r'  Last change in operational status: (.*$)')
        in_bytes_re = re.compile(r'    (\d+)\s*bytes')
        # out_bytes_re = re.compile(r' \s+ output, (\d+) bytes')
        rx_errors_re = re.compile(r'    (\d+)\s*error packets')
        crc_re = re.compile(r'    (\d+)\s*fcs errors')
        runts_re = re.compile(r'    (\d+)\s*undersize packets')

        fec_cc_re = re.compile(r'phy corrected bits\s*(\d+)')
        fec_uc_re = re.compile(r'phy symbol errors\s*(\d+)')

        # separator string between show int and detailed output
        cmdsep_re = re.compile(r'Domain names:')

        title_xcvr_re = re.compile(r'Port ([0-9/]+) state')
        title_xcvr_diag_re = re.compile('Port ([0-9/]+) transceiver diagnostic data:')

        # transceiver details
        opt_speed_re = re.compile(r'	ethernet speed and type: (.*)$')
        opt_vendor_re = re.compile(r'	vendor                 : (.*)$')
        opt_partnum_re = re.compile(r'	part number            : (.*)$')
        opt_rev_re = re.compile(r'	revision               : (.*)$')
        opt_sernum_re = re.compile(r'	serial number          : (.*)$')
        opt_no_diag_info = re.compile(r'.*No Diagnostic Data Available.*$')
        opt_temp_re = re.compile(r'\s+Temperature\s+: ([0-9]+) C$')
        opt_volt_re = re.compile(r'\s+Voltage\s+: ([0-9.]+) V$')
        opt_tx_curr_re = re.compile(r'\s+Ch(\d+)\s+Tx\s+Current\s+:\s+(.*)\s+mA$')
        opt_tx_curr_na_re = re.compile(r'\s+Ch(\d+)\s+Tx\s+Current\s+:\s+NA$')
        opt_tx_power_header_re = re.compile(r'\s+Tx Power .*')
        opt_tx_pwr_re = re.compile(r'\s+Ch(\d+)\s+Tx\s+Power\s+:\s+.*\s+mW / ([0-9.-]+)\s+dBm$')
        opt_rx_power_header_re = re.compile(r'\s+Rx Power .*')
        opt_rx_pwr_re = re.compile(r'\s+Ch(\d+)\s+Rx\s+Power\s+:\s+.*\s+mW / ([0-9.-]+)\s+dBm$')
        opt_vendor_date_code_re = re.compile(r'\s+Vendor Date Code.*')
        pfc_rx_mode_re = re.compile(r'\s+Rx')
        pfc_tx_mode_re = re.compile(r'\s+Tx')
        pfc_count_re = re.compile(r'\s+(\S+)\s+pause packets')
        discard_count_re = re.compile(r'\s+(\S+)\s+unicast no buffer discard')
        data_rate_re = re.compile(r'(\S+)\s+(\S+)\s+(\S+)\s+\S+\s+(\S+)\s+(\S+)\s+\S+')
        product_re = re.compile(r'Product name: .*')
        release_re = re.compile(r'Product release: \s*(\S+)\s*')
        uptime_re = re.compile(r'Uptime: \s*(.*)$')
        inventory_header_re = re.compile(r'Module\s+Part Number\s+Serial Number\s+Asic Rev.\s+HW Rev.')
        chassis_re = re.compile(r'CHASSIS\s+(\S+)\s+\S+\s+\S+\s+\S+')
        power_re = re.compile(r'(\S+)\s+(.*)\s+(\S+)\s+([0-9.-]+)\s+([0-9.-]+)\s+([0-9.-]+)\s+([0-9.-]+)\s+(\S+)\s+(.+)\s*$')
        temp_re = re.compile(r'(\S+)\s+(.*)\s+(\S+)\s+([0-9.-]+)\s+(.+)\s*$')
        fan_re = re.compile(r'(\S+)\s+(.*)\s+(\S+)\s+([0-9.-]+)\s+(.+)\s*$')

        pfc_re = re.compile(r'  PFC 5:')
        tc_re = re.compile(r'  TC 5:')
        rates_re = re.compile(
            r'Port           ingress          ingress          egress          egress'
        )
        power_header_re = re.compile(
            r'Module \s+Device \s+Sensor \s+Power \s+Voltage \s+Current \s+Capacity \s+Feed \s+Status'
        )
        temp_header_re = re.compile(
            r'Module \s+Component \s+Reg \s+CurTemp \s+Status'
        )
        fan_header_re = re.compile(
            r'Module \s+Device \s+Fan \s+Speed \s+Status'
        )
        title_res = None
        mac = None
        opstate = None
        carrier_changes = None
        firmware_version = None
        detail_idx = 1

        def encodeHealth(status: str) -> int:
            if status == 'ok':
                return 0
            elif status == 'not present':
                return 1 # warn
            # I couldn't find the list of possible statuses, so be conservative:
            # everything else is an error:
            return 2 # error

        for line in cmdoutput:
            line = line.strip('\r\n')
            # print("HPE state=%d got line: %r" % (state,line))
            if line == '':
                continue
            if state == ParserState.START_SHOWINT:
                res = title_re.match(line)
                if res:
                    title_res = res
                    pname = res.group(1)
                    if self.exclude_not_in_nj and not (
                        self.switches.get(switchname) and pname in self.switches[switchname]
                    ):
                        # it's not in net.json and we require that, so skip this iface block
                        state = ParserState.START_SHOWINT
                    else:
                        state = ParserState.STATE_OR_INT_ETH_COUNT
                    continue
                res = rates_re.match(line)
                if res:
                    state = ParserState.RATES
                    continue
                res = title_xcvr_re.match(line)
                if res:
                    state = ParserState.XCVR_ETHTYPE
                    pname = "Eth" + res.group(1)
                    continue
                res = title_xcvr_diag_re.match(line)
                if res:
                    state = ParserState.XCVR_DIAG_TEMP
                    pname = "Eth" + res.group(1)
                    continue
                res = cmdsep_re.match(line)
                if res:
                    # Found the output separator, next line will be show int phy detail output
                    state = ParserState.XCVR_COUNT
                    continue

            elif state == ParserState.STATE_OR_INT_ETH_COUNT:
                res = state_re.match(line)
                if res:
                    opstate = 1 if res.group(1).lower() == "up" else 0
                    state = ParserState.LINKSTATE
                    continue

                res = pfc_re.match(line)
                if res:
                    state = ParserState.PFC
                    continue

                res = tc_re.match(line)
                if res:
                    state = ParserState.TC
                    continue

            elif state == ParserState.LINKSTATE:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = lsc_re.match(line)
                if res:
                    if res.group(1) == "Never":
                        carrier_changes = 0
                    else:
                        ccres = re.match(r'.* ago \((\d+) oper change.*', res.group(1))
                        if ccres:
                            carrier_changes = int(ccres.group(1))
                        else:
                            carrier_changes = -1
                            logger.error(
                                "Unexpected error when parsing switch status: couldn't find carrier changes message"
                            )

                    state = ParserState.DESC

            elif state == ParserState.DESC:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = desc_re.match(line)
                if res:
                    self.stats[switchname].port_list[title_res.group(1)] = PortInfo(pname)
                    self.stats[switchname].port_list[pname].oper_status = opstate
                    self.stats[switchname].port_list[pname].description = res.group(1)
                    self.stats[switchname].port_list[pname].mac = mac
                    self.stats[switchname].port_list[pname].carrier_changes = carrier_changes

                    state = ParserState.MAC
                else:
                    res = mtu_re.match(line)
                    # looking for Description: but found MTU line, so this port doesn't have one
                    if res:
                        if self.exclude_unlabel and not (
                            self.switches.get(switchname) and pname in self.switches[switchname]
                        ):
                            # No description and it's not in net.json, so don't add
                            state = ParserState.START_SHOWINT
                        else:
                            # But add it anyway
                            self.stats[switchname].port_list[title_res.group(1)] = PortInfo(pname)
                            self.stats[switchname].port_list[pname].oper_status = opstate
                            self.stats[switchname].port_list[pname].description = ""
                            self.stats[switchname].port_list[
                                pname
                            ].carrier_changes = carrier_changes

                            state = ParserState.MAC

            elif state == ParserState.MAC:
                res = mac_re.match(line)
                if res:
                    mac = res.group(1)
                    self.stats[switchname].port_list[pname].mac = mac
                    state = ParserState.SPEED
                else:
                    state = ParserState.START_SHOWINT

            elif state == ParserState.SPEED:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = speed_re.match(line)
                if res:
                    speedstr = res.group(1)
                    # can be Unknown or \d+G.*
                    if speedstr == "Unknown":
                        self.stats[switchname].port_list[pname].speed = None
                    else:
                        speed = re.match(r'(\d+)G.*', speedstr)
                        self.stats[switchname].port_list[pname].speed = int(speed.group(1)) * 1000
                    state = ParserState.BYTES

            elif state == ParserState.BYTES:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = in_bytes_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].rx_bytes = int(res.group(1))
                    state = ParserState.RX_ERRORS

            elif state == ParserState.RX_ERRORS:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = rx_errors_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].input_errors = int(res.group(1))
                    state = ParserState.CRC

            elif state == ParserState.CRC:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = crc_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].crc_errors = int(res.group(1))
                    state = ParserState.RUNTS

            elif state == ParserState.RUNTS:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = runts_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].runts = int(res.group(1))
                    state = ParserState.START_SHOWINT

            elif state == ParserState.PFC:
                res = pfc_rx_mode_re.match(line)
                if res:
                    state = ParserState.PFC_RX
                    continue

                res = pfc_tx_mode_re.match(line)
                if res:
                    state = ParserState.PFC_TX
                    continue

            elif state == ParserState.PFC_RX:
                res = pfc_count_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].rx_pfc = int(res.group(1))
                    state = ParserState.PFC

            elif state == ParserState.PFC_TX:
                res = pfc_count_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].tx_pfc = int(res.group(1))
                    state = ParserState.START_SHOWINT

            elif state == ParserState.TC:
                res = discard_count_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].in_discards = int(res.group(1))
                    state = ParserState.START_SHOWINT

            elif state == ParserState.RATES:
                #        data_rate_re = re.compile(r'(\S+)\s+(\S+)\s+(\S+)\s+\S+\s+(\S+)\s+(\S+)\s+\S+')
                # what is end condition? hosts
                res = product_re.match(line)
                if res:
                    # Found the version section
                    state = ParserState.VERSION
                    continue
                res = data_rate_re.match(line)
                if res:
                    port = res.group(1)
                    if not port.startswith("Eth"):
                        continue
                    in_rate = float(res.group(2))
                    in_unit = res.group(3)
                    in_rate = data_rate_to_kbps_converter(in_rate, in_unit)
                    out_rate = float(res.group(4))
                    out_unit = res.group(5)
                    out_rate = data_rate_to_kbps_converter(out_rate, out_unit)
                    self.stats[switchname].port_list[port].rx_kbps = int(in_rate)
                    self.stats[switchname].port_list[port].tx_kbps = int(out_rate)
                    continue

            elif state == ParserState.VERSION:
                res = inventory_header_re.match(line)
                if res:
                    # Found the inventory header
                    state = ParserState.INVENTORY
                    continue
                res = release_re.match(line)
                if res:
                    firmware_version = res.group(1)
                    continue
                res = uptime_re.match(line)
                if res:
                    uptime = pytimeparse2.parse(res.group(1)) or 0
                    self.stats[switchname].switch_stats['uptime'].append(dict(val=uptime))
                    continue

            elif state == ParserState.INVENTORY:
                res = power_header_re.match(line)
                if res:
                    # Found the power header
                    state = ParserState.POWER
                    continue
                res = chassis_re.match(line)
                if res:
                    model = res.group(1)
                    self.stats[switchname].switch_stats['version_info'].append(dict(
                        vendor='HPE', model=model, firmware_version=firmware_version, val=1))
                    continue

            elif state == ParserState.POWER:
                res = temp_header_re.match(line)
                if res:
                    # Found the temperature header
                    state = ParserState.TEMPERATURE
                    continue
                res = power_re.match(line)
                if res:
                    module_name = res.group(1)
                    status = res.group(9).strip().lower()
                    health = encodeHealth(status)
                    self.stats[switchname].switch_stats['psu_health'].append(dict(
                        desc=module_name, val=health))
                    continue

            elif state == ParserState.TEMPERATURE:
                res = fan_header_re.match(line)
                if res:
                    # Found the fans header
                    state = ParserState.FAN
                    continue
                res = temp_re.match(line)
                if res:
                    module_name = res.group(1)
                    component = res.group(2).strip()
                    reg = res.group(3).strip()
                    temp_c = floatOrMinusOne(res.group(4))
                    status = res.group(5).strip().lower()
                    health = encodeHealth(status)
                    desc = f'{module_name} {component} {reg}'
                    self.stats[switchname].switch_stats['temperature_health'].append(dict(
                        desc=desc, val=health))
                    self.stats[switchname].switch_stats['temperature'].append(dict(
                        desc=desc, val=temp_c))
                    continue

            elif state == ParserState.FAN:
                res = fan_re.match(line)
                if res:
                    module_name = res.group(1)
                    device = res.group(2).strip()
                    fan = res.group(3).strip()
                    rpm = floatOrMinusOne(res.group(4))
                    status = res.group(5).strip().lower()
                    health = encodeHealth(status)
                    desc = f'{module_name} {device} {fan}'
                    self.stats[switchname].switch_stats['fan_health'].append(dict(
                        desc=desc, val=health))
                    self.stats[switchname].switch_stats['fan_rpm'].append(dict(
                        desc=desc, val=rpm))
                    continue
                res = cmdsep_re.match(line)
                if res:
                    # Found the output separator, go back to main start state
                    state = ParserState.START_SHOWINT
                    continue

            elif state == ParserState.XCVR_ETHTYPE:
                if 'Cable is not present' in line:
                    state = ParserState.START_SHOWINT
                    continue
                res = opt_speed_re.match(line)
                if res:
                    if pname not in self.stats[switchname].port_list:
                        self.stats[switchname].port_list[pname] = PortInfo(pname)
                    self.stats[switchname].port_list[pname].opt_speed = res.group(1).strip()
                    state = ParserState.XCVR_VENDOR

            elif state == ParserState.XCVR_VENDOR:
                res = opt_vendor_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].opt_vendor = res.group(1).strip()
                    state = ParserState.XCVR_PN

            elif state == ParserState.XCVR_PN:
                res = opt_partnum_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].opt_partnum = res.group(1).strip()
                    state = ParserState.XCVR_REV

            elif state == ParserState.XCVR_REV:
                res = opt_rev_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].opt_rev = res.group(1).strip()
                    state = ParserState.XCVR_SN

            elif state == ParserState.XCVR_SN:
                res = opt_sernum_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].opt_sernum = res.group(1).strip()
                    state = ParserState.START_SHOWINT

            elif state == ParserState.XCVR_DIAG_TEMP:
                # TODO: Not sure how this command handles the unconnected port case
                if 'Cable is not present' in line:
                    state = ParserState.START_SHOWINT
                    continue
                res = opt_no_diag_info.match(line)
                if res:
                    state = ParserState.START_SHOWINT
                    continue

                res = opt_temp_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].opt_temperature = float(res.group(1))
                    state = ParserState.XCVR_DIAG_VOLT

            elif state == ParserState.XCVR_DIAG_VOLT:
                res = opt_volt_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].opt_voltage = float(res.group(1))
                    state = ParserState.XCVR_DIAG_TX_CURRENT

            elif state == ParserState.XCVR_DIAG_TX_CURRENT:
                res = opt_tx_curr_na_re.match(line)
                if res:
                    state = ParserState.START_SHOWINT
                    continue

                res = opt_tx_power_header_re.match(line)
                if res:
                    state = ParserState.XCVR_DIAG_TX_POWER
                    continue

                res = opt_tx_curr_re.match(line)
                if res:
                    lane = res.group(1)
                    tx_curr = float(res.group(2))
                    self.stats[switchname].port_list[pname].opt_tx_current[lane] = tx_curr

            elif state == ParserState.XCVR_DIAG_TX_POWER:
                res = opt_rx_power_header_re.match(line)
                if res:
                    state = ParserState.XCVR_DIAG_RX_POWER
                    continue

                res = opt_tx_pwr_re.match(line)
                if res:
                    lane = res.group(1)
                    tx_pwr = float(res.group(2))
                    self.stats[switchname].port_list[pname].opt_tx_power[lane] = tx_pwr

            elif state == ParserState.XCVR_DIAG_RX_POWER:
                res = opt_vendor_date_code_re.match(line)
                if res:
                    state = ParserState.START_SHOWINT
                    continue
                res = opt_rx_pwr_re.match(line)
                if res:
                    lane = res.group(1)
                    rx_pwr = float(res.group(2))
                    self.stats[switchname].port_list[pname].opt_rx_power[lane] = rx_pwr

            # If link is down-ish, FEC errors don't get reported. Make sure we aren't missing
            # things- go back to XCVR_COUNT if we don't think we'll find FEC errors.
            elif state == ParserState.XCVR_COUNT:
                line = line.rstrip()
                if line == "Rx":  # in self.stats[switchname].port_list:
                    pname = f"Eth1/{detail_idx}"
                    detail_idx += 1
                    state = ParserState.UNCORR

            elif state == ParserState.UNCORR:
                res = fec_uc_re.match(line)
                if res:
                    try:
                        self.stats[switchname].port_list[pname].fec_uncorrected_codewords = int(
                            res.group(1)
                        )
                    except KeyError:
                        logging.error(
                            f"Parse failure: HPE switch has FEC stats for switch port {switchname}:{pname} but it wasn't in 'show int' output - hardware failure on switch?"
                        )
                    state = ParserState.CORR

            elif state == ParserState.CORR:
                res = fec_cc_re.match(line)
                if res:
                    try:
                        self.stats[switchname].port_list[pname].fec_corrected_codewords = int(
                            res.group(1)
                        )
                    except KeyError:
                        logging.error(
                            f"Parse failure: HPE switch has FEC stats for switch port {switchname}:{pname} but it wasn't in 'show int' output - hardware failure on switch?"
                        )
                    state = ParserState.XCVR_COUNT

    def parse_arista(self, switchname, cmdoutput):
        ParserState = IntEnum(
            'ParserState',
            [
                'START_SHOWINT',
                'MAC',
                'DESC',
                'SPEED',
                'LINKSTATE',
                'BYTES',
                'RUNTS',
                'CRC',
                'START_PHYDETAIL',
                'XCVR_DOM',
                'OPT_TEMP',
                'OPT_VOLT',
                'OPT_ETHNAME',
                'OPT_TX_CURR',
                'OPT_TX_PWR',
                'OPT_RX_PWR',
                'PFC',
                'IFINDEX',
                'DISCARDS',
                'QUEUE_DETAIL',
                'DATA_RATE',
                'ERRDISABLED',
                'VERSION',
                'PSU',
                'TEMPERATURE',
                'COOLING',
                'ECN_TC',
            ],
        )

        state = ParserState.START_SHOWINT
        pname = None

        # arista show int regexps
        title_re = re.compile(r'(Ethernet.*) is ([\w ]+), line protocol is (\w+) \((\w+)\)')
        mac_re = re.compile(
            r'  Hardware is Ethernet, address is (\w\w\w\w).(\w\w\w\w).(\w\w\w\w).*'
        )
        desc_re = re.compile(r'  Description: (.*)$')
        mtu_re = re.compile(r'  (IP|Ethernet) MTU.*')
        speed_re = re.compile(r'  .*, (\d+)(\w+/s),? .*')
        lsc_re = re.compile(r'  (\d+) link status changes since last clear')
        in_bytes_re = re.compile(r' \s+ \d+ packets input, (\d+) bytes')
        # out_bytes_re = re.compile(r' \s+ output, (\d+) bytes')

        runts_re = re.compile(r' \s+(\d+) runts, .*')

        crc_re = re.compile(r' \s+ (\d+) input errors, (\d+) CRC, .*')

        # arista show int phy detail regexps
        # extra spaces after Transceiver in opt_speed_re are necessary to avoid capturing other
        # Transceiver lines
        opt_speed_re = re.compile(r'\s+Transceiver   \s+(.*)\s+\d+\s+\d+\s+days, [0-9:]+ ago')
        opt_sernum_re = re.compile(r'\s+Transceiver\s+SN\s+(.*)\s*$')
        opt_partnum_re = re.compile(r'\s*Model\s+(.*)$')
        # phy_state_re = re.compile(r'\s+PHY state\s+(\w+).*')
        fec_al_re = re.compile(r'\s*FEC alignment lock\s+(\w+).*')
        fec_cc_re = re.compile(r'\s*FEC corrected codewords\s+(\d+)\s+(\d+)')
        fec_uc_re = re.compile(r'\s*FEC uncorrected codewords\s+(\d+)\s+(\d+)')
        # fec_lcs0_re = re.compile(r'\s*Lane 0\s+(\d+)\s+(\d+)')
        # fec_lcs1_re = re.compile(r'\s*Lane 1\s+(\d+)\s+(\d+)')
        # fec_lcs2_re = re.compile(r'\s*Lane 2\s+(\d+)\s+(\d+)')
        # fec_lcs3_re = re.compile(r'\s*Lane 3\s+(\d+)\s+(\d+)')

        # separator string between show int and detailed output
        cmdsep_re = re.compile(r'FQDN: .*')

        int_trans_dom_re = re.compile(
            r'Ch: Channel, N/A: not applicable, TX: transmit, RX: receive'
        )
        dom_port_re = re.compile(r'Port (\d+)$')
        opt_temp_re = re.compile(r'\s+Temperature\s+([0-9.]+) C$')
        missing_opt_temp_re = re.compile(r'\s+Temperature\s+N/A N/A$')
        opt_volt_re = re.compile(r'\s+Voltage\s+([0-9.]+) V$')
        opt_tx_curr_re = re.compile(r'\s+TX bias current$')
        opt_tx_pwr_re = re.compile(r'\s+Optical TX power$')
        opt_rx_pwr_re = re.compile(r'\s+Optical RX power$')
        opt_chan_re = re.compile(r'\s+Channel\s+(\d+)\s+((?:[0-9.-]+)|N/A)\s+(mA|dBm)$')

        opt_tx_curr_nochan_re = re.compile(r'\s+TX bias current\s+([0-9.-]+) mA$')
        opt_tx_power_header_re = re.compile(r'\s+Optical TX power.*')
        opt_tx_pwr_nochan_re = re.compile(r'\s+Optical TX power\s+([0-9.-]+) dBm$')
        opt_rx_power_header_re = re.compile(r'\s+Optical RX power.*')
        opt_rx_pwr_nochan_re = re.compile(r'\s+Optical RX power\s+([0-9.-]+) dBm$')

        pfc_header_re = re.compile(r'Port\s+RxPfc\s+TxPfc')
        pfc_discard_re = re.compile(r'(\S+)\s+(\d+)\s+(\d+)')
        ifindex_mapping_re = re.compile(r'(\S+):\s+Ifindex\s+=\s+(\d+)')
        count_discard_re = re.compile(r'Port\s+InDiscards\s+OutDiscards')

        queue_detail_header_re = re.compile(r'Port\s+TxQ\s+Counter/pkts\s+Counter/bytes\s+Drop/pkts\s+Drop/bytes')
        queue_detail_re = re.compile(r'(\S+)\s+(\S+)\s+\d+\s+\d+\s+(\d+)\s+\d+')

        counters_rates_re = re.compile(
            r'Port\s+Name\s+Intvl\s+In Mbps\s+%\s+In Kpps\s+Out Mbps\s+%\s+Out Kpps'
        )
        # data_rate_re = re.compile(r'(\w+)\s+.*\s+\S+\s+(\S+)\s+.*%\s+\S+\s+(\S+)\s+.*%')
        data_rate_re = re.compile(
            r'(Et\d+(?:/\d)?)\s+.*\s+\d+:\d+\s+([0-9-.]+)\s+[0-9-.]+%\s+\d+\s+([0-9-.]+)\s+[0-9-.]+%\s+\d+$'
        )

        errdisabled_re = re.compile(r"\s*(Et\d+(?:/\d)?) \s+.* \s+errdisabled \s+(.*)")

        json_accum = ""

        ecn_tc_re = re.compile(r'\s+(\d)\s+(\d+)$')

        ip_addr_re = re.compile(r'  Internet address .*')
        eth_at_start_re = re.compile(r'^(Ethernet(\d+/\d)?)')
        opt_revision_re = re.compile(r'^  Firmware revision\s+(.*)')
        stale_warning_re = re.compile(r'\*\*\* Warning: some information is stale')

        title_res = None
        mac = None
        opstate = None
        for line in cmdoutput:
            line = line.strip('\r\n')
            # print("ARISTA state=%d got line: %r" % (state,line))
            if state == ParserState.START_SHOWINT:
                res = title_re.match(line)
                if res:
                    title_res = res
                    pname = res.group(1)
                    opstate = 1 if res.group(2).lower() == "up" else 0
                    if self.exclude_not_in_nj and not (
                        self.switches.get(switchname) and pname in self.switches[switchname]
                    ):
                        # it's not in net.json and we require that, so skip this iface block
                        state = ParserState.START_SHOWINT
                    else:
                        state = ParserState.MAC
                else:
                    res = cmdsep_re.match(line)
                    if res:
                        # Found the output separator, next line will be show int phy detail output
                        state = ParserState.START_PHYDETAIL
                        pname = None
                        continue

            elif state == ParserState.MAC:
                res = mac_re.match(line)
                if res:
                    m = res.group(1) + res.group(2) + res.group(3)
                    mac = f"{m[0]}{m[1]}:{m[2]}{m[3]}:{m[4]}{m[5]}:{m[6]}{m[7]}:{m[8]}{m[9]}:{m[10]}{m[11]}"

                    # self.stats[switchname].port_list[pname].mac = mac
                    state = ParserState.DESC
                else:
                    state = ParserState.START_SHOWINT

            elif state == ParserState.DESC:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = desc_re.match(line)
                if res:
                    self.stats[switchname].port_list[title_res.group(1)] = PortInfo(pname)
                    self.stats[switchname].port_list[pname].oper_status = opstate
                    self.stats[switchname].port_list[pname].description = res.group(1)
                    self.stats[switchname].port_list[pname].mac = mac
                    self.stats[switchname].port_list[pname].errdisabled = 0


                    state = ParserState.SPEED
                else:
                    res = mtu_re.match(line)
                    res2 = ip_addr_re.match(line)
                    # looking for Description: but found MTU line, so this port doesn't have one
                    if res or res2:
                        if self.exclude_unlabel and not (
                            self.switches.get(switchname) and pname in self.switches[switchname]
                        ):
                            # No description and it's not in net.json, so don't add
                            state = ParserState.START_SHOWINT
                        else:
                            # But add it anyway
                            self.stats[switchname].port_list[title_res.group(1)] = PortInfo(pname)
                            self.stats[switchname].port_list[pname].oper_status = opstate
                            self.stats[switchname].port_list[pname].description = ""
                            self.stats[switchname].port_list[pname].mac = mac
                            self.stats[switchname].port_list[pname].errdisabled = 0

                            state = ParserState.SPEED

            elif state == ParserState.SPEED:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = speed_re.match(line)
                if res:
                    speedstr = res.group(1)
                    # can be Unknown or \d+G.*
                    if speedstr == "Unknown":
                        self.stats[switchname].port_list[pname].speed = 0
                    else:
                        self.stats[switchname].port_list[pname].speed = int(res.group(1))
                        if res.group(2) == "Gb/s":
                            self.stats[switchname].port_list[pname].speed *= 1000

                    state = ParserState.LINKSTATE

            elif state == ParserState.LINKSTATE:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = lsc_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].carrier_changes = int(res.group(1))

                    state = ParserState.BYTES

            elif state == ParserState.BYTES:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = in_bytes_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].rx_bytes = int(res.group(1))
                    state = ParserState.RUNTS

            elif state == ParserState.RUNTS:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = runts_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].runts = int(res.group(1))
                    state = ParserState.CRC

            elif state == ParserState.CRC:
                if line[0] != ' ':
                    state = ParserState.START_SHOWINT
                    continue
                res = crc_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].input_errors = int(res.group(1))
                    self.stats[switchname].port_list[pname].crc_errors = int(res.group(2))
                    state = ParserState.START_SHOWINT

            # If link is down-ish, FEC errors don't get reported. Make sure we aren't missing
            # things- go back to START_PHYDETAIL if we don't think we'll find FEC errors.
            elif state == ParserState.START_PHYDETAIL:
                line = line.rstrip()

                res = eth_at_start_re.match(line)
                if res:
                    if line in self.stats[switchname].port_list:
                        pname = line
                        continue
                    else:
                        pname = None
                        continue

                res = int_trans_dom_re.match(line)
                if res:
                    state = ParserState.XCVR_DOM
                    continue

                # since Arista 400G switches (one example running switch OS version 4.31.2F)
                # don't report phydetail entries in the same order as the 100G switches,
                # look for any line from this state
                if pname:
                    res = stale_warning_re.match(line)
                    if res:
                        self.stats[switchname].port_list[pname].clear_stale()
                        pname = None
                        state = ParserState.START_PHYDETAIL
                        continue

                    res = opt_speed_re.match(line)
                    if res:
                        self.stats[switchname].port_list[pname].opt_speed = res.group(1).strip()
                        continue

                    res = opt_sernum_re.match(line)
                    if res:
                        self.stats[switchname].port_list[pname].opt_sernum = res.group(1).strip()
                        continue

                    res = opt_partnum_re.match(line)
                    if res:
                        self.stats[switchname].port_list[pname].opt_partnum = res.group(1).strip()
                        continue

                    res = opt_revision_re.match(line)
                    if res:
                        self.stats[switchname].port_list[pname].opt_rev = res.group(1).strip()
                        continue

                    res = fec_al_re.match(line)
                    if res:
                        self.stats[switchname].port_list[pname].fec_alignment_lock = res.group(1)
                        continue

                    res = fec_cc_re.match(line)
                    if res:
                        self.stats[switchname].port_list[pname].fec_corrected_codewords = int(
                            res.group(1)
                        )
                        self.stats[switchname].port_list[pname].delta_fec_corrected_codewords = int(
                            res.group(2)
                        )
                        continue

                    res = fec_uc_re.match(line)
                    if res:
                        self.stats[switchname].port_list[pname].fec_uncorrected_codewords = int(
                            res.group(1)
                        )
                        self.stats[switchname].port_list[pname].delta_fec_uncorrected_codewords = (
                            int(res.group(2))
                        )
                        continue

            elif state == ParserState.XCVR_DOM:
                # First few lines are temp and voltage for the port (EthP/*)
                # Then there may be repeating sections starting with EthernetP/[1..] for
                # per-connector current and power
                res = dom_port_re.match(line)
                if res:
                    port_temp = None
                    port_volt = None
                    state = ParserState.OPT_TEMP
                    continue
                # could be another iface on the same port, aka ETHNAME
                res = eth_at_start_re.match(line)
                if res:
                    pname = res.group(1).strip()
                    if pname not in self.stats[switchname].port_list:
                        # We didn't record this port earlier so we should skip it here
                        pname = None
                        continue

                    # If this is a new sub-iface, we need to record temp/volt now since the sensors
                    # are shared with all sub-ifaces
                    self.stats[switchname].port_list[pname].opt_temperature = port_temp
                    self.stats[switchname].port_list[pname].opt_voltage = port_volt
                    state = ParserState.OPT_TX_CURR
                    continue

                res = pfc_header_re.match(line)
                if res:
                    state = ParserState.PFC

            elif state == ParserState.OPT_TEMP:
                res = opt_temp_re.match(line)
                if res:
                    # self.stats[switchname].port_list[pname].opt_temperature = float(res.group(1))
                    port_temp = float(res.group(1))
                    state = ParserState.OPT_VOLT
                    continue
                res = missing_opt_temp_re.match(line)
                if res:
                    state = ParserState.XCVR_DOM
                    continue

            elif state == ParserState.OPT_VOLT:
                res = opt_volt_re.match(line)
                if res:
                    # self.stats[switchname].port_list[pname].opt_voltage = float(res.group(1))
                    port_volt = float(res.group(1))
                    state = ParserState.OPT_ETHNAME

            elif state == ParserState.OPT_ETHNAME:
                res = eth_at_start_re.match(line)
                if res:
                    pname = res.group(1).strip()
                    if pname not in self.stats[switchname].port_list:
                        # We didn't record this port earlier so we should skip it here
                        pname = None
                        state = ParserState.XCVR_DOM
                        continue
                    self.stats[switchname].port_list[pname].opt_temperature = port_temp
                    self.stats[switchname].port_list[pname].opt_voltage = port_volt
                    state = ParserState.OPT_TX_CURR

            elif state == ParserState.OPT_TX_CURR:
                # first check the weird case where there's no channel section
                res = opt_tx_curr_nochan_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].opt_tx_current["1"] = float(
                        res.group(1)
                    )
                    state = ParserState.OPT_TX_PWR
                    continue

                # check for the start of the next section
                res = opt_tx_power_header_re.match(line)
                if res:
                    state = ParserState.OPT_TX_PWR
                    continue

                # now the normal case
                chan = opt_chan_re.match(line)
                if chan:
                    if chan.group(2) == "N/A":
                        state = ParserState.OPT_TX_PWR
                        continue
                    lane = chan.group(1)
                    tx_curr = float(chan.group(2))
                    self.stats[switchname].port_list[pname].opt_tx_current[lane] = tx_curr
                elif "Optical TX power" in line:
                    state = ParserState.OPT_TX_PWR
                    continue

            elif state == ParserState.OPT_TX_PWR:
                # first check the weird case where there's no channel section
                res = opt_tx_pwr_nochan_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].opt_tx_power["1"] = float(res.group(1))
                    state = ParserState.OPT_RX_PWR
                    continue

                # check for the start of the next section
                res = opt_rx_power_header_re.match(line)
                if res:
                    state = ParserState.OPT_RX_PWR
                    continue

                chan = opt_chan_re.match(line)
                if chan:
                    if chan.group(2) == "N/A":
                        state = ParserState.OPT_RX_PWR
                        continue
                    lane = chan.group(1)
                    val = float(chan.group(2))
                    self.stats[switchname].port_list[pname].opt_tx_power[lane] = val
                elif "Optical RX power" in line:
                    state = ParserState.OPT_TX_PWR
                    continue

            elif state == ParserState.OPT_RX_PWR:
                # first check the weird case where there's no channel section
                res = opt_rx_pwr_nochan_re.match(line)
                if res:
                    self.stats[switchname].port_list[pname].opt_rx_power["1"] = float(res.group(1))
                    state = ParserState.XCVR_DOM
                    continue

                if line.strip() == '':
                    state = ParserState.XCVR_DOM
                    continue

                # could be another iface on the same port, aka ETHNAME
                res = eth_at_start_re.match(line)
                if res:
                    pname = res.group(1).strip()
                    if pname not in self.stats[switchname].port_list:
                        # We didn't record this port earlier so we should skip it here
                        pname = None
                        continue
                    # If this is a new sub-iface, we need to record temp/volt now since the sensors
                    # are shared with all sub-ifaces
                    self.stats[switchname].port_list[pname].opt_temperature = port_temp
                    self.stats[switchname].port_list[pname].opt_voltage = port_volt
                    state = ParserState.OPT_TX_CURR
                    continue

                # check for start of next section
                res = pfc_header_re.match(line)
                if res:
                    state = ParserState.PFC

                chan = opt_chan_re.match(line)
                if chan:
                    if chan.group(2) == "N/A":
                        state = ParserState.XCVR_DOM
                        continue
                    lane = chan.group(1)
                    val = float(chan.group(2))
                    self.stats[switchname].port_list[pname].opt_rx_power[lane] = val

            elif state == ParserState.PFC:
                res = pfc_discard_re.match(line)
                if res:
                    port = res.group(1)
                    if port.startswith("Et"):
                        rx_pfc = int(res.group(2))
                        tx_pfc = int(res.group(3))
                        port = port.replace("Et", "Ethernet")
                        if port not in self.stats[switchname].port_list:
                            # We didn't record this port earlier so we should skip it here
                            port = None
                            continue
                        self.stats[switchname].port_list[port].tx_pfc = tx_pfc
                        self.stats[switchname].port_list[port].rx_pfc = rx_pfc
                    continue
                res = cmdsep_re.match(line)
                if res:
                    state = ParserState.IFINDEX

            elif state == ParserState.IFINDEX:
                res = ifindex_mapping_re.match(line)
                if res:
                    port = res.group(1)
                    if port not in self.stats[switchname].port_list:
                        # We didn't record this port earlier so we should skip it here
                        port = None
                        continue
                    self.stats[switchname].port_list[port].ifindex = int(res.group(2))
                    continue
                res = count_discard_re.match(line)
                if res:
                    state = ParserState.DISCARDS

            elif state == ParserState.DISCARDS:
                res = pfc_discard_re.match(line)
                if res:
                    port = res.group(1)
                    if port.startswith("Et"):
                        port = port.replace("Et", "Ethernet")
                        if port not in self.stats[switchname].port_list:
                            # We didn't record this port earlier so we should skip it here
                            port = None
                            continue

                        in_discards = int(res.group(2))
                        out_discards = int(res.group(3))

                        self.stats[switchname].port_list[port].in_discards = in_discards
                        self.stats[switchname].port_list[port].out_discards = out_discards
                    continue
                res = queue_detail_header_re.match(line)
                if res:
                    state = ParserState.QUEUE_DETAIL

            elif state == ParserState.QUEUE_DETAIL:
                res = queue_detail_re.match(line)
                if res:
                    port = res.group(1)
                    if port.startswith("Et"):
                        port = port.replace("Et", "Ethernet")
                        if port not in self.stats[switchname].port_list:
                            # We didn't record this port earlier so we should skip it here
                            port = None
                            continue

                        queue = res.group(2)
                        drop_pkts = int(res.group(3))

                        if queue != "UC5":
                            # We only care about the unicast priority 5 queue.
                            continue
                        self.stats[switchname].port_list[port].out_discards_prio5 = drop_pkts
                    continue
                res = counters_rates_re.match(line)
                if res:
                    state = ParserState.DATA_RATE

            elif state == ParserState.DATA_RATE:
                res = data_rate_re.match(line)
                if res:
                    ifc = res.group(1)
                    if ifc.startswith("Et"):
                        port = ifc.replace("Et", "Ethernet")
                    else:
                        continue
                    if port not in self.stats[switchname].port_list:
                        # We didn't record this port earlier so we should skip it here
                        port = None
                        continue
                    in_rate = data_rate_to_kbps_converter(float(res.group(2)), "Mbps")
                    out_rate = data_rate_to_kbps_converter(float(res.group(3)), "Mbps")
                    self.stats[switchname].port_list[port].rx_kbps = int(in_rate)
                    self.stats[switchname].port_list[port].tx_kbps = int(out_rate)
                    continue

                # check for next command
                res = cmdsep_re.match(line)
                if res:
                    # Found the output separator, next line will be show ERRDISABLED output
                    state = ParserState.ERRDISABLED
                    pname = None
                    continue

            elif state == ParserState.ERRDISABLED:
                res = errdisabled_re.match(line)
                if res:
                    ifc = res.group(1)
                    if ifc.startswith("Et"):
                        port = ifc.replace("Et", "Ethernet")
                    else:
                        continue
                    if port not in self.stats[switchname].port_list:
                        # We didn't record this port earlier so we should skip it here
                        port = None
                        continue
                    reason = res.group(2).strip()
                    self.stats[switchname].port_list[port].errdisabled_reason = reason
                    self.stats[switchname].port_list[port].errdisabled = 1
                    continue

                # check for next command
                res = cmdsep_re.match(line)
                if res:
                    # Found the output separator, next line will be show version output
                    state = ParserState.VERSION
                    pname = None
                    continue

            elif state == ParserState.VERSION:
                if line.startswith("{") or len(json_accum) > 0:
                    json_accum += line
                if line.strip() == '% Invalid input':
                    logger.error(f"version metric unavailable for {switchname}: "
                                 f"failed to read JSON for `show version | json`: {line}")
                    # Skip to next state
                    json_accum = ""
                    state = ParserState.PSU
                    continue
                # Check for end of JSON
                if not json_accum or not line.startswith("}"):
                    continue

                version_json = json.loads(json_accum)
                json_accum = ""
                if "This is an unconverted command" in version_json.get("errors", []):
                    logger.error(
                        f"version metric unavailable for {switchname}: switch cannot "
                        "give JSON output for 'show version'")
                    # Skip to next state
                    state = ParserState.PSU
                    continue

                model = version_json.get('modelName', '')
                firmware_version = version_json.get('version', '')
                uptime = float(version_json.get('uptime', '0'))
                self.stats[switchname].switch_stats['version_info'].append(dict(
                    vendor='Arista', model=model, firmware_version=firmware_version, val=1))
                self.stats[switchname].switch_stats['uptime'].append(dict(val=uptime))

                # Finished parsing JSON, transition to PSU
                state = ParserState.PSU

            elif state == ParserState.PSU:
                if line.startswith("{") or len(json_accum) > 0:
                    json_accum += line
                if line.strip() == '% Invalid input':
                    logger.error(
                        f"PSU metric unavailable for {switchname}: "
                        f"failed to read JSON for `show environment power | json`: {line}")
                    # Skip to next state
                    json_accum = ""
                    state = ParserState.TEMPERATURE
                    continue
                # Check for end of JSON
                if not json_accum or not line.startswith("}"):
                    continue

                # https://arista.my.site.com/AristaCommunity/s/article/Troubleshooting-PowerSupply-Failure
                def encodeHealth(state: str) -> int:
                    if state == 'ok':
                        return 0
                    elif 'fail' in state or 'loss' in state:
                        return 2 # error
                    else:
                        return 3 # unknown

                psu_json = json.loads(json_accum)
                json_accum = ""
                if "This is an unconverted command" in psu_json.get("errors", []):
                    logger.error(
                        f"PSU metric unavailable for {switchname}: switch cannot "
                        "give JSON output for 'show environment power'")
                    # Skip to next state
                    state = ParserState.TEMPERATURE
                    continue
                for psu_id, psu in psu_json.get('powerSupplies', {}).items():
                    psu_name = f"PSU{psu_id}"
                    state = psu.get('state', '').lower()
                    health = encodeHealth(state)
                    self.stats[switchname].switch_stats['psu_health'].append(dict(
                        desc=psu_name, val=health))

                # Finished parsing JSON, transition to temperature
                state = ParserState.TEMPERATURE

            elif state == ParserState.TEMPERATURE:
                if line.startswith("{") or len(json_accum) > 0:
                    json_accum += line
                if line.strip() == '% Invalid input':
                    logger.error(
                        f"temperature metric unavailable for {switchname}: "
                        f"Failed to read JSON for `show system environment temperature | json`: {line}")
                    # Skip to next state
                    json_accum = ""
                    state = ParserState.COOLING
                    continue
                # Check for end of JSON
                if not json_accum or not line.startswith("}"):
                    continue

                # https://www.arista.com/en/um-eos/eos-environment-commands#xx1142707
                def encodeHealth(status: str) -> int:
                    if status == 'ok':
                        return 0
                    elif 'warn' in status or 'overheating' in status:
                        return 1 # warn
                    elif 'fail' in status or 'critical' in status:
                        return 2 # error
                    else:
                        return 3 # unknown

                temp_json = json.loads(json_accum)
                if "This is an unconverted command" in temp_json.get("errors", []):
                    logger.error(
                        f"temperature metric unavailable for {switchname}: switch cannot"
                        "give JSON output for 'show system environment temperature'")
                    # Skip to next state
                    state = ParserState.COOLING
                    continue
                json_accum = ""
                sensors = temp_json.get('tempSensors', [])
                for psu in temp_json.get('powerSupplySlots', []):
                    psu_name = 'PSU' + psu.get('relPos', '') + ' '
                    for sensor in psu.get('tempSensors', []):
                        sensor['description'] = psu_name + sensor.get('description', '')
                        sensors.append(sensor)
                for sensor in sensors:
                    desc = sensor.get('description', '')
                    status = sensor.get('hwStatus', '').lower()
                    health = encodeHealth(status)
                    temp = float(sensor.get('currentTemperature', '-1'))
                    self.stats[switchname].switch_stats['temperature_health'].append(dict(
                        desc=desc, val=health))
                    self.stats[switchname].switch_stats['temperature'].append(dict(
                        desc=desc, val=temp))

                # Finished parsing JSON, transition to cooling
                state = ParserState.COOLING

            elif state == ParserState.COOLING:
                if line.startswith("{") or len(json_accum) > 0:
                    json_accum += line
                if line.strip() == '% Invalid input':
                    logger.error(f"fan metric unavailable for {switchname}: "
                                 f"failed to read JSON for `show system environment cooling | json`: {line}")
                    # Skip to next state
                    json_accum = ""
                    state = ParserState.ECN_TC
                    continue
                # Check for end of JSON
                if not json_accum or not line.startswith("}"):
                    continue

                def encodeHealth(status: str) -> int:
                    if status == 'ok':
                        return 0
                    elif 'warn' in status or 'inserted' in status or 'unsupported' in status:
                        return 1 # warn
                    elif 'fail' in status:
                        return 2 # error
                    else:
                        return 3 # unknown

                cooling_json = json.loads(json_accum)
                if "This is an unconverted command" in cooling_json.get("errors", []):
                    logger.error(
                        f"fan metric unavailable for {switchname}: switch cannot "
                        "give JSON output for 'show system environment cooling'")
                    # Skip to next state
                    state = ParserState.ECN_TC
                    continue
                json_accum = ""
                fans = dict()
                for slot in cooling_json.get('powerSupplySlots', []):
                    for fan in slot['fans']:
                        fans[fan['label']] = fan
                for slot in cooling_json.get('fanTraySlots', []):
                    for fan in slot['fans']:
                        fans['Fan tray ' + fan['label']] = fan
                for desc, fan in fans.items():
                    status = fan.get('status', '').lower()
                    health = encodeHealth(status)
                    pct = float(fan.get('actualSpeed', '0'))
                    maxspeed = float(fan.get('maxSpeed', '0'))
                    rpm = maxspeed * (pct / 100)
                    self.stats[switchname].switch_stats['fan_health'].append(dict(
                        desc=desc, val=health))
                    self.stats[switchname].switch_stats['fan_rpm'].append(dict(
                        desc=desc, val=rpm))

                # Finished parsing JSON, transition to ECN TC
                state = ParserState.ECN_TC

            elif state == ParserState.ECN_TC:
                res = eth_at_start_re.match(line)
                if res:
                    pname = res.group(1).strip()
                    if pname not in self.stats[switchname].port_list:
                        # We didn't record this port earlier so we should skip it here
                        pname = None
                    continue
                res = ecn_tc_re.match(line)
                if res:
                    if res.group(1) == '5':  # only grab this traffic class
                        self.stats[switchname].port_list[pname].ecn_marked_packets_tc5 = int(
                            res.group(2)
                        )
                        pname = None
                        continue

    def parse_sonic(self, switchname, cmdoutput):
        ParserState = IntEnum(
            'ParserState',
            [
                'START',  # may have banner/version/other noise
                'INT_STATUS',
                'INT_DESC',
                'INT_COUNTERS',
                'ADDR_CC_RXB',
                'PHY',
                'DOM',
                'EEPROM',
                'PKTDROPS_HEADER',
                'PKTDROPS',
                'PFC_RX',
                'PFC_TX',
            ],
        )

        status_header_re = re.compile(r'.*\s*Interface\s*Lanes\s*Speed\s*MTU\s*.*FEC\s*Alias.*')
        status_re = re.compile(
            r'\s*(Ethernet\d+)\s*\S*\s*(\d+)G\s*\d+\s*(\w+)\s*\S*\s*\w+\s*(\w+).*'
        )
        #                            ifname                speed          FEC                           operstatus
        desc_header_re = re.compile(r'.*\s*Interface\s*Oper\s*Admin\s*Alias\s*Description')
        desc_re = re.compile(r'\s*(Ethernet\d+)\s*\w+\s*\w+\s*\S*\s*(.*)')
        #                          ifname                                      description
        counters_header_re = re.compile(r'.*\s*IFACE\s*STATE\s*RX_OK\s*RX_BPS\s*RX_UTIL.*')
        counters_re = re.compile(
            r'\s*(Ethernet\d+)\s*\w+\s*\S+\s*(\d+\.?\d*) (\w?B/s)\s*.*%\s*(\d+)\s*\d+\s*\S+\s*\S+\s*(\d+\.?\d*) (\w?B/s).*'
        )
        #         ifname                      rxbps       unit             rxerr                     txbps       unit
        addr_cc_rxb_header_re = re.compile(r'(?:.*\$ )?addr cc rxb$')
        addr_cc_rxb_re = re.compile(r'(?:.*\$ )?(Ethernet\d+) ([a-fA-F0-9:]*) (\d+) (\d+)')
        #                              ifname        mac             cc    rxb
        phy_header_re = re.compile(r'(?:.*\$ )?phy$')
        phy_name_re = re.compile(r'(Ethernet\d+) :')
        #                           ifname
        phy_counter_re = re.compile(r' (\w+)\s*: (\d+).*')
        #                              ctr name  value
        dom_header_re = re.compile(r'(?:.*\$ )?dom$')
        dom_name_re = re.compile(r'(?:.*\$ )?(Ethernet\d+)\s*$')
        #                            ifname
        dom_temp_re = re.compile(r'\s*Temperature: (\d+\.?\d*) C')
        dom_volt_re = re.compile(r'\s*Vcc: (-?\d+\.?\d*) Volts')
        dom_rxp_re = re.compile(r'\s*Rx(\d)Power: (-?\d+\.?\d*) dBm')
        dom_txi_re = re.compile(r'\s*Tx(\d)Bias: (-?\d+\.?\d*) mA')
        dom_txp_re = re.compile(r'\s*Tx(\d)Power: (-?\d+\.?\d*) dBm')

        eeprom_header_re = re.compile(r'(?:.*\$ )?eeprom$')
        eeprom_name_re = re.compile(r'(Ethernet\d+): .* detected')
        eeprom_speed_re = re.compile(r'\s*1:.*\| (\S+).*')
        #                                        speed str
        eeprom_vendor_re = re.compile(r'\s*Vendor Name: (.*)')
        eeprom_part_re = re.compile(r'\s*Vendor PN: (.*)')
        eeprom_rev_re = re.compile(r'\s*Vendor Rev: (.*)')
        eeprom_sn_re = re.compile(r'\s*Vendor SN: (.*)')

        pktdrops_sep_re = re.compile(r'(?:.*\$ )?pktdrops$')
        pktdrops_header_re = re.compile(r'^RDISC\s+.*')
        pktdrops_re = re.compile(r'(Ethernet\d+)\s+(\w+)\s+(\d+).*')

        pfc_rx_header_re = re.compile(r'.*Port Rx\s+PFC0\s+PFC1\s+PFC2\s+PFC3\s+PFC4\s+PFC5\s+.*')
        pfc_rx_re = re.compile(r'\s*(Ethernet\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+.*')
        #                            ifname                                        val
        pfc_tx_header_re = re.compile(r'.*Port Tx\s+PFC0\s+PFC1\s+PFC2\s+PFC3\s+PFC4\s+PFC5\s+.*')
        pfc_tx_re = re.compile(r'\s*(Ethernet\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+.*')
        #                            ifname                                        val

        unsupported_soniccli_cmd_re = re.compile(r'Syntax error: Illegal parameter')
        unsupported_show_cmd_re = re.compile(r'Error: ')

        state = ParserState.START
        pname = None
        title_res = None
        mac = None
        opstate = None
        carrier_changes = None
        detail_idx = 1

        for line in cmdoutput:
            line = line.strip('\r\n')
            # print("SONIC state=%d got line: %r" % (state,line))

            if line == '':
                continue
            if state == ParserState.START:
                res = status_header_re.match(line)
                if res:
                    state = ParserState.INT_STATUS
                    continue

            # since everything following is elif, some of the continues can be removed
            if state == ParserState.INT_STATUS:
                res = status_re.match(line)
                if res:
                    pname = res.group(1)
                    speed = int(res.group(2))
                    fec_mode = res.group(3)
                    operstatus = 1 if res.group(4).lower() == "up" else 0

                    # don't need to support this anymore
                    # if self.exclude_not_in_nj and not (
                    #     self.switches.get(switchname) and pname in self.switches[switchname]
                    # ):
                    #     # it's not in net.json and we require that, so skip this iface block
                    #     continue
                    self.stats[switchname].port_list[pname] = PortInfo(pname)
                    self.stats[switchname].port_list[pname].speed = speed * 1000
                    self.stats[switchname].port_list[pname].oper_status = operstatus
                    # We only want to report a FEC corr/uncorr metric if we have a valid counter value, so we
                    # init these to None.
                    # But for this switch OS, a valid count of 0 is not reported in the output. So look at the FEC mode
                    # instead, and if FEC is on, init to 0 here. If the counter is nonzero it will get reset later...
                    # Unless it's an older/edgecore sonic build, which may not support accessing PHY counters.
                    # In that case we will set any fec=0 counters back to fec=None
                    if fec_mode != 'none':
                        self.stats[switchname].port_list[pname].fec_corrected_codewords = 0
                        self.stats[switchname].port_list[pname].fec_uncorrected_codewords = 0

                    continue

                res = desc_header_re.match(line)
                if res:
                    state = ParserState.INT_DESC
                    continue

            elif state == ParserState.INT_DESC:
                res = desc_re.match(line)
                if res:
                    pname = res.group(1)
                    desc = res.group(2)
                    self.stats[switchname].port_list[pname].description = desc
                    continue

                res = counters_header_re.match(line)
                if res:
                    state = ParserState.INT_COUNTERS
                    continue

            elif state == ParserState.INT_COUNTERS:
                res = counters_re.match(line)
                if res:
                    pname = res.group(1)
                    rx_rate = float(res.group(2))
                    rx_unit = res.group(3)
                    rx_errs = int(res.group(4))
                    tx_rate = float(res.group(5))
                    tx_unit = res.group(6)

                    rx_kbps = data_rate_to_kbps_converter(rx_rate, rx_unit)
                    tx_kbps = data_rate_to_kbps_converter(tx_rate, tx_unit)
                    self.stats[switchname].port_list[pname].rx_kbps = int(rx_kbps)
                    self.stats[switchname].port_list[pname].tx_kbps = int(tx_kbps)

                    self.stats[switchname].port_list[pname].input_errors = rx_errs
                    continue

                res = addr_cc_rxb_header_re.match(line)
                if res:
                    state = ParserState.ADDR_CC_RXB
                    continue

            elif state == ParserState.ADDR_CC_RXB:
                res = addr_cc_rxb_re.match(line)
                if res:
                    pname = res.group(1)
                    mac = res.group(2)
                    cc = int(res.group(3))
                    rxb = int(res.group(4))

                    if pname in self.stats[switchname].port_list:
                        self.stats[switchname].port_list[pname].mac = mac
                        self.stats[switchname].port_list[pname].carrier_changes = cc
                        self.stats[switchname].port_list[pname].rx_bytes = rxb
                    continue

                res = phy_header_re.match(line)
                if res:
                    pname = None
                    state = ParserState.PHY
                    continue

            elif state == ParserState.PHY:
                res = unsupported_soniccli_cmd_re.match(line)
                if res:
                    for pname in self.stats[switchname].port_list:
                        self.stats[switchname].port_list[pname].fec_corrected_codewords = None
                        self.stats[switchname].port_list[pname].fec_uncorrected_codewords = None
                    pname = None
                    state = ParserState.DOM
                    continue

                res = phy_name_re.match(line)
                if res:
                    pname = res.group(1)
                    continue

                res = phy_counter_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        counter_name = res.group(1)
                        counter_val = int(res.group(2))
                        if counter_name == 'FEC_CORR':
                            self.stats[switchname].port_list[
                                pname
                            ].fec_corrected_codewords = counter_val
                        elif counter_name == 'FEC_NON_CORR':
                            self.stats[switchname].port_list[
                                pname
                            ].fec_uncorrected_codewords = counter_val

                    continue

                res = dom_header_re.match(line)
                if res:
                    pname = None
                    state = ParserState.DOM
                    continue

            elif state == ParserState.DOM:
                res = unsupported_soniccli_cmd_re.match(line)
                if res:
                    pname = None
                    state = ParserState.EEPROM
                    continue

                res = dom_name_re.match(line)
                if res:
                    pname = res.group(1)
                    continue

                res = dom_temp_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        temp = float(res.group(1))
                        self.stats[switchname].port_list[pname].opt_temperature = temp
                    continue

                res = dom_volt_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        voltage = float(res.group(1))
                        self.stats[switchname].port_list[pname].opt_voltage = voltage
                    continue

                res = dom_rxp_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        lane = res.group(1)
                        power = float(res.group(2))
                        self.stats[switchname].port_list[pname].opt_rx_power[lane] = power
                    continue

                res = dom_txi_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        lane = res.group(1)
                        current = float(res.group(2))
                        self.stats[switchname].port_list[pname].opt_tx_current[lane] = current
                    continue

                res = dom_txp_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        lane = res.group(1)
                        power = float(res.group(2))
                        self.stats[switchname].port_list[pname].opt_tx_power[lane] = power
                    continue

                res = eeprom_header_re.match(line)
                if res:
                    pname = None
                    state = ParserState.EEPROM
                    continue

            elif state == ParserState.EEPROM:
                res = eeprom_name_re.match(line)
                if res:
                    pname = res.group(1)
                    continue

                res = eeprom_speed_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        speed = res.group(1)
                        self.stats[switchname].port_list[pname].opt_speed = speed
                    continue

                res = eeprom_vendor_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        vendor = res.group(1)
                        self.stats[switchname].port_list[pname].opt_vendor = vendor
                    continue

                res = eeprom_part_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        partnum = res.group(1)
                        self.stats[switchname].port_list[pname].opt_partnum = partnum
                    continue

                res = eeprom_rev_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        rev = res.group(1)
                        self.stats[switchname].port_list[pname].opt_rev = rev
                    continue

                res = eeprom_sn_re.match(line)
                if res:
                    if pname and pname in self.stats[switchname].port_list:
                        sn = res.group(1)
                        self.stats[switchname].port_list[pname].opt_sernum = sn
                    continue

                res = pktdrops_sep_re.match(line)
                if res:
                    state = ParserState.PKTDROPS_HEADER
                    continue

            elif state == ParserState.PKTDROPS_HEADER:
                # Dell has this command, EdgeCore doesn't.
                # Dell's CLI only outputs a line if a counter is nonzero.
                # If we see the header, we should assume all counters are zero and then
                # fill in values as we see them. If we're EdgeCore, let these values remain
                # as None so we don't report metrics we didn't actually see or infer.
                res = pktdrops_header_re.match(line)
                if res:
                    for pname in self.stats[switchname].port_list:
                        self.stats[switchname].port_list[pname].in_discards = 0
                        self.stats[switchname].port_list[pname].crc_errors = 0
                        self.stats[switchname].port_list[pname].runts = 0
                    state = ParserState.PKTDROPS
                    continue

                # If it's EdgeCore, we'll never see anything until the next section.
                res = pfc_rx_header_re.match(line)
                if res:
                    state = ParserState.PFC_RX
                    continue

            elif state == ParserState.PKTDROPS:
                res = pktdrops_re.match(line)
                if res:
                    pname = res.group(1)
                    counter_name = res.group(2)
                    counter_value = int(res.group(3))
                    if counter_name == 'RDISC':
                        self.stats[switchname].port_list[pname].in_discards = counter_value
                    elif counter_name == 'RFCS':
                        self.stats[switchname].port_list[pname].crc_errors = counter_value
                    elif counter_name == 'RRPKT':
                        self.stats[switchname].port_list[pname].runts = counter_value
                    continue

                res = pfc_rx_header_re.match(line)
                if res:
                    state = ParserState.PFC_RX
                    continue

            elif state == ParserState.PFC_RX:
                res = pfc_rx_re.match(line)
                if res:
                    pname = res.group(1)
                    pfc = int(res.group(2))
                    self.stats[switchname].port_list[pname].rx_pfc = pfc
                    continue

                res = pfc_tx_header_re.match(line)
                if res:
                    state = ParserState.PFC_TX
                    continue

            elif state == ParserState.PFC_TX:
                res = pfc_tx_re.match(line)
                if res:
                    pname = res.group(1)
                    pfc = int(res.group(2))
                    self.stats[switchname].port_list[pname].tx_pfc = pfc
                    continue

    # The following methods are not used when running under prometheus
    @classmethod
    def sqlite_create_table_cmd(cls):
        switch_create_query = """
CREATE TABLE IF NOT EXISTS switch (
id INTEGER PRIMARY KEY,
delta_corr STRING,
delta_uncorr STRING
"""
        for hkey in cls.headings:
            hkey = re.sub(r"\(|\)", "", hkey)
            hkey = re.sub(r"\.", "", hkey)
            hkey = re.sub(r" ", "_", hkey)
            switch_create_query += f",{hkey} TEXT NOT NULL"

        switch_create_query += ");"

        return switch_create_query

    @staticmethod
    def print_header(dest=sys.stdout):
        print(SwitchSurvey.fmt_str % SwitchSurvey.headings, file=dest)

    @classmethod
    def print_two_runs(cls, survey1, survey2, dest=sys.stdout):
        # find the switches and ports that weren't present in both runs
        # init_set = set(survey1.stats.keys())
        # final_set = set(survey2.stats.keys())
        init_set = set()
        final_set = set()
        for s in survey1.stats:
            for p in survey1.stats[s].port_list:
                init_set.add(s + ':' + p)
                # print("** added to iset: %s" % s + ':' + p)
        for s in survey2.stats:
            for p in survey2.stats[s].port_list:
                final_set.add(s + ':' + p)
                # print("** added to fset: %s" % s + ':' + p)

        up_down_set = init_set - final_set
        down_up_set = final_set - init_set
        for k in sorted((init_set | final_set)):  # everything
            switch_name, port_name = k.split(':')
            if k in init_set:
                port1 = pdata = survey1.stats[switch_name].port_list[port_name]
            if k in final_set:
                port2 = pdata = survey2.stats[switch_name].port_list[port_name]

            # print the data that's valid even if we only have one good survey
            vendor_name = survey1.vendors.get(switch_name, 'unknown')
            if vendor_name == "unknown":
                # try to guess
                if "gsw256" in switch_name or "-mhp" in switch_name:
                    vendor_name = "HPEComware"
                elif "hpe" in switch_name:
                    vendor_name = "hpe"
                else:
                    vendor_name = "arista"

            mac = pdata.mac
            desc = pdata.description

            if k in up_down_set:
                operstatus = port1.oper_status + " -> ?"
                flaps = rxbytes = "?"
                align = port1.fec_alignment_lock + ' -> ?'
                crc = corr = uncorr = "?"
                health = "BAD"
            elif k in down_up_set:
                operstatus = "? -> " + port2.oper_status
                flaps = rxbytes = "?"
                align = "? -> " + port2.fec_alignment_lock
                crc = corr = uncorr = "?"
                health = "BAD"
            else:
                operstatus = str(port1.oper_status) + ' -> ' + str(port2.oper_status)
                flaps = port2.carrier_changes - port1.carrier_changes
                rxbytes = int((port2.rx_bytes - port1.rx_bytes) / 1024)
                crc = port2.crc_errors - port1.crc_errors
                align = port1.fec_alignment_lock + ' -> ' + port2.fec_alignment_lock
                corr = port2.fec_corrected_codewords - port1.fec_corrected_codewords
                uncorr = port2.fec_uncorrected_codewords - port1.fec_uncorrected_codewords

                if port2.carrier_changes - port1.carrier_changes != 0:
                    health = "FLAP"
                elif port2.oper_status.lower() == "up":
                    if corr == 0 and uncorr == 0:
                        health = "GOOD"
                    elif uncorr:
                        health = "UNCORRECTED_ERRORS"
                    elif (corr / (1 + port2.rx_bytes - port1.rx_bytes) / 1024) > 1:
                        health = "MANY_CORRECTED_ERRORS"
                    else:
                        health = "SOME_CORRECTED"
                else:
                    health = "BAD"

            out_tuple = (
                switch_name,
                vendor_name,
                port_name,
                mac,
                desc,
                operstatus,
                flaps,
                rxbytes,
                crc,
                align,
                corr,
                uncorr,
                health,
            )

            print(SwitchSurvey.fmt_str % out_tuple, file=dest)

    def insert_run(self, dbfile):
        if not self.stats:
            return

        con = sqlite3.connect(dbfile)
        cur = con.cursor()
        for s in self.stats:
            for p in self.stats[s].port_list:
                dbcmd = "INSERT INTO switch VALUES(?,  ?,?,   ?, ?,?, ?, ?,   ?, ?, ?, ?,    ?, ?,?,   ?)"
                if self.vendors:
                    vname = self.vendors[s]
                else:
                    vname = "unknown"
                data = (
                    None,
                    self.stats[s].port_list[p].delta_fec_corrected_codewords,
                    self.stats[s].port_list[p].delta_fec_uncorrected_codewords,
                    # self.stats[s].port_list[p].delta_fec_lane_corrected_symbols[0],
                    # self.stats[s].port_list[p].delta_fec_lane_corrected_symbols[1],
                    # self.stats[s].port_list[p].delta_fec_lane_corrected_symbols[2],
                    # self.stats[s].port_list[p].delta_fec_lane_corrected_symbols[3],
                    s,
                    vname,
                    p,
                    self.stats[s].port_list[p].mac,
                    self.stats[s].port_list[p].description,
                    self.stats[s].port_list[p].oper_status,
                    self.stats[s].port_list[p].carrier_changes,
                    self.stats[s].port_list[p].rx_bytes,
                    self.stats[s].port_list[p].crc_errors,
                    self.stats[s].port_list[p].fec_alignment_lock,
                    self.stats[s].port_list[p].fec_corrected_codewords,
                    self.stats[s].port_list[p].fec_uncorrected_codewords,
                    # self.stats[s].port_list[p].fec_lane_corrected_symbols[0],
                    # self.stats[s].port_list[p].fec_lane_corrected_symbols[1],
                    # self.stats[s].port_list[p].fec_lane_corrected_symbols[2],
                    # self.stats[s].port_list[p].fec_lane_corrected_symbols[3],
                    "",
                )  # health
                cur.execute(dbcmd, data)
                self.stats[s].port_list[p].key = cur.lastrowid

        surveycmd = "INSERT INTO survey(survey_type) VALUES('switch')"
        cur.execute(surveycmd)
        survey_id = cur.lastrowid

        for s in self.stats:
            for p in self.stats[s].port_list:
                dscmd = f"INSERT INTO device_survey(survey, devicequery) VALUES(?, ?)"
                data = (survey_id, self.stats[s].port_list[p].key)
                cur.execute(dscmd, data)

        con.commit()
        con.close()
        self.key = survey_id

    def from_db(self, con, switch_id):
        cur = con.cursor()
        # get last runs
        qstring = """
SELECT *
FROM device_survey
    INNER JOIN switch ON device_survey.devicequery = switch.id
WHERE device_survey.survey = ?
"""
        res = cur.execute(qstring, (switch_id,))
        rows = res.fetchall()
        for db_row in rows:
            switchname = db_row["Switch"]
            portname = db_row["Port"]

            pi = PortInfo(portname)
            pi.from_db(db_row)

            if not switchname in self.stats:
                self.stats[switchname] = SwitchInfo(switchname)
                self.vendors[switchname] = pi.vendor

            self.stats[switchname].port_list[portname] = pi


class SwitchInfo:
    def __init__(self, name):
        self.name = name
        self.port_list = dict()
        self.switch_stats = dict(
            psu_health=[],
            temperature_health=[],
            temperature=[],
            fan_health=[],
            fan_rpm=[],
            uptime=[],
            version_info=[]
        )


def main():
    # test
    if len(sys.argv) < 2:
        sys.exit()
    if sys.argv[1] != "test" and sys.argv[1] != "test-txtonly":
        sys.exit()

    if sys.argv[1] == "test-txtonly":
        survey1 = SwitchSurvey([], {}, {}, False, False, "", "")
        survey2 = SwitchSurvey([], {}, {}, False, False, "", "")
        survey1.switches = ["test-switch"]
        survey2.switches = ["test-switch"]
        survey1.stats["test-switch"] = SwitchInfo("test-switch")
        survey2.stats["test-switch"] = SwitchInfo("test-switch")

        if len(sys.argv) > 2 and sys.argv[2] and sys.argv[2] != "-":
            with open(sys.argv[2], "r") as f1:
                survey1.parse("test-switch", f1.readlines())

        if len(sys.argv) > 3 and sys.argv[3] and sys.argv[3] != "-":
            with open(sys.argv[3], "r") as f2:
                survey2.parse("test-switch", f2.readlines())

        # SwitchSurvey.ugh_print_header()
        # SwitchSurvey.ugh_print_two_runs(survey1, survey2)

        SwitchSurvey.print_header()
        SwitchSurvey.print_two_runs(survey1, survey2)
    else:
        s1list = []
        s2list = []
        if len(sys.argv) > 2 and sys.argv[2] and sys.argv[2] != "-":
            s1list = [sys.argv[2]]
        if len(sys.argv) > 3 and sys.argv[3] and sys.argv[3] != "-":
            s2list = [sys.argv[3]]

        survey1 = SwitchSurvey(s1list, {}, {}, False, False, "", "")
        survey2 = SwitchSurvey(s2list, {}, {}, False, False, "", "")

        SwitchSurvey.print_header()
        SwitchSurvey.print_two_runs(survey1, survey2)


if __name__ == "__main__":
    main()
