"""
Arista Fast Polling module for linkmon
Used by the Switch component of linkmon on Arista switches - these switches
do not maintain counters of FEC stats, but only report the latest internal
scrape of the debug registers according to some internal (not-adjustable)
schedule. The Switch component creates a thread to run this code against
an Arista switch when FEC counts are increasing so that we can attempt to see
each reported value when the switch does a read of the debug registers.
Whenever the HF monitor sees a new value, it appends the update to a log.
The Switch class periodically locks the log, reads all values, and then
updates the Switch's port state records.
"""
import asyncio
import re
import threading
import time
from collections import namedtuple

import asyncssh

# watch the changes field. If we're missing changes, lower the period unless
# we're already attempting <1s
# if we're getting N reads in a row with no changes, increase the period


ErrorStat = namedtuple(
    'ErrorStat',
    [
        'time',
        'phystate',
        'corr',
        'corr_ch',
        'corr_ch_time',
        'uncorr',
        'uncorr_ch',
        'uncorr_ch_time',
    ],
)
# also start tracking PCS BER?


class AristaFastPollingWorker:
    def __init__(self, hostname, portname, init_delta_corr, init_delta_uncorr, users, pws):
        self.switch = hostname
        self.port = portname
        self.stop_request = False
        self.stopped = False
        self.poll_period = 5  # sec, min should be 0.7? max300?
        # period should really be 1, setting to 10 for test
        # dump to public log every 100 queries - about 1m
        self.log = []
        self.loglock = threading.Lock()

        # update these whenever log is cleared
        self.init_delta_corr = init_delta_corr
        self.init_delta_uncorr = init_delta_uncorr

        self.user = users[hostname]
        self.password = pws[hostname]

        self.thread = threading.Thread(target=self.async_run, args=())
        self.thread.start()

    def async_run(self):
        asyncio.get_event_loop().run_until_complete(self.run())

    # this is not the right way to use asyncio
    async def run(self):
        time_re = re.compile(r'Current System Time: .* (\d\d:\d\d:\d\d).*')
        phy_state_re = re.compile(r'\s+PHY state\s+(\w+).*')
        fec_cc_re = re.compile(r'\s*FEC corrected codewords\s+(\d+)\s+(\d+)\s+(.*)$')
        fec_uc_re = re.compile(r'\s*FEC uncorrected codewords\s+(\d+)\s+(\d+)\s+(.*)$')

        last_corr_ch = -1
        last_uncorr_ch = -1
        querycount = 0
        idle_count = 0
        async with asyncssh.connect(
            self.switch, self.user, self.password, known_hosts=None
        ) as conn:
            while not self.stop_request:  # True:
                ts = time.time()
                # EOS 4.23 disables grep by default (what?) so we can't filter on the switch
                # cmd = f"show int {self.port} phy det | grep -e 'System Time' -e 'codewords' -e 'PHY state'"
                # res = c.run_command(f"show int {self.port} phy det ")
                # c.join()
                res = await conn.run(f"show int {self.port} phy det ", check=True)

                st = sp = sc = scc = su = suc = None
                # for line in res[0].stdout:
                for line in res.stdout.split("\n"):
                    if not st:
                        tmatch = time_re.match(line)
                        if tmatch:
                            st = tmatch.group(1)

                    if not sp:
                        pmatch = phy_state_re.match(line)
                        if pmatch:
                            sp = pmatch.group(1)

                    if not sc:
                        cmatch = fec_cc_re.match(line)
                        if cmatch:
                            sc = int(cmatch.group(1))
                            scc = int(cmatch.group(2))

                    if not su:
                        umatch = fec_uc_re.match(line)
                        if umatch:
                            su = int(umatch.group(1))
                            suc = int(umatch.group(2))

                s = ErrorStat(st, sp, sc, scc, '', su, suc, '')

                with self.loglock:
                    self.log.append(s)
                if s.corr_ch == last_corr_ch and s.uncorr_ch == last_uncorr_ch:
                    self.poll_period *= 1.6
                    if self.poll_period > 300:
                        self.poll_period = 300

                    # if idle count reached, stop
                    idle_count += 1
                    if idle_count >= 5:
                        break
                elif s.corr_ch > last_corr_ch + 1 or s.uncorr_ch > last_uncorr_ch + 1:
                    self.poll_period *= 0.7
                    if self.poll_period < 0.7:
                        self.poll_period = 0.7
                    idle_count = 0
                else:  # just one error?
                    idle_count = 0
                last_corr_ch = s.corr_ch
                last_uncorr_ch = s.uncorr_ch

                querycount += 1

                te = time.time()
                if te - ts < self.poll_period:
                    time.sleep(self.poll_period - (te - ts))

        self.stopped = True
