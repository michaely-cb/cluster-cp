"""
Switch parser for Credo
"""

import logging
import math
import re
from enum import IntEnum

from switch_utils import PortInfo

logger = logging.getLogger(__name__)


class Credo:
    # Collects optics and overall switch status from the 32 port Credo media converter.
    # This device behaves in some strange ways, like all the rest.
    # We can read the transceiver stats over the i2c bus using the cmis-util tool which uses over i2c
    # (using the ipmitool program), or from the /sys files owned by the optoe kernel module.
    # We use the kernel managed interface.
    # The transceiver memory is split into multiple pages, with the sensor stats on the first page
    # and the ID strings, etc in the other pages.
    # The transceiver output is often corrupt (all zeros, repeated bytes, bytes not at their proper offset,
    # etc). The secondary pages are particularly unreliable.
    # We can detect many of these conditions by checking if the first byte is all zeros. If this happens,
    # we can retry the read. The vendor/model/etc are also protected by a checksum that we verify.
    # If we get a bad read, we retry up to 3 times, and then if the result is still bad, we take
    # the fields that should be ok.
    # The transceiver reads are very slow. It takes about 1s per read, whether you read 1 or 255 bytes.
    # If we try to read a field at a time, we can't scrape all 32 ports in the 5 minute scrape interval.
    # Instead we read the full page and then pick out the fields we need.
    # The specs for the memory map, interpretation for power fields, etc for this interface
    # are in the doc named: SFF-8636.
    cmd = [
        'ipmitool fru print 1 >/tmp/fruprint1 2>/dev/null &\n',
        'ipmitool sensor >/tmp/sensor 2>/dev/null &\n',
        'uname\n',
        'cat /proc/uptime\n',
        '/usr/local/bin/fw-util bmc --version\n',
        'export MAX_PORT=32\n',
        'export MAX_RETRY=3\n',
        'function read_loop() {\n',
        '  for r in `seq 1 $MAX_RETRY`; do\n',
        '    read_value=$(hexdump -n 256 -v -e "1/1 \\\"%02x\\\"" /sys/bus/nvmem/devices/Port${i}0/nvmem 2>/dev/null)\n',
        '    if [ -z "$read_value" ]; then\n',
        '      sleep 0.5 # read failed\n',
        '      continue\n',
        '    fi\n',
        '    first_byte=${read_value:0:2}\n',
        '    csum1=0\n',
        '    csum2=0\n',
        '    cc_base="0x${read_value:382:2}"\n',
        '    cc_ext="0x${read_value:446:2}"\n',
        '    for (( c = 128 ; c <= 190 ; c += 1 )) ; do\n',
        '      csum1=$(( $csum1 + "0x${read_value:($c*2):2}" ))\n',
        '    done\n',
        '    tmp1=$csum1\n',
        '    csum1=$(( $csum1 % 256 ))\n',
        '    for (( c = 192 ; c <= 222 ; c += 1 )) ; do\n',
        '      csum2=$(( $csum2 + "0x${read_value:($c*2):2}" ))\n',
        '    done\n',
        '    tmp2=$csum2\n',
        '    csum2=$(( $csum2 % 256 ))\n',
        '    if ! [[ "$first_byte" =~ ^00 ]] && (( $csum1 == $cc_base )) && (( $csum2 == $cc_ext )); then\n',
        '      break # good read\n',
        '    fi\n',
        '    sleep 0.5 # bad read, wait and try again\n',
        '  done\n',
        '}\n',
        'for i in `seq 1 $MAX_PORT`; do\n',
        '  echo ""\n',
        '  read_loop\n',
        '  if ! [[ "$first_byte" =~ ^00 ]]; then\n',
        '    echo "Port:$i,temp_volt: ${read_value:44:12},"\n',
        '    echo "Port:$i,power_current: ${read_value:68:48},"\n',
        '  fi\n',
        '  if (( $csum1 == $cc_base )); then\n',
        '    echo "Port:$i,vendor: ${read_value:296:32},"\n',
        '    echo "Port:$i,pn: ${read_value:336:32},"\n',
        '    echo "Port:$i,rev: ${read_value:368:4},"\n',
        '  fi\n',
        '  if (( $csum2 == $cc_ext )); then\n',
        '    echo "Port:$i,mod_cap: ${read_value:384:2},"\n',
        '    echo "Port:$i,sn: ${read_value:392:32},"\n',
        '  fi\n',
        '  fec_status=$(hexdump -v -e \"1/1 \\\"%02x\\\"" -s 614 -n 1 /sys/bus/nvmem/devices/Port${i}0/nvmem)\n',
        '  echo "Port:$i,fec_status: ${fec_status},"\n',
        '  sleep 0.5\n',
        'done\n',
        'wait\n',
        'cat /tmp/fruprint1\n',
        'cat /tmp/sensor\n',
        'exit\n',
    ]

    mod_cap_table = {"02": "100GBASE-SR4", "25": "100GBASE-DR"}

    def __init__(self):
        return

    @classmethod
    def is_valid_read(cls, test_string):
        # The switch shouldn't send us bad reads but we can check again
        if test_string and not test_string.startswith("00"):
            return True
        return False

    @classmethod
    def power_to_dbm(cls, power_bytes):
        int_val = int(power_bytes, 16)
        power_uw = int_val * 0.0001
        try:
            power_dbm = 10 * math.log10(power_uw)
        except ValueError:
            power_dbm = ""

        return power_dbm

    @classmethod
    def parse(cls, switchname, cmdoutput, switch_info):
        ParserState = IntEnum(
            "ParserState",
            [
                "UPTIME",
                "FIRMWARE_VERSION",
                "XCVR",
                "FRU_PARTNUM",
                "FRU_SERIAL",
                "SENSORS",
            ],
        )
        state = ParserState.UPTIME

        # The terminal doesn't echo the command, but it does echo the shell prompt??
        # So the first lines of a command output looks like
        # root@p3box:~# <output-first-line>
        # <output-second-line>
        fru_start_re = re.compile(r' Board Mfg.*')
        fru_pn_re = re.compile(r' Product Part Number\s+: (.*)$')
        fru_sn_re = re.compile(r' Product Serial\s+: (.*)$')
        sensor_re = re.compile(r'(\S+)\s*\| (\S+)\s*\| [^|]+\| (\S+)\s*\|.*')
        #                         name       value     unit     status
        uptime_re = re.compile(r'.*# ([0-9.]+) .*')
        firmware_version_re = re.compile(r'.*# BMC Version: (.*)')
        # The big 'for' loop causes lots of > > > > to be printed, so we always see
        # this line as expected, eventually
        port_read_re = re.compile(r'^Port:(\d+),(\w+):\s+(.*),$')

        model = None
        firmware_version = None
        uptime = None

        for line in cmdoutput:
            line = line.strip('\r\n')
            # print("credo: switch=%r state=%r line=%r" % (switchname, state, line))

            if state == ParserState.UPTIME:
                res = uptime_re.match(line)
                if res:
                    switch_info.switch_stats['uptime'].append(dict(val=res.group(1)))
                    state = ParserState.FIRMWARE_VERSION
                    continue

            elif state == ParserState.FIRMWARE_VERSION:
                res = firmware_version_re.match(line)
                if res:
                    firmware_version = res.group(1)
                    state = ParserState.XCVR
                    continue

            elif state == ParserState.XCVR:
                res = port_read_re.match(line)
                if res:
                    pname = f'{res.group(1)}'
                    if not switch_info.port_list.get(pname):
                        switch_info.port_list[pname] = PortInfo(pname)
                        switch_info.port_list[pname].mac = ""
                        switch_info.port_list[pname].fec_enabled = 'false'

                    read_type = res.group(2)
                    read_value = res.group(3)

                    read_value = read_value.replace(" ", "")

                    if read_type == "temp_volt" and cls.is_valid_read(read_value):
                        temp = int(read_value[0:4], 16) / 256
                        volt = int(read_value[8:12], 16) / 10000
                        switch_info.port_list[pname].opt_temperature = temp
                        switch_info.port_list[pname].opt_voltage = volt
                        continue

                    if read_type == "power_current" and cls.is_valid_read(read_value):
                        for lane in range(0, 4):
                            base = 4 * lane
                            rx_power = cls.power_to_dbm(read_value[base : base + 4])
                            tx_curr = int(read_value[base + 16 : base + 20], 16) * 0.002
                            tx_power = cls.power_to_dbm(read_value[base + 32 : base + 36])
                            if rx_power:
                                switch_info.port_list[pname].opt_rx_power[str(lane + 1)] = rx_power
                            if tx_curr:
                                switch_info.port_list[pname].opt_tx_current[str(lane + 1)] = tx_curr
                            if tx_power:
                                switch_info.port_list[pname].opt_tx_power[str(lane + 1)] = tx_power
                        continue

                    if read_type == "fec_status":
                        if read_value == "80":
                            switch_info.port_list[pname].fec_enabled = 'true'
                        continue

                    if read_type == "vendor" and cls.is_valid_read(read_value):
                        vendor = ""
                        for i in range(0, int(len(read_value) / 2)):
                            vendor += chr(int(read_value[i * 2 : i * 2 + 2], 16))
                        vendor = vendor.rstrip()
                        if vendor.isascii() and vendor.isprintable():
                            switch_info.port_list[pname].opt_vendor = vendor
                        continue

                    if read_type == "mod_cap" and cls.is_valid_read(read_value):
                        mod_cap = cls.mod_cap_table.get(read_value, None)
                        if mod_cap:
                            switch_info.port_list[pname].opt_speed = mod_cap
                        else:
                            logger.info(
                                f"Credo: switch {switchname}:{pname}: not setting modcap; value was {read_value}"
                            )
                        continue

                    if read_type == "pn" and cls.is_valid_read(read_value):
                        pn = ""
                        for i in range(0, int(len(read_value) / 2)):
                            pn += chr(int(read_value[i * 2 : i * 2 + 2], 16))
                        pn = pn.rstrip()
                        if pn.isascii() and pn.isprintable():
                            switch_info.port_list[pname].opt_partnum = pn
                        continue

                    if read_type == "rev" and cls.is_valid_read(read_value):
                        rev = ""
                        for i in range(0, int(len(read_value) / 2)):
                            rev += chr(int(read_value[i * 2 : i * 2 + 2], 16))
                        rev = rev.rstrip()
                        if rev.isascii() and rev.isprintable():
                            switch_info.port_list[pname].opt_rev = rev
                        continue

                    if read_type == "sn" and cls.is_valid_read(read_value):
                        sn = ""
                        for i in range(0, int(len(read_value) / 2)):
                            sn += chr(int(read_value[i * 2 : i * 2 + 2], 16))
                        sn = sn.rstrip()
                        if sn.isascii() and sn.isprintable():
                            switch_info.port_list[pname].opt_sernum = sn.rstrip()
                        continue

                res = fru_start_re.match(line)
                if res:
                    state = ParserState.FRU_PARTNUM
                    continue

            elif state == ParserState.FRU_PARTNUM:
                res = fru_pn_re.match(line)
                if res:
                    model = res.group(1).rstrip()
                    switch_info.switch_stats['version_info'].append(
                        dict(vendor='Credo', model=model, firmware_version=firmware_version, val=1)
                    )
                    state = ParserState.FRU_SERIAL
                    continue

            elif state == ParserState.FRU_SERIAL:
                res = fru_sn_re.match(line)
                if res:
                    # We don't collect this currently, this is a placeholder in case we add it
                    serial = res.group(1)
                    state = ParserState.SENSORS
                    continue

            elif state == ParserState.SENSORS:
                res = sensor_re.match(line)
                if res:
                    desc = res.group(1)
                    val = res.group(2)
                    status = res.group(3)
                    if "TACH" in desc:
                        switch_info.switch_stats['fan_health'].append(
                            dict(desc=desc, val=(0 if status == "ok" else 2))
                        )
                        switch_info.switch_stats['fan_rpm'].append(dict(desc=desc, val=val))
                    elif "POUT_DC" in desc:
                        switch_info.switch_stats['psu_health'].append(
                            dict(desc=desc, val=(0 if status == "ok" else 2))
                        )
                    elif "_TMP" in desc and not desc.startswith("XCVR"):
                        switch_info.switch_stats['temperature_health'].append(
                            dict(desc=desc, val=(0 if status == "ok" else 2))
                        )
                        switch_info.switch_stats['temperature'].append(dict(desc=desc, val=val))
                    continue

        return
