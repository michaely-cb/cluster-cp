"""
Switch parser for JUNOS
"""

import re
from enum import IntEnum

from switch_utils import PortInfo, data_rate_to_kbps_converter


class Juniper:
    cmd = [
        "set cli screen-width 1000\n",
        "set cli screen-length 0\n",
        "show interfaces extensive\n",
        "show version\n",
        "show interfaces diagnostics optics\n",
        "show version\n",
        "show interfaces queue\n",
        "show chassis pic fpc-slot 0 pic-slot 0\n",
        "show chassis hardware\n",
        "exit\n",
    ]

    def __init__(self):
        return

    @staticmethod
    def parse(switchname, cmdoutput):
        # returns the port_list[portname] dict
        ParserState = IntEnum(
            "ParserState",
            [
                "START",
                "INT_STATUS",
                "OPTICS_IFACE",
                "OPTICS",
                "OPTICS_LANE",
                "QUEUE_IFACE",
                "QUEUE_LIST",
                "QUEUE_ROCE",
                "XCVR",
                "XCVR_SN",
            ],
        )

        # These are in the START state
        int_header_re = re.compile(
            r"Physical interface: (et-[0-9/:]+), .*, Physical link is (\w+)"
        )  # name, up/down
        hostname_re = re.compile(r"Hostname: ")

        # These are in the INT_STATUS state - could do a separate state for each line,
        # but just do one in case they ever appear out of order
        desc_re = re.compile(r"  Description: (.*)$")
        speed_re = re.compile(r"  Link-level type: Ethernet, MTU:.* Speed: (\d+)Gbps,.*")
        mac_re = re.compile(r"  Current address: ([0-9a-f:]+),.*")
        in_bytes_re = re.compile(
            r"   Input\s*bytes\s*:\s*(\d+)\s*(\d+) (\w+)"
        )  # bytes, rate, rate-units
        out_bytes_re = re.compile(r"   Output\s*bytes\s*:\s*\d+\s*(\d+) (\w+)")  # rate, rate-units
        in_errors_re = re.compile(r"    Errors: (\d+), Drops: (\d+), Framing errors: (\d+), Runts: (\d+), .*")
        out_errors_re = re.compile(r"    Carrier transitions: (\d+), .* Drops: (\d+),.*")
        fec_corr_re = re.compile(r"    FEC Corrected Errors\s*(\d+)")
        fec_uncorr_re = re.compile(r"    FEC Uncorrected Errors\s*(\d+)")
        pfc_re = re.compile(r"    Priority :  5\s*(\d+)\s*(\d+)")  # rxpfc, txpfc
        logical_iface_re = re.compile(r"  Logical interface.*")  # exit condition

        # This is in the OPTICS_IFACE state
        optics_header_re = re.compile(r"Physical interface: (et-[0-9/:]+)")

        # These are in the OPTICS state
        optics_temp_re = re.compile(r"    Module temperature\s*:\s*(\d+) degrees C.*")
        optics_voltage_re = re.compile(r"    Module voltage\s*:\s*([0-9.]+) V")
        optics_lane_re = re.compile(r"  Lane (\d+)")

        # These are in the OPTICS_LANE state
        optics_current_re = re.compile(r"    Laser bias current\s*:\s*([0-9.]+) mA")
        optics_tx_power_re = re.compile(
            r"    Laser output power\s*:\s*[0-9.]+ mW / (-?[0-9.]+) dBm"
        )
        optics_rx_power_re = re.compile(
            r"    Laser receiver power\s*:\s*[0-9.]+ mW / (-?[0-9.]+) dBm"
        )

        # This is in the QUEUE_IFACE state
        queue_header_re = re.compile(r"Physical interface: (et-[0-9/:]+), .*")

        # This is in the QUEUE_LIST state
        queue_roce_re = re.compile(r"Queue:\s*5.*")

        # This is in the QUEUE_ROCE state
        queue_ecnce_packets_re = re.compile(r"    ECN-CE packets\s*:\s*(\d+).*")

        # This is in the XCVR state
        xcvr_header_re = re.compile(r"  Port\s*Cable type\s*type.*")
        xcvr_re = re.compile(
            r"  (\d+)\s+(\S*) \s*\S*\s*(\S*)\s*(\S*)\s*.*"
        )  # port, opt_speed, vendor, partno
        # ?? Do we need to look at the speed table that follows, or is the cable type here enough?
        xcvr_speed_header_re = re.compile(r"Port speed information:")

        # This is in the XCVR_SN state
        xcvr_sn_header_re = re.compile(r"Hardware inventory:")
        xcvr_sn_re = re.compile(r"    Xcvr (\d+)\s+(\S*\s?\S*)  \s*\S+\s+(\S*)\s+.*")  # rev, serno

        # Note: This assumes everything is in one CB/FPC ?
        xcvr_to_pname_re = re.compile(r"et-0/0/(\d+).*")

        state = ParserState.START
        pname = None
        lane = None

        port_list = dict()

        for line in cmdoutput:
            line = line.strip("\r\n")
            # print("JUNIPER state=%d got line: %r" % (state,line))

            if state == ParserState.START:
                res = int_header_re.match(line)
                if res:
                    pname = res.group(1)
                    port_list[pname] = PortInfo(pname)
                    port_list[pname].oper_status = 1 if res.group(2).lower() == "up" else 0

                    state = ParserState.INT_STATUS
                    continue

                res = hostname_re.match(line)
                if res:
                    state = ParserState.OPTICS_IFACE
                    continue

            elif state == ParserState.INT_STATUS:
                # Termination condition
                if line == "":
                    pname = None
                    state = ParserState.START
                    continue

                # Termination condition
                res = logical_iface_re.match(line)
                if res:
                    pname = None
                    state = ParserState.START
                    continue

                # Termination condition (don't expect to ever take this branch)
                res = int_header_re.match(line)
                if res:
                    pname = res.group(1)
                    port_list[pname] = PortInfo(pname)
                    port_list[pname].oper_status = 1 if res.group(2).lower() == "up" else 0

                    state = ParserState.INT_STATUS
                    continue

                res = desc_re.match(line)
                if res:
                    port_list[pname].description = res.group(1)
                    continue

                res = speed_re.match(line)
                if res:
                    port_list[pname].speed = int(res.group(1)) * 1000
                    continue

                res = mac_re.match(line)
                if res:
                    port_list[pname].mac = res.group(1)
                    continue

                res = in_bytes_re.match(line)
                if res:
                    port_list[pname].rx_bytes = int(res.group(1))
                    rx_rate = int(res.group(2))
                    rx_unit = res.group(3)
                    rx_kbps = data_rate_to_kbps_converter(rx_rate, rx_unit)
                    port_list[pname].rx_kbps = int(rx_kbps)
                    continue

                res = out_bytes_re.match(line)
                if res:
                    tx_rate = int(res.group(1))
                    tx_unit = res.group(2)
                    tx_kbps = data_rate_to_kbps_converter(tx_rate, tx_unit)
                    port_list[pname].tx_kbps = int(tx_kbps)
                    continue

                res = in_errors_re.match(line)
                if res:
                    port_list[pname].input_errors = int(res.group(1))
                    port_list[pname].in_discards = int(res.group(2))
                    port_list[pname].crc_errors = int(res.group(3))
                    port_list[pname].runts = int(res.group(4))
                    continue

                res = out_errors_re.match(line)
                if res:
                    port_list[pname].carrier_changes = int(res.group(1))
                    port_list[pname].out_discards = int(res.group(2))
                    continue

                res = fec_corr_re.match(line)
                if res:
                    port_list[pname].fec_corrected_codewords = int(res.group(1))
                    continue

                res = fec_uncorr_re.match(line)
                if res:
                    port_list[pname].fec_uncorrected_codewords = int(res.group(1))
                    continue

                res = pfc_re.match(line)
                if res:
                    port_list[pname].rx_pfc = int(res.group(1))
                    port_list[pname].tx_pfc = int(res.group(2))
                    continue

            elif state == ParserState.OPTICS_IFACE:
                lane = None
                pname = None
                res = optics_header_re.match(line)
                if res:
                    pname = res.group(1)
                    if pname in port_list:
                        state = ParserState.OPTICS
                    continue

                res = hostname_re.match(line)
                if res:
                    state = ParserState.QUEUE_IFACE
                    continue

            elif state == ParserState.OPTICS:
                if line == "":
                    state = ParserState.OPTICS_IFACE
                    continue

                res = optics_temp_re.match(line)
                if res:
                    port_list[pname].opt_temperature = float(res.group(1))
                    continue

                res = optics_voltage_re.match(line)
                if res:
                    port_list[pname].opt_voltage = float(res.group(1))
                    continue

                res = optics_lane_re.match(line)
                if res:
                    lane = res.group(1)
                    state = ParserState.OPTICS_LANE
                    continue

            elif state == ParserState.OPTICS_LANE:
                if line == "":
                    state = ParserState.OPTICS_IFACE
                    continue

                res = optics_lane_re.match(line)
                if res:
                    lane = res.group(1)
                    state = ParserState.OPTICS_LANE
                    continue

                res = optics_current_re.match(line)
                if res:
                    port_list[pname].opt_tx_current[lane] = float(res.group(1))
                    continue

                res = optics_tx_power_re.match(line)
                if res:
                    port_list[pname].opt_tx_power[lane] = float(res.group(1))
                    continue

                res = optics_rx_power_re.match(line)
                if res:
                    port_list[pname].opt_rx_power[lane] = float(res.group(1))
                    continue

            elif state == ParserState.QUEUE_IFACE:
                pname = None
                res = queue_header_re.match(line)
                if res:
                    pname = res.group(1)
                    if pname in port_list:
                        state = ParserState.QUEUE_LIST
                    continue

                res = xcvr_header_re.match(line)
                if res:
                    state = ParserState.XCVR
                    continue

            elif state == ParserState.QUEUE_LIST:
                if line == "":
                    state = ParserState.QUEUE_IFACE
                    continue

                res = queue_roce_re.match(line)
                if res:
                    state = ParserState.QUEUE_ROCE

            elif state == ParserState.QUEUE_ROCE:
                if line == "":
                    state = ParserState.QUEUE_IFACE
                    continue

                res = queue_ecnce_packets_re.match(line)
                if res:
                    port_list[pname].ecn_marked_packets_tc5 = int(res.group(1))
                    state = ParserState.QUEUE_IFACE
                    continue

            elif state == ParserState.XCVR:
                res = xcvr_sn_header_re.match(line)
                if res:
                    state = ParserState.XCVR_SN
                    continue

                res = xcvr_speed_header_re.match(line)
                if res:
                    state = ParserState.XCVR_SN
                    continue

                res = xcvr_re.match(line)
                if res:
                    # need to duplicate this on all sub-ifaces
                    port = res.group(1)
                    for pname in port_list:
                        pres = xcvr_to_pname_re.match(pname)
                        if pres:
                            if pres.group(1) == port:
                                port_list[pname].opt_speed = res.group(2)
                                port_list[pname].opt_vendor = res.group(3)
                                port_list[pname].opt_partnum = res.group(4)

            elif state == ParserState.XCVR_SN:
                res = xcvr_sn_re.match(line)
                if res:
                    # need to duplicate this on all sub-ifaces
                    port = res.group(1)
                    for pname in port_list:
                        pres = xcvr_to_pname_re.match(pname)
                        if pres:
                            if pres.group(1) == port:
                                port_list[pname].opt_rev = res.group(2)
                                port_list[pname].opt_sernum = res.group(3)

        return port_list
