def data_rate_to_kbps_converter(rate, unit):
    if unit == "b/s" or unit == "bps":
        rate /= 1024
    elif unit == "bytes/sec" or unit == "B/s":
        rate /= 1024 / 8
    elif unit == "Kb/s":
        pass
    elif unit == "KB/s":
        rate *= 8
    elif unit == "Mb/s" or unit == "Mbps":
        rate *= 1024
    elif unit == "MB/s":
        rate *= 1024 * 8
    elif unit == "Gb/s":
        rate *= 1024 * 1024
    elif unit == "GB/s":
        rate *= 1024 * 1024 * 8
    return rate


def floatOrMinusOne(s: str) -> float:
    try:
        return float(s)
    except ValueError:
        return -1

class PortInfo:
    def __init__(self, name):
        self.key = 0

        self.mac = None
        self.switch = None
        self.vendor = None
        self.name = name
        self.description = ""
        self.ifindex = None
        self.speed = None
        self.oper_status = None
        self.errdisabled = None
        self.errdisabled_reason = ""
        self.carrier_changes = None
        self.rx_bytes = None
        # self.ifLastChange = -1
        self.crc_errors = None
        self.input_errors = None
        self.runts = None
        self.fec_alignment_lock = ""
        self.fec_enabled = ""

        # These None values may be junk/stale on Arista if the port is down.
        # Don't report them in this case.
        self.fec_corrected_codewords = None
        self.fec_uncorrected_codewords = None

        self.delta_fec_corrected_codewords = None
        self.delta_fec_uncorrected_codewords = None

        self.opt_speed = ""
        self.opt_vendor = ""
        self.opt_partnum = ""
        self.opt_rev = ""
        self.opt_sernum = ""

        self.opt_temperature = None
        self.opt_voltage = None
        self.opt_tx_current = dict()
        self.opt_tx_power = dict()
        self.opt_rx_power = dict()

        self.rx_pfc = None
        self.tx_pfc = None

        self.in_discards = None
        self.out_discards = None
        self.out_discards_prio5 = None

        self.rx_kbps = None
        self.tx_kbps = None

        self.ecn_marked_packets_tc5 = None

    def clear_stale(self):
        # We may set a few values before we see the string indicating
        # that the results are stale, so we may need to clear them again.
        self.fec_corrected_codewords = None
        self.fec_uncorrected_codewords = None

        self.delta_fec_corrected_codewords = None
        self.delta_fec_uncorrected_codewords = None

        self.opt_speed = ""
        self.opt_vendor = ""
        self.opt_partnum = ""
        self.opt_rev = ""
        self.opt_sernum = ""

        self.opt_temperature = None
        self.opt_voltage = None

        self.opt_tx_current = dict()
        self.opt_tx_power = dict()
        self.opt_rx_power = dict()

        self.ecn_marked_packets_tc5 = None

    # This is not used when running under prometheus
    def from_db(self, db_row):
        # replace this with self.id ? self.key = db_row.
        self.mac = db_row["MAC"]
        self.switch = db_row["Switch"]
        self.vendor = db_row["Vendor"]
        self.name = db_row["Port"]
        self.description = db_row["Description"]
        self.oper_status = db_row["OperState"]
        self.carrier_changes = int(db_row["Flaps"])
        self.rx_bytes = int(db_row["RxKBytes"])
        self.crc_errors = int(db_row["CRC_errors"])
        self.fec_alignment_lock = db_row["FEC_align"]
        self.fec_corrected_codewords = int(db_row["FEC_cor_cword"])
        self.fec_uncorrected_codewords = int(db_row["FEC_uncor_cword"])

        self.delta_fec_corrected_codewords = int(db_row["delta_corr"])
        self.delta_fec_uncorrected_codewords = int(db_row["delta_uncorr"])
