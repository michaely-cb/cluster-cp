"""
NodeInfo module for linkmon.
Used in both prometheus and standalone linkmon modes. Each NodeInfo stores the
state of a single NIC on a node.
"""

import glob
import json
import logging
import os
import re
import subprocess
import tempfile
from typing import Dict, List, Optional, Tuple, Union

from prometheus_client.core import GaugeMetricFamily


def int_or_zero(s):
    try:
        return int(s)
    except ValueError:
        return 0


def int_or_none(s, base=10):
    try:
        return int(s, base)
    except:
        return None


def float_or_none(s):
    try:
        return float(s)
    except:
        return None


def read_file(path: str) -> str:
    """Read a file and return its contents."""
    try:
        with open(path, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def run_cmd(cmd: List[str]) -> Tuple[str, int]:
    """Run a command and return its output and return code."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout, result.returncode
    except Exception:
        return "", 1


def extract_value(lines: List[str], pattern: str) -> str:
    """Extract a value from lines using regex pattern."""
    for line in lines:
        match = re.search(pattern, line)
        if match:
            return match.group(1).strip()
    return ""


def find_network_interfaces() -> List[str]:
    """Find all network interfaces starting with 'e'."""
    return sorted([os.path.basename(path) for path in glob.glob("/sys/class/net/e*")])


def collect_interface_data(host, ifname: str) -> Dict[str, Union[str, Dict]]:
    """Collect data for a single network interface."""
    sys_path = f"/sys/class/net/{ifname}"

    data = {"host": host, "ifname": ifname}

    data["mac"] = read_file(f"{sys_path}/address")
    data["carrier"] = int(read_file(f"{sys_path}/carrier"))
    data["carrier_changes"] = int(read_file(f"{sys_path}/carrier_changes"))
    data["operstate"] = 1 if read_file(f"{sys_path}/operstate").lower() == "up" else 0
    data["speed"] = int_or_zero(read_file(f"{sys_path}/speed"))
    data["iftype"] = int(read_file(f"{sys_path}/type"))

    eth_output, data["ethtool_error"] = run_cmd(["ethtool", ifname])
    data["supports_1g"] = eth_output.count("1000base") > 0
    data["supports_10g"] = eth_output.count("10000base") > 0
    data["supports_100g"] = eth_output.count("100000base") > 0
    data["supports_200g"] = eth_output.count("200000base") > 0
    data["supports_400g"] = eth_output.count("400000base") > 0

    data["has_mlx"] = run_cmd(["lspci"])[0].lower().count("mellanox") > 0
    data["boottime"] = run_cmd(["uptime", "-s"])[0]

    supports_100g = data["supports_100g"] and not data["ethtool_error"]

    eth_stats = run_cmd(["ethtool", "-S", ifname])[0].splitlines()
    stat_patterns = [
        ("rx_crc_errors_phy", r'(?:rx_crc_errors_phy|rx_fcs_err_frames):\s*(\d+)'),
        ("rx_bytes", r'rx_bytes:\s*(\d+)'),
        ("rx_bytes_phy", r'rx_bytes_phy:\s*(\d+)'),
        ("tx_bytes", r'tx_bytes:\s*(\d+)'),
        ("tx_bytes_phy", r'tx_bytes_phy:\s*(\d+)'),
        ("rx_pcs_symbol_err_phy", r'(?:rx_pcs_symbol_err_phy|rx_pcs_symbol_err):\s*(\d+)'),
        ("rx_corrected_bits_phy", r'(?:rx_corrected_bits_phy|rx_corrected_bits):\s*(\d+)'),
        ("tx_cqe_err", r'tx_cqe_err:\s*(\d+)'),
        ("rx_wqe_err", r'(?:rx_wqe_err|res_wqe_format_err):\s*(\d+)'),
        ("rx_pfc", r'(?:rx_prio5_pause|tx_pfc_ena_frames_pri5):\s*(\d+)'),
        ("tx_pfc", r'(?:tx_prio5_pause|rx_pfc_ena_frames_pri5):\s*(\d+)'),
        ("rx_pfc_duration", r'rx_prio5_pause_duration:\s*(\d+)'),
        ("tx_pfc_duration", r'tx_prio5_pause_duration:\s*(\d+)'),
        ("tx_discards_phy", r'(?:tx_discards_phy|tx_discards):\s*(\d+)'),
        ("rx_discards_phy", r'(?:rx_discards_phy|rx_discards):\s*(\d+)'),
        ("rx_discards_prio5", r'rx_prio5_discards:\s*(\d+)'),
        ("module_bad_shorted", r'module_bad_shorted:\s*(\d+)'),
        ("module_high_temp", r'module_high_temp:\s*(\d+)'),
        (
            "tx_pause_storm_error_events",
            r'tx_pause_storm_error_events:\s*(\d+)',
        ),  # could be continuous_roce_pause_events on thor2 but may not be the same condition
    ]

    transceiver_patterns = [
        ("opt_vendor_name", r'Vendor name\s*:\s*(.+)'),
        ("opt_vendor_oui", r'Vendor OUI\s*:\s*(.+)'),
        ("opt_vendor_pn", r'Vendor PN\s*:\s*(.+)'),
        ("opt_vendor_rev", r'Vendor rev\s*:\s*(.+)'),
        ("opt_vendor_sn", r'Vendor SN\s*:\s*(.+)'),
        ("opt_temperature", r'Module temperature\s*:\s*([+-]?\d+\.?\d+)'),
        ("opt_temperature_alarm", r'Module temperature high alarm\s*:\s*(\w+)'),
    ]

    ib_counters = [
        "local_ack_timeout_err",
        "implied_nak_seq_err",
        "packet_seq_err",
        "out_of_sequence",
        "rnr_nak_retry_err",
        "out_of_buffer",
        "req_cqe_error",
        "resp_cqe_error",
        "np_cnp_sent",
        "np_ecn_marked_roce_packets",
        "rp_cnp_handled",
        "rp_cnp_ignored",
        "roce_slow_restart_cnps",
        "roce_adp_retrans",
        "roce_adp_retrans_to",
    ]

    if supports_100g:
        for key, pattern in stat_patterns:
            data[key] = int_or_none(extract_value(eth_stats, pattern))

        # Dynamically handle RX errors - find all lanes that have data
        for line in eth_stats:
            lane_match = re.search(r'rx_err_lane_(\d+)_phy:\s*(\d+)', line)
            if lane_match:
                lane_num = lane_match.group(1)
                lane_value = lane_match.group(2)
                data.setdefault("rx_err_lane_phy", {})[lane_num] = int_or_none(lane_value)

        nic_type = None
        # There should only be 1 or 0 mlx paths
        ib_paths = glob.glob(f"{sys_path}/device/infiniband/[mlx,bnxt]*")
        if ib_paths:
            ib_path = ib_paths[0]
            if "mlx" in ib_path:
                nic_type = "mlx"
            elif "bnxt" in ib_path:
                nic_type = "bnxt"
            for counter in ib_counters:
                path = f"{ib_path}/ports/1/hw_counters/{counter}"
                data[counter] = int_or_none(read_file(path))

        eth_module = None
        if nic_type == "mlx":
            bus_info = extract_value(
                run_cmd(["ethtool", "-i", ifname])[0].splitlines(),
                r'bus-info:\s*(.+)',
            )
            if bus_info:
                cmd = [
                    "mstreg",
                    "-d",
                    bus_info,
                    "--get",
                    "--reg_name",
                    "MTMP",
                    "--indexes",
                    "sensor_index=0,slot_index=0",
                ]
                mstreg_output = run_cmd(cmd)[0]
                temperature = extract_value(mstreg_output.splitlines(), r'temperature\s*\|\s*(.+)')

                if asic_temp := int_or_none(temperature, 16):
                    data["temperature"] = asic_temp / 8.0

            eth_module = run_cmd(["ethtool", "-m", ifname])[0].splitlines()

        elif nic_type == "bnxt":
            # niccli needs the NIC index
            nic_index = None
            with tempfile.NamedTemporaryFile() as f:
                _, rc = run_cmd(["niccli", "-e", "--json", f.name])
                if rc == 0:
                    try:
                        nic_idx_json = read_file(f.name)
                        ifjson = json.loads(nic_idx_json)
                        for idx in ifjson['niccli']['collections']['list-eth']:
                            k = next(iter(idx))
                            if idx[k]['Interface'] == ifname:
                                k_number_re = re.match(r'Adapter (\d+)', k)
                                k_number = k_number_re.group(1)
                                nic_index = k_number
                    except Exception as e:
                        logging.error(f"Could not find the broadcom NIC index for interface {ifname}: {e}")
                else:
                    logging.error("Failed to run niccli -e to find the broadcom NIC index mapping")

            if nic_index:
                eth_module = run_cmd(["niccli", "-i", nic_index, "cable", "-m", "--show"])[
                    0
                ].splitlines()
                show_d_output = run_cmd(["niccli", "-i", nic_index, "show", "-d"])[0].splitlines()
                temperature = extract_value(show_d_output, r'Device Temperature\s*: (\d+) Celsius')
                data["temperature"] = float_or_none(temperature)

        if eth_module and (nic_type == "mlx" or nic_type == "bnxt"):
            for key, pattern in transceiver_patterns:
                data[key] = extract_value(eth_module, pattern)
            data["opt_temperature"] = float_or_none(data["opt_temperature"])
            data['opt_temperature_alarm'] = (
                1 if data['opt_temperature_alarm'].lower() == "on" else 0
            )

            # Dynamically handle TX current and TX/RX power measurements
            # We report whatever ethtool gives us - which means we often report
            # I/P values of 0 for lanes 5-8 (or 2-8). Sometimes we can get the
            # actual lane count from ethtool (which gives a lane count, sometimes),
            # or ethtool -m (which gives a module type, sometimes) and then drop
            # the metrics we know aren't relevant, but the presence of this information
            # seems to be transceiver specific.
            for line in eth_module:
                tx_curr_match = re.search(
                    r'tx bias current\s*\(Channel\s*(\d+)\)\s*:\s*([+-]?\d+\.?\d+)',
                    line,
                )
                if tx_curr_match:
                    lane = tx_curr_match.group(1)
                    value = tx_curr_match.group(2)
                    data.setdefault("tx_curr", {})[lane] = float_or_none(value)

                tx_pwr_match = re.search(
                    r'Transmit avg optical power\s*\(Channel\s*(\d+)\)\s*:\s*([+-]?\d+\.?\d+)',
                    line,
                )
                if tx_pwr_match:
                    lane = tx_pwr_match.group(1)
                    value = tx_pwr_match.group(2)
                    data.setdefault("tx_pwr", {})[lane] = float_or_none(value)

                rx_pwr_match = re.search(
                    r'Rcvr signal avg optical power\s*\(Channel\s*(\d+)\)\s*:\s*([+-]?\d+\.?\d+)',
                    line,
                )
                if rx_pwr_match:
                    lane = rx_pwr_match.group(1)
                    value = rx_pwr_match.group(2)
                    data.setdefault("rx_pwr", {})[lane] = float_or_none(value)

    return data


class NodeInfo:
    metrics_to_descr = {
        'speed': 'NIC current speed',
        'carrier': 'NIC carrier present',
        'carrier_changes': 'NIC carrier changes',
        'operstate': 'NIC operating state',
        'ethtool_error': 'NIC driver state',
        'rx_crc_errors_phy': 'NIC PHY CRC error count',
        'rx_bytes': 'NIC bytes received count',
        'rx_bytes_phy': 'NIC PHY bytes received count',
        'tx_bytes': 'NIC bytes transmitted count',
        'tx_bytes_phy': 'NIC PHY bytes transmitted count',
        'rx_pcs_symbol_err_phy': 'NIC PHY PCS symbol error count',
        'rx_corrected_bits_phy': 'NIC FEC corrected bits count',
        'packet_seq_err': 'NIC packet_seq_err count',
        'out_of_sequence': 'NIC out_of_sequence count',
        'rnr_nak_retry_err': 'NIC RNR NAK retry error count',
        'out_of_buffer': 'NIC out of buffer count',
        'req_cqe_error': 'NIC request CQE error count',
        'resp_cqe_error': 'NIC response CQE error count',
        'np_cnp_sent': 'NIC CNP packets sent count',
        'np_ecn_marked_roce_packets': 'NIC RoCE congested packets received count',
        'rp_cnp_handled': 'NIC CNP packets handled count',
        'rp_cnp_ignored': 'NIC CNP packets ignored count',
        'roce_slow_restart_cnps': 'NIC CNP packets caused by RoCE slow restart count',
        'roce_adp_retrans': 'NIC adaptive retransmission count',
        'roce_adp_retrans_to': 'NIC timeout due to adaptive retransmission count',
        'tx_cqe_err': 'NIC TX CQE error count',
        'rx_wqe_err': 'NIC RX WQE error count',
        'opt_temperature': 'NIC optics temperature',
        'opt_temperature_alarm': 'NIC optics temperature in alarm condition',
        'temperature': 'NIC temperature',
        'tx_pfc': 'NIC TX PFC count',
        'rx_pfc': 'NIC RX PFC count',
        'tx_pfc_duration': 'NIC TX PFC duration',
        'rx_pfc_duration': 'NIC RX PFC duration',
        'tx_discards_phy': 'NIC PHY TX discards count',
        'rx_discards_phy': 'NIC PHY RX discards count',
        'rx_discards_prio5': 'NIC RX prio5 discards count',
        'local_ack_timeout_err': 'NIC local ack timeout error count',
        'implied_nak_seq_err': 'NIC implied nak seq error count',
        'module_bad_shorted': 'NIC PHY bad shorted count',
        'module_high_temp': 'NIC PHY high temp conditions count',
        'tx_pause_storm_error_events': 'NIC TX pause storm error events count',
    }

    lane_metrics_to_descr = {
        'rx_err_lane_phy': 'NIC PHY lane RX error count',
        'tx_curr': 'NIC TX current',
        'tx_pwr': 'NIC TX power',
        'rx_pwr': 'NIC RX power',
    }

    @classmethod
    def create_metrics(cls, dynamic_metrics: Optional[Dict[str, str]] = None):
        l = ['node', 'interface']
        ext_l = [
            'node',
            'interface',
            'mac',
            'max_speed',
            'opt_vendor_name',
            'opt_vendor_oui',
            'opt_vendor_pn',
            'opt_vendor_rev',
            'opt_vendor_sn',
        ]
        lane_l = ['node', 'interface', 'lane']

        ext_label_metric = GaugeMetricFamily(
            "node_nic_stats_extra_labels",
            "extra labels",
            labels=ext_l,
        )

        metrics = {
            k: GaugeMetricFamily("node_nic_stats_" + k, v, labels=l)
            for k, v in cls.metrics_to_descr.items()
        }

        lane_metrics = {
            k: GaugeMetricFamily("node_nic_stats_" + k, v, labels=lane_l)
            for k, v in cls.lane_metrics_to_descr.items()
        }

        return ext_label_metric, metrics, lane_metrics

    @classmethod
    def prom_collect(cls, node_name):
        if not node_name:
            logging.error("Refusing to create metrics without a node_name")
            return

        ext_label_metric, metrics, lane_metrics = NodeInfo.create_metrics()

        include_1g = False  # eventually make this configurable

        for ifname in find_network_interfaces():
            data = collect_interface_data("localhost", ifname)

            if (
                data["supports_100g"]
                or data["ethtool_error"]
                or (include_1g and data["supports_1g"])
            ):
                if data["supports_400g"]:
                    maxspeed = "400000"
                elif data["supports_200g"]:
                    maxspeed = "200000"
                elif data["supports_100g"]:
                    maxspeed = "100000"
                elif data["supports_10g"]:
                    maxspeed = "10000"
                elif data["supports_1g"]:
                    maxspeed = "1000"
                else:
                    maxspeed = "0"

                labels = [node_name, data["ifname"]]
                ext_labels = [
                    node_name,
                    data["ifname"],
                    data.get("mac", ""),
                    maxspeed,
                    data.get("opt_vendor_name", ""),
                    data.get("opt_vendor_oui", ""),
                    data.get("opt_vendor_pn", ""),
                    data.get("opt_vendor_rev", ""),
                    data.get("opt_vendor_sn", ""),
                ]
                ext_label_metric.add_metric(ext_labels, 1)

                supports_100g = data["supports_100g"] and not data["ethtool_error"]
                for key, m in metrics.items():
                    try:
                        val = data[key]
                        if val is not None:
                            m.add_metric(labels, val)
                    except KeyError:
                        if supports_100g:
                            print(f"Missing key {key}, may be ok if no value found on host")

                for key, m in lane_metrics.items():
                    try:
                        for lane, val in data[key].items():
                            if val is not None:
                                m.add_metric(labels + [lane], val)
                    except KeyError:
                        if supports_100g:
                            print(f"Missing key {key}, may be ok if no value found on host")

        yield ext_label_metric
        for v in metrics.values():
            yield v
        for v in lane_metrics.values():
            yield v
