"""
Node Survey module for linkmon - limited parser mock tests

Replicates linkmon ethtool exporting on specific nodes given
test inputs for mocking (command outputs and filesystem copy), and
compares it to expected test output (actual metrics produced on node).
"""

import glob
import re
from pathlib import Path
from unittest import mock

import nodeinfo
from prometheus_client.parser import text_string_to_metric_families

# used in the glob mock later
real_glob = glob.glob


@mock.patch('nodeinfo.glob.glob')
@mock.patch('nodeinfo.read_file')
@mock.patch('nodeinfo.run_cmd')
@mock.patch('nodeinfo.find_network_interfaces')
def test_nodeinfo(mock_find_interfaces, mock_run_cmd, mock_read_file, mock_glob):
    """
    Old test format which tests a sample JSON output array and compares the
    produced Prometheus metrics with the expected values.
    """

    test_data = {}
    with open("test-inputs/nics-swx008-sx-sr12", "r") as f:
        for line in f:
            if (line := line.strip()).startswith("{"):
                data = eval(line)
                if data.get("ifname", "").startswith("eth"):
                    test_data[data["ifname"]] = data

    eth_interfaces = [name for name in test_data.keys() if name.startswith("eth")]
    mock_find_interfaces.return_value = eth_interfaces

    def mock_glob_impl(pattern):
        pattern = pattern.split("/")
        pattern[-1] = "mlx5_0"
        return ["/".join(pattern)]

    mock_glob.side_effect = mock_glob_impl

    def mock_read_file_impl(path):
        path = path.split("/")

        ifname = None
        for name in eth_interfaces:
            if name in path:
                ifname = name
                break

        if ifname is None:
            return ""

        file_name = path[-1]
        if file_name == "address":
            return test_data[ifname]["addr"]
        elif file_name == "carrier":
            return test_data[ifname]["carrier"]
        elif file_name == "carrier_changes":
            return test_data[ifname]["carrier_changes"]
        elif file_name == "operstate":
            return test_data[ifname]["operstate"]
        elif file_name == "speed":
            return test_data[ifname]["speed"]
        elif file_name == "type":
            return test_data[ifname]["type"]
        else:  # mlx counters
            return test_data[ifname][file_name]

    mock_read_file.side_effect = mock_read_file_impl

    def mock_run_cmd_impl(cmd):
        if not cmd:
            return "", 0
        elif cmd[0] == "lspci":
            has_mlx = any(data.get("has_mlx", "0") != "0" for data in test_data.values())
            if has_mlx:
                return "Some PCI device info with Mellanox", 0
            else:
                return "", 0
        elif cmd[0] == "uptime":
            for data in test_data.values():
                if "boottime" in data:
                    return data["boottime"], 0
            return "", 0
        elif cmd[0] == "mstreg":
            # already passed ethtool -i check
            return f"temperature | {cmd[2]}", 0
        elif cmd[0] == "ethtool" and len(cmd) == 2:
            ifname = cmd[1]
            eth_100g = test_data.get(ifname, {}).get("eth_100g", "0")
            eth_10g = test_data.get(ifname, {}).get("eth_10g", "0")
            eth_1g = test_data.get(ifname, {}).get("eth_1g", "0")

            response = f"Settings for {ifname}:\n"
            if eth_100g and eth_100g != "0":
                response += "\tSupported link modes: 100000baseKR4/Full\n"
            if eth_10g and eth_10g != "0":
                response += "\tSupported link modes: 10000baseKR/Full\n"
            if eth_1g and eth_1g != "0":
                response += "\tSupported link modes: 1000baseT/Full\n"

            return response, int(test_data[ifname]["ethtool_error"])
        elif cmd[0] == "ethtool" and cmd[1] == "-S":
            ifname = cmd[2]
            if ifname not in test_data:
                return "", 0

            data = test_data[ifname]
            key_mapping = {
                "rx_pfc": "rx_prio5_pause",
                "tx_pfc": "tx_prio5_pause",
                "rx_pfc_duration": "rx_prio5_pause_duration",
                "tx_pfc_duration": "tx_prio5_pause_duration",
                "rx_discards_prio5": "rx_prio5_discards",
            }
            response = ""
            for key, value in data.items():
                if key in [
                    "rx_crc_errors_phy",
                    "rx_bytes",
                    "rx_bytes_phy",
                    "tx_bytes",
                    "tx_bytes_phy",
                    "rx_pcs_symbol_err_phy",
                    "rx_corrected_bits_phy",
                    "rx_err_lane_0_phy",
                    "rx_err_lane_1_phy",
                    "rx_err_lane_2_phy",
                    "rx_err_lane_3_phy",
                    "tx_cqe_err",
                    "rx_wqe_err",
                    "rx_pfc",
                    "tx_pfc",
                    "rx_pfc_duration",
                    "tx_pfc_duration",
                    "tx_discards_phy",
                    "rx_discards_phy",
                    "rx_discards_prio5",
                    "module_bad_shorted",
                    "module_high_temp",
                    "tx_pause_storm_error_events",
                ]:
                    key = key_mapping.get(key, key)
                    response += f"{key}:{value}\n"

            return response, 0
        elif cmd[0] == "ethtool" and cmd[1] == "-m":
            ifname = cmd[2]
            if ifname not in test_data:
                return "", 0

            data = test_data[ifname]
            response = ""

            if data.get("opt_vendor_name"):
                response += f"Vendor name    :{data['opt_vendor_name']}\n"
            if data.get("opt_vendor_oui"):
                response += f"Vendor OUI     :{data['opt_vendor_oui']}\n"
            if data.get("opt_vendor_pn"):
                response += f"Vendor PN      :{data['opt_vendor_pn']}\n"
            if data.get("opt_vendor_rev"):
                response += f"Vendor rev     :{data['opt_vendor_rev']}\n"
            if data.get("opt_vendor_sn"):
                response += f"Vendor SN      :{data['opt_vendor_sn']}\n"

            if data.get("opt_temperature"):
                response += f"Module temperature : {data['opt_temperature']}\n"
            if data.get("opt_temperature_alarm"):
                response += f"Module temperature high alarm : {data['opt_temperature_alarm']}\n"

            for i in range(1, 5):
                if data.get(f"tx_curr_{i}"):
                    response += f"tx bias current (Channel {i}) : {data[f'tx_curr_{i}']}\n"
                if data.get(f"tx_pwr_{i}"):
                    response += (
                        f"Transmit avg optical power (Channel {i}) : {data[f'tx_pwr_{i}']}\n"
                    )
                if data.get(f"rx_pwr_{i}"):
                    response += (
                        f"Rcvr signal avg optical power (Channel {i}) : {data[f'rx_pwr_{i}']}\n"
                    )

            return response, 0
        elif cmd[0] == "ethtool" and cmd[1] == "-i":
            ifname = cmd[2]
            if ifname in test_data and test_data[ifname].get("temperature"):
                # only way to associate temperature for a certain ifname in mstreg cmd
                return f"bus-info: {test_data[ifname]['temperature']}", 0
            return "", 0
        else:
            return "", 0

    mock_run_cmd.side_effect = mock_run_cmd_impl

    expected_values = []
    expected_values.append(
        {
            "labels": ['localhost', 'eth100g0'],
            "extended_labels": [
                'localhost',
                'eth100g0',
                '88:e9:a4:ad:ac:24',
                '100000',
                'TE Connectivity',
                '00:22:a7',
                '2428274-3',
                'A',
                '5BQ6AFC2327115',
            ],
            "node_nic_stats_speed": 100000,
            "node_nic_stats_carrier": 1,
            "node_nic_stats_carrier_changes": 17,
            "node_nic_stats_operstate": 1,
            "node_nic_stats_ethtool_error": 0,
            "node_nic_stats_rx_crc_errors_phy": 0,
            "node_nic_stats_rx_bytes": 11436237624595320,
            "node_nic_stats_rx_bytes_phy": 14436237624595320,
            "node_nic_stats_tx_bytes": 1143623762459532,
            "node_nic_stats_tx_bytes_phy": 1443623762459532,
            "node_nic_stats_rx_pcs_symbol_err_phy": 0,
            "node_nic_stats_rx_corrected_bits_phy": 103113,
            "node_nic_stats_rx_err_lane_phy": {
                "0": 103113,
                "1": 0,
                "2": 0,
                "3": 0,
            },
            "node_nic_stats_packet_seq_err": 29,
            "node_nic_stats_out_of_sequence": 1,
            "node_nic_stats_rnr_nak_retry_err": 0,
            "node_nic_stats_out_of_buffer": 4026613513,
            "node_nic_stats_req_cqe_error": 0,
            "node_nic_stats_resp_cqe_error": 0,
            "node_nic_stats_tx_cqe_err": 0,
            "node_nic_stats_rx_wqe_err": 0,
            "node_nic_stats_np_cnp_sent": 6,
            "node_nic_stats_np_ecn_marked_roce_packets": 0,
            "node_nic_stats_rp_cnp_handled": 439,
            "node_nic_stats_rp_cnp_ignored": 0,
            "node_nic_stats_roce_slow_restart_cnps": 236,
            "node_nic_stats_roce_adp_retrans": 1,
            "node_nic_stats_roce_adp_retrans_to": 5,
            "node_nic_stats_opt_temperature": 58.16,
            "node_nic_stats_opt_temperature_alarm": 0,
            "node_nic_stats_temperature": 71,
            "node_nic_stats_tx_curr": {"1": 7.2, "2": 7.2, "3": 7.2, "4": 7.2},
            "node_nic_stats_tx_pwr": {
                "1": 0.9013,
                "2": 0.9001,
                "3": 0.8995,
                "4": 0.9163,
            },
            "node_nic_stats_rx_pwr": {
                "1": 0.8756,
                "2": 0.9180,
                "3": 0.9823,
                "4": 0.8372,
            },
            "node_nic_stats_tx_pfc": 74573290,
            "node_nic_stats_rx_pfc": 1061944,
            "node_nic_stats_tx_pfc_duration": 4121867919,
            "node_nic_stats_rx_pfc_duration": 7322237,
            "node_nic_stats_tx_discards_phy": 0,
            "node_nic_stats_rx_discards_phy": 0,
            "node_nic_stats_rx_discards_prio5": 0,
            "node_nic_stats_local_ack_timeout_err": 0,
            "node_nic_stats_implied_nak_seq_err": 0,
            "node_nic_stats_module_bad_shorted": 0,
            "node_nic_stats_module_high_temp": 0,
            "node_nic_stats_tx_pause_storm_error_events": 0,
        }
    )
    expected_values.append(
        {
            "labels": ['localhost', 'eth100g1'],
            "extended_labels": [
                'localhost',
                'eth100g1',
                '88:e9:a4:ad:ac:25',
                '100000',
                'TE Connectivity',
                '00:22:a7',
                '2428274-3',
                'A',
                '5BQ6AFC2327064',
            ],
            "node_nic_stats_speed": 100000,
            "node_nic_stats_carrier": 1,
            "node_nic_stats_carrier_changes": 17,
            "node_nic_stats_operstate": 1,
            "node_nic_stats_ethtool_error": 0,
            "node_nic_stats_rx_crc_errors_phy": 0,
            "node_nic_stats_rx_bytes": 9082908750741402,
            "node_nic_stats_rx_bytes_phy": 9182908750741402,
            "node_nic_stats_tx_bytes": 908290875074140,
            "node_nic_stats_tx_bytes_phy": 918290875074140,
            "node_nic_stats_rx_pcs_symbol_err_phy": 0,
            "node_nic_stats_rx_corrected_bits_phy": 0,
            "node_nic_stats_rx_err_lane_phy": {"0": 0, "1": 0, "2": 0, "3": 0},
            "node_nic_stats_packet_seq_err": 5,
            "node_nic_stats_out_of_sequence": 0,
            "node_nic_stats_rnr_nak_retry_err": 1601634702,
            "node_nic_stats_out_of_buffer": 0,
            "node_nic_stats_req_cqe_error": 544,
            "node_nic_stats_resp_cqe_error": 0,
            "node_nic_stats_tx_cqe_err": 0,
            "node_nic_stats_rx_wqe_err": 0,
            "node_nic_stats_np_cnp_sent": 0,
            "node_nic_stats_np_ecn_marked_roce_packets": 2,
            "node_nic_stats_rp_cnp_handled": 1497,
            "node_nic_stats_rp_cnp_ignored": 0,
            "node_nic_stats_roce_slow_restart_cnps": 902,
            "node_nic_stats_roce_adp_retrans": 0,
            "node_nic_stats_roce_adp_retrans_to": 0,
            "node_nic_stats_opt_temperature": 96,
            "node_nic_stats_opt_temperature_alarm": 1,
            "node_nic_stats_temperature": 71,
            "node_nic_stats_tx_curr": {"1": 7.1, "2": 7.1, "3": 7.1, "4": 7.1},
            "node_nic_stats_tx_pwr": {
                "1": 0.8757,
                "2": 0.8909,
                "3": 0.8832,
                "4": 0.8755,
            },
            "node_nic_stats_rx_pwr": {
                "1": 0.8804,
                "2": 0.9235,
                "3": 0.9380,
                "4": 0.8811,
            },
            "node_nic_stats_tx_pfc": 117463721,
            "node_nic_stats_rx_pfc": 448658820,
            "node_nic_stats_tx_pfc_duration": 5197024241,
            "node_nic_stats_rx_pfc_duration": 1848133607,
            "node_nic_stats_tx_discards_phy": 0,
            "node_nic_stats_rx_discards_phy": 0,
            "node_nic_stats_rx_discards_prio5": 0,
            "node_nic_stats_local_ack_timeout_err": 42,
            "node_nic_stats_implied_nak_seq_err": 0,
            "node_nic_stats_module_bad_shorted": 0,
            "node_nic_stats_module_high_temp": 0,
            "node_nic_stats_tx_pause_storm_error_events": 0,
        }
    )
    expected_values.append(
        {
            "labels": ['localhost', 'eth100g2'],
            "extended_labels": [
                'localhost',
                'eth100g2',
                '88:e9:a4:b6:b6:7c',
                '100000',
                'TE Connectivity',
                '00:22:a7',
                '2428274-3',
                'A',
                '5BQ6AFC2327110',
            ],
            "node_nic_stats_speed": 100000,
            "node_nic_stats_carrier": 1,
            "node_nic_stats_carrier_changes": 17,
            "node_nic_stats_operstate": 1,
            "node_nic_stats_ethtool_error": 0,
            "node_nic_stats_rx_crc_errors_phy": 0,
            "node_nic_stats_rx_bytes": 9082134948480176,
            "node_nic_stats_rx_bytes_phy": 9182134948480176,
            "node_nic_stats_tx_bytes": 908213494848017,
            "node_nic_stats_tx_bytes_phy": 918213494848017,
            "node_nic_stats_rx_pcs_symbol_err_phy": 0,
            "node_nic_stats_rx_corrected_bits_phy": 208267,
            "node_nic_stats_rx_err_lane_phy": {
                "0": 1435,
                "1": 206530,
                "2": 0,
                "3": 302,
            },
            "node_nic_stats_packet_seq_err": 8,
            "node_nic_stats_out_of_sequence": 0,
            "node_nic_stats_rnr_nak_retry_err": 798646731,
            "node_nic_stats_out_of_buffer": 0,
            "node_nic_stats_req_cqe_error": 1015,
            "node_nic_stats_resp_cqe_error": 0,
            "node_nic_stats_tx_cqe_err": 0,
            "node_nic_stats_rx_wqe_err": 0,
            "node_nic_stats_np_cnp_sent": 0,
            "node_nic_stats_np_ecn_marked_roce_packets": 0,
            "node_nic_stats_rp_cnp_handled": 1132,
            "node_nic_stats_rp_cnp_ignored": 5,
            "node_nic_stats_roce_slow_restart_cnps": 567,
            "node_nic_stats_roce_adp_retrans": 2,
            "node_nic_stats_roce_adp_retrans_to": 1,
            "node_nic_stats_opt_temperature": 58.23,
            "node_nic_stats_opt_temperature_alarm": 0,
            "node_nic_stats_temperature": 69,
            "node_nic_stats_tx_curr": {"1": 7.2, "2": 7.2, "3": 7.2, "4": 7.2},
            "node_nic_stats_tx_pwr": {
                "1": 0.8752,
                "2": 0.8508,
                "3": 0.8636,
                "4": 0.8722,
            },
            "node_nic_stats_rx_pwr": {
                "1": 0.9196,
                "2": 0.9452,
                "3": 0.9564,
                "4": 0.9033,
            },
            "node_nic_stats_tx_pfc": 130326389,
            "node_nic_stats_rx_pfc": 207069489,
            "node_nic_stats_tx_pfc_duration": 6025077432,
            "node_nic_stats_rx_pfc_duration": 321082326,
            "node_nic_stats_tx_discards_phy": 0,
            "node_nic_stats_rx_discards_phy": 0,
            "node_nic_stats_rx_discards_prio5": 0,
            "node_nic_stats_local_ack_timeout_err": 84,
            "node_nic_stats_implied_nak_seq_err": 0,
            "node_nic_stats_module_bad_shorted": 1,
            "node_nic_stats_module_high_temp": 0,
            "node_nic_stats_tx_pause_storm_error_events": 0,
        }
    )
    expected_values.append(
        {
            "labels": ['localhost', 'eth100g3'],
            "extended_labels": [
                'localhost',
                'eth100g3',
                '88:e9:a4:b6:b6:7d',
                '100000',
                'TE Connectivity',
                '00:22:a7',
                '2428274-3',
                'A',
                '5BQ6AFC2327116',
            ],
            "node_nic_stats_speed": 100000,
            "node_nic_stats_carrier": 1,
            "node_nic_stats_carrier_changes": 17,
            "node_nic_stats_operstate": 1,
            "node_nic_stats_ethtool_error": 0,
            "node_nic_stats_rx_crc_errors_phy": 0,
            "node_nic_stats_rx_bytes": 9056519837324186,
            "node_nic_stats_rx_bytes_phy": 9156519837324186,
            "node_nic_stats_tx_bytes": 905651983732418,
            "node_nic_stats_tx_bytes_phy": 915651983732418,
            "node_nic_stats_rx_pcs_symbol_err_phy": 0,
            "node_nic_stats_rx_corrected_bits_phy": 2,
            "node_nic_stats_rx_err_lane_phy": {"0": 2, "1": 0, "2": 0, "3": 0},
            "node_nic_stats_packet_seq_err": 6,
            "node_nic_stats_out_of_sequence": 0,
            "node_nic_stats_rnr_nak_retry_err": 806458834,
            "node_nic_stats_out_of_buffer": 45737,
            "node_nic_stats_req_cqe_error": 423,
            "node_nic_stats_resp_cqe_error": 0,
            "node_nic_stats_tx_cqe_err": 0,
            "node_nic_stats_rx_wqe_err": 0,
            "node_nic_stats_np_cnp_sent": 0,
            "node_nic_stats_np_ecn_marked_roce_packets": 0,
            "node_nic_stats_rp_cnp_handled": 1099,
            "node_nic_stats_rp_cnp_ignored": 0,
            "node_nic_stats_roce_slow_restart_cnps": 474,
            "node_nic_stats_roce_adp_retrans": 0,
            "node_nic_stats_roce_adp_retrans_to": 0,
            "node_nic_stats_opt_temperature": 54.92,
            "node_nic_stats_opt_temperature_alarm": 0,
            "node_nic_stats_temperature": 69,
            "node_nic_stats_tx_curr": {"1": 7, "2": 7, "3": 7, "4": 7},
            "node_nic_stats_tx_pwr": {
                "1": 0.8625,
                "2": 0.8757,
                "3": 0.9299,
                "4": 0.9035,
            },
            "node_nic_stats_rx_pwr": {
                "1": 0.8185,
                "2": 0.8826,
                "3": 0.8680,
                "4": 0.8019,
            },
            "node_nic_stats_tx_pfc": 130332208,
            "node_nic_stats_rx_pfc": 190637837,
            "node_nic_stats_tx_pfc_duration": 6025279132,
            "node_nic_stats_rx_pfc_duration": 297845538,
            "node_nic_stats_tx_discards_phy": 0,
            "node_nic_stats_rx_discards_phy": 11,
            "node_nic_stats_rx_discards_prio5": 22,
            "node_nic_stats_local_ack_timeout_err": 42,
            "node_nic_stats_implied_nak_seq_err": 0,
            "node_nic_stats_module_bad_shorted": 0,
            "node_nic_stats_module_high_temp": 2,
            "node_nic_stats_tx_pause_storm_error_events": 0,
        }
    )
    expected_values.append(
        {
            "labels": ['localhost', 'eth100g4'],
            "extended_labels": [
                'localhost',
                'eth100g4',
                '88:e9:a4:cb:8b:6e',
                '100000',
                'TE Connectivity',
                '00:22:a7',
                '2428274-3',
                'A',
                '5BQ6AFC2327060',
            ],
            "node_nic_stats_speed": 100000,
            "node_nic_stats_carrier": 1,
            "node_nic_stats_carrier_changes": 17,
            "node_nic_stats_operstate": 1,
            "node_nic_stats_ethtool_error": 0,
            "node_nic_stats_rx_crc_errors_phy": 0,
            "node_nic_stats_rx_bytes": 9055280913292036,
            "node_nic_stats_rx_bytes_phy": 9155280913292036,
            "node_nic_stats_tx_bytes": 905528091329203,
            "node_nic_stats_tx_bytes_phy": 915528091329203,
            "node_nic_stats_rx_pcs_symbol_err_phy": 0,
            "node_nic_stats_rx_corrected_bits_phy": 0,
            "node_nic_stats_rx_err_lane_phy": {"0": 0, "1": 0, "2": 0, "3": 0},
            "node_nic_stats_packet_seq_err": 0,
            "node_nic_stats_out_of_sequence": 0,
            "node_nic_stats_rnr_nak_retry_err": 1511386728,
            "node_nic_stats_out_of_buffer": 0,
            "node_nic_stats_req_cqe_error": 1283,
            "node_nic_stats_resp_cqe_error": 0,
            "node_nic_stats_tx_cqe_err": 0,
            "node_nic_stats_rx_wqe_err": 0,
            "node_nic_stats_np_cnp_sent": 0,
            "node_nic_stats_np_ecn_marked_roce_packets": 0,
            "node_nic_stats_rp_cnp_handled": 534,
            "node_nic_stats_rp_cnp_ignored": 0,
            "node_nic_stats_roce_slow_restart_cnps": 534,
            "node_nic_stats_roce_adp_retrans": 0,
            "node_nic_stats_roce_adp_retrans_to": 0,
            "node_nic_stats_opt_temperature": 62.09,
            "node_nic_stats_opt_temperature_alarm": 0,
            "node_nic_stats_temperature": 79,
            "node_nic_stats_tx_curr": {"1": 7.3, "2": 7.3, "3": 7.3, "4": 7.3},
            "node_nic_stats_tx_pwr": {
                "1": 0.8402,
                "2": 0.8326,
                "3": 0.8395,
                "4": 0.8651,
            },
            "node_nic_stats_rx_pwr": {
                "1": 0.8602,
                "2": 0.9094,
                "3": 0.8781,
                "4": 0.8365,
            },
            "node_nic_stats_tx_pfc": 111493032,
            "node_nic_stats_rx_pfc": 173824079,
            "node_nic_stats_tx_pfc_duration": 4720626538,
            "node_nic_stats_rx_pfc_duration": 273301098,
            "node_nic_stats_tx_discards_phy": 0,
            "node_nic_stats_rx_discards_phy": 0,
            "node_nic_stats_rx_discards_prio5": 0,
            "node_nic_stats_local_ack_timeout_err": 90,
            "node_nic_stats_implied_nak_seq_err": 0,
            "node_nic_stats_module_bad_shorted": 0,
            "node_nic_stats_module_high_temp": 0,
            "node_nic_stats_tx_pause_storm_error_events": 3,
        }
    )
    expected_values.append(
        {
            "labels": ['localhost', 'eth100g5'],
            "extended_labels": [
                'localhost',
                'eth100g5',
                '88:e9:a4:cb:8b:6f',
                '100000',
                'TE Connectivity',
                '00:22:a7',
                '2428274-3',
                'A',
                '5BQ6AFC2327059',
            ],
            "node_nic_stats_speed": 100000,
            "node_nic_stats_carrier": 1,
            "node_nic_stats_carrier_changes": 17,
            "node_nic_stats_operstate": 1,
            "node_nic_stats_ethtool_error": 0,
            "node_nic_stats_rx_crc_errors_phy": 0,
            "node_nic_stats_rx_bytes": 2673086618436,
            "node_nic_stats_rx_bytes_phy": 2773086618436,
            "node_nic_stats_tx_bytes": 267308661843,
            "node_nic_stats_tx_bytes_phy": 277308661843,
            "node_nic_stats_rx_pcs_symbol_err_phy": 0,
            "node_nic_stats_rx_corrected_bits_phy": 0,
            "node_nic_stats_rx_err_lane_phy": {"0": 0, "1": 0, "2": 0, "3": 0},
            "node_nic_stats_packet_seq_err": 0,
            "node_nic_stats_out_of_sequence": 0,
            "node_nic_stats_rnr_nak_retry_err": 55569,
            "node_nic_stats_out_of_buffer": 0,
            "node_nic_stats_req_cqe_error": 0,
            "node_nic_stats_resp_cqe_error": 0,
            "node_nic_stats_tx_cqe_err": 0,
            "node_nic_stats_rx_wqe_err": 0,
            "node_nic_stats_np_cnp_sent": 0,
            "node_nic_stats_np_ecn_marked_roce_packets": 0,
            "node_nic_stats_rp_cnp_handled": 0,
            "node_nic_stats_rp_cnp_ignored": 0,
            "node_nic_stats_roce_slow_restart_cnps": 0,
            "node_nic_stats_roce_adp_retrans": 0,
            "node_nic_stats_roce_adp_retrans_to": 0,
            "node_nic_stats_opt_temperature": 62.02,
            "node_nic_stats_opt_temperature_alarm": 0,
            "node_nic_stats_temperature": 78,
            "node_nic_stats_tx_curr": {"1": 7.3, "2": 7.3, "3": 7.3, "4": 7.3},
            "node_nic_stats_tx_pwr": {
                "1": 0.8164,
                "2": 0.8159,
                "3": 0.8276,
                "4": 0.8351,
            },
            "node_nic_stats_rx_pwr": {
                "1": 0.8746,
                "2": 0.9691,
                "3": 0.9513,
                "4": 0.8616,
            },
            "node_nic_stats_tx_pfc": 31410,
            "node_nic_stats_rx_pfc": 0,
            "node_nic_stats_tx_pfc_duration": 528061,
            "node_nic_stats_rx_pfc_duration": 0,
            "node_nic_stats_tx_discards_phy": 0,
            "node_nic_stats_rx_discards_phy": 0,
            "node_nic_stats_rx_discards_prio5": 0,
            "node_nic_stats_local_ack_timeout_err": 0,
            "node_nic_stats_implied_nak_seq_err": 0,
            "node_nic_stats_module_bad_shorted": 0,
            "node_nic_stats_module_high_temp": 0,
            "node_nic_stats_tx_pause_storm_error_events": 0,
        }
    )

    # Use this to trim the Prom metric prefix from the name
    suf_re = re.compile(r'node_nic_stats_(.*)')

    # Create empty MetricFamilies to fill in the expected stats
    exlbl, metrics, lane_metrics = nodeinfo.NodeInfo.create_metrics()
    for nic in expected_values:
        labels = nic["labels"]
        ext_labels = nic["extended_labels"]

        exlbl.add_metric(ext_labels, 1)
        for k, v in nic.items():
            if k.startswith("node_nic_stats_"):
                if k == "node_nic_stats_extra_labels":
                    continue

                suffix = suf_re.match(k)
                if isinstance(v, dict):
                    for lane, lane_value in v.items():
                        lane_metrics[suffix.group(1)].add_metric(labels + [lane], lane_value)
                else:
                    metrics[suffix.group(1)].add_metric(labels, v)

    exp_tuple = (exlbl,) + tuple(metrics.values()) + tuple(lane_metrics.values())
    test_tuple = nodeinfo.NodeInfo.prom_collect("localhost")
    for idx, _ in enumerate(exp_tuple):
        assert exp_tuple[idx] == next(test_tuple)


def run(
    node_name,
    mock_run_cmd,
    mock_read_file,
    mock_glob,
    mock_tempfile_ntf=None,
):
    """Run the nodeinfo collection test for a given node."""

    test_root = Path(__file__).parent
    input_dir = Path(f"test-inputs/ethtool-exporter/{node_name}")
    output_dir = Path(f"test-outputs/ethtool-exporter/{node_name}")

    expected_results = None
    with open(test_root / output_dir) as f:
        expected_results = list(text_string_to_metric_families(f.read()))

    def mock_glob_impl(pattern):
        pattern_path = Path(pattern)
        if pattern_path.is_absolute():
            pattern_path = pattern_path.relative_to('/')
        new_path = test_root / input_dir / "fake_fs" / pattern_path
        return real_glob(str(new_path))

    mock_glob.side_effect = mock_glob_impl

    def mock_read_file_impl(path):
        path = Path(path)
        if not path.is_relative_to(test_root):
            if path.is_absolute():
                path = path.relative_to('/')
            new_path = test_root / input_dir / "fake_fs" / path
        else:
            new_path = path

        with open(new_path) as f:
            return f.read().strip()

    mock_read_file.side_effect = mock_read_file_impl

    def mock_run_cmd_impl(cmd):
        cmd_to_filename = "_".join(cmd)
        fpath = test_root / input_dir / "command_outputs" / cmd_to_filename
        with open(fpath) as f:
            return f.read(), 0

    mock_run_cmd.side_effect = mock_run_cmd_impl

    if mock_tempfile_ntf:
        mock_tempfile_ntf.return_value.__enter__.return_value.name = "tmpfile.json"

    test_tuple = nodeinfo.NodeInfo.prom_collect(node_name)
    test_results = test_tuple

    # trim off the python library metrics
    expected_results = [x for x in expected_results if x.name.startswith("node_nic_stats_")]

    for idx, _ in enumerate(expected_results):
        assert expected_results[idx] == next(test_results)


@mock.patch('nodeinfo.glob.glob')
@mock.patch('nodeinfo.read_file')
@mock.patch('nodeinfo.run_cmd')
def test_nodeinfo_perfdrop(mock_run_cmd, mock_read_file, mock_glob):
    run(
        "sc-r10ra19-s14",
        mock_run_cmd,
        mock_read_file,
        mock_glob
    )


@mock.patch('nodeinfo.tempfile.NamedTemporaryFile')
@mock.patch('nodeinfo.glob.glob')
@mock.patch('nodeinfo.read_file')
@mock.patch('nodeinfo.run_cmd')
def test_nodeinfo_thor2(mock_run_cmd, mock_read_file, mock_glob, mock_tempfile_ntf):
    run(
        "sc-r7rb5-s10",
        mock_run_cmd,
        mock_read_file,
        mock_glob,
        mock_tempfile_ntf
    )
