"""
Switch Survey module for linkmon - parser tests
Invokes the switch parser function directly on the saved sample output files.
Compares the produced Prometheus metrics with the expected values.
"""
from test_linkmon_util import compare_results

import switch

# Test the ComWare switch output
def test_comware_parse():
    expected_values = []

    l = [
        'comware-switch-gsw256',
        'HundredGigE2/0/1',
        'HundredGigE2/0/1 Interface',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                '54:77:8a:3b:06:01',
                '100G_AOC_QSFP28',
                'TE Connectivity',
                '2428274-3',
                '',
                '5BQ6AFC2326064',
                '',
                '',
                '',
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 34,
            "linkmon_switch_opt_voltage": 3.29,
            "linkmon_switch_opt_tx_current": {"1": 6.50,
                                              "2": 6.50,
                                              "3": 6.50,
                                              "4": 6.50},
            "linkmon_switch_opt_tx_power": {"1": -0.42,
                                            "2": -0.65,
                                            "3": -0.07,
                                            "4": -0.42,},
            "linkmon_switch_opt_rx_power": {"1": -1.22,
                                                  "2": -0.84,
                                                  "3": -1.09,
                                                  "4": -1.16,},
            "linkmon_switch_rx_pfc": 977397189,
            "linkmon_switch_tx_pfc": 181443,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )
    l = [
        'comware-switch-gsw256',
        'HundredGigE2/0/2',
        'L2 to swx001-sx-sr01/eth100g1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                '30:3f:bb:04:9c:00',
                '100G_AOC_QSFP28',
                'TE Connectivity',
                '2428274-3',
                '',
                '5BQ6AFC2326070',
                '',
                '',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 17510265452274433,
            "linkmon_switch_input_errors": 2,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 34,
            "linkmon_switch_opt_voltage": 3.27,
            "linkmon_switch_opt_tx_current": { "1": 6.50,
                                               "2": 6.50,
                                               "3": 6.50,
                                               "4": 6.50,},
            "linkmon_switch_opt_tx_power": { "1": -0.19,
                                             "2": -0.11,
                                             "3": -0.16,
                                             "4": 0.02,},
            "linkmon_switch_opt_rx_power": { "1": -1.01,
                                             "2": -0.73,
                                             "3": -0.69,
                                             "4": -0.76,},
            "linkmon_switch_rx_pfc": 34827218,
            "linkmon_switch_tx_pfc": 92576984,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 4704973,
            "linkmon_switch_rx_kbps": 17089908,
            "linkmon_switch_extra_labels": 1,
        }
    )
    l = [
        'comware-switch-gsw256',
        'HundredGigE2/0/3',
        'HundredGigE2/0/3 Interface',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                '54:77:8a:3b:06:01',
                '100G_AOC_QSFP28',
                'TE Connectivity',
                '2428274-3',
                '',
                '5BQ6AFC2326177',
                '',
                '',
                '',
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 34,
            "linkmon_switch_opt_voltage": 3.27,
            "linkmon_switch_opt_tx_current": { "1": 6.50,
                                               "2": 6.50,
                                               "3": 6.50,
                                               "4": 6.50,},
            "linkmon_switch_opt_tx_power": { "1": -0.44,
                                             "2": -0.70,
                                             "3": -0.51,
                                             "4": -0.38,},
            "linkmon_switch_opt_rx_power": { "1": -0.95,
                                             "2": -0.52,
                                             "3": -0.46,
                                             "4": -0.86,},
            "linkmon_switch_rx_pfc": 10869595,
            "linkmon_switch_tx_pfc": 96557255,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 66,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )
    l = [
        'comware-switch-gsw256',
        'HundredGigE3/0/1',
        'HundredGigE3/0/1 Interface',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                '54:77:8a:3b:06:00',
                '100G_AOC_QSFP28',
                'TE Connectivity',
                '2428274-3',
                '',
                '5BQ6AFC2326174',
                '',
                '',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 25,
            "linkmon_switch_rx_bytes": 6164555687605,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 7,
            "linkmon_switch_fec_corrected_codewords": 1341819,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 34,
            "linkmon_switch_opt_voltage": 3.31,
            "linkmon_switch_opt_tx_current": { "1": 6.50,
            "2": 6.50,
            "3": 6.50,
                                               "4": 6.50,},
            "linkmon_switch_opt_tx_power": { "1": -0.21,
            "2": -0.18,
            "3": -0.28,
                                             "4": -0.31,},
            "linkmon_switch_opt_rx_power": { "1": -0.93,
            "2": -0.80,
            "3": -0.56,
                                             "4": -0.84,},
            "linkmon_switch_rx_pfc": 59703424,
            "linkmon_switch_tx_pfc": 4064690,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )
    l = [
        'comware-switch-gsw256',
        'HundredGigE3/0/2',
        'HundredGigE3/0/2 Interface',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                '54:77:8a:3b:06:01',
                '100G_AOC_QSFP28',
                'TE Connectivity',
                '2428274-3',
                '',
                '5BQ6AFC2326105',
                '',
                '',
                '',
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 34,
            "linkmon_switch_opt_voltage": 3.30,
            "linkmon_switch_opt_tx_current": { "1": 6.50,
            "2": 6.50,
            "3": 6.50,
            "4": 6.50,
            },"linkmon_switch_opt_tx_power": { "1": -0.16,
            "2": -0.09,
            "3": -0.20,
            "4": -0.16,
            },"linkmon_switch_opt_rx_power": { "1": -0.88,
            "2": -0.64,
            "3": -0.67,
            "4": -0.96,
            },"linkmon_switch_rx_pfc": 95873868,
            "linkmon_switch_tx_pfc": 140111851,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )
    l = [
        'comware-switch-gsw256',
        'HundredGigE3/0/3',
        'HundredGigE3/0/3 Interface',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                '54:77:8a:3b:06:00',
                '100G_AOC_QSFP28',
                'TE Connectivity',
                '2428274-3',
                '',
                '5BQ6AFC2326054',
                '',
                '',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 15,
            "linkmon_switch_rx_bytes": 5975712906161,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 4,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 33,
            "linkmon_switch_opt_voltage": 3.29,
            "linkmon_switch_opt_tx_current": { "1": 6.50,
            "2": 6.50,
            "3": 6.50,
            "4": 6.50,
            },"linkmon_switch_opt_tx_power": { "1": -0.30,
            "2": -0.38,
            "3": -0.43,
            "4": -0.20,
            },"linkmon_switch_opt_rx_power": { "1": -0.98,
            "2": -0.64,
            "3": -0.65,
            "4": -0.73,
            },"linkmon_switch_rx_pfc": 105306409,
            "linkmon_switch_tx_pfc": 183320309,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 117,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    # Pass these fake args so that no SSH connections are made
    # - the other parse functions work the same way
    switch.SwitchSurvey.MAX_COMWARE_PORT = 3
    s = switch.SwitchSurvey([], {}, {}, False, False, "", "")
    s.switches = ["comware-switch-gsw256"]
    # mgmt_ips is unpopulated to check that empty string is exported for mgmt ip.
    s.switch_mgmt_ips = dict()
    s.stats["comware-switch-gsw256"] = switch.SwitchInfo("comware-switch-gsw256")

    with open("test-inputs/comware", "r") as f:
        s.parse("comware-switch-gsw256", f.readlines())

    expected_switch_values = [
        {"labels": ["comware-switch-gsw256", "HPE", "FF 12908E",
                    "9.1.058"], "linkmon_switch_version_info": 1},
        {"labels": ["comware-switch-gsw256", "slot 0 inflow 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 0 inflow 1"], "linkmon_switch_temperature": 31.0},
        {"labels": ["comware-switch-gsw256", "slot 0 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 0 hotspot 1"], "linkmon_switch_temperature": 42.0},
        {"labels": ["comware-switch-gsw256", "slot 0 hotspot 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 0 hotspot 2"], "linkmon_switch_temperature": 36.0},
        {"labels": ["comware-switch-gsw256", "slot 0 hotspot 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 0 hotspot 3"], "linkmon_switch_temperature": 29.0},
        {"labels": ["comware-switch-gsw256", "slot 0 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 0 cpu 1"], "linkmon_switch_temperature": 49.0},
        {"labels": ["comware-switch-gsw256", "slot 1 inflow 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 1 inflow 1"], "linkmon_switch_temperature": 30.0},
        {"labels": ["comware-switch-gsw256", "slot 1 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 1 hotspot 1"], "linkmon_switch_temperature": 40.0},
        {"labels": ["comware-switch-gsw256", "slot 1 hotspot 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 1 hotspot 2"], "linkmon_switch_temperature": 36.0},
        {"labels": ["comware-switch-gsw256", "slot 1 hotspot 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 1 hotspot 3"], "linkmon_switch_temperature": 30.0},
        {"labels": ["comware-switch-gsw256", "slot 1 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 1 cpu 1"], "linkmon_switch_temperature": 49.0},
        {"labels": ["comware-switch-gsw256", "slot 2 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 2 hotspot 1"], "linkmon_switch_temperature": 59.0},
        {"labels": ["comware-switch-gsw256", "slot 2 hotspot 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 2 hotspot 2"], "linkmon_switch_temperature": 42.0},
        {"labels": ["comware-switch-gsw256", "slot 2 hotspot 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 2 hotspot 3"], "linkmon_switch_temperature": 41.0},
        {"labels": ["comware-switch-gsw256", "slot 2 hotspot 4"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 2 hotspot 4"], "linkmon_switch_temperature": 61.0},
        {"labels": ["comware-switch-gsw256", "slot 2 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 2 chip 0"], "linkmon_switch_temperature": 76.0},
        {"labels": ["comware-switch-gsw256", "slot 2 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 2 chip 0 2"], "linkmon_switch_temperature": 75.0},
        {"labels": ["comware-switch-gsw256", "slot 2 chip 0 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 2 chip 0 3"], "linkmon_switch_temperature": 68.0},
        {"labels": ["comware-switch-gsw256", "slot 2 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 2 cpu 1"], "linkmon_switch_temperature": 50.0},
        {"labels": ["comware-switch-gsw256", "slot 3 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 3 hotspot 1"], "linkmon_switch_temperature": 57.0},
        {"labels": ["comware-switch-gsw256", "slot 3 hotspot 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 3 hotspot 2"], "linkmon_switch_temperature": 40.0},
        {"labels": ["comware-switch-gsw256", "slot 3 hotspot 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 3 hotspot 3"], "linkmon_switch_temperature": 39.0},
        {"labels": ["comware-switch-gsw256", "slot 3 hotspot 4"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 3 hotspot 4"], "linkmon_switch_temperature": 59.0},
        {"labels": ["comware-switch-gsw256", "slot 3 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 3 chip 0"], "linkmon_switch_temperature": 74.0},
        {"labels": ["comware-switch-gsw256", "slot 3 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 3 chip 0 2"], "linkmon_switch_temperature": 74.0},
        {"labels": ["comware-switch-gsw256", "slot 3 chip 0 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 3 chip 0 3"], "linkmon_switch_temperature": 68.0},
        {"labels": ["comware-switch-gsw256", "slot 3 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 3 cpu 1"], "linkmon_switch_temperature": 47.0},
        {"labels": ["comware-switch-gsw256", "slot 4 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 4 hotspot 1"], "linkmon_switch_temperature": 54.0},
        {"labels": ["comware-switch-gsw256", "slot 4 hotspot 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 4 hotspot 2"], "linkmon_switch_temperature": 39.0},
        {"labels": ["comware-switch-gsw256", "slot 4 hotspot 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 4 hotspot 3"], "linkmon_switch_temperature": 39.0},
        {"labels": ["comware-switch-gsw256", "slot 4 hotspot 4"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 4 hotspot 4"], "linkmon_switch_temperature": 56.0},
        {"labels": ["comware-switch-gsw256", "slot 4 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 4 chip 0"], "linkmon_switch_temperature": 70.0},
        {"labels": ["comware-switch-gsw256", "slot 4 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 4 chip 0 2"], "linkmon_switch_temperature": 69.0},
        {"labels": ["comware-switch-gsw256", "slot 4 chip 0 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 4 chip 0 3"], "linkmon_switch_temperature": 63.0},
        {"labels": ["comware-switch-gsw256", "slot 4 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 4 cpu 1"], "linkmon_switch_temperature": 45.0},
        {"labels": ["comware-switch-gsw256", "slot 5 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 5 hotspot 1"], "linkmon_switch_temperature": 55.0},
        {"labels": ["comware-switch-gsw256", "slot 5 hotspot 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 5 hotspot 2"], "linkmon_switch_temperature": 39.0},
        {"labels": ["comware-switch-gsw256", "slot 5 hotspot 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 5 hotspot 3"], "linkmon_switch_temperature": 38.0},
        {"labels": ["comware-switch-gsw256", "slot 5 hotspot 4"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 5 hotspot 4"], "linkmon_switch_temperature": 55.0},
        {"labels": ["comware-switch-gsw256", "slot 5 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 5 chip 0"], "linkmon_switch_temperature": 70.0},
        {"labels": ["comware-switch-gsw256", "slot 5 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 5 chip 0 2"], "linkmon_switch_temperature": 70.0},
        {"labels": ["comware-switch-gsw256", "slot 5 chip 0 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 5 chip 0 3"], "linkmon_switch_temperature": 63.0},
        {"labels": ["comware-switch-gsw256", "slot 5 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 5 cpu 1"], "linkmon_switch_temperature": 43.0},
        {"labels": ["comware-switch-gsw256", "slot 6 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 6 hotspot 1"], "linkmon_switch_temperature": 54.0},
        {"labels": ["comware-switch-gsw256", "slot 6 hotspot 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 6 hotspot 2"], "linkmon_switch_temperature": 40.0},
        {"labels": ["comware-switch-gsw256", "slot 6 hotspot 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 6 hotspot 3"], "linkmon_switch_temperature": 39.0},
        {"labels": ["comware-switch-gsw256", "slot 6 hotspot 4"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 6 hotspot 4"], "linkmon_switch_temperature": 57.0},
        {"labels": ["comware-switch-gsw256", "slot 6 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 6 chip 0"], "linkmon_switch_temperature": 70.0},
        {"labels": ["comware-switch-gsw256", "slot 6 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 6 chip 0 2"], "linkmon_switch_temperature": 69.0},
        {"labels": ["comware-switch-gsw256", "slot 6 chip 0 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 6 chip 0 3"], "linkmon_switch_temperature": 63.0},
        {"labels": ["comware-switch-gsw256", "slot 6 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 6 cpu 1"], "linkmon_switch_temperature": 46.0},
        {"labels": ["comware-switch-gsw256", "slot 7 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 7 hotspot 1"], "linkmon_switch_temperature": 49.0},
        {"labels": ["comware-switch-gsw256", "slot 7 hotspot 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 7 hotspot 2"], "linkmon_switch_temperature": 35.0},
        {"labels": ["comware-switch-gsw256", "slot 7 hotspot 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 7 hotspot 3"], "linkmon_switch_temperature": 34.0},
        {"labels": ["comware-switch-gsw256", "slot 7 hotspot 4"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 7 hotspot 4"], "linkmon_switch_temperature": 53.0},
        {"labels": ["comware-switch-gsw256", "slot 7 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 7 chip 0"], "linkmon_switch_temperature": 65.0},
        {"labels": ["comware-switch-gsw256", "slot 7 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 7 chip 0 2"], "linkmon_switch_temperature": 64.0},
        {"labels": ["comware-switch-gsw256", "slot 7 chip 0 3"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 7 chip 0 3"], "linkmon_switch_temperature": 58.0},
        {"labels": ["comware-switch-gsw256", "slot 7 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 7 cpu 1"], "linkmon_switch_temperature": 43.0},
        {"labels": ["comware-switch-gsw256", "slot 10 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 10 hotspot 1"], "linkmon_switch_temperature": 45.0},
        {"labels": ["comware-switch-gsw256", "slot 10 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 10 chip 0"], "linkmon_switch_temperature": 62.0},
        {"labels": ["comware-switch-gsw256", "slot 10 chip 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 10 chip 1"], "linkmon_switch_temperature": 60.0},
        {"labels": ["comware-switch-gsw256", "slot 10 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 10 chip 0 2"], "linkmon_switch_temperature": 59.0},
        {"labels": ["comware-switch-gsw256", "slot 10 chip 1 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 10 chip 1 2"], "linkmon_switch_temperature": 55.0},
        {"labels": ["comware-switch-gsw256", "slot 10 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 10 cpu 1"], "linkmon_switch_temperature": 42.0},
        {"labels": ["comware-switch-gsw256", "slot 11 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 11 hotspot 1"], "linkmon_switch_temperature": 48.0},
        {"labels": ["comware-switch-gsw256", "slot 11 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 11 chip 0"], "linkmon_switch_temperature": 66.0},
        {"labels": ["comware-switch-gsw256", "slot 11 chip 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 11 chip 1"], "linkmon_switch_temperature": 63.0},
        {"labels": ["comware-switch-gsw256", "slot 11 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 11 chip 0 2"], "linkmon_switch_temperature": 62.0},
        {"labels": ["comware-switch-gsw256", "slot 11 chip 1 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 11 chip 1 2"], "linkmon_switch_temperature": 57.0},
        {"labels": ["comware-switch-gsw256", "slot 11 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 11 cpu 1"], "linkmon_switch_temperature": 47.0},
        {"labels": ["comware-switch-gsw256", "slot 12 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 12 hotspot 1"], "linkmon_switch_temperature": 52.0},
        {"labels": ["comware-switch-gsw256", "slot 12 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 12 chip 0"], "linkmon_switch_temperature": 72.0},
        {"labels": ["comware-switch-gsw256", "slot 12 chip 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 12 chip 1"], "linkmon_switch_temperature": 67.0},
        {"labels": ["comware-switch-gsw256", "slot 12 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 12 chip 0 2"], "linkmon_switch_temperature": 68.0},
        {"labels": ["comware-switch-gsw256", "slot 12 chip 1 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 12 chip 1 2"], "linkmon_switch_temperature": 63.0},
        {"labels": ["comware-switch-gsw256", "slot 12 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 12 cpu 1"], "linkmon_switch_temperature": 54.0},
        {"labels": ["comware-switch-gsw256", "slot 13 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 13 hotspot 1"], "linkmon_switch_temperature": 48.0},
        {"labels": ["comware-switch-gsw256", "slot 13 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 13 chip 0"], "linkmon_switch_temperature": 69.0},
        {"labels": ["comware-switch-gsw256", "slot 13 chip 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 13 chip 1"], "linkmon_switch_temperature": 67.0},
        {"labels": ["comware-switch-gsw256", "slot 13 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 13 chip 0 2"], "linkmon_switch_temperature": 63.0},
        {"labels": ["comware-switch-gsw256", "slot 13 chip 1 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 13 chip 1 2"], "linkmon_switch_temperature": 62.0},
        {"labels": ["comware-switch-gsw256", "slot 13 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 13 cpu 1"], "linkmon_switch_temperature": 45.0},
        {"labels": ["comware-switch-gsw256", "slot 14 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 14 hotspot 1"], "linkmon_switch_temperature": 43.0},
        {"labels": ["comware-switch-gsw256", "slot 14 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 14 chip 0"], "linkmon_switch_temperature": 63.0},
        {"labels": ["comware-switch-gsw256", "slot 14 chip 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 14 chip 1"], "linkmon_switch_temperature": 62.0},
        {"labels": ["comware-switch-gsw256", "slot 14 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 14 chip 0 2"], "linkmon_switch_temperature": 59.0},
        {"labels": ["comware-switch-gsw256", "slot 14 chip 1 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 14 chip 1 2"], "linkmon_switch_temperature": 59.0},
        {"labels": ["comware-switch-gsw256", "slot 14 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 14 cpu 1"], "linkmon_switch_temperature": 42.0},
        {"labels": ["comware-switch-gsw256", "slot 15 hotspot 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 15 hotspot 1"], "linkmon_switch_temperature": 42.0},
        {"labels": ["comware-switch-gsw256", "slot 15 chip 0"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 15 chip 0"], "linkmon_switch_temperature": 60.0},
        {"labels": ["comware-switch-gsw256", "slot 15 chip 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 15 chip 1"], "linkmon_switch_temperature": 60.0},
        {"labels": ["comware-switch-gsw256", "slot 15 chip 0 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 15 chip 0 2"], "linkmon_switch_temperature": 54.0},
        {"labels": ["comware-switch-gsw256", "slot 15 chip 1 2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 15 chip 1 2"], "linkmon_switch_temperature": 53.0},
        {"labels": ["comware-switch-gsw256", "slot 15 cpu 1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["comware-switch-gsw256", "slot 15 cpu 1"], "linkmon_switch_temperature": 42.0},
        {"labels": ["comware-switch-gsw256", "slot 15 added by theo"], "linkmon_switch_temperature": 89.0},
        {"labels": ["comware-switch-gsw256", "slot 15 added by theo"], "linkmon_switch_temperature_health": 1},
        {"labels": ["comware-switch-gsw256", "slot 16 added by theo"], "linkmon_switch_temperature": 100.0},
        {"labels": ["comware-switch-gsw256", "slot 16 added by theo"], "linkmon_switch_temperature_health": 2},
        {"labels": ["comware-switch-gsw256", "PSU1"], "linkmon_switch_psu_health": 0.0},
        {"labels": ["comware-switch-gsw256", "PSU2"], "linkmon_switch_psu_health": 0.0},
        {"labels": ["comware-switch-gsw256", "PSU3"], "linkmon_switch_psu_health": 0.0},
        {"labels": ["comware-switch-gsw256", "PSU4"], "linkmon_switch_psu_health": 0.0},
        {"labels": ["comware-switch-gsw256", "PSU5"], "linkmon_switch_psu_health": 0.0},
        {"labels": ["comware-switch-gsw256", "PSU6"], "linkmon_switch_psu_health": 0.0},
        {"labels": ["comware-switch-gsw256", "PSU7"], "linkmon_switch_psu_health": 0.0},
        {"labels": ["comware-switch-gsw256", "PSU8"], "linkmon_switch_psu_health": 0.0},
        {"labels": ["comware-switch-gsw256", "PSU9"], "linkmon_switch_psu_health": 1.0},
        {"labels": ["comware-switch-gsw256", "PSU10"], "linkmon_switch_psu_health": 2.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 1"], "linkmon_switch_fan_health": 0.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 1 / 1"], "linkmon_switch_fan_rpm": 7122.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 1 / 2"], "linkmon_switch_fan_rpm": 5592.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 1 / 3"], "linkmon_switch_fan_rpm": 5597.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 2"], "linkmon_switch_fan_health": 0.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 2 / 1"], "linkmon_switch_fan_rpm": 7048.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 2 / 2"], "linkmon_switch_fan_rpm": 5613.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 2 / 3"], "linkmon_switch_fan_rpm": 5502.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 3"], "linkmon_switch_fan_health": 1.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 3 / 1"], "linkmon_switch_fan_rpm": 3333.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 4"], "linkmon_switch_fan_health": 2.0},
        {"labels": ["comware-switch-gsw256", "Fan-tray 4 / 1"], "linkmon_switch_fan_rpm": 4444.0},
        {"labels": ["comware-switch-gsw256"], "linkmon_switch_uptime": 33568140},
    ]
    compare_results(s, expected_switch_values, expected_values)


def test_arista_parse():
    expected_values = []

    l = [
        'arista-switch',
        'Ethernet1/1',
        'L3 to sc-r15ra1-100gsw256-hpe',
    ]

    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '1001',
                '1001',
                '100000',
                'c0:69:11:02:72:75',
                '100GBASE-SR4',
                '',
                'Bcm56965-tscf (A) (0x00c086,0x37,0xc2d5)',
                '',
                'V0PD8JA005X',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 8,
            "linkmon_switch_delta_fec_uncorrected_codewords": 5,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 32570174802336,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 2,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 8897,
            "linkmon_switch_fec_uncorrected_codewords": 5,
            "linkmon_switch_opt_temperature": 45.00,
            "linkmon_switch_opt_voltage": 3.22,
            "linkmon_switch_opt_tx_current": { "1": 6.33,
            "2": 6.34,
            "3": 6.33,
            "4": 6.34,
            },"linkmon_switch_opt_tx_power": { "1": -0.62,
            "2": -1.02,
            "3": -1.03,
            "4": -0.56,
            },"linkmon_switch_opt_rx_power": { "1": -0.40,
            "2": -0.51,
            "3": -0.85,
            "4": -1.19,
            },"linkmon_switch_rx_pfc": 897875284,
            "linkmon_switch_tx_pfc": 2359769705,
            "linkmon_switch_in_discards": 4,
            "linkmon_switch_out_discards": 69804,
            "linkmon_switch_out_discards_prio5": 1111,
            "linkmon_switch_tx_kbps": 5222,
            "linkmon_switch_rx_kbps": 10342,
            "linkmon_switch_extra_labels": 1,
        }
    )
    l = [
        'arista-switch',
        'Ethernet2/1',
        'L3 to sc-r15ra1-100gsw256-hpe',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '2001',
                '2001',
                '100000',
                'c0:69:11:02:72:75',
                '100GBASE-SR4',
                '',
                'Bcm56965-tscf (A) (0x00c086,0x37,0xc2d5)',
                '',
                'V0PD8JA0039',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 5,
            "linkmon_switch_delta_fec_uncorrected_codewords": 4,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 32979717992932,
            "linkmon_switch_input_errors": 4,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 114726,
            "linkmon_switch_fec_uncorrected_codewords": 661,
            "linkmon_switch_opt_temperature": 46.00,
            "linkmon_switch_opt_voltage": 3.23,
            "linkmon_switch_opt_tx_current": { "1": 6.33,
            "2": 6.34,
            "3": 6.33,
            "4": 6.34,
            },"linkmon_switch_opt_tx_power": { "1": 0.66,
            "2": -0.02,
            "3": 0.39,
            "4": 0.36,
            },"linkmon_switch_opt_rx_power": { "1": -0.62,
            "2": -0.62,
            "3": -0.76,
            "4": -0.40,
            },"linkmon_switch_rx_pfc": 1199789375,
            "linkmon_switch_tx_pfc": 2760380883,
            "linkmon_switch_in_discards": 20,
            "linkmon_switch_out_discards": 53220,
            "linkmon_switch_out_discards_prio5": 2222,
            "linkmon_switch_tx_kbps": 102,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )
    l = [
        'arista-switch',
        'Ethernet3/1',
        'L3 to sc-r15ra1-100gsw256-hpe',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '3001',
                '3001',
                '100000',
                'c0:69:11:02:72:75',
                '100GBASE-SR4',
                '',
                'Bcm56965-tscf (A) (0x00c086,0x37,0xc2d5)',
                '',
                'V0PD8JA004B',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 3,
            "linkmon_switch_delta_fec_uncorrected_codewords": 3,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 32802280958554,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 10,
            "linkmon_switch_fec_uncorrected_codewords": 3,
            "linkmon_switch_opt_temperature": 44.00,
            "linkmon_switch_opt_voltage": 3.22,
            "linkmon_switch_opt_tx_current": { "1": 6.33,
            "2": 6.34,
            "3": 6.33,
            "4": 6.34,
            },"linkmon_switch_opt_tx_power": { "1": -0.69,
            "2": -1.14,
            "3": -1.09,
            "4": -1.10,
            },"linkmon_switch_opt_rx_power": { "1": -0.06,
            "2": 0.00,
            "3": -0.51,
            "4": -0.50,
            },"linkmon_switch_rx_pfc": 1319496919,
            "linkmon_switch_tx_pfc": 2688539247,
            "linkmon_switch_in_discards": 37,
            "linkmon_switch_out_discards": 487034,
            "linkmon_switch_out_discards_prio5": 3333,
            "linkmon_switch_tx_kbps": 102,
            "linkmon_switch_rx_kbps": 102,
            "linkmon_switch_extra_labels": 1,
        }
    )
    l = [
        'arista-switch',
        'Ethernet4/1',
        'L3 to sc-r15ra1-100gsw256-hpe',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '4001',
                '4001',
                '100000',
                'c0:69:11:02:72:75',
                '100GBASE-SR4',
                '',
                'Bcm56965-tscf (A) (0x00c086,0x37,0xc2d5)',
                '',
                'V0PD8JA0007',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": ["lfs"],
            "linkmon_switch_delta_fec_corrected_codewords": 3,
            "linkmon_switch_delta_fec_uncorrected_codewords": 3,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 1,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 32399883641657,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 5,
            "linkmon_switch_fec_corrected_codewords": 9,
            "linkmon_switch_fec_uncorrected_codewords": 3,
            "linkmon_switch_opt_temperature": 44.00,
            "linkmon_switch_opt_voltage": 3.23,
            "linkmon_switch_opt_tx_current": { "1": 6.23,
            "2": 6.24,
            "3": 6.24,
            "4": 6.24,
            },"linkmon_switch_opt_tx_power": { "1": -0.62,
            "2": -0.55,
            "3": -0.54,
            "4": -1.27,
            },"linkmon_switch_opt_rx_power": { "1": -0.87,
            "2": -0.46,
            "3": -0.83,
            "4": -0.36,
            },"linkmon_switch_rx_pfc": 1692088617,
            "linkmon_switch_tx_pfc": 2414241463,
            "linkmon_switch_in_discards": 18,
            "linkmon_switch_out_discards": 88517,
            "linkmon_switch_out_discards_prio5": 4444,
            "linkmon_switch_tx_kbps": 102,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    s = switch.SwitchSurvey([], {}, {}, False, False, "", "")
    s.switches = ["arista-switch"]
    s.switch_mgmt_ips["arista-switch"] = "123.4.5.6"
    s.stats["arista-switch"] = switch.SwitchInfo("arista-switch")
    s.hpe_hostnames = []
    s.arista_hostnames = ["arista-switch"]
    s.sonic_hostnames = []
    s.juniper_hostnames = []
    s.credo_hostnames = []

    with open("test-inputs/arista", "r") as f:
        s.parse("arista-switch", f.readlines())

    expected_switch_values = [
        {"labels": ["arista-switch", "PSU1"], "linkmon_switch_psu_health": 0},
        # CLI output modified to simulate an error:
        {"labels": ["arista-switch", "PSU2"], "linkmon_switch_psu_health": 2},
        {
            "labels": ["arista-switch", "Cpu temp sensor"],
            "linkmon_switch_temperature": 36.692074981807096,
        },
        {
            "labels": ["arista-switch", "Cpu temp sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "CPU board temp sensor"],
            "linkmon_switch_temperature": 38.125,
        },
        {
            "labels": ["arista-switch", "CPU board temp sensor"],
            # CLI output modified to simulate a warning:
            "linkmon_switch_temperature_health": 1,
        },
        {
            "labels": ["arista-switch", "Back-panel temp sensor"],
            "linkmon_switch_temperature": 36.875,
        },
        {
            "labels": ["arista-switch", "Back-panel temp sensor"],
            # CLI output modified to simulate an error:
            "linkmon_switch_temperature_health": 2,
        },
        {
            "labels": ["arista-switch", "POSVDD_CPU_S0 Pol Loop0"],
            "linkmon_switch_temperature": 41.0,
        },
        {
            "labels": ["arista-switch", "POSVDD_CPU_S0 Pol Loop0"],
            # CLI output modified to simulate unknown:
            "linkmon_switch_temperature_health": 3,
        },
        {
            "labels": ["arista-switch", "POSVDD_SOC_S0 Pol Loop1"],
            "linkmon_switch_temperature": 41.0,
        },
        {
            "labels": ["arista-switch", "POSVDD_SOC_S0 Pol Loop1"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Switch Card temp sensor"],
            "linkmon_switch_temperature": 38.375,
        },
        {
            "labels": ["arista-switch", "Switch Card temp sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Air Exit Behind TH4"],
            "linkmon_switch_temperature": 41.3125,
        },
        {
            "labels": ["arista-switch", "Air Exit Behind TH4"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Air Inlet"],
            "linkmon_switch_temperature": 31.5625,
        },
        {
            "labels": ["arista-switch", "Air Inlet"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Front Panel Inlet"],
            "linkmon_switch_temperature": 29.875,
        },
        {
            "labels": ["arista-switch", "Front Panel Inlet"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PhyChip0-0"],
            "linkmon_switch_temperature": 57.34302956005567,
        },
        {
            "labels": ["arista-switch", "PhyChip0-0"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PhyChip0-1"],
            "linkmon_switch_temperature": 57.39256546625987,
        },
        {
            "labels": ["arista-switch", "PhyChip0-1"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PhyChip1-0"],
            "linkmon_switch_temperature": 51.89407987759353,
        },
        {
            "labels": ["arista-switch", "PhyChip1-0"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PhyChip1-1"],
            "linkmon_switch_temperature": 53.182013438902764,
        },
        {
            "labels": ["arista-switch", "PhyChip1-1"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PhyChip2-0"],
            "linkmon_switch_temperature": 43.076688573245704,
        },
        {
            "labels": ["arista-switch", "PhyChip2-0"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PhyChip2-1"],
            "linkmon_switch_temperature": 43.67111944769612,
        },
        {
            "labels": ["arista-switch", "PhyChip2-1"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PhyChip3-0"],
            "linkmon_switch_temperature": 45.75162750827258,
        },
        {
            "labels": ["arista-switch", "PhyChip3-0"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PhyChip3-1"],
            "linkmon_switch_temperature": 46.8909533509692,
        },
        {
            "labels": ["arista-switch", "PhyChip3-1"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT0 sensor"],
            "linkmon_switch_temperature": 43.9,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT0 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT1 sensor"],
            "linkmon_switch_temperature": 44.3,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT1 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT2 sensor"],
            "linkmon_switch_temperature": 45.5,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT2 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT3 sensor"],
            "linkmon_switch_temperature": 45.1,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT3 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT4 sensor"],
            "linkmon_switch_temperature": 42.7,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT4 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT5 sensor"],
            "linkmon_switch_temperature": 41.5,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT5 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT6 sensor"],
            "linkmon_switch_temperature": 44.1,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT6 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT7 sensor"],
            "linkmon_switch_temperature": 44.8,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT7 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT8 sensor"],
            "linkmon_switch_temperature": 43.4,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT8 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT9 sensor"],
            "linkmon_switch_temperature": 41.5,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT9 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT10 sensor"],
            "linkmon_switch_temperature": 42.7,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT10 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT11 sensor"],
            "linkmon_switch_temperature": 43.2,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT11 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT12 sensor"],
            "linkmon_switch_temperature": 42.0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT12 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT13 sensor"],
            "linkmon_switch_temperature": 41.3,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT13 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT14 sensor"],
            "linkmon_switch_temperature": 41.7,
        },
        {
            "labels": ["arista-switch", "Tomahawk4 PVT14 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PSU1 Inlet"],
            "linkmon_switch_temperature": 39.25,
        },
        {
            "labels": ["arista-switch", "PSU1 Inlet"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PSU1 Secondary hotspot"],
            "linkmon_switch_temperature": 55.25,
        },
        {
            "labels": ["arista-switch", "PSU1 Secondary hotspot"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PSU1 Primary hotspot"],
            "linkmon_switch_temperature": 61.375,
        },
        {
            "labels": ["arista-switch", "PSU1 Primary hotspot"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PSU2 Inlet"],
            "linkmon_switch_temperature": 43.75,
        },
        {
            "labels": ["arista-switch", "PSU2 Inlet"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PSU2 Secondary hotspot"],
            "linkmon_switch_temperature": 54.5,
        },
        {
            "labels": ["arista-switch", "PSU2 Secondary hotspot"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["arista-switch", "PSU2 Primary hotspot"],
            "linkmon_switch_temperature": 61.75,
        },
        {
            "labels": ["arista-switch", "PSU2 Primary hotspot"],
            "linkmon_switch_temperature_health": 0,
        },
        {"labels": ["arista-switch", "PowerSupply1/1"], "linkmon_switch_fan_health": 0},
        {
            "labels": ["arista-switch", "PowerSupply1/1"],
            "linkmon_switch_fan_rpm": 11500.0,
        },
        {"labels": ["arista-switch", "PowerSupply2/1"], "linkmon_switch_fan_health": 0},
        {
            "labels": ["arista-switch", "PowerSupply2/1"],
            "linkmon_switch_fan_rpm": 11500.0,
        },
        {"labels": ["arista-switch", "Fan tray 1/1"],
            "linkmon_switch_fan_health": 0},
        {"labels": ["arista-switch", "Fan tray 1/1"],
            "linkmon_switch_fan_rpm": 8925.0},
        # CLI output modified to simulate a warning:
        {"labels": ["arista-switch", "Fan tray 2/1"],
            "linkmon_switch_fan_health": 1},
        {"labels": ["arista-switch", "Fan tray 2/1"],
            "linkmon_switch_fan_rpm": 8925.0},
        # CLI output modified to simulate an error:
        {"labels": ["arista-switch", "Fan tray 3/1"],
            "linkmon_switch_fan_health": 2},
        {"labels": ["arista-switch", "Fan tray 3/1"],
            "linkmon_switch_fan_rpm": 8925.0},
        # CLI output modified to simulate unknown:
        {"labels": ["arista-switch", "Fan tray 4/1"],
            "linkmon_switch_fan_health": 3},
        {"labels": ["arista-switch", "Fan tray 4/1"],
            "linkmon_switch_fan_rpm": 8925.0},
        {"labels": ["arista-switch", "Arista", "DCS-7060CX-32S-F",
                    "4.23.0F"], "linkmon_switch_version_info": 1},
        {"labels": ["arista-switch"], "linkmon_switch_uptime": 48271281.9},
    ]

    compare_results(s, expected_switch_values, expected_values)


def test_hpe_parse():
    expected_values = []
    l = [
        'hpe-switch',
        'Eth1/1',
        'L3 to sc-r15ra1-100gsw256-hpe',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                '1c:34:da:23:70:88',
                '200GBASE-CR4',
                'Amphenal',
                'NDAAXF-0003',
                'D',
                'APF23240038CW8',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 194051945361319,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 3,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 176960073,
            "linkmon_switch_tx_pfc": 60912335,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 2948,
            "linkmon_switch_rx_kbps": 5426,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'hpe-switch',
        'Eth1/2',
        'L3 to sc-r15ra1-100gsw256-hpe',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                '1c:34:da:23:70:88',
                '200GBASE-CR4',
                'Amphenal',
                'NDAAXF-0003',
                'D',
                'APF23240038CVY',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 166953670204163,
            "linkmon_switch_input_errors": 7,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 1,
            "linkmon_switch_fec_corrected_codewords": 3,
            "linkmon_switch_fec_uncorrected_codewords": 2,
            # NOTE: I don't think we have any switches that support this command, but we may in the future.
            #       These values are faked in the test data based on the sample output in the switch docs.
            "linkmon_switch_opt_temperature": 26,
            "linkmon_switch_opt_voltage": 3.28980,
            "linkmon_switch_opt_tx_current": { "1": 6.6,
            "2": 6.7,
            "3": 6.8,
            "4": 6.9,
            },"linkmon_switch_opt_tx_power": { "1": 0.06124,
            "2": -0.14394,
            "3": -0.14439,
            "4": -0.17503,
            },"linkmon_switch_opt_rx_power": { "1": -0.03663,
            "2": 0.25633,
            "3": 0.40642,
            "4": -0.10995,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'hpe-switch',
        'Eth1/3',
        'N/A',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '',
                '1c:34:da:23:70:6c',
                '200GBASE-CR4',
                'Amphenal',
                'NDAAXF-0003',
                'D',
                'APF23240038CVW',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 164042994,
            "linkmon_switch_tx_pfc": 49506182,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 1,
            "linkmon_switch_rx_kbps": 1,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'hpe-switch',
        'Eth1/4',
        'N/A',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '',
                '1c:34:da:23:70:6e',
                '200GBASE-CR4',
                'Amphenal',
                'NDAAXF-0003',
                'D',
                'APF23240038CDC',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    s = switch.SwitchSurvey([], {}, {}, False, False, "", "")
    s.switches = ["hpe-switch"]
    s.switch_mgmt_ips["hpe-switch"] = "123.4.5.6"
    s.stats["hpe-switch"] = switch.SwitchInfo("hpe-switch")
    s.hpe_hostnames = ["hpe-switch"]
    s.arista_hostnames = []
    s.sonic_hostnames = []
    s.juniper_hostnames = []
    s.credo_hostnames = []

    with open("test-inputs/hpe", "r") as f:
        s.parse("hpe-switch", f.readlines())

    expected_switch_values = [
        {"labels": ["hpe-switch", "HPE", "R6R24-63001",
                    "3.10.4302"], "linkmon_switch_version_info": 1},
        {"labels": ["hpe-switch", "PS1"], "linkmon_switch_psu_health": 0},
        {"labels": ["hpe-switch", "PS2"], "linkmon_switch_psu_health": 0},
        {"labels": ["hpe-switch", "MGMT SPC2 T1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["hpe-switch", "MGMT SPC2 T1"], "linkmon_switch_temperature": 50.0},
        {"labels": ["hpe-switch", "MGMT FAN Board AMB temp T1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["hpe-switch", "MGMT FAN Board AMB temp T1"], "linkmon_switch_temperature": 23.69},
        {"labels": ["hpe-switch", "MGMT Front AMB temp T1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["hpe-switch", "MGMT Front AMB temp T1"], "linkmon_switch_temperature": 29.38},
        {"labels": ["hpe-switch", "MGMT CPU package Sensor T1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["hpe-switch", "MGMT CPU package Sensor T1"], "linkmon_switch_temperature": 50.0},
        {"labels": ["hpe-switch", "MGMT CPU Core Sensor T1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["hpe-switch", "MGMT CPU Core Sensor T1"], "linkmon_switch_temperature": 50.0},
        {"labels": ["hpe-switch", "MGMT CPU Core Sensor T2"], "linkmon_switch_temperature_health": 0},
        {"labels": ["hpe-switch", "MGMT CPU Core Sensor T2"], "linkmon_switch_temperature": 50.0},
        {"labels": ["hpe-switch", "MGMT COMEX Ambient Sensor T1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["hpe-switch", "MGMT COMEX Ambient Sensor T1"], "linkmon_switch_temperature": 39.06},
        {"labels": ["hpe-switch", "PS1 power-mon T1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["hpe-switch", "PS1 power-mon T1"], "linkmon_switch_temperature": 24.75},
        {"labels": ["hpe-switch", "PS2 power-mon T1"], "linkmon_switch_temperature_health": 0},
        {"labels": ["hpe-switch", "PS2 power-mon T1"], "linkmon_switch_temperature": 25.0},
        {"labels": ["hpe-switch", "TEST added by theo1 T1"], "linkmon_switch_temperature_health": 1},
        {"labels": ["hpe-switch", "TEST added by theo1 T1"], "linkmon_switch_temperature": -1},
        {"labels": ["hpe-switch", "TEST added by theo2 T1"], "linkmon_switch_temperature_health": 2},
        {"labels": ["hpe-switch", "TEST added by theo2 T1"], "linkmon_switch_temperature": 12.34},
        {"labels": ["hpe-switch", "FAN1 FAN F1"], "linkmon_switch_fan_health": 0},
        {"labels": ["hpe-switch", "FAN1 FAN F1"], "linkmon_switch_fan_rpm": 11938.0},
        {"labels": ["hpe-switch", "FAN1 FAN F2"], "linkmon_switch_fan_health": 0},
        {"labels": ["hpe-switch", "FAN1 FAN F2"], "linkmon_switch_fan_rpm": 10686.0},
        {"labels": ["hpe-switch", "FAN2 FAN F1"], "linkmon_switch_fan_health": 0},
        {"labels": ["hpe-switch", "FAN2 FAN F1"], "linkmon_switch_fan_rpm": 12046.0},
        {"labels": ["hpe-switch", "FAN2 FAN F2"], "linkmon_switch_fan_health": 0},
        {"labels": ["hpe-switch", "FAN2 FAN F2"], "linkmon_switch_fan_rpm": 10601.0},
        {"labels": ["hpe-switch", "FAN3 FAN F1"], "linkmon_switch_fan_health": 0},
        {"labels": ["hpe-switch", "FAN3 FAN F1"], "linkmon_switch_fan_rpm": 12157.0},
        {"labels": ["hpe-switch", "FAN3 FAN F2"], "linkmon_switch_fan_health": 0},
        {"labels": ["hpe-switch", "FAN3 FAN F2"], "linkmon_switch_fan_rpm": 10773.0},
        {"labels": ["hpe-switch", "FAN4 FAN F1"], "linkmon_switch_fan_health": 0},
        {"labels": ["hpe-switch", "FAN4 FAN F1"], "linkmon_switch_fan_rpm": 12157.0},
        {"labels": ["hpe-switch", "FAN4 FAN F2"], "linkmon_switch_fan_health": 0},
        {"labels": ["hpe-switch", "FAN4 FAN F2"], "linkmon_switch_fan_rpm": 10686.0},
        {"labels": ["hpe-switch", "PS1 FAN F1"], "linkmon_switch_fan_health": 0},
        {"labels": ["hpe-switch", "PS1 FAN F1"], "linkmon_switch_fan_rpm": 13136.0},
        {"labels": ["hpe-switch", "PS2 FAN F1"], "linkmon_switch_fan_health": 0},
        {"labels": ["hpe-switch", "PS2 FAN F1"], "linkmon_switch_fan_rpm": 13328.0},
        {"labels": ["hpe-switch", "TEST added by theo1 -"], "linkmon_switch_fan_health": 1},
        {"labels": ["hpe-switch", "TEST added by theo1 -"], "linkmon_switch_fan_rpm": -1},
        {"labels": ["hpe-switch", "TEST added by theo2 F1"], "linkmon_switch_fan_health": 2},
        {"labels": ["hpe-switch", "TEST added by theo2 F1"], "linkmon_switch_fan_rpm": 22.0},
        {"labels": ["hpe-switch"], "linkmon_switch_uptime": 20977596.416},
    ]
    compare_results(s, expected_switch_values, expected_values)


def test_arista400_parse():
    expected_values = []

    l = ['sc-r7rb14-400gsw',
         'Ethernet1/1',
         'L2 to cs301-wse001-sx-sr01/eth100g2']
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '1001',
                '1001',
                '100000',
                'e4:78:76:71:34:fb',
                '400GBASE-CR8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'APF23420045B2U',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 2205674,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1429898829319109,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 2,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1217,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 22679316,
            "linkmon_switch_tx_pfc": 358878289,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_out_discards_prio5": 1111,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 7530885,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = ['sc-r7rb14-400gsw',
         'Ethernet1/3',
         'L2 to cs301-wse001-sx-sr01/eth100g3']
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '1003',
                '1003',
                '100000',
                'e4:78:76:71:34:fd',
                '400GBASE-CR8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'APF23420045B2U',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 91882,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1324739640673672,
            "linkmon_switch_input_errors": 4,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 23043798,
            "linkmon_switch_tx_pfc": 356937706,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 289070773,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = ['sc-r7rb14-400gsw',
         'Ethernet1/5',
         'L2 to cs301-wse001-sx-sr01/eth100g4']
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '1005',
                '1005',
                '100000',
                'e4:78:76:71:34:ff',
                '400GBASE-CR8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'APF23420045B2U',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 27975,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1373253301860939,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 38719678,
            "linkmon_switch_tx_pfc": 577484001,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 112657868,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet1/7',
        'L2 to cs301-wse001-sx-sr01/eth100g5',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '1007',
                '1007',
                '100000',
                'e4:78:76:71:35:00',
                '400GBASE-CR8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'APF23420045B2U',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 520213,
            "linkmon_switch_delta_fec_uncorrected_codewords": 1,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 506913951589715,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1,
            "linkmon_switch_fec_uncorrected_codewords": 1,
            "linkmon_switch_rx_pfc": 35416849,
            "linkmon_switch_tx_pfc": 187973399,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 127752298,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet2/1',
        'L2 to xs10008/Port_1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '2001',
                '2001',
                '100000',
                'e4:78:76:71:35:01',
                '8x25GBASE-SR',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'V9NDBBWJ01X',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 2,
            "linkmon_switch_delta_fec_uncorrected_codewords": 44,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 32,
            "linkmon_switch_rx_bytes": 1157693510415245,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1,
            "linkmon_switch_fec_uncorrected_codewords": 4,
            "linkmon_switch_opt_temperature": 33.27,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_opt_tx_current": { "1": 5.73,
            "2": 5.74,
            "3": 5.73,
            "4": 5.74,
            },"linkmon_switch_opt_tx_power": { "1": -1.18,
            "2": -1.36,
            "3": -1.19,
            "4": -1.35,
            },"linkmon_switch_opt_rx_power": { "1": 1.26,
            "2": 1.43,
            "3": 1.64,
            "4": 1.67,
            },"linkmon_switch_rx_pfc": 412909853,
            "linkmon_switch_tx_pfc": 653456659,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 43230444,
            "linkmon_switch_out_discards_prio5": 2222,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 102,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet2/5',
        'L2 to xs10006/Port_1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '2005',
                '2005',
                '100000',
                'e4:78:76:71:35:05',
                '8x25GBASE-SR',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'V9NDBBWJ01X',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 367,
            "linkmon_switch_delta_fec_uncorrected_codewords": 56,
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 31,
            "linkmon_switch_rx_bytes": 552447258208669,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 2,
            "linkmon_switch_fec_uncorrected_codewords": 11,
            "linkmon_switch_opt_temperature": 33.27,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_opt_tx_current": { "5": 5.73,
            "6": 5.74,
            "7": 5.73,
            "8": 5.74,
            },"linkmon_switch_opt_tx_power": { "5": -0.72,
            "6": -0.98,
            "7": -0.97,
            "8": -1.31,
            },"linkmon_switch_opt_rx_power": { "5": -30.0,
            "6": -30.0,
            "7": -30.0,
            "8": -30.0,
            },"linkmon_switch_rx_pfc": 582204,
            "linkmon_switch_tx_pfc": 16206748,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet3/1',
        'L2 to cs301-wse001-mx-sr01/eth100g0',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '3001',
                '3001',
                '100000',
                'e4:78:76:71:35:07',
                '400GBASE-CR8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'C2311639867',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 387113,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1260503009180854,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 57615815,
            "linkmon_switch_tx_pfc": 167882033,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 308884,
            "linkmon_switch_out_discards_prio5": 3333,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 243717939,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet3/3',
        'L2 to cs301-wse001-mx-sr01/eth100g1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '3003',
                '3003',
                '100000',
                'e4:78:76:71:35:09',
                '400GBASE-CR8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'C2311639867',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 1570615,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1491981197374,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 1086,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_out_discards_prio5": 4444,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet3/5',
        'L2 to cs301-wse001-mx-sr09/eth100g0',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '3005',
                '3005',
                '100000',
                'e4:78:76:71:35:0b',
                '400GBASE-CR8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'C2311639867',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 1053515,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1124066104793801,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 27,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 45778646,
            "linkmon_switch_tx_pfc": 55068327,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 86264069,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet3/7',
        'L2 to cs301-wse001-mx-sr09/eth100g1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '3007',
                '3007',
                '100000',
                'e4:78:76:71:35:0c',
                '400GBASE-CR8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'C2311639867',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 898804,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1279093063289,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 1120,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet27/1',
        'L2 to cs301-wse001-mx-sr02/eth100g0',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '27001',
                '27001',
                '100000',
                'e4:78:76:71:35:97',
                '400GBASE-DR4',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'CRDR2333087PP',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": ["lfs"],
            "linkmon_switch_delta_fec_corrected_codewords": 1546125,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 1,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1303122455767547,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 374,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 46.08,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_opt_tx_current": { "1": 78.14,
            },"linkmon_switch_opt_tx_power": { "1": 0.62,
            },"linkmon_switch_opt_rx_power": { "1": 2.27,
            },"linkmon_switch_rx_pfc": 59463843,
            "linkmon_switch_tx_pfc": 268747231,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 53563,
            "linkmon_switch_tx_kbps": 91033,
            "linkmon_switch_rx_kbps": 409,
            "linkmon_switch_ecn_marked_packets_tc5": 272970058,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet27/3',
        'L2 to cs301-wse001-mx-sr02/eth100g1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '27003',
                '27003',
                '100000',
                'e4:78:76:71:35:99',
                '400GBASE-DR4',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'CRDR2333087PP',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": ["lfs"],
            "linkmon_switch_delta_fec_corrected_codewords": 2074244,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 1,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1279083824370,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 86,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 46.08,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_opt_tx_current": { "1": 81.31,
            },"linkmon_switch_opt_tx_power": { "1": 1.31,
            },"linkmon_switch_opt_rx_power": { "1": 1.32,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 2004,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet27/5',
        'L2 to cs301-wse001-mx-sr10/eth100g0',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '27005',
                '27005',
                '100000',
                'e4:78:76:71:35:9b',
                '400GBASE-DR4',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'CRDR2333087PP',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 1546125,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1136228243196009,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1068,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 46.08,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_opt_tx_current": { "1": 90.29,
            },"linkmon_switch_opt_tx_power": { "1": 2.0,
            },"linkmon_switch_opt_rx_power": { "1": 2.61,
            },"linkmon_switch_rx_pfc": 42978845,
            "linkmon_switch_tx_pfc": 551358482,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 2030162,
            "linkmon_switch_tx_kbps": 91340,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 197861355,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet27/7',
        'L2 to cs301-wse001-mx-sr10/eth100g1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '27007',
                '27007',
                '100000',
                'e4:78:76:71:35:9c',
                '400GBASE-DR4',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'CRDR2333087PP',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 1549100,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1279056579107,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 4933,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 46.08,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_opt_tx_current": { "1": 81.84,
            },"linkmon_switch_opt_tx_power": { "1": 1.21,
            },"linkmon_switch_opt_rx_power": { "1": 3.08,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 7188,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet31/1',
        'L2 to cs301-wse001-mx-sr11/eth100g0',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '31001',
                '31001',
                '100000',
                'e4:78:76:71:35:af',
                '400GBASE-CRA8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'BQ4Q12X32710001',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 1546099,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1116121876639582,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 3344,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 34.22,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_rx_pfc": 49258891,
            "linkmon_switch_tx_pfc": 526624611,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 608972,
            "linkmon_switch_rx_kbps": 1463603,
            "linkmon_switch_ecn_marked_packets_tc5": 142450599,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet31/3',
        'L2 to cs301-wse001-mx-sr11/eth100g1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '31003',
                '31003',
                '100000',
                'e4:78:76:71:35:b1',
                '400GBASE-CRA8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'BQ4Q12X32710001',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 1546510,
            "linkmon_switch_delta_fec_uncorrected_codewords": 1,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1279054341250,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 108,
            "linkmon_switch_fec_uncorrected_codewords": 1,
            "linkmon_switch_opt_temperature": 34.22,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 5566,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet31/5',
        'L2 to cs301-wse001-mx-sr12/eth100g0',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '31005',
                '31005',
                '100000',
                'e4:78:76:71:35:b3',
                '400GBASE-CRA8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'BQ4Q12X32710001',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 1543801,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1153130858032817,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 180,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 34.22,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_rx_pfc": 52227806,
            "linkmon_switch_tx_pfc": 280613057,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 96768,
            "linkmon_switch_rx_kbps": 102,
            "linkmon_switch_ecn_marked_packets_tc5": 353003195,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet31/7',
        'L2 to cs301-wse001-mx-sr12/eth100g1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '31007',
                '31007',
                '100000',
                'e4:78:76:71:35:b4',
                '400GBASE-CRA8',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'BQ4Q12X32710001',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 881582,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 1279052889063,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 2,
            "linkmon_switch_fec_corrected_codewords": 1,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 34.22,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 1416,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet32/1',
        'L3 to sc-r7rb10-400gsw-spine2',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '32001',
                '32001',
                '400000',
                'e4:78:76:71:34:fa',
                '400GBASE-DR4',
                '',
                'BCM56993-BLACKHAWKGEN3 (B0) (0x000000,0x27,0x0)',
                'D005_18',
                'CRDR233404PBP',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 1527193,
            "linkmon_switch_delta_fec_uncorrected_codewords": 1,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 2683002740197751,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 2,
            "linkmon_switch_fec_uncorrected_codewords": 2,
            "linkmon_switch_opt_temperature": 52.3,
            "linkmon_switch_opt_voltage": 3.31,
            "linkmon_switch_opt_tx_current": { "1": 70.75,
            "2": 68.11,
            "3": 73.39,
            "4": 80.78,
            },"linkmon_switch_opt_tx_power": { "1": 0.99,
            "2": 1.80,
            "3": 1.78,
            "4": 1.83,
            },"linkmon_switch_opt_rx_power": { "1": 0.66,
            "2": 0.94,
            "3": 0.85,
            "4": 1.02,
            },"linkmon_switch_rx_pfc": 371140038,
            "linkmon_switch_tx_pfc": 2689033069,
            "linkmon_switch_in_discards": 205,
            "linkmon_switch_out_discards": 9403,
            "linkmon_switch_tx_kbps": 102,
            "linkmon_switch_rx_kbps": 1906585,
            "linkmon_switch_ecn_marked_packets_tc5": 28279644,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet33',
        '',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '33',
                '33',
                '10000',
                'e4:78:76:71:35:bb',
                'Unknown',
                '',
                'BCM56993-MERLIN (B0)',
                'D000_02',
                '',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet46/1',
        'L3 to inf001-lf-sw01',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '46001',
                '46001',
                '400000',
                '68:8b:f4:c2:36:d0',
                '800GBASE-DR8',
                '',
                'BCM78900-PEREGRINE (B0) (0x000000,0x2d,0x0)',
                'D002_0B',
                '30618124723002R',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 33723,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 2,
            "linkmon_switch_rx_bytes": 8490236839249,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 977,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 59,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_opt_tx_current": { "1": 66.46,
            "2": 74.68,
            "3": 74.29,
            "4": 73.90,
            },"linkmon_switch_opt_tx_power": { "1": 2.00,
            "2": 2.33,
            "3": 2.04,
            "4": 2.40,
            },"linkmon_switch_opt_rx_power": { "1": 0.42,
            "2": 1.79,
            "3": 1.70,
            "4": 2.00,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 102,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )
    l = [
        'sc-r7rb14-400gsw',
        'Ethernet46/5',
        '',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '46005',
                '46005',
                '400000',
                '68:8b:f4:c2:36:d0',
                '800GBASE-DR8',
                '',
                'BCM78900-PEREGRINE (B0) (0x000000,0x2d,0x0)',
                'D002_0B',
                '30618124723002R',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_delta_fec_corrected_codewords": 0,
            "linkmon_switch_delta_fec_uncorrected_codewords": 0,
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 59,
            "linkmon_switch_opt_voltage": 3.28,
            "linkmon_switch_opt_tx_current": { "5": 63.14,
            "6": 73.50,
            "7": 70.96,
            "8": 78.00,
            },"linkmon_switch_opt_tx_power": { "5": 1.70,
            "6": 2.15,
            "7": 1.89,
            "8": 2.13,
            },"linkmon_switch_opt_rx_power": { "5": -30.00,
            "6": -30.00,
            "7": -30.00,
            "8": -30.00,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'sc-r7rb14-400gsw',
        'Ethernet61/1',
        'L2 to cs303-wse001-sx-sr07/eth400g5',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '61001',
                '61001',
                '400000',
                'fc:59:c0:11:06:1a',
                '',
                '',
                '',
                '',
                '',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "errdisabled_extra_labels": [""],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_errdisabled": 0,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 78283164514879,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_opt_temperature": 50.06,
            "linkmon_switch_opt_voltage": 3.29,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 3585,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    s = switch.SwitchSurvey([], {}, {}, False, False, "", "")
    s.switches = ["sc-r7rb14-400gsw"]
    s.switch_mgmt_ips["sc-r7rb14-400gsw"] = "123.4.5.6"
    s.stats["sc-r7rb14-400gsw"] = switch.SwitchInfo("sc-r7rb14-400gsw")
    s.hpe_hostnames = []
    s.arista_hostnames = ["sc-r7rb14-400gsw"]
    s.sonic_hostnames = []
    s.juniper_hostnames = []
    s.credo_hostnames = []

    with open("test-inputs/arista-400g", "r") as f:
        s.parse("sc-r7rb14-400gsw", f.readlines())

    expected_switch_values = [
        {"labels": ["sc-r7rb14-400gsw", "PSU1"], "linkmon_switch_psu_health": 0},
        {"labels": ["sc-r7rb14-400gsw", "PSU2"], "linkmon_switch_psu_health": 0},
        {
            "labels": ["sc-r7rb14-400gsw", "Cpu temp sensor"],
            "linkmon_switch_temperature": 52.86801083665395,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Cpu temp sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "CPU board temp sensor"],
            "linkmon_switch_temperature": 46.5,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "CPU board temp sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Back-panel temp sensor"],
            "linkmon_switch_temperature": 44.25,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Back-panel temp sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "POSVDD_CPU_S0 Pol Loop0"],
            "linkmon_switch_temperature": 53.0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "POSVDD_CPU_S0 Pol Loop0"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "POSVDD_SOC_S0 Pol Loop1"],
            "linkmon_switch_temperature": 53.0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "POSVDD_SOC_S0 Pol Loop1"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Switch Card temp sensor"],
            "linkmon_switch_temperature": 53.0625,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Switch Card temp sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Air Exit Behind TH4"],
            "linkmon_switch_temperature": 53.125,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Air Exit Behind TH4"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Air Inlet"],
            "linkmon_switch_temperature": 46.125,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Air Inlet"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Front Panel Inlet"],
            "linkmon_switch_temperature": 42.125,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Front Panel Inlet"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip0-0"],
            "linkmon_switch_temperature": 66.2099567706077,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip0-0"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip0-1"],
            "linkmon_switch_temperature": 66.45763630162871,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip0-1"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip1-0"],
            "linkmon_switch_temperature": 64.37712824105225,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip1-0"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip1-1"],
            "linkmon_switch_temperature": 65.86320542717829,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip1-1"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip2-0"],
            "linkmon_switch_temperature": 61.702189306025396,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip2-0"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip2-1"],
            "linkmon_switch_temperature": 62.1480124618632,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip2-1"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip3-0"],
            "linkmon_switch_temperature": 57.54117318487248,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip3-0"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip3-1"],
            "linkmon_switch_temperature": 56.89720640421786,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PhyChip3-1"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT0 sensor"],
            "linkmon_switch_temperature": 47.7,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT0 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT1 sensor"],
            "linkmon_switch_temperature": 49.3,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT1 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT2 sensor"],
            "linkmon_switch_temperature": 49.8,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT2 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT3 sensor"],
            "linkmon_switch_temperature": 48.4,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT3 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT4 sensor"],
            "linkmon_switch_temperature": 47.7,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT4 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT5 sensor"],
            "linkmon_switch_temperature": 47.2,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT5 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT6 sensor"],
            "linkmon_switch_temperature": 48.6,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT6 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT7 sensor"],
            "linkmon_switch_temperature": 49.6,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT7 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT8 sensor"],
            "linkmon_switch_temperature": 49.1,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT8 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT9 sensor"],
            "linkmon_switch_temperature": 47.2,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT9 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT10 sensor"],
            "linkmon_switch_temperature": 47.7,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT10 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT11 sensor"],
            "linkmon_switch_temperature": 47.2,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT11 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT12 sensor"],
            "linkmon_switch_temperature": 46.2,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT12 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT13 sensor"],
            "linkmon_switch_temperature": 46.7,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT13 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT14 sensor"],
            "linkmon_switch_temperature": 44.6,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Tomahawk4 PVT14 sensor"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU1 Inlet"],
            "linkmon_switch_temperature": 36.875,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU1 Inlet"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU1 Secondary hotspot"],
            "linkmon_switch_temperature": 36.25,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU1 Secondary hotspot"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU1 Primary hotspot"],
            "linkmon_switch_temperature": 0.0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU1 Primary hotspot"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU2 Inlet"],
            "linkmon_switch_temperature": 44.25,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU2 Inlet"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU2 Secondary hotspot"],
            "linkmon_switch_temperature": 64.5,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU2 Secondary hotspot"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU2 Primary hotspot"],
            "linkmon_switch_temperature": 65.125,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PSU2 Primary hotspot"],
            "linkmon_switch_temperature_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PowerSupply1/1"],
            "linkmon_switch_fan_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PowerSupply1/1"],
            "linkmon_switch_fan_rpm": 8160.0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PowerSupply2/1"],
            "linkmon_switch_fan_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "PowerSupply2/1"],
            "linkmon_switch_fan_rpm": 13770.0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Fan tray 1/1"],
            "linkmon_switch_fan_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Fan tray 1/1"],
            "linkmon_switch_fan_rpm": 3616.0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Fan tray 2/1"],
            "linkmon_switch_fan_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Fan tray 2/1"],
            "linkmon_switch_fan_rpm": 3616.0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Fan tray 3/1"],
            "linkmon_switch_fan_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Fan tray 3/1"],
            "linkmon_switch_fan_rpm": 3616.0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Fan tray 4/1"],
            "linkmon_switch_fan_health": 0,
        },
        {
            "labels": ["sc-r7rb14-400gsw", "Fan tray 4/1"],
            "linkmon_switch_fan_rpm": 3616.0,
        },
        {"labels": ["sc-r7rb14-400gsw", "Arista", "DCS-7060DX5-32-F",
                    "4.33.0F"], "linkmon_switch_version_info": 1},
        {"labels": ["sc-r7rb14-400gsw"], "linkmon_switch_uptime": 993171.37},
    ]

    compare_results(s, expected_switch_values, expected_values)


# Test the Dell SONiC switch output
def test_sonic_dell_parse():
    expected_values = []

    l = [
        'net001-lf-sw01',
        'Ethernet0',
        'L3 to net003-sp-sw01 Ethernet0',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'Intel Corp',
                'SPTSHP2PMCDF',
                '01',
                'CRDR234803YYP',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 2,
            "linkmon_switch_rx_bytes": 84540319,
            "linkmon_switch_input_errors": 2,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 2,
            "linkmon_switch_fec_corrected_codewords": 5073,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 49.8594,
            "linkmon_switch_opt_voltage": 3.1436,
            "linkmon_switch_opt_tx_current": { "1": 41.976,
            "2": 36.696,
            "3": 42.504,
            "4": 41.448,
            },"linkmon_switch_opt_tx_power": { "1": 1.6873,
            "2": 0.8994,
            "3": 1.1631,
            "4": 0.9545,
            },"linkmon_switch_opt_rx_power": { "1": 1.9551,
            "2": 1.7304,
            "3": 1.2746,
            "4": 2.1783,
            },"linkmon_switch_rx_pfc": 2816,
            "linkmon_switch_tx_pfc": 37154,
            "linkmon_switch_in_discards": 1,
            "linkmon_switch_tx_kbps": 2,
            "linkmon_switch_rx_kbps": 2,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet4',
        'L3 to net003-sp-sw01 Ethernet8',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'Intel Corp',
                'SPTSHP2PMCDF',
                '01',
                'CRDR23480AZ1P',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 2,
            "linkmon_switch_rx_bytes": 84546688,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 2,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 879,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 50.2148,
            "linkmon_switch_opt_voltage": 3.152,
            "linkmon_switch_opt_tx_current": { "1": 36.168,
            "2": 37.224,
            "3": 36.696,
            "4": 35.904,
            },"linkmon_switch_opt_tx_power": { "1": 1.0009,
            "2": 1.0619,
            "3": 1.3296,
            "4": 1.0514,
            },"linkmon_switch_opt_rx_power": { "1": 0.1808,
            "2": 0.7951,
            "3": 1.7099,
            "4": 0.7733,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 10,
            "linkmon_switch_tx_kbps": 5920,
            "linkmon_switch_rx_kbps": 2,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet20',
        'L3 to net003-sp-sw02 Ethernet8',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'Intel Corp',
                'SPTSHP2PMCDF',
                '01',
                'CRDR234804ERP',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 2,
            "linkmon_switch_rx_bytes": 82689717,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 11,
            "linkmon_switch_fec_corrected_codewords": 12582,
            "linkmon_switch_fec_uncorrected_codewords": 2,
            "linkmon_switch_opt_temperature": 49.5625,
            "linkmon_switch_opt_voltage": 3.1493,
            "linkmon_switch_opt_tx_current": { "1": 43.032,
            "2": 41.448,
            "3": 41.712,
            "4": 44.616,
            },"linkmon_switch_opt_tx_power": { "1": 1.5033,
            "2": 1.6814,
            "3": 2.0347,
            "4": 1.5779,
            },"linkmon_switch_opt_rx_power": { "1": 1.7038,
            "2": 1.867,
            "3": 2.1035,
            "4": 1.6679,
            },"linkmon_switch_rx_pfc": 818,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 12124,
            "linkmon_switch_rx_kbps": 735313,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet24',
        'L3 to net003-sp-sw02 Ethernet16',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'JABIL',
                'QD4CS3LCCCR0PAM',
                '01',
                'RMDR24040C7ES',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 2,
            "linkmon_switch_rx_bytes": 82689906,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 217,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 47.2031,
            "linkmon_switch_opt_voltage": 3.1526,
            "linkmon_switch_opt_tx_current": { "1": 50.952,
            "2": 58.872,
            "3": 52.008,
            "4": 57.552,
            },"linkmon_switch_opt_tx_power": { "1": 1.6259,
            "2": 1.5658,
            "3": 1.1331,
            "4": 2.1061,
            },"linkmon_switch_opt_rx_power": { "1": 0.6228,
            "2": 0.6744,
            "3": 1.2067,
            "4": 1.7621,
            },"linkmon_switch_rx_pfc": 3604,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 798801,
            "linkmon_switch_rx_kbps": 2,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet76',
        'L3 to net003-sp-sw05 Ethernet12',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'JABIL',
                'QD4CS3LCCCR0PAM',
                '01',
                'RMDR24060AC1S',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 2,
            "linkmon_switch_rx_bytes": 77809083,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 4853,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 51.3398,
            "linkmon_switch_opt_voltage": 3.2132,
            "linkmon_switch_opt_tx_current": { "1": 46.728,
            "2": 42.768,
            "3": 48.84,
            "4": 55.968,
            },"linkmon_switch_opt_tx_power": { "1": 0.7445,
            "2": 1.4934,
            "3": 1.3539,
            "4": 0.9297,
            },"linkmon_switch_opt_rx_power": { "1": 0.4328,
            "2": 0.5729,
            "3": -1.0007,
            "4": 0.7686,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 2,
            "linkmon_switch_rx_kbps": 52,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet97',
        'L2 to xs10047/Port_1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'JABIL',
                'QD4CS3LCCCR0PAM',
                '01',
                'RMDR24030C26S',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 4,
            "linkmon_switch_rx_bytes": 30729122,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 4634,
            "linkmon_switch_fec_uncorrected_codewords": 9,
            "linkmon_switch_opt_temperature": 47.4961,
            "linkmon_switch_opt_voltage": 3.1732,
            "linkmon_switch_opt_tx_current": { "1": 47.256,
            "2": 40.392,
            "3": 45.144,
            "4": 44.352,
            },"linkmon_switch_opt_tx_power": { "1": 1.5482,
            "2": 1.7624,
            "3": 1.8696,
            "4": 1.6161,
            },"linkmon_switch_opt_rx_power": { "1": 2.0737,
            "2": 2.0101,
            "3": 2.5285,
            "4": 2.3226,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 2,
            "linkmon_switch_rx_kbps": 2,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet98',
        'L2 to xs10019/Port_1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'JABIL',
                'QD4CS3LCCCR0PAM',
                '01',
                'RMDR24030C26S',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 4,
            "linkmon_switch_rx_bytes": 30415302,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 755,
            "linkmon_switch_fec_uncorrected_codewords": 8,
            "linkmon_switch_opt_temperature": 47.4961,
            "linkmon_switch_opt_voltage": 3.1732,
            "linkmon_switch_opt_tx_current": { "1": 47.256,
            "2": 40.392,
            "3": 45.144,
            "4": 44.352,
            },"linkmon_switch_opt_tx_power": { "1": 1.5482,
            "2": 1.7624,
            "3": 1.8696,
            "4": 1.6161,
            },"linkmon_switch_opt_rx_power": { "1": 2.0737,
            "2": 2.0101,
            "3": 2.5285,
            "4": 2.3226,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 2,
            "linkmon_switch_rx_kbps": 2,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet99',
        'L2 to xs10046/Port_1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'JABIL',
                'QD4CS3LCCCR0PAM',
                '01',
                'RMDR24030C26S',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 4,
            "linkmon_switch_rx_bytes": 30182854,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 536,
            "linkmon_switch_fec_uncorrected_codewords": 9,
            "linkmon_switch_opt_temperature": 47.4961,
            "linkmon_switch_opt_voltage": 3.1732,
            "linkmon_switch_opt_tx_current": { "1": 47.256,
            "2": 40.392,
            "3": 45.144,
            "4": 44.352,
            },"linkmon_switch_opt_tx_power": { "1": 1.5482,
            "2": 1.7624,
            "3": 1.8696,
            "4": 1.6161,
            },"linkmon_switch_opt_rx_power": { "1": 2.0737,
            "2": 2.0101,
            "3": 2.5285,
            "4": 2.3226,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 2,
            "linkmon_switch_rx_kbps": 2,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet116',
        'N/A',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'JABIL',
                'QD4CS3LCCCR0PAM',
                '01',
                'RMDR24060645S',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 45.0352,
            "linkmon_switch_opt_voltage": 3.2628,
            "linkmon_switch_opt_tx_current": { "1": 49.896,
            "2": 39.864,
            "3": 46.728,
            "4": 40.128,
            },"linkmon_switch_opt_tx_power": { "1": 2.9141,
            "2": 1.5241,
            "3": 2.5142,
            "4": 1.5186,
            },"linkmon_switch_opt_rx_power": { "1": -40,
            "2": -40,
            "3": -40,
            "4": -40,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet128',
        'L2 to cs307-net001-sx-sr01/eth400g5',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'Fast Photonics',
                '170-0477-01',
                'VR',
                '2833812432900JD',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 2,
            "linkmon_switch_rx_bytes": 1362950,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 470561440,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 46.7617,
            "linkmon_switch_opt_voltage": 3.1848,
            "linkmon_switch_opt_tx_current": { "1": 8.52,
            "2": 8.54,
            "3": 8.4,
            "4": 8.5,
            },"linkmon_switch_opt_tx_power": { "1": 2.9511,
            "2": 2.2324,
            "3": 2.5952,
            "4": 2.2699,
            },"linkmon_switch_opt_rx_power": { "1": 2.1418,
            "2": 2.7926,
            "3": 2.7221,
            "4": 2.8276,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet198',
        'L2 to cs307-net003-sx-sr03/eth100g2',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'JABIL',
                'QD4CS3LCCCR0PAM',
                '01',
                'RMDR24040978S',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 4,
            "linkmon_switch_rx_bytes": 2127908,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 3821,
            "linkmon_switch_fec_uncorrected_codewords": 4,
            "linkmon_switch_opt_temperature": 50.3711,
            "linkmon_switch_opt_voltage": 3.1924,
            "linkmon_switch_opt_tx_current": { "1": 35.112,
            "2": 38.28,
            "3": 38.28,
            "4": 45.936,
            },"linkmon_switch_opt_tx_power": { "1": 1.7403,
            "2": 2.1056,
            "3": 1.8969,
            "4": 2.6776,
            },"linkmon_switch_opt_rx_power": { "1": 1.0571,
            "2": 1.5174,
            "3": 1.4092,
            "4": 1.823,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet220',
        'L2 to cs307-net003-ax-sr02/eth400g1',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'e8:b2:65:7a:03:39',
                '400GBASE-DR4',
                'Fast Photonics',
                '170-0477-01',
                'VR',
                '2833812432900MC',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 2,
            "linkmon_switch_rx_bytes": 1356682,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 360484,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 43.7305,
            "linkmon_switch_opt_voltage": 3.2437,
            "linkmon_switch_opt_tx_current": { "1": 8.52,
            "2": 8.5,
            "3": 8.4,
            "4": 8.63,
            },"linkmon_switch_opt_tx_power": { "1": 2.6212,
            "2": 2.59,
            "3": 2.6853,
            "4": 2.8366,
            },"linkmon_switch_opt_rx_power": { "1": 2.8448,
            "2": 3.112,
            "3": 3.3672,
            "4": 2.4571,
            },"linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet224',
        'N/A',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'e8:b2:65:7a:03:39',
                '',
                '',
                '',
                '',
                '',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net001-lf-sw01',
        'Ethernet257',
        'N/A',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '10000',
                'e8:b2:65:7a:03:39',
                '',
                '',
                '',
                '',
                '',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    # Pass these fake args so that no SSH connections are made
    # - the other parse functions work the same way
    s = switch.SwitchSurvey([], {}, {}, False, False, "", "")
    s.switches = ["net001-lf-sw01"]
    s.switch_mgmt_ips["net001-lf-sw01"] = "123.4.5.6"
    s.stats["net001-lf-sw01"] = switch.SwitchInfo("net001-lf-sw01")
    s.hpe_hostnames = []
    s.arista_hostnames = []
    s.sonic_hostnames = ["net001-lf-sw01"]
    s.juniper_hostnames = []
    s.credo_hostnames = []

    with open("test-inputs/sonic-dell-4.2.1", "r") as f:
        s.parse("net001-lf-sw01", f.readlines())

    expected_switch_values = []
    compare_results(s, expected_switch_values, expected_values)


# Test the Dell SONiC switch output
def test_sonic_edgecore_parse():
    expected_values = []

    l = [
        'net002-lf-sw01',
        'Ethernet0',
        '',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'b4:6a:d4:df:37:e8',
                '400GBASE-DR4',
                'Intel Corp',
                'SPTSHP2PMCDF',
                '01',
                'CRDR23470ATSP',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 2,
            "linkmon_switch_rx_bytes": 59361170,
            "linkmon_switch_input_errors": 3,
            "linkmon_switch_rx_pfc": 1,
            "linkmon_switch_tx_pfc": 543,
            "linkmon_switch_tx_kbps": 2,
            "linkmon_switch_rx_kbps": 2,
            "linkmon_switch_extra_labels": 1,
        }
    )


    l = [
        'net002-lf-sw01',
        'Ethernet192',
        'L2_to_xs10048/Port_7',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                'b4:6a:d4:df:37:e8',
                '400GBASE-DR4',
                'JABIL',
                'QD4CS3LCCCR0PAM',
                '01',
                'RMDR24080288S',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 6,
            "linkmon_switch_rx_bytes": 23390232,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_tx_kbps": 1,
            "linkmon_switch_rx_kbps": 1,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net002-lf-sw01',
        'Ethernet256',
        'L2_to_cs307-net002-sx-sr01/eth400g5',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'b4:6a:d4:df:37:e8',
                '400GBASE-VR4',
                'Fast Photonics',
                '170-0477-01',
                '01',
                '2833812432900E3',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 4,
            "linkmon_switch_rx_bytes": 2086027,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net002-lf-sw01',
        'Ethernet398',
        'L2_to_cs307-net004-sx-sr03/eth100g3',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '100000',
                'b4:6a:d4:df:37:e8',
                '400GBASE-DR4',
                'JABIL',
                'QD4CS3LCCCR0PAM',
                '01',
                'RMDR240103C3S',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 8,
            "linkmon_switch_rx_bytes": 3515991,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net002-lf-sw01',
        'Ethernet504',
        '',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '400000',
                'b4:6a:d4:df:37:e8',
                '',
                '',
                '',
                '',
                '',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        'net002-lf-sw01',
        'Ethernet513',
        '',
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                '10000',
                'b4:6a:d4:df:37:e8',
                '',
                '',
                '',
                '',
                '',
                '123.4.5.6',
                '123.4.5.6',
                '',
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    # Pass these fake args so that no SSH connections are made
    # - the other parse functions work the same way
    s = switch.SwitchSurvey([], {}, {}, False, False, "", "")
    s.switches = ["net002-lf-sw01"]
    s.switch_mgmt_ips["net002-lf-sw01"] = "123.4.5.6"
    s.stats["net002-lf-sw01"] = switch.SwitchInfo("net002-lf-sw01")
    s.hpe_hostnames = []
    s.arista_hostnames = []
    s.sonic_hostnames = ["net002-lf-sw01"]
    s.juniper_hostnames = []
    s.credo_hostnames = []

    with open("test-inputs/sonic-edgecore-20231128", "r") as f:
        s.parse("net002-lf-sw01", f.readlines())

    expected_switch_values = []
    compare_results(s, expected_switch_values, expected_values)

# Test resolving mgmt IP address
def test_mgmt_ips_resolve():
    switches = ["localhost"]
    user_pass_dict = { "localhost": "a string" }

    mgmt_ips = None
    s = switch.SwitchSurvey(switches, {}, mgmt_ips, False, False, user_pass_dict, user_pass_dict)
    assert s.switch_mgmt_ips == {}, "Should not resolve IPs if mgmt_ips dict is None"

    mgmt_ips = {}
    s = switch.SwitchSurvey(switches, {}, mgmt_ips, False, False, user_pass_dict, user_pass_dict)
    assert len(s.switches) == 1
    assert len(s.switch_mgmt_ips) == 1
    assert "localhost" in s.switch_mgmt_ips
    assert s.switch_mgmt_ips["localhost"] == "127.0.0.1"

    mgmt_ips = {"arista-switch": "1.2.3.4"}
    s = switch.SwitchSurvey(["arista-switch"], {}, mgmt_ips, False, False, user_pass_dict, user_pass_dict)
    assert len(s.switch_mgmt_ips) == 1
    assert "arista-switch" in s.switch_mgmt_ips
    assert s.switch_mgmt_ips["arista-switch"] == "1.2.3.4", "Use specified IP. Don't resolve."
