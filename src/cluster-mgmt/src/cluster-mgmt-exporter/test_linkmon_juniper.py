"""
Switch Survey module for linkmon - parser tests for juniper
Invokes the switch parser function directly on the saved sample output files.
Compares the produced Prometheus metrics with the expected values.
"""

import switch
from test_linkmon_util import compare_results


# Test the JUNOS switch output
def test_juniper_parse():
    expected_values = []

    l = [
        "net006-lf-sw02",
        "et-0/0/0",
        "",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "800000",
                "6c:62:fe:a2:0f:18",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA927443",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 3,
            "linkmon_switch_rx_bytes": 283782128098083,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 3167433308,
            "linkmon_switch_fec_uncorrected_codewords": 14,
            "linkmon_switch_opt_temperature": 55,
            "linkmon_switch_opt_voltage": 3.308,
            "linkmon_switch_opt_tx_current": {
                "0": 60.431,
                "1": 60.431,
                "2": 63.178,
                "3": 58.198,
                "4": 60.613,
                "5": 61.931,
                "6": 54.644,
                "7": 56.916,
            },
            "linkmon_switch_opt_tx_power": {
                "0": 1.76,
                "1": 2.21,
                "2": 1.45,
                "3": 2.44,
                "4": 2.43,
                "5": 2.39,
                "6": 2.27,
                "7": 2.29,
            },
            "linkmon_switch_opt_rx_power": {
                "0": 3.08,
                "1": 2.57,
                "2": 2.44,
                "3": 2.20,
                "4": 2.53,
                "5": 3.46,
                "6": 3.91,
                "7": 3.94,
            },
            "linkmon_switch_rx_pfc": 3093607,
            "linkmon_switch_tx_pfc": 11545741,
            "linkmon_switch_in_discards": 1,
            "linkmon_switch_out_discards": 2,
            "linkmon_switch_tx_kbps": 2,
            "linkmon_switch_rx_kbps": 3,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/1",
        "",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "800000",
                "6c:62:fe:a2:0f:20",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA927441",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 3,
            "linkmon_switch_rx_bytes": 8509065927640,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 3016207007,
            "linkmon_switch_fec_uncorrected_codewords": 14,
            "linkmon_switch_opt_temperature": 57,
            "linkmon_switch_opt_voltage": 3.315,
            "linkmon_switch_opt_tx_current": {
                "0": 57.683,
                "1": 60.431,
                "2": 64.204,
                "3": 73.029,
                "4": 68.963,
                "5": 60.723,
                "6": 72.333,
                "7": 61.2,
            },
            "linkmon_switch_opt_tx_power": {
                "0": 2.43,
                "1": 2.14,
                "2": 1.84,
                "3": 1.77,
                "4": 1.81,
                "5": 2.40,
                "6": 1.71,
                "7": 2.19,
            },
            "linkmon_switch_opt_rx_power": {
                "0": 3.55,
                "1": 2.85,
                "2": 3.65,
                "3": 3.01,
                "4": 2.94,
                "5": 3.75,
                "6": 3.70,
                "7": 4.11,
            },
            "linkmon_switch_rx_pfc": 33090,
            "linkmon_switch_tx_pfc": 4175380,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 6,
            "linkmon_switch_rx_kbps": 2,
            "linkmon_switch_ecn_marked_packets_tc5": 123,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/16:0",
        "L2 to cs306-net006-ax-sr01/eth400g0",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "400000",
                "6c:62:fe:a2:0f:98",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA9273B5",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 3,
            "linkmon_switch_rx_bytes": 3354966352409,
            "linkmon_switch_input_errors": 2,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 265506,
            "linkmon_switch_fec_uncorrected_codewords": 7,
            "linkmon_switch_opt_temperature": 49,
            "linkmon_switch_opt_voltage": 3.308,
            "linkmon_switch_opt_tx_current": {
                "0": 63.178,
                "1": 63.909,
                "2": 61.931,
                "3": 60.431,
            },
            "linkmon_switch_opt_tx_power": {
                "0": 1.71,
                "1": 1.83,
                "2": 1.86,
                "3": 1.64,
            },
            "linkmon_switch_opt_rx_power": {
                "0": 2.11,
                "1": 2.42,
                "2": 1.77,
                "3": 1.24,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 3872876,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/16:1",
        "L2 to cs306-net006-ax-sr02/eth400g0",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "400000",
                "6c:62:fe:a2:0f:99",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA9273B5",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 3,
            "linkmon_switch_rx_bytes": 2694868788222,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 365982,
            "linkmon_switch_fec_uncorrected_codewords": 7,
            "linkmon_switch_opt_temperature": 49,
            "linkmon_switch_opt_voltage": 3.308,
            "linkmon_switch_opt_tx_current": {
                "4": 83.797,
                "5": 61.566,
                "6": 57.978,
                "7": 64.204,
            },
            "linkmon_switch_opt_tx_power": {
                "4": 1.68,
                "5": 1.47,
                "6": 2.22,
                "7": 1.56,
            },
            "linkmon_switch_opt_rx_power": {
                "4": 1.23,
                "5": 0.85,
                "6": 1.41,
                "7": 1.56,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 3172852,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 3,
            "linkmon_switch_rx_kbps": 4,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/32:0",
        "L2 to xs10161/Port_5",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:18",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA92734R",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 7,
            "linkmon_switch_rx_bytes": 6139282668726,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 9,
            "linkmon_switch_fec_corrected_codewords": 1526227,
            "linkmon_switch_fec_uncorrected_codewords": 66,
            "linkmon_switch_opt_temperature": 44,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {"0": 60.431},
            "linkmon_switch_opt_tx_power": {
                "0": 1.80,
            },
            "linkmon_switch_opt_rx_power": {
                "0": 5.19,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/32:1",
        "L2 to xs10156/Port_5",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:19",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA92734R",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 7,
            "linkmon_switch_rx_bytes": 7511336040324,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 17151178,
            "linkmon_switch_fec_uncorrected_codewords": 133,
            "linkmon_switch_opt_temperature": 44,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "1": 60.431,
            },
            "linkmon_switch_opt_tx_power": {
                "1": 1.93,
            },
            "linkmon_switch_opt_rx_power": {
                "1": 5.52,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/32:2",
        "L2 to xs10165/Port_5",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:1a",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA92734R",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 5,
            "linkmon_switch_rx_bytes": 4737607146179,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 2633271,
            "linkmon_switch_fec_uncorrected_codewords": 110,
            "linkmon_switch_opt_temperature": 44,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "2": 54.937,
            },
            "linkmon_switch_opt_tx_power": {
                "2": 2.09,
            },
            "linkmon_switch_opt_rx_power": {
                "2": 3.98,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 1,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/32:3",
        "L2 to xs10142/Port_5",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:1b",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA92734R",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 9,
            "linkmon_switch_rx_bytes": 9116716618948,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 21175157,
            "linkmon_switch_fec_uncorrected_codewords": 230,
            "linkmon_switch_opt_temperature": 44,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "3": 60.431,
            },
            "linkmon_switch_opt_tx_power": {
                "3": 2.36,
            },
            "linkmon_switch_opt_rx_power": {
                "3": 4.01,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/32:4",
        "L2 to xs10161/Port_6",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:1c",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA92734R",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 11,
            "linkmon_switch_rx_bytes": 6147703760430,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 4,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 11653699,
            "linkmon_switch_fec_uncorrected_codewords": 98,
            "linkmon_switch_opt_temperature": 44,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "4": 62.591,
            },
            "linkmon_switch_opt_tx_power": {
                "4": 2.10,
            },
            "linkmon_switch_opt_rx_power": {
                "4": 3.00,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/32:5",
        "L2 to xs10156/Port_6",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:1d",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA92734R",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 11,
            "linkmon_switch_rx_bytes": 7568831033509,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 15190064,
            "linkmon_switch_fec_uncorrected_codewords": 141,
            "linkmon_switch_opt_temperature": 44,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "5": 61.237,
            },
            "linkmon_switch_opt_tx_power": {
                "5": 1.96,
            },
            "linkmon_switch_opt_rx_power": {
                "5": 3.76,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/32:6",
        "L2 to xs10165/Port_6",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:1e",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA92734R",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 3,
            "linkmon_switch_rx_bytes": 4765350921242,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1599119,
            "linkmon_switch_fec_uncorrected_codewords": 44,
            "linkmon_switch_opt_temperature": 44,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "6": 59.296,
            },
            "linkmon_switch_opt_tx_power": {
                "6": 2.09,
            },
            "linkmon_switch_opt_rx_power": {
                "6": 4.05,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/32:7",
        "L2 to xs10142/Port_6",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:1f",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA92734R",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 21,
            "linkmon_switch_rx_bytes": 9177791629213,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 1036246,
            "linkmon_switch_fec_uncorrected_codewords": 323,
            "linkmon_switch_opt_temperature": 44,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "7": 62.335,
            },
            "linkmon_switch_opt_tx_power": {
                "7": 1.88,
            },
            "linkmon_switch_opt_rx_power": {
                "7": 3.30,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/40:0",
        "L2 to cs306-net006-sx-sr06/eth100g0",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:58",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA927466",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 18432931027816,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 8307692,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 45,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "0": 60.431,
            },
            "linkmon_switch_opt_tx_power": {
                "0": 2.48,
            },
            "linkmon_switch_opt_rx_power": {
                "0": 2.33,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 58310556,
            "linkmon_switch_in_discards": 118838567,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/40:1",
        "L2 to cs306-net006-sx-sr06/eth100g1",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:59",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA927466",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 20626858272251,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 149077215,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 45,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "1": 60.431,
            },
            "linkmon_switch_opt_tx_power": {
                "1": 2.12,
            },
            "linkmon_switch_opt_rx_power": {
                "1": 1.26,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 58195996,
            "linkmon_switch_in_discards": 118162806,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/40:2",
        "L2 to cs306-net006-sx-sr06/eth100g2",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:5a",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA927466",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 9434410,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 27205098,
            "linkmon_switch_fec_uncorrected_codewords": 1,
            "linkmon_switch_opt_temperature": 45,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "2": 60.431,
            },
            "linkmon_switch_opt_tx_power": {
                "2": 2.46,
            },
            "linkmon_switch_opt_rx_power": {
                "2": 2.64,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/40:3",
        "L2 to cs306-net006-sx-sr06/eth100g3",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:5b",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA927466",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 1,
            "linkmon_switch_carrier_changes": 1,
            "linkmon_switch_rx_bytes": 9434258,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 5878378,
            "linkmon_switch_fec_uncorrected_codewords": 1,
            "linkmon_switch_opt_temperature": 45,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "3": 54.937,
            },
            "linkmon_switch_opt_tx_power": {
                "3": 2.19,
            },
            "linkmon_switch_opt_rx_power": {
                "3": 2.98,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/40:4",
        "",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:5c",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA927466",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 45,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "4": 58.819,
            },
            "linkmon_switch_opt_tx_power": {
                "4": 2.20,
            },
            "linkmon_switch_opt_rx_power": {
                "4": -40.0,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/40:5",
        "",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "100000",
                "6c:62:fe:a2:10:5d",
                "800G-DR8",
                "JUNIPER-1A1",
                "740-174933",
                "REV 01",
                "1A1MVWA927466",
                "",
                "",
                ""
            ],
            "linkmon_switch_oper_status": 0,
            "linkmon_switch_carrier_changes": 0,
            "linkmon_switch_rx_bytes": 0,
            "linkmon_switch_input_errors": 0,
            "linkmon_switch_runts": 0,
            "linkmon_switch_crc_errors": 0,
            "linkmon_switch_fec_corrected_codewords": 0,
            "linkmon_switch_fec_uncorrected_codewords": 0,
            "linkmon_switch_opt_temperature": 45,
            "linkmon_switch_opt_voltage": 3.345,
            "linkmon_switch_opt_tx_current": {
                "5": 61.273,
            },
            "linkmon_switch_opt_tx_power": {
                "5": 2.12,
            },
            "linkmon_switch_opt_rx_power": {
                "5": -40,
            },
            "linkmon_switch_rx_pfc": 0,
            "linkmon_switch_tx_pfc": 0,
            "linkmon_switch_in_discards": 0,
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    l = [
        "net006-lf-sw02",
        "et-0/0/65",
        "",
    ]
    expected_values.append(
        {
            "labels": l,
            "extended_labels": l
            + [
                '',
                '',
                "25000",
                "6c:62:fe:a2:11:20",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                ""
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
            "linkmon_switch_out_discards": 0,
            "linkmon_switch_tx_kbps": 0,
            "linkmon_switch_rx_kbps": 0,
            "linkmon_switch_ecn_marked_packets_tc5": 0,
            "linkmon_switch_extra_labels": 1,
        }
    )

    # Pass these fake args so that no SSH connections are made
    # - the other parse functions work the same way
    s = switch.SwitchSurvey([], {}, {}, False, False, "", "")
    s.switches = ["net006-lf-sw02"]
    s.stats["net006-lf-sw02"] = switch.SwitchInfo("net006-lf-sw02")
    s.hpe_hostnames = []
    s.arista_hostnames = []
    s.sonic_hostnames = []
    s.juniper_hostnames = ["net006-lf-sw02"]
    s.credo_hostnames = []

    with open("test-inputs/juniper", "r") as f:
        s.parse("net006-lf-sw02", f.readlines())

    expected_switch_values = []
    compare_results(s, expected_switch_values, expected_values)
