import os

import switch
from prometheus_client.parser import text_string_to_metric_families


def test_credo():
    expected_results = None
    with open(os.path.join(os.path.dirname(__file__), "test-outputs/linkmon/credo")) as f:
        expected_results = list(text_string_to_metric_families(f.read()))

    with open(os.path.join(os.path.dirname(__file__), "test-inputs/linkmon/credo")) as f:
        test_input = f.readlines()

    s = switch.SwitchSurvey([], {}, {}, False, False, "", "")
    sname = "wse007-mc-sw02"
    s.switches = [sname]
    s.switch_mgmt_ips[sname] = "172.19.136.169"
    s.stats[sname] = switch.SwitchInfo(sname)
    s.hpe_hostnames = []
    s.arista_hostnames = []
    s.sonic_hostnames = []
    s.juniper_hostnames = []
    s.credo_hostnames = [sname]

    s.parse(sname, test_input)
    assert expected_results == list(s.prom_get())
