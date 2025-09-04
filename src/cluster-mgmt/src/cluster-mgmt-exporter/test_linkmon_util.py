"""
Switch Survey module for linkmon - utils for parser tests
"""
import re

import switch

# common functions
def compare_results(survey, expected_switch_values, expected_port_values):
    suf_re = re.compile(r'linkmon_switch_(.*)')
    exlbl, port_metrics, switch_metrics, scrape_duration = switch.SwitchSurvey.create_metrics()

    scrape_duration.add_metric([], 0)
    for swval in expected_switch_values:
        labels = swval["labels"]
        for k, v in swval.items():
            if k == "labels":
                continue
            suffix = suf_re.match(k)
            switch_metrics[suffix.group(1)].add_metric(labels, v)
    for port in expected_port_values:
        labels = port["labels"]
        ext_labels = port["extended_labels"]

        exlbl.add_metric(ext_labels, 1)
        for k, v in port.items():
            if k.startswith("linkmon_switch_"):
                if k == "linkmon_switch_extra_labels":
                    continue
                suffix = suf_re.match(k).group(1)
                if type(v) == dict:
                    for lane_k, lane_v in v.items():
                        port_metrics[suffix].add_metric(labels + [lane_k], lane_v)
                else:
                    extra_labels = port.get(f'{suffix}_extra_labels', [])
                    port_metrics[suffix].add_metric(labels + extra_labels, v)

    exp_tuple = (exlbl,) + tuple(port_metrics.values()) + tuple(switch_metrics.values()) + (scrape_duration,)
    test_tuple = survey.prom_get()
    for idx, _ in enumerate(exp_tuple):
        assert exp_tuple[idx] == next(test_tuple)
