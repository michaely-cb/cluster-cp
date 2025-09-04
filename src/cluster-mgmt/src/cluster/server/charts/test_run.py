import gzip
import json
import pytest
import os
from run import (
    set_test_mode,
    run,
    get_cmd,
    startup_probe,
    liveness_probe,
    load_cluster_details_from_config_map,
    check_fabrics,
    find_mismatches
)


# test cmd generation
def test_get_cmd():
    set_test_mode("br")
    cmd, _ = get_cmd()
    assert "--config_file=br-test.json" in cmd


# test subprocess exit code will be propagated to main process
def test_exit_code_propagation():
    set_test_mode()
    os.environ.update({'TEST_CMD': 'sleep 1; exit 13'})
    with pytest.raises(SystemExit) as e:
        run()
    assert e.type == SystemExit
    assert e.value.code == 13
    assert os.path.exists(f"{os.getcwd()}/.access-marker.out")
    if os.path.isfile("svc_up"):
        os.remove("svc_up")


# test load br/cd config
def test_load_config():
    set_test_mode()
    update_signal = 'test_cluster_details_updated'
    cm_json = 'test_cluster_details'
    cm_json_gc = 'test_cluster_details_gz'

    # test load json
    os.environ.update({'WS_CLUSTER_DETAILS_UPDATE_SIGNAL': update_signal})
    os.environ.update({'WS_CLUSTER_DETAILS_FP': cm_json})
    with open(update_signal, "w") as fp:
        fp.write("updated")
    with open(cm_json, "w") as fp:
        fp.write('{"format":"json"}')
    config = load_cluster_details_from_config_map()
    assert config['format'] == 'json', config

    # test load json + gzip
    os.environ.update({'WS_CLUSTER_DETAILS_FP': cm_json_gc})
    with gzip.open(cm_json_gc, "wb") as fp:
        fp.write(b'{"format":"json+gzip"}')
    config = load_cluster_details_from_config_map()
    assert config['format'] == 'json+gzip', config

    if os.path.isfile(update_signal):
        os.remove(update_signal)
    if os.path.isfile(cm_json):
        os.remove(cm_json)
    if os.path.isfile(cm_json_gc):
        os.remove(cm_json_gc)


def test_startup_probe():
    set_test_mode()
    with open("svc_up", "w") as fp:
        fp.write("127.0.0.1:9000")
    startup_probe()
    if os.path.isfile("svc_up"):
        os.remove("svc_up")


def test_startup_probe_fail():
    if os.path.isfile("svc_up"):
        os.remove("svc_up")
    with pytest.raises(SystemExit) as e:
        startup_probe()
    assert e.value.code == 1


def test_liveness_probe_success():
    with open("svc_up", "w") as fp:
        fp.write("127.0.0.1:9000")
    liveness_probe()
    if os.path.isfile("svc_up"):
        os.remove("svc_up")


def test_liveness_probe_fail():
    with open("svc_up", "w") as fp:
        fp.write("fake:9000")
    with pytest.raises(SystemExit) as e:
        liveness_probe()
    assert e.value.code != 0
    if os.path.isfile("svc_up"):
        os.remove("svc_up")


def test_check_fabric_misform_cmaddr():
    os.environ["OK_SYSTEMS_IN_NAMESPACE"] = "system1+~cmaddr1,system2"
    with pytest.raises(SystemExit) as e:
        check_fabrics()
    assert e.value.code != 0


def test_check_fabric_match():
    os.environ["OK_SYSTEMS_IN_NAMESPACE"] = "system1+~cmaddr1:0,system2+~cmaddr1:0"
    check_fabrics()

def test_check_fabric_no_systems():
    os.environ["OK_SYSTEMS_IN_NAMESPACE"] = ""
    check_fabrics()

def test_check_fabric_mismatch():
    os.environ["OK_SYSTEMS_IN_NAMESPACE"] = "system1+~cmaddr1:0,system2+~cmaddr2:0"
    with pytest.raises(SystemExit) as e:
        check_fabrics()
    assert e.value.code != 0

def test_check_fabric_20_systems_match():
    ok_systems="system0+~cmaddr0:0"
    for i in range(19):
        ok_systems = f"{ok_systems},system{i+1}+~cmaddr0:0"
    os.environ["OK_SYSTEMS_IN_NAMESPACE"] = ok_systems
    check_fabrics()

def test_check_fabric_20_systems_mismatch():
    ok_systems="system0+~cmaddr0:0"
    for i in range(19):
        ok_systems = f"{ok_systems},system{i+1}+~cmaddr{i+1}:0"
    os.environ["OK_SYSTEMS_IN_NAMESPACE"] = ok_systems
    with pytest.raises(SystemExit) as e:
        check_fabrics()
    assert e.value.code != 0

def test_find_mismatches():
    pes_data = [
        {"channel": 0, "domain": 3, "pos": {"x": 0, "y": 1}},
        {"channel": 1, "domain": 3, "pos": {"x": 0, "y": 19}},
        {"channel": 28, "domain": 2, "pos": {"x": 0, "y": 1063}}
    ]
    sample_unique_fabrics = {
        f'{{"architecture": "sdr", "core_frequency": 750000000.0, "ingress_pes": {json.dumps(pes_data)}}}': ['xs10012', 'xs10018'],
        f'{{"architecture": "sdr", "core_frequency": 1199776785.7142859, "egress_pes": {json.dumps(pes_data)}}}': ['xs20023'],
        '{"architecture": "arm", "core_frequency": 750000000.0}': ['xs20037']
    }
    expected_output = {
        'architecture': {'"sdr"': ['xs10012', 'xs10018', 'xs20023'], '"arm"': ['xs20037']},
        'core_frequency': {'750000000.0': ['xs10012', 'xs10018', 'xs20037'], '1199776785.7142859': ['xs20023']},
        'egress_pes': 'MISMATCH DETECTED for egress_pes, check previous loglines for detailed content',
        'ingress_pes': 'MISMATCH DETECTED for ingress_pes, check previous loglines for detailed content'
    }
    mismatches = find_mismatches(sample_unique_fabrics)
    assert mismatches == expected_output