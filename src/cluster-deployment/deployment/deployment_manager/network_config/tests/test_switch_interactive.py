import pytest

from deployment_manager.network_config.tasks.switch import SwitchLLDPTask

def switch_lldp(network_config, target_switch_obj):
    network_config["switches"].append(target_switch_obj)
    st = SwitchLLDPTask(
        network_config, target_switch_obj,
        target_switch_obj["username"], target_switch_obj["password"]
    )

    result = st()
    st.post(result)

    assert len(network_config.get("xconnect", {}).get("connections", [])) > 0, "Cross-connects not detected"

    # Uncomment below if system is expected to be connected to the test switch
    # assert len(network_config.get("system_connections", [])) > 0, "System connections not detected"

@pytest.mark.skip
def test_interactive_tasks():
    """
    This test is intended to be run by developer as a way to validate
    some common switch tasks that involve gathering information from
    switches. It can be used to validate a new switch vendor/model
    without the need for a full network_config.json or cluster

    target_switch_obj and network_config below can be modified to
    suit dev-specific tests.
    """
    target_switch_name = "sc-r14rb5-400gsw64-s-leaf"
    target_switch_type = "LF"
    target_switch_vendor = "arista"
    connected_switches = [
        "sc-r14rb5-400gsw32-spine"
    ]
    connected_systems = []

    def _generate_target_switch_obj():
        return {
            "name": target_switch_name,
            "username": "admin",
            "password": "CerebrasColo",
            "tier": target_switch_type,
            "tier_pos": 0,
            "vendor": target_switch_vendor
        }

    def _generate_network_config(target_switch):
        cfg = {
            "switches": [
            ],
            "systems": []
        }
        for s in connected_switches:
            cfg["switches"].append({
                "name": s
            })
        for s in connected_systems:
            cfg["systems"].append({
                "name": s
            })
        return cfg

    target_switch_obj = _generate_target_switch_obj()
    network_config = _generate_network_config(target_switch_obj)

    tests = [
        switch_lldp,
    ]

    for t in tests:
        t(network_config, target_switch_obj)
