import json

from deployment_manager.network_config.tasks.switch import SwitchUploadConfigTask


def test_ports_for(tmp_path):
    """
    test for ports_for parsing.
    """
    target_switch_name = "sp0"
    target_switch_type = "SP"
    target_switch_vendor = "arista"

    def _generate_target_switch_obj():
        return {
            "name": target_switch_name,
            "username": "admin",
            "password": "CerebrasColo",
            "tier": target_switch_type,
            "tier_pos": 0,
            "vendor": target_switch_vendor
        }

    def _generate_network_config(switch_name: str):
        cfg = {
            "interfaces": [
                {
                    "name": "p0",
                    "switch_name": "lf0",
                    "sections": [
                        "lf0"
                    ]
                },
                {
                    "name": "p1",
                    "switch_name": "lf1",
                    "sections": [
                        "lf1"
                    ]
                },
                {
                    "name": "p2",
                    "switch_name": "lf2",
                    "sections": [
                        "lf2"
                    ]
                },
            ],
            "final_cmd": "done"
        }
        out_file = tmp_path / "switches" / switch_name / "l3.json"
        out_file.parent.mkdir(parents=True)
        with open(out_file, 'w') as output_fd:
            output_fd.write(json.dumps(cfg, indent=2))

    target_switch_obj = _generate_target_switch_obj()
    _generate_network_config(target_switch_name)

    cmds = SwitchUploadConfigTask(target_switch_obj, "", "", tmp_path,
                                  "switches", ["sp0:lf1", "lf0:sp0", "sp1:lf2"]).get_cmd()
    assert "lf0" in cmds
    assert "lf1" in cmds
    assert "lf2" not in cmds

    non_target = _generate_target_switch_obj()
    non_target["name"] = "lf3"
    _generate_network_config(non_target["name"])
    cmds = SwitchUploadConfigTask(non_target, "", "", tmp_path,
                                  "switches", ["sp0:lf1", "lf0:sp0", "sp1:lf2"]).get_cmd()
    assert "lf0" not in cmds
    assert "lf1" not in cmds
    assert "lf2" not in cmds
