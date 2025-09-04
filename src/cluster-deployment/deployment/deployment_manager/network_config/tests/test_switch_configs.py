import json

from deployment_manager.network_config.configs.switch import SwitchDcqcnConfigWriter
from deployment_manager.network_config.tests.network_builder import LeafSpineNetworkBuilder


def test_dcqcn_config():
    switch_name = "sc-r14rb5-400gsw32-m-leaf"
    network_config = {
        "management_nodes": [
            {
              "interfaces": [
                {
                  "name": "ens2f0np0",
                  "switch_name": "sc-r14rb5-400gsw32-m-leaf",
                  "switch_port": "Ethernet22/1"
                }
              ],
              "name": "cs303-wse001-mg-sr01"
            }
        ],
        "switches": [
            {
                "name": "sc-r14rb5-400gsw32-m-leaf",
                "vendor": "arista",
                "model": "7060DX5",
                "tier": "LF"
            },
            {
                "name": "sc-r14rb5-spine",
                "vendor": "arista",
                "model": "7808",
                "tier": "SP"
            }
        ],
        "xconnect": {
            "connections": [
              {
                "links": [
                  {
                    "name": "sc-r14rb5-spine",
                    "port": "Ethernet17/1"
                  },
                  {
                    "name": "sc-r14rb5-400gsw32-m-leaf",
                    "port": "Ethernet1/1"
                  }
                ],
                "name": "L3 sc-r14rb5-spine and sc-r14rb5-400gsw32-m-leaf",
                "prefix": "10.250.228.158/31"
              }
            ]
        }
    }

    # DCQCN enable
    expected_config = """!
queue-monitor length
!

!
! Enable DCQCN for Ethernet22/1
!
interface Ethernet22/1
    uc-tx-queue 5
    random-detect ecn minimum-threshold 222 kbytes maximum-threshold 2000 kbytes max-mark-probability 1 weight 0
    random-detect ecn count

!
! Enable DCQCN for Ethernet1/1
!
interface Ethernet1/1
    uc-tx-queue 5
    random-detect ecn minimum-threshold 222 kbytes maximum-threshold 2000 kbytes max-mark-probability 1 weight 0
    random-detect ecn count

!
end
"""
    cfg_writer = SwitchDcqcnConfigWriter(network_config, "sc-r14rb5-400gsw32-m-leaf")
    output = cfg_writer.output_config()
    assert output == expected_config, f"Generated output is not expected: {output}"

    # DCQCN enable with custom parameters
    expected_config = """!
queue-monitor length
!

!
! Enable DCQCN for Ethernet22/1
!
interface Ethernet22/1
    uc-tx-queue 5
    random-detect ecn minimum-threshold 10 kbytes maximum-threshold 2000 kbytes max-mark-probability 20 weight 0
    random-detect ecn count

!
! Enable DCQCN for Ethernet1/1
!
interface Ethernet1/1
    uc-tx-queue 5
    random-detect ecn minimum-threshold 10 kbytes maximum-threshold 2000 kbytes max-mark-probability 20 weight 0
    random-detect ecn count

!
end
"""
    cfg_writer = SwitchDcqcnConfigWriter(
                    network_config, "sc-r14rb5-400gsw32-m-leaf",
                    ecn_probability=20, ecn_min_threshold=10
                )
    output = cfg_writer.output_config()
    assert output == expected_config, f"Generated output is not expected: {output}"

    # DCQCN enable for Arista J3AI spine
    expected_config = """!
queue-monitor length
!

!
! Enable DCQCN for Ethernet17/1
!
interface Ethernet17/1
    random-detect ecn delay threshold 3 milliseconds
    random-detect ecn count

!
end
"""
    cfg_writer = SwitchDcqcnConfigWriter(
                    network_config, "sc-r14rb5-spine"
                )
    output = cfg_writer.output_config()
    assert output == expected_config, f"Generated output is not expected: {output}"

    # DCQCN disable
    expected_config = """!
no queue-monitor length
!

!
! Disable DCQCN for Ethernet22/1
!
interface Ethernet22/1
    uc-tx-queue 5
    no random-detect ecn minimum-threshold
!

!
! Disable DCQCN for Ethernet1/1
!
interface Ethernet1/1
    uc-tx-queue 5
    no random-detect ecn minimum-threshold
!

!
end
"""
    cfg_writer = SwitchDcqcnConfigWriter(
                    network_config, "sc-r14rb5-400gsw32-m-leaf",
                    enable=False
                 )
    output = cfg_writer.output_config()
    assert output == expected_config, f"Generated output is not expected: {output}"


def test_mmu_profile_override(tmp_path):
    switch_base = {"vendor": "arista", "username": "admin", "tier_pos": 0}
    b = LeafSpineNetworkBuilder(1, switch_base={**switch_base, "model": "7808",})
    b.add_mleaf(0,0,1,0, model="7060X6", **switch_base)
    b.network["environment"]["cluster_prefix"] = "10.0.0.0/21"
    b.call_allocate_tiers(tmp_path, aw_size=256, br_size=64, system_prefix_size=128, vip_count=16, aw_physical=196)
    b.call_placer(tmp_path)
    sw = {s["name"]: s for s in b.network["switches"]}
    lf, sp = "", ""
    for xconn in b.network["xconnect"]["connections"]:
        for link in xconn["links"]:
            i = sw[link["name"]].get("ports", [])
            i.append({"name": link["port"], "speed": 800 * (10 ** 9)})
            sw[link["name"]]["ports"] = i
            if "leaf" in link["name"]:
                lf = link["name"]
            else:
                sp = link["name"]
    b.call_generate_cfg(tmp_path)
    with open(f"{tmp_path}/config/switches/{lf}/l3_config.json", 'r') as f:
        doc = json.loads(f.read())

    assert "platform trident mmu headroom-pool limit cells 24000" in doc["base"]
    for interface in doc.get("interfaces", []):
        if interface.get("switch_name", "") == sp:
            assert "mmu-profile-800g-jericho" in "".join(interface["sections"]), "mmu profile not found"
            break
    else:
        assert False, f"spine {sp} not found in interface configuration"
