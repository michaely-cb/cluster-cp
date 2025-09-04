from unittest.mock import Mock

from deployment_manager.network_config.tasks.switch import SwitchPingCheckTask
from deployment_manager.network_config.tasks.utils import PingTest

def test_hpe_switch_ping():
    """ Tests ping results parser for HPE HPE_MODEL_256 ie. 12908 switch
    """
    switch_name = "test-switch"
    HPE_MODEL_256 = "12908"
    switch = {"name": switch_name, "vendor" : "hpe", "model" : HPE_MODEL_256}
    task = SwitchPingCheckTask(switch, "", "", {}, [])

    def make_ping_targets() -> dict:
        return {
            "xconnects": [PingTest(switch_name, "Eth1/1", "", "sw1", "Eth2/1", "198.19.144.115")],
            "nodes": [PingTest(switch_name, "Eth20/1", "", "node0", "eth100g0", "198.19.144.116")],
            "systems": [PingTest(switch_name, "Eth40/1", "", "sys0", "Port 1", "198.19.144.117")],
        }

    successful_output = (
        """
--- Ping statistics for 198.19.144.115 in VPN instance cs1data ---
1 packet(s) transmitted, 1 packet(s) received, 0.0% packet loss
round-trip min/avg/max/std-dev = 0.730/0.730/0.730/0.000 ms
[swx01-100gsw-mhp]ping -vpn-instance cs1data -c 1 198.19.144.116
Ping 198.19.144.116 (198.19.144.116): 56 data bytes, press CTRL_C to break
56 bytes from 198.19.144.116: icmp_seq=0 ttl=255 time=0.871 ms

--- Ping statistics for 198.19.144.116 in VPN instance cs1data ---
1 packet(s) transmitted, 1 packet(s) received, 0.0% packet loss
round-trip min/avg/max/std-dev = 0.871/0.871/0.871/0.000 ms
[swx01-100gsw-mhp]ping -vpn-instance cs1data -c 1 198.19.144.117
Ping 198.19.144.117 (198.19.144.117): 56 data bytes, press CTRL_C to break
56 bytes from 198.19.144.117: icmp_seq=0 ttl=64 time=0.670 ms

--- Ping statistics for 198.19.144.117 in VPN instance cs1data ---
1 packet(s) transmitted, 1 packet(s) received, 0.0% packet loss
round-trip min/avg/max/std-dev = 0.670/0.670/0.670/0.000 ms
[swx01-100gsw-mhp]ping -vpn-instance cs1data -c 1 198.19.144.118
Ping 198.19.144.118 (198.19.144.118): 56 data bytes, press CTRL_C to break
56 bytes from 198.19.144.118: icmp_seq=0 ttl=255 time=0.752 ms
    """
    )
    ret = task.post((switch, make_ping_targets(), successful_output))
    assert ret[0], "Incorrect ping failure detected"

    failure_output = (
        """
--- Ping statistics for 198.19.144.115 in VPN instance cs1data ---
1 packet(s) transmitted, 1 packet(s) received, 0.0% packet loss
round-trip min/avg/max/std-dev = 0.730/0.730/0.730/0.000 ms
[swx01-100gsw-mhp]ping -vpn-instance cs1data -c 1 198.19.144.116
Ping 198.19.144.116 (198.19.144.116): 56 data bytes, press CTRL_C to break

--- Ping statistics for 198.19.144.116 in VPN instance cs1data ---
1 packet(s) transmitted, 1 packet(s) received, 100.0% packet loss
round-trip min/avg/max/std-dev = 0.871/0.871/0.871/0.000 ms
[swx01-100gsw-mhp]ping -vpn-instance cs1data -c 1 198.19.144.117
Ping 198.19.144.117 (198.19.144.117): 56 data bytes, press CTRL_C to break
56 bytes from 198.19.144.117: icmp_seq=0 ttl=64 time=0.670 ms

--- Ping statistics for 198.19.144.117 in VPN instance cs1data ---
1 packet(s) transmitted, 1 packet(s) received, 0.0% packet loss
round-trip min/avg/max/std-dev = 0.670/0.670/0.670/0.000 ms
[swx01-100gsw-mhp]ping -vpn-instance cs1data -c 1 198.19.144.118
Ping 198.19.144.118 (198.19.144.118): 56 data bytes, press CTRL_C to break
56 bytes from 198.19.144.118: icmp_seq=0 ttl=255 time=0.752 ms
    """
    )
    ret = task.post((switch, make_ping_targets(), failure_output))
    assert not ret[0], "Invalid target ping success detected"

    failure_output = (
        """
--- Ping statistics for 198.19.144.115 in VPN instance cs1data ---
1 packet(s) transmitted, 1 packet(s) received, 0.0% packet loss
round-trip min/avg/max/std-dev = 0.730/0.730/0.730/0.000 ms
[swx01-100gsw-mhp]ping -vpn-instance cs1data -c 1 198.19.144.116
Ping 198.19.144.116 (198.19.144.116): 56 data bytes, press CTRL_C to break

--- Ping statistics for 198.19.144.116 in VPN instance cs1data ---
1 packet(s) transmitted, 1 packet(s) received, 100.0% packet loss
round-trip min/avg/max/std-dev = 0.871/0.871/0.871/0.000 ms
[swx01-100gsw-mhp]ping -vpn-instance cs1data -A 1 198.19.144.117
ping: connect: Invalid argument
[swx01-100gsw-mhp]ping -vpn-instance cs1data -c 1 198.19.144.118
Ping 198.19.144.118 (198.19.144.118): 56 data bytes, press CTRL_C to break
56 bytes from 198.19.144.118: icmp_seq=0 ttl=255 time=0.752 ms
    """
    )
    ret = task.post((switch, make_ping_targets(), failure_output))
    assert not ret[0], "Incorrect ping success detected"

def test_switch_ping():
    """ Tests ping results parser
    """
    switch_name = "test-switch"
    switch = {"name": switch_name}
    task = SwitchPingCheckTask(switch, "", "", {}, [])

    def make_ping_targets() -> dict:
        return {
            "xconnects": [PingTest(switch_name, "Eth1/1", "", "sw1", "Eth2/1", "8.8.8.8")],
            "nodes": [PingTest(switch_name, "Eth20/1", "", "node0", "eth100g0", "1.2.3.4")],
            "systems": [PingTest(switch_name, "Eth40/1", "", "sys0", "Port 1", "10.1.2.5")],
        }

    successful_output = (
        """
sc-r14rb5-400gsw64-s-leaf#ping vrf cs1data 8.8.8.8 repeat 1
PING 8.8.8.8 (8.8.8.8) 72(100) bytes of data.
76 bytes from 8.8.8.8: icmp_seq=1 ttl=105 (truncated)

--- 8.8.8.8 ping statistics ---
1 packets transmitted, 1 received, 0% packet loss, time 0ms
rtt min/avg/max/mdev = 22.768/22.768/22.768/0.000 ms
sc-r14rb5-400gsw64-s-leaf#ping vrf cs1data 1.2.3.4 repeat 1
PING 1.2.3.4 (1.2.3.4) 72(100) bytes of data.
76 bytes from 1.2.3.4: icmp_seq=1 ttl=105 (truncated)

--- 1.2.3.4 ping statistics ---
1 packets transmitted, 1 received, 0% packet loss, time 0ms
rtt min/avg/max/mdev = 22.761/22.761/22.761/0.000 ms
sc-r14rb5-400gsw64-s-leaf#ping vrf cs1data 10.1.2.5 repeat 1
PING 10.1.2.5 (10.1.2.5) 72(100) bytes of data.
76 bytes from 10.1.2.5: icmp_seq=1 ttl=105 (truncated)

--- 10.1.2.5 ping statistics ---
1 packets transmitted, 1 received, 0% packet loss, time 0ms
rtt min/avg/max/mdev = 22.793/22.793/22.793/0.000 ms
    """
    )
    ret = task.post((switch, make_ping_targets(), successful_output))
    assert ret[0], "Incorrect ping failure detected"

    failure_output = (
        """
sc-r14rb5-400gsw64-s-leaf#ping vrf cs1data 8.8.8.8 repeat 1
PING 8.8.8.8 (8.8.8.8) 72(100) bytes of data.
76 bytes from 8.8.8.8: icmp_seq=1 ttl=105 (truncated)

--- 8.8.8.8 ping statistics ---
1 packets transmitted, 1 received, 0% packet loss, time 0ms
rtt min/avg/max/mdev = 22.793/22.793/22.793/0.000 ms
sc-r14rb5-400gsw64-s-leaf#ping vrf cs1data 1.2.3.4 repeat 1
PING 1.2.3.4 (1.2.3.4) 72(100) bytes of data.

--- 1.2.3.4 ping statistics ---
1 packets transmitted, 0 received, 100% packet loss, time 0ms

sc-r14rb5-400gsw64-s-leaf#ping vrf cs1data 1.2.3.4 repeat 1
PING 10.1.2.5 (10.1.2.5) 72(100) bytes of data.
76 bytes from 10.1.2.5: icmp_seq=1 ttl=105 (truncated)

--- 10.1.2.5 ping statistics ---
1 packets transmitted, 1 received, 0% packet loss, time 0ms
rtt min/avg/max/mdev = 22.744/22.744/22.744/0.000 ms
    """
    )
    ret = task.post((switch, make_ping_targets(), failure_output))
    assert not ret[0], "Invalid target ping success detected"

    failure_output = (
        """
sc-r14rb5-400gsw64-s-leaf#ping vrf cs1data 8.8.8.8 repeat 1
PING 8.8.8.8 (8.8.8.8) 72(100) bytes of data.
76 bytes from 8.8.8.8: icmp_seq=1 ttl=105 (truncated)

--- 8.8.8.8 ping statistics ---
1 packets transmitted, 1 received, 0% packet loss, time 0ms
rtt min/avg/max/mdev = 22.793/22.793/22.793/0.000 ms
sc-r14rb5-400gsw64-s-leaf#ping vrf cs1data 4.4.4.4 repeat 1
ping: connect: Invalid argument
sc-r14rb5-400gsw64-s-leaf#ping vrf cs1data 1.2.3.4 repeat 1
PING 1.2.3.4 (1.2.3.4) 72(100) bytes of data.
76 bytes from 1.2.3.4: icmp_seq=1 ttl=105 (truncated)

--- 1.2.3.4 ping statistics ---
1 packets transmitted, 1 received, 0% packet loss, time 0ms
rtt min/avg/max/mdev = 22.744/22.744/22.744/0.000 ms
    """
    )
    ret = task.post((switch, make_ping_targets(), failure_output))
    assert not ret[0], "Incorrect ping success detected"

