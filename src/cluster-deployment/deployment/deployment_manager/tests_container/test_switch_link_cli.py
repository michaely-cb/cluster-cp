import pytest
from django.test.testcases import TestCase

from deployment_manager.cli.switch.link.lldp import (
    perform_matching, LINK_STATE_MATCH, LINK_STATE_MISMATCH,
    LINK_STATE_MISSING
)
from deployment_manager.db.const import DeploymentDeviceTypes, DeploymentDeviceRoles
from deployment_manager.db.models import Link, DeploymentProfile, Device
from deployment_manager.tools.switch.interface import LldpNeighbor


@pytest.mark.skip
class DbTest(TestCase):
    """
    Django models tests.
    """

    def setUp(self):
        dp = DeploymentProfile.create("test", "test", True)
        d_lf = Device.add("sw1", DeploymentDeviceTypes.SWITCH, DeploymentDeviceRoles.LEAF, dp)

        def gen_link(src_if, dst_name, dst_if):
            l = Link(src_device=d_lf, src_if=src_if, dst_name=dst_name, dst_device=None, dst_if=dst_if, speed="100")
            l.save()
            return l

        self.links = [
            gen_link("Eth1/1", "device_a", "ifA"),
            gen_link("Eth2/1", "device_b", "ifB"),
            gen_link("Eth3/1", "device_c", "ifC"),
            gen_link("Eth4/1", "device_d", "ifD"),
            gen_link("Eth5/1", "device_e", "ifE"),
            gen_link("Eth6/1", "device_f", "ifF"),
            gen_link("Eth7/1", "device_f", "ifF"),

            gen_link("Eth10/1", "wse001-cs-sy01", "Port 1"),

            gen_link("Eth20/1", "wse002-cs-sy01", "Port 1"),
            gen_link("Eth21/1", "wse002-cs-sy02", "Port 1"),
        ]


    def test_perform_matching_various_scenarios(self):
        # alias names for B and J
        alias_names = {"device_b": "aliasB", "device_j": "aliasJ",
                       "wse001-cs-sy01": "xs100",
                       "wse002-cs-sy01": "xs201",
                       "wse002-cs-sy02": "xs202",
                       }

        # build LLDPNeighbor list
        neighbors = [
            # exact match
            LldpNeighbor(src="sw1", src_if="Eth1/1", dst_name="device_a.cerebras.local", dst_if="ifA"),
            # alias match for B
            LldpNeighbor(src="sw1", src_if="Eth2/1", dst_name="aliasB-cm", dst_if="ifB"),
            # mismatch on c
            LldpNeighbor(src="sw1", src_if="Eth3/1", dst_name="device_c", dst_if="ifOther"),
            # swap on d/e
            LldpNeighbor(src="sw1", src_if="Eth4/1", dst_name="device_e", dst_if="ifE"),
            LldpNeighbor(src="sw1", src_if="Eth5/1", dst_name="device_d", dst_if="ifD"),

            # unrecognized on Eth6/1
            LldpNeighbor(src="sw1", src_if="Eth6/1", dst_name="aliasJ", dst_if="ifJ"),

            # system on the wrong port with an alias
            LldpNeighbor(src="sw1", src_if="Eth11/1", dst_name="xs100-cm", dst_if="Port 1"),

            LldpNeighbor(src="sw1", src_if="Eth21/1", dst_name="xs201-cm", dst_if="Port 1"),
            LldpNeighbor(src="sw1", src_if="Eth20/1", dst_name="xs202-cm", dst_if="Port 1"),
        ]

        sw_lldp_data = {"sw1": neighbors}

        statuses = perform_matching(sw_lldp_data, self.links, alias_names)
        # map src_if to state/comment for easy assertions
        by_if = {s.src_if: s for s in statuses}

        # exact match
        assert by_if["Eth1/1"].state == LINK_STATE_MATCH
        assert by_if["Eth1/1"].comment == ""

        # alias match
        assert by_if["Eth2/1"].state == LINK_STATE_MATCH
        assert by_if["Eth2/1"].comment == ""

        # mismatch IF name
        assert by_if["Eth3/1"].state == LINK_STATE_MISMATCH

        # Eth4/1,Eth5/1 => swapped
        assert by_if["Eth4/1"].state == LINK_STATE_MISMATCH
        assert by_if["Eth4/1"].comment == "move from sw1:Eth5/1"
        assert by_if["Eth5/1"].state == LINK_STATE_MISMATCH
        assert by_if["Eth5/1"].comment == "move from sw1:Eth4/1"

        # eth6 => unrecognized
        assert by_if["Eth6/1"].state == LINK_STATE_MISMATCH

        # Eth6/1 missing
        assert by_if["Eth7/1"].state == LINK_STATE_MISSING

        # Eth10/1 => system on wrong port
        assert by_if["Eth10/1"].state == LINK_STATE_MISMATCH
        assert by_if["Eth10/1"].comment == "move from sw1:Eth11/1"

        # Eth20/1 and Eth21/1 => systems are swapped
        assert by_if["Eth20/1"].state == LINK_STATE_MISMATCH
        assert "Eth21/1" in by_if["Eth20/1"].comment
        assert by_if["Eth21/1"].state == LINK_STATE_MISMATCH
        assert "Eth20/1" in by_if["Eth21/1"].comment
