#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.


""" Tests for the network planner schema module
"""

import os
import sys

import unittest
import deployment_manager.network_config as network_config

class TestPlacedSchema(unittest.TestCase):
    """ Test the Network config schema class
    """
    @staticmethod
    def test_load_network_config_schema():
        """ Validate that the schema module can load. This implicitly
        validates the underlying JSON schema.
        """
        network_config.schema.NetworkConfigSchema()


class TestSchemaTypes(unittest.TestCase):
    """ Test schema types
    """
    def test_asn4(self):
        """ Validate the ASN4 object works as expected
        """

        # 2 and 4 byte standard ASNs
        ASN4_STR_TO_INT = (
            ('0', 0),
            ('1', 1),
            ('65534', 65534),
            ('65535', 65535),
            ('1.0', 65536),
            ('1.1', 65537),
            ('65535.0', 4294901760),
            ('65535.1', 4294901761),
            ('65535.65534', 4294967294),
            ('65535.65535', 4294967295)
        )

        for str_val, int_val in ASN4_STR_TO_INT:
            self.assertEqual(int(network_config.schema.ASN4(str_val)),
                             int_val)

            self.assertEqual(str(network_config.schema.ASN4(str_val)),
                             str_val)

            self.assertEqual(int(network_config.schema.ASN4(int_val)),
                             int_val)

            self.assertEqual(str(network_config.schema.ASN4(int_val)),
                             str_val)

        # Invalid ASNs
        ASN4_INVALID_VALUES = (
            -1, "-1", 4294967296, "4294967296", "fred", 0xFFFFFFFFFFFF,
            "65536.1", "65536.65536", "65535.65535.65535")

        for try_val in ASN4_INVALID_VALUES:
            self.assertRaises(ValueError, network_config.schema.ASN4, try_val)
