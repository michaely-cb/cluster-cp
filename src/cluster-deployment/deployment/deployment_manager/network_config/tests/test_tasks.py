#!/usr/bin/env python

# Copyright 2022, Cerebras Systems, Inc. All rights reserved.

""" Tests for the tasks module
"""


import os
import sys

import unittest
import deployment_manager.network_config as network_config

class TestTasks(unittest.TestCase):
    """ Basic tests for the tasks package
    """
    def test_load_tasks_package(self):
        """ Validate that the tasks module is sane
        """
        self.assertGreater(
            len(network_config.tasks.TaskGenerator.task_classes), 0)
