######################################################################
#
# File: b2/test_scan_policies.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import print_function

from b2.sync.scan_policies import DEFAULT_SCAN_MANAGER, ScanPoliciesManager
from .test_base import TestBase


class TestScanPolicies(TestBase):
    def test_default(self):
        self.assertFalse(DEFAULT_SCAN_MANAGER.should_exclude_directory(''))
        self.assertFalse(DEFAULT_SCAN_MANAGER.should_exclude_file(''))
        self.assertFalse(DEFAULT_SCAN_MANAGER.should_exclude_directory('a'))
        self.assertFalse(DEFAULT_SCAN_MANAGER.should_exclude_file('a'))

    def test_exclude_include(self):
        policy = ScanPoliciesManager(exclude_file_regexes=['a', 'b'], include_file_regexes=['ab'])
        self.assertTrue(policy.should_exclude_file('alfa'))
        self.assertTrue(policy.should_exclude_file('bravo'))
        self.assertFalse(policy.should_exclude_file('abend'))
        self.assertFalse(policy.should_exclude_file('charlie'))

    def test_exclude_dir(self):
        policy = ScanPoliciesManager(
            include_file_regexes=['.*[.]txt$'], exclude_dir_regexes=['alfa$']
        )
        self.assertTrue(policy.should_exclude_directory('alfa'))
        self.assertFalse(policy.should_exclude_directory('alfa2'))
        self.assertFalse(policy.should_exclude_directory('alfa/hello'))
