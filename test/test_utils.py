######################################################################
#
# File: test_utils.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import b2.utils
import unittest


class TestChooseParts(unittest.TestCase):
    def test_it(self):
        self._check_one([(0, 100), (100, 100)], 200, 100)
        self._check_one([(0, 149), (149, 150)], 299, 100)
        self._check_one([(0, 100), (100, 100), (200, 100)], 300, 100)

    def _check_one(self, expected, content_length, min_part_size):
        self.assertEqual(expected, b2.utils.choose_part_ranges(content_length, min_part_size))
