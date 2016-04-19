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


class TestFormatAndScaleNumber(unittest.TestCase):
    def test_it(self):
        self._check_one('1 B', 1)
        self._check_one('999 B', 999)
        self._check_one('1.00 kB', 1000)
        self._check_one('999 kB', 999000)

    def _check_one(self, expected, x):
        self.assertEqual(expected, b2.utils.format_and_scale_number(x, 'B'))


class TestFormatAndScaleFraction(unittest.TestCase):
    def test_it(self):
        self._check_one('0 / 100 B', 0, 100)
        self._check_one('0.0 / 10.0 kB', 0, 10000)
        self._check_one('9.4 / 10.0 kB', 9400, 10000)

    def _check_one(self, expected, numerator, denominator):
        self.assertEqual(expected, b2.utils.format_and_scale_fraction(numerator, denominator, 'B'))
