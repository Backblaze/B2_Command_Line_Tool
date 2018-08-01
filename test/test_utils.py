######################################################################
#
# File: test_utils.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import six

from .test_base import TestBase
import b2.utils

# These are from the B2 Docs (https://www.backblaze.com/b2/docs/string_encoding.html)
ENCODING_TEST_CASES = [
    {
        'fullyEncoded': '%20',
        'minimallyEncoded': '+',
        'string': ' '
    },
    {
        'fullyEncoded': '%21',
        'minimallyEncoded': '!',
        'string': '!'
    },
    {
        'fullyEncoded': '%22',
        'minimallyEncoded': '%22',
        'string': '"'
    },
    {
        'fullyEncoded': '%23',
        'minimallyEncoded': '%23',
        'string': '#'
    },
    {
        'fullyEncoded': '%24',
        'minimallyEncoded': '$',
        'string': '$'
    },
    {
        'fullyEncoded': '%25',
        'minimallyEncoded': '%25',
        'string': '%'
    },
    {
        'fullyEncoded': '%26',
        'minimallyEncoded': '%26',
        'string': '&'
    },
    {
        'fullyEncoded': '%27',
        'minimallyEncoded': "'",
        'string': "'"
    },
    {
        'fullyEncoded': '%28',
        'minimallyEncoded': '(',
        'string': '('
    },
    {
        'fullyEncoded': '%29',
        'minimallyEncoded': ')',
        'string': ')'
    },
    {
        'fullyEncoded': '%2A',
        'minimallyEncoded': '*',
        'string': '*'
    },
    {
        'fullyEncoded': '%2B',
        'minimallyEncoded': '%2B',
        'string': '+'
    },
    {
        'fullyEncoded': '%2C',
        'minimallyEncoded': '%2C',
        'string': ','
    },
    {
        'fullyEncoded': '%2D',
        'minimallyEncoded': '-',
        'string': '-'
    },
    {
        'fullyEncoded': '%2E',
        'minimallyEncoded': '.',
        'string': '.'
    },
    {
        'fullyEncoded': '/',
        'minimallyEncoded': '/',
        'string': '/'
    },
    {
        'fullyEncoded': '%30',
        'minimallyEncoded': '0',
        'string': '0'
    },
    {
        'fullyEncoded': '%39',
        'minimallyEncoded': '9',
        'string': '9'
    },
    {
        'fullyEncoded': '%3A',
        'minimallyEncoded': ':',
        'string': ':'
    },
    {
        'fullyEncoded': '%3B',
        'minimallyEncoded': ';',
        'string': ';'
    },
    {
        'fullyEncoded': '%3C',
        'minimallyEncoded': '%3C',
        'string': '<'
    },
    {
        'fullyEncoded': '%3D',
        'minimallyEncoded': '=',
        'string': '='
    },
    {
        'fullyEncoded': '%3E',
        'minimallyEncoded': '%3E',
        'string': '>'
    },
    {
        'fullyEncoded': '%3F',
        'minimallyEncoded': '%3F',
        'string': '?'
    },
    {
        'fullyEncoded': '%40',
        'minimallyEncoded': '@',
        'string': '@'
    },
    {
        'fullyEncoded': '%41',
        'minimallyEncoded': 'A',
        'string': 'A'
    },
    {
        'fullyEncoded': '%5A',
        'minimallyEncoded': 'Z',
        'string': 'Z'
    },
    {
        'fullyEncoded': '%5B',
        'minimallyEncoded': '%5B',
        'string': '['
    },
    {
        'fullyEncoded': '%5C',
        'minimallyEncoded': '%5C',
        'string': '\\'
    },
    {
        'fullyEncoded': '%5D',
        'minimallyEncoded': '%5D',
        'string': ']'
    },
    {
        'fullyEncoded': '%5E',
        'minimallyEncoded': '%5E',
        'string': '^'
    },
    {
        'fullyEncoded': '%5F',
        'minimallyEncoded': '_',
        'string': '_'
    },
    {
        'fullyEncoded': '%60',
        'minimallyEncoded': '%60',
        'string': '`'
    },
    {
        'fullyEncoded': '%61',
        'minimallyEncoded': 'a',
        'string': 'a'
    },
    {
        'fullyEncoded': '%7A',
        'minimallyEncoded': 'z',
        'string': 'z'
    },
    {
        'fullyEncoded': '%7B',
        'minimallyEncoded': '%7B',
        'string': '{'
    },
    {
        'fullyEncoded': '%7C',
        'minimallyEncoded': '%7C',
        'string': '|'
    },
    {
        'fullyEncoded': '%7D',
        'minimallyEncoded': '%7D',
        'string': '}'
    },
    {
        'fullyEncoded': '%7E',
        'minimallyEncoded': '~',
        'string': '~'
    },
    {
        'fullyEncoded': '%7F',
        'minimallyEncoded': '%7F',
        'string': u'\u007f'
    },
    {
        'fullyEncoded': '%E8%87%AA%E7%94%B1',
        'minimallyEncoded': '%E8%87%AA%E7%94%B1',
        'string': u'\u81ea\u7531'
    },
    {
        'fullyEncoded': '%F0%90%90%80',
        'minimallyEncoded': '%F0%90%90%80',
        'string': u'\U00010400'
    },
]


class TestUrlEncoding(TestBase):
    def test_it(self):
        for test_case in ENCODING_TEST_CASES:
            string = test_case['string']
            fully_encoded = test_case['fullyEncoded']
            minimally_encoded = test_case['minimallyEncoded']
            encoded = b2.utils.b2_url_encode(string)

            expected_encoded = (minimally_encoded, fully_encoded)
            if encoded not in expected_encoded:
                print(
                    'string: %s   encoded: %s   expected: %s' %
                    (repr(string), encoded, expected_encoded)
                )
            self.assertTrue(encoded in expected_encoded)
            self.assertEqual(string, b2.utils.b2_url_decode(fully_encoded))
            self.assertEqual(string, b2.utils.b2_url_decode(minimally_encoded))


class TestChooseParts(TestBase):
    def test_it(self):
        self._check_one([(0, 100), (100, 100)], 200, 100)
        self._check_one([(0, 149), (149, 150)], 299, 100)
        self._check_one([(0, 100), (100, 100), (200, 100)], 300, 100)

        ten_TB = 10 * 1000 * 1000 * 1000 * 1000
        one_GB = 1000 * 1000 * 1000

        expected = [(i * one_GB, one_GB) for i in six.moves.range(10000)]
        actual = b2.utils.choose_part_ranges(ten_TB, 100 * 1000 * 1000)
        self.assertEqual(expected, actual)

    def _check_one(self, expected, content_length, min_part_size):
        self.assertEqual(expected, b2.utils.choose_part_ranges(content_length, min_part_size))


class TestFormatAndScaleNumber(TestBase):
    def test_it(self):
        self._check_one('1 B', 1)
        self._check_one('999 B', 999)
        self._check_one('1.00 kB', 1000)
        self._check_one('999 kB', 999000)

    def _check_one(self, expected, x):
        self.assertEqual(expected, b2.utils.format_and_scale_number(x, 'B'))


class TestFormatAndScaleFraction(TestBase):
    def test_it(self):
        self._check_one('0 / 100 B', 0, 100)
        self._check_one('0.0 / 10.0 kB', 0, 10000)
        self._check_one('9.4 / 10.0 kB', 9400, 10000)

    def _check_one(self, expected, numerator, denominator):
        self.assertEqual(expected, b2.utils.format_and_scale_fraction(numerator, denominator, 'B'))
