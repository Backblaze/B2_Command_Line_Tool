######################################################################
#
# File: test/test_base.py
#
# Copyright 2017, Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2.progress import StreamWithHash
from b2.utils import hex_sha1_of_bytes
import six
from .test_base import TestBase


class TestHashingStream(TestBase):
    def setUp(self):
        self.data = b'01234567'
        self.stream = StreamWithHash(six.BytesIO(self.data))
        self.hash = hex_sha1_of_bytes(self.data)
        self.expected = self.data + self.hash.encode()

    def test_no_argument(self):
        output = self.stream.read()
        self.assertEqual(self.expected, output)

    def test_no_argument_less(self):
        output = self.stream.read(len(self.data) - 1)
        output += self.stream.read()
        self.assertEqual(self.expected, output)

    def test_no_argument_equal(self):
        output = self.stream.read(len(self.data))
        output += self.stream.read()
        self.assertEqual(self.expected, output)

    def test_no_argument_more(self):
        output = self.stream.read(len(self.data) + 1)
        output += self.stream.read()
        self.assertEqual(self.expected, output)

    def test_one_by_one(self):
        output = b''
        data = self.stream.read(1)
        while len(data) != 0:
            output += data
            data = self.stream.read(1)
        self.assertEqual(self.expected, output)

    def test_large_size(self):
        output = b''
        data = self.stream.read(1024)
        while len(data) != 0:
            output += data
            data = self.stream.read(1024)
        self.assertEqual(self.expected, output)

    def test_seek_zero(self):
        output0 = self.stream.read()
        self.stream.seek(0)
        output1 = self.stream.read()
        self.assertEqual(output0, output1)
