######################################################################
#
# File: utils.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import division

import hashlib
import shutil
import tempfile

import six
from six.moves import urllib


def b2_url_encode(s):
    """URL-encodes a unicode string to be sent to B2 in an HTTP header.
    """
    return urllib.parse.quote(s.encode('utf-8'))


def b2_url_decode(s):
    """Decodes a Unicode string returned from B2 in an HTTP header.

    Returns a Python unicode string.
    """
    # Use str() to make sure that the input to unquote is a str, not
    # unicode, which ensures that the result is a str, which allows
    # the decoding to work properly.
    return urllib.parse.unquote_plus(str(s)).decode('utf-8')


def choose_part_ranges(content_length, minimum_part_size):
    """
    Returns a list of (offset, length) for the parts of a large file.
    """

    # If the file is at least twice the minimum part size, we are guaranteed
    # to be able to break it into multiple parts that are all at least
    # the minimum part size.
    assert minimum_part_size * 2 <= content_length

    # How many parts can we make?
    part_count = content_length // minimum_part_size
    assert 2 <= part_count

    # All of the parts, except the last, are the same size
    part_size = content_length // part_count
    last_part_size = content_length - (part_size * (part_count - 1))
    assert minimum_part_size <= last_part_size

    # Make all of the parts except the last
    parts = [(i * part_size, part_size) for i in six.moves.range(part_count - 1)]

    # Add the last part
    start_of_last = (part_count - 1) * part_size
    last_part = (start_of_last, content_length - start_of_last)
    parts.append(last_part)

    return parts


def hex_sha1_of_stream(input_stream, content_length):
    """
    Returns the 40-character hex SHA1 checksum of the first content_length
    bytes in the input stream.
    """
    remaining = content_length
    block_size = 1024 * 1024
    digest = hashlib.sha1()
    while remaining != 0:
        to_read = min(remaining, block_size)
        data = input_stream.read(to_read)
        if len(data) != to_read:
            raise ValueError(
                'content_length(%s) is more than the size of the file' % content_length
            )
        digest.update(data)
        remaining -= to_read
    return digest.hexdigest()


def hex_sha1_of_bytes(data):
    """
    Returns the 40-character hex SHA1 checksum of the first content_length
    bytes in the input stream.
    """
    return hashlib.sha1(data).hexdigest()


def validate_b2_file_name(name):
    """
    Raises a ValueError if the name is not a valid B2 file name.

    :param name: a string
    :return: None
    """
    if not isinstance(name, six.string_types):
        raise ValueError('file name must be a string, not bytes')
    name_utf8 = name.encode('utf-8')
    if len(name_utf8) < 1:
        raise ValueError('file name too short (0 utf-8 bytes)')
    if 1000 < len(name_utf8):
        raise ValueError('file name too long (more than 1000 utf-8 bytes)')
    if name[0] == '/':
        raise ValueError("file names must not start with '/'")
    if name[-1] == '/':
        raise ValueError("file names must not end with '/'")
    if '\\' in name:
        raise ValueError("file names must not contain '\\'")
    if '//' in name:
        raise ValueError("file names must not contain '//'")
    if chr(127) in name:
        raise ValueError("file names must not contain DEL")
    if any(250 < len(segment) for segment in name_utf8.split(six.b('/'))):
        raise ValueError("file names segments (between '/') can be at most 250 utf-8 bytes")


class BytesIoContextManager(object):
    """
    A simple wrapper for a BytesIO that makes it look like
    a file-like object that can be a context manager.
    """

    def __init__(self, byte_data):
        self.byte_data = byte_data

    def __enter__(self):
        return six.BytesIO(self.byte_data)

    def __exit__(self, type, value, traceback):
        return None  # don't hide exception


class TempDir(object):
    """
    Context manager that creates and destroys a temporary directory.
    """

    def __enter__(self):
        self.dirpath = tempfile.mkdtemp()
        return self.dirpath

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.dirpath)
        return None  # do not hide exception
