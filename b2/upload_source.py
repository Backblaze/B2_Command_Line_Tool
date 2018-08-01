######################################################################
#
# File: b2/upload_source.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import hashlib
import os
from abc import (ABCMeta, abstractmethod)

import six

from .exception import InvalidUploadSource
from .utils import (BytesIoContextManager, hex_sha1_of_stream)


@six.add_metaclass(ABCMeta)
class AbstractUploadSource(object):
    """
    The source of data for uploading to B2.
    """

    @abstractmethod
    def get_content_length(self):
        """
        Returns the number of bytes of data in the file.
        """

    @abstractmethod
    def get_content_sha1(self):
        """
        Return a 40-character string containing the hex SHA1 checksum of the data in the file.
        """

    @abstractmethod
    def open(self):
        """
        Returns a binary file-like object from which the
        data can be read.
        :return:
        """


class UploadSourceBytes(AbstractUploadSource):
    def __init__(self, data_bytes):
        self.data_bytes = data_bytes

    def get_content_length(self):
        return len(self.data_bytes)

    def get_content_sha1(self):
        return hashlib.sha1(self.data_bytes).hexdigest()

    def open(self):
        return BytesIoContextManager(self.data_bytes)


class UploadSourceLocalFile(AbstractUploadSource):
    def __init__(self, local_path, content_sha1=None):
        self.local_path = local_path
        if not os.path.isfile(local_path):
            raise InvalidUploadSource(local_path)
        self.content_length = os.path.getsize(local_path)
        self.content_sha1 = content_sha1

    def get_content_length(self):
        return self.content_length

    def get_content_sha1(self):
        if self.content_sha1 is None:
            self.content_sha1 = self._hex_sha1_of_file(self.local_path)
        return self.content_sha1

    def open(self):
        return open(self.local_path, 'rb')

    def _hex_sha1_of_file(self, local_path):
        with open(local_path, 'rb') as f:
            return hex_sha1_of_stream(f, self.content_length)
