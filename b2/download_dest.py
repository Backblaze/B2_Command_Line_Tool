######################################################################
#
# File: b2/download_dest.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
from abc import (ABCMeta, abstractmethod)

import six

from .progress import (StreamWithProgress)


@six.add_metaclass(ABCMeta)
class AbstractDownloadDestination(object):
    """
    Interface to a destination for a downloaded file.

    This isn't an abstract base class because there is just
    one kind of download destination so far: a local file.
    """

    @abstractmethod
    def open(self, file_id, file_name, content_length, content_type, content_sha1, file_info):
        """
        Returns a binary file-like object to use for writing the contents of
        the file.

        :param file_id: the B2 file ID from the headers
        :param file_name: the B2 file name from the headers
        :param content_type: the content type from the headers
        :param content_sha1: the content sha1 from the headers (or "none" for large files)
        :param file_info: the user file info from the headers
        :return: None
        """


class OpenLocalFileForWriting(object):
    """
    Context manager that opens a local file for writing,
    tracks progress as it's written, and sets the modification
    time when it's done.

    Takes care of opening/closing the file, and closing the
    progress listener.
    """

    def __init__(self, local_path_name, progress_listener, mod_time_millis):
        self.local_path_name = local_path_name
        self.progress_listener = progress_listener
        self.mod_time_millis = mod_time_millis

    def __enter__(self):
        self.file = open(self.local_path_name, 'wb')
        return StreamWithProgress(self.file.__enter__(), self.progress_listener)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress_listener.close()
        result = self.file.__exit__(exc_type, exc_val, exc_tb)
        mod_time = self.mod_time_millis / 1000.0

        # This is an ugly hack to make the tests work.  I can't think
        # of any other cases where os.utime might fail.
        if self.local_path_name != '/dev/null':
            os.utime(self.local_path_name, (mod_time, mod_time))

        return result


class DownloadDestLocalFile(AbstractDownloadDestination):
    """
    Stores a downloaded file into a local file and sets its modification time.
    """

    def __init__(self, local_file_path, progress_listener):
        self.local_file_path = local_file_path
        self.progress_listener = progress_listener

    def open(
        self, file_id, file_name, content_length, content_type, content_sha1, file_info,
        mod_time_millis
    ):
        self.file_id = file_id
        self.file_name = file_name
        self.content_length = content_length
        self.content_type = content_type
        self.content_sha1 = content_sha1
        self.file_info = file_info

        self.progress_listener.set_total_bytes(content_length)

        return OpenLocalFileForWriting(
            self.local_file_path, self.progress_listener, mod_time_millis
        )


class BytesCapture(six.BytesIO):
    """
    The BytesIO class discards the data on close().  We don't want to do that.
    """

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class DownloadDestBytes(AbstractDownloadDestination):
    """
    Stores a downloaded file into bytes in memory.
    """

    def open(
        self, file_id, file_name, content_length, content_type, content_sha1, file_info,
        mod_time_millis
    ):
        self.file_id = file_id
        self.file_name = file_name
        self.content_length = content_length
        self.content_type = content_type
        self.content_sha1 = content_sha1
        self.file_info = file_info
        self.mod_time_millis = mod_time_millis
        self.bytes_io = BytesCapture()
        return self.bytes_io
