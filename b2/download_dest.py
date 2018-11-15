######################################################################
#
# File: b2/download_dest.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
from abc import abstractmethod
from contextlib import contextmanager

import six

from .utils import B2TraceMetaAbstract, limit_trace_arguments
from .progress import WritingStreamWithProgress


@six.add_metaclass(B2TraceMetaAbstract)
class AbstractDownloadDestination(object):
    """
    Interface to a destination for a downloaded file.
    """

    @abstractmethod
    @limit_trace_arguments(skip=[
        'content_sha1',
    ])
    def make_file_context(
        self,
        file_id,
        file_name,
        content_length,
        content_type,
        content_sha1,
        file_info,
        mod_time_millis,
        range_=None
    ):
        """
        Returns a context manager that yields a binary file-like object to use for
        writing the contents of the file.

        :param file_id: the B2 file ID from the headers
        :param file_name: the B2 file name from the headers
        :param content_type: the content type from the headers
        :param content_sha1: the content sha1 from the headers (or "none" for large files)
        :param file_info: the user file info from the headers
        :param mod_time_millis: the desired file modification date in ms since 1970-01-01
        :param range_: starting and ending offsets of the received file contents. Usually None,
                       which means that the whole file is downloaded.
        :return: None
        """


class DownloadDestLocalFile(AbstractDownloadDestination):
    """
    Stores a downloaded file into a local file and sets its modification time.
    """
    MODE = 'wb+'

    def __init__(self, local_file_path):
        self.local_file_path = local_file_path

    def make_file_context(
        self,
        file_id,
        file_name,
        content_length,
        content_type,
        content_sha1,
        file_info,
        mod_time_millis,
        range_=None
    ):
        self.file_id = file_id
        self.file_name = file_name
        self.content_length = content_length
        self.content_type = content_type
        self.content_sha1 = content_sha1
        self.file_info = file_info
        self.range_ = range_
        return self.write_to_local_file_context(mod_time_millis)

    @contextmanager
    def write_to_local_file_context(self, mod_time_millis):
        completed = False
        try:
            # Open the file and let the caller write it.
            with open(self.local_file_path, self.MODE) as f:
                yield f

            # After it's closed, set the mod time.
            # This is an ugly hack to make the tests work.  I can't think
            # of any other cases where os.utime might fail.
            if self.local_file_path != os.devnull:
                mod_time = mod_time_millis / 1000.0
                os.utime(self.local_file_path, (mod_time, mod_time))

            # Set the flag that means to leave the downloaded file on disk.
            completed = True

        finally:
            # This is a best-effort attempt to clean up files that
            # failed to download, so we don't leave partial files
            # sitting on disk.
            if not completed:
                os.unlink(self.local_file_path)


class PreSeekedDownloadDest(DownloadDestLocalFile):
    """
    Stores a downloaded file into a local file and sets its modification time.
    Does not truncate the target file, seeks to a given offset just after opening
    a descriptor.
    """
    MODE = 'rb+'

    def __init__(self, local_file_path, seek_target):
        self._seek_target = seek_target
        super(PreSeekedDownloadDest, self).__init__(local_file_path)

    @contextmanager
    def write_to_local_file_context(self, *args, **kwargs):
        with super(PreSeekedDownloadDest, self).write_to_local_file_context(*args, **kwargs) as f:
            f.seek(self._seek_target)
            yield f


class DownloadDestBytes(AbstractDownloadDestination):
    """
    Stores a downloaded file into bytes in memory.
    """

    def __init__(self):
        self.bytes_written = None

    def make_file_context(
        self,
        file_id,
        file_name,
        content_length,
        content_type,
        content_sha1,
        file_info,
        mod_time_millis,
        range_=None
    ):
        self.file_id = file_id
        self.file_name = file_name
        self.content_length = content_length
        self.content_type = content_type
        self.content_sha1 = content_sha1
        self.file_info = file_info
        self.mod_time_millis = mod_time_millis
        self.range_ = range_
        return self.capture_bytes_context()

    @contextmanager
    def capture_bytes_context(self):
        """
        Remembers the bytes written in self.bytes_written
        """
        # Make a place to store the data written
        bytes_io = six.BytesIO()

        # Let the caller write it
        yield bytes_io

        # Capture the result.  The BytesIO object won't let you grab
        # the data after it's closed
        self.bytes_written = bytes_io.getvalue()
        bytes_io.close()

    def get_bytes_written(self):
        if self.bytes_written is None:
            raise Exception('data not written yet')
        return self.bytes_written


class DownloadDestProgressWrapper(AbstractDownloadDestination):
    """
    Wraps a DownloadDestination, and reports progress to a ProgressListener.
    """

    def __init__(self, download_dest, progress_listener):
        self.download_dest = download_dest
        self.progress_listener = progress_listener

    def make_file_context(
        self,
        file_id,
        file_name,
        content_length,
        content_type,
        content_sha1,
        file_info,
        mod_time_millis,
        range_=None
    ):
        return self.write_file_and_report_progress_context(
            file_id, file_name, content_length, content_type, content_sha1, file_info,
            mod_time_millis, range_
        )

    @contextmanager
    def write_file_and_report_progress_context(
        self, file_id, file_name, content_length, content_type, content_sha1, file_info,
        mod_time_millis, range_
    ):
        with self.download_dest.make_file_context(
            file_id, file_name, content_length, content_type, content_sha1, file_info,
            mod_time_millis, range_
        ) as file_:
            total_bytes = content_length
            if range_ is not None:
                total_bytes = range_[1] - range_[0] + 1
            self.progress_listener.set_total_bytes(total_bytes)
            with self.progress_listener:
                yield WritingStreamWithProgress(file_, self.progress_listener)
