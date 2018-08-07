######################################################################
#
# File: b2/transferer.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import hashlib
import threading

from six.moves import queue
import six

from .download_dest import DownloadDestProgressWrapper
from .exception import ChecksumMismatch, UnexpectedCloudBehaviour, TruncatedOutput
from .progress import DoNothingProgressListener
from .raw_api import SRC_LAST_MODIFIED_MILLIS
from .utils import B2TraceMetaAbstract

# block size used when downloading file. If it is set to a high value, progress reporting will be jumpy, if it's too low, it impacts CPU
BLOCK_SIZE = 8192  # ~1MB file will show ~1% progress increment


@six.add_metaclass(B2TraceMetaAbstract)
class Transferer(object):
    """ Handles complex actions around downloads and uploads to free raw_api from that responsibility """

    # how many chunks to break a downloaded file into
    DEFAULT_MAX_STREAMS = 8

    # minimum size of a download chunk
    DEFAULT_MIN_PART_SIZE = 100 * 1024 * 1024

    def __init__(self, session, account_info):
        self.session = session
        self.account_info = account_info

    def download_file_from_url(
        self,
        url,
        download_dest,
        progress_listener=None,
        range_=None,
        max_streams=DEFAULT_MAX_STREAMS,
        min_part_size=DEFAULT_MIN_PART_SIZE,
    ):
        """
        :param url: url from which the file should be downloaded
        :param download_dest: where to put the file when it is downloaded
        :param progress_listener: where to notify about progress downloading
        :param range_: 2-element tuple containing data of http Range header
        :param max_streams: limit on a number of streams to use when downloading
        :param min_part_size: the smallest part size for which a stream will be run
        """
        progress_listener = progress_listener or DoNothingProgressListener()
        download_dest = DownloadDestProgressWrapper(download_dest, progress_listener)
        with self.session.download_file_from_url(
            url,
            url_factory=self.account_info.get_download_url,
            range_=range_,
        ) as response:
            if range_ is not None:
                if 'Content-Range' not in response.headers:
                    raise UnexpectedCloudBehaviour('Content-Range header was expected')

            metadata = FileMetadata.from_response(response)

            mod_time_millis = int(
                metadata.file_info.get(
                    SRC_LAST_MODIFIED_MILLIS,
                    response.headers['x-bz-upload-timestamp'],
                )
            )

            with download_dest.make_file_context(
                metadata.file_id,
                metadata.file_name,
                metadata.content_length,
                metadata.content_type,
                metadata.content_sha1,
                metadata.file_info,
                mod_time_millis,
                range_=range_,
            ) as file:

                if max_streams >= 2 and metadata.content_length >= 2 * min_part_size:
                    bytes_read, actual_sha1 = self._download_file_parallel(file, response, metadata)
                else:
                    bytes_read, actual_sha1 = self._download_file_simple(file, response)

                if range_ is None:
                    if bytes_read != metadata.content_length:
                        raise TruncatedOutput(bytes_read, metadata.content_length)

                    if metadata.content_sha1 != 'none' and \
                        actual_sha1 != metadata.content_sha1:  # no yapf
                        raise ChecksumMismatch(
                            checksum_type='sha1',
                            expected=metadata.content_length,
                            actual=actual_sha1,
                        )
                else:
                    desired_length = range_[1] - range_[0] + 1
                    if bytes_read != desired_length:
                        raise TruncatedOutput(bytes_read, desired_length)

                return metadata.as_info_dict()

    def _download_file_simple(self, file, response):
        digest = hashlib.sha1()
        bytes_read = 0
        for data in response.iter_content(chunk_size=BLOCK_SIZE):
            file.write(data)
            digest.update(data)
            bytes_read += len(data)
        return bytes_read, digest.hexdigest()

    def _download_file_parallel(
        self,
        file,
        response,
        metadata,
        max_streams=DEFAULT_MAX_STREAMS,
        min_part_size=DEFAULT_MIN_PART_SIZE,
    ):
        """
        Downloads a file from given url using parallel download sessions and stores it in the given download_destination.

        Returns a dict containing all of the file info from the headers in the reply.

        :param file: an opened file-like object to write to
        :param response: The response of the first request made to the cloud service with download intent
        :param max_streams: maximum number of simultaneous streams
        :param min_part_size: minimum amount of data a single stream will retrieve, in bytes
        :return:
        """
        assert max_streams >= 1

        info_dict = FileMetadata.from_response(response).as_info_dict()

        content_length = info_dict['contentLength']
        number_of_streams = min(max_streams, content_length // min_part_size)
        part_size = content_length // number_of_streams

        # start offset of the *second* stream, as the first stream will be hashed on the fly
        start_offset_of_second_stream = part_size

        hasher = hashlib.sha1()

        with WriterThread(file) as writer:
            self._get_parts(response, writer, hasher, part_size, number_of_streams)

        # At this point the hasher already consumed the data until the end of first stream.
        # Consume the rest of the file to complete the hashing process
        file.seek(start_offset_of_second_stream)
        file_read = file.read
        while 1:
            chunk = file_read(BLOCK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)

        # we are at the end of the file, so lets check where we are
        bytes_read = file.tell()
        return bytes_read, hasher.hexdigest()

    def _get_parts(self, response, writer, hasher, part_size, number_of_streams):
        stream = FirstPartDownloaderThread(response, self.session, writer, hasher, (0, part_size))
        stream.start()
        streams = [stream]

        start = part_size
        for part_number in range(1, number_of_streams):
            end = (part_number + 1) * part_size
            stream = NonHashingDownloaderThread(response, self.session, writer, (start, end))
            stream.start()
            streams.append(stream)
            start = end + 1
        for stream in streams:
            stream.join()


class FileMetadata(object):
    __slots__ = (
        'file_id',
        'file_name',
        'content_type',
        'content_length',
        'content_sha1',
        'file_info',
    )

    def __init__(
        self,
        file_id,
        file_name,
        content_type,
        content_length,
        content_sha1,
        file_info,
    ):
        self.file_id = file_id
        self.file_name = file_name
        self.content_type = content_type
        self.content_length = content_length
        self.content_sha1 = content_sha1
        self.file_info = file_info

    @classmethod
    def from_response(cls, response):
        info = response.headers
        return cls(
            file_id=info['x-bz-file-id'],
            file_name=info['x-bz-file-name'],
            content_type=info['content-type'],
            content_length=int(info['content-length']),
            content_sha1=info['x-bz-content-sha1'],
            file_info=dict((k[10:], info[k]) for k in info if k.startswith('x-bz-info-')),
        )

    def as_info_dict(self):
        return {
            'fileId': self.file_id,
            'fileName': self.file_name,
            'contentType': self.content_type,
            'contentLength': self.content_length,
            'contentSha1': self.content_sha1,
            'fileInfo': self.file_info,
        }


class WriterThread(threading.Thread):
    def __init__(self, file):
        self.file = file
        self.queue = queue.Queue()
        super(WriterThread, self).__init__()

    def run(self):
        file = self.file
        queue_get = self.queue.get
        while 1:
            shutdown, offset, data = queue_get()
            if shutdown:
                break
            file.seek(offset)
            file.write(data)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.queue.put((True, None, None))
        self.join()


class FirstPartDownloaderThread(threading.Thread):
    def __init__(self, response, session, writer, hasher, range_):
        self.response = response
        self.session = session
        self.writer = writer
        self.hasher = hasher
        assert range_[0] == 0, range_[0]
        self.range_ = range_
        super(FirstPartDownloaderThread, self).__init__()

    def run(self):
        writer_queue = self.writer.queue
        stop = False
        bytes_read = 0
        hasher_update = self.hasher.update
        first_offset = self.range_[0]
        last_offset = self.range_[1]
        for data in self.response.iter_content(chunk_size=BLOCK_SIZE):
            if bytes_read + len(data) >= last_offset:
                to_write = data[:last_offset - bytes_read]
                stop = True
            else:
                to_write = data
            writer_queue.put((False, first_offset + bytes_read, to_write))
            hasher_update(to_write)
            bytes_read += len(to_write)
            if stop:
                break
        # since we got everything we need, close the socket and free the buffer
        # to avoid a timeout exception during hashing and other trouble
        self.response.close()


class NonHashingDownloaderThread(threading.Thread):
    def __init__(self, response, session, writer, range_):
        self.session = session
        self.url = response.request.url
        self.writer = writer
        self.range_ = range_
        super(NonHashingDownloaderThread, self).__init__()

    def run(self):
        writer_queue_put = self.writer.queue.put
        start_range = self.range_[0]
        bytes_read = 0
        with self.session.download_file_from_url(self.url, self.range_) as response:
            for to_write in response.iter_content(chunk_size=BLOCK_SIZE):
                writer_queue_put((False, start_range + bytes_read, to_write))
                bytes_read += len(to_write)
