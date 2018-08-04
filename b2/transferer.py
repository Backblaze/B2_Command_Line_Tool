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

import six

from .download_dest import DownloadDestProgressWrapper
from .exception import ChecksumMismatch, UnexpectedCloudBehaviour, TruncatedOutput
from .progress import DoNothingProgressListener
from .raw_api import SRC_LAST_MODIFIED_MILLIS
from .utils import B2TraceMetaAbstract

# block size used when downloading file. If it is set to a high value, progress reporting will be jumpy, if it's too low, it impacts CPU
BLOCK_SIZE = 4096


@six.add_metaclass(B2TraceMetaAbstract)
class Transferer(object):
    """ Handles complex actions around downloads and uploads to free raw_api from that responsibility """

    def __init__(self, session, account_info):
        self.session = session
        self.account_info = account_info

    def download_file_from_url(self, url, download_dest, progress_listener=None, range_=None):
        """
        :param url: url from which the file should be downloaded
        :param download_dest: where to put the file when it is downloaded
        :param progress_listener: where to notify about progress downloading
        :param range_: 2-element tuple containing data of http Range header
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
