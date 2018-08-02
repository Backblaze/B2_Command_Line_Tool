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

from .download_dest import DownloadDestProgressWrapper
from .exception import ChecksumMismatch, UnexpectedCloudBehaviour, TruncatedOutput
from .progress import DoNothingProgressListener
from .raw_api import SRC_LAST_MODIFIED_MILLIS

# block size used when downloading file. If it is set to a high value, progress reporting will be jumpy, if it's too low, it impacts CPU
BLOCK_SIZE = 4096


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

            info = response.headers

            file_id = info['x-bz-file-id']
            file_name = info['x-bz-file-name']
            content_type = info['content-type']
            content_length = int(info['content-length'])
            content_sha1 = info['x-bz-content-sha1']
            if range_ is not None:
                if 'Content-Range' not in info:
                    raise UnexpectedCloudBehaviour('Content-Range header was expected')
            file_info = dict((k[10:], info[k]) for k in info if k.startswith('x-bz-info-'))

            digest = hashlib.sha1()
            bytes_read = 0

            info_dict = self._download_response_to_info_dict(response)

            mod_time_millis = int(
                info_dict['fileInfo'].get(
                    SRC_LAST_MODIFIED_MILLIS,
                    info['x-bz-upload-timestamp'],
                )
            )

            with download_dest.make_file_context(
                file_id,
                file_name,
                content_length,
                content_type,
                content_sha1,
                file_info,
                mod_time_millis,
                range_=range_,
            ) as file:
                for data in response.iter_content(chunk_size=BLOCK_SIZE):
                    file.write(data)
                    digest.update(data)
                    bytes_read += len(data)

                if range_ is None:
                    if bytes_read != int(info['content-length']):
                        raise TruncatedOutput(bytes_read, content_length)

                    if content_sha1 != 'none' and digest.hexdigest() != content_sha1:
                        raise ChecksumMismatch(
                            checksum_type='sha1',
                            expected=content_length,
                            actual=digest.hexdigest()
                        )
                else:
                    desired_length = range_[1] - range_[0] + 1
                    if bytes_read != desired_length:
                        raise TruncatedOutput(bytes_read, desired_length)

                return info_dict

    @classmethod
    def _download_response_to_info_dict(cls, response):
        info = response.headers
        return dict(
            fileId=info['x-bz-file-id'],
            fileName=info['x-bz-file-name'],
            contentType=info['content-type'],
            contentLength=int(info['content-length']),
            contentSha1=info['x-bz-content-sha1'],
            fileInfo=dict((k[10:], info[k]) for k in info if k.startswith('x-bz-info-')),
        )
