######################################################################
#
# File: b2/transferer/simple.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import hashlib

from .abstract import AbstractDownloader


class SimpleDownloader(AbstractDownloader):
    def __init__(self, *args, **kwargs):
        super(SimpleDownloader, self).__init__(*args, **kwargs)

    def is_suitable(self, metadata, progress_listener):
        return True

    def download(self, file, response, metadata, session):
        actual_size = self._get_remote_range(response, metadata).size()
        digest = hashlib.sha1()
        bytes_read = 0
        for data in response.iter_content(chunk_size=self._get_chunk_size(actual_size)):
            file.write(data)
            digest.update(data)
            bytes_read += len(data)
        return bytes_read, digest.hexdigest()
