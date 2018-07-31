######################################################################
#
# File: b2/transferer.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .download_dest import DownloadDestProgressWrapper
from .progress import DoNothingProgressListener


class Transferer(object):
    def __init__(self, session, account_info):
        self.session = session
        self.account_info = account_info

    def download_file_from_url(self, url, download_dest, progress_listener=None, range_=None):
        progress_listener = progress_listener or DoNothingProgressListener()
        with progress_listener:
            return self.session.download_file_from_url(
                url,
                DownloadDestProgressWrapper(download_dest, progress_listener),
                url_factory=self.account_info.get_download_url,
                range_=range_,
            )
