######################################################################
#
# File: b2/account_info/upload_url_pool.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import abstractmethod
import collections
import threading

from .abstract import AbstractAccountInfo


class UploadUrlPool(object):
    """
    For each key (either a bucket id or large file id), holds a pool
    of (url, auth_token) pairs, with thread-safe methods to add and
    remove them.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._pool = collections.defaultdict(list)

    def put(self, key, url, auth_token):
        """
        Adds the url and auth token to the pool for the given key.
        """
        with self._lock:
            pair = (url, auth_token)
            self._pool[key].append(pair)

    def take(self, key):
        """
        Returns (url, auth_token) if one is available, or (None, None) if not.
        """
        with self._lock:
            pair_list = self._pool[key]
            if pair_list:
                return pair_list.pop()
            else:
                return (None, None)

    def clear_for_key(self, key):
        with self._lock:
            if key in self._pool:
                del self._pool[key]


class UrlPoolAccountInfo(AbstractAccountInfo):
    def __init__(self):
        super(UrlPoolAccountInfo, self).__init__()
        self._reset_upload_pools()

    @abstractmethod
    def clear(self):
        self._reset_upload_pools()
        return super(UrlPoolAccountInfo, self).clear()

    def _reset_upload_pools(self):
        self._bucket_uploads = UploadUrlPool()
        self._large_file_uploads = UploadUrlPool()

    # bucket upload url
    def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
        self._bucket_uploads.put(bucket_id, upload_url, upload_auth_token)

    def clear_bucket_upload_data(self, bucket_id):
        self._bucket_uploads.clear_for_key(bucket_id)

    def take_bucket_upload_url(self, bucket_id):
        return self._bucket_uploads.take(bucket_id)

    # large file upload url
    def put_large_file_upload_url(self, file_id, upload_url, upload_auth_token):
        self._large_file_uploads.put(file_id, upload_url, upload_auth_token)

    def take_large_file_upload_url(self, file_id):
        return self._large_file_uploads.take(file_id)

    def clear_large_file_upload_urls(self, file_id):
        self._large_file_uploads.clear_for_key(file_id)
