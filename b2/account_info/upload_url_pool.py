######################################################################
#
# File: b2/account_info/upload_url_pool.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import collections
import threading


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
