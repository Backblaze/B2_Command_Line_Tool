######################################################################
#
# File: b2/account_info/test_upload_conncurrency.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
import threading

import six

from .sqlite_account_info import SqliteAccountInfo


def test_upload_url_concurrency():
    # Clean up from previous tests
    file_name = '/tmp/test_upload_conncurrency.db'
    try:
        os.unlink(file_name)
    except OSError:
        pass

    # Make an account info with a bunch of upload URLs in it.
    account_info = SqliteAccountInfo(file_name)
    available_urls = set()
    for i in six.moves.range(3000):
        url = 'url_%d' % i
        account_info.put_bucket_upload_url('bucket-id', url, 'auth-token-%d' % i)
        available_urls.add(url)

    # Pull them all from the account info, from multiple threads
    lock = threading.Lock()

    def run_thread():
        while True:
            (url, _) = account_info.take_bucket_upload_url('bucket-id')
            if url is None:
                break
            with lock:
                if url in available_urls:
                    available_urls.remove(url)
                else:
                    print('DOUBLE:', url)

    threads = []
    for i in six.moves.range(5):
        thread = threading.Thread(target=run_thread)
        thread.start()
        threads.append(thread)
    for t in threads:
        t.join()

    # Check
    if len(available_urls) != 0:
        print('LEAK:', available_urls)

    # Clean up
    os.unlink(file_name)
