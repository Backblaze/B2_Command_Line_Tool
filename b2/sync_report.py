######################################################################
#
# File: b2/sync.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import print_function

import six
import threading

from tqdm import tqdm


class SyncReport(object):
    """
    Handles reporting progress for syncing.

    Prints out each file as it is processed, and puts up a sequence
    of progress bars.

    The progress bars are:
       - Step 1/1: count local files
       - Step 2/2: compare file lists
       - Step 3/3: transfer files

    This class is THREAD SAFE so that it can be used from parallel sync threads.
    """

    STATE_LOCAL = 'local'
    STATE_COMPARE = 'compare'
    STATE_TRANSFER = 'transfer'
    STATE_DONE = 'done'

    def __init__(self):
        self.state = 'local'
        self.local_file_count = 0
        self.compare_count = 0
        self.total_transfer_bytes = 0  # set in end_compare()
        self.transfer_bytes = 0
        self.tqdm = self._make_tqdm()
        self.lock = threading.Lock()

    def close(self):
        with self.lock:
            if self.state != self.STATE_DONE:
                self._remove_tqdm()
                self.state = self.STATE_DONE

    def print_completion(self, message):
        """
        Removes the progress bar, prints a message, and puts the progress
        bar back.
        """
        with self.lock:
            self._remove_tqdm()
            print(message)
            self.tqdm = self._make_tqdm()

    def update_local(self, delta):
        """
        Reports that more local files have been found.
        """
        with self.lock:
            assert self.state == self.STATE_LOCAL
            self.local_file_count += delta
            self.tqdm.update(delta)

    def end_local(self):
        """
        Local file count is done.  Can proceed to step 2.
        """
        with self.lock:
            self._remove_tqdm()
            assert self.state == self.STATE_LOCAL
            self.state = self.STATE_COMPARE
            self.tqdm = self._make_tqdm()

    def update_compare(self, delta):
        """
        Reports that more files have been compared.
        """
        with self.lock:
            self.compare_count += delta
            if self.state == self.STATE_COMPARE:
                self.tqdm.update(delta)

    def end_compare(self, total_transfer_bytes):
        with self.lock:
            self._remove_tqdm()
            assert self.state == self.STATE_COMPARE
            self.state = self.STATE_TRANSFER
            self.tqdm = self._make_tqdm()
            self.total_transfer_bytes = total_transfer_bytes

    def update_transfer(self, delta):
        with self.lock:
            self.transfer_bytes += delta
            if self.state == self.STATE_TRANSFER:
                self.tqdm.update(delta)

    def _add_tqdm(self):
        """
        Creates a progress bar and displays it.
        """
        assert self.tqdm is None
        tqdm = self._make_tqdm()
        if tqdm is not None:
            self.tqdm = tqdm
            self.tqdm.__enter__()
            self.tqdm.update(0)

    def _remove_tqdm(self):
        """
        Gets rid of any progress bar that is being displayed.
        """
        if self.tqdm is not None:
            self.tqdm.__exit__(None, None, None)
            self.tqdm = None

    def _make_tqdm(self):
        """
        Makes a progress bar if we're in a state that needs one.

        The formats do not include speed.  We keep re-creating the bar,
        so speed computation doesn't work.
        """
        if self.state == self.STATE_LOCAL:
            return tqdm(
                total=123456,
                desc='Step 1/3: count local files',
                unit='file',
                bar_format='{desc} {n_fmt}',
                initial=self.local_file_count
            )
        elif self.state == self.STATE_COMPARE:
            return tqdm(
                total=self.local_file_count,
                desc='Step 2/3: compare file lists',
                unit='file',
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}',
                initial=self.compare_count
            )
        elif self.state == self.STATE_TRANSFER:
            return tqdm(
                total=self.total_transfer_bytes,
                desc='Step 3/3: transfer data',
                unit='B',
                unit_scale=True,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}',
                initial=self.transfer_bytes
            )

def sample_run():
    import time
    sync_report = SyncReport()

    for i in six.moves.range(20):
        sync_report.update_local(1)
        time.sleep(0.2)
        if i == 10:
            sync_report.print_completion('transferred: a.txt')
        if i % 2 == 0:
            sync_report.update_compare(1)
    sync_report.end_local()

    for i in six.moves.range(10):
        sync_report.update_compare(1)
        time.sleep(0.2)
        if i == 3:
            sync_report.print_completion('transferred: b.txt')
        if i == 4:
            sync_report.update_transfer(25)
    sync_report.end_compare(50)

    for i in six.moves.range(25):
        if i % 2 == 0:
            sync_report.print_completion('transferred: %d.txt' % i)
        sync_report.update_transfer(1)
        time.sleep(0.2)

    sync_report.close()

sample_run()




