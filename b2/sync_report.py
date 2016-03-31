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
import sys
import threading
import time

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
        self.start_time = time.time()
        self.state = 'local'
        self.local_file_count = 0
        self.compare_count = 0
        self.total_transfer_files = 0  # set in end_compare()
        self.total_transfer_bytes = 0  # set in end_compare()
        self.transfer_files = 0
        self.transfer_bytes = 0
        self.current_line = ''
        self.lock = threading.Lock()
        self._update_progress()

    def close(self):
        with self.lock:
            if self.state != self.STATE_DONE:
                self._print_line('', False)

    def print_completion(self, message):
        """
        Removes the progress bar, prints a message, and puts the progress
        bar back.
        """
        with self.lock:
            self._print_line(message, True)
            self._update_progress()

    def _update_progress(self):
        rate = int(self.transfer_bytes / (time.time() - self.start_time))
        if self.state == self.STATE_LOCAL:
            message = ' count: %d files   compare: %d files   transferred: %d files   %d bytes   %d B/s' % (
                self.local_file_count,
                self.compare_count,
                self.transfer_files,
                self.transfer_bytes,
                rate
            )
        elif self.state == self.STATE_COMPARE:
            message = ' compare: %d/%d files   transferred: %d files   %d bytes   %d B/s' % (
                self.compare_count,
                self.local_file_count,
                self.transfer_files,
                self.transfer_bytes,
                rate
            )
        elif self.state == self.STATE_TRANSFER:
            message = ' compare: %d/%d files   transferred: %d/%d files   %d/%d bytes   %d B/s' % (
                self.compare_count,
                self.local_file_count,
                self.transfer_files,
                self.total_transfer_files,
                self.transfer_bytes,
                self.total_transfer_bytes,
                rate
            )
        else:
            message = ''
        self._print_line(message, False)

    def _print_line(self, line, newline):
        """
        Prints a line to stdout.

        :param line: A string without a \r or \n in it.
        :param newline: True if the output should move to a new line after this one.
        """
        if len(line) < len(self.current_line):
            line += ' ' * (len(self.current_line) - len(line))
        sys.stdout.write(line)
        if newline:
            sys.stdout.write('\n')
            self.current_line = ''
        else:
            sys.stdout.write('\r')
            self.current_line = line
        sys.stdout.flush()

    def update_local(self, delta):
        """
        Reports that more local files have been found.
        """
        with self.lock:
            assert self.state == self.STATE_LOCAL
            self.local_file_count += delta
            self._update_progress()

    def end_local(self):
        """
        Local file count is done.  Can proceed to step 2.
        """
        with self.lock:
            assert self.state == self.STATE_LOCAL
            self.state = self.STATE_COMPARE
            self._update_progress()

    def update_compare(self, delta):
        """
        Reports that more files have been compared.
        """
        with self.lock:
            self.compare_count += delta
            self._update_progress()

    def end_compare(self, total_transfer_files, total_transfer_bytes):
        with self.lock:
            assert self.state == self.STATE_COMPARE
            self.state = self.STATE_TRANSFER
            self.total_transfer_files = total_transfer_files
            self.total_transfer_bytes = total_transfer_bytes
            self._update_progress()

    def update_transfer(self, file_delta, byte_delta):
        with self.lock:
            self.transfer_files += file_delta
            self.transfer_bytes += byte_delta
            self._update_progress()


def sample_run(report_class):
    sync_report = report_class()

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
            sync_report.update_transfer(25, 25000)
    sync_report.end_compare(50, 50000)

    for i in six.moves.range(25):
        if i % 2 == 0:
            sync_report.print_completion('transferred: %d.txt' % i)
        sync_report.update_transfer(1, 1000)
        time.sleep(0.2)

    sync_report.close()


sample_run(SyncReport)




