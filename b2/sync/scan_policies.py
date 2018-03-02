######################################################################
#
# File: b2/sync/scan_policies.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
import re

logger = logging.getLogger(__name__)


class RegexSet(object):
    """
    Holds a (possibly empty) set of regular expressions, and knows how to check
    whether a string matches any of them.
    """

    def __init__(self, regex_iterable):
        self._compiled_list = [re.compile(r) for r in regex_iterable]

    def matches(self, s):
        return any(c.match(s) is not None for c in self._compiled_list)


class ScanPoliciesManager(object):
    """
    Policy object used when scanning folders for syncing, used to decide
    which files to include in the list of files to be synced.
    """

    def __init__(
        self,
        exclude_dir_regexes=tuple(),
        exclude_file_regexes=tuple(),
        include_file_regexes=tuple(),
    ):
        self._exclude_dir_set = RegexSet(exclude_dir_regexes)
        self._exclude_file_set = RegexSet(exclude_file_regexes)
        self._include_file_set = RegexSet(include_file_regexes)

    def should_exclude_file(self, file_path):
        """
        Given the full path of a file, should it be excluded from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff excluded.
        """
        return self._exclude_file_set.matches(file_path) and \
               not self._include_file_set.matches(file_path)

    def should_exclude_directory(self, dir_path):
        """
        Given the full path of a directory, should all of the files in it be
        excluded from the scan?

        :param dir_path: The path of the directory, relative to the root directory
                         being scanned.  The path will never end in '/'.
        :return: True iff excluded.
        """
        return self._exclude_dir_set.matches(dir_path)


DEFAULT_SCAN_MANAGER = ScanPoliciesManager()
