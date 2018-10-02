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


def convert_dir_regex_to_dir_prefix_regex(dir_regex):
    """
    The patterns used to match directory names (and file names) are allowed
    to match a prefix of the name.  This 'feature' was unintentional, but is
    being retained for compatibility.

    This means that a regex that matches a directory name can't be used directly
    to match against a file name and test whether the file should be excluded
    because it matches the directory.

    The pattern 'photos' will match directory names 'photos' and 'photos2',
    and should exclude files 'photos/kitten.jpg', and 'photos2/puppy.jpg'.
    It should not exclude 'photos.txt', because there is no directory name
    that matches.

    On the other hand, the pattern 'photos$' should match 'photos/kitten.jpg',
    but not 'photos2/puppy.jpg', nor 'photos.txt'

    If the original regex is valid, there are only two cases to consider:
    either the regex ends in '$' or does not.
    """
    if dir_regex.endswith('$'):
        return dir_regex[:-1] + r'/'
    else:
        return dir_regex + r'.*?/'


class ScanPoliciesManager(object):
    """
    Policy object used when scanning folders for syncing, used to decide
    which files to include in the list of files to be synced.

    Code that scans through files should at least use should_exclude_file()
    to decide whether each file should be included; it will check include/exclude
    patterns for file names, as well as patterns for excluding directeries.

    Code that scans may optionally use should_exclude_directory() to test whether
    it can skip a directory completely and not bother listing the files and
    sub-directories in it.
    """

    def __init__(
        self,
        exclude_dir_regexes=tuple(),
        exclude_file_regexes=tuple(),
        include_file_regexes=tuple(),
        exclude_all_symlinks=False,
    ):
        self._exclude_dir_set = RegexSet(exclude_dir_regexes)
        self._exclude_file_because_of_dir_set = RegexSet(
            map(convert_dir_regex_to_dir_prefix_regex, exclude_dir_regexes)
        )
        self._exclude_file_set = RegexSet(exclude_file_regexes)
        self._include_file_set = RegexSet(include_file_regexes)
        self.exclude_all_symlinks = exclude_all_symlinks

    def should_exclude_file(self, file_path):
        """
        Given the full path of a file, should it be excluded from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff excluded.
        """
        exclude_because_of_dir = self._exclude_file_because_of_dir_set.matches(file_path)
        exclude_because_of_file = (
            self._exclude_file_set.matches(file_path) and
            not self._include_file_set.matches(file_path)
        )
        return exclude_because_of_dir or exclude_because_of_file

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
