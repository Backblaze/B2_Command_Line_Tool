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


class ScanExcludeDirRegex(object):
    """
    Policy object that decides which files should be excluded in a scan.
    """

    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __repr__(self):
        return 'ScanExcludeDirRegex(%s)' % (self.regex_str,)

    def should_exclude_directory(self, dir_path):
        """
        Given the full path of a directory, should all of the files in it be
        excluded from the scan?

        :param dir_path: The path of the directory, relative to the root directory
                         being scanned.  This is a directory, so the path will
                         always end in '/'.
        :return: True iff excluded.
        """
        return self.regex.match(dir_path)


class ScanExcludeFileRegex(object):
    """
    Policy object that decides which files should be excluded in a scan.
    """

    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __repr__(self):
        return 'ScanExcludeFileRegex(%s)' % (self.regex_str,)

    def should_exclude_file(self, file_path):
        """
        Given the full path of a file, should it be excluded from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff excluded.
        """
        return self.regex.match(file_path)


class ScanIncludeFileRegex(object):
    """
    Policy object that decides which files should be included in a scan.
    """

    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __repr__(self):
        return 'ScanIncludeFileRegex(%s)' % (self.regex_str,)

    def should_include_file(self, file_path):
        """
        Given the full path of a file, should it be included into the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff excluded.
        """
        return self.regex.match(file_path)


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
        self._exclude_dir_polices = [ScanExcludeDirRegex(regex) for regex in exclude_dir_regexes]
        self._include_file_polices = [ScanIncludeFileRegex(regex) for regex in include_file_regexes]
        self._exclude_file_polices = [ScanExcludeFileRegex(regex) for regex in exclude_file_regexes]

    def should_exclude_file(self, file_path):
        """
        Given the full path of a file, should it be excluded from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff excluded.
        """
        if any(policy.should_include_file(file_path) for policy in self._include_file_polices):
            return False
        return any(policy.should_exclude_file(file_path) for policy in self._exclude_file_polices)

    def should_exclude_directory(self, dir_path):
        """
        Given the full path of a directory, should all of the files in it be
        excluded from the scan?

        :param dir_path: The path of the directory, relative to the root directory
                         being scanned.  The path will never end in '/'.
        :return: True iff excluded.
        """
        return any(
            policy.should_exclude_directory(dir_path) for policy in self._exclude_dir_polices
        )


DEFAULT_SCAN_MANAGER = ScanPoliciesManager()
