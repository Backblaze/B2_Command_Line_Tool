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
import six
import os.path
from abc import ABCMeta, abstractmethod

logger = logging.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class ScanExcludePolicy(object):
    """
    Policy object that decides which files should be excluded in a scan.
    Exclude policies never force file. Matched files are dropped irreversibly.
    """

    @abstractmethod
    def should_exclude_file(self, file_path):
        """
        Given the full path of a file, should it be excluded from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff excluded.
        """

    @abstractmethod
    def should_exclude_directory(self, dir_path):
        """
        Given the full path of a directory, should all of the files in it be
        excluded from the scan?

        :param dir_path: The path of the directory, relative to the root directory
                         being scanned.  This is a directory, so the path will
                         always end in '/'.
        :return: True iff excluded.
        """

    def should_exclude(self, path):
        if path.endswith('/'):
            return self.should_exclude_directory(path)
        return self.should_exclude_file(path)


@six.add_metaclass(ABCMeta)
class ScanIncludePolicy(object):
    """
    Policy object that decides which files should be included in a scan.
    Include policies always force matched file. Once forced file is always in scan.
    """

    @abstractmethod
    def should_include_file(self, file_path):
        """
        Given the full path of a file, should it be included from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff included.
        """

    @abstractmethod
    def should_include_directory(self, dir_path):
        """
        Given the full path of a directory, should all of the files in it be
        included from the scan?

        :param dir_path: The path of the directory, relative to the root directory
                         being scanned.  This is a directory, so the path will
                         always end in '/'.
        :return: True iff excluded.
        """

    def should_include(self, path):
        if path.endswith('/'):
            return self.should_include_directory(path)
        return self.should_include_file(path)


class ScanExcludeDirRegex(ScanExcludePolicy):
    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __repr__(self):
        return 'ExcludeDirRegex(%s)' % (self.regex_str,)

    def should_exclude_file(self, file_path):
        head, _ = os.path.split(file_path)
        return self.regex.match(head)

    def should_exclude_directory(self, dir_path):
        return self.regex.match(dir_path)


class ScanExcludeFileRegex(ScanExcludePolicy):
    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __repr__(self):
        return 'ExcludeFileRegex(%s)' % (self.regex_str,)

    def should_exclude_file(self, file_path):
        return self.regex.match(file_path)

    def should_exclude_directory(self, dir_path):
        # exclude file regex never excludes directories
        return False


class ScanIncludeFileRegex(ScanIncludePolicy):
    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __repr__(self):
        return 'IncludeFileRegex(%s)' % (self.regex_str,)

    def should_include_file(self, file_path):
        return self.regex.match(file_path)

    def should_include_directory(self, dir_path):
        # include file regex never includes directories
        return False


class ScanPoliciesManager(object):
    def __init__(
        self,
        exclude_dir_regexes=tuple(),
        exclude_file_regexes=tuple(),
        include_file_regexes=tuple(),
    ):
        self._exclude_dir_polices = [ScanExcludeDirRegex(regex) for regex in exclude_dir_regexes]
        self._include_file_polices = [ScanIncludeFileRegex(regex) for regex in include_file_regexes]
        self._exclude_file_polices = [ScanExcludeFileRegex(regex) for regex in exclude_file_regexes]

    def exclude(self, path):
        if any(policy.should_exclude(path) for policy in self._exclude_dir_polices):
            return True
        if any(policy.should_include(path) for policy in self._include_file_polices):
            return False
        return any(policy.should_exclude(path) for policy in self._exclude_file_polices)


DEFAULT_SCAN_MANAGER = ScanPoliciesManager()
