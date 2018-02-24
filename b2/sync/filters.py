######################################################################
#
# File: b2/sync/filters.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
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
class FileFilter(object):
    @abstractmethod
    def is_exclude_filter(self):
        pass


@six.add_metaclass(ABCMeta)
class ExcludeFileFilter(FileFilter):
    """
    Policy object that decides which files should be excluded in a scan.
    Exclude filters never force file. Matched files are dropped irreversibly.
    """

    @abstractmethod
    def should_exclude_file(self, file_path):
        """
        Given the full path of a file, should it be excluded from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff excluded.
        """

    def is_exclude_filter(self):
        return True


@six.add_metaclass(ABCMeta)
class IncludeFileFilter(FileFilter):
    """
    Policy object that decides which files should be included in a scan.
    Include filters always force matched file. Once forced file is always in scan.
    """

    @abstractmethod
    def should_include_file(self, file_path):
        """
        Given the full path of a file, should it be included from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff included.
        """

    def is_exclude_filter(self):
        return False


class ExcludeDirRegexFilter(ExcludeFileFilter):
    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __repr__(self):
        return 'ExcludeDirRegexFilter(%s)' % (self.regex_str,)

    def should_exclude_file(self, file_path):
        head, _ = os.path.split(file_path)
        return self.regex.match(head)


class ExcludeFileRegexFilter(ExcludeFileFilter):
    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __repr__(self):
        return 'ExcludeFileRegexFilter(%s)' % (self.regex_str,)

    def should_exclude_file(self, file_path):
        return self.regex.match(file_path)


class IncludeFileRegexFilter(IncludeFileFilter):
    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __repr__(self):
        return 'IncludeFileRegexFilter(%s)' % (self.regex_str,)

    def should_include_file(self, file_path):
        return self.regex.match(file_path)


class FilterManager(object):

    FILTERS_PRIORITES = {
        ExcludeDirRegexFilter: 100,
        IncludeFileRegexFilter: 50,
        ExcludeFileRegexFilter: 0,
    }

    def __init__(self, filters):
        self._filters = filters
        # Sorting filters by priorites
        self._filters.sort(key=lambda f: self.FILTERS_PRIORITES[f.__class__], reverse=True)

    def exclude(self, file_path):
        for filter_obj in self._filters:
            if filter_obj.is_exclude_filter():
                if filter_obj.should_exclude_file(file_path):
                    logger.debug("%s excluded from scan by filter %s", file_path, filter_obj)
                    return True
            else:
                if filter_obj.should_include_file(file_path):
                    logger.debug("%s included to scan by filter %s", file_path, filter_obj)
                    return False
        return False
