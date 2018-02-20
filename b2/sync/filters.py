import re
import six
from abc import ABCMeta, abstractmethod


@six.add_metaclass(ABCMeta)
class FileFilter(object):

    @abstractmethod
    def is_exclude_filter(self, file_path):
        pass


@six.add_metaclass(ABCMeta)
class ExcludeFileFilter(FileFilter):
    """
    Policy object that decides which files should be excluded in a scan.
    Exclude filters never force file. Matched files are dropped.
    """
    @abstractmethod
    def should_exclude_file(self, file_path):
        """
        Given the full path of a file, should it be excluded from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff excluded.
        """

    def is_exclude_filter(self, file_path):
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

    def is_exclude_filter(self, file_path):
        return False


class ExcludeDirRegexFilter(ExcludeFileFilter):
    def __init__(self, regex):
        self._regex = re.compile(regex + '.*')

    def should_exclude_file(self, file_path):
        return self._regex.match(file_path)


class ExcludeFileRegexFilter(ExcludeFileFilter):
    def __init__(self, regex):
        self._regex = re.compile(regex)

    def should_exclude_file(self, file_path):
        return self._regex.match(file_path)


class IncludeFileRegexFilter(IncludeFileFilter):
    def __init__(self, regex):
        self._regex = re.compile(regex)

    def should_include_file(self, file_path):
        return self._regex.match(file_path)


class FilterManager(object):

    FILTERS_PRIORITES = {
        ExcludeDirRegexFilter: 100,
        IncludeFileFilter: 50,
        ExcludeFileRegexFilter: 0,
    }

    def __init__(self, filters):
        # Sorting filters by priorites
        self._filters = filters.sort(key=lambda f: self.FILTERS_PRIORITES[f.__class__])

    def exclude(self, file_path):
        for filter_obj in self._filters:
            if filter_obj.is_exclude_filter():
                if filter_obj.should_exclude_file(file_path):
                    return True
            else:
                if filter_obj.should_include_file(file_path):
                    return False
        return False
